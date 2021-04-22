package ownmapdb

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/gogo/protobuf/proto"
	"github.com/jamesrr39/goutil/algorithms"
	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/goutil/gofs"
	"github.com/jamesrr39/ownmap-app/ownmap"
	"github.com/jamesrr39/ownmap-app/ownmapdal"
	"github.com/paulmach/osm"
	"golang.org/x/exp/errors/fmt"
)

const (
	blockSize = 8192 // items in block. TODO: make configurable
)

type OpenFileFunc func() (gofs.File, errorsx.Error)

type FileHandlerPool struct {
	freeHandlers chan gofs.File
}

func NewFileHandlerPool(openFileFunc OpenFileFunc, limit uint) (*FileHandlerPool, errorsx.Error) {
	freeHandlersChan := make(chan gofs.File, limit)
	for i := 0; i < int(limit); i++ {
		handler, err := openFileFunc()
		if err != nil {
			return nil, errorsx.Wrap(err)
		}
		freeHandlersChan <- handler
	}
	return &FileHandlerPool{freeHandlersChan}, nil
}

func (p *FileHandlerPool) Get() gofs.File {
	return <-p.freeHandlers
}

func (p *FileHandlerPool) Release(handler gofs.File) {
	p.freeHandlers <- handler
}

type MapmakerDBConn struct {
	name            string
	header          *Header
	fileHandlerPool *FileHandlerPool
}

func NewMapmakerDBConn(openFileFunc OpenFileFunc, name string, fileHandlerLimit uint) (*MapmakerDBConn, errorsx.Error) {
	var err error
	fileHandlerPool, err := NewFileHandlerPool(openFileFunc, fileHandlerLimit)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}
	file := fileHandlerPool.Get()
	defer fileHandlerPool.Release(file)

	headerSizeBuffer := make([]byte, HeaderSizeContainerSize)
	_, err = file.Read(headerSizeBuffer)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}
	headerSize := binary.LittleEndian.Uint32(headerSizeBuffer)

	headerBuffer := make([]byte, headerSize)
	_, err = file.Read(headerBuffer)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	header := new(Header)
	err = proto.Unmarshal(headerBuffer, header)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	return &MapmakerDBConn{name, header, fileHandlerPool}, nil
}

func (db *MapmakerDBConn) Name() string {
	return db.name
}

func (db *MapmakerDBConn) DatasetInfo() (*ownmap.DatasetInfo, errorsx.Error) {
	return db.header.DatasetInfo, nil
}

func (db *MapmakerDBConn) offsetOfNodeSectionFromStartOfFile() int64 {
	return int64(HeaderSizeContainerSize) + int64(db.header.Size())
}

func (db *MapmakerDBConn) offsetOfWaySectionFromStartOfFile() int64 {
	return int64(HeaderSizeContainerSize) + int64(db.header.Size()) + int64(db.header.NodesSectionMetadata.TotalSize)
}

func (db *MapmakerDBConn) offsetOfRelationSectionFromStartOfFile() int64 {
	return int64(HeaderSizeContainerSize) + int64(db.header.Size()) + int64(db.header.NodesSectionMetadata.TotalSize) + int64(db.header.WaysSectionMetadata.TotalSize)
}

func (db *MapmakerDBConn) offsetOfTagIndexSectionFromStartOfFile() int64 {
	return int64(HeaderSizeContainerSize) + int64(db.header.Size()) + int64(db.header.NodesSectionMetadata.TotalSize) + int64(db.header.WaysSectionMetadata.TotalSize) + int64(db.header.RelationsSectionMetadata.TotalSize)
}

func getTagIndexesFoundFunc(tagCollectionKeysMap TagIndexMap) onFoundBlockDataFuncType {
	return func(blockDataBytes *bytes.Buffer, wantedKeys []KeyType) errorsx.Error {
		blockData := new(TagIndexBlockData)

		err := proto.Unmarshal(blockDataBytes.Bytes(), blockData)
		if err != nil {
			return errorsx.Wrap(err)
		}

		for _, key := range wantedKeys {
			tagKeyCollectionKey := key.(*tagCollectionKeyType)
			tagKeyIdx, searchResult := algorithms.BinarySearch(len(blockData.TagIndexRecords), func(i int) algorithms.SearchResult {
				thisTagKeyCollection := new(tagCollectionKeyType)
				err := thisTagKeyCollection.UnmarshalKey(blockData.TagIndexRecords[i].IndexKey)
				if err != nil {
					panic("error unmarshalling tagKeyCollectionType: " + err.Error())
				}

				result := tagKeyCollectionKey.Compare(thisTagKeyCollection)
				switch result {
				case ComparisonResultEqual:
					return algorithms.SearchResultFound
				case ComparisonResultAGreaterThanB:
					return algorithms.SearchResultGoHigher
				case ComparisonResultALessThanB:
					return algorithms.SearchResultGoLower
				}

				panic("didn't understand comparison result")
			})

			if searchResult != algorithms.SearchResultFound {
				// no objects found in this tag collection key grouping
				continue
			}

			tagCollectionKeysMap[*tagKeyCollectionKey] = append(
				tagCollectionKeysMap[*tagKeyCollectionKey],
				blockData.TagIndexRecords[tagKeyIdx].ItemIDs...,
			)
		}

		return nil
	}
}

// node IDs, way IDs, error
func (db *MapmakerDBConn) addItemIDsForTagCollectionKeys(file io.ReadSeeker, tagCollectionKeysMap TagIndexMap) errorsx.Error {
	// make various data structures needed later in the function
	var keys []KeyType

	for wantedTagCollectionKey := range tagCollectionKeysMap {
		tagCollectionKeyCopy := wantedTagCollectionKey
		keys = append(keys, &tagCollectionKeyCopy)
	}

	err := db.getBlockDatasByIDs(
		file,
		keys,
		getTagIndexesFoundFunc(tagCollectionKeysMap),
		db.offsetOfTagIndexSectionFromStartOfFile(),
		db.header.TagIndexSectionMetadata,
		new(tagCollectionKeyType),
	)
	if err != nil {
		return errorsx.Wrap(err)
	}

	return nil
}

type ComparisonResult int

const (
	ComparisonResultEqual         ComparisonResult = 0
	ComparisonResultAGreaterThanB ComparisonResult = 1
	ComparisonResultALessThanB    ComparisonResult = -1
)

type KeyType interface {
	MarshalKey() []byte
	UnmarshalKey(data []byte) errorsx.Error
	Compare(other KeyType) ComparisonResult
	String() string
	LowerThanLowestValidValue() KeyType // Zero value is the starting value to compare against (i.e., it is smaller than everything)
}

// FIXME: move to algorithms package
func CompareInt64s(a, b int64) algorithms.SearchResult {
	if a == b {
		return algorithms.SearchResultFound
	}

	if a > b {
		return algorithms.SearchResultGoHigher
	}

	return algorithms.SearchResultGoLower
}

func getNodesFoundFunc(nodeMap map[int64]*ownmap.OSMNode) onFoundBlockDataFuncType {
	return func(blockDataBytes *bytes.Buffer, wantedKeys []KeyType) errorsx.Error {
		blockData := new(NodesBlockData)
		err := proto.Unmarshal(blockDataBytes.Bytes(), blockData)
		if err != nil {
			return errorsx.Wrap(err)
		}

		for _, key := range wantedKeys {
			keyInt64 := int64(*key.(*int64ItemType))

			idx, searchResult := algorithms.BinarySearch(len(blockData.Nodes), func(i int) algorithms.SearchResult {
				return CompareInt64s(keyInt64, blockData.Nodes[i].ID)
			})

			if searchResult != algorithms.SearchResultFound {
				panic(fmt.Sprintf("key not found: %#v", key, keyInt64))
			}

			node := blockData.Nodes[idx]

			// FIXME: start debug: not needed
			_, ok := nodeMap[node.ID]
			if !ok {
				panic(fmt.Sprintf("node found but not wanted: %#v", node))
			}
			// FIXME: end debug

			nodeMap[node.ID] = node
		}

		// for _, node := range blockData.Nodes {
		// 	_, ok := nodeMap[node.ID]
		// 	if !ok {
		// 		// node not wanted
		// 		continue
		// 	}

		// 	nodeMap[node.ID] = node
		// }

		return nil
	}
}

func (db *MapmakerDBConn) addNodesToNodeMap(file io.ReadSeeker, nodeMap map[int64]*ownmap.OSMNode) errorsx.Error {

	// make various data structures needed later in the function
	var keys []KeyType

	for id := range nodeMap {
		key := int64ItemType(id)
		keys = append(keys, &key)
	}

	// FIXME keys => map?
	err := db.getBlockDatasByIDs(
		file,
		keys,
		getNodesFoundFunc(nodeMap),
		db.offsetOfNodeSectionFromStartOfFile(),
		db.header.NodesSectionMetadata,
		new(int64ItemType),
	)
	if err != nil {
		return errorsx.Wrap(err)
	}

	return nil
}

func getWaysFoundFunc(wayMap map[int64]*ownmap.OSMWay) onFoundBlockDataFuncType {
	return func(blockDataBytes *bytes.Buffer, wantedKeys []KeyType) errorsx.Error {
		blockData := new(WaysBlockData)
		err := proto.Unmarshal(blockDataBytes.Bytes(), blockData)
		if err != nil {
			return errorsx.Wrap(err)
		}

		for _, key := range wantedKeys {
			keyInt64 := int64(*key.(*int64ItemType))

			idx, searchResult := algorithms.BinarySearch(len(blockData.Ways), func(i int) algorithms.SearchResult {
				return CompareInt64s(keyInt64, blockData.Ways[i].ID)
			})

			if searchResult != algorithms.SearchResultFound {
				panic(fmt.Sprintf("way: key not found: %#v", key, keyInt64))
			}

			way := blockData.Ways[idx]

			wayMap[way.ID] = way
		}

		return nil
	}
}

func getRelationsFoundFunc(relationMap map[int64]*ownmap.OSMRelation) onFoundBlockDataFuncType {
	return func(blockDataBytes *bytes.Buffer, wantedKeys []KeyType) errorsx.Error {
		blockData := new(RelationsBlockData)
		err := proto.Unmarshal(blockDataBytes.Bytes(), blockData)
		if err != nil {
			return errorsx.Wrap(err)
		}

		for _, key := range wantedKeys {
			keyInt64 := int64(*key.(*int64ItemType))

			idx, searchResult := algorithms.BinarySearch(len(blockData.Relations), func(i int) algorithms.SearchResult {
				return CompareInt64s(keyInt64, blockData.Relations[i].ID)
			})

			if searchResult != algorithms.SearchResultFound {
				panic(fmt.Sprintf("way: key not found: %#v", key, keyInt64))
			}

			relation := blockData.Relations[idx]

			relationMap[relation.ID] = relation
		}

		return nil
	}
}

func (db *MapmakerDBConn) addWaysToWayMap(file io.ReadSeeker, wayMap map[int64]*ownmap.OSMWay) errorsx.Error {
	// make various data structures needed later in the function
	var keys []KeyType

	for id := range wayMap {
		key := int64ItemType(id)
		keys = append(keys, &key)
	}

	err := db.getBlockDatasByIDs(
		file,
		keys,
		getWaysFoundFunc(wayMap),
		db.offsetOfWaySectionFromStartOfFile(),
		db.header.WaysSectionMetadata,
		new(int64ItemType),
	)
	if err != nil {
		return errorsx.Wrap(err)
	}

	return nil
}

// onFoundBlockDataFuncType is called when getBlockDatasByIDs finds a block that is useful to the caller.
// The caller should take note of the itemID if needed and return the correct proto.Message type to decode into.
type onFoundBlockDataFuncType func(blockDataBytes *bytes.Buffer, wantedKeys []KeyType) errorsx.Error

// map[blockIdx][]KeyType
type blockIdxKeyMapType map[int][]KeyType

func getBlockIndexesForKeys(sectionMetadata *SectionMetadata, keys []KeyType, emptyKey KeyType) (blockIdxKeyMapType, errorsx.Error) {
	blockIdxKeyMap := make(blockIdxKeyMapType)

	for _, key := range keys {
		idx, searchResult := algorithms.BinarySearch(len(sectionMetadata.BlockMetadatas), func(i int) algorithms.SearchResult {
			err := emptyKey.UnmarshalKey(sectionMetadata.BlockMetadatas[i].LastItemInBlockValue)
			if err != nil {
				panic("error unmarshalling key: " + err.Error())
			}
			result := key.Compare(emptyKey)
			switch result {
			case ComparisonResultEqual:
				return algorithms.SearchResultFound
			case ComparisonResultALessThanB:
				// could be in this block, or lower
				if i == 0 {
					// must be in this block
					return algorithms.SearchResultFound
				}
				err = emptyKey.UnmarshalKey(sectionMetadata.BlockMetadatas[i-1].LastItemInBlockValue)
				if err != nil {
					panic("error unmarshalling key: " + err.Error())
				}

				previousBlockResult := key.Compare(emptyKey)
				if previousBlockResult != ComparisonResultAGreaterThanB {
					return algorithms.SearchResultGoLower
				}
				return algorithms.SearchResultFound
			case ComparisonResultAGreaterThanB:
				return algorithms.SearchResultGoHigher
			}

			panic(fmt.Sprintf("got unexpected bytes compare result: %d. Key: %#v. Last block ID: %d", result, key, i))
		})

		if searchResult != algorithms.SearchResultFound {
			return nil, errorsx.Errorf("couldn't find Key: %#v.", key)
		}

		blockIdxKeyMap[idx] = append(blockIdxKeyMap[idx], key)
	}

	return blockIdxKeyMap, nil
}

func (db *MapmakerDBConn) getBlockDatasByIDs(
	file io.ReadSeeker,
	wantedKeys []KeyType,
	onFoundBlockDataFunc onFoundBlockDataFuncType,
	sectionOffset int64,
	sectionMetadata *SectionMetadata,
	emptyKey KeyType,
) errorsx.Error {

	blockIndexesForKeys, err := getBlockIndexesForKeys(sectionMetadata, wantedKeys, emptyKey)
	if err != nil {
		return err
	}

	// FIXME: sort keys
	for blockID, wantedKeysInBlock := range blockIndexesForKeys {
		err = db.decodeBlock(file, sectionMetadata.BlockMetadatas[blockID], sectionOffset, onFoundBlockDataFunc, wantedKeysInBlock)
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *MapmakerDBConn) decodeBlock(
	file io.ReadSeeker,
	thisBlockMetadata *BlockMetadata,
	sectionOffset int64,
	onFoundBlockDataFunc onFoundBlockDataFuncType,
	wantedKeys []KeyType,
) errorsx.Error {

	seekToLocation := sectionOffset + thisBlockMetadata.StartOffsetFromStartOfSectionData
	_, err := file.Seek(seekToLocation, io.SeekStart)
	if err != nil {
		return errorsx.Wrap(err)
	}

	bb := make([]byte, thisBlockMetadata.BlockSize)
	_, err = file.Read(bb)
	if err != nil {
		return errorsx.Wrap(err)
	}

	err = onFoundBlockDataFunc(bytes.NewBuffer(bb), wantedKeys)
	if err != nil {
		return errorsx.Wrap(err)
	}

	return nil
}

func (db *MapmakerDBConn) addRelationsToRelationMap(
	file io.ReadSeeker,
	relationMap map[int64]*ownmap.OSMRelation,
	wayMap map[int64]*ownmap.OSMWay,
	nodeMap map[int64]*ownmap.OSMNode,
) errorsx.Error {
	// make various data structures needed later in the function
	var keys []KeyType

	for id, relation := range relationMap {
		if relation != nil {
			// skip this relation, it has already been fetched and added to the map
			continue
		}
		key := int64ItemType(id)
		keys = append(keys, &key)
	}

	err := db.getBlockDatasByIDs(
		file,
		keys,
		getRelationsFoundFunc(relationMap),
		db.offsetOfRelationSectionFromStartOfFile(),
		db.header.RelationsSectionMetadata,
		new(int64ItemType),
	)
	if err != nil {
		return errorsx.Wrap(err)
	}

	mustRescanRelations := false
	for _, relation := range relationMap {
		for _, member := range relation.Members {
			switch member.MemberType {
			case ownmap.OSM_MEMBER_TYPE_NODE:
				nodeMap[member.ObjectID] = nil
			case ownmap.OSM_MEMBER_TYPE_WAY:
				wayMap[member.ObjectID] = nil
			case ownmap.OSM_MEMBER_TYPE_RELATION:
				relationMap[member.ObjectID] = nil
				mustRescanRelations = true
			default:
				return errorsx.Errorf("unrecognized member type: %v", member.MemberType)
			}
		}
	}

	if mustRescanRelations {
		return db.addRelationsToRelationMap(file, relationMap, wayMap, nodeMap)
	}

	return nil
}

func (db *MapmakerDBConn) GetInBounds(bounds osm.Bounds, filter *ownmapdal.GetInBoundsFilter) (ownmapdal.TagNodeMap, ownmapdal.TagWayMap, ownmapdal.TagRelationMap, errorsx.Error) {
	if filter == nil {
		return nil, nil, nil, errorsx.Errorf("filter not supplied")
	}

	minLatBucket := bucketFromLatOrLon(bounds.MinLat)
	maxLatBucket := bucketFromLatOrLon(bounds.MaxLat)
	minLonBucket := bucketFromLatOrLon(bounds.MinLon)
	maxLonBucket := bucketFromLatOrLon(bounds.MaxLon)

	// get tag index sections
	wantedTagIndexSections := make(map[tagCollectionKeyType][]int64)
	for _, filterObject := range filter.Objects {
		for latBucket := minLatBucket; latBucket <= maxLatBucket; latBucket++ {
			for lonBucket := minLonBucket; lonBucket <= maxLonBucket; lonBucket++ {
				tagCollectionKey := newTagCollectionKeyFromLatLonBucket(
					latBucket,
					lonBucket,
					filterObject.ObjectType,
					string(filterObject.TagKey),
				)
				wantedTagIndexSections[tagCollectionKey] = nil
			}
		}
	}

	file := db.fileHandlerPool.Get()
	defer db.fileHandlerPool.Release(file)

	err := db.addItemIDsForTagCollectionKeys(file, wantedTagIndexSections)
	if err != nil {
		return nil, nil, nil, errorsx.Wrap(err)
	}

	nodeMap := make(map[int64]*ownmap.OSMNode)
	wayMap := make(map[int64]*ownmap.OSMWay)
	relationMap := make(map[int64]*ownmap.OSMRelation)
	for tagIndex, ids := range wantedTagIndexSections {
		switch tagIndex.ObjectType {
		case ownmap.ObjectTypeNode:
			for _, id := range ids {
				nodeMap[id] = nil
			}
		case ownmap.ObjectTypeWay:
			for _, id := range ids {
				wayMap[id] = nil
			}
		case ownmap.ObjectTypeRelation:
			for _, id := range ids {
				relationMap[id] = nil
			}
		default:
			return nil, nil, nil, errorsx.Errorf("didn't understand object type: %v", tagIndex.ObjectType)
		}
	}

	err = db.addRelationsToRelationMap(file, relationMap, wayMap, nodeMap)
	if err != nil {
		return nil, nil, nil, errorsx.Wrap(err)
	}

	// add ways
	err = db.addWaysToWayMap(file, wayMap)
	if err != nil {
		return nil, nil, nil, errorsx.Wrap(err)
	}

	// add nodes
	err = db.addNodesToNodeMap(file, nodeMap)
	if err != nil {
		return nil, nil, nil, errorsx.Wrap(err)
	}

	nodeTagMap := make(ownmapdal.TagNodeMap)
	wayTagMap := make(ownmapdal.TagWayMap)
	relationTagMap := make(ownmapdal.TagRelationMap)

	for _, tagKeyWithType := range filter.Objects {
		switch tagKeyWithType.ObjectType {
		case ownmap.ObjectTypeNode:
			nodeTagMap[tagKeyWithType.TagKey] = nil
		case ownmap.ObjectTypeWay:
			wayTagMap[tagKeyWithType.TagKey] = nil
		case ownmap.ObjectTypeRelation:
			relationTagMap[tagKeyWithType.TagKey] = nil
		default:
			return nil, nil, nil, errorsx.Errorf("unknown object type: %q", tagKeyWithType.ObjectType)
		}
	}

	for _, node := range nodeMap {
		for _, tag := range node.Tags {
			tagKey := ownmap.TagKey(tag.Key)
			_, ok := nodeTagMap[tagKey]
			if !ok {
				// not wanted
				continue
			}

			nodeTagMap[tagKey] = append(nodeTagMap[tagKey], node)
		}
	}

	for _, way := range wayMap {
		for _, tag := range way.Tags {
			tagKey := ownmap.TagKey(tag.Key)
			_, ok := wayTagMap[tagKey]
			if !ok {
				// not wanted
				continue
			}

			wayTagMap[tagKey] = append(wayTagMap[tagKey], way)
		}
	}

	for _, relation := range relationMap {
		for _, tag := range relation.Tags {
			tagKey := ownmap.TagKey(tag.Key)
			_, ok := relationTagMap[tagKey]
			if !ok {
				// not wanted
				continue
			}

			relationMembers := []*ownmap.RelationMemberData{}
			for _, member := range relation.Members {
				switch member.MemberType {
				case ownmap.OSM_MEMBER_TYPE_NODE:
					relationMembers = append(relationMembers, &ownmap.RelationMemberData{
						Role:   member.Role,
						Object: nodeMap[member.ObjectID],
					})
				case ownmap.OSM_MEMBER_TYPE_WAY:
					relationMembers = append(relationMembers, &ownmap.RelationMemberData{
						Role:   member.Role,
						Object: wayMap[member.ObjectID],
					})
				case ownmap.OSM_MEMBER_TYPE_RELATION:
					relationMembers = append(relationMembers, &ownmap.RelationMemberData{
						Role:   member.Role,
						Object: relationMap[member.ObjectID],
					})
				default:
					return nil, nil, nil, errorsx.Errorf("unrecognised object type: %v", member.MemberType)
				}
			}

			relationData := &ownmap.RelationData{
				RelationID: relation.ID,
				Tags:       relation.Tags,
				Members:    relationMembers,
			}
			relationTagMap[tagKey] = append(relationTagMap[tagKey], relationData)
		}
	}

	return nodeTagMap, wayTagMap, relationTagMap, nil
}
