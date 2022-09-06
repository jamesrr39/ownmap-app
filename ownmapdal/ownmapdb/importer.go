package ownmapdb

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"path/filepath"
	"sort"

	"github.com/gogo/protobuf/proto"
	"github.com/jamesrr39/goutil/binaryx"
	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/goutil/gofs"
	"github.com/jamesrr39/goutil/humanise"
	"github.com/jamesrr39/goutil/logpkg"
	"github.com/jamesrr39/ownmap-app/ownmap"
	"github.com/jamesrr39/ownmap-app/ownmapdal"
	"github.com/jamesrr39/ownmap-app/ownmapdal/ownmapdb/diskfilemap"
	"github.com/paulmach/osm/osmpbf"
)

const (
	// HeaderSizeContainerSize is the size of the container that holds the amount of bytes that must be read to read the header
	HeaderSizeContainerSize = 4
)

type TagIndexMap map[tagCollectionKeyType][]int64

func (m TagIndexMap) ToTagIndexRecords() []*TagIndexRecord {
	var tagIndexRecords []*TagIndexRecord
	for k, v := range m {
		tagIndexRecords = append(tagIndexRecords, &TagIndexRecord{
			IndexKey: k.MarshalKey(),
			ItemIDs:  v,
		})
	}
	return tagIndexRecords
}

const _1K = 1 << 10

func makeInt64BucketNameFunc(key []byte) (diskfilemap.BucketName, errorsx.Error) {
	id := int64(binary.LittleEndian.Uint64(key))
	bucketID := id / _1K
	return binaryx.LittleEndianPutUint64(uint64(bucketID)), nil
}

func isKey1GreaterThanKey2CompareInt64Func(key1, key2 []byte) (bool, errorsx.Error) {
	val1 := binary.LittleEndian.Uint64(key1)
	val2 := binary.LittleEndian.Uint64(key2)
	return val1 > val2, nil
}

func makeTagIndexBucketNameFunc(key []byte) (diskfilemap.BucketName, errorsx.Error) {
	tck := new(tagCollectionKeyType)
	err := tck.UnmarshalKey(key)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	// have buckets separated by LatBucket, LonBucket, ObjectType. (Group all Tag Keys together)
	tck.TagKey = ""
	return tck.MarshalKey(), nil
}

func isKey1GreaterThanKey2CompareTagsFunc(key1, key2 []byte) (bool, errorsx.Error) {
	return IsTagCollectionKey1BytesLargerThanKey2(key1, key2), nil
}

type Collections struct {
	NodeCollection, WayCollection, RelationCollection, TagCollection diskfilemap.OnDiskCollection
}

type ImportOptions struct {
	KeepWorkDir bool
}

type Importer struct {
	logger                   *logpkg.Logger
	fs                       gofs.Fs
	workDir, outFilePath     string
	ownmapDBFileHandlerLimit uint
	collections              *Collections
	pbfHeader                *osmpbf.Header
	options                  ImportOptions
}

func NewFinalStorage(logger *logpkg.Logger, fs gofs.Fs, workDir, outFilePath string, ownmapDBFileHandlerLimit uint, pbfHeader *osmpbf.Header, options ImportOptions) (*Importer, errorsx.Error) {
	collections, err := makeCollections(fs, workDir)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}
	return &Importer{logger, fs, workDir, outFilePath, ownmapDBFileHandlerLimit, collections, pbfHeader, options}, nil
}

func makeCollections(fs gofs.Fs, workDir string) (*Collections, errorsx.Error) {
	var err error
	collections := new(Collections)

	collections.NodeCollection, err = diskfilemap.NewDiskCollection(fs, filepath.Join(workDir, "node_collection.ownmap_import_cache"), makeInt64BucketNameFunc, isKey1GreaterThanKey2CompareInt64Func)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}
	collections.WayCollection, err = diskfilemap.NewDiskCollection(fs, filepath.Join(workDir, "way_collection.ownmap_import_cache"), makeInt64BucketNameFunc, isKey1GreaterThanKey2CompareInt64Func)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}
	collections.RelationCollection, err = diskfilemap.NewDiskCollection(fs, filepath.Join(workDir, "relation_collection.ownmap_import_cache"), makeInt64BucketNameFunc, isKey1GreaterThanKey2CompareInt64Func)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}
	collections.TagCollection, err = diskfilemap.NewDiskCollection(fs, filepath.Join(workDir, "tag_collection.ownmap_import_cache"), makeTagIndexBucketNameFunc, isKey1GreaterThanKey2CompareTagsFunc)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	return collections, nil
}

func setTagsOnCollection(keys []*tagCollectionKeyType, collection diskfilemap.OnDiskCollection, itemID int64) errorsx.Error {
	for _, tagsCollectionKey := range keys {
		var err error

		key := tagsCollectionKey.MarshalKey()
		tagIndexRecord := &TagIndexRecord{
			IndexKey: key,
		}
		existingBytes, err := collection.Get(key)
		if err != nil {
			if errorsx.Cause(err) != errorsx.ObjectNotFound {
				// unexpected error
				return errorsx.Wrap(err)
			}
		}

		if err == nil {
			// unmarshal existing into tagIndexRecord
			err = proto.Unmarshal(existingBytes, tagIndexRecord)
			if err != nil {
				return errorsx.Wrap(err)
			}
		}

		tagIndexRecord.ItemIDs = append(tagIndexRecord.ItemIDs, itemID)

		valBytes, err := proto.Marshal(tagIndexRecord)
		if err != nil {
			return errorsx.Wrap(err)
		}

		err = collection.Set(key, valBytes)
		if err != nil {
			return errorsx.Wrap(err)
		}
	}

	return nil
}

func (importer *Importer) ImportNodes(nodes []*ownmap.OSMNode) errorsx.Error {
	for _, node := range nodes {
		bb := binaryx.LittleEndianPutUint64(uint64(node.ID))
		ownmapNodeBytes, err := proto.Marshal(node)
		if err != nil {
			return errorsx.Wrap(err)
		}

		err = importer.collections.NodeCollection.Set(bb, ownmapNodeBytes)
		if err != nil {
			return errorsx.Wrap(err)
		}

		var tagCollectionKeys []*tagCollectionKeyType
		for _, tag := range node.Tags {
			tagCollectionKeys = append(tagCollectionKeys, newTagCollectionKey(node.Lat, node.Lon, ownmap.ObjectTypeNode, tag.Key))
		}

		err = setTagsOnCollection(tagCollectionKeys, importer.collections.TagCollection, node.ID)
		if err != nil {
			return errorsx.Wrap(err)
		}
	}

	return nil
}

func (importer *Importer) ImportWays(ownmapWays []*ownmap.OSMWay) errorsx.Error {
	for _, ownmapWay := range ownmapWays {
		var err error
		nodes, err := importer.getNodesInWay(ownmapWay)
		if err != nil {
			return errorsx.Wrap(err)
		}

		bb := binaryx.LittleEndianPutUint64(uint64(ownmapWay.ID))
		ownmapWayBytes, err := proto.Marshal(ownmapWay)
		if err != nil {
			return errorsx.Wrap(err)
		}
		err = importer.collections.WayCollection.Set(bb, ownmapWayBytes)
		if err != nil {
			return errorsx.Wrap(err)
		}

		tagCollectionKeys := buildTagIndexesForObject(ownmapWay.Tags, ownmap.GetPointsFromNodes(nodes), ownmap.ObjectTypeWay)

		err = setTagsOnCollection(tagCollectionKeys, importer.collections.TagCollection, ownmapWay.ID)
		if err != nil {
			return errorsx.Wrap(err)
		}
	}

	return nil
}

func (importer *Importer) getObjectByID(id int64, collection diskfilemap.OnDiskCollection, object proto.Message) error {
	var err error

	val, err := collection.Get(binaryx.LittleEndianPutUint64(uint64(id)))
	if err != nil {
		return err
	}

	err = proto.Unmarshal(val, object)
	if err != nil {
		return errorsx.Wrap(err)
	}

	return nil
}
func (importer *Importer) GetNodeByID(id int64) (*ownmap.OSMNode, error) {
	node := new(ownmap.OSMNode)
	err := importer.getObjectByID(id, importer.collections.NodeCollection, node)
	if err != nil {
		return nil, err
	}

	return node, nil
}
func (importer *Importer) GetWayByID(id int64) (*ownmap.OSMWay, error) {
	way := new(ownmap.OSMWay)
	err := importer.getObjectByID(id, importer.collections.WayCollection, way)
	if err != nil {
		return nil, err
	}

	return way, nil
}
func (importer *Importer) GetRelationByID(id int64) (*ownmap.OSMRelation, error) {
	relation := new(ownmap.OSMRelation)
	err := importer.getObjectByID(id, importer.collections.RelationCollection, relation)
	if err != nil {
		return nil, err
	}

	return relation, nil
}

type tagCollectionKeySet map[tagCollectionKeyType]struct{}

func (importer *Importer) getTagIndexesForRelation(relation *ownmap.OSMRelation) (tagCollectionKeySet, errorsx.Error) {
	set := make(tagCollectionKeySet)

	for _, member := range relation.Members {
		switch member.MemberType {
		case ownmap.OSM_MEMBER_TYPE_NODE:
			node, err := importer.GetNodeByID(member.ObjectID)
			if err != nil {
				if errorsx.Cause(err) != errorsx.ObjectNotFound {
					return nil, errorsx.Wrap(err)
				}

				// node not found. Ignore.
				continue
			}

			tagIndexes := buildTagIndexesForObject(relation.Tags, []*ownmap.Location{{Lat: node.Lat, Lon: node.Lon}}, ownmap.ObjectTypeRelation)
			for _, tagIndex := range tagIndexes {
				set[*tagIndex] = struct{}{}
			}
		case ownmap.OSM_MEMBER_TYPE_WAY:
			way, err := importer.GetWayByID(member.ObjectID)
			if err != nil {
				if errorsx.Cause(err) != errorsx.ObjectNotFound {
					return nil, errorsx.Wrap(err)
				}

				// way not found. Ignore.
				continue
			}

			var points []*ownmap.Location
			for _, waypoint := range way.WayPoints {
				points = append(points, waypoint.Point)
			}

			tagIndexes := buildTagIndexesForObject(relation.Tags, points, ownmap.ObjectTypeRelation)
			for _, tagIndex := range tagIndexes {
				set[*tagIndex] = struct{}{}
			}
		case ownmap.OSM_MEMBER_TYPE_RELATION:
			subRelation, err := importer.GetRelationByID(member.ObjectID)
			if err != nil {
				if errorsx.Cause(err) != errorsx.ObjectNotFound {
					return nil, errorsx.Wrap(err)
				}

				// way not found. Ignore.
				continue
			}

			subRelationTagIndexesSet, err := importer.getTagIndexesForRelation(subRelation)
			if err != nil {
				return nil, errorsx.Wrap(err)
			}

			for tagIndex := range subRelationTagIndexesSet {
				set[tagIndex] = struct{}{}
			}
		}
	}

	return set, nil
}

func (importer *Importer) getNodesInWay(way *ownmap.OSMWay) ([]*ownmap.OSMNode, errorsx.Error) {
	var nodes []*ownmap.OSMNode
	for _, point := range way.WayPoints {
		node, err := importer.GetNodeByID(point.NodeID)
		if err != nil {
			if errorsx.Cause(err) != errorsx.ObjectNotFound {
				return nil, errorsx.Wrap(err)
			}

			// nodeID not imported (out of bounds)
			continue
		}

		nodes = append(nodes, node)
	}
	return nodes, nil
}

func (importer *Importer) ImportRelations(relations []*ownmap.OSMRelation) errorsx.Error {
	for _, relation := range relations {
		bb := binaryx.LittleEndianPutUint64(uint64(relation.ID))
		relationBytes, err := proto.Marshal(relation)
		if err != nil {
			return errorsx.Wrap(err)
		}

		importer.logger.Debug("about to set relation in collection. ID: %d", relation.ID)
		err = importer.collections.RelationCollection.Set(bb, relationBytes)
		if err != nil {
			return errorsx.Wrap(err)
		}
		importer.logger.Debug("finished setting relation in collection. ID: %d", relation.ID)
		tagIndexSet, err := importer.getTagIndexesForRelation(relation)
		if err != nil {
			return errorsx.Wrap(err)
		}

		var tagCollectionKeys []*tagCollectionKeyType
		for tagIndex := range tagIndexSet {
			// copy tagIndex so the pointer is set to different memory locations
			copyOfTagIndex := tagIndex
			tagCollectionKeys = append(tagCollectionKeys, &copyOfTagIndex)
		}

		importer.logger.Debug("about to set tags on tag collection for relation. ID: %d", relation.ID)
		err = setTagsOnCollection(tagCollectionKeys, importer.collections.TagCollection, relation.ID)
		if err != nil {
			return errorsx.Wrap(err)
		}
		importer.logger.Debug("finished setting tags on tag collection for relation. ID: %d", relation.ID)
	}

	return nil
}

type sectionMetadataImportType struct {
	Metadata *SectionMetadata
	File     io.ReadCloser
}

func (importer *Importer) Rollback() errorsx.Error {
	if importer.options.KeepWorkDir {
		return nil
	}

	err := importer.fs.RemoveAll(importer.workDir)
	if err != nil {
		return errorsx.Wrap(err)
	}

	return nil
}

func (importer *Importer) Commit() (ownmapdal.DataSourceConn, errorsx.Error) {

	err := importer.fs.MkdirAll(importer.workDir, 0700)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	datasetInfo := &ownmap.DatasetInfo{
		Bounds:            ownmap.OSMBoundsToDatasetInfoBounds(*importer.pbfHeader.Bounds),
		ReplicationTimeMs: uint64(importer.pbfHeader.ReplicationTimestamp.UnixNano() / (1000 * 1000)),
	}

	// save data to files
	nodesSectionMetadata, nodesFile, err := createSectionFromDisk(importer.fs, importer.collections.NodeCollection, NewNodesFromDiskBlockData(), importer.workDir, "nodes_workdir", blockSize)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}
	defer nodesFile.Close()

	waysSectionMetadata, waysFile, err := createSectionFromDisk(importer.fs, importer.collections.WayCollection, NewWaysFromDiskBlockData(), importer.workDir, "ways_workdir", blockSize)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}
	defer waysFile.Close()

	relationsSectionMetadata, relationsFile, err := createSectionFromDisk(importer.fs, importer.collections.RelationCollection, NewRelationsFromDiskBlockData(), importer.workDir, "relations_workdir", blockSize)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}
	defer relationsFile.Close()

	tagIndexSectionMetadata, tagIndexFile, err := createSectionFromDisk(importer.fs, importer.collections.TagCollection, NewTagsFromDiskBlockData(), importer.workDir, "tags_workdir", blockSize)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}
	defer tagIndexFile.Close()

	// concat files and write header
	header := &Header{
		Version:                  1,
		DatasetInfo:              datasetInfo,
		NodesSectionMetadata:     nodesSectionMetadata,
		WaysSectionMetadata:      waysSectionMetadata,
		RelationsSectionMetadata: relationsSectionMetadata,
		TagIndexSectionMetadata:  tagIndexSectionMetadata,
	}

	headerBytes, err := proto.Marshal(header)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	headerSizeContainer := make([]byte, HeaderSizeContainerSize)
	binary.LittleEndian.PutUint32(headerSizeContainer, uint32(len(headerBytes)))

	finalBuildFilePath := filepath.Join(importer.workDir, "final_build.ownmapdb")
	finalBuildFile, err := importer.fs.Create(finalBuildFilePath)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}
	// do not close outFile, since we need it later on to create the MapmakerDBConn

	type labelledReaderType struct {
		Name string
		io.Reader
	}

	headerSizeBB := bytes.NewBuffer(headerSizeContainer)
	headerBB := bytes.NewBuffer(headerBytes)

	readers := []labelledReaderType{
		{"Header Size Container", headerSizeBB},
		{"Header", headerBB},
		{"Nodes", nodesFile},
		{"Ways", waysFile},
		{"Relations", relationsFile},
		{"Tags", tagIndexFile},
	}

	for _, bb := range readers {
		bytesWritten, err := io.Copy(finalBuildFile, bb)
		if err != nil {
			return nil, errorsx.Wrap(err)
		}

		log.Printf("section %q, bytes written: %s:: %d\n", bb.Name, humanise.HumaniseBytes(bytesWritten), bytesWritten)
	}

	err = finalBuildFile.Sync()
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	err = importer.fs.Rename(finalBuildFilePath, importer.outFilePath)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	openFileFunc := func() (gofs.File, errorsx.Error) {
		outFile, err := importer.fs.Open(importer.outFilePath)
		if err != nil {
			return nil, errorsx.Wrap(err)
		}
		return outFile, nil
	}

	if !importer.options.KeepWorkDir {
		err = importer.fs.RemoveAll(importer.workDir)
		if err != nil {
			return nil, errorsx.Wrap(err)
		}
	}

	return NewMapmakerDBConn(openFileFunc, filepath.Base(importer.outFilePath), importer.ownmapDBFileHandlerLimit)
}

func buildTagIndexesForObject(objectTags []*ownmap.OSMTag, points []*ownmap.Location, objectType ownmap.ObjectType) []*tagCollectionKeyType {
	type locationBucketType struct {
		LatBucket, LonBucket int
	}

	locationBucketMap := make(map[locationBucketType]bool)

	var previousPoint *ownmap.Location
	for _, point := range points {
		// calculate lat/lon indexes between the nodes
		var locationBuckets []locationBucketType

		locationBucket := locationBucketType{
			LatBucket: bucketFromLatOrLon(point.Lat),
			LonBucket: bucketFromLatOrLon(point.Lon),
		}
		locationBuckets = append(locationBuckets, locationBucket)

		if previousPoint != nil {
			previousNodeBucket := locationBucketType{
				LatBucket: bucketFromLatOrLon(previousPoint.Lat),
				LonBucket: bucketFromLatOrLon(previousPoint.Lon),
			}
			locationBuckets = append(locationBuckets, previousNodeBucket)

			var lowestLatBucket locationBucketType
			var highestLatBucket locationBucketType
			if previousNodeBucket.LatBucket < locationBucket.LatBucket {
				lowestLatBucket = previousNodeBucket
				highestLatBucket = locationBucket
			} else {
				lowestLatBucket = locationBucket
				highestLatBucket = previousNodeBucket
			}

			var lowestLonBucket locationBucketType
			var highestLonBucket locationBucketType
			if previousNodeBucket.LonBucket < locationBucket.LonBucket {
				lowestLonBucket = previousNodeBucket
				highestLonBucket = locationBucket
			} else {
				lowestLonBucket = locationBucket
				highestLonBucket = previousNodeBucket
			}

			for latBucket := lowestLatBucket.LatBucket; latBucket <= highestLatBucket.LatBucket; latBucket++ {
				for lonBucket := lowestLonBucket.LonBucket; lonBucket <= highestLonBucket.LonBucket; lonBucket++ {
					locationBuckets = append(locationBuckets, locationBucketType{
						LatBucket: latBucket,
						LonBucket: lonBucket,
					})
				}
			}
		}

		for _, locationBucket := range locationBuckets {
			locationBucketMap[locationBucket] = true
		}
		previousPoint = point

	}

	var tagCollectionKeys []*tagCollectionKeyType

	for locationBucket := range locationBucketMap {
		for _, tag := range objectTags {
			tagCollectionKey := &tagCollectionKeyType{
				LatBucket:  locationBucket.LatBucket,
				LonBucket:  locationBucket.LonBucket,
				ObjectType: objectType,
				TagKey:     tag.Key,
			}

			tagCollectionKeys = append(tagCollectionKeys, tagCollectionKey)
		}
	}

	// deterministic output for tests
	sort.Slice(tagCollectionKeys, func(i, j int) bool {
		result := tagCollectionKeys[i].Compare(tagCollectionKeys[j])
		if result == ComparisonResultAGreaterThanB {
			return false
		}
		return true
	})

	return tagCollectionKeys
}
