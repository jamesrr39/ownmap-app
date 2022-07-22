package ownmapdal

import (
	"time"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/goutil/gofs"
	"github.com/jamesrr39/goutil/logpkg"
	"github.com/jamesrr39/ownmap-app/ownmap"
	"github.com/paulmach/osm"
)

type NodeBatch struct {
	objects []*ownmap.OSMNode
}

type WayBatch struct {
	objects          []*ownmap.OSMWay
	requiredNodesMap map[int64]*ownmap.OSMNode
}

type RelationsBatch struct {
	objects          []*ownmap.OSMRelation
	requiredNodesMap map[int64]*ownmap.OSMNode
	requiredWaysMap  map[int64]*ownmap.OSMWay
	allRelationsMap  map[int64]*ownmap.OSMRelation
}

type Importer2Opts struct {
	BatchSize          int
	BatchSleepDuration time.Duration
}

func DefaultImporter2Opts() Importer2Opts {
	return Importer2Opts{
		BatchSize:          1024 * 32,
		BatchSleepDuration: time.Second / 2,
	}
}

type FinalStorage interface {
	ImportNodes(objs []*ownmap.OSMNode) errorsx.Error
	ImportWays(objs []*ownmap.OSMWay) errorsx.Error
	ImportRelations(objs []*ownmap.OSMRelation) errorsx.Error
	Commit() (DataSourceConn, errorsx.Error)
	Rollback() errorsx.Error
}

const MaxBatches = 1000 * 1000

type Importer2 struct {
	logger                        *logpkg.Logger
	pbfReader, auxillaryPbfReader PBFReader
	fs                            gofs.Fs
	finalStorage                  FinalStorage

	opts Importer2Opts
}

func Import2(
	logger *logpkg.Logger,
	pbfReader PBFReader,
	auxillaryPbfReader PBFReader,
	fs gofs.Fs,
	finalStorage FinalStorage,
) (DataSourceConn, errorsx.Error) {
	var successful bool

	defer func() {
		if !successful {
			err := finalStorage.Rollback()
			if err != nil {
				logger.Error("couldn't rollback. Error: %s\nStack trace:\n%s\n", err.Error(), err.Stack())
			}
		}
	}()

	importer := &Importer2{
		logger:             logger,
		pbfReader:          pbfReader,
		auxillaryPbfReader: auxillaryPbfReader,
		fs:                 fs,
		finalStorage:       finalStorage,
		opts:               DefaultImporter2Opts(),
	}

	header, err := pbfReader.Header()
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	logger.Info("pbf replication timestamp: %v", header.ReplicationTimestamp)

	if importer.opts.BatchSize < 1 {
		return nil, errorsx.Errorf("BatchSize must be more than 0")
	}

	err = importer.pbfReader.Reset()
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	for i := 0; i < MaxBatches; i++ {
		logger.Info("scanning batch %d", i)
		reachedEnd, err := importer.ScanBatch()
		if err != nil {
			logger.Error("error scanning: %s\nStack trace:\n%s\n", err.Error(), err.Stack())
			return nil, errorsx.Wrap(err)
		}

		logger.Info("sleeping at the end of the batch for %s to allow computer time to perform other tasks", importer.opts.BatchSleepDuration)
		time.Sleep(importer.opts.BatchSleepDuration)
		if reachedEnd {
			logger.Info("reached the end of scanning!")
			break
		}
	}

	dataSourceConn, err := finalStorage.Commit()
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	successful = true

	return dataSourceConn, nil

}

// (finished, error)
func (importer *Importer2) ScanBatch() (bool, errorsx.Error) {
	var err error
	err = importer.auxillaryPbfReader.Reset()
	if err != nil {
		return false, errorsx.Wrap(err)
	}

	var batchType osm.Type
	var nodeBatch NodeBatch
	wayBatch := WayBatch{
		requiredNodesMap: make(map[int64]*ownmap.OSMNode),
	}
	relationBatch := RelationsBatch{
		requiredNodesMap: make(map[int64]*ownmap.OSMNode),
		requiredWaysMap:  make(map[int64]*ownmap.OSMWay),
		allRelationsMap:  make(map[int64]*ownmap.OSMRelation),
	}

	var reachedEnd bool

	for i := 0; i < importer.opts.BatchSize; i++ {
		next := importer.pbfReader.Scan()
		if !next {
			reachedEnd = true
			break
		}

		object := importer.pbfReader.Object()
		objectType := object.ObjectID().Type()
		if i == 0 {
			batchType = objectType
		} else if objectType != batchType {
			// change in type. save the batch and move to a new batch
			break
		}

		switch obj := object.(type) {
		case *osm.Node:
			nodeBatch.objects = append(nodeBatch.objects, ownmap.NewMapmakerNodeFromOSMRelation(obj))
		case *osm.Way:
			var wayPoints []*ownmap.WayPoint
			for _, node := range obj.Nodes {
				wayBatch.requiredNodesMap[int64(node.ID)] = nil
				wayPoints = append(wayPoints, &ownmap.WayPoint{
					NodeID: int64(node.ID),
				})
			}
			wayBatch.objects = append(wayBatch.objects, ownmap.NewMapmakerWayFromOSMRelation(obj, wayPoints))
		case *osm.Relation:
			relation, err := ownmap.NewMapmakerRelationFromOSMRelation(obj)
			if err != nil {
				return false, errorsx.Wrap(err)
			}

			// for now, add all relations to the final storage
			relationBatch.allRelationsMap[relation.ID] = relation
			relationBatch.objects = append(relationBatch.objects, relation)

			for _, member := range obj.Members {
				switch member.Type {
				case osm.TypeNode:
					relationBatch.requiredNodesMap[member.Ref] = nil
				case osm.TypeWay:
					relationBatch.requiredWaysMap[member.Ref] = nil
				case osm.TypeRelation:
					// do nothing, all relations are added anyway
				default:
					return false, errorsx.Errorf("not implemented member type: %v. Relation ID: %v", member.Type, obj.ID)
				}
			}
		default:
			return false, errorsx.Errorf("unknown object type: %v. ID: %v", obj.ObjectID().Type(), obj.ObjectID().Ref())
		}
	}

	err = importer.pbfReader.Err()
	if err != nil {
		return false, errorsx.Wrap(err)
	}

	if batchType == "" {
		// no objects in this batch. Skip importing it.
		return reachedEnd, nil
	}

	// all objects for this batch have been scanned. Now scan any dependencies if necessary, and if not, import
	switch batchType {
	case osm.TypeNode:
		err := importer.finalStorage.ImportNodes(nodeBatch.objects)
		if err != nil {
			return false, err
		}
	case osm.TypeWay:
		err := scanWaypoints(importer.auxillaryPbfReader, wayBatch)
		if err != nil {
			return false, err
		}

		err = importer.finalStorage.ImportWays(wayBatch.objects)
		if err != nil {
			return false, err
		}
	case osm.TypeRelation:
		// err := scanRelations(importer.auxillaryPbfReader, relationBatch)
		// if err != nil {
		// 	return err
		// }

		err := importer.finalStorage.ImportRelations(relationBatch.objects)
		if err != nil {
			return false, err
		}
	default:
		return false, errorsx.Errorf("unknown batch type: %v", batchType)
	}

	return reachedEnd, nil
}

func scanWaypoints(auxillaryPbfReader PBFReader, wayBatch WayBatch) errorsx.Error {
	var itemsScanned int
	totalToScan := len(wayBatch.requiredNodesMap)
	var nodesScannedCount, nodesInRequiredCount int

	for auxillaryPbfReader.Scan() {
		object := auxillaryPbfReader.Object()

		if object.ObjectID().Type() != osm.TypeNode {
			// only nodes are of interest to us
			continue
		}

		nodesScannedCount++

		_, ok := wayBatch.requiredNodesMap[object.ObjectID().Ref()]
		if !ok {
			// not needed
			continue
		}

		nodesInRequiredCount++

		obj := object.(*osm.Node)
		wayBatch.requiredNodesMap[int64(obj.ID)] = ownmap.NewMapmakerNodeFromOSMRelation(obj)
		itemsScanned++
	}

	err := auxillaryPbfReader.Err()
	if err != nil {
		return errorsx.Wrap(err)
	}

	if itemsScanned != totalToScan {
		return errorsx.Errorf("items scanned (%d) not equal to total to scan (%d). Nodes scanned count:%d, nodesInRequiredCount:%d", itemsScanned, totalToScan, nodesScannedCount, nodesInRequiredCount)
	}

	// now go through each way, set required nodes
	for _, way := range wayBatch.objects {
		for _, wayPoint := range way.WayPoints {
			extracted, ok := wayBatch.requiredNodesMap[wayPoint.NodeID]
			if !ok {
				return errorsx.Errorf("node not found in requiredNodesMap. Node ID: %d")
			}

			wayPoint.Point = &ownmap.Location{
				Lat: extracted.Lat,
				Lon: extracted.Lon,
			}
		}
	}

	return nil
}

func addMemberRelationsToBatch(relation *ownmap.OSMRelation, relationBatch RelationsBatch) {
	for _, member := range relation.Members {
		if member.MemberType != ownmap.OSM_MEMBER_TYPE_RELATION {
			continue
		}

		memberRelation := relationBatch.allRelationsMap[member.ObjectID]
		addMemberRelationsToBatch(memberRelation, relationBatch)
	}
}

func scanRelations(auxillaryPbfReader PBFReader, relationBatch RelationsBatch) errorsx.Error {
	var itemsScanned int
	totalToScan := len(relationBatch.requiredNodesMap)

	// step 1: for each required relation, add all the member relations to requiredRelationsMap
	for _, relation := range relationBatch.objects {
		addMemberRelationsToBatch(relation, relationBatch)
	}

	// step 2:

	for auxillaryPbfReader.Scan() {
		object := auxillaryPbfReader.Object()

		if object.ObjectID().Type() != osm.TypeNode {
			// only nodes are of interest to us
			continue
		}

		_, ok := relationBatch.requiredNodesMap[object.ObjectID().Ref()]
		if !ok {
			// not needed
			continue
		}

		obj := object.(*osm.Node)
		relationBatch.requiredNodesMap[int64(obj.ID)] = ownmap.NewMapmakerNodeFromOSMRelation(obj)
	}

	err := auxillaryPbfReader.Err()
	if err != nil {
		return errorsx.Wrap(err)
	}

	if itemsScanned != totalToScan {
		return errorsx.Errorf("items scanned (%d) not equal to total to scan (%d)", itemsScanned, totalToScan)
	}

	// now go through each way, set required nodes
	// for _, way := range relationBatch.objects {
	// for _, wayPoint := range way.WayPoints {
	// 	extracted, ok := wayBatch.requiredNodesMap[wayPoint.NodeID]
	// 	if !ok {
	// 		return errorsx.Errorf("node not found in requiredNodesMap. Node ID: %d")
	// 	}

	// 	wayPoint.Point = &ownmap.Location{
	// 		Lat: extracted.Lat,
	// 		Lon: extracted.Lon,
	// 	}
	// }
	// }

	return nil
}
