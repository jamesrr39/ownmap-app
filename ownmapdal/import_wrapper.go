package ownmapdal

import (
	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/goutil/gofs"
	"github.com/jamesrr39/goutil/logpkg"
	"github.com/jamesrr39/ownmap-app/ownmap"
	"github.com/paulmach/osm"
)

type Importer interface {
	GetNodeByID(id int64) (*ownmap.OSMNode, error)
	GetWayByID(id int64) (*ownmap.OSMWay, error)
	GetRelationByID(id int64) (*ownmap.OSMRelation, error)
	ImportNode(obj *ownmap.OSMNode) errorsx.Error
	ImportWay(obj *ownmap.OSMWay) errorsx.Error
	ImportRelation(obj *ownmap.OSMRelation) errorsx.Error
	Commit() (DataSourceConn, errorsx.Error)
	Rollback() errorsx.Error
}

type scanObjectFunc func(obj osm.Object) errorsx.Error

func createScanFirstPassFunc(
	importRun *ImportRunType,
	importer Importer,
) scanObjectFunc {
	return func(obj osm.Object) errorsx.Error {
		var err errorsx.Error

		switch obj := obj.(type) {
		case *osm.Node:
			if !importRun.Bounds.ContainsNode(obj) {
				return nil
			}

			node := &ownmap.OSMNode{
				ID:   int64(obj.ID),
				Lat:  obj.Lat,
				Lon:  obj.Lon,
				Tags: ownmap.NewMapmakerTagsFromOSMTags(obj.Tags),
			}

			err = importer.ImportNode(node)
			if err != nil {
				return err
			}
		}

		return nil
	}
}

func createRescanRunFunc(
	logger *logpkg.Logger,
	importRun *ImportRunType,
	importer Importer,
) scanObjectFunc {
	return func(obj osm.Object) errorsx.Error {
		switch obj := obj.(type) {
		case *osm.Node:
			// not interested in nodes on rescan
		case *osm.Way:
			logger.Debug("scanning way ID: %d", obj.ID)
			var err error
			wayID := int64(obj.ID)

			var wayPoints []*ownmap.WayPoint
			var points []*ownmap.Location
			for _, n := range obj.Nodes {
				node, err := importer.GetNodeByID(int64(n.ID))
				if err != nil {
					if errorsx.Cause(err) != errorsx.ObjectNotFound {
						return errorsx.Wrap(err)
					}

					// node not in import bounds, skip this node
					continue
				}

				point := &ownmap.Location{
					Lat: node.Lat,
					Lon: node.Lon,
				}

				points = append(points, point)
				wayPoints = append(wayPoints, &ownmap.WayPoint{
					NodeID: node.ID,
					Point:  point,
				})
			}

			if !atLeastOneNodeInBounds(points, importRun.Bounds) {
				// way not in bounds. See if we should 'remember it' in the import run, if it is requested by relations scanner
				if importRun.Rescan.WayRescanMap.IsItemRequestedForRescan(wayID) {
					importRun.Rescan.WayRescanMap.MarkIDAsScannedButNotInBounds(wayID)
				}

				return nil
			}

			way := &ownmap.OSMWay{
				ID:        wayID,
				Tags:      ownmap.NewMapmakerTagsFromOSMTags(obj.Tags),
				WayPoints: wayPoints,
			}

			// see if this object is already imported
			_, err = importer.GetWayByID(wayID)
			if err != nil && errorsx.Cause(err) != errorsx.ObjectNotFound {
				// unexpected error
				return errorsx.Wrap(err)
			}
			if err == nil {
				// object already imported
				return nil
			}

			err = importer.ImportWay(way)
			if err != nil {
				return errorsx.Wrap(err)
			}
		case *osm.Relation:
			logger.Debug("scanning relation ID: %d", obj.ID)
			var err error

			relation, err := ownmap.NewMapmakerRelationFromOSMRelation(obj)
			if err != nil {
				return errorsx.Wrap(err)
			}

			if importRun.Rescan.RelationRescanMap.IsItemRequestedForRescan(relation.ID) {
				importRun.Rescan.RelationRescanMap.MarkIDAsScannedButNotInBounds(relation.ID)
			}

			isAtLeastOneMemberInBounds := false
			haveAllSubElementsBeenScanned := true
			for _, member := range relation.Members {
				switch member.MemberType {
				case ownmap.OSM_MEMBER_TYPE_NODE:
					// get node from import run. If not available, skip, since it has already been deemed out of bounds
					_, err = importer.GetNodeByID(member.ObjectID)
					if err != nil {
						if errorsx.Cause(err) != errorsx.ObjectNotFound {
							return errorsx.Wrap(err)
						}

						continue
					}

					// since the node was found in the importer, it means at least some of the relation is in bounds
					isAtLeastOneMemberInBounds = true
				case ownmap.OSM_MEMBER_TYPE_WAY:
					_, err = importer.GetWayByID(member.ObjectID)
					if err != nil {
						if errorsx.Cause(err) != errorsx.ObjectNotFound {
							return errorsx.Wrap(err)
						}

						rescanResult := importRun.Rescan.WayRescanMap.GetValueOfWayMarkedForRescan(member.ObjectID)
						if rescanResult.KnownToBeOutOfBounds {
							// if it was found in the map, but not imported, it means it was out of bounds
							continue
						}
						// debug
						logger.Debug("way marking for rescan. Relation ID: %d, Way ID: %d", obj.ID, member.ObjectID)

						// we're interested in this way. So mark it as required and don't import this relation
						importRun.Rescan.WayRescanMap.RequestIDToBeRescanned(member.ObjectID)
						haveAllSubElementsBeenScanned = false
						continue
					}

					// the way has been imported, so it's in bounds
					isAtLeastOneMemberInBounds = true
				case ownmap.OSM_MEMBER_TYPE_RELATION:
					_, err = importer.GetRelationByID(member.ObjectID)
					if err != nil {
						if errorsx.Cause(err) != errorsx.ObjectNotFound {
							return errorsx.Wrap(err)
						}

						rescanResult := importRun.Rescan.RelationRescanMap.GetValueOfWayMarkedForRescan(member.ObjectID)
						if rescanResult.KnownToBeOutOfBounds {
							// if it was found in the map, but not imported, it means it was out of bounds
							continue
						}

						// debug
						logger.Debug("subrelation marking for rescan. Relation ID: %d, SubRelation ID: %d", obj.ID, member.ObjectID)

						// we're interested in this subrelation. So mark it as required and don't import this relation
						importRun.Rescan.RelationRescanMap.RequestIDToBeRescanned(member.ObjectID)
						haveAllSubElementsBeenScanned = false

						continue
					}

					isAtLeastOneMemberInBounds = true
				}
			}

			if !haveAllSubElementsBeenScanned {
				logger.Debug("waiting for all subelements to be rescanned. Relation ID: %d", obj.ID)
				// wait to rescan this
				return nil
			}

			if !isAtLeastOneMemberInBounds {
				logger.Debug("declaring out of bounds. Relation ID: %d", obj.ID)
				// out of bounds
				return nil
			}

			// see if this object is already imported
			_, err = importer.GetRelationByID(relation.ID)
			if err != nil && errorsx.Cause(err) != errorsx.ObjectNotFound {
				// unexpected error
				return errorsx.Wrap(err)
			}
			if err == nil {
				// object already imported
				return nil
			}

			logger.Debug("importing relation. ID: %d", obj.ID)
			err = importer.ImportRelation(relation)
			if err != nil {
				return errorsx.Wrap(err)
			}
		default:
			// not interesting to us. do nothing
		}
		return nil
	}
}

func Import(
	logger *logpkg.Logger,
	pbfReader PBFReader,
	fs gofs.Fs,
	importer Importer,
	bounds osm.Bounds,
) (DataSourceConn, errorsx.Error) {
	var successful bool
	defer func() {
		if successful {
			return
		}

		err := importer.Rollback()
		if err != nil {
			logger.Error("error rolling back import: %q\nStack:\n%s\n", err.Error(), err.Stack())
			return
		}
	}()

	importRun := &ImportRunType{
		Rescan:           NewRescan(),
		MaxItemsPerBatch: 10 * 1000,
		Bounds:           bounds,
	}

	err := importRun.Validate()
	if err != nil {
		return nil, errorsx.Wrap(err)
	}
	scanFirstPass := createScanFirstPassFunc(importRun, importer)
	rescanImportRunFunc := createRescanRunFunc(logger, importRun, importer)

	batchNumber := 0
	for {
		batchNumber++

		logger.Info(
			"running import batch %d. %.02f%% read.",
			batchNumber,
			float64(pbfReader.FullyScannedBytes())*100/float64(pbfReader.TotalSize()),
		)

		shouldContinue, err := scanBatch(
			pbfReader,
			importRun,
			scanFirstPass,
		)
		if err != nil {
			return nil, errorsx.Wrap(err)
		}

		if !shouldContinue {
			break
		}
	}

	logger.Info("First scan finished. Now re-scanning unscanned relations.")

	const MaxRescans = 10000
	for i := 0; i < MaxRescans; i++ {
		logger.Info("running rescan %d", i)
		err := pbfReader.Reset()
		if err != nil {
			return nil, errorsx.Wrap(err)
		}

		importRun.Rescan.MarkNewIteration()

		batchNumber := 0
		for {
			batchNumber++

			logger.Info(
				"running import batch %d of rescan #%d. %.02f%% read.",
				batchNumber,
				i+1,
				float64(pbfReader.FullyScannedBytes())*100/float64(pbfReader.TotalSize()),
			)

			shouldContinue, err := scanBatch(pbfReader, importRun, rescanImportRunFunc)
			if err != nil {
				return nil, errorsx.Wrap(err)
			}

			if !shouldContinue {
				break
			}
		}

		rescanObjectsCount := importRun.Rescan.GetRescanItemRequestsThisIteration()
		logger.Info("End of iteration. Objects requested for rescan: %d", rescanObjectsCount)
		if rescanObjectsCount == 0 {
			// if there have been no requests for any item rescans, we are all done here
			break
		}
	}

	dataSourceConn, err := importer.Commit()
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	successful = true

	return dataSourceConn, nil
}
