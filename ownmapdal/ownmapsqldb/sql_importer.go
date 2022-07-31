package ownmapsqldb

import (
	"fmt"
	"log"
	"strings"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/ownmap-app/ownmap"
	"github.com/jamesrr39/ownmap-app/ownmapdal"
	"github.com/jmoiron/sqlx"
	"github.com/paulmach/osm/osmpbf"
)

type toDatasourceConnFunc func() (ownmapdal.DataSourceConn, errorsx.Error)

type Importer struct {
	tx               *sqlx.Tx
	pbfHeader        *osmpbf.Header
	toDatasourceConn toDatasourceConnFunc
}

func NewImporter(db *sqlx.DB, pbfHeader *osmpbf.Header, toDatasourceConn toDatasourceConnFunc) (*Importer, errorsx.Error) {
	tx, err := db.Beginx()
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	return &Importer{
		tx, pbfHeader, toDatasourceConn,
	}, nil
}

const maxBatchSize = 20 * 1000 // to avoid: pq: got 99999 parameters but PostgreSQL only supports 65535 parameters

func (importer *Importer) ImportNodes(objs []*ownmap.OSMNode) errorsx.Error {
	var totalImportCount int64
	for {
		var nodeInsertSQLValueRows, tagsInsertSQLValueRows []string
		var nodeValues, tagValues []any
		var lastNodeArgsCounter, lastTagArgsCounter int
		var nodesInBatch, tagsInBatch int

		for i := 0; i < maxBatchSize; i++ {
			if totalImportCount >= int64(len(objs)) {
				// reached the last object. Break out of this loop, import the last ones
				break
			}
			obj := objs[totalImportCount]

			nodeInsertSQLValueRows = append(
				nodeInsertSQLValueRows,
				fmt.Sprintf("($%d, $%d, $%d)", lastNodeArgsCounter+1, lastNodeArgsCounter+2, lastNodeArgsCounter+3),
			)
			nodeValues = append(nodeValues, obj.ID, obj.Lat, obj.Lon)

			nodesInBatch++
			lastNodeArgsCounter += 3

			for _, tag := range obj.Tags {
				tagsInsertSQLValueRows = append(
					tagsInsertSQLValueRows,
					fmt.Sprintf(`($%d, $%d, $%d, $%d)`, lastTagArgsCounter+1, lastTagArgsCounter+2, lastTagArgsCounter+3, lastTagArgsCounter+4),
				)
				tagValues = append(tagValues, obj.ID, ownmap.ObjectTypeNode, tag.Key, tag.Value)

				tagsInBatch++
				lastTagArgsCounter += 4
			}

			totalImportCount++

			// check if there are too many nodes/ways/relations/tags/waypoints/relation members
			if len(nodeValues) >= maxBatchSize || len(tagValues) >= maxBatchSize {
				break
			}
		}

		if len(nodeValues) == 0 {
			// all done
			break
		}

		log.Printf("importing %d nodes and %d tags\n", nodesInBatch, tagsInBatch)

		// insert nodes
		insertSQL := fmt.Sprintf(`INSERT INTO nodes (id, lat, lon) VALUES %s`, strings.Join(nodeInsertSQLValueRows, ", "))
		_, err := importer.tx.Exec(insertSQL, nodeValues...)
		if err != nil {
			return errorsx.Wrap(err)
		}

		if len(tagValues) == 0 {
			// nothing to insert
			return nil
		}

		// insert tags
		tagsInsertSQL := fmt.Sprintf(`INSERT INTO tags (object_id, object_type_id, key, value) VALUES %s`, strings.Join(tagsInsertSQLValueRows, ", "))
		_, err = importer.tx.Exec(tagsInsertSQL, tagValues...)
		if err != nil {
			return errorsx.Wrap(err)
		}
	}

	return nil
}
func (importer *Importer) ImportWays(objs []*ownmap.OSMWay) errorsx.Error {
	var totalImportCount int64
	for {
		var wayInsertSQLValueRows, tagsInsertSQLValueRows, waypointInsertSQLValueRows []string
		var wayValues, tagValues, waypointValues []any
		var lastNodeArgsCounter, lastTagArgsCounter, lastWaypointArgsCount int
		var waysInBatch, tagsInBatch, waypointsInBatch int

		for i := 0; i < maxBatchSize; i++ {
			if totalImportCount >= int64(len(objs)) {
				// reached the last object. Break out of this loop, import the last ones
				break
			}

			obj := objs[totalImportCount]

			wayInsertSQLValueRows = append(
				wayInsertSQLValueRows,
				fmt.Sprintf("($%d)", lastNodeArgsCounter+1),
			)
			wayValues = append(wayValues, obj.ID)

			waysInBatch++
			lastNodeArgsCounter += 1

			if len(obj.Tags) > 1000 {
				log.Printf("over 1000 tags. Way ID: %d\n", obj.ID)
			}

			for _, tag := range obj.Tags {
				tagsInsertSQLValueRows = append(
					tagsInsertSQLValueRows,
					fmt.Sprintf(`($%d, $%d, $%d, $%d)`, lastTagArgsCounter+1, lastTagArgsCounter+2, lastTagArgsCounter+3, lastTagArgsCounter+4),
				)
				tagValues = append(tagValues, obj.ID, ownmap.ObjectTypeWay, tag.Key, tag.Value)

				tagsInBatch++
				lastTagArgsCounter += 4
			}

			for _, waypoint := range obj.WayPoints {
				waypointInsertSQLValueRows = append(
					waypointInsertSQLValueRows,
					fmt.Sprintf(`($%d, $%d)`, lastWaypointArgsCount+1, lastWaypointArgsCount+2),
				)
				waypointValues = append(waypointValues, waypoint.NodeID, obj.ID)

				waypointsInBatch++
				lastWaypointArgsCount += 2
			}

			totalImportCount++

			// check if there are too many nodes/ways/relations/tags/waypoints/relation members
			if len(wayValues) >= maxBatchSize || len(tagValues) >= maxBatchSize || len(waypointValues) >= maxBatchSize {
				break
			}
		}

		if len(wayValues) == 0 {
			// all done
			break
		}

		log.Printf("importing %d ways, %d tags and %d waypoints\n", waysInBatch, tagsInBatch, waypointsInBatch)

		waysInsertSQL := fmt.Sprintf(`INSERT INTO ways (id) VALUES %s`, strings.Join(wayInsertSQLValueRows, ", "))
		_, err := importer.tx.Exec(waysInsertSQL, wayValues...)
		if err != nil {
			return errorsx.Wrap(err)
		}

		// insert tags
		tagsInsertSQL := fmt.Sprintf(`INSERT INTO tags (object_id, object_type_id, key, value) VALUES %s`, strings.Join(tagsInsertSQLValueRows, ", "))
		_, err = importer.tx.Exec(tagsInsertSQL, tagValues...)
		if err != nil {
			return errorsx.Wrap(err)
		}

		waypointsInsertSQL := fmt.Sprintf(`INSERT INTO way_nodes (way_id, node_id) VALUES %s`, strings.Join(waypointInsertSQLValueRows, ", "))
		_, err = importer.tx.Exec(waypointsInsertSQL, waypointValues...)
		if err != nil {
			return errorsx.Wrap(err)
		}
	}

	return nil
}

func (importer *Importer) ImportRelations(objs []*ownmap.OSMRelation) errorsx.Error {
	var totalImportCount int64
	for {
		var relationsInsertSQLValueRows, tagsInsertSQLValueRows, memberInsertSQLValueRows []string
		var relationsValues, tagValues, memberValues []any
		var lastRelationArgsCounter, lastTagArgsCounter, lastMemberArgsCount int
		var relationsInBatch, tagsInBatch, membersInBatch int

		for i := 0; i < maxBatchSize; i++ {
			if totalImportCount >= int64(len(objs)) {
				// reached the last object. Break out of this loop, import the last ones
				break
			}

			obj := objs[totalImportCount]

			relationsInsertSQLValueRows = append(
				relationsInsertSQLValueRows,
				fmt.Sprintf("($%d)", lastRelationArgsCounter+1),
			)
			relationsValues = append(relationsValues, obj.ID)

			relationsInBatch++
			lastRelationArgsCounter += 1

			for _, tag := range obj.Tags {
				tagsInsertSQLValueRows = append(
					tagsInsertSQLValueRows,
					fmt.Sprintf(`($%d, $%d, $%d, $%d)`, lastTagArgsCounter+1, lastTagArgsCounter+2, lastTagArgsCounter+3, lastTagArgsCounter+4),
				)
				tagValues = append(tagValues, obj.ID, ownmap.ObjectTypeRelation, tag.Key, tag.Value)

				tagsInBatch++
				lastTagArgsCounter += 4
			}

			for _, member := range obj.Members {
				memberInsertSQLValueRows = append(
					memberInsertSQLValueRows,
					fmt.Sprintf(
						`($%d, $%d, $%d, $%d, $%d)`,
						lastMemberArgsCount+1, lastMemberArgsCount+2, lastMemberArgsCount+3, lastMemberArgsCount+4, lastMemberArgsCount+5,
					),
				)
				memberValues = append(memberValues, member.ObjectID, member.MemberType, member.Role, member.Orientation, obj.ID)

				membersInBatch++
				lastMemberArgsCount += 5
			}

			totalImportCount++

			// check if there are too many nodes/ways/relations/tags/waypoints/relation members
			if len(relationsValues) >= maxBatchSize || len(tagValues) >= maxBatchSize || len(memberValues) >= maxBatchSize {
				break
			}
		}

		if len(relationsValues) == 0 {
			// all done
			break
		}

		log.Printf("importing %d relations, %d tags and %d relation members\n", relationsInBatch, tagsInBatch, membersInBatch)

		relationsInsertSQL := fmt.Sprintf(`INSERT INTO relations (id) VALUES %s`, strings.Join(relationsInsertSQLValueRows, ", "))
		_, err := importer.tx.Exec(relationsInsertSQL, relationsValues...)
		if err != nil {
			return errorsx.Wrap(err)
		}

		// insert tags
		tagsInsertSQL := fmt.Sprintf(`INSERT INTO tags (object_id, object_type_id, key, value) VALUES %s`, strings.Join(tagsInsertSQLValueRows, ", "))
		_, err = importer.tx.Exec(tagsInsertSQL, tagValues...)
		if err != nil {
			return errorsx.Wrap(err)
		}

		// insert relation members
		membersInsertSQL := fmt.Sprintf(`INSERT INTO relation_members (
			member_id,
			member_type,
			role,
			orientation,
			parent_id
		) VALUES %s`, strings.Join(memberInsertSQLValueRows, ", "))
		_, err = importer.tx.Exec(membersInsertSQL, memberValues...)
		if err != nil {
			return errorsx.Wrap(err)
		}
	}

	return nil
}

func (importer *Importer) Rollback() errorsx.Error {
	err := importer.tx.Rollback()
	if err != nil {
		return errorsx.Wrap(err)
	}

	return nil
}

func (importer *Importer) Commit() (ownmapdal.DataSourceConn, errorsx.Error) {
	_, err := importer.tx.Exec(`
	INSERT INTO dataset_info (
		bounds_min_lat,
		bounds_max_lat,
		bounds_min_lon,
		bounds_max_lon,
		replication_time
	) VALUES ($1, $2, $3, $4, $5)`,
		importer.pbfHeader.Bounds.MinLat,
		importer.pbfHeader.Bounds.MaxLat,
		importer.pbfHeader.Bounds.MinLon,
		importer.pbfHeader.Bounds.MaxLon,
		importer.pbfHeader.ReplicationTimestamp,
	)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	err = importer.tx.Commit()
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	return importer.toDatasourceConn()
}
