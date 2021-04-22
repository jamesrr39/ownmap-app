package ownmapsqldb

import (
	"fmt"
	"strings"
	"time"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/ownmap-app/ownmap"
	"github.com/jamesrr39/ownmap-app/ownmapdal"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/paulmach/osm"
)

var _ ownmapdal.DataSourceConn = &MapmakerSQLDB{}

type MapmakerSQLDB struct {
	name string
	db   *sqlx.DB
}

func NewMapmakerSQLDB(db *sqlx.DB, name string) *MapmakerSQLDB {
	return &MapmakerSQLDB{
		name: name,
		db:   db,
	}
}

func (db *MapmakerSQLDB) Name() string {
	return db.name
}

func (db *MapmakerSQLDB) DatasetInfo() (*ownmap.DatasetInfo, errorsx.Error) {
	tx, err := db.db.Begin()
	if err != nil {
		return nil, errorsx.Wrap(err)
	}
	defer tx.Rollback()

	row := tx.QueryRow(`
		SELECT
			bounds_min_lat, 
			bounds_max_lat, 
			bounds_min_lon, 
			bounds_max_lon, 
			replication_time 
		FROM dataset_info`)
	if row.Err() != nil {
		return nil, errorsx.Wrap(row.Err())
	}

	var replicationTime time.Time
	datasetInfo := new(ownmap.DatasetInfo)
	datasetInfo.Bounds = new(ownmap.DatasetInfo_Bounds)
	err = row.Scan(
		&datasetInfo.Bounds.MinLat,
		&datasetInfo.Bounds.MaxLat,
		&datasetInfo.Bounds.MinLon,
		&datasetInfo.Bounds.MaxLon,
		&replicationTime)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	datasetInfo.ReplicationTimeMs = uint64(replicationTime.UnixNano() / (1000 * 1000))

	return datasetInfo, nil
}

func (db *MapmakerSQLDB) GetInBounds(bounds osm.Bounds, filter *ownmapdal.GetInBoundsFilter) (ownmapdal.TagNodeMap, ownmapdal.TagWayMap, ownmapdal.TagRelationMap, errorsx.Error) {
	var whereClauseLines []string
	var args []interface{}

	for i, object := range filter.Objects {
		if object.ObjectType == ownmap.ObjectTypeRelation {
			panic("relation getting not supported yet")
		}

		whereClauseLines = append(whereClauseLines, fmt.Sprintf(`(t.object_type_id = $%d AND t.key = $%d)`, (i*2)+1, (i*2)+2))
		args = append(args, object.ObjectType, object.TagKey)
	}

	tx, err := db.db.Beginx()
	if err != nil {
		return nil, nil, nil, errorsx.Wrap(err)
	}
	defer tx.Rollback()

	query := fmt.Sprintf(`
	SELECT object_id, object_type_id, key, value
	FROM tags t
	WHERE
		%s
	`, strings.Join(whereClauseLines, " OR\n"))

	var tagRows []*tagRowType
	err = tx.Select(&tagRows, query, args...)
	if err != nil {
		return nil, nil, nil, errorsx.Wrap(err)
	}

	return db.getObjectsFromTagRows(tx, tagRows)
}

func (db *MapmakerSQLDB) getWayNodesWanted(tx *sqlx.Tx, wayIDs []int64) (map[int64][]*ownmap.WayPoint, errorsx.Error) {
	type nodeLocationWithWay struct {
		WayID int64 `db:"way_id"`
		*ownmap.WayPoint
	}

	var wayNodeLocations []*nodeLocationWithWay

	err := tx.Select(&wayNodeLocations, `
		SELECT wn.way_id as way_id, n.id as node_id, n.lat, n.lon
		FROM nodes n
		INNER JOIN way_nodes wn
		ON n.id = wn.node_id
		WHERE wn.way_id = ANY($1)
	`, pq.Array(wayIDs))
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	wayIDsWantedMap := make(map[int64][]*ownmap.WayPoint)
	for _, wayNodeLocation := range wayNodeLocations {
		wayIDsWantedMap[wayNodeLocation.WayID] = append(wayIDsWantedMap[wayNodeLocation.WayID], wayNodeLocation.WayPoint)
	}

	return wayIDsWantedMap, nil
}

func (db *MapmakerSQLDB) getAllTagsForObjectIDs(tx *sqlx.Tx, wayIDsWanted []int64) (map[int64][]*ownmap.OSMTag, errorsx.Error) {
	var x []tagRowType
	err := tx.Select(&x, `
		SELECT t.object_id, t.key, t.value
		FROM tags t
		WHERE t.object_type_id = $1
		AND t.object_id = ANY ($2)
	`, ownmap.ObjectTypeWay, pq.Array(wayIDsWanted))
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	tagMap := make(map[int64][]*ownmap.OSMTag)
	for _, tagRow := range x {
		tagMap[tagRow.ObjectID] = append(tagMap[tagRow.ObjectID], &ownmap.OSMTag{
			Key:   string(tagRow.Key),
			Value: tagRow.Value,
		})
	}

	return tagMap, nil
}

func (db *MapmakerSQLDB) getObjectsFromTagRows(tx *sqlx.Tx, tagRows []*tagRowType) (ownmapdal.TagNodeMap, ownmapdal.TagWayMap, ownmapdal.TagRelationMap, errorsx.Error) {

	// first, get ways wanted
	// map[nodeID]*WayNodeLocation

	var nodeIDsWanted []int64
	var wayIDsWanted []int64
	for _, tagRow := range tagRows {
		switch tagRow.ObjectTypeID {
		case ownmap.ObjectTypeWay:
			wayIDsWanted = append(wayIDsWanted, tagRow.ObjectID)
		case ownmap.ObjectTypeNode:
			nodeIDsWanted = append(nodeIDsWanted, tagRow.ObjectID)
		}
	}

	tagWayMap, err := db.getWaysFromWayIDList(tx, wayIDsWanted)
	if err != nil {
		return nil, nil, nil, errorsx.Wrap(err)
	}

	tagNodeMap, err := db.getNodesFromNodeIDList(tx, nodeIDsWanted)
	if err != nil {
		return nil, nil, nil, errorsx.Wrap(err)
	}

	return tagNodeMap, tagWayMap, make(ownmapdal.TagRelationMap), nil
}

type tagNodeRecordType struct {
	NodeID     int64 `db:"node_id"`
	Lat, Lon   float64
	Key, Value string
}

func (db *MapmakerSQLDB) getNodesFromNodeIDList(tx *sqlx.Tx, nodeIDsWanted []int64) (ownmapdal.TagNodeMap, errorsx.Error) {

	var tagNodeRecords []*tagNodeRecordType
	err := tx.Select(&tagNodeRecords, `
		SELECT n.id as node_id, n.lat, n.lon, t.key, t.value
		FROM nodes n
		LEFT JOIN tags t
		ON n.id = t.object_id
		WHERE t.object_type_id = $1
		AND t.object_id = ANY ($2)
	`, ownmap.ObjectTypeNode, pq.Array(nodeIDsWanted))
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	wantedNodesMap := make(map[int64]*ownmap.OSMNode)
	for _, tagNodeRecord := range tagNodeRecords {
		existingNode, ok := wantedNodesMap[tagNodeRecord.NodeID]
		if !ok {
			// if this node is not yet known to us, create it and add it into the map
			existingNode = &ownmap.OSMNode{
				ID:  tagNodeRecord.NodeID,
				Lat: tagNodeRecord.Lat,
				Lon: tagNodeRecord.Lon,
			}
			wantedNodesMap[tagNodeRecord.NodeID] = existingNode
		}

		if tagNodeRecord.Key != "" {
			existingNode.Tags = append(existingNode.Tags, &ownmap.OSMTag{
				Key:   tagNodeRecord.Key,
				Value: tagNodeRecord.Value,
			})
		}
	}

	tagNodeMap := make(ownmapdal.TagNodeMap)
	for _, node := range wantedNodesMap {
		for _, tag := range node.Tags {
			tagNodeMap[ownmap.TagKey(tag.Key)] = append(tagNodeMap[ownmap.TagKey(tag.Key)], node)
		}
	}

	return tagNodeMap, nil
}

func (db *MapmakerSQLDB) getWaysFromWayIDList(tx *sqlx.Tx, wayIDsWanted []int64) (ownmapdal.TagWayMap, errorsx.Error) {
	if len(wayIDsWanted) == 0 {
		return make(ownmapdal.TagWayMap), nil
	}

	wayNodesWanted, err := db.getWayNodesWanted(tx, wayIDsWanted)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	tagMap, err := db.getAllTagsForObjectIDs(tx, wayIDsWanted)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	wayTagMap := make(ownmapdal.TagWayMap)
	for _, wayID := range wayIDsWanted {
		tags := tagMap[wayID]
		waypoints := wayNodesWanted[wayID]

		way := &ownmap.OSMWay{
			ID:        wayID,
			Tags:      tags,
			WayPoints: waypoints,
		}

		for _, tag := range tags {
			wayTagMap[ownmap.TagKey(tag.Key)] = append(wayTagMap[ownmap.TagKey(tag.Key)], way)
		}
	}

	return wayTagMap, nil

}

type tagRowType struct {
	ObjectID     int64             `db:"object_id"`
	ObjectTypeID ownmap.ObjectType `db:"object_type_id"`
	Key          ownmap.TagKey
	Value        string
}
