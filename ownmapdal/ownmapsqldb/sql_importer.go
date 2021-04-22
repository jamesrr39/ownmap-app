package ownmapsqldb

import (
	"database/sql"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/ownmap-app/ownmap"
	"github.com/jamesrr39/ownmap-app/ownmapdal"
	"github.com/jmoiron/sqlx"
	"github.com/paulmach/osm/osmpbf"
)

var _ ownmapdal.Importer = &Importer{}

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

func (importer *Importer) GetNodeByID(id int64) (*ownmap.OSMNode, error) {
	row := importer.tx.QueryRow("SELECT id, lat, lon FROM nodes WHERE id = $1", id)
	if row.Err() != nil {
		return nil, errorsx.Wrap(row.Err())
	}

	node := new(ownmap.OSMNode)
	err := row.Scan(&node.ID, &node.Lat, &node.Lon)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errorsx.ObjectNotFound
		}

		return nil, errorsx.Wrap(err)
	}

	rows, err := importer.tx.Query("SELECT key, value FROM tags t WHERE t.object_id = $1 AND t.object_type_id = $2", id, ownmap.ObjectTypeNode)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}
	defer rows.Close()

	for rows.Next() {
		tag := new(ownmap.OSMTag)
		err = rows.Scan(&tag.Key, &tag.Value)
		if err != nil {
			return nil, errorsx.Wrap(err)
		}

		node.Tags = append(node.Tags, tag)
	}

	if rows.Err() != nil {
		return nil, errorsx.Wrap(rows.Err())
	}

	return node, nil
}
func (importer *Importer) GetWayByID(id int64) (*ownmap.OSMWay, error) {
	row := importer.tx.QueryRow("SELECT id FROM ways WHERE id = $1", id)
	if row.Err() != nil {
		return nil, errorsx.Wrap(row.Err())
	}

	var discard int64
	err := row.Scan(&discard)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errorsx.ObjectNotFound
		}
		return nil, errorsx.Wrap(err)
	}

	way := &ownmap.OSMWay{ID: id}

	rows, err := importer.tx.Query("SELECT key, value FROM tags t WHERE t.object_id = $1 AND t.object_type_id = $2", id, ownmap.ObjectTypeWay)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}
	defer rows.Close()

	for rows.Next() {
		tag := new(ownmap.OSMTag)
		err = rows.Scan(&tag.Key, &tag.Value)
		if err != nil {
			return nil, errorsx.Wrap(err)
		}

		way.Tags = append(way.Tags, tag)
	}

	if rows.Err() != nil {
		return nil, errorsx.Wrap(rows.Err())
	}

	// nodes in way

	rows, err = importer.tx.Query("SELECT node_id FROM way_nodes wn WHERE wn.way_id = $1", id)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}
	defer rows.Close()

	for rows.Next() {
		var nodeID int64
		err = rows.Scan(&nodeID)
		if err != nil {
			return nil, errorsx.Wrap(err)
		}

		way.WayPoints = append(way.WayPoints, &ownmap.WayPoint{
			NodeID: nodeID,
		})
	}

	if rows.Err() != nil {
		return nil, errorsx.Wrap(rows.Err())
	}

	return way, nil
}
func (importer *Importer) GetRelationByID(id int64) (*ownmap.OSMRelation, error) {
	row := importer.tx.QueryRow("SELECT id FROM relations WHERE id = $1", id)
	if row.Err() != nil {
		return nil, errorsx.Wrap(row.Err())
	}

	var discard int64
	err := row.Scan(&discard)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errorsx.ObjectNotFound
		}
		return nil, errorsx.Wrap(err)
	}

	relation := &ownmap.OSMRelation{ID: id}

	rows, err := importer.tx.Query("SELECT key, value FROM tags t WHERE t.object_id = $1 AND t.object_type_id = $2", id, ownmap.ObjectTypeRelation)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}
	defer rows.Close()

	for rows.Next() {
		tag := new(ownmap.OSMTag)
		err = rows.Scan(&tag.Key, &tag.Value)
		if err != nil {
			return nil, errorsx.Wrap(err)
		}

		relation.Tags = append(relation.Tags, tag)
	}

	if rows.Err() != nil {
		return nil, errorsx.Wrap(rows.Err())
	}

	// nodes in way

	rows, err = importer.tx.Query("SELECT member_id, member_type, role, orientation FROM relation_members rm WHERE rm.parent_id = $1", id)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}
	defer rows.Close()

	for rows.Next() {
		member := new(ownmap.OSMRelationMember)
		err = rows.Scan(&member.ObjectID, &member.MemberType, &member.Role, &member.Orientation)
		if err != nil {
			return nil, errorsx.Wrap(err)
		}

		relation.Members = append(relation.Members, member)
	}

	if rows.Err() != nil {
		return nil, errorsx.Wrap(rows.Err())
	}

	return relation, nil
}
func (importer *Importer) ImportNode(obj *ownmap.OSMNode) errorsx.Error {
	_, err := importer.tx.Exec(`INSERT INTO nodes (id, lat, lon) VALUES ($1, $2, $3)`, obj.ID, obj.Lat, obj.Lon)
	if err != nil {
		return errorsx.Wrap(err)
	}

	for _, tag := range obj.Tags {
		_, err = importer.tx.Exec(`INSERT INTO tags (object_id, object_type_id, key, value) VALUES ($1, $2, $3, $4)`, obj.ID, ownmap.ObjectTypeNode, tag.Key, tag.Value)
		if err != nil {
			return errorsx.Wrap(err)
		}
	}

	return nil
}
func (importer *Importer) ImportWay(obj *ownmap.OSMWay) errorsx.Error {
	_, err := importer.tx.Exec(`INSERT INTO ways (id) VALUES ($1)`, obj.ID)
	if err != nil {
		return errorsx.Wrap(err)
	}

	for _, tag := range obj.Tags {
		_, err = importer.tx.Exec(`INSERT INTO tags (object_id, object_type_id, key, value) VALUES ($1, $2, $3, $4)`, obj.ID, ownmap.ObjectTypeWay, tag.Key, tag.Value)
		if err != nil {
			return errorsx.Wrap(err)
		}
	}

	for _, waypoint := range obj.WayPoints {
		_, err = importer.tx.Exec(`INSERT INTO way_nodes (way_id, node_id) VALUES ($1, $2)`, obj.ID, waypoint.NodeID)
		if err != nil {
			return errorsx.Wrap(err)
		}
	}

	return nil
}

func (importer *Importer) ImportRelation(obj *ownmap.OSMRelation) errorsx.Error {
	_, err := importer.tx.Exec(`INSERT INTO relations (id) VALUES ($1)`, obj.ID)
	if err != nil {
		return errorsx.Wrap(err)
	}

	for _, tag := range obj.Tags {
		_, err = importer.tx.Exec(`INSERT INTO tags (object_id, object_type_id, key, value) VALUES ($1, $2, $3, $4)`, obj.ID, ownmap.ObjectTypeRelation, tag.Key, tag.Value)
		if err != nil {
			return errorsx.Wrap(err)
		}
	}

	for _, member := range obj.Members {
		_, err = importer.tx.Exec(`INSERT INTO relation_members (member_id, member_type, role, orientation, parent_id) VALUES ($1, $2, $3, $4, $5)`,
			member.ObjectID, member.MemberType, member.Role, member.Orientation, obj.ID,
		)
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
