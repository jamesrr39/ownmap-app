package ownmappostgresql

import (
	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/ownmap-app/ownmapdal"
	"github.com/jamesrr39/ownmap-app/ownmapdal/ownmapsqldb"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/paulmach/osm/osmpbf"
)

const postgresqlSchema = `
CREATE TABLE nodes (
	id BIGINT PRIMARY KEY,
	lat DOUBLE PRECISION NOT NULL,
	lon DOUBLE PRECISION NOT NULL
);

CREATE INDEX ON nodes (lat, lon);

CREATE TABLE ways (
	id BIGINT PRIMARY KEY
);
CREATE TABLE relations (
	id BIGINT PRIMARY KEY
);

-- Join tables

CREATE TABLE way_nodes (
	node_id BIGINT NOT NULL REFERENCES nodes(id),
	way_id BIGINT NOT NULL REFERENCES ways(id)
);

CREATE INDEX ON way_nodes (node_id);
CREATE INDEX ON way_nodes (way_id);

CREATE TABLE relation_members (
	member_id BIGINT NOT NULL,
	member_type SMALLINT NOT NULL, -- see ownmap.OSMRelationMember_OSMMemberType
	role TEXT NOT NULL,
	orientation SMALLINT NOT NULL,
	parent_id BIGINT NOT NULL REFERENCES relations(id)
);

CREATE INDEX ON relation_members (parent_id);

-- Tags

CREATE TABLE tags (
	object_id BIGINT NOT NULL,
	object_type_id BIGINT NOT NULL, -- see ownmap.ObjectType
	key TEXT NOT NULL,
	value TEXT NOT NULL
);

CREATE INDEX ON tags (object_type_id, key);
CREATE INDEX ON tags (object_type_id, object_id);

-- dataset info

CREATE TABLE dataset_info (
	bounds_min_lat DOUBLE PRECISION NOT NULL, 
	bounds_max_lat DOUBLE PRECISION NOT NULL, 
	bounds_min_lon DOUBLE PRECISION NOT NULL, 
	bounds_max_lon DOUBLE PRECISION NOT NULL, 
	replication_time TIMESTAMP WITHOUT TIME ZONE
)`

func NewFinalStorage(connStr string, pbfHeader *osmpbf.Header) (ownmapdal.Importer, errorsx.Error) {
	db, err := sqlx.Open("postgres", "postgresql://"+connStr)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	_, err = db.Exec(postgresqlSchema)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	toDatasourceConnFunc := func() (ownmapdal.DataSourceConn, errorsx.Error) {
		return ownmapsqldb.NewMapmakerSQLDB(db, "postgresql database"), nil
	}

	return ownmapsqldb.NewImporter(db, pbfHeader, toDatasourceConnFunc)
}

func NewDBConn(connStr string) (ownmapdal.DataSourceConn, errorsx.Error) {
	db, err := sqlx.Open("postgres", "postgresql://"+connStr)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	return ownmapsqldb.NewMapmakerSQLDB(db, "postgresql database"), nil
}
