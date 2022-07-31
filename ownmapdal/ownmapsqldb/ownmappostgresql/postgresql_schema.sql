CREATE TABLE IF NOT EXISTS nodes (
	id BIGINT PRIMARY KEY,
	lat DOUBLE PRECISION NOT NULL,
	lon DOUBLE PRECISION NOT NULL
);

CREATE INDEX IF NOT EXISTS nodes_lat_lon_idx ON nodes (lat, lon);

CREATE TABLE IF NOT EXISTS ways (
	id BIGINT PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS relations (
	id BIGINT PRIMARY KEY
);

-- Join tables

CREATE TABLE IF NOT EXISTS way_nodes (
	node_id BIGINT NOT NULL,
	way_id BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS way_nodes_node_id_idx ON way_nodes (node_id);
CREATE INDEX IF NOT EXISTS way_nodes_way_id_idx ON way_nodes (way_id);

CREATE TABLE IF NOT EXISTS relation_members (
	member_id BIGINT NOT NULL,
	member_type SMALLINT NOT NULL, -- see ownmap.OSMRelationMember_OSMMemberType
	role TEXT NOT NULL,
	orientation SMALLINT NOT NULL,
	parent_id BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS relation_members_parent_id_idx ON relation_members (parent_id);

-- Tags

CREATE TABLE IF NOT EXISTS tags (
	object_id BIGINT NOT NULL,
	object_type_id BIGINT NOT NULL, -- see ownmap.ObjectType
	key TEXT NOT NULL,
	value TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS tags_object_type_id_key_idx ON tags (object_type_id, key);
CREATE INDEX IF NOT EXISTS tags_object_type_id_object_id_idx ON tags (object_type_id, object_id);

-- dataset info

CREATE TABLE IF NOT EXISTS dataset_info (
	bounds_min_lat DOUBLE PRECISION NOT NULL, 
	bounds_max_lat DOUBLE PRECISION NOT NULL, 
	bounds_min_lon DOUBLE PRECISION NOT NULL, 
	bounds_max_lon DOUBLE PRECISION NOT NULL, 
	replication_time TIMESTAMP WITHOUT TIME ZONE
);