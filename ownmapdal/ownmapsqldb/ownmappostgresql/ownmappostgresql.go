package ownmappostgresql

import (
	_ "embed"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/ownmap-app/ownmapdal"
	"github.com/jamesrr39/ownmap-app/ownmapdal/ownmapsqldb"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/paulmach/osm/osmpbf"
)

//go:embed postgresql_schema.sql
var postgresqlSchema string

func NewFinalStorage(connStr string, pbfHeader *osmpbf.Header) (ownmapdal.FinalStorage, errorsx.Error) {
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
