package parquetdb

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/goutil/gofs"
	"github.com/jamesrr39/ownmap-app/ownmap"
	"github.com/jamesrr39/ownmap-app/ownmapdal"
	"github.com/paulmach/osm"

	"github.com/jmoiron/sqlx"
	_ "github.com/marcboeker/go-duckdb"
)

type DuckDBDataSourceConn struct {
	DirPath        string
	DatasetInfoObj *ownmap.DatasetInfo
	DBConn         *sqlx.DB
}

func NewDuckDBDataSourceConn(fs gofs.Fs, dirPath string) (*DuckDBDataSourceConn, errorsx.Error) {
	datasourceFile, err := fs.Open(filepath.Join(dirPath, DatasetInfoFileName))
	if err != nil {
		return nil, errorsx.Wrap(err)
	}
	defer datasourceFile.Close()

	datasourceObj := new(ownmap.DatasetInfo)

	err = json.NewDecoder(datasourceFile).Decode(datasourceObj)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	dbConn, err := sqlx.Open("duckdb", "")
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	return &DuckDBDataSourceConn{
		DirPath:        dirPath,
		DatasetInfoObj: datasourceObj,
		DBConn:         dbConn,
	}, nil
}

func (ds *DuckDBDataSourceConn) Name() string {
	return fmt.Sprintf("duckdb://%s", ds.DirPath)
}

func (ds *DuckDBDataSourceConn) DatasetInfo() (*ownmap.DatasetInfo, errorsx.Error) {
	return ds.DatasetInfoObj, nil
}

// Data fetch methods
func (ds *DuckDBDataSourceConn) GetInBounds(ctx context.Context, bounds osm.Bounds, filter *ownmapdal.GetInBoundsFilter) (ownmapdal.TagNodeMap, ownmapdal.TagWayMap, ownmapdal.TagRelationMap, errorsx.Error) {
	// https://duckdb.org/docs/sql/query_syntax/with

	// 1. get all relations with tag
	// 2. get all recursive (a) relation and (b) way and (c) node children

	var whereClause []string
	var args []interface{}
	for _, filterObject := range filter.Objects {
		switch filterObject.ObjectType {
		case ownmap.ObjectTypeRelation:
			args = append(args, filterObject.TagKey)
			whereClause = append(
				whereClause,
				fmt.Sprintf("array_length(tags[$%d]) != 0", len(args)),
			)
		}
	}

	query := fmt.Sprintf(`
		SELECT *
		FROM '%s'
		WHERE %s`,
		filepath.Join(ds.DirPath, "relations.parquet"),
		strings.Join(whereClause, " OR "))
	// query := "show tables;"

	type resultType struct {
		ID      string
		Tags    map[string]string
		Members interface{}
	}
	var results []resultType
	err := ds.DBConn.Select(&results, query, args...)
	if err != nil {
		return nil, nil, nil, errorsx.Wrap(err)
	}

	json.NewEncoder(os.Stderr).Encode(results)

	// 	WITH RECURSIVE unnested AS (
	// 		SELECT id, tags, members, [members] AS path
	// 		FROM relations.parquet r1
	// 		WHERE
	// 	UNION ALL
	// 		SELECT id, tags, members, list_prepend(r2, unnested.path)
	// 		FROM relations.parquet r2
	// 		WHERE r2.
	// 	)
	// 	SELECT * FROM unnested;
	// `)

	panic("not implemented")
}

func (ds *DuckDBDataSourceConn) Close() errorsx.Error {
	err := ds.DBConn.Close()
	if err != nil {
		return errorsx.Wrap(err)
	}
	return nil
}
