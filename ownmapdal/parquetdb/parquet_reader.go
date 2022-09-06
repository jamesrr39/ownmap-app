package parquetdb

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/ownmap-app/ownmap"
	"github.com/jamesrr39/ownmap-app/ownmapdal"
	"github.com/jamesrr39/ownmap-app/ownmapdal/parquetdb/parquetqueryengine"
	"github.com/paulmach/osm"
	"github.com/xitongsys/parquet-go-source/local"
	parquetreader "github.com/xitongsys/parquet-go/reader"
)

var (
	// TODO: configurable parallelism
	parallelism = int64(runtime.NumCPU())

	_ ownmapdal.DataSourceConn = &ParquetDatasource{}
)

type ParquetDatasource struct {
	dirPath     string
	datasetInfo *ownmap.DatasetInfo

	nodesParquetReaderFile     *parquetreader.ParquetReader
	waysParquetReaderFile      *parquetreader.ParquetReader
	relationsParquetReaderFile *parquetreader.ParquetReader

	mu *sync.Mutex
}

func NewParquetDatasource(dirPath string) (*ParquetDatasource, errorsx.Error) {
	datasetInfoPath := filepath.Join(dirPath, datasetInfoFileName)

	infoFile, err := os.Open(datasetInfoPath)
	if err != nil {
		return nil, errorsx.Wrap(err, "filepath", datasetInfoPath)
	}
	defer infoFile.Close()

	datasetInfo := new(ownmap.DatasetInfo)
	err = json.NewDecoder(infoFile).Decode(datasetInfo)
	if err != nil {
		return nil, errorsx.Wrap(err, "filepath", datasetInfoPath)
	}

	var readers []*parquetreader.ParquetReader
	for _, fileNameAndSchema := range getFileNamesAndSchemas() {
		filePath := filepath.Join(dirPath, fileNameAndSchema.Name)

		fileReader, err := local.NewLocalFileReader(filePath)
		if err != nil {
			return nil, errorsx.Wrap(err, "filepath", filePath)
		}

		pr, err := parquetreader.NewParquetReader(fileReader, fileNameAndSchema.Schema, parallelism)
		if err != nil {
			return nil, errorsx.Wrap(err, "filepath", filePath)
		}

		readers = append(readers, pr)
	}

	return &ParquetDatasource{
		dirPath:     dirPath,
		datasetInfo: datasetInfo,

		nodesParquetReaderFile:     readers[0],
		waysParquetReaderFile:      readers[1],
		relationsParquetReaderFile: readers[2],

		mu: new(sync.Mutex),
	}, nil
}

func (ds *ParquetDatasource) Name() string {
	return filepath.Base(ds.dirPath)
}
func (ds *ParquetDatasource) DatasetInfo() (*ownmap.DatasetInfo, errorsx.Error) {
	return ds.datasetInfo, nil
}
func (ds *ParquetDatasource) GetInBounds(ctx context.Context, bounds osm.Bounds, filter *ownmapdal.GetInBoundsFilter) (ownmapdal.TagNodeMap, ownmapdal.TagWayMap, ownmapdal.TagRelationMap, errorsx.Error) {
	// parquet reader is (I think) not thread-safe. TODO: pool of readers, or something similar.
	ds.mu.Lock()
	defer ds.mu.Unlock()

	var nodeFilters []parquetqueryengine.Filter

	for _, filterObject := range filter.Objects {
		switch filterObject.ObjectType {
		case ownmap.ObjectTypeNode:
			nodeFilters = append(nodeFilters, &parquetqueryengine.ComparativeFilter{
				FieldName: "Tags",
				Operator:  parquetqueryengine.ComparativeOperatorContains,
				Operand:   parquetqueryengine.StringOperand(filterObject.TagKey),
			})
		}
	}

	coordinateFilter := []parquetqueryengine.Filter{
		&parquetqueryengine.ComparativeFilter{
			FieldName: "Lat",
			Operator:  parquetqueryengine.ComparativeOperatorGreaterThanOrEqualTo,
			Operand:   parquetqueryengine.Float64Operand(bounds.MinLat),
		},
		&parquetqueryengine.ComparativeFilter{
			FieldName: "Lat",
			Operator:  parquetqueryengine.ComparativeOperatorLessThanOrEqualTo,
			Operand:   parquetqueryengine.Float64Operand(bounds.MaxLat),
		},
		&parquetqueryengine.ComparativeFilter{
			FieldName: "Lon",
			Operator:  parquetqueryengine.ComparativeOperatorGreaterThanOrEqualTo,
			Operand:   parquetqueryengine.Float64Operand(bounds.MinLon),
		},
		&parquetqueryengine.ComparativeFilter{
			FieldName: "Lon",
			Operator:  parquetqueryengine.ComparativeOperatorLessThanOrEqualTo,
			Operand:   parquetqueryengine.Float64Operand(bounds.MaxLon),
		},
	}

	query := parquetqueryengine.Query{
		Select: []string{"Id", "Lat", "Lon", "Tags"},
		Where: &parquetqueryengine.LogicalFilter{
			Operator:     parquetqueryengine.LogicalFilterOperatorAnd,
			ChildFilters: append(coordinateFilter), //, nodeFilters...), // FIXME
		},
	}

	results, err := query.Run(ds.nodesParquetReaderFile, ds.nodesParquetReaderFile.Footer.GetRowGroups(), "Parquet_go_root")
	if err != nil {
		return nil, nil, nil, err
	}

	// reset reader
	ds.nodesParquetReaderFile.ReadStop()

	tagNodeMap := make(ownmapdal.TagNodeMap)

	for _, result := range results {
		// Id, Lat, Lon, Tags
		tags := (*result)[3]
		log.Printf("got result. Tags type: %T. Result: %#v\n", tags, result)
	}
	log.Printf("total results: %d\n", len(results))

	return tagNodeMap, make(ownmapdal.TagWayMap), make(ownmapdal.TagRelationMap), nil
}
