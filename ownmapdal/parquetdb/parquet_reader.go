package parquetdb

import (
	"context"
	"io/ioutil"
	"log"
	"path/filepath"
	"runtime"

	"github.com/gogo/protobuf/proto"
	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/ownmap-app/ownmap"
	"github.com/jamesrr39/ownmap-app/ownmapdal"
	"github.com/paulmach/osm"
	"github.com/xitongsys/parquet-go-source/local"
	parquetreader "github.com/xitongsys/parquet-go/reader"
)

var _ ownmapdal.DataSourceConn = &ParquetDatasource{}

type ParquetDatasource struct {
	dirPath     string
	datasetInfo *ownmap.DatasetInfo

	nodesParquetReaderFile     *parquetreader.ParquetReader
	waysParquetReaderFile      *parquetreader.ParquetReader
	relationsParquetReaderFile *parquetreader.ParquetReader
}

func NewParquetDatasource(dirPath string) (*ParquetDatasource, errorsx.Error) {
	datasetInfoPath := filepath.Join(dirPath, datasetInfoFileName)

	infoBytes, err := ioutil.ReadFile(datasetInfoPath)
	if err != nil {
		return nil, errorsx.Wrap(err, "filepath", datasetInfoPath)
	}

	datasetInfo := new(ownmap.DatasetInfo)
	err = proto.Unmarshal(infoBytes, datasetInfo)
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

		pr, err := parquetreader.NewParquetReader(fileReader, fileNameAndSchema.Schema, int64(runtime.NumCPU()))
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
	}, nil
}

func (ds *ParquetDatasource) Name() string {
	return filepath.Base(ds.dirPath)
}
func (ds *ParquetDatasource) DatasetInfo() (*ownmap.DatasetInfo, errorsx.Error) {
	return ds.datasetInfo, nil
}
func (ds *ParquetDatasource) GetInBounds(ctx context.Context, bounds osm.Bounds, filter *ownmapdal.GetInBoundsFilter) (ownmapdal.TagNodeMap, ownmapdal.TagWayMap, ownmapdal.TagRelationMap, errorsx.Error) {
	// numRows := ds.nodesParquetReaderFile.GetNumRows()
	numRows := int64(10)

	classes, rls, dls, err := ds.nodesParquetReaderFile.ReadColumnByPath("parquet_go_root.tags", numRows)
	if err != nil {
		return nil, nil, nil, errorsx.Wrap(err)
	}

	log.Printf("%#v :: %#v :: %#v\n", classes, rls, dls)
	panic("not implemented")
}
