package parquetdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/ownmap-app/ownmap"
	"github.com/jamesrr39/ownmap-app/ownmapdal"
	"github.com/paulmach/osm"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/encoding"
	parquetreader "github.com/xitongsys/parquet-go/reader"
)

var _ ownmapdal.DataSourceConn = &ParquetDatasource{}

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
	ds.mu.Lock()
	defer ds.mu.Unlock()

	startTime := time.Now()
	numRows := ds.nodesParquetReaderFile.GetNumRows()

	classes, repititionLevels, dls, err := ds.nodesParquetReaderFile.ReadColumnByPath("Parquet_go_root.Tags.List.Element.Key", numRows)
	if err != nil {
		return nil, nil, nil, errorsx.Wrap(err)
	}

	for _, rowGroup := range ds.nodesParquetReaderFile.Footer.RowGroups {
		for _, column := range rowGroup.GetColumns() {
			log.Printf("path: %q, min: %v, max: %v\n", strings.Join(column.MetaData.PathInSchema, ","), column.MetaData.Statistics.MinValue, column.MetaData.Statistics.MaxValue)
			switch column.MetaData.PathInSchema[0] {
			case "Id":
				log.Printf("min ID: %d, max ID: %d\n", binary.LittleEndian.Uint64(column.MetaData.Statistics.MinValue), binary.LittleEndian.Uint64(column.MetaData.Statistics.MaxValue))
			case "Lat":
				vals, err := encoding.ReadPlain(
					bytes.NewReader(column.MetaData.Statistics.MinValue),
					column.MetaData.Type,
					// uint64(len(column.MetaData.Statistics.MinValue)),
					uint64(len(column.MetaData.Statistics.MinValue)/8),
					0)
				if err != nil {
					panic(err)
				}
				log.Printf("min Lat: %f\n", vals[0])
			case "Tags":
				log.Printf("path: %s, min: %q, max: %q\n", strings.Join(column.MetaData.PathInSchema, ","), column.MetaData.Statistics.MinValue, column.MetaData.Statistics.MaxValue)
			}

		}
	}

	ds.nodesParquetReaderFile.ReadStop()

	duration := time.Since(startTime)
	log.Printf("read %d rows in %s\n", numRows, duration)

	println("classes, repetitionLevels, DLS", classes, repititionLevels, dls)

	// var currItemTags []string

	// rowID := -1
	// for i, class := range classes {
	// 	repititionLevel := repititionLevels[i]
	// 	if repititionLevel == 0 {
	// 		rowID++
	// 	}

	// 	if class == nil {
	// 		continue
	// 	}

	// 	tagKey := class.(string)

	// 	isNewItem := repititionLevel == 0
	// 	if isNewItem && currItemTags != nil {
	// 		// log.Printf("end of item with tags: %#v, at index %d\n", currItemTags, rowID)
	// 		currItemTags = nil
	// 	}

	// 	currItemTags = append(currItemTags, tagKey)

	// 	// log.Println("tk:", tagKey, "i", i, "rls", rls[i], "dls", dls[i])
	// }
	// log.Printf("end of item with tags: %#v", currItemTags)
	// log.Printf("%T :: %d :: %d :: %d\n", classes, len(classes), len(repititionLevels), len(dls))
	time.Sleep(time.Second)
	panic("not implemented")
}
