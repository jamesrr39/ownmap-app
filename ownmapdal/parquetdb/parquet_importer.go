package parquetdb

import (
	_ "embed"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/ownmap-app/ownmap"
	"github.com/jamesrr39/ownmap-app/ownmapdal"
	"github.com/paulmach/osm/osmpbf"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	parquetwriter "github.com/xitongsys/parquet-go/writer"
)

// JSON writer example: https://github.com/xitongsys/parquet-go/blob/62cf52a8dad4f8b729e6c38809f091cd134c3749/example/json_write.go

type parquetFileType struct {
	Name   string
	Schema string
}

func getFileNamesAndSchemas() []parquetFileType {
	return []parquetFileType{
		{Name: "nodes.parquet", Schema: nodesSchema},
		{Name: "ways.parquet", Schema: waysSchema},
		{Name: "relations.parquet", Schema: relationsSchema},
	}
}

const datasetInfoFileName = "dataset_info.json"

var (
	//go:embed nodes_schema.json
	nodesSchema string
	//go:embed ways_schema.json
	waysSchema string
	//go:embed relations_schema.json
	relationsSchema string
)

type Importer struct {
	dirPath                    string
	nodesParquetWriterFile     *parquetwriter.JSONWriter
	waysParquetWriterFile      *parquetwriter.JSONWriter
	relationsParquetWriterFile *parquetwriter.JSONWriter
	nodeMap                    map[int64]*ownmap.OSMNode
	wayMap                     map[int64]*ownmap.OSMWay
	relationMap                map[int64]*ownmap.OSMRelation
	ImportBounds               ownmap.DatasetInfo_Bounds
	ReplicationTime            time.Time
}

func NewFinalStorage(dirPath string, pbfHeader *osmpbf.Header, rowGroupSize int64) (*Importer, errorsx.Error) {
	var writerFiles []*parquetwriter.JSONWriter
	for _, fileNameAndSchema := range getFileNamesAndSchemas() {
		f, err := local.NewLocalFileWriter(filepath.Join(dirPath, fileNameAndSchema.Name))
		if err != nil {
			return nil, errorsx.Wrap(err)
		}

		writerFile, err := parquetwriter.NewJSONWriter(fileNameAndSchema.Schema, f, int64(runtime.NumCPU()))
		if err != nil {
			return nil, errorsx.Wrap(err)
		}
		writerFile.RowGroupSize = rowGroupSize
		writerFile.CompressionType = parquet.CompressionCodec_SNAPPY
		writerFiles = append(writerFiles, writerFile)
	}

	return &Importer{
		dirPath,
		writerFiles[0], writerFiles[1], writerFiles[2],
		make(map[int64]*ownmap.OSMNode), make(map[int64]*ownmap.OSMWay), make(map[int64]*ownmap.OSMRelation),
		*ownmap.OSMBoundsToDatasetInfoBounds(*pbfHeader.Bounds), pbfHeader.ReplicationTimestamp,
	}, nil
}

func (i *Importer) GetNodeByID(id int64) (*ownmap.OSMNode, error) {
	node, ok := i.nodeMap[id]
	if !ok {
		return nil, errorsx.ObjectNotFound
	}

	return node, nil
}
func (i *Importer) GetWayByID(id int64) (*ownmap.OSMWay, error) {
	way, ok := i.wayMap[id]
	if !ok {
		return nil, errorsx.ObjectNotFound
	}

	return way, nil
}
func (i *Importer) GetRelationByID(id int64) (*ownmap.OSMRelation, error) {
	relation, ok := i.relationMap[id]
	if !ok {
		return nil, errorsx.ObjectNotFound
	}

	return relation, nil
}
func (i *Importer) ImportNode(obj *ownmap.OSMNode) errorsx.Error {
	i.nodeMap[obj.ID] = obj

	j, err := json.Marshal(obj)
	if err != nil {
		return errorsx.Wrap(err)
	}

	err = i.nodesParquetWriterFile.Write(string(j))
	if err != nil {
		return errorsx.Wrap(err)
	}

	return nil
}
func (i *Importer) ImportWay(obj *ownmap.OSMWay) errorsx.Error {
	i.wayMap[obj.ID] = obj

	j, err := json.Marshal(obj)
	if err != nil {
		return errorsx.Wrap(err)
	}

	err = i.waysParquetWriterFile.Write(string(j))
	if err != nil {
		return errorsx.Wrap(err)
	}

	return nil
}
func (i *Importer) ImportRelation(obj *ownmap.OSMRelation) errorsx.Error {
	i.relationMap[obj.ID] = obj

	j, err := json.Marshal(obj)
	if err != nil {
		return errorsx.Wrap(err)
	}

	err = i.relationsParquetWriterFile.Write(string(j))
	if err != nil {
		return errorsx.Wrap(err)
	}

	return nil
}

func (i *Importer) ImportNodes(objs []*ownmap.OSMNode) errorsx.Error {
	for _, obj := range objs {
		err := i.ImportNode(obj)
		if err != nil {
			return errorsx.Wrap(err)
		}
	}
	return nil
}
func (i *Importer) ImportWays(objs []*ownmap.OSMWay) errorsx.Error {
	for _, obj := range objs {
		err := i.ImportWay(obj)
		if err != nil {
			return errorsx.Wrap(err)
		}
	}
	return nil
}
func (i *Importer) ImportRelations(objs []*ownmap.OSMRelation) errorsx.Error {
	for _, obj := range objs {
		err := i.ImportRelation(obj)
		if err != nil {
			return errorsx.Wrap(err)
		}
	}
	return nil
}

func (i *Importer) Commit() (ownmapdal.DataSourceConn, errorsx.Error) {
	err := i.nodesParquetWriterFile.WriteStop()
	if err != nil {
		return nil, errorsx.Wrap(err)
	}
	err = i.waysParquetWriterFile.WriteStop()
	if err != nil {
		return nil, errorsx.Wrap(err)
	}
	err = i.relationsParquetWriterFile.WriteStop()
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	datasetInfo := &ownmap.DatasetInfo{
		Bounds:            &i.ImportBounds,
		ReplicationTimeMs: uint64(i.ReplicationTime.UnixNano() / (1000 * 1000)),
	}

	datasetInfoFile, err := os.Create(filepath.Join(i.dirPath, datasetInfoFileName))
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	err = json.NewEncoder(datasetInfoFile).Encode(datasetInfo)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	return NewParquetDatasource(i.dirPath)
}
func (i *Importer) Rollback() errorsx.Error {
	return errorsx.Errorf("unhandled: Rollback")
}
