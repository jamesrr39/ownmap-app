package parquetdb

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"path/filepath"
	"runtime"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/ownmap-app/ownmap"
	"github.com/jamesrr39/ownmap-app/ownmapdal"
	"github.com/paulmach/osm/osmpbf"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	parquetwriter "github.com/xitongsys/parquet-go/writer"
)

// JSON writer example: https://github.com/xitongsys/parquet-go/blob/62cf52a8dad4f8b729e6c38809f091cd134c3749/example/json_write.go

var (
	//go:embed nodes_schema.json
	nodesSchema string
	//go:embed ways_schema.json
	waysSchema string
	//go:embed relations_schema.json
	relationsSchema string
)

type Importer struct {
	nodesParquetWriterFile     *parquetwriter.JSONWriter
	waysParquetWriterFile      *parquetwriter.JSONWriter
	relationsParquetWriterFile *parquetwriter.JSONWriter
	nodeMap                    map[int64]*ownmap.OSMNode
	wayMap                     map[int64]*ownmap.OSMWay
	relationMap                map[int64]*ownmap.OSMRelation
}

func NewImporter(dirPath string, pbfHeader *osmpbf.Header) (*Importer, errorsx.Error) {

	schemes := []string{nodesSchema, waysSchema, relationsSchema}
	fileNames := []string{"nodes", "ways", "relations"}

	var writerFiles []*parquetwriter.JSONWriter
	for i := 0; i < 3; i++ {
		f, err := local.NewLocalFileWriter(filepath.Join(dirPath, fmt.Sprintf("%s.parquet", fileNames[i])))
		if err != nil {
			return nil, errorsx.Wrap(err)
		}

		writerFile, err := parquetwriter.NewJSONWriter(schemes[i], f, int64(runtime.NumCPU()))
		if err != nil {
			return nil, errorsx.Wrap(err)
		}
		writerFile.RowGroupSize = 128 * 1024 * 1024 * 4 //128M * 4
		writerFile.CompressionType = parquet.CompressionCodec_SNAPPY
		writerFiles = append(writerFiles, writerFile)
	}

	return &Importer{
		writerFiles[0], writerFiles[1], writerFiles[2],
		make(map[int64]*ownmap.OSMNode), make(map[int64]*ownmap.OSMWay), make(map[int64]*ownmap.OSMRelation),
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
	panic("done!!")
}
func (i *Importer) Rollback() errorsx.Error {
	return nil // TODO?
}
