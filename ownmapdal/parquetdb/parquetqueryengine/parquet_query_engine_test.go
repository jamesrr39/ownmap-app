package parquetqueryengine

import (
	"runtime"
	"testing"

	"github.com/jamesrr39/ownmap-app/ownmapdal/parquetdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

func Test_Run(t *testing.T) {
	filePath := "../../../data/data_files/parquet_files/nodes.parquet"

	f, err := local.NewLocalFileReader(filePath)
	require.NoError(t, err)
	defer f.Close()

	parquetReader, err := reader.NewParquetReader(f, parquetdb.NodesSchema, int64(runtime.NumCPU()))
	require.NoError(t, err)

	query := &Query{
		Select: []string{"Id"},
		Where: &ComparativeFilter{
			FieldName: "Id",
			Operator:  ComparativeOperatorGreaterThan,
			Operand:   Int64Operand(123456),
		},
		Limit: 100,
	}

	results, runErr := query.Run(parquetReader, "Parquet_go_root")
	require.NoError(t, runErr, string(runErr.Stack()))
	assert.Nil(t, results)
}
