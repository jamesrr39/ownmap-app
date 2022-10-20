package parquetqueryengine

import (
	"runtime"
	"sort"
	"testing"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/ownmap-app/ownmapdal/parquetdb/parquetqueryengine/pqetestutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go/reader"
)

func Test_Run(t *testing.T) {
	var err error

	f, err := pqetestutil.EnsureTestFile()
	require.NoError(t, err)
	defer f.Close()

	parquetReader, err := reader.NewParquetReader(f, pqetestutil.OsmNodesSchema, int64(runtime.NumCPU()))
	require.NoError(t, err)

	t.Run("query with WHERE clause", func(t *testing.T) {
		// Tags format:
		// Parquet_go_root\x01Tags\x01Key_value\x01Key":(*reader.ColumnBufferType)(0xc00038abd0), "Parquet_go_root\x01Tags\x01Key_value\x01Value
		query := &Query{
			Select: []string{"Id", "Lat", "Lon", "Tags"},
			Where: &LogicalFilter{
				Operator: LogicalFilterOperatorAnd,
				ChildFilters: []Filter{
					&ComparativeFilter{
						FieldName: "Id",
						Operator:  ComparativeOperatorLessThan,
						Operand:   Int64Operand(5),
					},
					&ComparativeFilter{
						FieldName: "Lon",
						Operator:  ComparativeOperatorLessThan,
						Operand:   Float64Operand(-10.15),
					},
				},
			},
		}

		expectedResults := []*ResultRow{
			{int64(3), float64(10.2), float64(-10.2)},
			{int64(4), float64(10.3), float64(-10.3)},
		}

		results, runErr := query.Run(parquetReader, parquetReader.Footer.GetRowGroups(), "Parquet_go_root")
		require.NoError(t, runErr, errorsx.ErrWithStack(runErr))

		// sort results for deterministic result set
		sort.Slice(results, func(i, j int) bool {
			iResult := *results[i]
			jResult := *results[j]

			// [0] field is Id field
			return iResult[0].(int64) < jResult[0].(int64)

		})
		assert.Equal(t, expectedResults, results)
	})

	// t.Run("WHERE == nil", func(t *testing.T) {
	// 	query := &Query{
	// 		Select: []string{"Id", "Lat", "Lon", "Tags"},
	// 	}

	// 	rows, err := query.Run(parquetReader, parquetReader.Footer.GetRowGroups(), "Parquet_go_root")
	// 	require.NoError(t, err, errorsx.ErrWithStack(err))

	// 	assert.Len(t, rows, len(pqetestutil.TestNodes))
	// })
}
