package parquetqueryengine

import (
	"runtime"
	"testing"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/ownmap-app/ownmapdal/parquetdb/parquetqueryengine/pqetestutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go/encoding"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
)

func TestLogicalFilter_ShouldColumnBeScanned(t *testing.T) {
	type fields struct {
		Operator     LogicalFilterOperator
		ChildFilters []Filter
	}
	type args struct {
		columnMetadata *parquet.ColumnMetaData
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   ShouldScanResult
		want1  errorsx.Error
	}{
		{
			name: "10 < x < 20",
			fields: fields{
				Operator: LogicalFilterOperatorAnd,
				ChildFilters: []Filter{
					&ComparativeFilter{
						FieldName: "Id",
						Operator:  ComparativeOperatorGreaterThan,
						Operand:   Int64Operand(10),
					},
					&ComparativeFilter{
						FieldName: "Id",
						Operator:  ComparativeOperatorLessThan,
						Operand:   Int64Operand(20),
					},
				},
			},
			args: args{
				columnMetadata: &parquet.ColumnMetaData{
					PathInSchema: []string{"Id"},
					Statistics: &parquet.Statistics{
						MinValue: encoding.WritePlainINT64([]interface{}{int64(5)}),
						MaxValue: encoding.WritePlainINT64([]interface{}{int64(15)}),
					},
					Type: parquet.Type_INT64,
				},
			},
			want: ShouldScanResultYes,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lf := &LogicalFilter{
				Operator:     tt.fields.Operator,
				ChildFilters: tt.fields.ChildFilters,
			}
			got, err := lf.ShouldColumnBeScanned(tt.args.columnMetadata)
			require.Equal(t, tt.want1, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLogicalFilter_ScanRowGroup_oneCol(t *testing.T) {
	var err error

	f, err := pqetestutil.EnsureTestFile()
	require.NoError(t, err)
	defer f.Close()

	parquetReader, err := reader.NewParquetReader(f, pqetestutil.OsmNodesSchema, int64(runtime.NumCPU()))
	require.NoError(t, err)

	filter := &LogicalFilter{
		Operator: LogicalFilterOperatorAnd,
		ChildFilters: []Filter{
			&ComparativeFilter{
				FieldName: "Lat",
				Operator:  ComparativeOperatorGreaterThanOrEqualTo,
				Operand:   Float64Operand(11),
			},
			&ComparativeFilter{
				FieldName: "Lat",
				Operator:  ComparativeOperatorLessThanOrEqualTo,
				Operand:   Float64Operand(12),
			},
		},
	}

	queryRunner := newQueryRunnerType()
	rowGroupValues := make(rowGroupValuesCollectionType)
	err = filter.ScanRowGroup(parquetReader.Footer.RowGroups[0], parquetReader, queryRunner, rowGroupValues, "Parquet_go_root")
	require.NoError(t, err)

	type expectedResultType struct {
		Lat float64
		Lon float64
	}

	// map[rowGroupIndex]expectedResult
	expectedResults := map[int]expectedResultType{
		10: {Lat: 11, Lon: -11},
		11: {Lat: 11.1, Lon: -11.1},
		12: {Lat: 11.2, Lon: -11.2},
		13: {Lat: 11.3, Lon: -11.3},
		14: {Lat: 11.4, Lon: -11.4},
		15: {Lat: 11.5, Lon: -11.5},
		16: {Lat: 11.6, Lon: -11.6},
		17: {Lat: 11.7, Lon: -11.7},
		18: {Lat: 11.8, Lon: -11.8},
		19: {Lat: 11.9, Lon: -11.9},
		20: {Lat: 12, Lon: -12},
	}

	require.Len(t, rowGroupValues, len(expectedResults))
	for rowGroupIdx, resultRow := range rowGroupValues {
		assert.Equal(t, expectedResults[rowGroupIdx].Lat, resultRow.values["Lat"])
	}

}

func TestLogicalFilter_ScanRowGroup_twoCols(t *testing.T) {
	var err error

	f, err := pqetestutil.EnsureTestFile()
	require.NoError(t, err)
	defer f.Close()

	parquetReader, err := reader.NewParquetReader(f, pqetestutil.OsmNodesSchema, int64(runtime.NumCPU()))
	require.NoError(t, err)

	filter := &LogicalFilter{
		Operator: LogicalFilterOperatorAnd,
		ChildFilters: []Filter{
			&ComparativeFilter{
				FieldName: "Lat",
				Operator:  ComparativeOperatorGreaterThanOrEqualTo,
				Operand:   Float64Operand(11),
			},
			&ComparativeFilter{
				FieldName: "Lat",
				Operator:  ComparativeOperatorLessThanOrEqualTo,
				Operand:   Float64Operand(11.3),
			},
			&ComparativeFilter{
				FieldName: "Lon",
				Operator:  ComparativeOperatorGreaterThanOrEqualTo,
				Operand:   Float64Operand(-11.2),
			},
			&ComparativeFilter{
				FieldName: "Lon",
				Operator:  ComparativeOperatorLessThanOrEqualTo,
				Operand:   Float64Operand(-11.1),
			},
		},
	}

	queryRunner := newQueryRunnerType()
	rowGroupValues := make(rowGroupValuesCollectionType)
	err = filter.ScanRowGroup(parquetReader.Footer.RowGroups[0], parquetReader, queryRunner, rowGroupValues, "Parquet_go_root")
	require.NoError(t, err)

	type expectedResultType struct {
		Lat float64
		Lon float64
	}

	// map[rowGroupIndex]expectedResult
	expectedResults := map[int]expectedResultType{
		11: {Lat: 11.1, Lon: -11.1},
		12: {Lat: 11.2, Lon: -11.2},
	}

	require.Len(t, rowGroupValues, len(expectedResults))
	for rowGroupIdx, resultRow := range rowGroupValues {
		assert.Equal(t, expectedResults[rowGroupIdx].Lat, resultRow.values["Lat"])
		assert.Equal(t, expectedResults[rowGroupIdx].Lon, resultRow.values["Lon"])
	}
}
