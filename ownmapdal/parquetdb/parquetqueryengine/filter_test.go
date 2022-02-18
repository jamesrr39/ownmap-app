package parquetqueryengine

import (
	"testing"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go/encoding"
	"github.com/xitongsys/parquet-go/parquet"
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
