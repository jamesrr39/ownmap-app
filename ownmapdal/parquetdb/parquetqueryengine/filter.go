package parquetqueryengine

import (
	"github.com/jamesrr39/goutil/errorsx"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
)

type ShouldScanResult int

const (
	ShouldScanResultYes     ShouldScanResult = iota + 1
	ShouldScanResultNotSure                  // appropriate when asking about a column that doesn't concern the given filter
	ShouldScanResultNo
)

var columnScanResultStrings = []string{
	"Unknown",
	"Yes",
	"Not Sure",
	"No",
}

func (csr ShouldScanResult) String() string {
	return columnScanResultStrings[csr]
}

type Filter interface {
	ShouldColumnBeScanned(columnMetadata *parquet.ColumnMetaData) (ShouldScanResult, errorsx.Error)
	GetComparativeFilters() []*ComparativeFilter
	ShouldFilterItemIn(fieldName string, value Operand) (ShouldScanResult, errorsx.Error)
	ShouldRowGroupBeScanned(rowGroup *parquet.RowGroup) (ShouldScanResult, errorsx.Error)
	Validate() errorsx.Error
	ScanRowGroup(rowGroup *parquet.RowGroup, parquetReader *reader.ParquetReader, rowGroupValues rowGroupValuesCollectionType, rootSchemaElementName string) errorsx.Error
	BuildColumnNamesWanted() map[string]struct{} // map[columnName]void
}
