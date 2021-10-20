package parquetqueryengine

import (
	"bytes"
	"log"
	"strings"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/xitongsys/parquet-go/encoding"
	"github.com/xitongsys/parquet-go/parquet"
)

type ColumnScanResult int

const (
	ColumnScanResultYes     ColumnScanResult = iota + 1
	ColumnScanResultNotSure                  // appropriate when asking about a column that doesn't concern the given filter
	ColumnScanResultNo
)

var columnScanResultStrings = []string{
	"Unknown",
	"Yes",
	"Not Sure",
	"No",
}

func (csr ColumnScanResult) String() string {
	return columnScanResultStrings[csr]
}

type Filter interface {
	ShouldColumnBeScanned(columnMetadata *parquet.ColumnMetaData) (ColumnScanResult, errorsx.Error)
	GetComparativeFilters() []*ComparativeFilter
	ShouldFilterItemIn(fieldName string, value Operand) (ColumnScanResult, errorsx.Error)
}

type LogicalFilterOperator string

const (
	LogicalFilterOperatorAnd = "AND"
)

type LogicalFilter struct {
	Operator     LogicalFilterOperator
	ChildFilters []Filter
}

func (lf *LogicalFilter) ShouldColumnBeScanned(columnMetadata *parquet.ColumnMetaData) (ColumnScanResult, errorsx.Error) {
	switch lf.Operator {
	case LogicalFilterOperatorAnd:
		if len(lf.ChildFilters) == 0 {
			return 0, errorsx.Errorf("no child filters supplied")
		}

		var shouldScanColumn ColumnScanResult = ColumnScanResultNotSure
		for _, childFilter := range lf.ChildFilters {
			childFilterScanResult, err := childFilter.ShouldColumnBeScanned(columnMetadata)
			if err != nil {
				return 0, errorsx.Wrap(err)
			}

			log.Printf("col: %q, cfsr: %s (cf: %#v)\n", strings.Join(columnMetadata.PathInSchema, "."), childFilterScanResult, childFilter)

			switch childFilterScanResult {
			case ColumnScanResultNo:
				return ColumnScanResultNo, nil
			case ColumnScanResultYes:
				// mark column as wanted, but don't exit yet (wait to see the result of the other child filters)
				shouldScanColumn = ColumnScanResultYes
			case ColumnScanResultNotSure:
				// no effect on this
			default:
				return 0, errorsx.Errorf("unknown filter scan result: %v", childFilterScanResult)
			}
		}
		return shouldScanColumn, nil
	default:
		return 0, errorsx.Errorf("unrecognised operator: %q", lf.Operator)
	}
}

func (lf *LogicalFilter) GetComparativeFilters() []*ComparativeFilter {
	var filters []*ComparativeFilter
	for _, childFilter := range lf.ChildFilters {
		filters = append(filters, childFilter.GetComparativeFilters()...)
	}
	return filters
}

func (lf *LogicalFilter) ShouldFilterItemIn(fieldName string, value Operand) (ColumnScanResult, errorsx.Error) {
	switch lf.Operator {
	case LogicalFilterOperatorAnd:
		if len(lf.ChildFilters) == 0 {
			return 0, errorsx.Errorf("no child filters supplied")
		}

		var shouldScanColumn ColumnScanResult = ColumnScanResultNotSure

		for _, childFilter := range lf.ChildFilters {
			shouldFilterResult, err := childFilter.ShouldFilterItemIn(fieldName, value)
			if err != nil {
				return 0, err
			}
			switch shouldFilterResult {
			case ColumnScanResultNo:
				return ColumnScanResultNo, nil
			case ColumnScanResultYes:
				// mark column as wanted, but don't exit yet (wait to see the result of the other child filters)
				shouldScanColumn = ColumnScanResultYes
			case ColumnScanResultNotSure:
				// no effect on this
			default:
				return 0, errorsx.Errorf("unknown filter scan result: %v", shouldFilterResult)
			}
		}
		return shouldScanColumn, nil
	default:
		return 0, errorsx.Errorf("unrecognised operator: %q", lf.Operator)
	}
}

type ComparativeOperator string

const (
	ComparativeOperatorGreaterThan = ">"
	ComparativeOperatorLessThan    = "<"
)

type ComparativeFilter struct {
	FieldName string // if a nested field, use a dot "." to denote the separate parts (e.g. "user.address.street")
	Operator  ComparativeOperator
	Operand   Operand
}

func (cf *ComparativeFilter) ShouldColumnBeScanned(columnMetadata *parquet.ColumnMetaData) (ColumnScanResult, errorsx.Error) {
	if cf.FieldName != strings.Join(columnMetadata.PathInSchema, ".") {
		return ColumnScanResultNotSure, nil
	}

	switch cf.Operator {
	case ComparativeOperatorLessThan:
		if cf.Operand == nil {
			return ColumnScanResultNo, errorsx.Errorf("operand is nil")
		}

		colMinVal, err := bytesToOperand(columnMetadata.Statistics.MinValue, columnMetadata.Type)
		if err != nil {
			return ColumnScanResultNo, errorsx.Wrap(err)
		}

		isLess, err := colMinVal.IsLessThan(cf.Operand)
		if err != nil {
			return ColumnScanResultNo, errorsx.Wrap(err)
		}

		if isLess {
			return ColumnScanResultYes, nil
		}
		return ColumnScanResultNo, nil
	case ComparativeOperatorGreaterThan:
		if cf.Operand == nil {
			return ColumnScanResultNo, errorsx.Errorf("operand is nil")
		}

		colMaxVal, err := bytesToOperand(columnMetadata.Statistics.MaxValue, columnMetadata.Type)
		if err != nil {
			return ColumnScanResultNo, errorsx.Wrap(err)
		}

		log.Printf("colMinVal:: %#v :: %#v\n", colMaxVal, cf.Operand)

		isGreater, err := colMaxVal.IsGreaterThan(cf.Operand)
		if err != nil {
			return ColumnScanResultNo, errorsx.Wrap(err)
		}

		if isGreater {
			return ColumnScanResultYes, nil
		}
		return ColumnScanResultNo, nil
	}

	return ColumnScanResultNo, errorsx.Errorf("unrecognised operator: %q", cf.Operator)
}

func (cf *ComparativeFilter) ShouldFilterItemIn(fieldName string, value Operand) (ColumnScanResult, errorsx.Error) {
	if fieldName != cf.FieldName {
		return ColumnScanResultNotSure, nil
	}

	switch cf.Operator {
	case ComparativeOperatorLessThan:
		if cf.Operand == nil {
			return 0, errorsx.Errorf("operand is nil")
		}

		isLess, err := value.IsLessThan(cf.Operand)
		if err != nil {
			return 0, errorsx.Wrap(err)
		}
		if isLess {
			return ColumnScanResultYes, nil
		}
		return ColumnScanResultNo, nil
	case ComparativeOperatorGreaterThan:
		if cf.Operand == nil {
			return 0, errorsx.Errorf("operand is nil")
		}

		isGreater, err := value.IsGreaterThan(cf.Operand)
		if err != nil {
			return 0, errorsx.Wrap(err)
		}
		if isGreater {
			return ColumnScanResultYes, nil
		}
		return ColumnScanResultNo, nil
	default:
		return 0, errorsx.Errorf("unsupported operator: %q", cf.Operator)
	}
}

func (cf *ComparativeFilter) GetComparativeFilters() []*ComparativeFilter {
	return []*ComparativeFilter{cf}
}

func bytesToOperand(val []byte, valueType parquet.Type) (Operand, errorsx.Error) {
	switch valueType {
	case parquet.Type_INT64:
		items := make([]interface{}, 1)
		err := encoding.BinaryReadINT64(bytes.NewBuffer(val), items)
		if err != nil {
			return nil, errorsx.Wrap(err)
		}

		return Int64Operand(items[0].(int64)), nil
	case parquet.Type_DOUBLE:
		items := make([]interface{}, 1)
		err := encoding.BinaryReadFLOAT64(bytes.NewBuffer(val), items)
		if err != nil {
			return nil, errorsx.Wrap(err)
		}

		return Float64Operand(items[0].(float64)), nil
	default:
		return nil, errorsx.Errorf("unhandled type: %q", valueType)
	}
}
