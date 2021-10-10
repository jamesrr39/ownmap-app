package parquetqueryengine

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/xitongsys/parquet-go/encoding"
	"github.com/xitongsys/parquet-go/parquet"
)

type Filter interface {
	ShouldColumnBeScanned(columnMetadata *parquet.ColumnMetaData) (bool, error)
	GetFieldNamesToScan() []string
	ShouldFilterItemIn(fieldName string, value Operand) (bool, error)
}

type LogicalFilterOperator string

const (
	LogicalFilterOperatorAnd = "AND"
)

type LogicalFilter struct {
	Operator     LogicalFilterOperator
	ChildFilters []Filter
}

func (lf *LogicalFilter) ShouldColumnBeScanned(columnMetadata *parquet.ColumnMetaData) (bool, error) {
	switch lf.Operator {
	case LogicalFilterOperatorAnd:
		if len(lf.ChildFilters) == 0 {
			return false, errors.New("no child filters supplied")
		}

		for _, childFilter := range lf.ChildFilters {
			childFilterFilteredIn, err := childFilter.ShouldColumnBeScanned(columnMetadata)
			if err != nil {
				return false, err
			}
			if !childFilterFilteredIn {
				return false, nil
			}
		}
		return true, nil
	default:
		return false, fmt.Errorf("unrecognised operator: %q", lf.Operator)
	}
}

func (lf *LogicalFilter) GetFieldNamesToScan() []string {
	var fieldNames []string
	for _, childFilter := range lf.ChildFilters {
		fieldNames = append(fieldNames, childFilter.GetFieldNamesToScan()...)
	}
	return fieldNames
}

type FilterResult int

const (
	FilterResultInclude     FilterResult = 1
	FilterResultExclude     FilterResult = 2
	FilterResultNotRelevant FilterResult = 3
)

func (lf *LogicalFilter) ShouldFilterItemIn(fieldName string, value Operand) (bool, error) {
	switch lf.Operator {
	case LogicalFilterOperatorAnd:
		if len(lf.ChildFilters) == 0 {
			return false, errors.New("no child filters supplied")
		}

		for _, childFilter := range lf.ChildFilters {
			shouldFilterIn, err := childFilter.ShouldFilterItemIn(fieldName, value)
			if err != nil {
				return false, err
			}
			if !shouldFilterIn {
				return false, nil
			}
		}
		return true, nil
	default:
		return false, fmt.Errorf("unrecognised operator: %q", lf.Operator)
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

func (cf *ComparativeFilter) ShouldColumnBeScanned(columnMetadata *parquet.ColumnMetaData) (bool, error) {
	if cf.FieldName != strings.Join(columnMetadata.PathInSchema, ".") {
		return false, nil
	}

	switch cf.Operator {
	case ComparativeOperatorLessThan:
		if cf.Operand == nil {
			return false, errors.New("operand is nil")
		}

		colMaxVal, err := bytesToOperand(columnMetadata.Statistics.MaxValue, columnMetadata.Type)
		if err != nil {
			return false, errorsx.Wrap(err)
		}
		return cf.Operand.IsLessThan(colMaxVal)
	case ComparativeOperatorGreaterThan:
		if cf.Operand == nil {
			return false, errors.New("operand is nil")
		}

		colMinVal, err := bytesToOperand(columnMetadata.Statistics.MinValue, columnMetadata.Type)
		if err != nil {
			return false, errorsx.Wrap(err)
		}
		return cf.Operand.IsGreaterThan(colMinVal)
	}

	return false, fmt.Errorf("unrecognised operator: %q", cf.Operator)
}

func (cf *ComparativeFilter) ShouldFilterItemIn(fieldName string, value Operand) (bool, error) {
	if fieldName != cf.FieldName {
		return false, nil
	}

	switch cf.Operator {
	case ComparativeOperatorLessThan:
		if cf.Operand == nil {
			return false, errorsx.Errorf("operand is nil")
		}

		return value.IsLessThan(cf.Operand)
	case ComparativeOperatorGreaterThan:
		if cf.Operand == nil {
			return false, errorsx.Errorf("operand is nil")
		}

		return value.IsGreaterThan(cf.Operand)
	default:
		return false, errorsx.Errorf("unsupported operator: %q", cf.Operator)
	}
}

func (cf *ComparativeFilter) GetFieldNamesToScan() []string {
	return []string{cf.FieldName}
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
		return nil, errorsx.Errorf("unhandled type: %s", valueType)
	}
}
