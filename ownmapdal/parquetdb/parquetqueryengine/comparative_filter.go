package parquetqueryengine

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/xitongsys/parquet-go/encoding"
	"github.com/xitongsys/parquet-go/parquet"
)

type ComparativeOperator string

const (
	ComparativeOperatorGreaterThan          ComparativeOperator = ">"
	ComparativeOperatorLessThan             ComparativeOperator = "<"
	ComparativeOperatorLessThanOrEqualTo    ComparativeOperator = "<="
	ComparativeOperatorGreaterThanOrEqualTo ComparativeOperator = ">="
	ComparativeOperatorContains             ComparativeOperator = "ANY"
)

type ComparativeFilter struct {
	FieldName string // if a nested field, use a dot "." to denote the separate parts (e.g. "user.address.street")
	Operator  ComparativeOperator
	Operand   Operand
}

func (cf *ComparativeFilter) Validate() errorsx.Error {
	if cf.Operand == nil {
		return errorsx.Errorf("operand is nil")
	}

	return nil
}

func (cf *ComparativeFilter) ScanRowGroup(rowGroup *parquet.RowGroup, parquetReader ParquetReader, queryRunner *queryRunnerType, rowGroupValues rowGroupValuesCollectionType, rootSchemaElementName string) errorsx.Error {
	var column *parquet.ColumnChunk

	for _, col := range rowGroup.GetColumns() {
		if cf.FieldName == strings.Join(col.MetaData.PathInSchema, ".") {
			column = col
			break
		}
	}

	if column == nil {
		// return nil, since IIRC a parquet file can have columns added later(?);
		// so the column could be absent in early row groups but can be present in later row groups
		return nil
	}

	// TODO: pr.SkipRowsByPath()
	path := getFullParquetFieldPath(column, rootSchemaElementName)
	_, columnAlreadyScanned := queryRunner.columnsScannedMap[path]
	if columnAlreadyScanned {
		// TODO: store the value, repetition level, definition level?
		for i, value := range queryRunner.columnsScannedMap[path] {
			err := cf.processValue(value, 0, 0, i, rowGroupValues)
			if err != nil {
				return errorsx.Wrap(err)
			}
		}

	} else {

		values, repetitionLevels, definitionLevels, err := parquetReader.ReadColumnByPath(path, parquetReader.GetNumRows())
		if err != nil {
			return errorsx.Wrap(err, "path", path)
		}

		for i, value := range values {
			if repetitionLevels[i] != 0 {
				panic("not supported")
			}
			if definitionLevels[i] != 0 {
				panic("not supported")
			}

			queryRunner.columnsScannedMap[path] = append(queryRunner.columnsScannedMap[path], value)

			err = cf.processValue(value, repetitionLevels[i], definitionLevels[i], i, rowGroupValues)
			if err != nil {
				return errorsx.Wrap(err)
			}
		}
	}
	return nil
}

func (cf *ComparativeFilter) processValue(value interface{}, repetitionLevel, definitionLevel int32, rowIndex int, rowGroupValues rowGroupValuesCollectionType) errorsx.Error {
	if repetitionLevel != 0 {
		panic("not supported: repetitionLevels")
	}
	if definitionLevel != 0 {
		panic("not supported: definitionLevels")
	}

	var op Operand

	switch val := value.(type) {
	case int64:
		op = Int64Operand(val)
	case float64:
		op = Float64Operand(val)
	default:
		panic(fmt.Sprintf("unsupported value type:: %T", val))
	}

	result, err := cf.ShouldFilterItemIn(cf.FieldName, op)
	if err != nil {
		return errorsx.Wrap(err)
	}

	switch result {
	case ShouldScanResultNo:
		return nil
	case ShouldScanResultYes:
		row, ok := rowGroupValues[rowIndex]
		if !ok {
			row = &rowMapType{
				values: make(map[fieldNameType]interface{}),
			}
			rowGroupValues[rowIndex] = row
		}

		row.values[fieldNameType(cf.FieldName)] = value

	default:
		panic(fmt.Sprintf("unexpected scan result: %v", result))
	}

	return nil
}

func (cf *ComparativeFilter) ShouldRowGroupBeScanned(rowGroup *parquet.RowGroup) (ShouldScanResult, errorsx.Error) {
	var column *parquet.ColumnChunk

	for _, col := range rowGroup.GetColumns() {
		if cf.FieldName == strings.Join(col.MetaData.PathInSchema, ".") {
			column = col
			break
		}
	}

	if column == nil {
		// return No, since IIRC a parquet file can have columns added later(?);
		// so the column could be absent in early row groups but can be present in later row groups
		return ShouldScanResultNo, nil
	}

	switch cf.Operator {
	case ComparativeOperatorLessThan:
		colMinVal, err := bytesToOperand(column.MetaData.Statistics.MinValue, column.MetaData.Type)
		if err != nil {
			return ShouldScanResultNo, errorsx.Wrap(err)
		}

		isLess, err := colMinVal.IsLessThan(cf.Operand)
		if err != nil {
			return ShouldScanResultNo, errorsx.Wrap(err)
		}

		if isLess {
			return ShouldScanResultYes, nil
		}
		return ShouldScanResultNo, nil
	case ComparativeOperatorLessThanOrEqualTo:
		colMinVal, err := bytesToOperand(column.MetaData.Statistics.MinValue, column.MetaData.Type)
		if err != nil {
			return ShouldScanResultNo, errorsx.Wrap(err)
		}

		isLess, err := colMinVal.IsLessThanOrEqualTo(cf.Operand)
		if err != nil {
			return ShouldScanResultNo, errorsx.Wrap(err)
		}

		if isLess {
			return ShouldScanResultYes, nil
		}
		return ShouldScanResultNo, nil
	case ComparativeOperatorGreaterThan:

		colMaxVal, err := bytesToOperand(column.MetaData.Statistics.MaxValue, column.MetaData.Type)
		if err != nil {
			return ShouldScanResultNo, errorsx.Wrap(err)
		}

		isGreater, err := colMaxVal.IsGreaterThan(cf.Operand)
		if err != nil {
			return ShouldScanResultNo, errorsx.Wrap(err)
		}

		if isGreater {
			return ShouldScanResultYes, nil
		}
		return ShouldScanResultNo, nil
	case ComparativeOperatorGreaterThanOrEqualTo:

		colMaxVal, err := bytesToOperand(column.MetaData.Statistics.MaxValue, column.MetaData.Type)
		if err != nil {
			return ShouldScanResultNo, errorsx.Wrap(err)
		}

		isGreater, err := colMaxVal.IsGreaterThanOrEqualTo(cf.Operand)
		if err != nil {
			return ShouldScanResultNo, errorsx.Wrap(err)
		}

		if isGreater {
			return ShouldScanResultYes, nil
		}
		return ShouldScanResultNo, nil
	case ComparativeOperatorContains:
		colMaxVal, err := bytesToOperand(column.MetaData.Statistics.MaxValue, column.MetaData.Type)
		if err != nil {
			return ShouldScanResultNo, errorsx.Wrap(err)
		}

		colMaxLessThanOperand, err := colMaxVal.IsLessThan(cf.Operand)
		if err != nil {
			return ShouldScanResultNo, errorsx.Wrap(err)
		}

		if colMaxLessThanOperand {
			return ShouldScanResultNo, nil
		}

		colMinVal, err := bytesToOperand(column.MetaData.Statistics.MinValue, column.MetaData.Type)
		if err != nil {
			return ShouldScanResultNo, errorsx.Wrap(err)
		}

		colMinGreaterThanOperand, err := colMinVal.IsGreaterThan(cf.Operand)
		if err != nil {
			return ShouldScanResultNo, errorsx.Wrap(err)
		}

		if colMinGreaterThanOperand {
			return ShouldScanResultNo, nil
		}

		return ShouldScanResultYes, nil
	}

	return ShouldScanResultNo, errorsx.Errorf("unrecognised operator: %q", cf.Operator)
}

func (cf *ComparativeFilter) ShouldColumnBeScanned(columnMetadata *parquet.ColumnMetaData) (ShouldScanResult, errorsx.Error) {
	if cf.FieldName != strings.Join(columnMetadata.PathInSchema, ".") {
		return ShouldScanResultNotSure, nil
	}

	switch cf.Operator {
	case ComparativeOperatorLessThan:
		if cf.Operand == nil {
			return ShouldScanResultNo, errorsx.Errorf("operand is nil")
		}

		colMinVal, err := bytesToOperand(columnMetadata.Statistics.MinValue, columnMetadata.Type)
		if err != nil {
			return ShouldScanResultNo, errorsx.Wrap(err)
		}

		isLess, err := colMinVal.IsLessThan(cf.Operand)
		if err != nil {
			return ShouldScanResultNo, errorsx.Wrap(err)
		}

		if isLess {
			return ShouldScanResultYes, nil
		}
		return ShouldScanResultNo, nil

	case ComparativeOperatorLessThanOrEqualTo:
		if cf.Operand == nil {
			return ShouldScanResultNo, errorsx.Errorf("operand is nil")
		}

		colMinVal, err := bytesToOperand(columnMetadata.Statistics.MinValue, columnMetadata.Type)
		if err != nil {
			return ShouldScanResultNo, errorsx.Wrap(err)
		}

		isLess, err := colMinVal.IsLessThanOrEqualTo(cf.Operand)
		if err != nil {
			return ShouldScanResultNo, errorsx.Wrap(err)
		}

		if isLess {
			return ShouldScanResultYes, nil
		}
		return ShouldScanResultNo, nil
	case ComparativeOperatorGreaterThan:
		if cf.Operand == nil {
			return ShouldScanResultNo, errorsx.Errorf("operand is nil")
		}

		colMaxVal, err := bytesToOperand(columnMetadata.Statistics.MaxValue, columnMetadata.Type)
		if err != nil {
			return ShouldScanResultNo, errorsx.Wrap(err)
		}

		isGreater, err := colMaxVal.IsGreaterThan(cf.Operand)
		if err != nil {
			return ShouldScanResultNo, errorsx.Wrap(err)
		}

		if isGreater {
			return ShouldScanResultYes, nil
		}
		return ShouldScanResultNo, nil
	case ComparativeOperatorGreaterThanOrEqualTo:
		if cf.Operand == nil {
			return ShouldScanResultNo, errorsx.Errorf("operand is nil")
		}

		colMaxVal, err := bytesToOperand(columnMetadata.Statistics.MaxValue, columnMetadata.Type)
		if err != nil {
			return ShouldScanResultNo, errorsx.Wrap(err)
		}

		isGreater, err := colMaxVal.IsGreaterThanOrEqualTo(cf.Operand)
		if err != nil {
			return ShouldScanResultNo, errorsx.Wrap(err)
		}

		if isGreater {
			return ShouldScanResultYes, nil
		}
		return ShouldScanResultNo, nil
	}

	return ShouldScanResultNo, errorsx.Errorf("unrecognised operator: %q", cf.Operator)
}

func (cf *ComparativeFilter) ShouldFilterItemIn(fieldName string, value Operand) (ShouldScanResult, errorsx.Error) {
	if fieldName != cf.FieldName {
		return ShouldScanResultNotSure, nil
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
			return ShouldScanResultYes, nil
		}
		return ShouldScanResultNo, nil
	case ComparativeOperatorLessThanOrEqualTo:
		if cf.Operand == nil {
			return 0, errorsx.Errorf("operand is nil")
		}

		isLess, err := value.IsLessThanOrEqualTo(cf.Operand)
		if err != nil {
			return 0, errorsx.Wrap(err)
		}
		if isLess {
			return ShouldScanResultYes, nil
		}
		return ShouldScanResultNo, nil
	case ComparativeOperatorGreaterThan:
		if cf.Operand == nil {
			return 0, errorsx.Errorf("operand is nil")
		}

		isGreater, err := value.IsGreaterThan(cf.Operand)
		if err != nil {
			return 0, errorsx.Wrap(err)
		}
		if isGreater {
			return ShouldScanResultYes, nil
		}
		return ShouldScanResultNo, nil
	case ComparativeOperatorGreaterThanOrEqualTo:
		if cf.Operand == nil {
			return 0, errorsx.Errorf("operand is nil")
		}

		isGreater, err := value.IsGreaterThanOrEqualTo(cf.Operand)
		if err != nil {
			return 0, errorsx.Wrap(err)
		}
		if isGreater {
			return ShouldScanResultYes, nil
		}
		return ShouldScanResultNo, nil
	default:
		return 0, errorsx.Errorf("unsupported operator: %q", cf.Operator)
	}
}

func (cf *ComparativeFilter) BuildColumnNamesWanted() map[string]struct{} {
	return map[string]struct{}{
		cf.FieldName: {},
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

func (cf *ComparativeFilter) String() string {
	return fmt.Sprintf("%s %s %v", cf.FieldName, cf.Operator, cf.Operand)
}
