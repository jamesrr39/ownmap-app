package parquetqueryengine

import (
	"fmt"
	"log"
	"strings"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/paulmach/osm"
	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/parquet"
)

type ParquetReader interface {
	ReadColumnByPath(path string, numRows int64) (values []interface{}, repetitionLevels []int32, definitionLevels []int32, err error)
	GetNumRows() int64
}

type Query struct {
	Select []string
	// From   ParquetReader
	Where Filter
	// Limit int32 // 0 = no limit. Int32 because a Go slice can only be int32 items long
}

type fieldNameType string

type rowMapType struct {
	values map[fieldNameType]interface{}
}

type rowGroupValuesCollectionType map[int]*rowMapType

type valueType interface{}

type queryRunnerType struct {
	columnsScannedMap map[string][]valueType
	// rowGroupValues    rowGroupValuesCollectionType
}

func newQueryRunnerType() *queryRunnerType {
	return &queryRunnerType{
		columnsScannedMap: make(map[string][]valueType),
		// rowGroupValues:    make(rowGroupValuesCollectionType),
	}
}

func newRowMapType() *rowMapType {
	return &rowMapType{
		values: make(map[fieldNameType]interface{}),
	}
}

type ResultRow []interface{}

func (r *ResultRow) GoString() string {
	var fragments []string
	for _, item := range *r {
		fragments = append(fragments, fmt.Sprintf("%#v", item))
	}

	return fmt.Sprintf("{%s}", strings.Join(fragments, ", "))
}

func (q *Query) isRowGroupInteresting(rowGroup *parquet.RowGroup) (bool, errorsx.Error) {
	if q.Where == nil {
		return true, nil
	}

	scanResult, err := q.Where.ShouldRowGroupBeScanned(rowGroup)
	if err != nil {
		return false, err
	}

	switch scanResult {
	case ShouldScanResultYes:
		return true, nil
	case ShouldScanResultNo, ShouldScanResultNotSure:
		return false, nil
	default:
		return false, errorsx.Errorf("unknown scan result: %v", scanResult)
	}
}

func (q *Query) Run(parquetReader ParquetReader, rowGroups []*parquet.RowGroup, rootSchemaElementName string) ([]*ResultRow, errorsx.Error) {
	fieldNamesWantedForWhereClause := make(map[string]struct{})
	comparativeFilters := q.Where.GetComparativeFilters()
	for _, comparativeFilter := range comparativeFilters {
		fieldNamesWantedForWhereClause[comparativeFilter.FieldName] = struct{}{}
	}

	results := []*ResultRow{}

	// first evaluate rows IDs for scanning by WHERE clause
	for i, rowGroup := range rowGroups {
		log.Printf("evaluating rowGroup ID: %d :: num rows: %d\n", i, rowGroup.NumRows)

		// 1. are we interested in this row group?
		isInteresting, err := q.isRowGroupInteresting(rowGroup)
		if err != nil {
			return nil, err
		}

		if !isInteresting {
			println("row group not interesting")
			continue
		}

		rowGroupValues := make(rowGroupValuesCollectionType)

		if q.Where == nil {
			// get all values from row group
			for i := 0; i < int(rowGroup.GetNumRows()); i++ {
				rowGroupValues[i] = newRowMapType()
			}
		}

		rowGroupBounds, err := debug__BoundsForRowGroup(rowGroup)
		if err != nil {
			return nil, err
		}
		log.Printf("filter:\nWHERE: %s\nrow group bounds: %#v\n", q.Where, rowGroupBounds)

		// 2. scan interesting rows
		queryRunner := newQueryRunnerType()
		err = q.Where.ScanRowGroup(rowGroup, parquetReader, queryRunner, rowGroupValues, rootSchemaElementName)
		if err != nil {
			return nil, err
		}

		log.Printf("i: %d, rowGroupValues len: %d\n", i, len(rowGroupValues))
		for rowID, rowMap := range rowGroupValues {
			log.Printf("rowID: %d, values: %#v\n", rowID, rowMap.values)
		}

		// 3. now scan SELECT columns
		whereColMap := q.Where.BuildColumnNamesWanted()
		for _, selectCol := range q.Select {
			_, ok := whereColMap[selectCol]
			if ok {
				// column has already been scanned in WHERE clause
				continue
			}

			err = addColumnValsToRowGroupValues(parquetReader, rowGroup, rowGroupValues, selectCol, rootSchemaElementName)
			if err != nil {
				return nil, err
			}
		}

		rowGroupResults := rowGroupValuesToResults(rowGroupValues, q.Select)

		results = append(results, rowGroupResults...)
	}

	log.Printf("row groups: %d, results len: %d\n", len(rowGroups), len(results))
	// parquetReader.ReadColumnByPath()

	return results, nil
}

func rowGroupValuesToResults(rowGroupValues rowGroupValuesCollectionType, selectCols []string) []*ResultRow {
	var results []*ResultRow
	for _, valSet := range rowGroupValues {
		var resultRow ResultRow

		for _, selectCol := range selectCols {
			resultRow = append(resultRow, valSet.values[fieldNameType(selectCol)])
		}

		results = append(results, &resultRow)
	}
	return results
}

// func fetchOtherValues(
// 	valuesToFetchMap map[fieldNameType][]int,
// 	rowIDsToFetchInRowGroup map[int]*rowMapType,
// 	parquetReader ParquetReader,
// 	rowGroup *parquet.RowGroup,
// 	rootSchemaElementName string,
// ) errorsx.Error {
// 	for _, column := range rowGroup.Columns {
// 		_, ok := valuesToFetchMap[fieldNameType(strings.Join(column.MetaData.PathInSchema, "."))]
// 		if !ok {
// 			// column not needed
// 			continue
// 		}
// 		fullFieldName := getFullParquetFieldPath(column, rootSchemaElementName)
// 		// TODO read only wanted partions of columns
// 		values, repetionLevels, definitionLevels, err := parquetReader.ReadColumnByPath(fullFieldName, column.MetaData.NumValues)
// 		if err != nil {
// 			return errorsx.Wrap(err)
// 		}

// 		println("definitionLevels", definitionLevels)

// 		for i, value := range values {
// 			repetionLevel := repetionLevels[i]
// 			if repetionLevel != 0 {
// 				panic("not handled repetition levels non-0")
// 			}

// 			rowMap, ok := rowIDsToFetchInRowGroup[i]
// 			if !ok {
// 				// not wanted
// 				continue
// 			}

// 			rowMap.values[fieldNameType(strings.Join(column.MetaData.PathInSchema, "."))] = value
// 		}
// 	}

// 	return nil
// }

func addColumnValsToRowGroupValues(parquetReader ParquetReader, rowGroup *parquet.RowGroup, rowGroupValues rowGroupValuesCollectionType, selectCol, rootSchemaElementName string) errorsx.Error {
	fullPath := common.PathToStr([]string{rootSchemaElementName, selectCol})
	// fullPath := strings.Join([]string{rootSchemaElementName, selectCol}, ".")
	// fullPath := selectCol

	// log.Printf("mapIndex: %#v\n", parquetReader.SchemaHandler.MapIndex)

	vals, repetionLevels, definitionLevels, err := parquetReader.ReadColumnByPath(fullPath, rowGroup.GetNumRows())
	if err != nil {
		return errorsx.Wrap(err)
	}

	for i, val := range vals {
		if repetionLevels[i] != 0 {
			panic("not handled: repetionLevels")
		}
		if definitionLevels[i] != 0 {
			panic("not handled: definitionLevels")
		}

		existingVals, wanted := rowGroupValues[i]
		if !wanted {
			// TODO skip row instead of reading all
			continue
		}

		existingVals.values[fieldNameType(selectCol)] = val
	}

	return nil
}

func getFullParquetFieldPath(column *parquet.ColumnChunk, rootSchemaElementName string) string {
	if rootSchemaElementName == "" {
		return strings.Join(column.GetMetaData().PathInSchema, ".")
	}

	return common.PathToStr(
		append(
			[]string{rootSchemaElementName},
			column.GetMetaData().PathInSchema...,
		),
	)
}

func debug__BoundsForRowGroup(rowGroup *parquet.RowGroup) (osm.Bounds, errorsx.Error) {
	var bounds osm.Bounds

	for _, column := range rowGroup.GetColumns() {
		columnNameText := strings.Join(column.MetaData.PathInSchema, ".")
		switch columnNameText {
		case "Lat":
			maxLat, err := bytesToOperand(column.MetaData.Statistics.MaxValue, column.MetaData.Type)
			if err != nil {
				return bounds, errorsx.Wrap(err)
			}

			bounds.MaxLat = float64(maxLat.(Float64Operand))

			minLat, err := bytesToOperand(column.MetaData.Statistics.MinValue, column.MetaData.Type)
			if err != nil {
				return bounds, errorsx.Wrap(err)
			}

			bounds.MinLat = float64(minLat.(Float64Operand))
		case "Lon":
			maxLon, err := bytesToOperand(column.MetaData.Statistics.MaxValue, column.MetaData.Type)
			if err != nil {
				return bounds, errorsx.Wrap(err)
			}

			bounds.MaxLon = float64(maxLon.(Float64Operand))

			minLon, err := bytesToOperand(column.MetaData.Statistics.MinValue, column.MetaData.Type)
			if err != nil {
				return bounds, errorsx.Wrap(err)
			}

			bounds.MinLon = float64(minLon.(Float64Operand))
		default:
			log.Printf("not using column: %s\n", columnNameText)
		}
	}

	return bounds, nil
}
