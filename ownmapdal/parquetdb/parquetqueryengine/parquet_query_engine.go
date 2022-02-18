package parquetqueryengine

import (
	"fmt"
	"log"
	"strings"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
)

type Query struct {
	Select []string
	// From   *reader.ParquetReader
	Where Filter
	// Limit int32 // 0 = no limit. Int32 because a Go slice can only be int32 items long
}

type fieldNameType string

type rowMapType struct {
	values map[fieldNameType]interface{}
}

func newRowMapType() *rowMapType {
	return &rowMapType{
		values: make(map[fieldNameType]interface{}),
	}
}

type rowGroupValuesCollectionType map[int]*rowMapType

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

func (q *Query) Run(parquetReader *reader.ParquetReader, rootSchemaElementName string) ([]*ResultRow, errorsx.Error) {
	fieldNamesWantedForWhereClause := make(map[string]struct{})
	comparativeFilters := q.Where.GetComparativeFilters()
	for _, comparativeFilter := range comparativeFilters {
		fieldNamesWantedForWhereClause[comparativeFilter.FieldName] = struct{}{}
	}

	// results := []*ResultRow{}

	// first evaluate rows IDs for scanning by WHERE clause
	for i, rowGroup := range parquetReader.Footer.GetRowGroups() {
		log.Printf("evaluating rowGroup ID: %d :: num rows: %d\n", i, rowGroup.NumRows)

		// 1. are we interested in this row group?
		isInteresting, err := q.isRowGroupInteresting(rowGroup)
		if err != nil {
			return nil, err
		}

		if !isInteresting {
			continue
		}

		rowGroupValues := make(rowGroupValuesCollectionType)

		// 2. scan interesting rows
		err = q.Where.ScanRowGroup(rowGroup, parquetReader, rowGroupValues, rootSchemaElementName)
		if err != nil {
			return nil, err
		}

		for rowID, rowMap := range rowGroupValues {
			log.Printf("rowID: %d, values: %#v\n", rowID, rowMap.values)
		}
	}
	// parquetReader.ReadColumnByPath()

	panic("TODO")
	/*
			rowIDsToFetchInRowGroup := make(map[int]*rowMapType)

			for _, column := range rowGroup.GetColumns() {
				fieldName := strings.Join(column.GetMetaData().PathInSchema, ".")

				_, ok := fieldNamesWantedForWhereClause[fieldName]
				if !ok {
					// column not wanted for where clause filter
					continue
				}

				// min, err := encoding.ReadPlain(bytes.NewReader(column.MetaData.Statistics.GetMinValue()), column.MetaData.Type, 1, 8)
				// if err != nil {
				// 	return nil, errorsx.Wrap(err)
				// }

				// max, err := encoding.ReadPlain(bytes.NewReader(column.MetaData.Statistics.GetMaxValue()), column.MetaData.Type, 1, 8)
				// if err != nil {
				// 	return nil, errorsx.Wrap(err)
				// }

				// log.Printf("evaluating column %q. (num values: %d). Min/Max: %v, %v\n",
				// 	strings.Join(column.MetaData.PathInSchema, "."),
				// 	column.MetaData.NumValues,
				// 	min, max,
				// )

				if q.Where != nil {
					shouldScanForWhereClause, err := q.Where.ShouldColumnBeScanned(column.GetMetaData())
					if err != nil {
						return nil, errorsx.Wrap(err)
					}

					switch shouldScanForWhereClause {
					case ShouldScanResultYes:
						// read this column!
					case ShouldScanResultNo, ShouldScanResultNotSure:
						log.Printf("not scanning column %q; where clause conditions not met", fieldName)
						continue
					default:
						return nil, errorsx.Errorf("unknown ColumnScanResult: %q", shouldScanForWhereClause)
					}
				}

				columnFullPath := getFullParquetFieldPath(column, rootSchemaElementName)

				// TODO skip rows?
				values, repetionLevels, definitionLevels, err := parquetReader.ReadColumnByPath(columnFullPath, column.MetaData.NumValues)
				if err != nil {
					return nil, errorsx.Wrap(err)
				}

				println("dls, rps:", definitionLevels, repetionLevels)

				for i, value := range values {
					switch val := value.(type) {
					case float64:
						shouldFilterInResult, err := q.Where.ShouldFilterItemIn(getFullParquetFieldPath(column, ""), Float64Operand(val))
						if err != nil {
							return nil, errorsx.Wrap(err)
						}

						switch shouldFilterInResult {
						case ShouldScanResultYes:
							// continue with adding this to row IDs to fetch map
						case ShouldScanResultNo:
							continue
						default:
							return nil, errorsx.Errorf("unexpected ColumnScanResult: %d", shouldFilterInResult)
						}

						item, ok := rowIDsToFetchInRowGroup[i]
						if !ok {
							item = &rowMapType{
								values: make(map[fieldNameType]interface{}),
							}
							rowIDsToFetchInRowGroup[i] = item
						}

						item.values[fieldNameType(fieldName)] = val
					case int64:
						shouldFilterInResult, err := q.Where.ShouldFilterItemIn(getFullParquetFieldPath(column, ""), Int64Operand(val))
						if err != nil {
							return nil, errorsx.Wrap(err)
						}
						switch shouldFilterInResult {
						case ShouldScanResultYes:
							// continue with adding this to row IDs to fetch map
						case ShouldScanResultNo:
							continue
						default:
							return nil, errorsx.Errorf("unexpected ColumnScanResult: %d", shouldFilterInResult)
						}

						item, ok := rowIDsToFetchInRowGroup[i]
						if !ok {
							item = &rowMapType{
								values: make(map[fieldNameType]interface{}),
							}
							rowIDsToFetchInRowGroup[i] = item
						}

						item.values[fieldNameType(fieldName)] = val
					default:
						panic("unhandled type: " + column.MetaData.Type.String())
					}

					// value

				}
			}

			log.Printf("wanted: %#v\n", rowIDsToFetchInRowGroup)

			if len(rowIDsToFetchInRowGroup) == 0 {
				// nothing to fetch here
				continue
			}

			// fieldname, row ID
			valuesToFetchMap := make(map[fieldNameType][]int)

			for rowID, item := range rowIDsToFetchInRowGroup {
				for _, wantedFieldName := range q.Select {
					_, ok := item.values[fieldNameType(wantedFieldName)]
					if ok {
						// value has already been fetched, no need to refetch it
						continue
					}
					fmt.Printf("row ID in row group: %d :: field name: %q:: values: %#v\n", rowID, wantedFieldName, item.values)
					valuesToFetchMap[fieldNameType(wantedFieldName)] = append(valuesToFetchMap[fieldNameType(wantedFieldName)], rowID)
				}
			}

			err = fetchOtherValues(valuesToFetchMap, rowIDsToFetchInRowGroup, parquetReader, rowGroup, rootSchemaElementName)
			if err != nil {
				return nil, errorsx.Wrap(err)
			}

			for _, item := range rowIDsToFetchInRowGroup {
				resultRow := ResultRow{}

				for _, selectField := range q.Select {
					log.Printf("values:: %#v\n", item.values)
					value, ok := item.values[fieldNameType(selectField)]
					if !ok {
						panic("implementation error: field not fetched: " + selectField)
					}
					resultRow = append(resultRow, value)
				}

				log.Printf("adding row: %#v\n", resultRow)

				results = append(results, &resultRow)
				fmt.Printf("row ID after fetch in row group: values: %#v\n", item.values)
			}

		}

		return results, nil
	*/
}

func fetchOtherValues(
	valuesToFetchMap map[fieldNameType][]int,
	rowIDsToFetchInRowGroup map[int]*rowMapType,
	parquetReader *reader.ParquetReader,
	rowGroup *parquet.RowGroup,
	rootSchemaElementName string,
) errorsx.Error {
	for _, column := range rowGroup.Columns {
		_, ok := valuesToFetchMap[fieldNameType(strings.Join(column.MetaData.PathInSchema, "."))]
		if !ok {
			// column not needed
			continue
		}
		fullFieldName := getFullParquetFieldPath(column, rootSchemaElementName)
		// TODO read only wanted partions of columns
		values, repetionLevels, definitionLevels, err := parquetReader.ReadColumnByPath(fullFieldName, column.MetaData.NumValues)
		if err != nil {
			return errorsx.Wrap(err)
		}

		println("definitionLevels", definitionLevels)

		for i, value := range values {
			repetionLevel := repetionLevels[i]
			if repetionLevel != 0 {
				panic("not handled repetition levels non-0")
			}

			rowMap, ok := rowIDsToFetchInRowGroup[i]
			if !ok {
				// not wanted
				continue
			}

			rowMap.values[fieldNameType(strings.Join(column.MetaData.PathInSchema, "."))] = value
		}
	}

	return nil
}

func getFullParquetFieldPath(column *parquet.ColumnChunk, rootSchemaElementName string) string {
	if rootSchemaElementName == "" {
		return strings.Join(column.GetMetaData().PathInSchema, ".")
	}

	return strings.Join(
		append(
			[]string{rootSchemaElementName},
			column.GetMetaData().PathInSchema...,
		),
		".",
	)
}
