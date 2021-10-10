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
	Limit int32 // 0 = no limit. Int32 because a Go slice can only be int32 items long
}

type fieldNameType string

type rowMap struct {
	values map[fieldNameType]interface{}
}

type ResultRow []interface{}

func (q *Query) Run(parquetReader *reader.ParquetReader, rootSchemaElementName string) ([]*ResultRow, errorsx.Error) {
	fieldNamesWantedForWhereClause := make(map[string]struct{})
	for _, fieldName := range q.Where.GetFieldNamesToScan() {
		fieldNamesWantedForWhereClause[fieldName] = struct{}{}
	}

	var debugAddedCount int

	// first evaluate rows IDs for scanning by WHERE clause
	for _, rowGroup := range parquetReader.Footer.GetRowGroups() {

		rowIDsToFetchInRowGroup := make(map[int]*rowMap)

		for _, column := range rowGroup.GetColumns() {
			fieldName := strings.Join(column.GetMetaData().PathInSchema, ".")

			_, ok := fieldNamesWantedForWhereClause[fieldName]
			if !ok {
				println("column not wanted for where clause filter: ", strings.Join(column.GetMetaData().PathInSchema, "."))
				continue
			}
			println("scanning column:", strings.Join(column.MetaData.PathInSchema, "."), column.MetaData.NumValues)

			if q.Where != nil {
				shouldScanForWhereClause, err := q.Where.ShouldColumnBeScanned(column.GetMetaData())
				if err != nil {
					return nil, errorsx.Wrap(err)
				}

				if !shouldScanForWhereClause {
					log.Println("skipping scanning column; where clause conditions not met")
					continue
				}
			}

			columnFullPath := getFullParquetFieldPath(column, rootSchemaElementName)

			// TODO skip rows?
			values, repetionLevels, definitionLevels, err := parquetReader.ReadColumnByPath(columnFullPath, column.MetaData.NumValues)
			if err != nil {
				return nil, errorsx.Wrap(err)
			}

			println(definitionLevels, repetionLevels)

			for i, value := range values {
				// if value
				// switch column.MetaData.Type {
				// case parquet.Type_INT64:
				// case parquet.Type_DOUBLE:
				switch val := value.(type) {
				case float64:
					shouldFilterIn, err := q.Where.ShouldFilterItemIn(getFullParquetFieldPath(column, ""), Float64Operand(val))
					if err != nil {
						return nil, errorsx.Wrap(err)
					}

					if !shouldFilterIn {
						continue
					}

					println("adding item float64::", val)
					item, ok := rowIDsToFetchInRowGroup[i]
					if !ok {
						item = &rowMap{}
						rowIDsToFetchInRowGroup[i] = item
					}

					item.values[fieldNameType(fieldName)] = val
				case int64:
					shouldFilterIn, err := q.Where.ShouldFilterItemIn(getFullParquetFieldPath(column, ""), Int64Operand(val))
					if err != nil {
						return nil, errorsx.Wrap(err)
					}

					if !shouldFilterIn {
						continue
					}

					println("adding item int64::", val, "added count::", debugAddedCount)
					item, ok := rowIDsToFetchInRowGroup[i]
					if !ok {
						item = &rowMap{
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

		println("row IDs to fetch len:", len(rowIDsToFetchInRowGroup))
		for _, item := range rowIDsToFetchInRowGroup {
			fmt.Printf("%#v\n", item.values)
		}
	}

	// then scan columns requested

	// use LIMIT field to stop scanning early if provided

	panic("not implemented")
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
