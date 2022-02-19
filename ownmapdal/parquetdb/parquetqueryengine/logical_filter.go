package parquetqueryengine

import (
	"log"
	"strings"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
)

type LogicalFilterOperator string

const (
	LogicalFilterOperatorAnd = "AND"
)

type LogicalFilter struct {
	Operator     LogicalFilterOperator
	ChildFilters []Filter
}

func (lf *LogicalFilter) Validate() errorsx.Error {
	if len(lf.ChildFilters) == 0 {
		return errorsx.Errorf("no child filters supplied")
	}

	return nil
}

func (lf *LogicalFilter) ScanRowGroup(
	rowGroup *parquet.RowGroup,
	parquetReader *reader.ParquetReader,
	rowGroupValues rowGroupValuesCollectionType,
	rootSchemaElementName string,
) errorsx.Error {
	var subRowGroupValuesList []rowGroupValuesCollectionType
	for _, childFilter := range lf.ChildFilters {

		subRowGroupValues := make(rowGroupValuesCollectionType)
		err := childFilter.ScanRowGroup(rowGroup, parquetReader, subRowGroupValues, rootSchemaElementName)
		if err != nil {
			return err
		}

		subRowGroupValuesList = append(subRowGroupValuesList, subRowGroupValues)
	}

	switch lf.Operator {
	case LogicalFilterOperatorAnd:
		firstSubRowGroupValuesList := subRowGroupValuesList[0]
		otherRowGroupValuesList := subRowGroupValuesList[1:]
		for i, val := range firstSubRowGroupValuesList {
			var skipRow bool
			for _, otherRowGroup := range otherRowGroupValuesList {
				otherValWrapper, ok := otherRowGroup[i]
				if !ok {
					// not in other row group, so row doesn't satisfy AND operator
					skipRow = true
					break
				}

				for fieldName, otherVal := range otherValWrapper.values {
					val.values[fieldName] = otherVal
				}
			}

			if skipRow {
				continue
			}

			rowGroupValues[i] = val
		}
	default:
		return errorsx.Errorf("operator %q not supported", lf.Operator)
	}

	return nil
}

func (lf *LogicalFilter) BuildColumnNamesWanted() map[string]struct{} {
	wantedMap := make(map[string]struct{})
	for _, subFilter := range lf.ChildFilters {
		wantedSubMap := subFilter.BuildColumnNamesWanted()
		for wantedSubCol := range wantedSubMap {
			wantedMap[wantedSubCol] = struct{}{}
		}
	}
	return wantedMap
}

func (lf *LogicalFilter) ShouldRowGroupBeScanned(rowGroup *parquet.RowGroup) (ShouldScanResult, errorsx.Error) {
	switch lf.Operator {
	case LogicalFilterOperatorAnd:
		var shouldScanColumn ShouldScanResult = ShouldScanResultNotSure
		for _, childFilter := range lf.ChildFilters {
			childFilterScanResult, err := childFilter.ShouldRowGroupBeScanned(rowGroup)
			if err != nil {
				return 0, errorsx.Wrap(err)
			}

			switch childFilterScanResult {
			case ShouldScanResultNo:
				return ShouldScanResultNo, nil
			case ShouldScanResultYes:
				// mark column as wanted, but don't exit yet (wait to see the result of the other child filters)
				shouldScanColumn = ShouldScanResultYes
			case ShouldScanResultNotSure:
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

func (lf *LogicalFilter) ShouldColumnBeScanned(columnMetadata *parquet.ColumnMetaData) (ShouldScanResult, errorsx.Error) {
	switch lf.Operator {
	case LogicalFilterOperatorAnd:
		if len(lf.ChildFilters) == 0 {
			return 0, errorsx.Errorf("no child filters supplied")
		}

		var shouldScanColumn ShouldScanResult = ShouldScanResultNotSure
		for _, childFilter := range lf.ChildFilters {
			childFilterScanResult, err := childFilter.ShouldColumnBeScanned(columnMetadata)
			if err != nil {
				return 0, errorsx.Wrap(err)
			}

			log.Printf("col: %q, cfsr: %s (cf: %#v)\n", strings.Join(columnMetadata.PathInSchema, "."), childFilterScanResult, childFilter)

			switch childFilterScanResult {
			case ShouldScanResultNo:
				return ShouldScanResultNo, nil
			case ShouldScanResultYes:
				// mark column as wanted, but don't exit yet (wait to see the result of the other child filters)
				shouldScanColumn = ShouldScanResultYes
			case ShouldScanResultNotSure:
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

func (lf *LogicalFilter) ShouldFilterItemIn(fieldName string, value Operand) (ShouldScanResult, errorsx.Error) {
	switch lf.Operator {
	case LogicalFilterOperatorAnd:
		if len(lf.ChildFilters) == 0 {
			return 0, errorsx.Errorf("no child filters supplied")
		}

		var shouldScanColumn ShouldScanResult = ShouldScanResultNotSure

		for _, childFilter := range lf.ChildFilters {
			shouldFilterResult, err := childFilter.ShouldFilterItemIn(fieldName, value)
			if err != nil {
				return 0, err
			}
			switch shouldFilterResult {
			case ShouldScanResultNo:
				return ShouldScanResultNo, nil
			case ShouldScanResultYes:
				// mark column as wanted, but don't exit yet (wait to see the result of the other child filters)
				shouldScanColumn = ShouldScanResultYes
			case ShouldScanResultNotSure:
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