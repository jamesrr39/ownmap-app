package parquetqueryengine

import (
	"github.com/jamesrr39/goutil/errorsx"
)

type Operand interface {
	IsGreaterThan(val Operand) (bool, errorsx.Error)
	IsLessThan(val Operand) (bool, errorsx.Error)
}

type Float64Operand float64

func (f Float64Operand) IsGreaterThan(val Operand) (bool, errorsx.Error) {
	return float64(f) > float64(val.(Float64Operand)), nil
}

func (f Float64Operand) IsLessThan(val Operand) (bool, errorsx.Error) {
	return float64(f) < float64(val.(Float64Operand)), nil
}

type Int64Operand int64

func (f Int64Operand) IsGreaterThan(val Operand) (bool, errorsx.Error) {
	return int64(f) > int64(val.(Int64Operand)), nil
}
func (f Int64Operand) IsLessThan(val Operand) (bool, errorsx.Error) {
	return int64(f) < int64(val.(Int64Operand)), nil
}
