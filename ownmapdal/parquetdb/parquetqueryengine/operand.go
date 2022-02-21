package parquetqueryengine

import (
	"github.com/jamesrr39/goutil/errorsx"
)

type Operand interface {
	IsGreaterThan(val Operand) (bool, errorsx.Error)
	IsLessThan(val Operand) (bool, errorsx.Error)
	IsGreaterThanOrEqualTo(val Operand) (bool, errorsx.Error)
	IsLessThanOrEqualTo(val Operand) (bool, errorsx.Error)
	EqualTo(val Operand) (bool, errorsx.Error)
}

type Float64Operand float64

func (f Float64Operand) IsGreaterThan(val Operand) (bool, errorsx.Error) {
	return float64(f) > float64(val.(Float64Operand)), nil
}

func (f Float64Operand) IsLessThan(val Operand) (bool, errorsx.Error) {
	return float64(f) < float64(val.(Float64Operand)), nil
}

func (f Float64Operand) IsGreaterThanOrEqualTo(val Operand) (bool, errorsx.Error) {
	return float64(f) >= float64(val.(Float64Operand)), nil
}

func (f Float64Operand) IsLessThanOrEqualTo(val Operand) (bool, errorsx.Error) {
	return float64(f) <= float64(val.(Float64Operand)), nil
}

func (f Float64Operand) EqualTo(val Operand) (bool, errorsx.Error) {
	return float64(f) == float64(val.(Float64Operand)), nil
}

type Int64Operand int64

func (f Int64Operand) IsGreaterThan(val Operand) (bool, errorsx.Error) {
	return int64(f) > int64(val.(Int64Operand)), nil
}
func (f Int64Operand) IsLessThan(val Operand) (bool, errorsx.Error) {
	return int64(f) < int64(val.(Int64Operand)), nil
}

func (f Int64Operand) IsGreaterThanOrEqualTo(val Operand) (bool, errorsx.Error) {
	return int64(f) >= int64(val.(Int64Operand)), nil
}
func (f Int64Operand) IsLessThanOrEqualTo(val Operand) (bool, errorsx.Error) {
	return int64(f) <= int64(val.(Int64Operand)), nil
}

func (f Int64Operand) EqualTo(val Operand) (bool, errorsx.Error) {
	return int64(f) == int64(val.(Int64Operand)), nil
}

type StringOperand string

func (f StringOperand) IsGreaterThan(val Operand) (bool, errorsx.Error) {
	return false, errorsx.Errorf("string operand is not not suitable for this operator")
}
func (f StringOperand) IsLessThan(val Operand) (bool, errorsx.Error) {
	return false, errorsx.Errorf("string operand is not not suitable for this operator")
}

func (f StringOperand) IsGreaterThanOrEqualTo(val Operand) (bool, errorsx.Error) {
	return false, errorsx.Errorf("string operand is not not suitable for this operator")
}
func (f StringOperand) IsLessThanOrEqualTo(val Operand) (bool, errorsx.Error) {
	return false, errorsx.Errorf("string operand is not not suitable for this operator")
}

func (f StringOperand) EqualTo(val Operand) (bool, errorsx.Error) {
	return string(f) == string(val.(StringOperand)), nil
}
