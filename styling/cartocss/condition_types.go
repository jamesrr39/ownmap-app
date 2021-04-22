package styling

type conditionType int

const (
	conditionTypeLogical conditionType = iota
	conditionTypeComparator
)

type condition interface {
}

type logicalOperator int

const (
	logicalOperatorOr logicalOperator = iota
	logicalOperatorAnd
)

type logicalCondition struct {
	Left     condition
	Right    condition
	Operator logicalOperator
}

type comparatorOperator int

const (
	comparatorOperatorEquals comparatorOperator = iota
	comparatorOperatorLessThanOrEqualTo
	comparatorOperatorLessThan
	comparatorOperatorGreaterThanOrEqualTo
	comparatorOperatorGreaterThan
)

type category int

const (
	categoryZoomLevel category = iota
	categoryAttribute
)

type stringComparatorCondition struct {
	Category category
	Thing    string
	Operator comparatorOperator
	Value    string
}

type numericComparatorCondition struct {
	Thing    string
	Operator comparatorOperator
	Value    int
}
