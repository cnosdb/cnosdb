package storage

import (
	"github.com/cnosdb/cnosdb/vend/cnosql"
	"github.com/cnosdb/cnosdb/vend/db/models"
)

var measurementRemap = map[string]string{
	"_measurement":           "_name",
	models.MeasurementTagKey: "_name",
	models.FieldKeyTagKey:    "_field",
}

func RewriteExprRemoveFieldKeyAndValue(expr cnosql.Expr) cnosql.Expr {
	return cnosql.RewriteExpr(expr, func(expr cnosql.Expr) cnosql.Expr {
		if be, ok := expr.(*cnosql.BinaryExpr); ok {
			if ref, ok := be.LHS.(*cnosql.VarRef); ok {
				if ref.Val == "_field" || ref.Val == "$" {
					return &cnosql.BooleanLiteral{Val: true}
				}
			}
		}

		return expr
	})
}

// HasSingleMeasurementNoOR determines if an index optimisation is available.
//
// Typically the read service will use the query engine to retrieve all field
// keys for all measurements that match the expression, which can be very
// inefficient if it can be proved that only one measurement matches the expression.
//
// This condition is determined when the following is true:
//
//		* there is only one occurrence of the tag key `_measurement`.
//		* there are no OR operators in the expression tree.
//		* the operator for the `_measurement` binary expression is ==.
//
func HasSingleMeasurementNoOR(expr cnosql.Expr) (string, bool) {
	var lastMeasurement string
	foundOnce := true
	var invalidOP bool

	cnosql.WalkFunc(expr, func(node cnosql.Node) {
		if !foundOnce || invalidOP {
			return
		}

		if be, ok := node.(*cnosql.BinaryExpr); ok {
			if be.Op == cnosql.OR {
				invalidOP = true
				return
			}

			if ref, ok := be.LHS.(*cnosql.VarRef); ok {
				if ref.Val == measurementRemap[measurementKey] {
					if be.Op != cnosql.EQ {
						invalidOP = true
						return
					}

					if lastMeasurement != "" {
						foundOnce = false
					}

					// Check that RHS is a literal string
					if ref, ok := be.RHS.(*cnosql.StringLiteral); ok {
						lastMeasurement = ref.Val
					}
				}
			}
		}
	})
	return lastMeasurement, len(lastMeasurement) > 0 && foundOnce && !invalidOP
}

type hasRefs struct {
	refs  []string
	found []bool
}

func (v *hasRefs) allFound() bool {
	for _, val := range v.found {
		if !val {
			return false
		}
	}
	return true
}

func (v *hasRefs) Visit(node cnosql.Node) cnosql.Visitor {
	if v.allFound() {
		return nil
	}

	if n, ok := node.(*cnosql.VarRef); ok {
		for i, r := range v.refs {
			if !v.found[i] && r == n.Val {
				v.found[i] = true
				if v.allFound() {
					return nil
				}
			}
		}
	}
	return v
}

func HasFieldKeyOrValue(expr cnosql.Expr) (bool, bool) {
	refs := hasRefs{refs: []string{fieldKey, "$"}, found: make([]bool, 2)}
	cnosql.Walk(&refs, expr)
	return refs.found[0], refs.found[1]
}
