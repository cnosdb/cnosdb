package reads

import (
	"math"
	"regexp"

	"github.com/cnosdb/cnosdb/vend/cnosql"
)

// evalExpr evaluates expr against a map.
func evalExpr(expr cnosql.Expr, m Valuer) interface{} {
	if expr == nil {
		return nil
	}

	switch expr := expr.(type) {
	case *cnosql.BinaryExpr:
		return evalBinaryExpr(expr, m)
	case *cnosql.BooleanLiteral:
		return expr.Val
	case *cnosql.IntegerLiteral:
		return expr.Val
	case *cnosql.UnsignedLiteral:
		return expr.Val
	case *cnosql.NumberLiteral:
		return expr.Val
	case *cnosql.ParenExpr:
		return evalExpr(expr.Expr, m)
	case *cnosql.RegexLiteral:
		return expr.Val
	case *cnosql.StringLiteral:
		return expr.Val
	case *cnosql.VarRef:
		v, _ := m.Value(expr.Val)
		return v
	default:
		return nil
	}
}

func evalBinaryExpr(expr *cnosql.BinaryExpr, m Valuer) interface{} {
	lhs := evalExpr(expr.LHS, m)
	rhs := evalExpr(expr.RHS, m)
	if lhs == nil && rhs != nil {
		// When the LHS is nil and the RHS is a boolean, implicitly cast the
		// nil to false.
		if _, ok := rhs.(bool); ok {
			lhs = false
		}
	} else if lhs != nil && rhs == nil {
		// Implicit cast of the RHS nil to false when the LHS is a boolean.
		if _, ok := lhs.(bool); ok {
			rhs = false
		}
	}

	// Evaluate if both sides are simple types.
	switch lhs := lhs.(type) {
	case bool:
		rhs, ok := rhs.(bool)
		switch expr.Op {
		case cnosql.AND:
			return ok && (lhs && rhs)
		case cnosql.OR:
			return ok && (lhs || rhs)
		case cnosql.BITWISE_AND:
			return ok && (lhs && rhs)
		case cnosql.BITWISE_OR:
			return ok && (lhs || rhs)
		case cnosql.BITWISE_XOR:
			return ok && (lhs != rhs)
		case cnosql.EQ:
			return ok && (lhs == rhs)
		case cnosql.NEQ:
			return ok && (lhs != rhs)
		}
	case float64:
		// Try the rhs as a float64 or int64
		rhsf, ok := rhs.(float64)
		if !ok {
			var rhsi int64
			if rhsi, ok = rhs.(int64); ok {
				rhsf = float64(rhsi)
			}
		}

		rhs := rhsf
		switch expr.Op {
		case cnosql.EQ:
			return ok && (lhs == rhs)
		case cnosql.NEQ:
			return ok && (lhs != rhs)
		case cnosql.LT:
			return ok && (lhs < rhs)
		case cnosql.LTE:
			return ok && (lhs <= rhs)
		case cnosql.GT:
			return ok && (lhs > rhs)
		case cnosql.GTE:
			return ok && (lhs >= rhs)
		case cnosql.ADD:
			if !ok {
				return nil
			}
			return lhs + rhs
		case cnosql.SUB:
			if !ok {
				return nil
			}
			return lhs - rhs
		case cnosql.MUL:
			if !ok {
				return nil
			}
			return lhs * rhs
		case cnosql.DIV:
			if !ok {
				return nil
			} else if rhs == 0 {
				return float64(0)
			}
			return lhs / rhs
		case cnosql.MOD:
			if !ok {
				return nil
			}
			return math.Mod(lhs, rhs)
		}
	case int64:
		// Try as a float64 to see if a float cast is required.
		rhsf, ok := rhs.(float64)
		if ok {
			lhs := float64(lhs)
			rhs := rhsf
			switch expr.Op {
			case cnosql.EQ:
				return lhs == rhs
			case cnosql.NEQ:
				return lhs != rhs
			case cnosql.LT:
				return lhs < rhs
			case cnosql.LTE:
				return lhs <= rhs
			case cnosql.GT:
				return lhs > rhs
			case cnosql.GTE:
				return lhs >= rhs
			case cnosql.ADD:
				return lhs + rhs
			case cnosql.SUB:
				return lhs - rhs
			case cnosql.MUL:
				return lhs * rhs
			case cnosql.DIV:
				if rhs == 0 {
					return float64(0)
				}
				return lhs / rhs
			case cnosql.MOD:
				return math.Mod(lhs, rhs)
			}
		} else {
			rhs, ok := rhs.(int64)
			switch expr.Op {
			case cnosql.EQ:
				return ok && (lhs == rhs)
			case cnosql.NEQ:
				return ok && (lhs != rhs)
			case cnosql.LT:
				return ok && (lhs < rhs)
			case cnosql.LTE:
				return ok && (lhs <= rhs)
			case cnosql.GT:
				return ok && (lhs > rhs)
			case cnosql.GTE:
				return ok && (lhs >= rhs)
			case cnosql.ADD:
				if !ok {
					return nil
				}
				return lhs + rhs
			case cnosql.SUB:
				if !ok {
					return nil
				}
				return lhs - rhs
			case cnosql.MUL:
				if !ok {
					return nil
				}
				return lhs * rhs
			case cnosql.DIV:
				if !ok {
					return nil
				} else if rhs == 0 {
					return float64(0)
				}
				return lhs / rhs
			case cnosql.MOD:
				if !ok {
					return nil
				} else if rhs == 0 {
					return int64(0)
				}
				return lhs % rhs
			case cnosql.BITWISE_AND:
				if !ok {
					return nil
				}
				return lhs & rhs
			case cnosql.BITWISE_OR:
				if !ok {
					return nil
				}
				return lhs | rhs
			case cnosql.BITWISE_XOR:
				if !ok {
					return nil
				}
				return lhs ^ rhs
			}
		}
	case string:
		switch expr.Op {
		case cnosql.EQ:
			rhs, ok := rhs.(string)
			if !ok {
				return nil
			}
			return lhs == rhs
		case cnosql.NEQ:
			rhs, ok := rhs.(string)
			if !ok {
				return nil
			}
			return lhs != rhs
		case cnosql.EQREGEX:
			rhs, ok := rhs.(*regexp.Regexp)
			if !ok {
				return nil
			}
			return rhs.MatchString(lhs)
		case cnosql.NEQREGEX:
			rhs, ok := rhs.(*regexp.Regexp)
			if !ok {
				return nil
			}
			return !rhs.MatchString(lhs)
		}
	case []byte:
		switch expr.Op {
		case cnosql.EQ:
			rhs, ok := rhs.(string)
			if !ok {
				return nil
			}
			return string(lhs) == rhs
		case cnosql.NEQ:
			rhs, ok := rhs.(string)
			if !ok {
				return nil
			}
			return string(lhs) != rhs
		case cnosql.EQREGEX:
			rhs, ok := rhs.(*regexp.Regexp)
			if !ok {
				return nil
			}
			return rhs.Match(lhs)
		case cnosql.NEQREGEX:
			rhs, ok := rhs.(*regexp.Regexp)
			if !ok {
				return nil
			}
			return !rhs.Match(lhs)
		}
	}
	return nil
}

func EvalExprBool(expr cnosql.Expr, m Valuer) bool {
	v, _ := evalExpr(expr, m).(bool)
	return v
}
