package reads

import (
	"bytes"
	"regexp"
	"strconv"

	"github.com/cnosdatabase/cnosql"
	"github.com/cnosdatabase/db/storage/reads/datatypes"

	"github.com/pkg/errors"
)

const (
	fieldRef = "$"
)

// NodeVisitor can be called by Walk to traverse the Node hierarchy.
// The Visit() function is called once per node.
type NodeVisitor interface {
	Visit(*datatypes.Node) NodeVisitor
}

func WalkChildren(v NodeVisitor, node *datatypes.Node) {
	for _, n := range node.Children {
		WalkNode(v, n)
	}
}

func WalkNode(v NodeVisitor, node *datatypes.Node) {
	if v = v.Visit(node); v == nil {
		return
	}

	WalkChildren(v, node)
}

func PredicateToExprString(p *datatypes.Predicate) string {
	if p == nil {
		return "[none]"
	}

	var v predicateExpressionPrinter
	WalkNode(&v, p.Root)
	return v.Buffer.String()
}

type predicateExpressionPrinter struct {
	bytes.Buffer
}

func (v *predicateExpressionPrinter) Visit(n *datatypes.Node) NodeVisitor {
	switch n.NodeType {
	case datatypes.NodeTypeLogicalExpression:
		if len(n.Children) > 0 {
			var op string
			if n.GetLogical() == datatypes.LogicalAnd {
				op = " AND "
			} else {
				op = " OR "
			}
			WalkNode(v, n.Children[0])
			for _, e := range n.Children[1:] {
				v.Buffer.WriteString(op)
				WalkNode(v, e)
			}
		}

		return nil

	case datatypes.NodeTypeParenExpression:
		if len(n.Children) == 1 {
			v.Buffer.WriteString("( ")
			WalkNode(v, n.Children[0])
			v.Buffer.WriteString(" )")
		}

		return nil

	case datatypes.NodeTypeComparisonExpression:
		WalkNode(v, n.Children[0])
		v.Buffer.WriteByte(' ')
		switch n.GetComparison() {
		case datatypes.ComparisonEqual:
			v.Buffer.WriteByte('=')
		case datatypes.ComparisonNotEqual:
			v.Buffer.WriteString("!=")
		case datatypes.ComparisonStartsWith:
			v.Buffer.WriteString("startsWith")
		case datatypes.ComparisonRegex:
			v.Buffer.WriteString("=~")
		case datatypes.ComparisonNotRegex:
			v.Buffer.WriteString("!~")
		case datatypes.ComparisonLess:
			v.Buffer.WriteByte('<')
		case datatypes.ComparisonLessEqual:
			v.Buffer.WriteString("<=")
		case datatypes.ComparisonGreater:
			v.Buffer.WriteByte('>')
		case datatypes.ComparisonGreaterEqual:
			v.Buffer.WriteString(">=")
		}

		v.Buffer.WriteByte(' ')
		WalkNode(v, n.Children[1])
		return nil

	case datatypes.NodeTypeTagRef:
		v.Buffer.WriteByte('\'')
		v.Buffer.WriteString(n.GetTagRefValue())
		v.Buffer.WriteByte('\'')
		return nil

	case datatypes.NodeTypeFieldRef:
		v.Buffer.WriteByte('$')
		return nil

	case datatypes.NodeTypeLiteral:
		switch val := n.Value.(type) {
		case *datatypes.Node_StringValue:
			v.Buffer.WriteString(strconv.Quote(val.StringValue))

		case *datatypes.Node_RegexValue:
			v.Buffer.WriteByte('/')
			v.Buffer.WriteString(val.RegexValue)
			v.Buffer.WriteByte('/')

		case *datatypes.Node_IntegerValue:
			v.Buffer.WriteString(strconv.FormatInt(val.IntegerValue, 10))

		case *datatypes.Node_UnsignedValue:
			v.Buffer.WriteString(strconv.FormatUint(val.UnsignedValue, 10))

		case *datatypes.Node_FloatValue:
			v.Buffer.WriteString(strconv.FormatFloat(val.FloatValue, 'f', 10, 64))

		case *datatypes.Node_BooleanValue:
			if val.BooleanValue {
				v.Buffer.WriteString("true")
			} else {
				v.Buffer.WriteString("false")
			}
		}

		return nil

	default:
		return v
	}
}

// NodeToExpr transforms a predicate node to an cnosql.Expr.
func NodeToExpr(node *datatypes.Node, remap map[string]string) (cnosql.Expr, error) {
	v := &nodeToExprVisitor{remap: remap}
	WalkNode(v, node)
	if err := v.Err(); err != nil {
		return nil, err
	}

	if len(v.exprs) > 1 {
		return nil, errors.New("invalid expression")
	}

	if len(v.exprs) == 0 {
		return nil, nil
	}

	// TODO: It would be preferable if RewriteRegexConditions was a
	// package level function in cnosql.
	stmt := &cnosql.SelectStatement{
		Condition: v.exprs[0],
	}
	stmt.RewriteRegexConditions()
	return stmt.Condition, nil
}

type nodeToExprVisitor struct {
	remap map[string]string
	exprs []cnosql.Expr
	err   error
}

func (v *nodeToExprVisitor) Visit(n *datatypes.Node) NodeVisitor {
	if v.err != nil {
		return nil
	}

	switch n.NodeType {
	case datatypes.NodeTypeLogicalExpression:
		if len(n.Children) > 1 {
			op := cnosql.AND
			if n.GetLogical() == datatypes.LogicalOr {
				op = cnosql.OR
			}

			WalkNode(v, n.Children[0])
			if v.err != nil {
				return nil
			}

			for i := 1; i < len(n.Children); i++ {
				WalkNode(v, n.Children[i])
				if v.err != nil {
					return nil
				}

				if len(v.exprs) >= 2 {
					lhs, rhs := v.pop2()
					v.exprs = append(v.exprs, &cnosql.BinaryExpr{LHS: lhs, Op: op, RHS: rhs})
				}
			}

			return nil
		}

	case datatypes.NodeTypeParenExpression:
		if len(n.Children) != 1 {
			v.err = errors.New("parenExpression expects one child")
			return nil
		}

		WalkNode(v, n.Children[0])
		if v.err != nil {
			return nil
		}

		if len(v.exprs) > 0 {
			v.exprs = append(v.exprs, &cnosql.ParenExpr{Expr: v.pop()})
		}

		return nil

	case datatypes.NodeTypeComparisonExpression:
		WalkChildren(v, n)

		if len(v.exprs) < 2 {
			v.err = errors.New("comparisonExpression expects two children")
			return nil
		}

		lhs, rhs := v.pop2()

		be := &cnosql.BinaryExpr{LHS: lhs, RHS: rhs}
		switch n.GetComparison() {
		case datatypes.ComparisonEqual:
			be.Op = cnosql.EQ
		case datatypes.ComparisonNotEqual:
			be.Op = cnosql.NEQ
		case datatypes.ComparisonStartsWith:
			// TODO: rewrite to anchored RE, as index does not support startsWith yet
			v.err = errors.New("startsWith not implemented")
			return nil
		case datatypes.ComparisonRegex:
			be.Op = cnosql.EQREGEX
		case datatypes.ComparisonNotRegex:
			be.Op = cnosql.NEQREGEX
		case datatypes.ComparisonLess:
			be.Op = cnosql.LT
		case datatypes.ComparisonLessEqual:
			be.Op = cnosql.LTE
		case datatypes.ComparisonGreater:
			be.Op = cnosql.GT
		case datatypes.ComparisonGreaterEqual:
			be.Op = cnosql.GTE
		default:
			v.err = errors.New("invalid comparison operator")
			return nil
		}

		v.exprs = append(v.exprs, be)

		return nil

	case datatypes.NodeTypeTagRef:
		ref := n.GetTagRefValue()
		if v.remap != nil {
			if nk, ok := v.remap[ref]; ok {
				ref = nk
			}
		}

		v.exprs = append(v.exprs, &cnosql.VarRef{Val: ref, Type: cnosql.Tag})
		return nil

	case datatypes.NodeTypeFieldRef:
		v.exprs = append(v.exprs, &cnosql.VarRef{Val: fieldRef})
		return nil

	case datatypes.NodeTypeLiteral:
		switch val := n.Value.(type) {
		case *datatypes.Node_StringValue:
			v.exprs = append(v.exprs, &cnosql.StringLiteral{Val: val.StringValue})

		case *datatypes.Node_RegexValue:
			// TODO: consider hashing the RegexValue and cache compiled version
			re, err := regexp.Compile(val.RegexValue)
			if err != nil {
				v.err = err
			}
			v.exprs = append(v.exprs, &cnosql.RegexLiteral{Val: re})
			return nil

		case *datatypes.Node_IntegerValue:
			v.exprs = append(v.exprs, &cnosql.IntegerLiteral{Val: val.IntegerValue})

		case *datatypes.Node_UnsignedValue:
			v.exprs = append(v.exprs, &cnosql.UnsignedLiteral{Val: val.UnsignedValue})

		case *datatypes.Node_FloatValue:
			v.exprs = append(v.exprs, &cnosql.NumberLiteral{Val: val.FloatValue})

		case *datatypes.Node_BooleanValue:
			v.exprs = append(v.exprs, &cnosql.BooleanLiteral{Val: val.BooleanValue})

		default:
			v.err = errors.New("unexpected literal type")
			return nil
		}

		return nil

	default:
		return v
	}
	return nil
}

func (v *nodeToExprVisitor) Err() error {
	return v.err
}

func (v *nodeToExprVisitor) pop() cnosql.Expr {
	if len(v.exprs) == 0 {
		panic("stack empty")
	}

	var top cnosql.Expr
	top, v.exprs = v.exprs[len(v.exprs)-1], v.exprs[:len(v.exprs)-1]
	return top
}

func (v *nodeToExprVisitor) pop2() (cnosql.Expr, cnosql.Expr) {
	if len(v.exprs) < 2 {
		panic("stack empty")
	}

	rhs := v.exprs[len(v.exprs)-1]
	lhs := v.exprs[len(v.exprs)-2]
	v.exprs = v.exprs[:len(v.exprs)-2]
	return lhs, rhs
}

func IsTrueBooleanLiteral(expr cnosql.Expr) bool {
	b, ok := expr.(*cnosql.BooleanLiteral)
	if ok {
		return b.Val
	}
	return false
}

func RewriteExprRemoveFieldValue(expr cnosql.Expr) cnosql.Expr {
	return cnosql.RewriteExpr(expr, func(expr cnosql.Expr) cnosql.Expr {
		if be, ok := expr.(*cnosql.BinaryExpr); ok {
			if ref, ok := be.LHS.(*cnosql.VarRef); ok {
				if ref.Val == fieldRef {
					return &cnosql.BooleanLiteral{Val: true}
				}
			}
		}

		return expr
	})
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

func HasFieldValueKey(expr cnosql.Expr) bool {
	refs := hasRefs{refs: []string{fieldRef}, found: make([]bool, 1)}
	cnosql.Walk(&refs, expr)
	return refs.found[0]
}
