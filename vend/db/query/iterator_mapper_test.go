package query_test

import (
	"github.com/cnosdb/cnosdb/vend/cnosql"
	"github.com/cnosdb/cnosdb/vend/db/pkg/deep"
	"github.com/cnosdb/cnosdb/vend/db/query"
	"github.com/davecgh/go-spew/spew"
	"testing"
)

func TestIteratorMapper(t *testing.T) {
	cur := query.RowCursor([]query.Row{
		{
			Time: 0,
			Series: query.Series{
				Name: "air",
				Tags: ParseTags("station=A"),
			},
			Values: []interface{}{float64(1), "a"},
		},
		{
			Time: 5,
			Series: query.Series{
				Name: "air",
				Tags: ParseTags("station=A"),
			},
			Values: []interface{}{float64(3), "c"},
		},
		{
			Time: 2,
			Series: query.Series{
				Name: "air",
				Tags: ParseTags("station=B"),
			},
			Values: []interface{}{float64(2), "b"},
		},
		{
			Time: 8,
			Series: query.Series{
				Name: "air",
				Tags: ParseTags("station=B"),
			},
			Values: []interface{}{float64(8), "h"},
		},
	}, []cnosql.VarRef{
		{Val: "val1", Type: cnosql.Float},
		{Val: "val2", Type: cnosql.String},
	})

	opt := query.IteratorOptions{
		Ascending: true,
		Aux: []cnosql.VarRef{
			{Val: "val1", Type: cnosql.Float},
			{Val: "val2", Type: cnosql.String},
		},
		Dimensions: []string{"station"},
	}
	itr := query.NewIteratorMapper(cur, nil, []query.IteratorMap{
		query.FieldMap{Index: 0},
		query.FieldMap{Index: 1},
		query.TagMap("station"),
	}, opt)
	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if !deep.Equal(a, [][]query.Point{
		{&query.FloatPoint{Name: "air", Tags: ParseTags("station=A"), Time: 0, Aux: []interface{}{float64(1), "a", "A"}}},
		{&query.FloatPoint{Name: "air", Tags: ParseTags("station=A"), Time: 5, Aux: []interface{}{float64(3), "c", "A"}}},
		{&query.FloatPoint{Name: "air", Tags: ParseTags("station=B"), Time: 2, Aux: []interface{}{float64(2), "b", "B"}}},
		{&query.FloatPoint{Name: "air", Tags: ParseTags("station=B"), Time: 8, Aux: []interface{}{float64(8), "h", "B"}}},
	}) {
		t.Errorf("unexpected points: %s", spew.Sdump(a))
	}
}
