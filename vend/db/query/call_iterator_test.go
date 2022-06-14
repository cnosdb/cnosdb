package query_test

import (
	"github.com/cnosdb/cnosdb/vend/db/query"
	"github.com/google/go-cmp/cmp"
	"testing"
	"time"
)

// Ensure that a float iterator can be created for a count() call.
func TestCallIterator_Count_Float(t *testing.T) {
	itr, _ := query.NewCallIterator(
		&FloatIterator{Points: []query.FloatPoint{
			{Name: "air", Time: 0, Value: 15, Tags: ParseTags("region=us-east,station=hostA")},
			{Name: "air", Time: 2, Value: 10, Tags: ParseTags("region=us-east,station=hostA")},
			{Name: "air", Time: 1, Value: 10, Tags: ParseTags("region=us-west,station=hostA")},
			{Name: "air", Time: 5, Value: 20, Tags: ParseTags("region=us-east,station=hostA")},

			{Name: "air", Time: 1, Value: 11, Tags: ParseTags("region=us-west,station=hostB")},
			{Name: "air", Time: 23, Value: 8, Tags: ParseTags("region=us-west,station=hostB")},
			{Name: "sea", Time: 23, Value: 10, Tags: ParseTags("region=us-west,station=hostB")},
		}},
		query.IteratorOptions{
			Expr:       MustParseExpr(`count("value")`),
			Dimensions: []string{"station"},
			Interval:   query.Interval{Duration: 5 * time.Nanosecond},
			Ordered:    true,
			Ascending:  true,
		},
	)

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.IntegerPoint{Name: "air", Time: 0, Value: 3, Tags: ParseTags("station=hostA"), Aggregated: 3}},
		{&query.IntegerPoint{Name: "air", Time: 5, Value: 1, Tags: ParseTags("station=hostA"), Aggregated: 1}},
		{&query.IntegerPoint{Name: "air", Time: 0, Value: 1, Tags: ParseTags("station=hostB"), Aggregated: 1}},
		{&query.IntegerPoint{Name: "air", Time: 20, Value: 1, Tags: ParseTags("station=hostB"), Aggregated: 1}},
		{&query.IntegerPoint{Name: "sea", Time: 20, Value: 1, Tags: ParseTags("station=hostB"), Aggregated: 1}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure that an integer iterator can be created for a count() call.
func TestCallIterator_Count_Integer(t *testing.T) {
	itr, _ := query.NewCallIterator(
		&IntegerIterator{Points: []query.IntegerPoint{
			{Name: "air", Time: 0, Value: 15, Tags: ParseTags("region=us-east,station=hostA")},
			{Name: "air", Time: 2, Value: 10, Tags: ParseTags("region=us-east,station=hostA")},
			{Name: "air", Time: 1, Value: 10, Tags: ParseTags("region=us-west,station=hostA")},
			{Name: "air", Time: 5, Value: 20, Tags: ParseTags("region=us-east,station=hostA")},

			{Name: "air", Time: 1, Value: 11, Tags: ParseTags("region=us-west,station=hostB")},
			{Name: "air", Time: 23, Value: 8, Tags: ParseTags("region=us-west,station=hostB")},
			{Name: "sea", Time: 23, Value: 10, Tags: ParseTags("region=us-west,station=hostB")},
		}},
		query.IteratorOptions{
			Expr:       MustParseExpr(`count("value")`),
			Dimensions: []string{"station"},
			Interval:   query.Interval{Duration: 5 * time.Nanosecond},
			Ordered:    true,
			Ascending:  true,
		},
	)

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.IntegerPoint{Name: "air", Time: 0, Value: 3, Tags: ParseTags("station=hostA"), Aggregated: 3}},
		{&query.IntegerPoint{Name: "air", Time: 5, Value: 1, Tags: ParseTags("station=hostA"), Aggregated: 1}},
		{&query.IntegerPoint{Name: "air", Time: 0, Value: 1, Tags: ParseTags("station=hostB"), Aggregated: 1}},
		{&query.IntegerPoint{Name: "air", Time: 20, Value: 1, Tags: ParseTags("station=hostB"), Aggregated: 1}},
		{&query.IntegerPoint{Name: "sea", Time: 20, Value: 1, Tags: ParseTags("station=hostB"), Aggregated: 1}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure that an unsigned iterator can be created for a count() call.
func TestCallIterator_Count_Unsigned(t *testing.T) {
	itr, _ := query.NewCallIterator(
		&UnsignedIterator{Points: []query.UnsignedPoint{
			{Name: "air", Time: 0, Value: 15, Tags: ParseTags("region=us-east,station=hostA")},
			{Name: "air", Time: 2, Value: 10, Tags: ParseTags("region=us-east,station=hostA")},
			{Name: "air", Time: 1, Value: 10, Tags: ParseTags("region=us-west,station=hostA")},
			{Name: "air", Time: 5, Value: 20, Tags: ParseTags("region=us-east,station=hostA")},

			{Name: "air", Time: 1, Value: 11, Tags: ParseTags("region=us-west,station=hostB")},
			{Name: "air", Time: 23, Value: 8, Tags: ParseTags("region=us-west,station=hostB")},
			{Name: "sea", Time: 23, Value: 10, Tags: ParseTags("region=us-west,station=hostB")},
		}},
		query.IteratorOptions{
			Expr:       MustParseExpr(`count("value")`),
			Dimensions: []string{"station"},
			Interval:   query.Interval{Duration: 5 * time.Nanosecond},
			Ordered:    true,
			Ascending:  true,
		},
	)

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.IntegerPoint{Name: "air", Time: 0, Value: 3, Tags: ParseTags("station=hostA"), Aggregated: 3}},
		{&query.IntegerPoint{Name: "air", Time: 5, Value: 1, Tags: ParseTags("station=hostA"), Aggregated: 1}},
		{&query.IntegerPoint{Name: "air", Time: 0, Value: 1, Tags: ParseTags("station=hostB"), Aggregated: 1}},
		{&query.IntegerPoint{Name: "air", Time: 20, Value: 1, Tags: ParseTags("station=hostB"), Aggregated: 1}},
		{&query.IntegerPoint{Name: "sea", Time: 20, Value: 1, Tags: ParseTags("station=hostB"), Aggregated: 1}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure that a string iterator can be created for a count() call.
func TestCallIterator_Count_String(t *testing.T) {
	itr, _ := query.NewCallIterator(
		&StringIterator{Points: []query.StringPoint{
			{Name: "air", Time: 0, Value: "d", Tags: ParseTags("region=us-east,station=hostA")},
			{Name: "air", Time: 2, Value: "b", Tags: ParseTags("region=us-east,station=hostA")},
			{Name: "air", Time: 1, Value: "b", Tags: ParseTags("region=us-west,station=hostA")},
			{Name: "air", Time: 5, Value: "e", Tags: ParseTags("region=us-east,station=hostA")},

			{Name: "air", Time: 1, Value: "c", Tags: ParseTags("region=us-west,station=hostB")},
			{Name: "air", Time: 23, Value: "a", Tags: ParseTags("region=us-west,station=hostB")},
			{Name: "sea", Time: 23, Value: "b", Tags: ParseTags("region=us-west,station=hostB")},
		}},
		query.IteratorOptions{
			Expr:       MustParseExpr(`count("value")`),
			Dimensions: []string{"station"},
			Interval:   query.Interval{Duration: 5 * time.Nanosecond},
			Ordered:    true,
			Ascending:  true,
		},
	)

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.IntegerPoint{Name: "air", Time: 0, Value: 3, Tags: ParseTags("station=hostA"), Aggregated: 3}},
		{&query.IntegerPoint{Name: "air", Time: 5, Value: 1, Tags: ParseTags("station=hostA"), Aggregated: 1}},
		{&query.IntegerPoint{Name: "air", Time: 0, Value: 1, Tags: ParseTags("station=hostB"), Aggregated: 1}},
		{&query.IntegerPoint{Name: "air", Time: 20, Value: 1, Tags: ParseTags("station=hostB"), Aggregated: 1}},
		{&query.IntegerPoint{Name: "sea", Time: 20, Value: 1, Tags: ParseTags("station=hostB"), Aggregated: 1}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure that a boolean iterator can be created for a count() call.
func TestCallIterator_Count_Boolean(t *testing.T) {
	itr, _ := query.NewCallIterator(
		&BooleanIterator{Points: []query.BooleanPoint{
			{Name: "air", Time: 0, Value: true, Tags: ParseTags("region=us-east,station=hostA")},
			{Name: "air", Time: 2, Value: false, Tags: ParseTags("region=us-east,station=hostA")},
			{Name: "air", Time: 1, Value: true, Tags: ParseTags("region=us-west,station=hostA")},
			{Name: "air", Time: 5, Value: false, Tags: ParseTags("region=us-east,station=hostA")},

			{Name: "air", Time: 1, Value: true, Tags: ParseTags("region=us-west,station=hostB")},
			{Name: "air", Time: 23, Value: false, Tags: ParseTags("region=us-west,station=hostB")},
			{Name: "sea", Time: 23, Value: true, Tags: ParseTags("region=us-west,station=hostB")},
		}},
		query.IteratorOptions{
			Expr:       MustParseExpr(`count("value")`),
			Dimensions: []string{"station"},
			Interval:   query.Interval{Duration: 5 * time.Nanosecond},
			Ordered:    true,
			Ascending:  true,
		},
	)

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.IntegerPoint{Name: "air", Time: 0, Value: 3, Tags: ParseTags("station=hostA"), Aggregated: 3}},
		{&query.IntegerPoint{Name: "air", Time: 5, Value: 1, Tags: ParseTags("station=hostA"), Aggregated: 1}},
		{&query.IntegerPoint{Name: "air", Time: 0, Value: 1, Tags: ParseTags("station=hostB"), Aggregated: 1}},
		{&query.IntegerPoint{Name: "air", Time: 20, Value: 1, Tags: ParseTags("station=hostB"), Aggregated: 1}},
		{&query.IntegerPoint{Name: "sea", Time: 20, Value: 1, Tags: ParseTags("station=hostB"), Aggregated: 1}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure that a float iterator can be created for a min() call.
func TestCallIterator_Min_Float(t *testing.T) {
	itr, _ := query.NewCallIterator(
		&FloatIterator{Points: []query.FloatPoint{
			{Time: 0, Value: 15, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 2, Value: 10, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 4, Value: 12, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 1, Value: 10, Tags: ParseTags("region=us-west,station=hostA")},
			{Time: 5, Value: 20, Tags: ParseTags("region=us-east,station=hostA")},

			{Time: 1, Value: 11, Tags: ParseTags("region=us-west,station=hostB")},
			{Time: 23, Value: 8, Tags: ParseTags("region=us-west,station=hostB")},
		}},
		query.IteratorOptions{
			Expr:       MustParseExpr(`min("value")`),
			Dimensions: []string{"station"},
			Interval:   query.Interval{Duration: 5 * time.Nanosecond},
			Ordered:    true,
			Ascending:  true,
		},
	)

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Time: 1, Value: 10, Tags: ParseTags("station=hostA"), Aggregated: 4}},
		{&query.FloatPoint{Time: 5, Value: 20, Tags: ParseTags("station=hostA"), Aggregated: 1}},
		{&query.FloatPoint{Time: 1, Value: 11, Tags: ParseTags("station=hostB"), Aggregated: 1}},
		{&query.FloatPoint{Time: 23, Value: 8, Tags: ParseTags("station=hostB"), Aggregated: 1}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure that a integer iterator can be created for a min() call.
func TestCallIterator_Min_Integer(t *testing.T) {
	itr, _ := query.NewCallIterator(
		&IntegerIterator{Points: []query.IntegerPoint{
			{Time: 0, Value: 15, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 2, Value: 10, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 4, Value: 12, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 1, Value: 10, Tags: ParseTags("region=us-west,station=hostA")},
			{Time: 5, Value: 20, Tags: ParseTags("region=us-east,station=hostA")},

			{Time: 1, Value: 11, Tags: ParseTags("region=us-west,station=hostB")},
			{Time: 23, Value: 8, Tags: ParseTags("region=us-west,station=hostB")},
		}},
		query.IteratorOptions{
			Expr:       MustParseExpr(`min("value")`),
			Dimensions: []string{"station"},
			Interval:   query.Interval{Duration: 5 * time.Nanosecond},
			Ordered:    true,
			Ascending:  true,
		},
	)

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.IntegerPoint{Time: 1, Value: 10, Tags: ParseTags("station=hostA"), Aggregated: 4}},
		{&query.IntegerPoint{Time: 5, Value: 20, Tags: ParseTags("station=hostA"), Aggregated: 1}},
		{&query.IntegerPoint{Time: 1, Value: 11, Tags: ParseTags("station=hostB"), Aggregated: 1}},
		{&query.IntegerPoint{Time: 23, Value: 8, Tags: ParseTags("station=hostB"), Aggregated: 1}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure that a unsigned iterator can be created for a min() call.
func TestCallIterator_Min_Unsigned(t *testing.T) {
	itr, _ := query.NewCallIterator(
		&UnsignedIterator{Points: []query.UnsignedPoint{
			{Time: 0, Value: 15, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 2, Value: 10, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 4, Value: 12, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 1, Value: 10, Tags: ParseTags("region=us-west,station=hostA")},
			{Time: 5, Value: 20, Tags: ParseTags("region=us-east,station=hostA")},

			{Time: 1, Value: 11, Tags: ParseTags("region=us-west,station=hostB")},
			{Time: 23, Value: 8, Tags: ParseTags("region=us-west,station=hostB")},
		}},
		query.IteratorOptions{
			Expr:       MustParseExpr(`min("value")`),
			Dimensions: []string{"station"},
			Interval:   query.Interval{Duration: 5 * time.Nanosecond},
			Ordered:    true,
			Ascending:  true,
		},
	)

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.UnsignedPoint{Time: 1, Value: 10, Tags: ParseTags("station=hostA"), Aggregated: 4}},
		{&query.UnsignedPoint{Time: 5, Value: 20, Tags: ParseTags("station=hostA"), Aggregated: 1}},
		{&query.UnsignedPoint{Time: 1, Value: 11, Tags: ParseTags("station=hostB"), Aggregated: 1}},
		{&query.UnsignedPoint{Time: 23, Value: 8, Tags: ParseTags("station=hostB"), Aggregated: 1}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure that a boolean iterator can be created for a min() call.
func TestCallIterator_Min_Boolean(t *testing.T) {
	itr, _ := query.NewCallIterator(
		&BooleanIterator{Points: []query.BooleanPoint{
			{Time: 0, Value: true, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 2, Value: false, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 1, Value: true, Tags: ParseTags("region=us-west,station=hostA")},
			{Time: 5, Value: false, Tags: ParseTags("region=us-east,station=hostA")},

			{Time: 1, Value: false, Tags: ParseTags("region=us-west,station=hostB")},
			{Time: 23, Value: true, Tags: ParseTags("region=us-west,station=hostB")},
		}},
		query.IteratorOptions{
			Expr:       MustParseExpr(`min("value")`),
			Dimensions: []string{"station"},
			Interval:   query.Interval{Duration: 5 * time.Nanosecond},
			Ordered:    true,
			Ascending:  true,
		},
	)

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.BooleanPoint{Time: 2, Value: false, Tags: ParseTags("station=hostA"), Aggregated: 3}},
		{&query.BooleanPoint{Time: 5, Value: false, Tags: ParseTags("station=hostA"), Aggregated: 1}},
		{&query.BooleanPoint{Time: 1, Value: false, Tags: ParseTags("station=hostB"), Aggregated: 1}},
		{&query.BooleanPoint{Time: 23, Value: true, Tags: ParseTags("station=hostB"), Aggregated: 1}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure that a float iterator can be created for a max() call.
func TestCallIterator_Max_Float(t *testing.T) {
	itr, _ := query.NewCallIterator(
		&FloatIterator{Points: []query.FloatPoint{
			{Time: 0, Value: 15, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 2, Value: 10, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 1, Value: 10, Tags: ParseTags("region=us-west,station=hostA")},
			{Time: 5, Value: 20, Tags: ParseTags("region=us-east,station=hostA")},

			{Time: 1, Value: 11, Tags: ParseTags("region=us-west,station=hostB")},
			{Time: 23, Value: 8, Tags: ParseTags("region=us-west,station=hostB")},
		}},
		query.IteratorOptions{
			Expr:       MustParseExpr(`max("value")`),
			Dimensions: []string{"station"},
			Interval:   query.Interval{Duration: 5 * time.Nanosecond},
			Ordered:    true,
			Ascending:  true,
		},
	)

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Time: 0, Value: 15, Tags: ParseTags("station=hostA"), Aggregated: 3}},
		{&query.FloatPoint{Time: 5, Value: 20, Tags: ParseTags("station=hostA"), Aggregated: 1}},
		{&query.FloatPoint{Time: 1, Value: 11, Tags: ParseTags("station=hostB"), Aggregated: 1}},
		{&query.FloatPoint{Time: 23, Value: 8, Tags: ParseTags("station=hostB"), Aggregated: 1}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure that a integer iterator can be created for a max() call.
func TestCallIterator_Max_Integer(t *testing.T) {
	itr, _ := query.NewCallIterator(
		&IntegerIterator{Points: []query.IntegerPoint{
			{Time: 0, Value: 15, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 2, Value: 10, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 1, Value: 10, Tags: ParseTags("region=us-west,station=hostA")},
			{Time: 5, Value: 20, Tags: ParseTags("region=us-east,station=hostA")},

			{Time: 1, Value: 11, Tags: ParseTags("region=us-west,station=hostB")},
			{Time: 23, Value: 8, Tags: ParseTags("region=us-west,station=hostB")},
		}},
		query.IteratorOptions{
			Expr:       MustParseExpr(`max("value")`),
			Dimensions: []string{"station"},
			Interval:   query.Interval{Duration: 5 * time.Nanosecond},
			Ordered:    true,
			Ascending:  true,
		},
	)

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.IntegerPoint{Time: 0, Value: 15, Tags: ParseTags("station=hostA"), Aggregated: 3}},
		{&query.IntegerPoint{Time: 5, Value: 20, Tags: ParseTags("station=hostA"), Aggregated: 1}},
		{&query.IntegerPoint{Time: 1, Value: 11, Tags: ParseTags("station=hostB"), Aggregated: 1}},
		{&query.IntegerPoint{Time: 23, Value: 8, Tags: ParseTags("station=hostB"), Aggregated: 1}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure that a unsigned iterator can be created for a max() call.
func TestCallIterator_Max_Unsigned(t *testing.T) {
	itr, _ := query.NewCallIterator(
		&UnsignedIterator{Points: []query.UnsignedPoint{
			{Time: 0, Value: 15, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 2, Value: 10, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 1, Value: 10, Tags: ParseTags("region=us-west,station=hostA")},
			{Time: 5, Value: 20, Tags: ParseTags("region=us-east,station=hostA")},

			{Time: 1, Value: 11, Tags: ParseTags("region=us-west,station=hostB")},
			{Time: 23, Value: 8, Tags: ParseTags("region=us-west,station=hostB")},
		}},
		query.IteratorOptions{
			Expr:       MustParseExpr(`max("value")`),
			Dimensions: []string{"station"},
			Interval:   query.Interval{Duration: 5 * time.Nanosecond},
			Ordered:    true,
			Ascending:  true,
		},
	)

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.UnsignedPoint{Time: 0, Value: 15, Tags: ParseTags("station=hostA"), Aggregated: 3}},
		{&query.UnsignedPoint{Time: 5, Value: 20, Tags: ParseTags("station=hostA"), Aggregated: 1}},
		{&query.UnsignedPoint{Time: 1, Value: 11, Tags: ParseTags("station=hostB"), Aggregated: 1}},
		{&query.UnsignedPoint{Time: 23, Value: 8, Tags: ParseTags("station=hostB"), Aggregated: 1}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure that a boolean iterator can be created for a max() call.
func TestCallIterator_Max_Boolean(t *testing.T) {
	itr, _ := query.NewCallIterator(
		&BooleanIterator{Points: []query.BooleanPoint{
			{Time: 0, Value: true, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 2, Value: false, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 1, Value: true, Tags: ParseTags("region=us-west,station=hostA")},
			{Time: 5, Value: false, Tags: ParseTags("region=us-east,station=hostA")},

			{Time: 1, Value: false, Tags: ParseTags("region=us-west,station=hostB")},
			{Time: 23, Value: true, Tags: ParseTags("region=us-west,station=hostB")},
		}},
		query.IteratorOptions{
			Expr:       MustParseExpr(`max("value")`),
			Dimensions: []string{"station"},
			Interval:   query.Interval{Duration: 5 * time.Nanosecond},
			Ordered:    true,
			Ascending:  true,
		},
	)

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.BooleanPoint{Time: 0, Value: true, Tags: ParseTags("station=hostA"), Aggregated: 3}},
		{&query.BooleanPoint{Time: 5, Value: false, Tags: ParseTags("station=hostA"), Aggregated: 1}},
		{&query.BooleanPoint{Time: 1, Value: false, Tags: ParseTags("station=hostB"), Aggregated: 1}},
		{&query.BooleanPoint{Time: 23, Value: true, Tags: ParseTags("station=hostB"), Aggregated: 1}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure that a float iterator can be created for a sum() call.
func TestCallIterator_Sum_Float(t *testing.T) {
	itr, _ := query.NewCallIterator(
		&FloatIterator{Points: []query.FloatPoint{
			{Time: 0, Value: 15, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 2, Value: 10, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 1, Value: 10, Tags: ParseTags("region=us-west,station=hostA")},
			{Time: 5, Value: 20, Tags: ParseTags("region=us-east,station=hostA")},

			{Time: 1, Value: 11, Tags: ParseTags("region=us-west,station=hostB")},
			{Time: 23, Value: 8, Tags: ParseTags("region=us-west,station=hostB")},
		}},
		query.IteratorOptions{
			Expr:       MustParseExpr(`sum("value")`),
			Dimensions: []string{"station"},
			Interval:   query.Interval{Duration: 5 * time.Nanosecond},
			Ordered:    true,
			Ascending:  true,
		},
	)

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Time: 0, Value: 35, Tags: ParseTags("station=hostA"), Aggregated: 3}},
		{&query.FloatPoint{Time: 5, Value: 20, Tags: ParseTags("station=hostA"), Aggregated: 1}},
		{&query.FloatPoint{Time: 0, Value: 11, Tags: ParseTags("station=hostB"), Aggregated: 1}},
		{&query.FloatPoint{Time: 20, Value: 8, Tags: ParseTags("station=hostB"), Aggregated: 1}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure that an integer iterator can be created for a sum() call.
func TestCallIterator_Sum_Integer(t *testing.T) {
	itr, _ := query.NewCallIterator(
		&IntegerIterator{Points: []query.IntegerPoint{
			{Time: 0, Value: 15, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 2, Value: 10, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 1, Value: 10, Tags: ParseTags("region=us-west,station=hostA")},
			{Time: 5, Value: 20, Tags: ParseTags("region=us-east,station=hostA")},

			{Time: 1, Value: 11, Tags: ParseTags("region=us-west,station=hostB")},
			{Time: 23, Value: 8, Tags: ParseTags("region=us-west,station=hostB")},
		}},
		query.IteratorOptions{
			Expr:       MustParseExpr(`sum("value")`),
			Dimensions: []string{"station"},
			Interval:   query.Interval{Duration: 5 * time.Nanosecond},
			Ordered:    true,
			Ascending:  true,
		},
	)

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.IntegerPoint{Time: 0, Value: 35, Tags: ParseTags("station=hostA"), Aggregated: 3}},
		{&query.IntegerPoint{Time: 5, Value: 20, Tags: ParseTags("station=hostA"), Aggregated: 1}},
		{&query.IntegerPoint{Time: 0, Value: 11, Tags: ParseTags("station=hostB"), Aggregated: 1}},
		{&query.IntegerPoint{Time: 20, Value: 8, Tags: ParseTags("station=hostB"), Aggregated: 1}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure that an unsigned iterator can be created for a sum() call.
func TestCallIterator_Sum_Unsigned(t *testing.T) {
	itr, _ := query.NewCallIterator(
		&UnsignedIterator{Points: []query.UnsignedPoint{
			{Time: 0, Value: 15, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 2, Value: 10, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 1, Value: 10, Tags: ParseTags("region=us-west,station=hostA")},
			{Time: 5, Value: 20, Tags: ParseTags("region=us-east,station=hostA")},

			{Time: 1, Value: 11, Tags: ParseTags("region=us-west,station=hostB")},
			{Time: 23, Value: 8, Tags: ParseTags("region=us-west,station=hostB")},
		}},
		query.IteratorOptions{
			Expr:       MustParseExpr(`sum("value")`),
			Dimensions: []string{"station"},
			Interval:   query.Interval{Duration: 5 * time.Nanosecond},
			Ordered:    true,
			Ascending:  true,
		},
	)

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.UnsignedPoint{Time: 0, Value: 35, Tags: ParseTags("station=hostA"), Aggregated: 3}},
		{&query.UnsignedPoint{Time: 5, Value: 20, Tags: ParseTags("station=hostA"), Aggregated: 1}},
		{&query.UnsignedPoint{Time: 0, Value: 11, Tags: ParseTags("station=hostB"), Aggregated: 1}},
		{&query.UnsignedPoint{Time: 20, Value: 8, Tags: ParseTags("station=hostB"), Aggregated: 1}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure that a float iterator can be created for a first() call.
func TestCallIterator_First_Float(t *testing.T) {
	itr, _ := query.NewCallIterator(
		&FloatIterator{Points: []query.FloatPoint{
			{Time: 2, Value: 10, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 0, Value: 15, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 1, Value: 10, Tags: ParseTags("region=us-west,station=hostA")},
			{Time: 6, Value: 20, Tags: ParseTags("region=us-east,station=hostA")},

			{Time: 1, Value: 11, Tags: ParseTags("region=us-west,station=hostB")},
			{Time: 23, Value: 8, Tags: ParseTags("region=us-west,station=hostB")},
		}},
		query.IteratorOptions{
			Expr:       MustParseExpr(`first("value")`),
			Dimensions: []string{"station"},
			Interval:   query.Interval{Duration: 5 * time.Nanosecond},
			Ordered:    true,
			Ascending:  true,
		},
	)

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Time: 0, Value: 15, Tags: ParseTags("station=hostA"), Aggregated: 3}},
		{&query.FloatPoint{Time: 6, Value: 20, Tags: ParseTags("station=hostA"), Aggregated: 1}},
		{&query.FloatPoint{Time: 1, Value: 11, Tags: ParseTags("station=hostB"), Aggregated: 1}},
		{&query.FloatPoint{Time: 23, Value: 8, Tags: ParseTags("station=hostB"), Aggregated: 1}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure that an integer iterator can be created for a first() call.
func TestCallIterator_First_Integer(t *testing.T) {
	itr, _ := query.NewCallIterator(
		&IntegerIterator{Points: []query.IntegerPoint{
			{Time: 2, Value: 10, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 0, Value: 15, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 1, Value: 10, Tags: ParseTags("region=us-west,station=hostA")},
			{Time: 6, Value: 20, Tags: ParseTags("region=us-east,station=hostA")},

			{Time: 1, Value: 11, Tags: ParseTags("region=us-west,station=hostB")},
			{Time: 23, Value: 8, Tags: ParseTags("region=us-west,station=hostB")},
		}},
		query.IteratorOptions{
			Expr:       MustParseExpr(`first("value")`),
			Dimensions: []string{"station"},
			Interval:   query.Interval{Duration: 5 * time.Nanosecond},
			Ordered:    true,
			Ascending:  true,
		},
	)

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.IntegerPoint{Time: 0, Value: 15, Tags: ParseTags("station=hostA"), Aggregated: 3}},
		{&query.IntegerPoint{Time: 6, Value: 20, Tags: ParseTags("station=hostA"), Aggregated: 1}},
		{&query.IntegerPoint{Time: 1, Value: 11, Tags: ParseTags("station=hostB"), Aggregated: 1}},
		{&query.IntegerPoint{Time: 23, Value: 8, Tags: ParseTags("station=hostB"), Aggregated: 1}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure that an unsigned iterator can be created for a first() call.
func TestCallIterator_First_Unsigned(t *testing.T) {
	itr, _ := query.NewCallIterator(
		&UnsignedIterator{Points: []query.UnsignedPoint{
			{Time: 2, Value: 10, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 0, Value: 15, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 1, Value: 10, Tags: ParseTags("region=us-west,station=hostA")},
			{Time: 6, Value: 20, Tags: ParseTags("region=us-east,station=hostA")},

			{Time: 1, Value: 11, Tags: ParseTags("region=us-west,station=hostB")},
			{Time: 23, Value: 8, Tags: ParseTags("region=us-west,station=hostB")},
		}},
		query.IteratorOptions{
			Expr:       MustParseExpr(`first("value")`),
			Dimensions: []string{"station"},
			Interval:   query.Interval{Duration: 5 * time.Nanosecond},
			Ordered:    true,
			Ascending:  true,
		},
	)

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.UnsignedPoint{Time: 0, Value: 15, Tags: ParseTags("station=hostA"), Aggregated: 3}},
		{&query.UnsignedPoint{Time: 6, Value: 20, Tags: ParseTags("station=hostA"), Aggregated: 1}},
		{&query.UnsignedPoint{Time: 1, Value: 11, Tags: ParseTags("station=hostB"), Aggregated: 1}},
		{&query.UnsignedPoint{Time: 23, Value: 8, Tags: ParseTags("station=hostB"), Aggregated: 1}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure that a string iterator can be created for a first() call.
func TestCallIterator_First_String(t *testing.T) {
	itr, _ := query.NewCallIterator(
		&StringIterator{Points: []query.StringPoint{
			{Time: 2, Value: "b", Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 0, Value: "d", Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 1, Value: "b", Tags: ParseTags("region=us-west,station=hostA")},
			{Time: 6, Value: "e", Tags: ParseTags("region=us-east,station=hostA")},

			{Time: 1, Value: "c", Tags: ParseTags("region=us-west,station=hostB")},
			{Time: 23, Value: "a", Tags: ParseTags("region=us-west,station=hostB")},
		}},
		query.IteratorOptions{
			Expr:       MustParseExpr(`first("value")`),
			Dimensions: []string{"station"},
			Interval:   query.Interval{Duration: 5 * time.Nanosecond},
			Ordered:    true,
			Ascending:  true,
		},
	)

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.StringPoint{Time: 0, Value: "d", Tags: ParseTags("station=hostA"), Aggregated: 3}},
		{&query.StringPoint{Time: 6, Value: "e", Tags: ParseTags("station=hostA"), Aggregated: 1}},
		{&query.StringPoint{Time: 1, Value: "c", Tags: ParseTags("station=hostB"), Aggregated: 1}},
		{&query.StringPoint{Time: 23, Value: "a", Tags: ParseTags("station=hostB"), Aggregated: 1}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure that a boolean iterator can be created for a first() call.
func TestCallIterator_First_Boolean(t *testing.T) {
	itr, _ := query.NewCallIterator(
		&BooleanIterator{Points: []query.BooleanPoint{
			{Time: 2, Value: false, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 0, Value: true, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 1, Value: false, Tags: ParseTags("region=us-west,station=hostA")},
			{Time: 6, Value: false, Tags: ParseTags("region=us-east,station=hostA")},

			{Time: 1, Value: true, Tags: ParseTags("region=us-west,station=hostB")},
			{Time: 23, Value: false, Tags: ParseTags("region=us-west,station=hostB")},
		}},
		query.IteratorOptions{
			Expr:       MustParseExpr(`first("value")`),
			Dimensions: []string{"station"},
			Interval:   query.Interval{Duration: 5 * time.Nanosecond},
			Ordered:    true,
			Ascending:  true,
		},
	)

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.BooleanPoint{Time: 0, Value: true, Tags: ParseTags("station=hostA"), Aggregated: 3}},
		{&query.BooleanPoint{Time: 6, Value: false, Tags: ParseTags("station=hostA"), Aggregated: 1}},
		{&query.BooleanPoint{Time: 1, Value: true, Tags: ParseTags("station=hostB"), Aggregated: 1}},
		{&query.BooleanPoint{Time: 23, Value: false, Tags: ParseTags("station=hostB"), Aggregated: 1}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure that a float iterator can be created for a last() call.
func TestCallIterator_Last_Float(t *testing.T) {
	itr, _ := query.NewCallIterator(
		&FloatIterator{Points: []query.FloatPoint{
			{Time: 2, Value: 10, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 0, Value: 15, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 1, Value: 10, Tags: ParseTags("region=us-west,station=hostA")},
			{Time: 6, Value: 20, Tags: ParseTags("region=us-east,station=hostA")},

			{Time: 1, Value: 11, Tags: ParseTags("region=us-west,station=hostB")},
			{Time: 23, Value: 8, Tags: ParseTags("region=us-west,station=hostB")},
		}},
		query.IteratorOptions{
			Expr:       MustParseExpr(`last("value")`),
			Dimensions: []string{"station"},
			Interval:   query.Interval{Duration: 5 * time.Nanosecond},
			Ordered:    true,
			Ascending:  true,
		},
	)

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Time: 2, Value: 10, Tags: ParseTags("station=hostA"), Aggregated: 3}},
		{&query.FloatPoint{Time: 6, Value: 20, Tags: ParseTags("station=hostA"), Aggregated: 1}},
		{&query.FloatPoint{Time: 1, Value: 11, Tags: ParseTags("station=hostB"), Aggregated: 1}},
		{&query.FloatPoint{Time: 23, Value: 8, Tags: ParseTags("station=hostB"), Aggregated: 1}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure that an integer iterator can be created for a last() call.
func TestCallIterator_Last_Integer(t *testing.T) {
	itr, _ := query.NewCallIterator(
		&IntegerIterator{Points: []query.IntegerPoint{
			{Time: 2, Value: 10, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 0, Value: 15, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 1, Value: 10, Tags: ParseTags("region=us-west,station=hostA")},
			{Time: 6, Value: 20, Tags: ParseTags("region=us-east,station=hostA")},

			{Time: 1, Value: 11, Tags: ParseTags("region=us-west,station=hostB")},
			{Time: 23, Value: 8, Tags: ParseTags("region=us-west,station=hostB")},
		}},
		query.IteratorOptions{
			Expr:       MustParseExpr(`last("value")`),
			Dimensions: []string{"station"},
			Interval:   query.Interval{Duration: 5 * time.Nanosecond},
			Ordered:    true,
			Ascending:  true,
		},
	)

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.IntegerPoint{Time: 2, Value: 10, Tags: ParseTags("station=hostA"), Aggregated: 3}},
		{&query.IntegerPoint{Time: 6, Value: 20, Tags: ParseTags("station=hostA"), Aggregated: 1}},
		{&query.IntegerPoint{Time: 1, Value: 11, Tags: ParseTags("station=hostB"), Aggregated: 1}},
		{&query.IntegerPoint{Time: 23, Value: 8, Tags: ParseTags("station=hostB"), Aggregated: 1}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure that an unsigned iterator can be created for a last() call.
func TestCallIterator_Last_Unsigned(t *testing.T) {
	itr, _ := query.NewCallIterator(
		&UnsignedIterator{Points: []query.UnsignedPoint{
			{Time: 2, Value: 10, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 0, Value: 15, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 1, Value: 10, Tags: ParseTags("region=us-west,station=hostA")},
			{Time: 6, Value: 20, Tags: ParseTags("region=us-east,station=hostA")},

			{Time: 1, Value: 11, Tags: ParseTags("region=us-west,station=hostB")},
			{Time: 23, Value: 8, Tags: ParseTags("region=us-west,station=hostB")},
		}},
		query.IteratorOptions{
			Expr:       MustParseExpr(`last("value")`),
			Dimensions: []string{"station"},
			Interval:   query.Interval{Duration: 5 * time.Nanosecond},
			Ordered:    true,
			Ascending:  true,
		},
	)

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.UnsignedPoint{Time: 2, Value: 10, Tags: ParseTags("station=hostA"), Aggregated: 3}},
		{&query.UnsignedPoint{Time: 6, Value: 20, Tags: ParseTags("station=hostA"), Aggregated: 1}},
		{&query.UnsignedPoint{Time: 1, Value: 11, Tags: ParseTags("station=hostB"), Aggregated: 1}},
		{&query.UnsignedPoint{Time: 23, Value: 8, Tags: ParseTags("station=hostB"), Aggregated: 1}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure that a string iterator can be created for a last() call.
func TestCallIterator_Last_String(t *testing.T) {
	itr, _ := query.NewCallIterator(
		&StringIterator{Points: []query.StringPoint{
			{Time: 2, Value: "b", Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 0, Value: "d", Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 1, Value: "b", Tags: ParseTags("region=us-west,station=hostA")},
			{Time: 6, Value: "e", Tags: ParseTags("region=us-east,station=hostA")},

			{Time: 1, Value: "c", Tags: ParseTags("region=us-west,station=hostB")},
			{Time: 23, Value: "a", Tags: ParseTags("region=us-west,station=hostB")},
		}},
		query.IteratorOptions{
			Expr:       MustParseExpr(`last("value")`),
			Dimensions: []string{"station"},
			Interval:   query.Interval{Duration: 5 * time.Nanosecond},
			Ordered:    true,
			Ascending:  true,
		},
	)

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.StringPoint{Time: 2, Value: "b", Tags: ParseTags("station=hostA"), Aggregated: 3}},
		{&query.StringPoint{Time: 6, Value: "e", Tags: ParseTags("station=hostA"), Aggregated: 1}},
		{&query.StringPoint{Time: 1, Value: "c", Tags: ParseTags("station=hostB"), Aggregated: 1}},
		{&query.StringPoint{Time: 23, Value: "a", Tags: ParseTags("station=hostB"), Aggregated: 1}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure that a boolean iterator can be created for a last() call.
func TestCallIterator_Last_Boolean(t *testing.T) {
	itr, _ := query.NewCallIterator(
		&BooleanIterator{Points: []query.BooleanPoint{
			{Time: 2, Value: false, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 0, Value: true, Tags: ParseTags("region=us-east,station=hostA")},
			{Time: 1, Value: false, Tags: ParseTags("region=us-west,station=hostA")},
			{Time: 6, Value: false, Tags: ParseTags("region=us-east,station=hostA")},

			{Time: 1, Value: true, Tags: ParseTags("region=us-west,station=hostB")},
			{Time: 23, Value: false, Tags: ParseTags("region=us-west,station=hostB")},
		}},
		query.IteratorOptions{
			Expr:       MustParseExpr(`last("value")`),
			Dimensions: []string{"station"},
			Interval:   query.Interval{Duration: 5 * time.Nanosecond},
			Ordered:    true,
			Ascending:  true,
		},
	)

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.BooleanPoint{Time: 2, Value: false, Tags: ParseTags("station=hostA"), Aggregated: 3}},
		{&query.BooleanPoint{Time: 6, Value: false, Tags: ParseTags("station=hostA"), Aggregated: 1}},
		{&query.BooleanPoint{Time: 1, Value: true, Tags: ParseTags("station=hostB"), Aggregated: 1}},
		{&query.BooleanPoint{Time: 23, Value: false, Tags: ParseTags("station=hostB"), Aggregated: 1}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure that a float iterator can be created for a mode() call.
func TestCallIterator_Mode_Float(t *testing.T) {
	itr, _ := query.NewModeIterator(&FloatIterator{Points: []query.FloatPoint{
		{Time: 0, Value: 15, Tags: ParseTags("region=us-east,station=hostA")},
		{Time: 1, Value: 10, Tags: ParseTags("region=us-west,station=hostA")},
		{Time: 2, Value: 10, Tags: ParseTags("region=us-east,station=hostA")},
		{Time: 3, Value: 10, Tags: ParseTags("region=us-east,station=hostA")},
		{Time: 4, Value: 10, Tags: ParseTags("region=us-east,station=hostA")},
		{Time: 6, Value: 20, Tags: ParseTags("region=us-east,station=hostA")},
		{Time: 7, Value: 21, Tags: ParseTags("region=us-east,station=hostA")},
		{Time: 8, Value: 21, Tags: ParseTags("region=us-east,station=hostA")},

		{Time: 1, Value: 11, Tags: ParseTags("region=us-west,station=hostB")},
		{Time: 22, Value: 8, Tags: ParseTags("region=us-west,station=hostB")},
		{Time: 23, Value: 8, Tags: ParseTags("region=us-west,station=hostB")},
		{Time: 24, Value: 25, Tags: ParseTags("region=us-west,station=hostB")},
	}},
		query.IteratorOptions{
			Expr:       MustParseExpr(`mode("value")`),
			Dimensions: []string{"station"},
			Interval:   query.Interval{Duration: 5 * time.Nanosecond},
			Ordered:    true,
			Ascending:  true,
		},
	)

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Time: 0, Value: 10, Tags: ParseTags("station=hostA"), Aggregated: 0}},
		{&query.FloatPoint{Time: 5, Value: 21, Tags: ParseTags("station=hostA"), Aggregated: 0}},
		{&query.FloatPoint{Time: 1, Value: 11, Tags: ParseTags("station=hostB"), Aggregated: 0}},
		{&query.FloatPoint{Time: 20, Value: 8, Tags: ParseTags("station=hostB"), Aggregated: 0}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure that a integer iterator can be created for a mode() call.
func TestCallIterator_Mode_Integer(t *testing.T) {
	itr, _ := query.NewModeIterator(&IntegerIterator{Points: []query.IntegerPoint{
		{Time: 0, Value: 15, Tags: ParseTags("region=us-east,station=hostA")},
		{Time: 1, Value: 10, Tags: ParseTags("region=us-west,station=hostA")},
		{Time: 2, Value: 10, Tags: ParseTags("region=us-east,station=hostA")},
		{Time: 3, Value: 10, Tags: ParseTags("region=us-east,station=hostA")},
		{Time: 4, Value: 10, Tags: ParseTags("region=us-east,station=hostA")},
		{Time: 6, Value: 20, Tags: ParseTags("region=us-east,station=hostA")},
		{Time: 7, Value: 21, Tags: ParseTags("region=us-east,station=hostA")},
		{Time: 8, Value: 21, Tags: ParseTags("region=us-east,station=hostA")},
		{Time: 1, Value: 11, Tags: ParseTags("region=us-west,station=hostB")},
		{Time: 22, Value: 8, Tags: ParseTags("region=us-west,station=hostB")},
		{Time: 23, Value: 8, Tags: ParseTags("region=us-west,station=hostB")},
		{Time: 24, Value: 25, Tags: ParseTags("region=us-west,station=hostB")},
	}},
		query.IteratorOptions{
			Expr:       MustParseExpr(`mode("value")`),
			Dimensions: []string{"station"},
			Interval:   query.Interval{Duration: 5 * time.Nanosecond},
			Ordered:    true,
			Ascending:  true,
		},
	)

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.IntegerPoint{Time: 0, Value: 10, Tags: ParseTags("station=hostA")}},
		{&query.IntegerPoint{Time: 5, Value: 21, Tags: ParseTags("station=hostA")}},
		{&query.IntegerPoint{Time: 1, Value: 11, Tags: ParseTags("station=hostB")}},
		{&query.IntegerPoint{Time: 20, Value: 8, Tags: ParseTags("station=hostB")}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure that a unsigned iterator can be created for a mode() call.
func TestCallIterator_Mode_Unsigned(t *testing.T) {
	itr, _ := query.NewModeIterator(&UnsignedIterator{Points: []query.UnsignedPoint{
		{Time: 0, Value: 15, Tags: ParseTags("region=us-east,station=hostA")},
		{Time: 1, Value: 10, Tags: ParseTags("region=us-west,station=hostA")},
		{Time: 2, Value: 10, Tags: ParseTags("region=us-east,station=hostA")},
		{Time: 3, Value: 10, Tags: ParseTags("region=us-east,station=hostA")},
		{Time: 4, Value: 10, Tags: ParseTags("region=us-east,station=hostA")},
		{Time: 6, Value: 20, Tags: ParseTags("region=us-east,station=hostA")},
		{Time: 7, Value: 21, Tags: ParseTags("region=us-east,station=hostA")},
		{Time: 8, Value: 21, Tags: ParseTags("region=us-east,station=hostA")},
		{Time: 1, Value: 11, Tags: ParseTags("region=us-west,station=hostB")},
		{Time: 22, Value: 8, Tags: ParseTags("region=us-west,station=hostB")},
		{Time: 23, Value: 8, Tags: ParseTags("region=us-west,station=hostB")},
		{Time: 24, Value: 25, Tags: ParseTags("region=us-west,station=hostB")},
	}},
		query.IteratorOptions{
			Expr:       MustParseExpr(`mode("value")`),
			Dimensions: []string{"station"},
			Interval:   query.Interval{Duration: 5 * time.Nanosecond},
			Ordered:    true,
			Ascending:  true,
		},
	)

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.UnsignedPoint{Time: 0, Value: 10, Tags: ParseTags("station=hostA")}},
		{&query.UnsignedPoint{Time: 5, Value: 21, Tags: ParseTags("station=hostA")}},
		{&query.UnsignedPoint{Time: 1, Value: 11, Tags: ParseTags("station=hostB")}},
		{&query.UnsignedPoint{Time: 20, Value: 8, Tags: ParseTags("station=hostB")}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure that a string iterator can be created for a mode() call.
func TestCallIterator_Mode_String(t *testing.T) {
	itr, _ := query.NewModeIterator(&StringIterator{Points: []query.StringPoint{
		{Time: 0, Value: "15", Tags: ParseTags("region=us-east,station=hostA")},
		{Time: 1, Value: "10", Tags: ParseTags("region=us-west,station=hostA")},
		{Time: 2, Value: "10", Tags: ParseTags("region=us-east,station=hostA")},
		{Time: 3, Value: "10", Tags: ParseTags("region=us-east,station=hostA")},
		{Time: 4, Value: "10", Tags: ParseTags("region=us-east,station=hostA")},
		{Time: 6, Value: "20", Tags: ParseTags("region=us-east,station=hostA")},
		{Time: 7, Value: "21", Tags: ParseTags("region=us-east,station=hostA")},
		{Time: 7, Value: "21", Tags: ParseTags("region=us-east,station=hostA")},

		{Time: 1, Value: "11", Tags: ParseTags("region=us-west,station=hostB")},
		{Time: 22, Value: "8", Tags: ParseTags("region=us-west,station=hostB")},
		{Time: 23, Value: "8", Tags: ParseTags("region=us-west,station=hostB")},
		{Time: 24, Value: "25", Tags: ParseTags("region=us-west,station=hostB")},
	}},
		query.IteratorOptions{
			Expr:       MustParseExpr(`mode("value")`),
			Dimensions: []string{"station"},
			Interval:   query.Interval{Duration: 5 * time.Nanosecond},
			Ordered:    true,
			Ascending:  true,
		},
	)

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.StringPoint{Time: 0, Value: "10", Tags: ParseTags("station=hostA")}},
		{&query.StringPoint{Time: 5, Value: "21", Tags: ParseTags("station=hostA")}},
		{&query.StringPoint{Time: 1, Value: "11", Tags: ParseTags("station=hostB")}},
		{&query.StringPoint{Time: 20, Value: "8", Tags: ParseTags("station=hostB")}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure that a boolean iterator can be created for a modBooleanl.
func TestCallIterator_Mode_Boolean(t *testing.T) {
	itr, _ := query.NewModeIterator(&BooleanIterator{Points: []query.BooleanPoint{
		{Time: 0, Value: true, Tags: ParseTags("region=us-east,station=hostA")},
		{Time: 1, Value: true, Tags: ParseTags("region=us-west,station=hostA")},
		{Time: 2, Value: true, Tags: ParseTags("region=us-east,station=hostA")},
		{Time: 3, Value: true, Tags: ParseTags("region=us-east,station=hostA")},
		{Time: 4, Value: false, Tags: ParseTags("region=us-east,station=hostA")},
		{Time: 6, Value: false, Tags: ParseTags("region=us-east,station=hostA")},
		{Time: 7, Value: false, Tags: ParseTags("region=us-east,station=hostA")},
		{Time: 8, Value: false, Tags: ParseTags("region=us-east,station=hostA")},

		{Time: 1, Value: false, Tags: ParseTags("region=us-west,station=hostB")},
		{Time: 22, Value: false, Tags: ParseTags("region=us-west,station=hostB")},
		{Time: 23, Value: true, Tags: ParseTags("region=us-west,station=hostB")},
		{Time: 24, Value: true, Tags: ParseTags("region=us-west,station=hostB")},
	}},
		query.IteratorOptions{
			Expr:       MustParseExpr(`mode("value")`),
			Dimensions: []string{"station"},
			Interval:   query.Interval{Duration: 5 * time.Nanosecond},
			Ordered:    true,
			Ascending:  true,
		},
	)

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.BooleanPoint{Time: 0, Value: true, Tags: ParseTags("station=hostA")}},
		{&query.BooleanPoint{Time: 5, Value: false, Tags: ParseTags("station=hostA")}},
		{&query.BooleanPoint{Time: 1, Value: false, Tags: ParseTags("station=hostB")}},
		{&query.BooleanPoint{Time: 20, Value: true, Tags: ParseTags("station=hostB")}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestNewCallIterator_UnsupportedExprName(t *testing.T) {
	_, err := query.NewCallIterator(
		&FloatIterator{},
		query.IteratorOptions{
			Expr: MustParseExpr(`foobar("value")`),
		},
	)

	if err == nil || err.Error() != "unsupported function call: foobar" {
		t.Errorf("unexpected error: %s", err)
	}
}
