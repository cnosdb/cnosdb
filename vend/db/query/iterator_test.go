package query_test

import (
	"bytes"
	"context"
	"fmt"
	"github.com/cnosdb/cnosdb/vend/cnosql"
	"github.com/cnosdb/cnosdb/vend/db/pkg/deep"
	"github.com/cnosdb/cnosdb/vend/db/query"
	"github.com/davecgh/go-spew/spew"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Ensure that a set of iterators can be merged together, sorted by window and name/tag.
func TestMergeIterator_Float(t *testing.T) {
	inputs := []*FloatIterator{
		{Points: []query.FloatPoint{
			{Name: "air", Tags: ParseTags("station=A"), Time: 0, Value: 1},
			{Name: "air", Tags: ParseTags("station=A"), Time: 12, Value: 3},
			{Name: "air", Tags: ParseTags("station=A"), Time: 30, Value: 4},
			{Name: "air", Tags: ParseTags("station=B"), Time: 1, Value: 2},
			{Name: "sea", Tags: ParseTags("station=B"), Time: 11, Value: 8},
		}},
		{Points: []query.FloatPoint{
			{Name: "air", Tags: ParseTags("station=A"), Time: 20, Value: 7},
			{Name: "air", Tags: ParseTags("station=B"), Time: 11, Value: 5},
			{Name: "air", Tags: ParseTags("station=B"), Time: 13, Value: 6},
			{Name: "sea", Tags: ParseTags("station=A"), Time: 25, Value: 9},
		}},
		{Points: []query.FloatPoint{}},
		{Points: []query.FloatPoint{}},
	}

	itr := query.NewMergeIterator(FloatIterators(inputs), query.IteratorOptions{
		Interval: query.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions: []string{"station"},
		Ascending:  true,
	})
	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if !deep.Equal(a, [][]query.Point{
		{&query.FloatPoint{Name: "air", Tags: ParseTags("station=A"), Time: 0, Value: 1}},
		{&query.FloatPoint{Name: "air", Tags: ParseTags("station=A"), Time: 12, Value: 3}},
		{&query.FloatPoint{Name: "air", Tags: ParseTags("station=A"), Time: 20, Value: 7}},
		{&query.FloatPoint{Name: "air", Tags: ParseTags("station=A"), Time: 30, Value: 4}},
		{&query.FloatPoint{Name: "air", Tags: ParseTags("station=B"), Time: 1, Value: 2}},
		{&query.FloatPoint{Name: "air", Tags: ParseTags("station=B"), Time: 11, Value: 5}},
		{&query.FloatPoint{Name: "air", Tags: ParseTags("station=B"), Time: 13, Value: 6}},
		{&query.FloatPoint{Name: "sea", Tags: ParseTags("station=A"), Time: 25, Value: 9}},
		{&query.FloatPoint{Name: "sea", Tags: ParseTags("station=B"), Time: 11, Value: 8}},
	}) {
		t.Errorf("unexpected points: %s", spew.Sdump(a))
	}

	for i, input := range inputs {
		if !input.Closed {
			t.Errorf("iterator %d not closed", i)
		}
	}
}

// Ensure that a set of iterators can be merged together, sorted by window and name/tag.
func TestMergeIterator_Integer(t *testing.T) {
	inputs := []*IntegerIterator{
		{Points: []query.IntegerPoint{
			{Name: "air", Tags: ParseTags("station=A"), Time: 0, Value: 1},
			{Name: "air", Tags: ParseTags("station=A"), Time: 12, Value: 3},
			{Name: "air", Tags: ParseTags("station=A"), Time: 30, Value: 4},
			{Name: "air", Tags: ParseTags("station=B"), Time: 1, Value: 2},
			{Name: "sea", Tags: ParseTags("station=B"), Time: 11, Value: 8},
		}},
		{Points: []query.IntegerPoint{
			{Name: "air", Tags: ParseTags("station=A"), Time: 20, Value: 7},
			{Name: "air", Tags: ParseTags("station=B"), Time: 11, Value: 5},
			{Name: "air", Tags: ParseTags("station=B"), Time: 13, Value: 6},
			{Name: "sea", Tags: ParseTags("station=A"), Time: 25, Value: 9},
		}},
		{Points: []query.IntegerPoint{}},
	}
	itr := query.NewMergeIterator(IntegerIterators(inputs), query.IteratorOptions{
		Interval: query.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions: []string{"station"},
		Ascending:  true,
	})

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if !deep.Equal(a, [][]query.Point{
		{&query.IntegerPoint{Name: "air", Tags: ParseTags("station=A"), Time: 0, Value: 1}},
		{&query.IntegerPoint{Name: "air", Tags: ParseTags("station=A"), Time: 12, Value: 3}},
		{&query.IntegerPoint{Name: "air", Tags: ParseTags("station=A"), Time: 20, Value: 7}},
		{&query.IntegerPoint{Name: "air", Tags: ParseTags("station=A"), Time: 30, Value: 4}},
		{&query.IntegerPoint{Name: "air", Tags: ParseTags("station=B"), Time: 1, Value: 2}},
		{&query.IntegerPoint{Name: "air", Tags: ParseTags("station=B"), Time: 11, Value: 5}},
		{&query.IntegerPoint{Name: "air", Tags: ParseTags("station=B"), Time: 13, Value: 6}},
		{&query.IntegerPoint{Name: "sea", Tags: ParseTags("station=A"), Time: 25, Value: 9}},
		{&query.IntegerPoint{Name: "sea", Tags: ParseTags("station=B"), Time: 11, Value: 8}},
	}) {
		t.Errorf("unexpected points: %s", spew.Sdump(a))
	}

	for i, input := range inputs {
		if !input.Closed {
			t.Errorf("iterator %d not closed", i)
		}
	}
}

// Ensure that a set of iterators can be merged together, sorted by window and name/tag.
func TestMergeIterator_Unsigned(t *testing.T) {
	inputs := []*UnsignedIterator{
		{Points: []query.UnsignedPoint{
			{Name: "air", Tags: ParseTags("station=A"), Time: 0, Value: 1},
			{Name: "air", Tags: ParseTags("station=A"), Time: 12, Value: 3},
			{Name: "air", Tags: ParseTags("station=A"), Time: 30, Value: 4},
			{Name: "air", Tags: ParseTags("station=B"), Time: 1, Value: 2},
			{Name: "sea", Tags: ParseTags("station=B"), Time: 11, Value: 8},
		}},
		{Points: []query.UnsignedPoint{
			{Name: "air", Tags: ParseTags("station=A"), Time: 20, Value: 7},
			{Name: "air", Tags: ParseTags("station=B"), Time: 11, Value: 5},
			{Name: "air", Tags: ParseTags("station=B"), Time: 13, Value: 6},
			{Name: "sea", Tags: ParseTags("station=A"), Time: 25, Value: 9},
		}},
		{Points: []query.UnsignedPoint{}},
	}
	itr := query.NewMergeIterator(UnsignedIterators(inputs), query.IteratorOptions{
		Interval: query.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions: []string{"station"},
		Ascending:  true,
	})

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if !deep.Equal(a, [][]query.Point{
		{&query.UnsignedPoint{Name: "air", Tags: ParseTags("station=A"), Time: 0, Value: 1}},
		{&query.UnsignedPoint{Name: "air", Tags: ParseTags("station=A"), Time: 12, Value: 3}},
		{&query.UnsignedPoint{Name: "air", Tags: ParseTags("station=A"), Time: 20, Value: 7}},
		{&query.UnsignedPoint{Name: "air", Tags: ParseTags("station=A"), Time: 30, Value: 4}},
		{&query.UnsignedPoint{Name: "air", Tags: ParseTags("station=B"), Time: 1, Value: 2}},
		{&query.UnsignedPoint{Name: "air", Tags: ParseTags("station=B"), Time: 11, Value: 5}},
		{&query.UnsignedPoint{Name: "air", Tags: ParseTags("station=B"), Time: 13, Value: 6}},
		{&query.UnsignedPoint{Name: "sea", Tags: ParseTags("station=A"), Time: 25, Value: 9}},
		{&query.UnsignedPoint{Name: "sea", Tags: ParseTags("station=B"), Time: 11, Value: 8}},
	}) {
		t.Errorf("unexpected points: %s", spew.Sdump(a))
	}

	for i, input := range inputs {
		if !input.Closed {
			t.Errorf("iterator %d not closed", i)
		}
	}
}

// Ensure that a set of iterators can be merged together, sorted by window and name/tag.
func TestMergeIterator_String(t *testing.T) {
	inputs := []*StringIterator{
		{Points: []query.StringPoint{
			{Name: "air", Tags: ParseTags("station=A"), Time: 0, Value: "a"},
			{Name: "air", Tags: ParseTags("station=A"), Time: 12, Value: "c"},
			{Name: "air", Tags: ParseTags("station=A"), Time: 30, Value: "d"},
			{Name: "air", Tags: ParseTags("station=B"), Time: 1, Value: "b"},
			{Name: "sea", Tags: ParseTags("station=B"), Time: 11, Value: "h"},
		}},
		{Points: []query.StringPoint{
			{Name: "air", Tags: ParseTags("station=A"), Time: 20, Value: "g"},
			{Name: "air", Tags: ParseTags("station=B"), Time: 11, Value: "e"},
			{Name: "air", Tags: ParseTags("station=B"), Time: 13, Value: "f"},
			{Name: "sea", Tags: ParseTags("station=A"), Time: 25, Value: "i"},
		}},
		{Points: []query.StringPoint{}},
	}
	itr := query.NewMergeIterator(StringIterators(inputs), query.IteratorOptions{
		Interval: query.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions: []string{"station"},
		Ascending:  true,
	})

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if !deep.Equal(a, [][]query.Point{
		{&query.StringPoint{Name: "air", Tags: ParseTags("station=A"), Time: 0, Value: "a"}},
		{&query.StringPoint{Name: "air", Tags: ParseTags("station=A"), Time: 12, Value: "c"}},
		{&query.StringPoint{Name: "air", Tags: ParseTags("station=A"), Time: 20, Value: "g"}},
		{&query.StringPoint{Name: "air", Tags: ParseTags("station=A"), Time: 30, Value: "d"}},
		{&query.StringPoint{Name: "air", Tags: ParseTags("station=B"), Time: 1, Value: "b"}},
		{&query.StringPoint{Name: "air", Tags: ParseTags("station=B"), Time: 11, Value: "e"}},
		{&query.StringPoint{Name: "air", Tags: ParseTags("station=B"), Time: 13, Value: "f"}},
		{&query.StringPoint{Name: "sea", Tags: ParseTags("station=A"), Time: 25, Value: "i"}},
		{&query.StringPoint{Name: "sea", Tags: ParseTags("station=B"), Time: 11, Value: "h"}},
	}) {
		t.Errorf("unexpected points: %s", spew.Sdump(a))
	}

	for i, input := range inputs {
		if !input.Closed {
			t.Errorf("iterator %d not closed", i)
		}
	}
}

// Ensure that a set of iterators can be merged together, sorted by window and name/tag.
func TestMergeIterator_Boolean(t *testing.T) {
	inputs := []*BooleanIterator{
		{Points: []query.BooleanPoint{
			{Name: "air", Tags: ParseTags("station=A"), Time: 0, Value: true},
			{Name: "air", Tags: ParseTags("station=A"), Time: 12, Value: true},
			{Name: "air", Tags: ParseTags("station=A"), Time: 30, Value: false},
			{Name: "air", Tags: ParseTags("station=B"), Time: 1, Value: false},
			{Name: "sea", Tags: ParseTags("station=B"), Time: 11, Value: true},
		}},
		{Points: []query.BooleanPoint{
			{Name: "air", Tags: ParseTags("station=A"), Time: 20, Value: true},
			{Name: "air", Tags: ParseTags("station=B"), Time: 11, Value: true},
			{Name: "air", Tags: ParseTags("station=B"), Time: 13, Value: false},
			{Name: "sea", Tags: ParseTags("station=A"), Time: 25, Value: false},
		}},
		{Points: []query.BooleanPoint{}},
	}
	itr := query.NewMergeIterator(BooleanIterators(inputs), query.IteratorOptions{
		Interval: query.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions: []string{"station"},
		Ascending:  true,
	})

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if !deep.Equal(a, [][]query.Point{
		{&query.BooleanPoint{Name: "air", Tags: ParseTags("station=A"), Time: 0, Value: true}},
		{&query.BooleanPoint{Name: "air", Tags: ParseTags("station=A"), Time: 12, Value: true}},
		{&query.BooleanPoint{Name: "air", Tags: ParseTags("station=A"), Time: 20, Value: true}},
		{&query.BooleanPoint{Name: "air", Tags: ParseTags("station=A"), Time: 30, Value: false}},
		{&query.BooleanPoint{Name: "air", Tags: ParseTags("station=B"), Time: 1, Value: false}},
		{&query.BooleanPoint{Name: "air", Tags: ParseTags("station=B"), Time: 11, Value: true}},
		{&query.BooleanPoint{Name: "air", Tags: ParseTags("station=B"), Time: 13, Value: false}},
		{&query.BooleanPoint{Name: "sea", Tags: ParseTags("station=A"), Time: 25, Value: false}},
		{&query.BooleanPoint{Name: "sea", Tags: ParseTags("station=B"), Time: 11, Value: true}},
	}) {
		t.Errorf("unexpected points: %s", spew.Sdump(a))
	}

	for i, input := range inputs {
		if !input.Closed {
			t.Errorf("iterator %d not closed", i)
		}
	}
}

func TestMergeIterator_Nil(t *testing.T) {
	itr := query.NewMergeIterator([]query.Iterator{nil}, query.IteratorOptions{})
	if itr != nil {
		t.Fatalf("unexpected iterator: %#v", itr)
	}
}

func TestMergeIterator_Coerce_Float(t *testing.T) {
	inputs := []query.Iterator{
		&FloatIterator{Points: []query.FloatPoint{
			{Name: "air", Tags: ParseTags("station=A"), Time: 20, Value: 7},
			{Name: "air", Tags: ParseTags("station=B"), Time: 11, Value: 5},
			{Name: "air", Tags: ParseTags("station=B"), Time: 13, Value: 6},
			{Name: "sea", Tags: ParseTags("station=A"), Time: 25, Value: 9},
		}},
		&IntegerIterator{Points: []query.IntegerPoint{
			{Name: "air", Tags: ParseTags("station=A"), Time: 0, Value: 1},
			{Name: "air", Tags: ParseTags("station=A"), Time: 12, Value: 3},
			{Name: "air", Tags: ParseTags("station=A"), Time: 30, Value: 4},
			{Name: "air", Tags: ParseTags("station=B"), Time: 1, Value: 2},
			{Name: "sea", Tags: ParseTags("station=B"), Time: 11, Value: 8},
		}},
	}

	itr := query.NewMergeIterator(inputs, query.IteratorOptions{
		Interval: query.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions: []string{"station"},
		Ascending:  true,
	})
	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if !deep.Equal(a, [][]query.Point{
		{&query.FloatPoint{Name: "air", Tags: ParseTags("station=A"), Time: 20, Value: 7}},
		{&query.FloatPoint{Name: "air", Tags: ParseTags("station=B"), Time: 11, Value: 5}},
		{&query.FloatPoint{Name: "air", Tags: ParseTags("station=B"), Time: 13, Value: 6}},
		{&query.FloatPoint{Name: "sea", Tags: ParseTags("station=A"), Time: 25, Value: 9}},
	}) {
		t.Errorf("unexpected points: %s", spew.Sdump(a))
	}

	for i, input := range inputs {
		switch input := input.(type) {
		case *FloatIterator:
			if !input.Closed {
				t.Errorf("iterator %d not closed", i)
			}
		case *IntegerIterator:
			if !input.Closed {
				t.Errorf("iterator %d not closed", i)
			}
		case *UnsignedIterator:
			if !input.Closed {
				t.Errorf("iterator %d not closed", i)
			}
		}
	}
}

// Ensure that a set of iterators can be merged together, sorted by name/tag.
func TestSortedMergeIterator_Float(t *testing.T) {
	inputs := []*FloatIterator{
		{Points: []query.FloatPoint{
			{Name: "air", Tags: ParseTags("station=A"), Time: 0, Value: 1},
			{Name: "air", Tags: ParseTags("station=A"), Time: 12, Value: 3},
			{Name: "air", Tags: ParseTags("station=A"), Time: 30, Value: 4},
			{Name: "air", Tags: ParseTags("station=B"), Time: 1, Value: 2},
			{Name: "sea", Tags: ParseTags("station=B"), Time: 4, Value: 8},
		}},
		{Points: []query.FloatPoint{
			{Name: "air", Tags: ParseTags("station=A"), Time: 20, Value: 7},
			{Name: "air", Tags: ParseTags("station=B"), Time: 11, Value: 5},
			{Name: "air", Tags: ParseTags("station=B"), Time: 13, Value: 6},
			{Name: "sea", Tags: ParseTags("station=A"), Time: 25, Value: 9},
		}},
		{Points: []query.FloatPoint{}},
	}
	itr := query.NewSortedMergeIterator(FloatIterators(inputs), query.IteratorOptions{
		Interval: query.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions: []string{"station"},
		Ascending:  true,
	})
	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if !deep.Equal(a, [][]query.Point{
		{&query.FloatPoint{Name: "air", Tags: ParseTags("station=A"), Time: 0, Value: 1}},
		{&query.FloatPoint{Name: "air", Tags: ParseTags("station=A"), Time: 12, Value: 3}},
		{&query.FloatPoint{Name: "air", Tags: ParseTags("station=A"), Time: 20, Value: 7}},
		{&query.FloatPoint{Name: "air", Tags: ParseTags("station=A"), Time: 30, Value: 4}},
		{&query.FloatPoint{Name: "air", Tags: ParseTags("station=B"), Time: 1, Value: 2}},
		{&query.FloatPoint{Name: "air", Tags: ParseTags("station=B"), Time: 11, Value: 5}},
		{&query.FloatPoint{Name: "air", Tags: ParseTags("station=B"), Time: 13, Value: 6}},
		{&query.FloatPoint{Name: "sea", Tags: ParseTags("station=A"), Time: 25, Value: 9}},
		{&query.FloatPoint{Name: "sea", Tags: ParseTags("station=B"), Time: 4, Value: 8}},
	}) {
		t.Errorf("unexpected points: %s", spew.Sdump(a))
	}

	for i, input := range inputs {
		if !input.Closed {
			t.Errorf("iterator %d not closed", i)
		}
	}
}

// Ensure that a set of iterators can be merged together, sorted by name/tag.
func TestSortedMergeIterator_Integer(t *testing.T) {
	inputs := []*IntegerIterator{
		{Points: []query.IntegerPoint{
			{Name: "air", Tags: ParseTags("station=A"), Time: 0, Value: 1},
			{Name: "air", Tags: ParseTags("station=A"), Time: 12, Value: 3},
			{Name: "air", Tags: ParseTags("station=A"), Time: 30, Value: 4},
			{Name: "air", Tags: ParseTags("station=B"), Time: 1, Value: 2},
			{Name: "sea", Tags: ParseTags("station=B"), Time: 4, Value: 8},
		}},
		{Points: []query.IntegerPoint{
			{Name: "air", Tags: ParseTags("station=A"), Time: 20, Value: 7},
			{Name: "air", Tags: ParseTags("station=B"), Time: 11, Value: 5},
			{Name: "air", Tags: ParseTags("station=B"), Time: 13, Value: 6},
			{Name: "sea", Tags: ParseTags("station=A"), Time: 25, Value: 9},
		}},
		{Points: []query.IntegerPoint{}},
	}
	itr := query.NewSortedMergeIterator(IntegerIterators(inputs), query.IteratorOptions{
		Interval: query.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions: []string{"station"},
		Ascending:  true,
	})
	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if !deep.Equal(a, [][]query.Point{
		{&query.IntegerPoint{Name: "air", Tags: ParseTags("station=A"), Time: 0, Value: 1}},
		{&query.IntegerPoint{Name: "air", Tags: ParseTags("station=A"), Time: 12, Value: 3}},
		{&query.IntegerPoint{Name: "air", Tags: ParseTags("station=A"), Time: 20, Value: 7}},
		{&query.IntegerPoint{Name: "air", Tags: ParseTags("station=A"), Time: 30, Value: 4}},
		{&query.IntegerPoint{Name: "air", Tags: ParseTags("station=B"), Time: 1, Value: 2}},
		{&query.IntegerPoint{Name: "air", Tags: ParseTags("station=B"), Time: 11, Value: 5}},
		{&query.IntegerPoint{Name: "air", Tags: ParseTags("station=B"), Time: 13, Value: 6}},
		{&query.IntegerPoint{Name: "sea", Tags: ParseTags("station=A"), Time: 25, Value: 9}},
		{&query.IntegerPoint{Name: "sea", Tags: ParseTags("station=B"), Time: 4, Value: 8}},
	}) {
		t.Errorf("unexpected points: %s", spew.Sdump(a))
	}

	for i, input := range inputs {
		if !input.Closed {
			t.Errorf("iterator %d not closed", i)
		}
	}
}

// Ensure that a set of iterators can be merged together, sorted by name/tag.
func TestSortedMergeIterator_Unsigned(t *testing.T) {
	inputs := []*UnsignedIterator{
		{Points: []query.UnsignedPoint{
			{Name: "air", Tags: ParseTags("station=A"), Time: 0, Value: 1},
			{Name: "air", Tags: ParseTags("station=A"), Time: 12, Value: 3},
			{Name: "air", Tags: ParseTags("station=A"), Time: 30, Value: 4},
			{Name: "air", Tags: ParseTags("station=B"), Time: 1, Value: 2},
			{Name: "sea", Tags: ParseTags("station=B"), Time: 4, Value: 8},
		}},
		{Points: []query.UnsignedPoint{
			{Name: "air", Tags: ParseTags("station=A"), Time: 20, Value: 7},
			{Name: "air", Tags: ParseTags("station=B"), Time: 11, Value: 5},
			{Name: "air", Tags: ParseTags("station=B"), Time: 13, Value: 6},
			{Name: "sea", Tags: ParseTags("station=A"), Time: 25, Value: 9},
		}},
		{Points: []query.UnsignedPoint{}},
	}
	itr := query.NewSortedMergeIterator(UnsignedIterators(inputs), query.IteratorOptions{
		Interval: query.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions: []string{"station"},
		Ascending:  true,
	})
	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if !deep.Equal(a, [][]query.Point{
		{&query.UnsignedPoint{Name: "air", Tags: ParseTags("station=A"), Time: 0, Value: 1}},
		{&query.UnsignedPoint{Name: "air", Tags: ParseTags("station=A"), Time: 12, Value: 3}},
		{&query.UnsignedPoint{Name: "air", Tags: ParseTags("station=A"), Time: 20, Value: 7}},
		{&query.UnsignedPoint{Name: "air", Tags: ParseTags("station=A"), Time: 30, Value: 4}},
		{&query.UnsignedPoint{Name: "air", Tags: ParseTags("station=B"), Time: 1, Value: 2}},
		{&query.UnsignedPoint{Name: "air", Tags: ParseTags("station=B"), Time: 11, Value: 5}},
		{&query.UnsignedPoint{Name: "air", Tags: ParseTags("station=B"), Time: 13, Value: 6}},
		{&query.UnsignedPoint{Name: "sea", Tags: ParseTags("station=A"), Time: 25, Value: 9}},
		{&query.UnsignedPoint{Name: "sea", Tags: ParseTags("station=B"), Time: 4, Value: 8}},
	}) {
		t.Errorf("unexpected points: %s", spew.Sdump(a))
	}

	for i, input := range inputs {
		if !input.Closed {
			t.Errorf("iterator %d not closed", i)
		}
	}
}

// Ensure that a set of iterators can be merged together, sorted by name/tag.
func TestSortedMergeIterator_String(t *testing.T) {
	inputs := []*StringIterator{
		{Points: []query.StringPoint{
			{Name: "air", Tags: ParseTags("station=A"), Time: 0, Value: "a"},
			{Name: "air", Tags: ParseTags("station=A"), Time: 12, Value: "c"},
			{Name: "air", Tags: ParseTags("station=A"), Time: 30, Value: "d"},
			{Name: "air", Tags: ParseTags("station=B"), Time: 1, Value: "b"},
			{Name: "sea", Tags: ParseTags("station=B"), Time: 4, Value: "h"},
		}},
		{Points: []query.StringPoint{
			{Name: "air", Tags: ParseTags("station=A"), Time: 20, Value: "g"},
			{Name: "air", Tags: ParseTags("station=B"), Time: 11, Value: "e"},
			{Name: "air", Tags: ParseTags("station=B"), Time: 13, Value: "f"},
			{Name: "sea", Tags: ParseTags("station=A"), Time: 25, Value: "i"},
		}},
		{Points: []query.StringPoint{}},
	}
	itr := query.NewSortedMergeIterator(StringIterators(inputs), query.IteratorOptions{
		Interval: query.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions: []string{"station"},
		Ascending:  true,
	})
	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if !deep.Equal(a, [][]query.Point{
		{&query.StringPoint{Name: "air", Tags: ParseTags("station=A"), Time: 0, Value: "a"}},
		{&query.StringPoint{Name: "air", Tags: ParseTags("station=A"), Time: 12, Value: "c"}},
		{&query.StringPoint{Name: "air", Tags: ParseTags("station=A"), Time: 20, Value: "g"}},
		{&query.StringPoint{Name: "air", Tags: ParseTags("station=A"), Time: 30, Value: "d"}},
		{&query.StringPoint{Name: "air", Tags: ParseTags("station=B"), Time: 1, Value: "b"}},
		{&query.StringPoint{Name: "air", Tags: ParseTags("station=B"), Time: 11, Value: "e"}},
		{&query.StringPoint{Name: "air", Tags: ParseTags("station=B"), Time: 13, Value: "f"}},
		{&query.StringPoint{Name: "sea", Tags: ParseTags("station=A"), Time: 25, Value: "i"}},
		{&query.StringPoint{Name: "sea", Tags: ParseTags("station=B"), Time: 4, Value: "h"}},
	}) {
		t.Errorf("unexpected points: %s", spew.Sdump(a))
	}

	for i, input := range inputs {
		if !input.Closed {
			t.Errorf("iterator %d not closed", i)
		}
	}
}

// Ensure that a set of iterators can be merged together, sorted by name/tag.
func TestSortedMergeIterator_Boolean(t *testing.T) {
	inputs := []*BooleanIterator{
		{Points: []query.BooleanPoint{
			{Name: "air", Tags: ParseTags("station=A"), Time: 0, Value: true},
			{Name: "air", Tags: ParseTags("station=A"), Time: 12, Value: true},
			{Name: "air", Tags: ParseTags("station=A"), Time: 30, Value: false},
			{Name: "air", Tags: ParseTags("station=B"), Time: 1, Value: false},
			{Name: "sea", Tags: ParseTags("station=B"), Time: 4, Value: true},
		}},
		{Points: []query.BooleanPoint{
			{Name: "air", Tags: ParseTags("station=A"), Time: 20, Value: true},
			{Name: "air", Tags: ParseTags("station=B"), Time: 11, Value: true},
			{Name: "air", Tags: ParseTags("station=B"), Time: 13, Value: false},
			{Name: "sea", Tags: ParseTags("station=A"), Time: 25, Value: true},
		}},
		{Points: []query.BooleanPoint{}},
	}
	itr := query.NewSortedMergeIterator(BooleanIterators(inputs), query.IteratorOptions{
		Interval: query.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions: []string{"station"},
		Ascending:  true,
	})
	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if !deep.Equal(a, [][]query.Point{
		{&query.BooleanPoint{Name: "air", Tags: ParseTags("station=A"), Time: 0, Value: true}},
		{&query.BooleanPoint{Name: "air", Tags: ParseTags("station=A"), Time: 12, Value: true}},
		{&query.BooleanPoint{Name: "air", Tags: ParseTags("station=A"), Time: 20, Value: true}},
		{&query.BooleanPoint{Name: "air", Tags: ParseTags("station=A"), Time: 30, Value: false}},
		{&query.BooleanPoint{Name: "air", Tags: ParseTags("station=B"), Time: 1, Value: false}},
		{&query.BooleanPoint{Name: "air", Tags: ParseTags("station=B"), Time: 11, Value: true}},
		{&query.BooleanPoint{Name: "air", Tags: ParseTags("station=B"), Time: 13, Value: false}},
		{&query.BooleanPoint{Name: "sea", Tags: ParseTags("station=A"), Time: 25, Value: true}},
		{&query.BooleanPoint{Name: "sea", Tags: ParseTags("station=B"), Time: 4, Value: true}},
	}) {
		t.Errorf("unexpected points: %s", spew.Sdump(a))
	}

	for i, input := range inputs {
		if !input.Closed {
			t.Errorf("iterator %d not closed", i)
		}
	}
}

func TestSortedMergeIterator_Nil(t *testing.T) {
	itr := query.NewSortedMergeIterator([]query.Iterator{nil}, query.IteratorOptions{})
	if itr != nil {
		t.Fatalf("unexpected iterator: %#v", itr)
	}
}

func TestSortedMergeIterator_Coerce_Float(t *testing.T) {
	inputs := []query.Iterator{
		&FloatIterator{Points: []query.FloatPoint{
			{Name: "air", Tags: ParseTags("station=A"), Time: 20, Value: 7},
			{Name: "air", Tags: ParseTags("station=B"), Time: 11, Value: 5},
			{Name: "air", Tags: ParseTags("station=B"), Time: 13, Value: 6},
			{Name: "sea", Tags: ParseTags("station=A"), Time: 25, Value: 9},
		}},
		&IntegerIterator{Points: []query.IntegerPoint{
			{Name: "air", Tags: ParseTags("station=A"), Time: 0, Value: 1},
			{Name: "air", Tags: ParseTags("station=A"), Time: 12, Value: 3},
			{Name: "air", Tags: ParseTags("station=A"), Time: 30, Value: 4},
			{Name: "air", Tags: ParseTags("station=B"), Time: 1, Value: 2},
			{Name: "sea", Tags: ParseTags("station=B"), Time: 4, Value: 8},
		}},
	}

	itr := query.NewSortedMergeIterator(inputs, query.IteratorOptions{
		Interval: query.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions: []string{"station"},
		Ascending:  true,
	})
	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if !deep.Equal(a, [][]query.Point{
		{&query.FloatPoint{Name: "air", Tags: ParseTags("station=A"), Time: 20, Value: 7}},
		{&query.FloatPoint{Name: "air", Tags: ParseTags("station=B"), Time: 11, Value: 5}},
		{&query.FloatPoint{Name: "air", Tags: ParseTags("station=B"), Time: 13, Value: 6}},
		{&query.FloatPoint{Name: "sea", Tags: ParseTags("station=A"), Time: 25, Value: 9}},
	}) {
		t.Errorf("unexpected points: %s", spew.Sdump(a))
	}

	for i, input := range inputs {
		switch input := input.(type) {
		case *FloatIterator:
			if !input.Closed {
				t.Errorf("iterator %d not closed", i)
			}
		case *IntegerIterator:
			if !input.Closed {
				t.Errorf("iterator %d not closed", i)
			}
		case *UnsignedIterator:
			if !input.Closed {
				t.Errorf("iterator %d not closed", i)
			}
		}
	}
}

// Ensure limit iterators work with limit and offset.
func TestLimitIterator_Float(t *testing.T) {
	input := &FloatIterator{Points: []query.FloatPoint{
		{Name: "air", Time: 0, Value: 1},
		{Name: "air", Time: 5, Value: 3},
		{Name: "air", Time: 10, Value: 5},
		{Name: "sea", Time: 5, Value: 3},
		{Name: "sea", Time: 7, Value: 8},
	}}

	itr := query.NewLimitIterator(input, query.IteratorOptions{
		Limit:  1,
		Offset: 1,
	})

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if !deep.Equal(a, [][]query.Point{
		{&query.FloatPoint{Name: "air", Time: 5, Value: 3}},
		{&query.FloatPoint{Name: "sea", Time: 7, Value: 8}},
	}) {
		t.Fatalf("unexpected points: %s", spew.Sdump(a))
	}

	if !input.Closed {
		t.Error("iterator not closed")
	}
}

// Ensure limit iterators work with limit and offset.
func TestLimitIterator_Integer(t *testing.T) {
	input := &IntegerIterator{Points: []query.IntegerPoint{
		{Name: "air", Time: 0, Value: 1},
		{Name: "air", Time: 5, Value: 3},
		{Name: "air", Time: 10, Value: 5},
		{Name: "sea", Time: 5, Value: 3},
		{Name: "sea", Time: 7, Value: 8},
	}}

	itr := query.NewLimitIterator(input, query.IteratorOptions{
		Limit:  1,
		Offset: 1,
	})

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if !deep.Equal(a, [][]query.Point{
		{&query.IntegerPoint{Name: "air", Time: 5, Value: 3}},
		{&query.IntegerPoint{Name: "sea", Time: 7, Value: 8}},
	}) {
		t.Fatalf("unexpected points: %s", spew.Sdump(a))
	}

	if !input.Closed {
		t.Error("iterator not closed")
	}
}

// Ensure limit iterators work with limit and offset.
func TestLimitIterator_Unsigned(t *testing.T) {
	input := &UnsignedIterator{Points: []query.UnsignedPoint{
		{Name: "air", Time: 0, Value: 1},
		{Name: "air", Time: 5, Value: 3},
		{Name: "air", Time: 10, Value: 5},
		{Name: "sea", Time: 5, Value: 3},
		{Name: "sea", Time: 7, Value: 8},
	}}

	itr := query.NewLimitIterator(input, query.IteratorOptions{
		Limit:  1,
		Offset: 1,
	})

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if !deep.Equal(a, [][]query.Point{
		{&query.UnsignedPoint{Name: "air", Time: 5, Value: 3}},
		{&query.UnsignedPoint{Name: "sea", Time: 7, Value: 8}},
	}) {
		t.Fatalf("unexpected points: %s", spew.Sdump(a))
	}

	if !input.Closed {
		t.Error("iterator not closed")
	}
}

// Ensure limit iterators work with limit and offset.
func TestLimitIterator_String(t *testing.T) {
	input := &StringIterator{Points: []query.StringPoint{
		{Name: "air", Time: 0, Value: "a"},
		{Name: "air", Time: 5, Value: "b"},
		{Name: "air", Time: 10, Value: "c"},
		{Name: "sea", Time: 5, Value: "d"},
		{Name: "sea", Time: 7, Value: "e"},
	}}

	itr := query.NewLimitIterator(input, query.IteratorOptions{
		Limit:  1,
		Offset: 1,
	})

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if !deep.Equal(a, [][]query.Point{
		{&query.StringPoint{Name: "air", Time: 5, Value: "b"}},
		{&query.StringPoint{Name: "sea", Time: 7, Value: "e"}},
	}) {
		t.Fatalf("unexpected points: %s", spew.Sdump(a))
	}

	if !input.Closed {
		t.Error("iterator not closed")
	}
}

// Ensure limit iterators work with limit and offset.
func TestLimitIterator_Boolean(t *testing.T) {
	input := &BooleanIterator{Points: []query.BooleanPoint{
		{Name: "air", Time: 0, Value: true},
		{Name: "air", Time: 5, Value: false},
		{Name: "air", Time: 10, Value: true},
		{Name: "sea", Time: 5, Value: false},
		{Name: "sea", Time: 7, Value: true},
	}}

	itr := query.NewLimitIterator(input, query.IteratorOptions{
		Limit:  1,
		Offset: 1,
	})

	if a, err := Iterators([]query.Iterator{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if !deep.Equal(a, [][]query.Point{
		{&query.BooleanPoint{Name: "air", Time: 5, Value: false}},
		{&query.BooleanPoint{Name: "sea", Time: 7, Value: true}},
	}) {
		t.Fatalf("unexpected points: %s", spew.Sdump(a))
	}

	if !input.Closed {
		t.Error("iterator not closed")
	}
}

// Ensure limit iterator returns a subset of points.
func TestLimitIterator(t *testing.T) {
	itr := query.NewLimitIterator(
		&FloatIterator{Points: []query.FloatPoint{
			{Time: 0, Value: 0},
			{Time: 1, Value: 1},
			{Time: 2, Value: 2},
			{Time: 3, Value: 3},
		}},
		query.IteratorOptions{
			Limit:     2,
			Offset:    1,
			StartTime: cnosql.MinTime,
			EndTime:   cnosql.MaxTime,
		},
	)

	if a, err := (Iterators{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if !deep.Equal(a, [][]query.Point{
		{&query.FloatPoint{Time: 1, Value: 1}},
		{&query.FloatPoint{Time: 2, Value: 2}},
	}) {
		t.Fatalf("unexpected points: %s", spew.Sdump(a))
	}
}

func TestFillIterator_ImplicitStartTime(t *testing.T) {
	opt := query.IteratorOptions{
		StartTime: cnosql.MinTime,
		EndTime:   mustParseTime("2000-01-01T01:00:00Z").UnixNano() - 1,
		Interval: query.Interval{
			Duration: 20 * time.Minute,
		},
		Ascending: true,
	}
	start := mustParseTime("2000-01-01T00:00:00Z").UnixNano()
	itr := query.NewFillIterator(
		&FloatIterator{Points: []query.FloatPoint{
			{Time: start, Value: 0},
		}},
		nil,
		opt,
	)

	if a, err := (Iterators{itr}).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if !deep.Equal(a, [][]query.Point{
		{&query.FloatPoint{Time: start, Value: 0}},
		{&query.FloatPoint{Time: start + int64(20*time.Minute), Nil: true}},
		{&query.FloatPoint{Time: start + int64(40*time.Minute), Nil: true}},
	}) {
		t.Fatalf("unexpected points: %s", spew.Sdump(a))
	}
}

func TestFillIterator_DST(t *testing.T) {
	for _, tt := range []struct {
		name       string
		start, end time.Time
		points     []time.Duration
		opt        query.IteratorOptions
	}{
		{
			name:  "Start_GroupByDay_Ascending",
			start: mustParseTime("2000-04-01T00:00:00-08:00"),
			end:   mustParseTime("2000-04-05T00:00:00-07:00"),
			points: []time.Duration{
				24 * time.Hour,
				47 * time.Hour,
				71 * time.Hour,
			},
			opt: query.IteratorOptions{
				Interval: query.Interval{
					Duration: 24 * time.Hour,
				},
				Location:  LosAngeles,
				Ascending: true,
			},
		},
		{
			name:  "Start_GroupByDay_Descending",
			start: mustParseTime("2000-04-01T00:00:00-08:00"),
			end:   mustParseTime("2000-04-05T00:00:00-07:00"),
			points: []time.Duration{
				71 * time.Hour,
				47 * time.Hour,
				24 * time.Hour,
			},
			opt: query.IteratorOptions{
				Interval: query.Interval{
					Duration: 24 * time.Hour,
				},
				Location:  LosAngeles,
				Ascending: false,
			},
		},
		{
			name:  "Start_GroupByHour_Ascending",
			start: mustParseTime("2000-04-02T00:00:00-08:00"),
			end:   mustParseTime("2000-04-02T05:00:00-07:00"),
			points: []time.Duration{
				1 * time.Hour,
				2 * time.Hour,
				3 * time.Hour,
			},
			opt: query.IteratorOptions{
				Interval: query.Interval{
					Duration: 1 * time.Hour,
				},
				Location:  LosAngeles,
				Ascending: true,
			},
		},
		{
			name:  "Start_GroupByHour_Descending",
			start: mustParseTime("2000-04-02T00:00:00-08:00"),
			end:   mustParseTime("2000-04-02T05:00:00-07:00"),
			points: []time.Duration{
				3 * time.Hour,
				2 * time.Hour,
				1 * time.Hour,
			},
			opt: query.IteratorOptions{
				Interval: query.Interval{
					Duration: 1 * time.Hour,
				},
				Location:  LosAngeles,
				Ascending: false,
			},
		},
		{
			name:  "Start_GroupBy2Hour_Ascending",
			start: mustParseTime("2000-04-02T00:00:00-08:00"),
			end:   mustParseTime("2000-04-02T07:00:00-07:00"),
			points: []time.Duration{
				2 * time.Hour,
				3 * time.Hour,
				5 * time.Hour,
			},
			opt: query.IteratorOptions{
				Interval: query.Interval{
					Duration: 2 * time.Hour,
				},
				Location:  LosAngeles,
				Ascending: true,
			},
		},
		{
			name:  "Start_GroupBy2Hour_Descending",
			start: mustParseTime("2000-04-02T00:00:00-08:00"),
			end:   mustParseTime("2000-04-02T07:00:00-07:00"),
			points: []time.Duration{
				5 * time.Hour,
				3 * time.Hour,
				2 * time.Hour,
			},
			opt: query.IteratorOptions{
				Interval: query.Interval{
					Duration: 2 * time.Hour,
				},
				Location:  LosAngeles,
				Ascending: false,
			},
		},
		{
			name:  "End_GroupByDay_Ascending",
			start: mustParseTime("2000-10-28T00:00:00-07:00"),
			end:   mustParseTime("2000-11-01T00:00:00-08:00"),
			points: []time.Duration{
				24 * time.Hour,
				49 * time.Hour,
				73 * time.Hour,
			},
			opt: query.IteratorOptions{
				Interval: query.Interval{
					Duration: 24 * time.Hour,
				},
				Location:  LosAngeles,
				Ascending: true,
			},
		},
		{
			name:  "End_GroupByDay_Descending",
			start: mustParseTime("2000-10-28T00:00:00-07:00"),
			end:   mustParseTime("2000-11-01T00:00:00-08:00"),
			points: []time.Duration{
				73 * time.Hour,
				49 * time.Hour,
				24 * time.Hour,
			},
			opt: query.IteratorOptions{
				Interval: query.Interval{
					Duration: 24 * time.Hour,
				},
				Location:  LosAngeles,
				Ascending: false,
			},
		},
		{
			name:  "End_GroupByHour_Ascending",
			start: mustParseTime("2000-10-29T00:00:00-07:00"),
			end:   mustParseTime("2000-10-29T03:00:00-08:00"),
			points: []time.Duration{
				1 * time.Hour,
				2 * time.Hour,
				3 * time.Hour,
			},
			opt: query.IteratorOptions{
				Interval: query.Interval{
					Duration: 1 * time.Hour,
				},
				Location:  LosAngeles,
				Ascending: true,
			},
		},
		{
			name:  "End_GroupByHour_Descending",
			start: mustParseTime("2000-10-29T00:00:00-07:00"),
			end:   mustParseTime("2000-10-29T03:00:00-08:00"),
			points: []time.Duration{
				3 * time.Hour,
				2 * time.Hour,
				1 * time.Hour,
			},
			opt: query.IteratorOptions{
				Interval: query.Interval{
					Duration: 1 * time.Hour,
				},
				Location:  LosAngeles,
				Ascending: false,
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			opt := tt.opt
			opt.StartTime = tt.start.UnixNano()
			opt.EndTime = tt.end.UnixNano() - 1

			points := make([][]query.Point, 0, len(tt.points)+1)
			if opt.Ascending {
				points = append(points, []query.Point{
					&query.FloatPoint{
						Time: tt.start.UnixNano(),
					},
				})
			}
			for _, d := range tt.points {
				points = append(points, []query.Point{
					&query.FloatPoint{
						Time: tt.start.Add(d).UnixNano(),
						Nil:  true,
					},
				})
			}
			if !opt.Ascending {
				points = append(points, []query.Point{
					&query.FloatPoint{
						Time: tt.start.UnixNano(),
					},
				})
			}
			itr := query.NewFillIterator(
				&FloatIterator{Points: []query.FloatPoint{{Time: tt.start.UnixNano(), Value: 0}}},
				nil,
				opt,
			)

			if a, err := (Iterators{itr}).ReadAll(); err != nil {
				t.Fatalf("unexpected error: %s", err)
			} else if !deep.Equal(a, points) {
				t.Fatalf("unexpected points: %s", spew.Sdump(a))
			}
		})
	}
}

func TestIteratorOptions_Window_Interval(t *testing.T) {
	opt := query.IteratorOptions{
		Interval: query.Interval{
			Duration: 10,
		},
	}

	start, end := opt.Window(4)
	if start != 0 {
		t.Errorf("expected start to be 0, got %d", start)
	}
	if end != 10 {
		t.Errorf("expected end to be 10, got %d", end)
	}
}

func TestIteratorOptions_Window_Offset(t *testing.T) {
	opt := query.IteratorOptions{
		Interval: query.Interval{
			Duration: 10,
			Offset:   8,
		},
	}

	start, end := opt.Window(14)
	if start != 8 {
		t.Errorf("expected start to be 8, got %d", start)
	}
	if end != 18 {
		t.Errorf("expected end to be 18, got %d", end)
	}
}

func TestIteratorOptions_Window_Default(t *testing.T) {
	opt := query.IteratorOptions{
		StartTime: 0,
		EndTime:   60,
	}

	start, end := opt.Window(34)
	if start != 0 {
		t.Errorf("expected start to be 0, got %d", start)
	}
	if end != 61 {
		t.Errorf("expected end to be 61, got %d", end)
	}
}

func TestIteratorOptions_Window_Location(t *testing.T) {
	for _, tt := range []struct {
		now        time.Time
		start, end time.Time
		interval   time.Duration
	}{
		{
			now:      mustParseTime("2000-04-02T12:14:15-07:00"),
			start:    mustParseTime("2000-04-02T00:00:00-08:00"),
			end:      mustParseTime("2000-04-03T00:00:00-07:00"),
			interval: 24 * time.Hour,
		},
		{
			now:      mustParseTime("2000-04-02T01:17:12-08:00"),
			start:    mustParseTime("2000-04-02T00:00:00-08:00"),
			end:      mustParseTime("2000-04-03T00:00:00-07:00"),
			interval: 24 * time.Hour,
		},
		{
			now:      mustParseTime("2000-04-02T01:14:15-08:00"),
			start:    mustParseTime("2000-04-02T00:00:00-08:00"),
			end:      mustParseTime("2000-04-02T03:00:00-07:00"),
			interval: 2 * time.Hour,
		},
		{
			now:      mustParseTime("2000-04-02T03:17:12-07:00"),
			start:    mustParseTime("2000-04-02T03:00:00-07:00"),
			end:      mustParseTime("2000-04-02T04:00:00-07:00"),
			interval: 2 * time.Hour,
		},
		{
			now:      mustParseTime("2000-04-02T01:14:15-08:00"),
			start:    mustParseTime("2000-04-02T01:00:00-08:00"),
			end:      mustParseTime("2000-04-02T03:00:00-07:00"),
			interval: 1 * time.Hour,
		},
		{
			now:      mustParseTime("2000-04-02T03:17:12-07:00"),
			start:    mustParseTime("2000-04-02T03:00:00-07:00"),
			end:      mustParseTime("2000-04-02T04:00:00-07:00"),
			interval: 1 * time.Hour,
		},
		{
			now:      mustParseTime("2000-10-29T12:14:15-08:00"),
			start:    mustParseTime("2000-10-29T00:00:00-07:00"),
			end:      mustParseTime("2000-10-30T00:00:00-08:00"),
			interval: 24 * time.Hour,
		},
		{
			now:      mustParseTime("2000-10-29T01:17:12-07:00"),
			start:    mustParseTime("2000-10-29T00:00:00-07:00"),
			end:      mustParseTime("2000-10-30T00:00:00-08:00"),
			interval: 24 * time.Hour,
		},
		{
			now:      mustParseTime("2000-10-29T01:14:15-07:00"),
			start:    mustParseTime("2000-10-29T00:00:00-07:00"),
			end:      mustParseTime("2000-10-29T02:00:00-08:00"),
			interval: 2 * time.Hour,
		},
		{
			now:      mustParseTime("2000-10-29T03:17:12-08:00"),
			start:    mustParseTime("2000-10-29T02:00:00-08:00"),
			end:      mustParseTime("2000-10-29T04:00:00-08:00"),
			interval: 2 * time.Hour,
		},
		{
			now:      mustParseTime("2000-10-29T01:14:15-07:00"),
			start:    mustParseTime("2000-10-29T01:00:00-07:00"),
			end:      mustParseTime("2000-10-29T01:00:00-08:00"),
			interval: 1 * time.Hour,
		},
		{
			now:      mustParseTime("2000-10-29T02:17:12-07:00"),
			start:    mustParseTime("2000-10-29T02:00:00-07:00"),
			end:      mustParseTime("2000-10-29T03:00:00-07:00"),
			interval: 1 * time.Hour,
		},
	} {
		t.Run(fmt.Sprintf("%s/%s", tt.now, tt.interval), func(t *testing.T) {
			opt := query.IteratorOptions{
				Location: LosAngeles,
				Interval: query.Interval{
					Duration: tt.interval,
				},
			}
			start, end := opt.Window(tt.now.UnixNano())
			if have, want := time.Unix(0, start).In(LosAngeles), tt.start; !have.Equal(want) {
				t.Errorf("unexpected start time: %s != %s", have, want)
			}
			if have, want := time.Unix(0, end).In(LosAngeles), tt.end; !have.Equal(want) {
				t.Errorf("unexpected end time: %s != %s", have, want)
			}
		})
	}
}

func TestIteratorOptions_Window_MinTime(t *testing.T) {
	opt := query.IteratorOptions{
		StartTime: cnosql.MinTime,
		EndTime:   cnosql.MaxTime,
		Interval: query.Interval{
			Duration: time.Hour,
		},
	}
	expected := time.Unix(0, cnosql.MinTime).Add(time.Hour).Truncate(time.Hour)

	start, end := opt.Window(cnosql.MinTime)
	if start != cnosql.MinTime {
		t.Errorf("expected start to be %d, got %d", cnosql.MinTime, start)
	}
	if have, want := end, expected.UnixNano(); have != want {
		t.Errorf("expected end to be %d, got %d", want, have)
	}
}

func TestIteratorOptions_Window_MaxTime(t *testing.T) {
	opt := query.IteratorOptions{
		StartTime: cnosql.MinTime,
		EndTime:   cnosql.MaxTime,
		Interval: query.Interval{
			Duration: time.Hour,
		},
	}
	expected := time.Unix(0, cnosql.MaxTime).Truncate(time.Hour)

	start, end := opt.Window(cnosql.MaxTime)
	if have, want := start, expected.UnixNano(); have != want {
		t.Errorf("expected start to be %d, got %d", want, have)
	}
	if end != cnosql.MaxTime {
		t.Errorf("expected end to be %d, got %d", cnosql.MaxTime, end)
	}
}

func TestIteratorOptions_SeekTime_Ascending(t *testing.T) {
	opt := query.IteratorOptions{
		StartTime: 30,
		EndTime:   60,
		Ascending: true,
	}

	oTime := opt.SeekTime()
	if oTime != 30 {
		t.Errorf("expected time to be 30, got %d", oTime)
	}
}

func TestIteratorOptions_SeekTime_Descending(t *testing.T) {
	opt := query.IteratorOptions{
		StartTime: 30,
		EndTime:   60,
		Ascending: false,
	}

	oTime := opt.SeekTime()
	if oTime != 60 {
		t.Errorf("expected time to be 60, got %d", oTime)
	}
}

func TestIteratorOptions_DerivativeInterval_Default(t *testing.T) {
	opt := query.IteratorOptions{}
	expected := query.Interval{Duration: time.Second}
	actual := opt.DerivativeInterval()
	if actual != expected {
		t.Errorf("expected derivative interval to be %v, got %v", expected, actual)
	}
}

func TestIteratorOptions_DerivativeInterval_GroupBy(t *testing.T) {
	opt := query.IteratorOptions{
		Interval: query.Interval{
			Duration: 10,
			Offset:   2,
		},
	}
	expected := query.Interval{Duration: 10}
	actual := opt.DerivativeInterval()
	if actual != expected {
		t.Errorf("expected derivative interval to be %v, got %v", expected, actual)
	}
}

func TestIteratorOptions_DerivativeInterval_Call(t *testing.T) {
	opt := query.IteratorOptions{
		Expr: &cnosql.Call{
			Name: "mean",
			Args: []cnosql.Expr{
				&cnosql.VarRef{Val: "value"},
				&cnosql.DurationLiteral{Val: 2 * time.Second},
			},
		},
		Interval: query.Interval{
			Duration: 10,
			Offset:   2,
		},
	}
	expected := query.Interval{Duration: 2 * time.Second}
	actual := opt.DerivativeInterval()
	if actual != expected {
		t.Errorf("expected derivative interval to be %v, got %v", expected, actual)
	}
}

func TestIteratorOptions_ElapsedInterval_Default(t *testing.T) {
	opt := query.IteratorOptions{}
	expected := query.Interval{Duration: time.Nanosecond}
	actual := opt.ElapsedInterval()
	if actual != expected {
		t.Errorf("expected elapsed interval to be %v, got %v", expected, actual)
	}
}

func TestIteratorOptions_ElapsedInterval_GroupBy(t *testing.T) {
	opt := query.IteratorOptions{
		Interval: query.Interval{
			Duration: 10,
			Offset:   2,
		},
	}
	expected := query.Interval{Duration: time.Nanosecond}
	actual := opt.ElapsedInterval()
	if actual != expected {
		t.Errorf("expected elapsed interval to be %v, got %v", expected, actual)
	}
}

func TestIteratorOptions_ElapsedInterval_Call(t *testing.T) {
	opt := query.IteratorOptions{
		Expr: &cnosql.Call{
			Name: "mean",
			Args: []cnosql.Expr{
				&cnosql.VarRef{Val: "value"},
				&cnosql.DurationLiteral{Val: 2 * time.Second},
			},
		},
		Interval: query.Interval{
			Duration: 10,
			Offset:   2,
		},
	}
	expected := query.Interval{Duration: 2 * time.Second}
	actual := opt.ElapsedInterval()
	if actual != expected {
		t.Errorf("expected elapsed interval to be %v, got %v", expected, actual)
	}
}

func TestIteratorOptions_IntegralInterval_Default(t *testing.T) {
	opt := query.IteratorOptions{}
	expected := query.Interval{Duration: time.Second}
	actual := opt.IntegralInterval()
	if actual != expected {
		t.Errorf("expected default integral interval to be %v, got %v", expected, actual)
	}
}

// Ensure iterator options can be marshaled to and from a binary format.
func TestIteratorOptions_MarshalBinary(t *testing.T) {
	opt := &query.IteratorOptions{
		Expr: MustParseExpr("count(value)"),
		Aux:  []cnosql.VarRef{{Val: "a"}, {Val: "b"}, {Val: "c"}},
		Interval: query.Interval{
			Duration: 1 * time.Hour,
			Offset:   20 * time.Minute,
		},
		Dimensions: []string{"region", "station"},
		GroupBy: map[string]struct{}{
			"region":  {},
			"station": {},
			"cluster": {},
		},
		Fill:      cnosql.NumberFill,
		FillValue: float64(100),
		Condition: MustParseExpr(`foo = 'bar'`),
		StartTime: 1000,
		EndTime:   2000,
		Ascending: true,
		Limit:     100,
		Offset:    200,
		SLimit:    300,
		SOffset:   400,
		StripName: true,
		Dedupe:    true,
	}

	// Marshal to binary.
	buf, err := opt.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	// Unmarshal back to an object.
	var other query.IteratorOptions
	if err := other.UnmarshalBinary(buf); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(&other, opt) {
		t.Fatalf("unexpected options: %s", spew.Sdump(other))
	}
}

// Ensure iterator can be encoded and decoded over a byte stream.
func TestIterator_EncodeDecode(t *testing.T) {
	var buf bytes.Buffer

	// Create an iterator with several points & stats.
	itr := &FloatIterator{
		Points: []query.FloatPoint{
			{Name: "air", Tags: ParseTags("station=A"), Time: 0, Value: 0},
			{Name: "sea", Tags: ParseTags("station=B"), Time: 1, Value: 10},
		},
		stats: query.IteratorStats{
			SeriesN: 2,
			PointN:  0,
		},
	}

	// Encode to the buffer.
	enc := query.NewIteratorEncoder(&buf)
	enc.StatsInterval = 100 * time.Millisecond
	if err := enc.EncodeIterator(itr); err != nil {
		t.Fatal(err)
	}

	// Decode from the buffer.
	dec := query.NewReaderIterator(context.Background(), &buf, cnosql.Float, itr.Stats())

	// Initial stats should exist immediately.
	fdec := dec.(query.FloatIterator)
	if stats := fdec.Stats(); !reflect.DeepEqual(stats, query.IteratorStats{SeriesN: 2, PointN: 0}) {
		t.Fatalf("unexpected stats(initial): %#v", stats)
	}

	// Read both points.
	if p, err := fdec.Next(); err != nil {
		t.Fatalf("unexpected error(0): %#v", err)
	} else if !reflect.DeepEqual(p, &query.FloatPoint{Name: "air", Tags: ParseTags("station=A"), Time: 0, Value: 0}) {
		t.Fatalf("unexpected point(0); %#v", p)
	}
	if p, err := fdec.Next(); err != nil {
		t.Fatalf("unexpected error(1): %#v", err)
	} else if !reflect.DeepEqual(p, &query.FloatPoint{Name: "sea", Tags: ParseTags("station=B"), Time: 1, Value: 10}) {
		t.Fatalf("unexpected point(1); %#v", p)
	}
	if p, err := fdec.Next(); err != nil {
		t.Fatalf("unexpected error(eof): %#v", err)
	} else if p != nil {
		t.Fatalf("unexpected point(eof); %#v", p)
	}
}

// Iterators is a test wrapper for iterators.
type Iterators []query.Iterator

// Next returns the next value from each iterator.
// Returns nil if any iterator returns a nil.
func (itrs Iterators) Next() ([]query.Point, error) {
	a := make([]query.Point, len(itrs))
	for i, itr := range itrs {
		switch itr := itr.(type) {
		case query.FloatIterator:
			fp, err := itr.Next()
			if fp == nil || err != nil {
				return nil, err
			}
			a[i] = fp
		case query.IntegerIterator:
			ip, err := itr.Next()
			if ip == nil || err != nil {
				return nil, err
			}
			a[i] = ip
		case query.UnsignedIterator:
			up, err := itr.Next()
			if up == nil || err != nil {
				return nil, err
			}
			a[i] = up
		case query.StringIterator:
			sp, err := itr.Next()
			if sp == nil || err != nil {
				return nil, err
			}
			a[i] = sp
		case query.BooleanIterator:
			bp, err := itr.Next()
			if bp == nil || err != nil {
				return nil, err
			}
			a[i] = bp
		default:
			panic(fmt.Sprintf("iterator type not supported: %T", itr))
		}
	}
	return a, nil
}

// ReadAll reads all points from all iterators.
func (itrs Iterators) ReadAll() ([][]query.Point, error) {
	var a [][]query.Point

	// Read from every iterator until a nil is encountered.
	for {
		points, err := itrs.Next()
		if err != nil {
			return nil, err
		} else if points == nil {
			break
		}
		a = append(a, query.Points(points).Clone())
	}

	// Close all iterators.
	query.Iterators(itrs).Close()

	return a, nil
}

// Test implementation of cnosql.FloatIterator
type FloatIterator struct {
	Context context.Context
	Points  []query.FloatPoint
	Closed  bool
	Delay   time.Duration
	stats   query.IteratorStats
	point   query.FloatPoint
}

func (itr *FloatIterator) Stats() query.IteratorStats { return itr.stats }
func (itr *FloatIterator) Close() error               { itr.Closed = true; return nil }

// Next returns the next value and shifts it off the beginning of the points slice.
func (itr *FloatIterator) Next() (*query.FloatPoint, error) {
	if len(itr.Points) == 0 || itr.Closed {
		return nil, nil
	}

	// If we have asked for a delay, then delay the returning of the point
	// until either an (optional) context is done or the time has passed.
	if itr.Delay > 0 {
		var done <-chan struct{}
		if itr.Context != nil {
			done = itr.Context.Done()
		}

		timer := time.NewTimer(itr.Delay)
		select {
		case <-timer.C:
		case <-done:
			timer.Stop()
			return nil, itr.Context.Err()
		}
	}
	v := &itr.Points[0]
	itr.Points = itr.Points[1:]

	// Copy the returned point into a static point that we return.
	// This actual storage engine returns a point from the same memory location
	// so we need to test that the query engine does not misuse this memory.
	itr.point.Name = v.Name
	itr.point.Tags = v.Tags
	itr.point.Time = v.Time
	itr.point.Value = v.Value
	itr.point.Nil = v.Nil
	if len(itr.point.Aux) != len(v.Aux) {
		itr.point.Aux = make([]interface{}, len(v.Aux))
	}
	copy(itr.point.Aux, v.Aux)
	return &itr.point, nil
}

func FloatIterators(inputs []*FloatIterator) []query.Iterator {
	itrs := make([]query.Iterator, len(inputs))
	for i := range itrs {
		itrs[i] = query.Iterator(inputs[i])
	}
	return itrs
}

// Test implementation of query.IntegerIterator
type IntegerIterator struct {
	Points []query.IntegerPoint
	Closed bool
	stats  query.IteratorStats
	point  query.IntegerPoint
}

func (itr *IntegerIterator) Stats() query.IteratorStats { return itr.stats }
func (itr *IntegerIterator) Close() error               { itr.Closed = true; return nil }

// Next returns the next value and shifts it off the beginning of the points slice.
func (itr *IntegerIterator) Next() (*query.IntegerPoint, error) {
	if len(itr.Points) == 0 || itr.Closed {
		return nil, nil
	}

	v := &itr.Points[0]
	itr.Points = itr.Points[1:]

	// Copy the returned point into a static point that we return.
	// This actual storage engine returns a point from the same memory location
	// so we need to test that the query engine does not misuse this memory.
	itr.point.Name = v.Name
	itr.point.Tags = v.Tags
	itr.point.Time = v.Time
	itr.point.Value = v.Value
	itr.point.Nil = v.Nil
	if len(itr.point.Aux) != len(v.Aux) {
		itr.point.Aux = make([]interface{}, len(v.Aux))
	}
	copy(itr.point.Aux, v.Aux)
	return &itr.point, nil
}

func IntegerIterators(inputs []*IntegerIterator) []query.Iterator {
	itrs := make([]query.Iterator, len(inputs))
	for i := range itrs {
		itrs[i] = query.Iterator(inputs[i])
	}
	return itrs
}

// Test implementation of query.UnsignedIterator
type UnsignedIterator struct {
	Points []query.UnsignedPoint
	Closed bool
	stats  query.IteratorStats
	point  query.UnsignedPoint
}

func (itr *UnsignedIterator) Stats() query.IteratorStats { return itr.stats }
func (itr *UnsignedIterator) Close() error               { itr.Closed = true; return nil }

// Next returns the next value and shifts it off the beginning of the points slice.
func (itr *UnsignedIterator) Next() (*query.UnsignedPoint, error) {
	if len(itr.Points) == 0 || itr.Closed {
		return nil, nil
	}

	v := &itr.Points[0]
	itr.Points = itr.Points[1:]

	// Copy the returned point into a static point that we return.
	// This actual storage engine returns a point from the same memory location
	// so we need to test that the query engine does not misuse this memory.
	itr.point.Name = v.Name
	itr.point.Tags = v.Tags
	itr.point.Time = v.Time
	itr.point.Value = v.Value
	itr.point.Nil = v.Nil
	if len(itr.point.Aux) != len(v.Aux) {
		itr.point.Aux = make([]interface{}, len(v.Aux))
	}
	copy(itr.point.Aux, v.Aux)
	return &itr.point, nil
}

func UnsignedIterators(inputs []*UnsignedIterator) []query.Iterator {
	itrs := make([]query.Iterator, len(inputs))
	for i := range itrs {
		itrs[i] = query.Iterator(inputs[i])
	}
	return itrs
}

// Test implementation of query.StringIterator
type StringIterator struct {
	Points []query.StringPoint
	Closed bool
	stats  query.IteratorStats
	point  query.StringPoint
}

func (itr *StringIterator) Stats() query.IteratorStats { return itr.stats }
func (itr *StringIterator) Close() error               { itr.Closed = true; return nil }

// Next returns the next value and shifts it off the beginning of the points slice.
func (itr *StringIterator) Next() (*query.StringPoint, error) {
	if len(itr.Points) == 0 || itr.Closed {
		return nil, nil
	}

	v := &itr.Points[0]
	itr.Points = itr.Points[1:]

	// Copy the returned point into a static point that we return.
	// This actual storage engine returns a point from the same memory location
	// so we need to test that the query engine does not misuse this memory.
	itr.point.Name = v.Name
	itr.point.Tags = v.Tags
	itr.point.Time = v.Time
	itr.point.Value = v.Value
	itr.point.Nil = v.Nil
	if len(itr.point.Aux) != len(v.Aux) {
		itr.point.Aux = make([]interface{}, len(v.Aux))
	}
	copy(itr.point.Aux, v.Aux)
	return &itr.point, nil
}

func StringIterators(inputs []*StringIterator) []query.Iterator {
	itrs := make([]query.Iterator, len(inputs))
	for i := range itrs {
		itrs[i] = query.Iterator(inputs[i])
	}
	return itrs
}

// Test implementation of query.BooleanIterator
type BooleanIterator struct {
	Points []query.BooleanPoint
	Closed bool
	stats  query.IteratorStats
	point  query.BooleanPoint
}

func (itr *BooleanIterator) Stats() query.IteratorStats { return itr.stats }
func (itr *BooleanIterator) Close() error               { itr.Closed = true; return nil }

// Next returns the next value and shifts it off the beginning of the points slice.
func (itr *BooleanIterator) Next() (*query.BooleanPoint, error) {
	if len(itr.Points) == 0 || itr.Closed {
		return nil, nil
	}

	v := &itr.Points[0]
	itr.Points = itr.Points[1:]

	// Copy the returned point into a static point that we return.
	// This actual storage engine returns a point from the same memory location
	// so we need to test that the query engine does not misuse this memory.
	itr.point.Name = v.Name
	itr.point.Tags = v.Tags
	itr.point.Time = v.Time
	itr.point.Value = v.Value
	itr.point.Nil = v.Nil
	if len(itr.point.Aux) != len(v.Aux) {
		itr.point.Aux = make([]interface{}, len(v.Aux))
	}
	copy(itr.point.Aux, v.Aux)
	return &itr.point, nil
}

func BooleanIterators(inputs []*BooleanIterator) []query.Iterator {
	itrs := make([]query.Iterator, len(inputs))
	for i := range itrs {
		itrs[i] = query.Iterator(inputs[i])
	}
	return itrs
}

// MustParseSelectStatement parses a select statement. Panic on error.
func MustParseSelectStatement(s string) *cnosql.SelectStatement {
	stmt, err := cnosql.NewParser(strings.NewReader(s)).ParseStatement()
	if err != nil {
		panic(err)
	}
	return stmt.(*cnosql.SelectStatement)
}
