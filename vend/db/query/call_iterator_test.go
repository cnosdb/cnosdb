package query_test

import (
	"context"
	"github.com/cnosdb/cnosdb/vend/db/query"
	"testing"
	"time"
)

func TestCallIterator_Count_Float(t *testing.T) {

}

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
