package query_test

import (
	"context"
	"github.com/cnosdb/cnosdb/vend/cnosql"
	"github.com/cnosdb/cnosdb/vend/db/query"
	"testing"
	"time"
)

func TestPointLimitMonitor(t *testing.T) {
	t.Parallel()

	stmt := MustParseSelectStatement(`SELECT mean(value) FROM air`)

	// Create a new task manager so we can use the query task as a monitor.
	taskManager := query.NewTaskManager()
	ctx, detach, err := taskManager.AttachQuery(&cnosql.Query{
		Statements: []cnosql.Statement{stmt},
	}, query.ExecutionOptions{}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer detach()

	shardMapper := ShardMapper{
		MapShardsFn: func(sources cnosql.Sources, t cnosql.TimeRange) query.ShardGroup {
			return &ShardGroup{
				CreateIteratorFn: func(ctx context.Context, m *cnosql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
					return &FloatIterator{
						Points: []query.FloatPoint{
							{Name: "air", Value: 35},
						},
						Context: ctx,
						Delay:   2 * time.Second,
						stats: query.IteratorStats{
							PointN: 10,
						},
					}, nil
				},
				Fields: map[string]cnosql.DataType{
					"value": cnosql.Float,
				},
			}
		},
	}

	cur, err := query.Select(ctx, stmt, &shardMapper, query.SelectOptions{
		MaxPointN: 1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if err := query.DrainCursor(cur); err == nil {
		t.Fatalf("expected an error")
	} else if got, want := err.Error(), "max-select-point limit exceeed: (10/1)"; got != want {
		t.Fatalf("unexpected error: got=%v want=%v", got, want)
	}
}
