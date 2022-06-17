package query_test

import (
	"context"
	"fmt"
	"github.com/cnosdb/cnosdb/vend/cnosql"
	"github.com/cnosdb/cnosdb/vend/db/query"
	"github.com/davecgh/go-spew/spew"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

func BenchmarkSelect_Raw_1K(b *testing.B) { benchmarkSelectRaw(b, 1000) }

func BenchmarkSelect_Raw_100K(b *testing.B) { benchmarkSelectRaw(b, 1000000) }

func BenchmarkSelect_Dedupe_1K(b *testing.B) { benchmarkSelectDedupe(b, 1000, 100) }

func BenchmarkSelect_Top_1K(b *testing.B) { benchmarkSelectTop(b, 1000, 1000) }

func benchmarkSelectRaw(b *testing.B, pointN int) {
	benchmarkSelect(b, MustParseSelectStatement(`SELECT fval FROM air`), NewRawBenchmarkIteratorCreator(pointN))
}

func benchmarkSelect(b *testing.B, stmt *cnosql.SelectStatement, shardMapper query.ShardMapper) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		cur, err := query.Select(context.Background(), stmt, shardMapper, query.SelectOptions{})
		if err != nil {
			b.Fatal(err)
		}
		query.DrainCursor(cur)
	}
}

// NewRawBenchmarkIteratorCreator returns a new mock iterator creator with generated fields.
func NewRawBenchmarkIteratorCreator(pointN int) query.ShardMapper {
	return &ShardMapper{
		MapShardsFn: func(sources cnosql.Sources, t cnosql.TimeRange) query.ShardGroup {
			return &ShardGroup{
				Fields: map[string]cnosql.DataType{
					"fval": cnosql.Float,
				},
				CreateIteratorFn: func(ctx context.Context, m *cnosql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
					if opt.Expr != nil {
						panic("unexpected expression")
					}

					p := query.FloatPoint{
						Name: "air",
						Aux:  make([]interface{}, len(opt.Aux)),
					}

					for i := range opt.Aux {
						switch opt.Aux[i].Val {
						case "fval":
							p.Aux[i] = float64(100)
						default:
							panic("unknown iterator expr: " + opt.Expr.String())
						}
					}

					return &FloatPointGenerator{N: pointN, Fn: func(i int) *query.FloatPoint {
						p.Time = int64(time.Duration(i) * (10 * time.Second))
						return &p
					}}, nil
				},
			}
		},
	}
}

func benchmarkSelectDedupe(b *testing.B, seriesN, pointsPerSeries int) {
	stmt := MustParseSelectStatement(`SELECT sval::string FROM air`)
	stmt.Dedupe = true

	shardMapper := ShardMapper{
		MapShardsFn: func(sources cnosql.Sources, t cnosql.TimeRange) query.ShardGroup {
			return &ShardGroup{
				Fields: map[string]cnosql.DataType{
					"sval": cnosql.String,
				},
				CreateIteratorFn: func(ctx context.Context, m *cnosql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
					if opt.Expr != nil {
						panic("unexpected expression")
					}

					p := query.FloatPoint{
						Name: "tags",
						Aux:  []interface{}{nil},
					}

					return &FloatPointGenerator{N: seriesN * pointsPerSeries, Fn: func(i int) *query.FloatPoint {
						p.Aux[0] = fmt.Sprintf("server%d", i%seriesN)
						return &p
					}}, nil
				},
			}
		},
	}

	b.ResetTimer()
	benchmarkSelect(b, stmt, &shardMapper)
}

func benchmarkSelectTop(b *testing.B, seriesN, pointsPerSeries int) {
	stmt := MustParseSelectStatement(`SELECT top(fval::float, 10) FROM air`)

	shardMapper := ShardMapper{
		MapShardsFn: func(sources cnosql.Sources, t cnosql.TimeRange) query.ShardGroup {
			return &ShardGroup{
				Fields: map[string]cnosql.DataType{
					"fval": cnosql.Float,
				},
				CreateIteratorFn: func(ctx context.Context, m *cnosql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
					if m.Name != "air" {
						b.Fatalf("unexpected source: %s", m.Name)
					}
					if !reflect.DeepEqual(opt.Expr, MustParseExpr(`fval::float`)) {
						b.Fatalf("unexpected expr: %s", spew.Sdump(opt.Expr))
					}

					p := query.FloatPoint{
						Name: "air",
					}

					return &FloatPointGenerator{N: seriesN * pointsPerSeries, Fn: func(i int) *query.FloatPoint {
						p.Value = float64(rand.Int63())
						p.Time = int64(time.Duration(i) * (10 * time.Second))
						return &p
					}}, nil
				},
			}
		},
	}

	b.ResetTimer()
	benchmarkSelect(b, stmt, &shardMapper)
}
