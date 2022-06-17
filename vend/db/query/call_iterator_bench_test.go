package query_test

import (
	"github.com/cnosdb/cnosdb/vend/cnosql"
	"github.com/cnosdb/cnosdb/vend/db/query"
	"testing"
)

func BenchmarkCountIterator_1K(b *testing.B)   { benchmarkCountIterator(b, 1000) }
func BenchmarkCountIterator_100K(b *testing.B) { benchmarkCountIterator(b, 100000) }
func BenchmarkCountIterator_1M(b *testing.B)   { benchmarkCountIterator(b, 1000000) }

func benchmarkCountIterator(b *testing.B, pointN int) {
	benchmarkCallIterator(b, query.IteratorOptions{
		Expr:      MustParseExpr("count(value)"),
		StartTime: cnosql.MinTime,
		EndTime:   cnosql.MaxTime,
	}, pointN)
}

func benchmarkCallIterator(b *testing.B, opt query.IteratorOptions, pointN int) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Create a lightweight point generator.
		p := query.FloatPoint{Name: "air", Value: 100}
		input := FloatPointGenerator{
			N:  pointN,
			Fn: func(i int) *query.FloatPoint { return &p },
		}

		// Execute call against input.
		itr, err := query.NewCallIterator(&input, opt)
		if err != nil {
			b.Fatal(err)
		}
		query.DrainIterator(itr)
	}
}

func BenchmarkSampleIterator_1k(b *testing.B)   { benchmarkSampleIterator(b, 1000) }
func BenchmarkSampleIterator_100k(b *testing.B) { benchmarkSampleIterator(b, 100000) }
func BenchmarkSampleIterator_1M(b *testing.B)   { benchmarkSampleIterator(b, 1000000) }

func benchmarkSampleIterator(b *testing.B, pointN int) {
	b.ReportAllocs()

	// Create a lightweight point generator.
	p := query.FloatPoint{Name: "air"}
	input := FloatPointGenerator{
		N: pointN,
		Fn: func(i int) *query.FloatPoint {
			p.Value = float64(i)
			return &p
		},
	}

	for i := 0; i < b.N; i++ {
		// Execute call against input.
		itr, err := query.NewSampleIterator(&input, query.IteratorOptions{}, 100)
		if err != nil {
			b.Fatal(err)
		}
		query.DrainIterator(itr)
	}
}

func BenchmarkDistinctIterator_1K(b *testing.B)   { benchmarkDistinctIterator(b, 1000) }
func BenchmarkDistinctIterator_100K(b *testing.B) { benchmarkDistinctIterator(b, 100000) }
func BenchmarkDistinctIterator_1M(b *testing.B)   { benchmarkDistinctIterator(b, 1000000) }

func benchmarkDistinctIterator(b *testing.B, pointN int) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Create a lightweight point generator.
		p := query.FloatPoint{Name: "air"}
		input := FloatPointGenerator{
			N: pointN,
			Fn: func(i int) *query.FloatPoint {
				p.Value = float64(i % 10)
				return &p
			},
		}

		// Execute call against input.
		itr, err := query.NewDistinctIterator(&input, query.IteratorOptions{})
		if err != nil {
			b.Fatal(err)
		}
		query.DrainIterator(itr)
	}
}

func BenchmarkModeIterator_1K(b *testing.B)   { benchmarkModeIterator(b, 1000) }
func BenchmarkModeIterator_100K(b *testing.B) { benchmarkModeIterator(b, 100000) }
func BenchmarkModeIterator_1M(b *testing.B)   { benchmarkModeIterator(b, 1000000) }

func benchmarkModeIterator(b *testing.B, pointN int) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Create a lightweight point generator.
		p := query.FloatPoint{Name: "air"}
		input := FloatPointGenerator{
			N: pointN,
			Fn: func(i int) *query.FloatPoint {
				p.Value = float64(10)
				return &p
			},
		}

		// Execute call against input.
		itr, err := query.NewModeIterator(&input, query.IteratorOptions{})
		if err != nil {
			b.Fatal(err)
		}
		query.DrainIterator(itr)
	}
}

type FloatPointGenerator struct {
	i  int
	N  int
	Fn func(i int) *query.FloatPoint
}

func (g *FloatPointGenerator) Close() error               { return nil }
func (g *FloatPointGenerator) Stats() query.IteratorStats { return query.IteratorStats{} }

func (g *FloatPointGenerator) Next() (*query.FloatPoint, error) {
	if g.i == g.N {
		return nil, nil
	}
	p := g.Fn(g.i)
	g.i++
	return p, nil
}
