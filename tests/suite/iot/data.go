package iot

import (
	"fmt"
	"github.com/cnosdb/cnosdb/tests"
	"github.com/cnosdb/cnosdb/tests/suite"
	"github.com/cnosdb/cnosdb/vend/db/models"
	"sync"
	"time"
)

const (
	db = "db0"
	rp = "rp0"
)

type Generator struct {
	Parallel int
	Scale    int // Total number of trucks
	Seed     int64
	Interval time.Duration
	Start    time.Time
	End      time.Time
	wg       sync.WaitGroup
}

func (g *Generator) Run(server tests.Server) {
	trucks := make([]truckGen, g.Scale)
	for i := 0; i < g.Scale; i++ {
		trucks[i] = truckGen{Num: i, Seed: g.Seed + int64(i)}
		trucks[i].Init()
	}

	g.wg.Add(g.Parallel)
	size := g.Scale / g.Parallel
	for i := 0; i < g.Parallel; i++ {
		begin := i * size
		end := (i + 1) * size
		if i+1 == g.Parallel {
			end = g.Scale - 1
		}
		go g.run(trucks[begin:end], server)
	}
	g.wg.Wait()
}

func (g *Generator) run(trucks []truckGen, server tests.Server) {
	i := 0
	for now := g.Start; now.Before(g.End); now = now.Add(g.Interval) {
		for _, t := range trucks {
			i++
			if i%1000 == 0 {
				fmt.Printf("Count: %d, Time: %s, Truck: %d\n", i, now.String(), t.Num)
			}
			suite.PanicErr(server.WritePoints(db, rp, models.ConsistencyLevelAll, nil, t.New(now)))
		}
	}
	g.wg.Done()
}
