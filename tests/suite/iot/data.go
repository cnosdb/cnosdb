package iot

import (
	"fmt"
	"github.com/cnosdb/cnosdb/tests"
	"github.com/cnosdb/cnosdb/vend/db/models"
	"sync"
	"time"
)

const (
	db = "db0"
	rp = "rp0"
)

type Generator struct {
	Server   tests.Server
	Parallel int
	Scale    int // Total number of trucks
	Seed     int64
	Interval time.Duration
	Start    time.Time
	End      time.Time
	trucks   []truckGen
	wg       sync.WaitGroup
}

func (g *Generator) Init() {
	g.trucks = make([]truckGen, g.Scale)
	for i := 0; i < g.Scale; i++ {
		g.trucks[i] = truckGen{Num: i, Seed: g.Seed + int64(i)}
		g.trucks[i].Init()
	}
}

func (g *Generator) Run() {
	runners := make([]genRunner, g.Parallel)
	g.wg.Add(g.Parallel)

	size := g.Scale / g.Parallel
	for i := 0; i < g.Parallel; i++ {
		begin := i * size
		end := (i + 1) * size
		if i+1 == g.Parallel {
			end = g.Scale - 1
		}
		runners[i] = genRunner{
			Server:   g.Server,
			Wg:       &g.wg,
			Interval: g.Interval,
			Start:    g.Start,
			End:      g.End,
			Trucks:   g.trucks[begin:end],
		}
		go runners[i].Run()
	}
	g.wg.Wait()
}

type genRunner struct {
	Server   tests.Server
	Wg       *sync.WaitGroup
	Interval time.Duration
	Start    time.Time
	End      time.Time
	Trucks   []truckGen
}

func (r *genRunner) Run() {
	i := 0
	for now := r.Start; now.Before(r.End); now = now.Add(r.Interval) {
		for _, t := range r.Trucks {
			i++
			if i%100 == 0 {
				fmt.Printf("Count: %d, Time: %s, Truck: %d\n", i, now.String(), t.Num)
			}
			_ = r.Server.WritePoints(db, rp, models.ConsistencyLevelAll, nil, t.New(now))
		}
	}
	r.Wg.Done()
}
