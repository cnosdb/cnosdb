package suite

import (
	"errors"
	"fmt"
	"github.com/cnosdb/cnosdb/tests"
	"sync"
	"sync/atomic"
	"testing"
)

type Step struct {
	Name   string
	Query  string
	Result Results
}

func (s *Step) Run(caseName string, server tests.Server, t *testing.T) {
	te := func(e error) {
		if e != nil {
			t.Errorf("Case: %s, Step: %s, Error: %v", caseName, s.Name, e)
		}
	}
	resStr, err := server.Query(s.Query)
	te(err)
	var res Results
	te(res.Unmarshal(resStr))
	if !s.Result.Equal(res) {
		te(errors.New("Mismatch. "))
	}
}

func (s *Step) ResCode(caseName string, server tests.Server) {
	resStr, err := server.Query(s.Query)
	if err != nil {
		panic(err)
	}
	if err = s.Result.Unmarshal(resStr); err != nil {
		panic(err)
	}
	s.Result.ToCode(fmt.Sprintf("%s|%s", caseName, s.Name))
}

type Case struct {
	Name  string
	Steps []Step
}

func (c *Case) Run(server tests.Server, t *testing.T) {
	for _, s := range c.Steps {
		s.Run(c.Name, server, t)
	}
}

func (c *Case) ResCode(server tests.Server) {
	for _, s := range c.Steps {
		s.ResCode(c.Name, server)
	}
}

type Suite struct {
	Gen      Generator
	Cases    []Case
	Parallel int
	flag     ParallelFlag
}

func (s *Suite) Run(server tests.Server, t *testing.T) {
	s.Gen.Run(server)
	var wg sync.WaitGroup
	wg.Add(s.Parallel)
	for i := 0; i < s.Parallel; i++ {
		go func() {
			f := int32(0)
			for {
				f = s.flag.Add()
				if f >= int32(len(s.Cases)) {
					break
				}
				s.Cases[f].Run(server, t)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func (s *Suite) ResCode(server tests.Server) {
	s.Gen.Run(server)
	for _, c := range s.Cases {
		c.ResCode(server)
	}
}

type ParallelFlag struct {
	flag int32
}

func (p *ParallelFlag) Add() int32 {
	return atomic.AddInt32(&p.flag, 1) - 1 // begin with zero
}
