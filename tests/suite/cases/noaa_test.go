package cases

import (
	"fmt"
	"github.com/cnosdb/cnosdb/tests/suite"
	"github.com/cnosdb/cnosdb/tests/suite/noaa"
	"testing"
)

func TestNoaa(t *testing.T) {
	n := noaa.NOAA{S: server, T: t}
	n.Load()
	n.Test()
}

const (
	db = "NOAA_water_database"
	rp = "rp0"
)

func TestGenCode(t *testing.T) {

	n := noaa.NOAA{S: server, T: t}
	n.Load()
	s := suite.Step{
		Name:  "water_level_2_4",
		Query: fmt.Sprintf(`SELECT ("water_level" * 2) + 4 FROM "%s"."%s".h2o_feet LIMIT 10 OFFSET 2000`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	}
	s.ResCode(server)
}
