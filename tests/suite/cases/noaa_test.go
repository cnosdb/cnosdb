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
		Name:  "h2o_feet_level_description_9_feet",
		Query: fmt.Sprintf(`SELECT "level description" FROM "%s"."%s"."h2o_feet" WHERE "level description" = 'at or greater than 9 feet' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	}
	//s := suite.Step{
	//	Name:  "h2o_feet_h2o_pH",
	//	Query: fmt.Sprintf(`SELECT * FROM "%s"."%s"."h2o_feet","%s"."%s"."h2o_pH" LIMIT 10 OFFSET 1000`, db, rp, db, rp),
	//	Result: suite.Results{
	//		Results: []suite.Result{},
	//	},
	//}
	s.ResCode(server)
}

func TestGenCode2(t *testing.T) {

	n := noaa.NOAA{S: server, T: t}
	n.Load()
	var steps = [...]suite.Step{
		//Addition
		{
			Name:  "math_add_a_5",
			Query: fmt.Sprintf(`SELECT "A" + 5 FROM "%s"."%s"."add"`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "math_add_a_5_10",
			Query: fmt.Sprintf(`SELECT * FROM "add" WHERE "A" + 5 > 10`),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "math_a_b",
			Query: fmt.Sprintf(`SELECT "A" + "B" FROM "add"`),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "math_a_b_10",
			Query: fmt.Sprintf(`SELECT * FROM "add" WHERE "A" + "B" >= 10`),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		//
		//{
		//	Name:  "",
		//	Query: fmt.Sprintf,
		//	Result: suite.Results{
		//		Results: []suite.Result{},
		//	},
		//},
	}
	var i int
	for i = 0; i < len(steps); i++ {
		fmt.Printf(steps[i].Name)
		fmt.Printf("\n")
		steps[i].ResCode(server)
	}
}
