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
	//s := suite.Step{
	//	Name:  "h2o_feet_level_description_9_feet",
	//	Query: fmt.Sprintf(`SELECT "level description" FROM "%s"."%s"."h2o_feet" WHERE "level description" = 'at or greater than 9 feet' limit 20`, db, rp),
	//	Result: suite.Results{
	//		Results: []suite.Result{},
	//	},
	//}
	//s.ResCode(server)
	var steps = [...]suite.Step{
		{
			Name:  "time_desc_location_santa_monica",
			Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' ORDER BY time DESC limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		}, {
			Name:  "time_desc_mean_water_level_time",
			Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:42:00Z' GROUP BY time(12m) ORDER BY time DESC limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "limit_water_level_location_3",
			Query: fmt.Sprintf(`SELECT "water_level","location" FROM "%s"."%s"."h2o_feet" LIMIT 3`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "limit_mean_water_level_12m_2",
			Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:42:00Z' GROUP BY *,time(12m) LIMIT 2`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "slimit_water_level_1",
			Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" GROUP BY * SLIMIT 1`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "slimit_water_level_12m_1",
			Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:42:00Z' GROUP BY *,time(12m) SLIMIT 1`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "limit_3_slimit_1_water_level",
			Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" GROUP BY * LIMIT 3 SLIMIT 1`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "limit_2_slimit_1_water_level_12m",
			Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:42:00Z' GROUP BY *,time(12m) LIMIT 2 SLIMIT 1`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
	}
	var i int
	for i = 0; i < len(steps); i++ {
		fmt.Printf(steps[i].Name)
		fmt.Printf("\n")
		steps[i].ResCode(server)
	}
}
