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
		//HOLT_WINTERS()
		{
			Name:  "pred_hw_clauses_first",
			Query: fmt.Sprintf(`SELECT HOLT_WINTERS_WITH_FIT(FIRST("water_level"),10,4) FROM "%s"."%s"."h2o_feet" WHERE "location"='santa_monica' AND time >= '2015-08-22 22:12:00' AND time <= '2015-08-28 03:00:00' GROUP BY time(379m,348m)`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "pred_hw_first",
			Query: fmt.Sprintf(`SELECT FIRST("water_level") FROM "%s"."%s"."h2o_feet" WHERE "location"='santa_monica' and time >= '2015-08-22 22:12:00' and time <= '2015-08-28 03:00:00' GROUP BY time(379m,348m)`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "pred_hw_select_location_time",
			Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE "location"='santa_monica' AND time >= '2015-08-22 22:12:00' AND time <= '2015-08-28 03:00:00' limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		//other
		{
			Name:  "other_mean_median",
			Query: fmt.Sprintf(`SELECT MEAN("water_level"),MEDIAN("water_level") FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "other_mode_mode",
			Query: fmt.Sprintf(`SELECT MODE("water_level"),MODE("level description") FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "other_min",
			Query: fmt.Sprintf(`SELECT MIN("water_level") FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "other_max",
			Query: fmt.Sprintf(`SELECT MAX("water_level") FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "other_mean_as",
			Query: fmt.Sprintf(`SELECT MEAN("water_level") AS "dream_name" FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "other_mean",
			Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "other_median_as_mode_as",
			Query: fmt.Sprintf(`SELECT MEDIAN("water_level") AS "med_wat",MODE("water_level") AS "mode_wat" FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "other_median_mode",
			Query: fmt.Sprintf(`SELECT MEDIAN("water_level"),MODE("water_level") FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "other_sum",
			Query: fmt.Sprintf(`SELECT SUM("water_level") FROM "%s"."%s"."h2o_feet"`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "other_sum_time",
			Query: fmt.Sprintf(`SELECT SUM("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "other_sum_time_12m",
			Query: fmt.Sprintf(`SELECT SUM("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:18:00Z' GROUP BY time(12m) limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "other_sum_location",
			Query: fmt.Sprintf(`SELECT SUM("water_level"),"location" FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "other_max_time",
			Query: fmt.Sprintf(`SELECT MAX("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "other_first_all",
			Query: fmt.Sprintf(`SELECT FIRST(*) FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "other_max_all",
			Query: fmt.Sprintf(`SELECT MAX(*) FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "other_max_min",
			Query: fmt.Sprintf(`SELECT MAX("water_level"),MIN("water_level") FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "other_max_min_time",
			Query: fmt.Sprintf(`SELECT MAX("water_level"),MIN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "other_max_time_12m",
			Query: fmt.Sprintf(`SELECT MAX("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:18:00Z' GROUP BY time(12m) limit 20`, db, rp),
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
