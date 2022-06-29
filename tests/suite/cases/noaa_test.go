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
		//LIMIT and SLIMIT
		{
			Name:  "slimit_water_level_1",
			Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" GROUP BY * SLIMIT 1 limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		//OFFSET and SOFFSET
		{
			Name:  "limit_3_offset_3_water_level_location",
			Query: fmt.Sprintf(`SELECT "water_level","location" FROM "%s"."%s"."h2o_feet" LIMIT 3 OFFSET 3`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "limit_2_offset_2_slimit_1_water_level_time",
			Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:42:00Z' GROUP BY *,time(12m) ORDER BY time DESC LIMIT 2 OFFSET 2 SLIMIT 1`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "slimit_1_soffset_1_water_level",
			Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" GROUP BY * SLIMIT 1 SOFFSET 1 limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "offset_2_slimit_1_soffset_1_waterlevel_time_12m",
			Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:42:00Z' GROUP BY *,time(12m) ORDER BY time DESC LIMIT 2 OFFSET 2 SLIMIT 1 SOFFSET 1`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		//Time Zone
		{
			Name:  "tz_america_chicago_location_santa_monica",
			Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:18:00Z' tz('America/Chicago') limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		//Time syntax
		{
			Name:  "time_water_level_location_rfc3339",
			Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00.000000000Z' AND time <= '2015-08-18T00:12:00Z' limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "time_water_level_location_rfc3339_like",
			Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18' AND time <= '2015-08-18 00:12:00' limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "time_water_level_location_timestamps",
			Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= 1439856000000000000 AND time <= 1439856720000000000 limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "time_water_level_location_second_timestamps",
			Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= 1439856000s AND time <= 1439856720s limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "time_water_level_rfc3999_6m",
			Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE time > '2015-09-18T21:24:00Z' + 6m limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "time_water_level_timestamp_6m",
			Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE time > 24043524m - 6m limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "time_now_water_level_1h",
			Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE time > now() - 1h limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "time_now_level_description_1000d",
			Query: fmt.Sprintf(`SELECT "level description" FROM "%s"."%s"."h2o_feet" WHERE time > '2015-09-18T21:18:00Z' AND time < now() + 1000d limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "time_fill_water_level_santa_monica_12m",
			Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE "location"='santa_monica' AND time >= '2015-09-18T21:30:00Z' GROUP BY time(12m) fill(none) limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "time_fill_water_level_180w_12m",
			Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE "location"='santa_monica' AND time >= '2015-09-18T21:30:00Z' AND time <= now() + 180w GROUP BY time(12m) fill(none) limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "time_now_fill_water_level_12m",
			Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE "location"='santa_monica' AND time >= now() GROUP BY time(12m) fill(none) limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		//Regular expressions
		{
			Name:  "re_l_limit_1",
			Query: fmt.Sprintf(`SELECT /l/ FROM "%s"."%s"."h2o_feet" LIMIT 1`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "re_mean_degrees",
			Query: fmt.Sprintf(`SELECT MEAN("degrees") FROM /temperature/`),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "re_water_level_3",
			Query: fmt.Sprintf(`SELECT MEAN(water_level) FROM "%s"."%s"."h2o_feet" WHERE "location" =~ /[m]/ AND "water_level" > 3 limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "re_all_location",
			Query: fmt.Sprintf(`SELECT * FROM "%s"."%s"."h2o_feet" WHERE "location" !~ /./ limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "re_mean_water_level_location",
			Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE "location" =~ /./ limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "re_mean_water_level_location_asnta_monica",
			Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND "level description" =~ /between/ limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "re_first_index_l",
			Query: fmt.Sprintf(`SELECT FIRST("index") FROM "%s"."%s"."h2o_quality" GROUP BY /l/ limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		//Data types
		{
			Name:  "data_water_level_float_4",
			Query: fmt.Sprintf(`SELECT "water_level"::float FROM "%s"."%s"."h2o_feet" LIMIT 4`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "data_water_level_integer_4",
			Query: fmt.Sprintf(`SELECT "water_level"::integer FROM "%s"."%s"."h2o_feet" LIMIT 4`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		//Merge behavior
		{
			Name:  "merge_mean_water_level",
			Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "merge_mean_water_level_location",
			Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE "location" = 'coyote_creek' limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "merge_mean_water_level_group_by_location",
			Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" GROUP BY "location" limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		//Multiple statements
		{
			Name:  "mul_mean_water_level_water_level_limit_2",
			Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet"; SELECT "water_level" FROM "%s"."%s"."h2o_feet" LIMIT 2`, db, rp, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		//Subqueries
		{
			Name:  "sub_sum_max_water_level_group_by_location",
			Query: fmt.Sprintf(`SELECT SUM("max") FROM (SELECT MAX("water_level") FROM "%s"."%s"."h2o_feet" GROUP BY "location") limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "sub_max_water_level_group_by_location",
			Query: fmt.Sprintf(`SELECT MAX("water_level") FROM "%s"."%s"."h2o_feet" GROUP BY "location" limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "sub_mean_difference_pet_daycare",
			Query: fmt.Sprintf(`SELECT MEAN("difference") FROM (SELECT "cats" - "dogs" AS "difference" FROM "%s"."%s"."pet_daycare") limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "sub_all_the_means_12m_5",
			Query: fmt.Sprintf(`SELECT "all_the_means" FROM (SELECT MEAN("water_level") AS "all_the_means" FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m) ) WHERE "all_the_means" > 5 limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "sub_all_the_means_12m",
			Query: fmt.Sprintf(`SELECT MEAN("water_level") AS "all_the_means" FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m) limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "sub_sum_derivative",
			Query: fmt.Sprintf(`SELECT SUM("water_level_derivative") AS "sum_derivative" FROM (SELECT DERIVATIVE(MEAN("water_level")) AS "water_level_derivative" FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m),"location") GROUP BY "location" limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "sub_water_level_derivative",
			Query: fmt.Sprintf(`SELECT DERIVATIVE(MEAN("water_level")) AS "water_level_derivative" FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m),"location" limit 20`, db, rp),
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
