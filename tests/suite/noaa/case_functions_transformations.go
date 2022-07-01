package noaa

import (
	"fmt"
	"github.com/cnosdb/cnosdb/tests/suite"
)

var cases_f_t_gen = []suite.Step{
	//ABS()
	{
		Name:  "trans_abs_select",
		Query: fmt.Sprintf(`SELECT * FROM "%s"."%s"."data" WHERE time >= '2018-06-24T12:00:00Z' AND time <= '2018-06-24T12:05:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_abs_key",
		Query: fmt.Sprintf(`SELECT ABS("a") FROM "%s"."%s"."data" WHERE time >= '2018-06-24T12:00:00Z' AND time <= '2018-06-24T12:05:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_abs_all",
		Query: fmt.Sprintf(`SELECT ABS(*) FROM "%s"."%s"."data" WHERE time >= '2018-06-24T12:00:00Z' AND time <= '2018-06-24T12:05:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_abs_clauses",
		Query: fmt.Sprintf(`SELECT ABS("a") FROM "%s"."%s"."data" WHERE time >= '2018-06-24T12:00:00Z' AND time <= '2018-06-24T12:05:00Z' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_abs_mean_a_12m",
		Query: fmt.Sprintf(`SELECT ABS(MEAN("a")) FROM "%s"."%s"."data" WHERE time >= '2018-06-24T12:00:00Z' AND time <= '2018-06-24T13:00:00Z' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_mean_a_12m",
		Query: fmt.Sprintf(`SELECT MEAN("a") FROM "%s"."%s"."data" WHERE time >= '2018-06-24T12:00:00Z' AND time <= '2018-06-24T13:00:00Z' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	//ACOS()
	{
		Name:  "trans_acos_select",
		Query: fmt.Sprintf(`SELECT "of_capacity" FROM "%s"."%s"."park_occupancy" WHERE time >= '2017-05-01T00:00:00Z' AND time <= '2017-05-09T00:00:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_acos_key",
		Query: fmt.Sprintf(`SELECT ACOS("of_capacity") FROM "%s"."%s"."park_occupancy" WHERE time >= '2017-05-01T00:00:00Z' AND time <= '2017-05-09T00:00:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_acos_all",
		Query: fmt.Sprintf(`SELECT ACOS(*) FROM "%s"."%s"."park_occupancy" WHERE time >= '2017-05-01T00:00:00Z' AND time <= '2017-05-09T00:00:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_acos_clauses",
		Query: fmt.Sprintf(`SELECT ACOS("of_capacity") FROM "%s"."%s"."park_occupancy" WHERE time >= '2017-05-01T00:00:00Z' AND time <= '2017-05-09T00:00:00Z' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_acos_clauses_3d",
		Query: fmt.Sprintf(`SELECT ACOS(MEAN("of_capacity")) FROM "%s"."%s"."park_occupancy" WHERE time >= '2017-05-01T00:00:00Z' AND time <= '2017-05-09T00:00:00Z' GROUP BY time(3d) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_acos_mean_3d",
		Query: fmt.Sprintf(`SELECT MEAN("of_capacity") FROM "%s"."%s"."park_occupancy" WHERE time >= '2017-05-01T00:00:00Z' AND time <= '2017-05-09T00:00:00Z' GROUP BY time(3d) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	//ASIN()
	{
		Name:  "trans_asin_select",
		Query: fmt.Sprintf(`SELECT "of_capacity" FROM "%s"."%s"."park_occupancy" WHERE time >= '2017-05-01T00:00:00Z' AND time <= '2017-05-09T00:00:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_asin_key",
		Query: fmt.Sprintf(`SELECT ASIN("of_capacity") FROM "%s"."%s"."park_occupancy" WHERE time >= '2017-05-01T00:00:00Z' AND time <= '2017-05-09T00:00:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_asin_all",
		Query: fmt.Sprintf(`SELECT ASIN(*) FROM "%s"."%s"."park_occupancy" WHERE time >= '2017-05-01T00:00:00Z' AND time <= '2017-05-09T00:00:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_asin_clauses",
		Query: fmt.Sprintf(`SELECT ASIN("of_capacity") FROM "%s"."%s"."park_occupancy" WHERE time >= '2017-05-01T00:00:00Z' AND time <= '2017-05-09T00:00:00Z' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_asin_clauses_mean_3d",
		Query: fmt.Sprintf(`SELECT ASIN(MEAN("of_capacity")) FROM "%s"."%s"."park_occupancy" WHERE time >= '2017-05-01T00:00:00Z' AND time <= '2017-05-09T00:00:00Z' GROUP BY time(3d) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_asin_means_3d",
		Query: fmt.Sprintf(`SELECT MEAN("of_capacity") FROM "%s"."%s"."park_occupancy" WHERE time >= '2017-05-01T00:00:00Z' AND time <= '2017-05-09T00:00:00Z' GROUP BY time(3d) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	//ATAN()
	{
		Name:  "trans_atan_select",
		Query: fmt.Sprintf(`SELECT "of_capacity" FROM "%s"."%s"."park_occupancy" WHERE time >= '2017-05-01T00:00:00Z' AND time <= '2017-05-09T00:00:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_atan_key",
		Query: fmt.Sprintf(`SELECT ATAN("of_capacity") FROM "%s"."%s"."park_occupancy" WHERE time >= '2017-05-01T00:00:00Z' AND time <= '2017-05-09T00:00:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_atan_all",
		Query: fmt.Sprintf(`SELECT ATAN(*) FROM "%s"."%s"."park_occupancy" WHERE time >= '2017-05-01T00:00:00Z' AND time <= '2017-05-09T00:00:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_atan_clauses",
		Query: fmt.Sprintf(`SELECT ATAN("of_capacity") FROM "%s"."%s"."park_occupancy" WHERE time >= '2017-05-01T00:00:00Z' AND time <= '2017-05-09T00:00:00Z' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_atan_clauses_mean_3d",
		Query: fmt.Sprintf(`SELECT ATAN(MEAN("of_capacity")) FROM "%s"."%s"."park_occupancy" WHERE time >= '2017-05-01T00:00:00Z' AND time <= '2017-05-09T00:00:00Z' GROUP BY time(3d) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_atan_mean_3d",
		Query: fmt.Sprintf(`SELECT MEAN("of_capacity") FROM "%s"."%s"."park_occupancy" WHERE time >= '2017-05-01T00:00:00Z' AND time <= '2017-05-09T00:00:00Z' GROUP BY time(3d) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	//ATAN2()
	{
		Name:  "trans_atan2_select",
		Query: fmt.Sprintf(`SELECT "altitude_ft", "distance_ft" FROM "%s"."%s"."flight_data" WHERE time >= '2018-05-16T12:01:00Z' AND time <= '2018-05-16T12:10:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_atan2_keys",
		Query: fmt.Sprintf(`SELECT ATAN2("altitude_ft", "distance_ft") FROM "%s"."%s"."flight_data" WHERE time >= '2018-05-16T12:01:00Z' AND time <= '2018-05-16T12:10:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_atan2_all",
		Query: fmt.Sprintf(`SELECT ATAN2(*, "distance_ft") FROM "%s"."%s"."flight_data" WHERE time >= '2018-05-16T12:01:00Z' AND time <= '2018-05-16T12:10:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_atan2_clauses",
		Query: fmt.Sprintf(`SELECT ATAN2("altitude_ft", "distance_ft") FROM "%s"."%s"."flight_data" WHERE time >= '2018-05-16T12:01:00Z' AND time <= '2018-05-16T12:10:00Z' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_atan2_clauses_mean",
		Query: fmt.Sprintf(`SELECT ATAN2(MEAN("altitude_ft"), MEAN("distance_ft")) FROM "%s"."%s"."flight_data" WHERE time >= '2018-05-16T12:01:00Z' AND time <= '2018-05-16T13:01:00Z' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_atan2_mean_mean_12m",
		Query: fmt.Sprintf(`SELECT MEAN("altitude_ft"), MEAN("distance_ft") FROM "%s"."%s"."flight_data" WHERE time >= '2018-05-16T12:01:00Z' AND time <= '2018-05-16T13:01:00Z' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	//CEIL()
	{
		Name:  "trans_ceil_select",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_ceil_key",
		Query: fmt.Sprintf(`SELECT CEIL("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_ceil_all",
		Query: fmt.Sprintf(`SELECT CEIL(*) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_ceil_clauses",
		Query: fmt.Sprintf(`SELECT CEIL("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_ceil_clauses_mean_12m",
		Query: fmt.Sprintf(`SELECT CEIL(MEAN("water_level")) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_ceil_mean_12m",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	//COS()
	{
		Name:  "trans_cos_select",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_cos_key",
		Query: fmt.Sprintf(`SELECT COS("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_cos_all",
		Query: fmt.Sprintf(`SELECT COS(*) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_cos_clauses",
		Query: fmt.Sprintf(`SELECT COS("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_cos_clauses_mean_12m",
		Query: fmt.Sprintf(`SELECT COS(MEAN("water_level")) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_cos_mean_12m",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	//CUMULATIVE_SUM()
	{
		Name:  "trans_cs_select",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_cs_key",
		Query: fmt.Sprintf(`SELECT CUMULATIVE_SUM("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_cs_all",
		Query: fmt.Sprintf(`SELECT CUMULATIVE_SUM(*) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_cs_re",
		Query: fmt.Sprintf(`SELECT CUMULATIVE_SUM(/water/) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_cs_clauses",
		Query: fmt.Sprintf(`SELECT CUMULATIVE_SUM("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_cs_clauses_mean_12m",
		Query: fmt.Sprintf(`SELECT CUMULATIVE_SUM(MEAN("water_level")) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_cs_mean_12m",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	//DERIVATIVE()
	{
		Name:  "trans_der_select",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_der_key_unit",
		Query: fmt.Sprintf(`SELECT DERIVATIVE("water_level",6m) FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_der_key",
		Query: fmt.Sprintf(`SELECT DERIVATIVE("water_level") FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_der_all",
		Query: fmt.Sprintf(`SELECT DERIVATIVE(*,3m) FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_der_re",
		Query: fmt.Sprintf(`SELECT DERIVATIVE(/water/,2m) FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_der_clauses",
		Query: fmt.Sprintf(`SELECT DERIVATIVE("water_level") FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' ORDER BY time DESC LIMIT 1 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_der_clauses_mean_12m",
		Query: fmt.Sprintf(`SELECT DERIVATIVE(MEAN("water_level")) FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_der_mean_12m",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_der_clauses_mean_6m_12m",
		Query: fmt.Sprintf(`SELECT DERIVATIVE(MEAN("water_level"),6m) FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	//DIFFERENCE()
	{
		Name:  "trans_diff_select",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_diff_key",
		Query: fmt.Sprintf(`SELECT DIFFERENCE("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_diff_all",
		Query: fmt.Sprintf(`SELECT DIFFERENCE(*) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit all`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_diff_re",
		Query: fmt.Sprintf(`SELECT DIFFERENCE(/water/) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_diff_clauses",
		Query: fmt.Sprintf(`SELECT DIFFERENCE("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' ORDER BY time DESC LIMIT 2 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_diff_clauses_max",
		Query: fmt.Sprintf(`SELECT DIFFERENCE(MAX("water_level")) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_diff_max",
		Query: fmt.Sprintf(`SELECT MAX("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	//ELAPSED()
	{
		Name:  "trans_elapsed_select",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:12:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_elapsed_key",
		Query: fmt.Sprintf(`SELECT ELAPSED("water_level") FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:12:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_elapsed_key_unit",
		Query: fmt.Sprintf(`SELECT ELAPSED("water_level",1m) FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:12:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_elapsed_all",
		Query: fmt.Sprintf(`SELECT ELAPSED(*,1m) FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:12:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_elapsed_re",
		Query: fmt.Sprintf(`SELECT ELAPSED(/level/,1s) FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:12:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_elapsed_clauses_1ms",
		Query: fmt.Sprintf(`SELECT ELAPSED("water_level",1ms) FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:12:00Z' ORDER BY time DESC LIMIT 1 OFFSET 1`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_elapsed_clauses_1h",
		Query: fmt.Sprintf(`SELECT ELAPSED("water_level",1h) FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:12:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_elapsed_clauses_min_1m",
		Query: fmt.Sprintf(`SELECT ELAPSED(MIN("water_level"),1m) FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:36:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_elapsed_min",
		Query: fmt.Sprintf(`SELECT MIN("water_level") FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:36:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	//
	{
		Name:  "trans_exp_select",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_exp_key",
		Query: fmt.Sprintf(`SELECT EXP("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_exp_all",
		Query: fmt.Sprintf(`SELECT EXP(*) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_exp_clauses",
		Query: fmt.Sprintf(`SELECT EXP("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_exp_clauses_mean_12m",
		Query: fmt.Sprintf(`SELECT EXP(MEAN("water_level")) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_exp_mean_12m",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	//FLOOR()
	{
		Name:  "trans_floor_select",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_floor_key",
		Query: fmt.Sprintf(`SELECT FLOOR("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_floor_all",
		Query: fmt.Sprintf(`SELECT FLOOR(*) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_floor_clauses",
		Query: fmt.Sprintf(`SELECT FLOOR("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_floor_clauses_mean_12m",
		Query: fmt.Sprintf(`SELECT FLOOR(MEAN("water_level")) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_floor_mean_12m",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	//LN()
	{
		Name:  "trans_ln_select",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_ln_key",
		Query: fmt.Sprintf(`SELECT LN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_ln_all",
		Query: fmt.Sprintf(`SELECT LN(*) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_ln_clauses",
		Query: fmt.Sprintf(`SELECT LN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_ln_clauses_mean",
		Query: fmt.Sprintf(`SELECT LN(MEAN("water_level")) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_ln_mean",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	//LOG()
	{
		Name:  "trans_log_select",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_log_key",
		Query: fmt.Sprintf(`SELECT LOG("water_level", 4) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_log_clauses",
		Query: fmt.Sprintf(`SELECT LOG("water_level", 4) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_log_all",
		Query: fmt.Sprintf(`SELECT LOG(*, 4) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_log_clauses_mean",
		Query: fmt.Sprintf(`SELECT LOG(MEAN("water_level"), 4) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_log_mean",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	//LOG2()
	{
		Name:  "trans_log2_select",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_log2_key",
		Query: fmt.Sprintf(`SELECT LOG2("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_log2_all",
		Query: fmt.Sprintf(`SELECT LOG2(*) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_log2_clauses",
		Query: fmt.Sprintf(`SELECT LOG2("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_log2_clauses_mean",
		Query: fmt.Sprintf(`SELECT LOG2(MEAN("water_level")) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_log2_mean",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	//
	{
		Name:  "trans_log10_select",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_log10_key",
		Query: fmt.Sprintf(`SELECT LOG10("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_log10_all",
		Query: fmt.Sprintf(`SELECT LOG10(*) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_log10_clauses",
		Query: fmt.Sprintf(`SELECT LOG10("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_log10_clauses_mean",
		Query: fmt.Sprintf(`SELECT LOG10(MEAN("water_level")) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 10`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_log10_mean",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	//MOVING_AVERAGE()
	{
		Name:  "trans_ma_select",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_ma_key",
		Query: fmt.Sprintf(`SELECT MOVING_AVERAGE("water_level",2) FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_ma_all",
		Query: fmt.Sprintf(`SELECT MOVING_AVERAGE(*,3) FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_ma_re",
		Query: fmt.Sprintf(`SELECT MOVING_AVERAGE(/level/,4) FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_ma_clauses",
		Query: fmt.Sprintf(`SELECT MOVING_AVERAGE("water_level",2) FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' ORDER BY time DESC LIMIT 2 OFFSET 3`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_ma_clauses_max",
		Query: fmt.Sprintf(`SELECT MOVING_AVERAGE(MAX("water_level"),2) FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_ma_max",
		Query: fmt.Sprintf(`SELECT MAX("water_level") FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	//POW()
	{
		Name:  "trans_pow_select",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_pow_key",
		Query: fmt.Sprintf(`SELECT POW("water_level", 4) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_pow_all",
		Query: fmt.Sprintf(`SELECT POW(*, 4) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_pow_clauses",
		Query: fmt.Sprintf(`SELECT POW("water_level", 4) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_pow_clauses_mean_12m",
		Query: fmt.Sprintf(`SELECT POW(MEAN("water_level"), 4) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_pow_mean_12m",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	//ROUND()
	{
		Name:  "trans_round_key",
		Query: fmt.Sprintf(`SELECT ROUND("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_round_all",
		Query: fmt.Sprintf(`SELECT ROUND(*) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_round_clauses",
		Query: fmt.Sprintf(`SELECT ROUND("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_round_clauses_mean",
		Query: fmt.Sprintf(`SELECT ROUND(MEAN("water_level")) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m)`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	//SIN()
	{
		Name:  "trans_sin_key",
		Query: fmt.Sprintf(`SELECT SIN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_sin_all",
		Query: fmt.Sprintf(`SELECT SIN(*) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_sin_clauses",
		Query: fmt.Sprintf(`SELECT SIN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_sin_clauses_mean",
		Query: fmt.Sprintf(`SELECT SIN(MEAN("water_level")) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	//SQRT()
	{
		Name:  "trans_sqrt_key",
		Query: fmt.Sprintf(`SELECT SQRT("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_sqrt_all",
		Query: fmt.Sprintf(`SELECT SQRT(*) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_sqrt_clauses",
		Query: fmt.Sprintf(`SELECT SQRT("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_sqrt_clauses_mean",
		Query: fmt.Sprintf(`SELECT SQRT(MEAN("water_level")) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	//TAN()
	{
		Name:  "trans_tan_key",
		Query: fmt.Sprintf(`SELECT TAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_tan_all",
		Query: fmt.Sprintf(`SELECT TAN(*) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_tan_clauses",
		Query: fmt.Sprintf(`SELECT TAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "trans_tan_clauses_mean",
		Query: fmt.Sprintf(`SELECT TAN(MEAN("water_level")) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
}

var cases_f_t_cases = []suite.Step{
	//ABS()
	{
		Name:  "trans_abs_select",
		Query: fmt.Sprintf(`SELECT * FROM "%s"."%s"."data" WHERE time >= '2018-06-24T12:00:00Z' AND time <= '2018-06-24T12:05:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_abs_key",
		Query: fmt.Sprintf(`SELECT ABS("a") FROM "%s"."%s"."data" WHERE time >= '2018-06-24T12:00:00Z' AND time <= '2018-06-24T12:05:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_abs_all",
		Query: fmt.Sprintf(`SELECT ABS(*) FROM "%s"."%s"."data" WHERE time >= '2018-06-24T12:00:00Z' AND time <= '2018-06-24T12:05:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_abs_clauses",
		Query: fmt.Sprintf(`SELECT ABS("a") FROM "%s"."%s"."data" WHERE time >= '2018-06-24T12:00:00Z' AND time <= '2018-06-24T12:05:00Z' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_abs_mean_a_12m",
		Query: fmt.Sprintf(`SELECT ABS(MEAN("a")) FROM "%s"."%s"."data" WHERE time >= '2018-06-24T12:00:00Z' AND time <= '2018-06-24T13:00:00Z' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_mean_a_12m",
		Query: fmt.Sprintf(`SELECT MEAN("a") FROM "%s"."%s"."data" WHERE time >= '2018-06-24T12:00:00Z' AND time <= '2018-06-24T13:00:00Z' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//ACOS()
	{
		Name:  "trans_acos_select",
		Query: fmt.Sprintf(`SELECT "of_capacity" FROM "%s"."%s"."park_occupancy" WHERE time >= '2017-05-01T00:00:00Z' AND time <= '2017-05-09T00:00:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_acos_key",
		Query: fmt.Sprintf(`SELECT ACOS("of_capacity") FROM "%s"."%s"."park_occupancy" WHERE time >= '2017-05-01T00:00:00Z' AND time <= '2017-05-09T00:00:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_acos_all",
		Query: fmt.Sprintf(`SELECT ACOS(*) FROM "%s"."%s"."park_occupancy" WHERE time >= '2017-05-01T00:00:00Z' AND time <= '2017-05-09T00:00:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_acos_clauses",
		Query: fmt.Sprintf(`SELECT ACOS("of_capacity") FROM "%s"."%s"."park_occupancy" WHERE time >= '2017-05-01T00:00:00Z' AND time <= '2017-05-09T00:00:00Z' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_acos_clauses_3d",
		Query: fmt.Sprintf(`SELECT ACOS(MEAN("of_capacity")) FROM "%s"."%s"."park_occupancy" WHERE time >= '2017-05-01T00:00:00Z' AND time <= '2017-05-09T00:00:00Z' GROUP BY time(3d) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_acos_mean_3d",
		Query: fmt.Sprintf(`SELECT MEAN("of_capacity") FROM "%s"."%s"."park_occupancy" WHERE time >= '2017-05-01T00:00:00Z' AND time <= '2017-05-09T00:00:00Z' GROUP BY time(3d) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//ASIN()
	{
		Name:  "trans_asin_select",
		Query: fmt.Sprintf(`SELECT "of_capacity" FROM "%s"."%s"."park_occupancy" WHERE time >= '2017-05-01T00:00:00Z' AND time <= '2017-05-09T00:00:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_asin_key",
		Query: fmt.Sprintf(`SELECT ASIN("of_capacity") FROM "%s"."%s"."park_occupancy" WHERE time >= '2017-05-01T00:00:00Z' AND time <= '2017-05-09T00:00:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_asin_all",
		Query: fmt.Sprintf(`SELECT ASIN(*) FROM "%s"."%s"."park_occupancy" WHERE time >= '2017-05-01T00:00:00Z' AND time <= '2017-05-09T00:00:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_asin_clauses",
		Query: fmt.Sprintf(`SELECT ASIN("of_capacity") FROM "%s"."%s"."park_occupancy" WHERE time >= '2017-05-01T00:00:00Z' AND time <= '2017-05-09T00:00:00Z' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_asin_clauses_mean_3d",
		Query: fmt.Sprintf(`SELECT ASIN(MEAN("of_capacity")) FROM "%s"."%s"."park_occupancy" WHERE time >= '2017-05-01T00:00:00Z' AND time <= '2017-05-09T00:00:00Z' GROUP BY time(3d) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_asin_means_3d",
		Query: fmt.Sprintf(`SELECT MEAN("of_capacity") FROM "%s"."%s"."park_occupancy" WHERE time >= '2017-05-01T00:00:00Z' AND time <= '2017-05-09T00:00:00Z' GROUP BY time(3d) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//ATAN()
	{
		Name:  "trans_atan_select",
		Query: fmt.Sprintf(`SELECT "of_capacity" FROM "%s"."%s"."park_occupancy" WHERE time >= '2017-05-01T00:00:00Z' AND time <= '2017-05-09T00:00:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_atan_key",
		Query: fmt.Sprintf(`SELECT ATAN("of_capacity") FROM "%s"."%s"."park_occupancy" WHERE time >= '2017-05-01T00:00:00Z' AND time <= '2017-05-09T00:00:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_atan_all",
		Query: fmt.Sprintf(`SELECT ATAN(*) FROM "%s"."%s"."park_occupancy" WHERE time >= '2017-05-01T00:00:00Z' AND time <= '2017-05-09T00:00:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_atan_clauses",
		Query: fmt.Sprintf(`SELECT ATAN("of_capacity") FROM "%s"."%s"."park_occupancy" WHERE time >= '2017-05-01T00:00:00Z' AND time <= '2017-05-09T00:00:00Z' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_atan_clauses_mean_3d",
		Query: fmt.Sprintf(`SELECT ATAN(MEAN("of_capacity")) FROM "%s"."%s"."park_occupancy" WHERE time >= '2017-05-01T00:00:00Z' AND time <= '2017-05-09T00:00:00Z' GROUP BY time(3d) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_atan_mean_3d",
		Query: fmt.Sprintf(`SELECT MEAN("of_capacity") FROM "%s"."%s"."park_occupancy" WHERE time >= '2017-05-01T00:00:00Z' AND time <= '2017-05-09T00:00:00Z' GROUP BY time(3d) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//ATAN2()
	{
		Name:  "trans_atan2_select",
		Query: fmt.Sprintf(`SELECT "altitude_ft", "distance_ft" FROM "%s"."%s"."flight_data" WHERE time >= '2018-05-16T12:01:00Z' AND time <= '2018-05-16T12:10:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_atan2_keys",
		Query: fmt.Sprintf(`SELECT ATAN2("altitude_ft", "distance_ft") FROM "%s"."%s"."flight_data" WHERE time >= '2018-05-16T12:01:00Z' AND time <= '2018-05-16T12:10:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_atan2_all",
		Query: fmt.Sprintf(`SELECT ATAN2(*, "distance_ft") FROM "%s"."%s"."flight_data" WHERE time >= '2018-05-16T12:01:00Z' AND time <= '2018-05-16T12:10:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_atan2_clauses",
		Query: fmt.Sprintf(`SELECT ATAN2("altitude_ft", "distance_ft") FROM "%s"."%s"."flight_data" WHERE time >= '2018-05-16T12:01:00Z' AND time <= '2018-05-16T12:10:00Z' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_atan2_clauses_mean",
		Query: fmt.Sprintf(`SELECT ATAN2(MEAN("altitude_ft"), MEAN("distance_ft")) FROM "%s"."%s"."flight_data" WHERE time >= '2018-05-16T12:01:00Z' AND time <= '2018-05-16T13:01:00Z' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_atan2_mean_mean_12m",
		Query: fmt.Sprintf(`SELECT MEAN("altitude_ft"), MEAN("distance_ft") FROM "%s"."%s"."flight_data" WHERE time >= '2018-05-16T12:01:00Z' AND time <= '2018-05-16T13:01:00Z' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//CEIL()
	{
		Name:  "trans_ceil_select",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_ceil_key",
		Query: fmt.Sprintf(`SELECT CEIL("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_ceil_all",
		Query: fmt.Sprintf(`SELECT CEIL(*) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_ceil_clauses",
		Query: fmt.Sprintf(`SELECT CEIL("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_ceil_clauses_mean_12m",
		Query: fmt.Sprintf(`SELECT CEIL(MEAN("water_level")) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_ceil_mean_12m",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//COS()
	{
		Name:  "trans_cos_select",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_cos_key",
		Query: fmt.Sprintf(`SELECT COS("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_cos_all",
		Query: fmt.Sprintf(`SELECT COS(*) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_cos_clauses",
		Query: fmt.Sprintf(`SELECT COS("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_cos_clauses_mean_12m",
		Query: fmt.Sprintf(`SELECT COS(MEAN("water_level")) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_cos_mean_12m",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//CUMULATIVE_SUM()
	{
		Name:  "trans_cs_select",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_cs_key",
		Query: fmt.Sprintf(`SELECT CUMULATIVE_SUM("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_cs_all",
		Query: fmt.Sprintf(`SELECT CUMULATIVE_SUM(*) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_cs_re",
		Query: fmt.Sprintf(`SELECT CUMULATIVE_SUM(/water/) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_cs_clauses",
		Query: fmt.Sprintf(`SELECT CUMULATIVE_SUM("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_cs_clauses_mean_12m",
		Query: fmt.Sprintf(`SELECT CUMULATIVE_SUM(MEAN("water_level")) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_cs_mean_12m",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//DERIVATIVE()
	{
		Name:  "trans_der_select",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_der_key_unit",
		Query: fmt.Sprintf(`SELECT DERIVATIVE("water_level",6m) FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_der_key",
		Query: fmt.Sprintf(`SELECT DERIVATIVE("water_level") FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_der_all",
		Query: fmt.Sprintf(`SELECT DERIVATIVE(*,3m) FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_der_re",
		Query: fmt.Sprintf(`SELECT DERIVATIVE(/water/,2m) FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_der_clauses",
		Query: fmt.Sprintf(`SELECT DERIVATIVE("water_level") FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' ORDER BY time DESC LIMIT 1 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_der_clauses_mean_12m",
		Query: fmt.Sprintf(`SELECT DERIVATIVE(MEAN("water_level")) FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_der_mean_12m",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_der_clauses_mean_6m_12m",
		Query: fmt.Sprintf(`SELECT DERIVATIVE(MEAN("water_level"),6m) FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//DIFFERENCE()
	{
		Name:  "trans_diff_select",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_diff_key",
		Query: fmt.Sprintf(`SELECT DIFFERENCE("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_diff_all",
		Query: fmt.Sprintf(`SELECT DIFFERENCE(*) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit all`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_diff_re",
		Query: fmt.Sprintf(`SELECT DIFFERENCE(/water/) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_diff_clauses",
		Query: fmt.Sprintf(`SELECT DIFFERENCE("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' ORDER BY time DESC LIMIT 2 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_diff_clauses_max",
		Query: fmt.Sprintf(`SELECT DIFFERENCE(MAX("water_level")) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_diff_max",
		Query: fmt.Sprintf(`SELECT MAX("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//ELAPSED()
	{
		Name:  "trans_elapsed_select",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:12:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_elapsed_key",
		Query: fmt.Sprintf(`SELECT ELAPSED("water_level") FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:12:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_elapsed_key_unit",
		Query: fmt.Sprintf(`SELECT ELAPSED("water_level",1m) FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:12:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_elapsed_all",
		Query: fmt.Sprintf(`SELECT ELAPSED(*,1m) FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:12:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_elapsed_re",
		Query: fmt.Sprintf(`SELECT ELAPSED(/level/,1s) FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:12:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_elapsed_clauses_1ms",
		Query: fmt.Sprintf(`SELECT ELAPSED("water_level",1ms) FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:12:00Z' ORDER BY time DESC LIMIT 1 OFFSET 1`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_elapsed_clauses_1h",
		Query: fmt.Sprintf(`SELECT ELAPSED("water_level",1h) FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:12:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_elapsed_clauses_min_1m",
		Query: fmt.Sprintf(`SELECT ELAPSED(MIN("water_level"),1m) FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:36:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_elapsed_min",
		Query: fmt.Sprintf(`SELECT MIN("water_level") FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:36:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//EXP()
	{
		Name:  "trans_exp_select",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_exp_key",
		Query: fmt.Sprintf(`SELECT EXP("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_exp_all",
		Query: fmt.Sprintf(`SELECT EXP(*) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_exp_clauses",
		Query: fmt.Sprintf(`SELECT EXP("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_exp_clauses_mean_12m",
		Query: fmt.Sprintf(`SELECT EXP(MEAN("water_level")) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_exp_mean_12m",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//FLOOR()
	{
		Name:  "trans_floor_select",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_floor_key",
		Query: fmt.Sprintf(`SELECT FLOOR("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_floor_all",
		Query: fmt.Sprintf(`SELECT FLOOR(*) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_floor_clauses",
		Query: fmt.Sprintf(`SELECT FLOOR("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_floor_clauses_mean_12m",
		Query: fmt.Sprintf(`SELECT FLOOR(MEAN("water_level")) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_floor_mean_12m",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//LN()
	{
		Name:  "trans_ln_select",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_ln_key",
		Query: fmt.Sprintf(`SELECT LN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_ln_all",
		Query: fmt.Sprintf(`SELECT LN(*) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_ln_clauses",
		Query: fmt.Sprintf(`SELECT LN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_ln_clauses_mean",
		Query: fmt.Sprintf(`SELECT LN(MEAN("water_level")) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_ln_mean",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//LOG()
	{
		Name:  "trans_log_select",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_log_key",
		Query: fmt.Sprintf(`SELECT LOG("water_level", 4) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_log_clauses",
		Query: fmt.Sprintf(`SELECT LOG("water_level", 4) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_log_all",
		Query: fmt.Sprintf(`SELECT LOG(*, 4) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_log_clauses_mean",
		Query: fmt.Sprintf(`SELECT LOG(MEAN("water_level"), 4) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_log_mean",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//LOG2()
	{
		Name:  "trans_log2_select",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_log2_key",
		Query: fmt.Sprintf(`SELECT LOG2("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_log2_all",
		Query: fmt.Sprintf(`SELECT LOG2(*) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_log2_clauses",
		Query: fmt.Sprintf(`SELECT LOG2("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_log2_clauses_mean",
		Query: fmt.Sprintf(`SELECT LOG2(MEAN("water_level")) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_log2_mean",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//LOG10()
	{
		Name:  "trans_log10_select",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_log10_key",
		Query: fmt.Sprintf(`SELECT LOG10("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_log10_all",
		Query: fmt.Sprintf(`SELECT LOG10(*) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_log10_clauses",
		Query: fmt.Sprintf(`SELECT LOG10("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_log10_clauses_mean",
		Query: fmt.Sprintf(`SELECT LOG10(MEAN("water_level")) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 10`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_log10_mean",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//MOVING_AVERAGE()
	{
		Name:  "trans_ma_select",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_ma_key",
		Query: fmt.Sprintf(`SELECT MOVING_AVERAGE("water_level",2) FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_ma_all",
		Query: fmt.Sprintf(`SELECT MOVING_AVERAGE(*,3) FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_ma_re",
		Query: fmt.Sprintf(`SELECT MOVING_AVERAGE(/level/,4) FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_ma_clauses",
		Query: fmt.Sprintf(`SELECT MOVING_AVERAGE("water_level",2) FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' ORDER BY time DESC LIMIT 2 OFFSET 3`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_ma_clauses_max",
		Query: fmt.Sprintf(`SELECT MOVING_AVERAGE(MAX("water_level"),2) FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_ma_max",
		Query: fmt.Sprintf(`SELECT MAX("water_level") FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//POW()
	{
		Name:  "trans_pow_select",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_pow_key",
		Query: fmt.Sprintf(`SELECT POW("water_level", 4) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_pow_all",
		Query: fmt.Sprintf(`SELECT POW(*, 4) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_pow_clauses",
		Query: fmt.Sprintf(`SELECT POW("water_level", 4) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_pow_clauses_mean_12m",
		Query: fmt.Sprintf(`SELECT POW(MEAN("water_level"), 4) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_pow_mean_12m",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//ROUND()
	{
		Name:  "trans_round_key",
		Query: fmt.Sprintf(`SELECT ROUND("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_round_all",
		Query: fmt.Sprintf(`SELECT ROUND(*) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_round_clauses",
		Query: fmt.Sprintf(`SELECT ROUND("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_round_clauses_mean",
		Query: fmt.Sprintf(`SELECT ROUND(MEAN("water_level")) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m)`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//SIN()
	{
		Name:  "trans_sin_key",
		Query: fmt.Sprintf(`SELECT SIN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_sin_all",
		Query: fmt.Sprintf(`SELECT SIN(*) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_sin_clauses",
		Query: fmt.Sprintf(`SELECT SIN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_sin_clauses_mean",
		Query: fmt.Sprintf(`SELECT SIN(MEAN("water_level")) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//SQRT()
	{
		Name:  "trans_sqrt_key",
		Query: fmt.Sprintf(`SELECT SQRT("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_sqrt_all",
		Query: fmt.Sprintf(`SELECT SQRT(*) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_sqrt_clauses",
		Query: fmt.Sprintf(`SELECT SQRT("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_sqrt_clauses_mean",
		Query: fmt.Sprintf(`SELECT SQRT(MEAN("water_level")) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//TAN()
	{
		Name:  "trans_tan_key",
		Query: fmt.Sprintf(`SELECT TAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_tan_all",
		Query: fmt.Sprintf(`SELECT TAN(*) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_tan_clauses",
		Query: fmt.Sprintf(`SELECT TAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' ORDER BY time DESC LIMIT 4 OFFSET 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "trans_tan_clauses_mean",
		Query: fmt.Sprintf(`SELECT TAN(MEAN("water_level")) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(12m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
}
