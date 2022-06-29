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
		//LIMIT and SLIMIT
		{
			Name:  "slimit_water_level_1_",
			Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" GROUP BY * SLIMIT 1`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "slimit_water_level_1",
			Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" GROUP BY * limit 20 SLIMIT 1`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		//INTO
		{
			Name:  "into_measurement_noaa_autogen",
			Query: fmt.Sprintf(`SELECT * INTO "copy_NOAA_water_database"."autogen".:MEASUREMENT FROM "NOAA_water_database"."autogen"./.* limit 20`),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "into_copy1_location_coyote_creek",
			Query: fmt.Sprintf(`SELECT "water_level" INTO "h2o_feet_copy_1" FROM "%s"."%s"."h2o_feet" WHERE "location" = 'coyote_creek' limit 50`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "into_h2o_feet_copy_1",
			Query: fmt.Sprintf(`SELECT * FROM "%s"."%s"."h2o_feet_copy_1" limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "into_copy2_location_coyote_creek",
			Query: fmt.Sprintf(`SELECT "water_level" INTO "where_else"."autogen"."h2o_feet_copy_2" FROM "%s"."%s"."h2o_feet" WHERE "location" = 'coyote_creek' limit 50`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "into_h2o_feet_copy_2",
			Query: fmt.Sprintf(`SELECT * FROM "where_else"."autogen"."h2o_feet_copy_2"`),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "into_all_my_averages",
			Query: fmt.Sprintf(`SELECT MEAN("water_level") INTO "all_my_averages" FROM "%s"."%s"."h2o_feet" WHERE "location" = 'coyote_creek' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m) limit 50`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "into_select_all_my_averages",
			Query: fmt.Sprintf(`SELECT * FROM "%s"."%s"."all_my_averages"`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "into_where_else_12m",
			Query: fmt.Sprintf(`SELECT MEAN(*) INTO "where_else"."autogen".:MEASUREMENT FROM /.*/ WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:06:00Z' GROUP BY time(12m) limit 50`),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "into_where_else_autogen",
			Query: fmt.Sprintf(`SELECT * FROM "where_else"."autogen"./.*/`),
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
