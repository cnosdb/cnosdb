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
		//SHOW DATABASES
		{
			Name:  "show_databasaes",
			Query: fmt.Sprintf(`SHOW DATABASES`),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		//SHOW MEASUREMENTS
		{
			Name:  "show_measurements_on",
			Query: fmt.Sprintf(`SHOW MEASUREMENTS ON "%s"`, db),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "show_measurements",
			Query: fmt.Sprintf(`SHOW MEASUREMENTS`),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "show_measurements_on_with_limit_2_offset_1",
			Query: fmt.Sprintf(`SHOW MEASUREMENTS ON "%s" WITH MEASUREMENT =~ /h2o.*/ LIMIT 2 `, db),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "show_measurements_on_with",
			Query: fmt.Sprintf(`SHOW MEASUREMENTS ON "%s" WITH MEASUREMENT =~ /h2o.*/ WHERE "randtag"  =~ /\d/`, db),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		//SHOW FIELD KEYS
		{
			Name:  "show_field_keys_on",
			Query: fmt.Sprintf(`SHOW FIELD KEYS ON "%s"`, db),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "show_field_keys",
			Query: fmt.Sprintf(`SHOW FIELD KEYS`),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "show_field_keys_on_from",
			Query: fmt.Sprintf(`SHOW FIELD KEYS ON "%s" FROM "%s"."%s"."h2o_feet"`, db, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		//SHOW RETENTION POLICIES
		{
			Name:  "show_rp_on_noaa",
			Query: fmt.Sprintf(`SHOW RETENTION POLICIES ON "%s"`, db),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "show_rp",
			Query: fmt.Sprintf(`SHOW RETENTION POLICIES`),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		//SHOW TAG KEYS
		{
			Name:  "show_tag_keys_on",
			Query: fmt.Sprintf(`SHOW TAG KEYS ON "%s"`, db),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "show_tag_keys",
			Query: fmt.Sprintf(`SHOW TAG KEYS`),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "show_tag_keys_1_1",
			Query: fmt.Sprintf(`SHOW TAG KEYS ON "%s" FROM "h2o_quality" LIMIT 1 OFFSET 1`, db),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		//SHOW SERIES
		{
			Name:  "show_series_on_noaa",
			Query: fmt.Sprintf(`SHOW SERIES ON "%s"`, db),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "show_series",
			Query: fmt.Sprintf(`SHOW SERIES`),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "show_series_on_noaa_from",
			Query: fmt.Sprintf(`SHOW SERIES ON "%s" FROM "%s"."%s"."h2o_quality" WHERE "location" = 'coyote_creek' LIMIT 2`, db, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "show_series_on_noaa_now_1m",
			Query: fmt.Sprintf(`SHOW SERIES ON "%s" WHERE time < now() - 1m`, db),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "show_series_now_28d",
			Query: fmt.Sprintf(`SHOW SERIES ON "%s" WHERE time < now() - 28d`, db),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		//SHOW TAG VALUES
		{
			Name:  "show_tag_values_on_randtag",
			Query: fmt.Sprintf(`SHOW TAG VALUES ON "%s" WITH KEY = "randtag"`, db),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "show_tag_values_randtag",
			Query: fmt.Sprintf(`SHOW TAG VALUES WITH KEY = "randtag"`),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "show_tag_values_on_location_randtag",
			Query: fmt.Sprintf(`SHOW TAG VALUES ON "%s" WITH KEY IN ("location","randtag") WHERE "randtag" =~ /./ LIMIT 3`, db),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		//SHOW SHARDS
		{
			Name:  "show_shards",
			Query: fmt.Sprintf(`SHOW SHARDS`),
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
