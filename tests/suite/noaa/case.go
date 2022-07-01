package noaa

import (
	"fmt"
	"github.com/cnosdb/cnosdb/tests/suite"
)

var cases = []suite.Step{
	//SELECT
	{
		Name:  "water_level_2_4",
		Query: fmt.Sprintf(`SELECT ("water_level" * 2) + 4 FROM "%s"."%s".h2o_feet LIMIT 10 OFFSET 2000`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "water_level"},
							Values: []suite.Row{
								{"2019-08-21T04:00:00Z", 15.464},
								{"2019-08-21T04:00:00Z", 8.666},
								{"2019-08-21T04:06:00Z", 15.246},
								{"2019-08-21T04:06:00Z", 8.646},
								{"2019-08-21T04:12:00Z", 15.004},
								{"2019-08-21T04:12:00Z", 8.64},
								{"2019-08-21T04:18:00Z", 14.754},
								{"2019-08-21T04:18:00Z", 8.527999999999999},
								{"2019-08-21T04:24:00Z", 14.518},
								{"2019-08-21T04:24:00Z", 8.402000000000001},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "water_level_count",
		Query: fmt.Sprintf(`SELECT COUNT("water_level") FROM "%s"."%s".h2o_feet`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "count"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 15258},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "show_measurements",
		Query: fmt.Sprintf(`SHOW measurements ON "%s"`, db),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "measurements",
							Columns: []string{"name"},
							Values: []suite.Row{
								{"average_temperature"},
								{"h2o_feet"},
								{"h2o_pH"},
								{"h2o_quality"},
								{"h2o_temperature"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "h2o_feet_h2o_pH",
		Query: fmt.Sprintf(`SELECT * FROM "%s"."%s"."h2o_feet","%s"."%s"."h2o_pH" LIMIT 10 OFFSET 1000`, db, rp, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "level description", "location", "pH", "water_level"},
							Values: []suite.Row{
								{"2019-08-19T02:00:00Z", "below 3 feet", "santa_monica", nil, 2.211},
								{"2019-08-19T02:00:00Z", "between 6 and 9 feet", "coyote_creek", nil, 6.768},
								{"2019-08-19T02:06:00Z", "below 3 feet", "santa_monica", nil, 2.188},
								{"2019-08-19T02:06:00Z", "between 6 and 9 feet", "coyote_creek", nil, 6.631},
								{"2019-08-19T02:12:00Z", "below 3 feet", "santa_monica", nil, 2.306},
								{"2019-08-19T02:12:00Z", "between 6 and 9 feet", "coyote_creek", nil, 6.49},
								{"2019-08-19T02:18:00Z", "below 3 feet", "santa_monica", nil, 2.323},
								{"2019-08-19T02:18:00Z", "between 6 and 9 feet", "coyote_creek", nil, 6.358},
								{"2019-08-19T02:24:00Z", "below 3 feet", "santa_monica", nil, 2.297},
								{"2019-08-19T02:24:00Z", "between 6 and 9 feet", "coyote_creek", nil, 6.207},
							},
						},
						{
							Name:    "h2o_pH",
							Columns: []string{"time", "level description", "location", "pH", "water_level"},
							Values: []suite.Row{
								{"2019-08-19T02:00:00Z", nil, "coyote_creek", 7, nil},
								{"2019-08-19T02:00:00Z", nil, "santa_monica", 8, nil},
								{"2019-08-19T02:06:00Z", nil, "coyote_creek", 7, nil},
								{"2019-08-19T02:06:00Z", nil, "santa_monica", 8, nil},
								{"2019-08-19T02:12:00Z", nil, "coyote_creek", 8, nil},
								{"2019-08-19T02:12:00Z", nil, "santa_monica", 8, nil},
								{"2019-08-19T02:18:00Z", nil, "coyote_creek", 7, nil},
								{"2019-08-19T02:18:00Z", nil, "santa_monica", 7, nil},
								{"2019-08-19T02:24:00Z", nil, "coyote_creek", 8, nil},
								{"2019-08-19T02:24:00Z", nil, "santa_monica", 8, nil},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "h2o_feet_all_fields",
		Query: fmt.Sprintf(`SELECT *::field FROM "%s"."%s".h2o_feet LIMIT 10 OFFSET 3000`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "level description", "water_level"},
							Values: []suite.Row{
								{"2019-08-23T06:00:00Z", "below 3 feet", 1.594},
								{"2019-08-23T06:00:00Z", "between 3 and 6 feet", 5.564},
								{"2019-08-23T06:06:00Z", "below 3 feet", 1.545},
								{"2019-08-23T06:06:00Z", "between 3 and 6 feet", 5.43},
								{"2019-08-23T06:12:00Z", "below 3 feet", 1.526},
								{"2019-08-23T06:12:00Z", "between 3 and 6 feet", 5.308},
								{"2019-08-23T06:18:00Z", "below 3 feet", 1.457},
								{"2019-08-23T06:18:00Z", "between 3 and 6 feet", 5.18},
								{"2019-08-23T06:24:00Z", "below 3 feet", 1.414},
								{"2019-08-23T06:24:00Z", "between 3 and 6 feet", 5.052},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "h2o_feet_specific_tags_fields",
		Query: fmt.Sprintf(`SELECT "level description"::field,"location"::tag,"water_level"::field FROM "%s"."%s".h2o_feet LIMIT 10 OFFSET 5000`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "level description", "location", "water_level"},
							Values: []suite.Row{
								{"2019-08-27T10:00:00Z", "below 3 feet", "santa_monica", -0.062},
								{"2019-08-27T10:00:00Z", "between 3 and 6 feet", "coyote_creek", 5.525},
								{"2019-08-27T10:06:00Z", "below 3 feet", "santa_monica", -0.092},
								{"2019-08-27T10:06:00Z", "between 3 and 6 feet", "coyote_creek", 5.354},
								{"2019-08-27T10:12:00Z", "below 3 feet", "santa_monica", -0.105},
								{"2019-08-27T10:12:00Z", "between 3 and 6 feet", "coyote_creek", 5.187},
								{"2019-08-27T10:18:00Z", "below 3 feet", "santa_monica", -0.089},
								{"2019-08-27T10:18:00Z", "between 3 and 6 feet", "coyote_creek", 5.02},
								{"2019-08-27T10:24:00Z", "below 3 feet", "santa_monica", -0.112},
								{"2019-08-27T10:24:00Z", "between 3 and 6 feet", "coyote_creek", 4.849},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "h2o_feet_50",
		Query: fmt.Sprintf(`SELECT "level description","location","water_level" FROM "%s"."%s".h2o_feet limit 50`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "level description", "location", "water_level"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", "below 3 feet", "santa_monica", 2.064},
								{"2019-08-17T00:00:00Z", "between 6 and 9 feet", "coyote_creek", 8.12},
								{"2019-08-17T00:06:00Z", "below 3 feet", "santa_monica", 2.116},
								{"2019-08-17T00:06:00Z", "between 6 and 9 feet", "coyote_creek", 8.005},
								{"2019-08-17T00:12:00Z", "below 3 feet", "santa_monica", 2.028},
								{"2019-08-17T00:12:00Z", "between 6 and 9 feet", "coyote_creek", 7.887},
								{"2019-08-17T00:18:00Z", "below 3 feet", "santa_monica", 2.126},
								{"2019-08-17T00:18:00Z", "between 6 and 9 feet", "coyote_creek", 7.762},
								{"2019-08-17T00:24:00Z", "below 3 feet", "santa_monica", 2.041},
								{"2019-08-17T00:24:00Z", "between 6 and 9 feet", "coyote_creek", 7.635},
								{"2019-08-17T00:30:00Z", "below 3 feet", "santa_monica", 2.051},
								{"2019-08-17T00:30:00Z", "between 6 and 9 feet", "coyote_creek", 7.5},
								{"2019-08-17T00:36:00Z", "below 3 feet", "santa_monica", 2.067},
								{"2019-08-17T00:36:00Z", "between 6 and 9 feet", "coyote_creek", 7.372},
								{"2019-08-17T00:42:00Z", "below 3 feet", "santa_monica", 2.057},
								{"2019-08-17T00:42:00Z", "between 6 and 9 feet", "coyote_creek", 7.234},
								{"2019-08-17T00:48:00Z", "below 3 feet", "santa_monica", 1.991},
								{"2019-08-17T00:48:00Z", "between 6 and 9 feet", "coyote_creek", 7.11},
								{"2019-08-17T00:54:00Z", "below 3 feet", "santa_monica", 2.054},
								{"2019-08-17T00:54:00Z", "between 6 and 9 feet", "coyote_creek", 6.982},
								{"2019-08-17T01:00:00Z", "below 3 feet", "santa_monica", 2.018},
								{"2019-08-17T01:00:00Z", "between 6 and 9 feet", "coyote_creek", 6.837},
								{"2019-08-17T01:06:00Z", "below 3 feet", "santa_monica", 2.096},
								{"2019-08-17T01:06:00Z", "between 6 and 9 feet", "coyote_creek", 6.713},
								{"2019-08-17T01:12:00Z", "below 3 feet", "santa_monica", 2.1},
								{"2019-08-17T01:12:00Z", "between 6 and 9 feet", "coyote_creek", 6.578},
								{"2019-08-17T01:18:00Z", "below 3 feet", "santa_monica", 2.106},
								{"2019-08-17T01:18:00Z", "between 6 and 9 feet", "coyote_creek", 6.44},
								{"2019-08-17T01:24:00Z", "below 3 feet", "santa_monica", 2.126144146},
								{"2019-08-17T01:24:00Z", "between 6 and 9 feet", "coyote_creek", 6.299},
								{"2019-08-17T01:30:00Z", "below 3 feet", "santa_monica", 2.1},
								{"2019-08-17T01:30:00Z", "between 6 and 9 feet", "coyote_creek", 6.168},
								{"2019-08-17T01:36:00Z", "below 3 feet", "santa_monica", 2.136},
								{"2019-08-17T01:36:00Z", "between 6 and 9 feet", "coyote_creek", 6.024},
								{"2019-08-17T01:42:00Z", "below 3 feet", "santa_monica", 2.182},
								{"2019-08-17T01:42:00Z", "between 3 and 6 feet", "coyote_creek", 5.879},
								{"2019-08-17T01:48:00Z", "below 3 feet", "santa_monica", 2.306},
								{"2019-08-17T01:48:00Z", "between 3 and 6 feet", "coyote_creek", 5.745},
								{"2019-08-17T01:54:00Z", "below 3 feet", "santa_monica", 2.448},
								{"2019-08-17T01:54:00Z", "between 3 and 6 feet", "coyote_creek", 5.617},
								{"2019-08-17T02:00:00Z", "below 3 feet", "santa_monica", 2.464},
								{"2019-08-17T02:00:00Z", "between 3 and 6 feet", "coyote_creek", 5.472},
								{"2019-08-17T02:06:00Z", "below 3 feet", "santa_monica", 2.467},
								{"2019-08-17T02:06:00Z", "between 3 and 6 feet", "coyote_creek", 5.348},
								{"2019-08-17T02:12:00Z", "below 3 feet", "santa_monica", 2.516},
								{"2019-08-17T02:12:00Z", "between 3 and 6 feet", "coyote_creek", 5.2},
								{"2019-08-17T02:18:00Z", "below 3 feet", "santa_monica", 2.674},
								{"2019-08-17T02:18:00Z", "between 3 and 6 feet", "coyote_creek", 5.072},
								{"2019-08-17T02:24:00Z", "below 3 feet", "santa_monica", 2.684},
								{"2019-08-17T02:24:00Z", "between 3 and 6 feet", "coyote_creek", 4.934},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "h2o_feet_200",
		Query: fmt.Sprintf(`SELECT * FROM "%s"."%s"."h2o_feet" limit 200`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "level description", "location", "water_level"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", "below 3 feet", "santa_monica", 2.064},
								{"2019-08-17T00:00:00Z", "between 6 and 9 feet", "coyote_creek", 8.12},
								{"2019-08-17T00:06:00Z", "below 3 feet", "santa_monica", 2.116},
								{"2019-08-17T00:06:00Z", "between 6 and 9 feet", "coyote_creek", 8.005},
								{"2019-08-17T00:12:00Z", "below 3 feet", "santa_monica", 2.028},
								{"2019-08-17T00:12:00Z", "between 6 and 9 feet", "coyote_creek", 7.887},
								{"2019-08-17T00:18:00Z", "below 3 feet", "santa_monica", 2.126},
								{"2019-08-17T00:18:00Z", "between 6 and 9 feet", "coyote_creek", 7.762},
								{"2019-08-17T00:24:00Z", "below 3 feet", "santa_monica", 2.041},
								{"2019-08-17T00:24:00Z", "between 6 and 9 feet", "coyote_creek", 7.635},
								{"2019-08-17T00:30:00Z", "below 3 feet", "santa_monica", 2.051},
								{"2019-08-17T00:30:00Z", "between 6 and 9 feet", "coyote_creek", 7.5},
								{"2019-08-17T00:36:00Z", "below 3 feet", "santa_monica", 2.067},
								{"2019-08-17T00:36:00Z", "between 6 and 9 feet", "coyote_creek", 7.372},
								{"2019-08-17T00:42:00Z", "below 3 feet", "santa_monica", 2.057},
								{"2019-08-17T00:42:00Z", "between 6 and 9 feet", "coyote_creek", 7.234},
								{"2019-08-17T00:48:00Z", "below 3 feet", "santa_monica", 1.991},
								{"2019-08-17T00:48:00Z", "between 6 and 9 feet", "coyote_creek", 7.11},
								{"2019-08-17T00:54:00Z", "below 3 feet", "santa_monica", 2.054},
								{"2019-08-17T00:54:00Z", "between 6 and 9 feet", "coyote_creek", 6.982},
								{"2019-08-17T01:00:00Z", "below 3 feet", "santa_monica", 2.018},
								{"2019-08-17T01:00:00Z", "between 6 and 9 feet", "coyote_creek", 6.837},
								{"2019-08-17T01:06:00Z", "below 3 feet", "santa_monica", 2.096},
								{"2019-08-17T01:06:00Z", "between 6 and 9 feet", "coyote_creek", 6.713},
								{"2019-08-17T01:12:00Z", "below 3 feet", "santa_monica", 2.1},
								{"2019-08-17T01:12:00Z", "between 6 and 9 feet", "coyote_creek", 6.578},
								{"2019-08-17T01:18:00Z", "below 3 feet", "santa_monica", 2.106},
								{"2019-08-17T01:18:00Z", "between 6 and 9 feet", "coyote_creek", 6.44},
								{"2019-08-17T01:24:00Z", "below 3 feet", "santa_monica", 2.126144146},
								{"2019-08-17T01:24:00Z", "between 6 and 9 feet", "coyote_creek", 6.299},
								{"2019-08-17T01:30:00Z", "below 3 feet", "santa_monica", 2.1},
								{"2019-08-17T01:30:00Z", "between 6 and 9 feet", "coyote_creek", 6.168},
								{"2019-08-17T01:36:00Z", "below 3 feet", "santa_monica", 2.136},
								{"2019-08-17T01:36:00Z", "between 6 and 9 feet", "coyote_creek", 6.024},
								{"2019-08-17T01:42:00Z", "below 3 feet", "santa_monica", 2.182},
								{"2019-08-17T01:42:00Z", "between 3 and 6 feet", "coyote_creek", 5.879},
								{"2019-08-17T01:48:00Z", "below 3 feet", "santa_monica", 2.306},
								{"2019-08-17T01:48:00Z", "between 3 and 6 feet", "coyote_creek", 5.745},
								{"2019-08-17T01:54:00Z", "below 3 feet", "santa_monica", 2.448},
								{"2019-08-17T01:54:00Z", "between 3 and 6 feet", "coyote_creek", 5.617},
								{"2019-08-17T02:00:00Z", "below 3 feet", "santa_monica", 2.464},
								{"2019-08-17T02:00:00Z", "between 3 and 6 feet", "coyote_creek", 5.472},
								{"2019-08-17T02:06:00Z", "below 3 feet", "santa_monica", 2.467},
								{"2019-08-17T02:06:00Z", "between 3 and 6 feet", "coyote_creek", 5.348},
								{"2019-08-17T02:12:00Z", "below 3 feet", "santa_monica", 2.516},
								{"2019-08-17T02:12:00Z", "between 3 and 6 feet", "coyote_creek", 5.2},
								{"2019-08-17T02:18:00Z", "below 3 feet", "santa_monica", 2.674},
								{"2019-08-17T02:18:00Z", "between 3 and 6 feet", "coyote_creek", 5.072},
								{"2019-08-17T02:24:00Z", "below 3 feet", "santa_monica", 2.684},
								{"2019-08-17T02:24:00Z", "between 3 and 6 feet", "coyote_creek", 4.934},
								{"2019-08-17T02:30:00Z", "below 3 feet", "santa_monica", 2.799},
								{"2019-08-17T02:30:00Z", "between 3 and 6 feet", "coyote_creek", 4.793},
								{"2019-08-17T02:36:00Z", "below 3 feet", "santa_monica", 2.927},
								{"2019-08-17T02:36:00Z", "between 3 and 6 feet", "coyote_creek", 4.662},
								{"2019-08-17T02:42:00Z", "below 3 feet", "santa_monica", 2.956},
								{"2019-08-17T02:42:00Z", "between 3 and 6 feet", "coyote_creek", 4.534},
								{"2019-08-17T02:48:00Z", "below 3 feet", "santa_monica", 2.927},
								{"2019-08-17T02:48:00Z", "between 3 and 6 feet", "coyote_creek", 4.403},
								{"2019-08-17T02:54:00Z", "between 3 and 6 feet", "coyote_creek", 4.281},
								{"2019-08-17T02:54:00Z", "between 3 and 6 feet", "santa_monica", 3.022},
								{"2019-08-17T03:00:00Z", "between 3 and 6 feet", "coyote_creek", 4.154},
								{"2019-08-17T03:00:00Z", "between 3 and 6 feet", "santa_monica", 3.087},
								{"2019-08-17T03:06:00Z", "between 3 and 6 feet", "coyote_creek", 4.029},
								{"2019-08-17T03:06:00Z", "between 3 and 6 feet", "santa_monica", 3.215},
								{"2019-08-17T03:12:00Z", "between 3 and 6 feet", "coyote_creek", 3.911},
								{"2019-08-17T03:12:00Z", "between 3 and 6 feet", "santa_monica", 3.33},
								{"2019-08-17T03:18:00Z", "between 3 and 6 feet", "coyote_creek", 3.786},
								{"2019-08-17T03:18:00Z", "between 3 and 6 feet", "santa_monica", 3.32},
								{"2019-08-17T03:24:00Z", "between 3 and 6 feet", "coyote_creek", 3.645},
								{"2019-08-17T03:24:00Z", "between 3 and 6 feet", "santa_monica", 3.419},
								{"2019-08-17T03:30:00Z", "between 3 and 6 feet", "coyote_creek", 3.524},
								{"2019-08-17T03:30:00Z", "between 3 and 6 feet", "santa_monica", 3.547},
								{"2019-08-17T03:36:00Z", "between 3 and 6 feet", "coyote_creek", 3.399},
								{"2019-08-17T03:36:00Z", "between 3 and 6 feet", "santa_monica", 3.655},
								{"2019-08-17T03:42:00Z", "between 3 and 6 feet", "coyote_creek", 3.278},
								{"2019-08-17T03:42:00Z", "between 3 and 6 feet", "santa_monica", 3.658},
								{"2019-08-17T03:48:00Z", "between 3 and 6 feet", "coyote_creek", 3.159},
								{"2019-08-17T03:48:00Z", "between 3 and 6 feet", "santa_monica", 3.76},
								{"2019-08-17T03:54:00Z", "between 3 and 6 feet", "coyote_creek", 3.048},
								{"2019-08-17T03:54:00Z", "between 3 and 6 feet", "santa_monica", 3.819},
								{"2019-08-17T04:00:00Z", "below 3 feet", "coyote_creek", 2.943},
								{"2019-08-17T04:00:00Z", "between 3 and 6 feet", "santa_monica", 3.911},
								{"2019-08-17T04:06:00Z", "below 3 feet", "coyote_creek", 2.831},
								{"2019-08-17T04:06:00Z", "between 3 and 6 feet", "santa_monica", 4.055},
								{"2019-08-17T04:12:00Z", "below 3 feet", "coyote_creek", 2.717},
								{"2019-08-17T04:12:00Z", "between 3 and 6 feet", "santa_monica", 4.055},
								{"2019-08-17T04:18:00Z", "below 3 feet", "coyote_creek", 2.625},
								{"2019-08-17T04:18:00Z", "between 3 and 6 feet", "santa_monica", 4.124},
								{"2019-08-17T04:24:00Z", "below 3 feet", "coyote_creek", 2.533},
								{"2019-08-17T04:24:00Z", "between 3 and 6 feet", "santa_monica", 4.183},
								{"2019-08-17T04:30:00Z", "below 3 feet", "coyote_creek", 2.451},
								{"2019-08-17T04:30:00Z", "between 3 and 6 feet", "santa_monica", 4.242},
								{"2019-08-17T04:36:00Z", "below 3 feet", "coyote_creek", 2.385},
								{"2019-08-17T04:36:00Z", "between 3 and 6 feet", "santa_monica", 4.36},
								{"2019-08-17T04:42:00Z", "below 3 feet", "coyote_creek", 2.339},
								{"2019-08-17T04:42:00Z", "between 3 and 6 feet", "santa_monica", 4.501},
								{"2019-08-17T04:48:00Z", "below 3 feet", "coyote_creek", 2.293},
								{"2019-08-17T04:48:00Z", "between 3 and 6 feet", "santa_monica", 4.501},
								{"2019-08-17T04:54:00Z", "below 3 feet", "coyote_creek", 2.287},
								{"2019-08-17T04:54:00Z", "between 3 and 6 feet", "santa_monica", 4.567},
								{"2019-08-17T05:00:00Z", "below 3 feet", "coyote_creek", 2.29},
								{"2019-08-17T05:00:00Z", "between 3 and 6 feet", "santa_monica", 4.675},
								{"2019-08-17T05:06:00Z", "below 3 feet", "coyote_creek", 2.313},
								{"2019-08-17T05:06:00Z", "between 3 and 6 feet", "santa_monica", 4.642},
								{"2019-08-17T05:12:00Z", "below 3 feet", "coyote_creek", 2.359},
								{"2019-08-17T05:12:00Z", "between 3 and 6 feet", "santa_monica", 4.751},
								{"2019-08-17T05:18:00Z", "below 3 feet", "coyote_creek", 2.425},
								{"2019-08-17T05:18:00Z", "between 3 and 6 feet", "santa_monica", 4.888},
								{"2019-08-17T05:24:00Z", "below 3 feet", "coyote_creek", 2.513},
								{"2019-08-17T05:24:00Z", "between 3 and 6 feet", "santa_monica", 4.82},
								{"2019-08-17T05:30:00Z", "below 3 feet", "coyote_creek", 2.608},
								{"2019-08-17T05:30:00Z", "between 3 and 6 feet", "santa_monica", 4.8},
								{"2019-08-17T05:36:00Z", "below 3 feet", "coyote_creek", 2.703},
								{"2019-08-17T05:36:00Z", "between 3 and 6 feet", "santa_monica", 4.882},
								{"2019-08-17T05:42:00Z", "below 3 feet", "coyote_creek", 2.822},
								{"2019-08-17T05:42:00Z", "between 3 and 6 feet", "santa_monica", 4.898},
								{"2019-08-17T05:48:00Z", "below 3 feet", "coyote_creek", 2.927},
								{"2019-08-17T05:48:00Z", "between 3 and 6 feet", "santa_monica", 4.98},
								{"2019-08-17T05:54:00Z", "between 3 and 6 feet", "coyote_creek", 3.054},
								{"2019-08-17T05:54:00Z", "between 3 and 6 feet", "santa_monica", 4.997},
								{"2019-08-17T06:00:00Z", "between 3 and 6 feet", "coyote_creek", 3.176},
								{"2019-08-17T06:00:00Z", "between 3 and 6 feet", "santa_monica", 4.977},
								{"2019-08-17T06:06:00Z", "between 3 and 6 feet", "coyote_creek", 3.304},
								{"2019-08-17T06:06:00Z", "between 3 and 6 feet", "santa_monica", 4.987},
								{"2019-08-17T06:12:00Z", "between 3 and 6 feet", "coyote_creek", 3.432},
								{"2019-08-17T06:12:00Z", "between 3 and 6 feet", "santa_monica", 4.931},
								{"2019-08-17T06:18:00Z", "between 3 and 6 feet", "coyote_creek", 3.57},
								{"2019-08-17T06:18:00Z", "between 3 and 6 feet", "santa_monica", 5.02},
								{"2019-08-17T06:24:00Z", "between 3 and 6 feet", "coyote_creek", 3.72},
								{"2019-08-17T06:24:00Z", "between 3 and 6 feet", "santa_monica", 5.052},
								{"2019-08-17T06:30:00Z", "between 3 and 6 feet", "coyote_creek", 3.881},
								{"2019-08-17T06:30:00Z", "between 3 and 6 feet", "santa_monica", 5.052},
								{"2019-08-17T06:36:00Z", "between 3 and 6 feet", "coyote_creek", 4.049},
								{"2019-08-17T06:36:00Z", "between 3 and 6 feet", "santa_monica", 5.105},
								{"2019-08-17T06:42:00Z", "between 3 and 6 feet", "coyote_creek", 4.209},
								{"2019-08-17T06:42:00Z", "between 3 and 6 feet", "santa_monica", 5.089},
								{"2019-08-17T06:48:00Z", "between 3 and 6 feet", "coyote_creek", 4.383},
								{"2019-08-17T06:48:00Z", "between 3 and 6 feet", "santa_monica", 5.066},
								{"2019-08-17T06:54:00Z", "between 3 and 6 feet", "coyote_creek", 4.56},
								{"2019-08-17T06:54:00Z", "between 3 and 6 feet", "santa_monica", 5.059},
								{"2019-08-17T07:00:00Z", "between 3 and 6 feet", "coyote_creek", 4.744},
								{"2019-08-17T07:00:00Z", "between 3 and 6 feet", "santa_monica", 5.033},
								{"2019-08-17T07:06:00Z", "between 3 and 6 feet", "coyote_creek", 4.915},
								{"2019-08-17T07:06:00Z", "between 3 and 6 feet", "santa_monica", 5.039},
								{"2019-08-17T07:12:00Z", "between 3 and 6 feet", "coyote_creek", 5.102},
								{"2019-08-17T07:12:00Z", "between 3 and 6 feet", "santa_monica", 5.059},
								{"2019-08-17T07:18:00Z", "between 3 and 6 feet", "coyote_creek", 5.289},
								{"2019-08-17T07:18:00Z", "between 3 and 6 feet", "santa_monica", 4.964},
								{"2019-08-17T07:24:00Z", "between 3 and 6 feet", "coyote_creek", 5.469},
								{"2019-08-17T07:24:00Z", "between 3 and 6 feet", "santa_monica", 5.007},
								{"2019-08-17T07:30:00Z", "between 3 and 6 feet", "coyote_creek", 5.643},
								{"2019-08-17T07:30:00Z", "between 3 and 6 feet", "santa_monica", 4.921},
								{"2019-08-17T07:36:00Z", "between 3 and 6 feet", "coyote_creek", 5.814},
								{"2019-08-17T07:36:00Z", "between 3 and 6 feet", "santa_monica", 4.875},
								{"2019-08-17T07:42:00Z", "between 3 and 6 feet", "coyote_creek", 5.974},
								{"2019-08-17T07:42:00Z", "between 3 and 6 feet", "santa_monica", 4.839},
								{"2019-08-17T07:48:00Z", "between 3 and 6 feet", "santa_monica", 4.8},
								{"2019-08-17T07:48:00Z", "between 6 and 9 feet", "coyote_creek", 6.138},
								{"2019-08-17T07:54:00Z", "between 3 and 6 feet", "santa_monica", 4.724},
								{"2019-08-17T07:54:00Z", "between 6 and 9 feet", "coyote_creek", 6.293},
								{"2019-08-17T08:00:00Z", "between 3 and 6 feet", "santa_monica", 4.547},
								{"2019-08-17T08:00:00Z", "between 6 and 9 feet", "coyote_creek", 6.447},
								{"2019-08-17T08:06:00Z", "between 3 and 6 feet", "santa_monica", 4.488},
								{"2019-08-17T08:06:00Z", "between 6 and 9 feet", "coyote_creek", 6.601},
								{"2019-08-17T08:12:00Z", "between 3 and 6 feet", "santa_monica", 4.508},
								{"2019-08-17T08:12:00Z", "between 6 and 9 feet", "coyote_creek", 6.749},
								{"2019-08-17T08:18:00Z", "between 3 and 6 feet", "santa_monica", 4.393},
								{"2019-08-17T08:18:00Z", "between 6 and 9 feet", "coyote_creek", 6.893},
								{"2019-08-17T08:24:00Z", "between 3 and 6 feet", "santa_monica", 4.334},
								{"2019-08-17T08:24:00Z", "between 6 and 9 feet", "coyote_creek", 7.037},
								{"2019-08-17T08:30:00Z", "between 3 and 6 feet", "santa_monica", 4.314},
								{"2019-08-17T08:30:00Z", "between 6 and 9 feet", "coyote_creek", 7.172},
								{"2019-08-17T08:36:00Z", "between 3 and 6 feet", "santa_monica", 4.173},
								{"2019-08-17T08:36:00Z", "between 6 and 9 feet", "coyote_creek", 7.3},
								{"2019-08-17T08:42:00Z", "between 3 and 6 feet", "santa_monica", 4.131},
								{"2019-08-17T08:42:00Z", "between 6 and 9 feet", "coyote_creek", 7.428},
								{"2019-08-17T08:48:00Z", "between 3 and 6 feet", "santa_monica", 3.996},
								{"2019-08-17T08:48:00Z", "between 6 and 9 feet", "coyote_creek", 7.549},
								{"2019-08-17T08:54:00Z", "between 3 and 6 feet", "santa_monica", 3.924},
								{"2019-08-17T08:54:00Z", "between 6 and 9 feet", "coyote_creek", 7.667},
								{"2019-08-17T09:00:00Z", "between 3 and 6 feet", "santa_monica", 3.93},
								{"2019-08-17T09:00:00Z", "between 6 and 9 feet", "coyote_creek", 7.776},
								{"2019-08-17T09:06:00Z", "between 3 and 6 feet", "santa_monica", 3.78},
								{"2019-08-17T09:06:00Z", "between 6 and 9 feet", "coyote_creek", 7.874},
								{"2019-08-17T09:12:00Z", "between 3 and 6 feet", "santa_monica", 3.773},
								{"2019-08-17T09:12:00Z", "between 6 and 9 feet", "coyote_creek", 7.963},
								{"2019-08-17T09:18:00Z", "between 3 and 6 feet", "santa_monica", 3.724},
								{"2019-08-17T09:18:00Z", "between 6 and 9 feet", "coyote_creek", 8.045},
								{"2019-08-17T09:24:00Z", "between 3 and 6 feet", "santa_monica", 3.556},
								{"2019-08-17T09:24:00Z", "between 6 and 9 feet", "coyote_creek", 8.114},
								{"2019-08-17T09:30:00Z", "between 3 and 6 feet", "santa_monica", 3.461},
								{"2019-08-17T09:30:00Z", "between 6 and 9 feet", "coyote_creek", 8.166},
								{"2019-08-17T09:36:00Z", "between 3 and 6 feet", "santa_monica", 3.373},
								{"2019-08-17T09:36:00Z", "between 6 and 9 feet", "coyote_creek", 8.209},
								{"2019-08-17T09:42:00Z", "between 3 and 6 feet", "santa_monica", 3.281},
								{"2019-08-17T09:42:00Z", "between 6 and 9 feet", "coyote_creek", 8.238},
								{"2019-08-17T09:48:00Z", "between 3 and 6 feet", "santa_monica", 3.126144146},
								{"2019-08-17T09:48:00Z", "between 6 and 9 feet", "coyote_creek", 8.258},
								{"2019-08-17T09:54:00Z", "between 3 and 6 feet", "santa_monica", 3.077},
								{"2019-08-17T09:54:00Z", "between 6 and 9 feet", "coyote_creek", 8.271},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "NOAA_water_database_autogen_h2o_feet",
		Query: fmt.Sprintf(`SELECT * FROM "NOAA_water_database"."autogen"."h2o_feet" limit 20`),
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
		Name:  "NOAA_water_database_h2o_feet",
		Query: fmt.Sprintf(`SELECT * FROM "NOAA_water_database".."h2o_feet" limit 50`),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "level description", "location", "water_level"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", "below 3 feet", "santa_monica", 2.064},
								{"2019-08-17T00:00:00Z", "between 6 and 9 feet", "coyote_creek", 8.12},
								{"2019-08-17T00:06:00Z", "below 3 feet", "santa_monica", 2.116},
								{"2019-08-17T00:06:00Z", "between 6 and 9 feet", "coyote_creek", 8.005},
								{"2019-08-17T00:12:00Z", "below 3 feet", "santa_monica", 2.028},
								{"2019-08-17T00:12:00Z", "between 6 and 9 feet", "coyote_creek", 7.887},
								{"2019-08-17T00:18:00Z", "below 3 feet", "santa_monica", 2.126},
								{"2019-08-17T00:18:00Z", "between 6 and 9 feet", "coyote_creek", 7.762},
								{"2019-08-17T00:24:00Z", "below 3 feet", "santa_monica", 2.041},
								{"2019-08-17T00:24:00Z", "between 6 and 9 feet", "coyote_creek", 7.635},
								{"2019-08-17T00:30:00Z", "below 3 feet", "santa_monica", 2.051},
								{"2019-08-17T00:30:00Z", "between 6 and 9 feet", "coyote_creek", 7.5},
								{"2019-08-17T00:36:00Z", "below 3 feet", "santa_monica", 2.067},
								{"2019-08-17T00:36:00Z", "between 6 and 9 feet", "coyote_creek", 7.372},
								{"2019-08-17T00:42:00Z", "below 3 feet", "santa_monica", 2.057},
								{"2019-08-17T00:42:00Z", "between 6 and 9 feet", "coyote_creek", 7.234},
								{"2019-08-17T00:48:00Z", "below 3 feet", "santa_monica", 1.991},
								{"2019-08-17T00:48:00Z", "between 6 and 9 feet", "coyote_creek", 7.11},
								{"2019-08-17T00:54:00Z", "below 3 feet", "santa_monica", 2.054},
								{"2019-08-17T00:54:00Z", "between 6 and 9 feet", "coyote_creek", 6.982},
								{"2019-08-17T01:00:00Z", "below 3 feet", "santa_monica", 2.018},
								{"2019-08-17T01:00:00Z", "between 6 and 9 feet", "coyote_creek", 6.837},
								{"2019-08-17T01:06:00Z", "below 3 feet", "santa_monica", 2.096},
								{"2019-08-17T01:06:00Z", "between 6 and 9 feet", "coyote_creek", 6.713},
								{"2019-08-17T01:12:00Z", "below 3 feet", "santa_monica", 2.1},
								{"2019-08-17T01:12:00Z", "between 6 and 9 feet", "coyote_creek", 6.578},
								{"2019-08-17T01:18:00Z", "below 3 feet", "santa_monica", 2.106},
								{"2019-08-17T01:18:00Z", "between 6 and 9 feet", "coyote_creek", 6.44},
								{"2019-08-17T01:24:00Z", "below 3 feet", "santa_monica", 2.126144146},
								{"2019-08-17T01:24:00Z", "between 6 and 9 feet", "coyote_creek", 6.299},
								{"2019-08-17T01:30:00Z", "below 3 feet", "santa_monica", 2.1},
								{"2019-08-17T01:30:00Z", "between 6 and 9 feet", "coyote_creek", 6.168},
								{"2019-08-17T01:36:00Z", "below 3 feet", "santa_monica", 2.136},
								{"2019-08-17T01:36:00Z", "between 6 and 9 feet", "coyote_creek", 6.024},
								{"2019-08-17T01:42:00Z", "below 3 feet", "santa_monica", 2.182},
								{"2019-08-17T01:42:00Z", "between 3 and 6 feet", "coyote_creek", 5.879},
								{"2019-08-17T01:48:00Z", "below 3 feet", "santa_monica", 2.306},
								{"2019-08-17T01:48:00Z", "between 3 and 6 feet", "coyote_creek", 5.745},
								{"2019-08-17T01:54:00Z", "below 3 feet", "santa_monica", 2.448},
								{"2019-08-17T01:54:00Z", "between 3 and 6 feet", "coyote_creek", 5.617},
								{"2019-08-17T02:00:00Z", "below 3 feet", "santa_monica", 2.464},
								{"2019-08-17T02:00:00Z", "between 3 and 6 feet", "coyote_creek", 5.472},
								{"2019-08-17T02:06:00Z", "below 3 feet", "santa_monica", 2.467},
								{"2019-08-17T02:06:00Z", "between 3 and 6 feet", "coyote_creek", 5.348},
								{"2019-08-17T02:12:00Z", "below 3 feet", "santa_monica", 2.516},
								{"2019-08-17T02:12:00Z", "between 3 and 6 feet", "coyote_creek", 5.2},
								{"2019-08-17T02:18:00Z", "below 3 feet", "santa_monica", 2.674},
								{"2019-08-17T02:18:00Z", "between 3 and 6 feet", "coyote_creek", 5.072},
								{"2019-08-17T02:24:00Z", "below 3 feet", "santa_monica", 2.684},
								{"2019-08-17T02:24:00Z", "between 3 and 6 feet", "coyote_creek", 4.934},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "location_h2o_feet",
		Query: fmt.Sprintf(`SELECT "location" FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
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
		Name:  "water_level_location_h2o_feet",
		Query: fmt.Sprintf(`SELECT "water_level","location" FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "water_level", "location"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", 8.12, "coyote_creek"},
								{"2019-08-17T00:00:00Z", 2.064, "santa_monica"},
								{"2019-08-17T00:06:00Z", 8.005, "coyote_creek"},
								{"2019-08-17T00:06:00Z", 2.116, "santa_monica"},
								{"2019-08-17T00:12:00Z", 7.887, "coyote_creek"},
								{"2019-08-17T00:12:00Z", 2.028, "santa_monica"},
								{"2019-08-17T00:18:00Z", 7.762, "coyote_creek"},
								{"2019-08-17T00:18:00Z", 2.126, "santa_monica"},
								{"2019-08-17T00:24:00Z", 7.635, "coyote_creek"},
								{"2019-08-17T00:24:00Z", 2.041, "santa_monica"},
								{"2019-08-17T00:30:00Z", 7.5, "coyote_creek"},
								{"2019-08-17T00:30:00Z", 2.051, "santa_monica"},
								{"2019-08-17T00:36:00Z", 7.372, "coyote_creek"},
								{"2019-08-17T00:36:00Z", 2.067, "santa_monica"},
								{"2019-08-17T00:42:00Z", 7.234, "coyote_creek"},
								{"2019-08-17T00:42:00Z", 2.057, "santa_monica"},
								{"2019-08-17T00:48:00Z", 7.11, "coyote_creek"},
								{"2019-08-17T00:48:00Z", 1.991, "santa_monica"},
								{"2019-08-17T00:54:00Z", 6.982, "coyote_creek"},
								{"2019-08-17T00:54:00Z", 2.054, "santa_monica"},
							},
						},
					},
				},
			},
		},
	},
	//WHERE
	{
		Name:  "h2o_feet_water_level_8",
		Query: fmt.Sprintf(`SELECT * FROM "%s"."%s"."h2o_feet" WHERE "water_level" > 8 limit 30`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "level description", "location", "water_level"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", "between 6 and 9 feet", "coyote_creek", 8.12},
								{"2019-08-17T00:06:00Z", "between 6 and 9 feet", "coyote_creek", 8.005},
								{"2019-08-17T09:18:00Z", "between 6 and 9 feet", "coyote_creek", 8.045},
								{"2019-08-17T09:24:00Z", "between 6 and 9 feet", "coyote_creek", 8.114},
								{"2019-08-17T09:30:00Z", "between 6 and 9 feet", "coyote_creek", 8.166},
								{"2019-08-17T09:36:00Z", "between 6 and 9 feet", "coyote_creek", 8.209},
								{"2019-08-17T09:42:00Z", "between 6 and 9 feet", "coyote_creek", 8.238},
								{"2019-08-17T09:48:00Z", "between 6 and 9 feet", "coyote_creek", 8.258},
								{"2019-08-17T09:54:00Z", "between 6 and 9 feet", "coyote_creek", 8.271},
								{"2019-08-17T10:00:00Z", "between 6 and 9 feet", "coyote_creek", 8.281},
								{"2019-08-17T10:06:00Z", "between 6 and 9 feet", "coyote_creek", 8.284},
								{"2019-08-17T10:12:00Z", "between 6 and 9 feet", "coyote_creek", 8.291},
								{"2019-08-17T10:18:00Z", "between 6 and 9 feet", "coyote_creek", 8.297},
								{"2019-08-17T10:24:00Z", "between 6 and 9 feet", "coyote_creek", 8.297},
								{"2019-08-17T10:30:00Z", "between 6 and 9 feet", "coyote_creek", 8.287},
								{"2019-08-17T10:36:00Z", "between 6 and 9 feet", "coyote_creek", 8.287},
								{"2019-08-17T10:42:00Z", "between 6 and 9 feet", "coyote_creek", 8.278},
								{"2019-08-17T10:48:00Z", "between 6 and 9 feet", "coyote_creek", 8.258},
								{"2019-08-17T10:54:00Z", "between 6 and 9 feet", "coyote_creek", 8.222},
								{"2019-08-17T11:00:00Z", "between 6 and 9 feet", "coyote_creek", 8.182},
								{"2019-08-17T11:06:00Z", "between 6 and 9 feet", "coyote_creek", 8.133},
								{"2019-08-17T11:12:00Z", "between 6 and 9 feet", "coyote_creek", 8.061},
								{"2019-08-17T21:42:00Z", "between 6 and 9 feet", "coyote_creek", 8.022},
								{"2019-08-17T21:48:00Z", "between 6 and 9 feet", "coyote_creek", 8.123},
								{"2019-08-17T21:54:00Z", "between 6 and 9 feet", "coyote_creek", 8.215},
								{"2019-08-17T22:00:00Z", "between 6 and 9 feet", "coyote_creek", 8.294},
								{"2019-08-17T22:06:00Z", "between 6 and 9 feet", "coyote_creek", 8.373},
								{"2019-08-17T22:12:00Z", "between 6 and 9 feet", "coyote_creek", 8.435},
								{"2019-08-17T22:18:00Z", "between 6 and 9 feet", "coyote_creek", 8.488},
								{"2019-08-17T22:24:00Z", "between 6 and 9 feet", "coyote_creek", 8.527},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "h2o_feet_level_description_below_3_feet",
		Query: fmt.Sprintf(`SELECT * FROM "%s"."%s"."h2o_feet" WHERE "level description" = 'below 3 feet' limit 20 offset 1000`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "level description", "location", "water_level"},
							Values: []suite.Row{
								{"2019-08-24T06:12:00Z", "below 3 feet", "santa_monica", 1.424},
								{"2019-08-24T06:18:00Z", "below 3 feet", "santa_monica", 1.437},
								{"2019-08-24T06:24:00Z", "below 3 feet", "santa_monica", 1.375},
								{"2019-08-24T06:30:00Z", "below 3 feet", "santa_monica", 1.289},
								{"2019-08-24T06:36:00Z", "below 3 feet", "santa_monica", 1.161},
								{"2019-08-24T06:42:00Z", "below 3 feet", "santa_monica", 1.138},
								{"2019-08-24T06:48:00Z", "below 3 feet", "santa_monica", 1.102},
								{"2019-08-24T06:54:00Z", "below 3 feet", "santa_monica", 1.043},
								{"2019-08-24T07:00:00Z", "below 3 feet", "santa_monica", 0.984},
								{"2019-08-24T07:06:00Z", "below 3 feet", "santa_monica", 0.896},
								{"2019-08-24T07:12:00Z", "below 3 feet", "santa_monica", 0.86},
								{"2019-08-24T07:18:00Z", "below 3 feet", "santa_monica", 0.807},
								{"2019-08-24T07:24:00Z", "below 3 feet", "santa_monica", 0.81},
								{"2019-08-24T07:30:00Z", "below 3 feet", "santa_monica", 0.807},
								{"2019-08-24T07:36:00Z", "below 3 feet", "santa_monica", 0.797},
								{"2019-08-24T07:42:00Z", "below 3 feet", "santa_monica", 0.715},
								{"2019-08-24T07:48:00Z", "below 3 feet", "santa_monica", 0.676},
								{"2019-08-24T07:54:00Z", "below 3 feet", "santa_monica", 0.712},
								{"2019-08-24T08:00:00Z", "below 3 feet", "santa_monica", 0.781},
								{"2019-08-24T08:06:00Z", "below 3 feet", "santa_monica", 0.745},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "h2o_feet_waterlevel_2_11",
		Query: fmt.Sprintf(`SELECT * FROM "%s"."%s"."h2o_feet" WHERE "water_level" + 2 > 11.9 limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "level description", "location", "water_level"},
							Values: []suite.Row{
								{"2019-08-28T07:06:00Z", "at or greater than 9 feet", "coyote_creek", 9.902},
								{"2019-08-28T07:12:00Z", "at or greater than 9 feet", "coyote_creek", 9.938},
								{"2019-08-28T07:18:00Z", "at or greater than 9 feet", "coyote_creek", 9.957},
								{"2019-08-28T07:24:00Z", "at or greater than 9 feet", "coyote_creek", 9.964},
								{"2019-08-28T07:30:00Z", "at or greater than 9 feet", "coyote_creek", 9.954},
								{"2019-08-28T07:36:00Z", "at or greater than 9 feet", "coyote_creek", 9.941},
								{"2019-08-28T07:42:00Z", "at or greater than 9 feet", "coyote_creek", 9.925},
								{"2019-08-28T07:48:00Z", "at or greater than 9 feet", "coyote_creek", 9.902},
								{"2019-09-01T23:30:00Z", "at or greater than 9 feet", "coyote_creek", 9.902},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "h2o_feet_water_level_location_santa_monica",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "water_level"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", 2.064},
								{"2019-08-17T00:06:00Z", 2.116},
								{"2019-08-17T00:12:00Z", 2.028},
								{"2019-08-17T00:18:00Z", 2.126},
								{"2019-08-17T00:24:00Z", 2.041},
								{"2019-08-17T00:30:00Z", 2.051},
								{"2019-08-17T00:36:00Z", 2.067},
								{"2019-08-17T00:42:00Z", 2.057},
								{"2019-08-17T00:48:00Z", 1.991},
								{"2019-08-17T00:54:00Z", 2.054},
								{"2019-08-17T01:00:00Z", 2.018},
								{"2019-08-17T01:06:00Z", 2.096},
								{"2019-08-17T01:12:00Z", 2.1},
								{"2019-08-17T01:18:00Z", 2.106},
								{"2019-08-17T01:24:00Z", 2.126144146},
								{"2019-08-17T01:30:00Z", 2.1},
								{"2019-08-17T01:36:00Z", 2.136},
								{"2019-08-17T01:42:00Z", 2.182},
								{"2019-08-17T01:48:00Z", 2.306},
								{"2019-08-17T01:54:00Z", 2.448},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "h2o_feet_location_santa_monica_water_level",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE "location" <> 'santa_monica' AND (water_level < -0.59 OR water_level > 9.95) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "water_level"},
							Values: []suite.Row{
								{"2019-08-28T07:18:00Z", 9.957},
								{"2019-08-28T07:24:00Z", 9.964},
								{"2019-08-28T07:30:00Z", 9.954},
								{"2019-08-28T14:30:00Z", -0.61},
								{"2019-08-28T14:36:00Z", -0.591},
								{"2019-08-29T15:18:00Z", -0.594},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "h2o_feet_time_7d",
		Query: fmt.Sprintf(`SELECT * FROM "%s"."%s"."h2o_feet" WHERE time > now() - 7d limit 20`, db, rp),
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
		Name:  "h2o_feet_level_description_9_feet",
		Query: fmt.Sprintf(`SELECT "level description" FROM "%s"."%s"."h2o_feet" WHERE "level description" = 'at or greater than 9 feet' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "level description"},
							Values: []suite.Row{
								{"2019-08-25T04:00:00Z", "at or greater than 9 feet"},
								{"2019-08-25T04:06:00Z", "at or greater than 9 feet"},
								{"2019-08-25T04:12:00Z", "at or greater than 9 feet"},
								{"2019-08-25T04:18:00Z", "at or greater than 9 feet"},
								{"2019-08-25T04:24:00Z", "at or greater than 9 feet"},
								{"2019-08-25T04:30:00Z", "at or greater than 9 feet"},
								{"2019-08-25T04:36:00Z", "at or greater than 9 feet"},
								{"2019-08-25T04:42:00Z", "at or greater than 9 feet"},
								{"2019-08-25T04:48:00Z", "at or greater than 9 feet"},
								{"2019-08-25T04:54:00Z", "at or greater than 9 feet"},
								{"2019-08-25T05:00:00Z", "at or greater than 9 feet"},
								{"2019-08-25T05:06:00Z", "at or greater than 9 feet"},
								{"2019-08-25T05:12:00Z", "at or greater than 9 feet"},
								{"2019-08-25T05:18:00Z", "at or greater than 9 feet"},
								{"2019-08-26T04:42:00Z", "at or greater than 9 feet"},
								{"2019-08-26T04:48:00Z", "at or greater than 9 feet"},
								{"2019-08-26T04:54:00Z", "at or greater than 9 feet"},
								{"2019-08-26T05:00:00Z", "at or greater than 9 feet"},
								{"2019-08-26T05:06:00Z", "at or greater than 9 feet"},
								{"2019-08-26T05:12:00Z", "at or greater than 9 feet"},
							},
						},
					},
				},
			},
		},
	},
	//GROUP BY
	{
		Name:  "h2o_feet_mean_water_level_location",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" GROUP BY "location" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 5.3591424203039155},
							},
						},
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 3.5307120942458803},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "h2o_quality_mean_index_location_randtag",
		Query: fmt.Sprintf(`SELECT MEAN("index") FROM "%s"."%s"."h2o_quality" GROUP BY location,randtag limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_quality",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 50.69033760186263},
							},
						},
						{
							Name:    "h2o_quality",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 49.661867544220485},
							},
						},
						{
							Name:    "h2o_quality",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 49.360939907550076},
							},
						},
						{
							Name:    "h2o_quality",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 49.132712456344585},
							},
						},
						{
							Name:    "h2o_quality",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 50.2937984496124},
							},
						},
						{
							Name:    "h2o_quality",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 49.99919903884662},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "h2o_quality_mean_index_group_by_all",
		Query: fmt.Sprintf(`SELECT MEAN("index") FROM "%s"."%s"."h2o_quality" GROUP BY * limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_quality",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 50.69033760186263},
							},
						},
						{
							Name:    "h2o_quality",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 49.661867544220485},
							},
						},
						{
							Name:    "h2o_quality",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 49.360939907550076},
							},
						},
						{
							Name:    "h2o_quality",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 49.132712456344585},
							},
						},
						{
							Name:    "h2o_quality",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 50.2937984496124},
							},
						},
						{
							Name:    "h2o_quality",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 49.99919903884662},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "water_level_location_time",
		Query: fmt.Sprintf(`SELECT "water_level","location" FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' limits 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "water_level_time_12m_location",
		Query: fmt.Sprintf(`SELECT COUNT("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m),"location" limits 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "water_level_location_coyote_creek_time",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE "location"='coyote_creek' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:18:00Z' limits 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "water_level_coyote_creek_12m",
		Query: fmt.Sprintf(`SELECT COUNT("water_level") FROM "%s"."%s"."h2o_feet" WHERE "location"='coyote_creek' AND time >= '2015-08-18T00:06:00Z' AND time < '2015-08-18T00:18:00Z' GROUP BY time(12m) limit 20`, db, rp),
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
		Name:  "water_level_coyote_creek_18m_6m",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE "location"='coyote_creek' AND time >= '2015-08-18T00:06:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(18m,6m) limit 20`, db, rp),
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
		Name:  "water_level_coyote_creek_18m",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE "location"='coyote_creek' AND time >= '2015-08-18T00:06:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(18m) limit 20`, db, rp),
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
		Name:  "water_level_coyote_creek_18m_12m",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE "location"='coyote_creek' AND time >= '2015-08-18T00:06:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(18m,-12m) limit 20`, db, rp),
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
		Name:  "water_level_coyote_creek_12m_6m",
		Query: fmt.Sprintf(`SELECT COUNT("water_level") FROM "%s"."%s"."h2o_feet" WHERE "location"='coyote_creek' AND time >= '2015-08-18T00:06:00Z' AND time < '2015-08-18T00:18:00Z' GROUP BY time(12m,6m) limit 20`, db, rp),
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
		Name:  "water_level_coyote_creek_12m_fill_100",
		Query: fmt.Sprintf(`SELECT MAX("water_level") FROM "%s"."%s"."h2o_feet" WHERE "location"='coyote_creek' AND time >= '2015-09-18T16:00:00Z' AND time <= '2015-09-18T16:42:00Z' GROUP BY time(12m) fill(100) limit 20`, db, rp),
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
		Name:  "water_level_coyote_creek_12m_fill_800",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s","h2o_feet" WHERE "location" = 'coyote_creek' AND time >= '2015-09-18T22:00:00Z' AND time <= '2015-09-18T22:18:00Z' GROUP BY time(12m) fill(800) limit 200`, db, rp),
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
		Name:  "water_level_coyote_creek_24_12m_fill_previous",
		Query: fmt.Sprintf(`SELECT MAX("water_level") FROM "%s"."%s"."h2o_feet" WHERE location = 'coyote_creek' AND time >= '2015-09-18T16:24:00Z' AND time <= '2015-09-18T16:54:00Z' GROUP BY time(12m) fill(previous) limit 20`, db, rp),
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
		Name:  "water_level_coyote_creek_36_12m_fill_previous",
		Query: fmt.Sprintf(`SELECT MAX("water_level") FROM "%s"."%s"."h2o_feet" WHERE location = 'coyote_creek' AND time >= '2015-09-18T16:36:00Z' AND time <= '2015-09-18T16:54:00Z' GROUP BY time(12m) fill(previous) limit 20`, db, rp),
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
		Name:  "tadpoles_12m_fill_linear",
		Query: fmt.Sprintf(`SELECT MEAN("tadpoles") FROM "%s"."%s"."pond" WHERE time > '2016-11-11T21:24:00Z' AND time <= '2016-11-11T22:06:00Z' GROUP BY time(12m) fill(linear) limit 20`, db, rp),
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
		Name:  "tadpoles_12m_fill_linear_equal",
		Query: fmt.Sprintf(`SELECT MEAN("tadpoles") FROM "%s"."%s"."pond" WHERE time >= '2016-11-11T21:36:00Z' AND time <= '2016-11-11T22:06:00Z' GROUP BY time(12m) fill(linear) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//ORDER BY time DESC
	{
		Name:  "time_desc_location_santa_monica",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' ORDER BY time DESC limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "water_level"},
							Values: []suite.Row{
								{"2019-09-17T21:42:00Z", 4.938},
								{"2019-09-17T21:36:00Z", 5.066},
								{"2019-09-17T21:30:00Z", 5.01},
								{"2019-09-17T21:24:00Z", 5.013},
								{"2019-09-17T21:18:00Z", 5.072},
								{"2019-09-17T21:12:00Z", 5.213},
								{"2019-09-17T21:06:00Z", 5.341},
								{"2019-09-17T21:00:00Z", 5.338},
								{"2019-09-17T20:54:00Z", 5.322},
								{"2019-09-17T20:48:00Z", 5.24},
								{"2019-09-17T20:42:00Z", 5.302},
								{"2019-09-17T20:36:00Z", 5.62},
								{"2019-09-17T20:30:00Z", 5.604},
								{"2019-09-17T20:24:00Z", 5.502},
								{"2019-09-17T20:18:00Z", 5.551},
								{"2019-09-17T20:12:00Z", 5.459},
								{"2019-09-17T20:06:00Z", 5.62},
								{"2019-09-17T20:00:00Z", 5.627},
								{"2019-09-17T19:54:00Z", 5.522},
								{"2019-09-17T19:48:00Z", 5.499},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "time_desc_mean_water_level_time",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:42:00Z' GROUP BY time(12m) ORDER BY time DESC limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//LIMIT and SLIMIT
	{
		Name:  "limit_water_level_location_3",
		Query: fmt.Sprintf(`SELECT "water_level","location" FROM "%s"."%s"."h2o_feet" LIMIT 3`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "water_level", "location"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", 8.12, "coyote_creek"},
								{"2019-08-17T00:00:00Z", 2.064, "santa_monica"},
								{"2019-08-17T00:06:00Z", 8.005, "coyote_creek"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "limit_mean_water_level_12m_2",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:42:00Z' GROUP BY *,time(12m) LIMIT 2`, db, rp),
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
		Name:  "slimit_water_level_1",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" GROUP BY * limit 20 SLIMIT 1`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "water_level"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", 8.12},
								{"2019-08-17T00:06:00Z", 8.005},
								{"2019-08-17T00:12:00Z", 7.887},
								{"2019-08-17T00:18:00Z", 7.762},
								{"2019-08-17T00:24:00Z", 7.635},
								{"2019-08-17T00:30:00Z", 7.5},
								{"2019-08-17T00:36:00Z", 7.372},
								{"2019-08-17T00:42:00Z", 7.234},
								{"2019-08-17T00:48:00Z", 7.11},
								{"2019-08-17T00:54:00Z", 6.982},
								{"2019-08-17T01:00:00Z", 6.837},
								{"2019-08-17T01:06:00Z", 6.713},
								{"2019-08-17T01:12:00Z", 6.578},
								{"2019-08-17T01:18:00Z", 6.44},
								{"2019-08-17T01:24:00Z", 6.299},
								{"2019-08-17T01:30:00Z", 6.168},
								{"2019-08-17T01:36:00Z", 6.024},
								{"2019-08-17T01:42:00Z", 5.879},
								{"2019-08-17T01:48:00Z", 5.745},
								{"2019-08-17T01:54:00Z", 5.617},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "slimit_water_level_12m_1",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:42:00Z' GROUP BY *,time(12m) SLIMIT 1`, db, rp),
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
		Name:  "limit_3_slimit_1_water_level",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" GROUP BY * LIMIT 3 SLIMIT 1`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "water_level"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", 8.12},
								{"2019-08-17T00:06:00Z", 8.005},
								{"2019-08-17T00:12:00Z", 7.887},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "limit_2_slimit_1_water_level_12m",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:42:00Z' GROUP BY *,time(12m) LIMIT 2 SLIMIT 1`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//OFFSET and SOFFSET
	{
		Name:  "limit_3_offset_3_water_level_location",
		Query: fmt.Sprintf(`SELECT "water_level","location" FROM "%s"."%s"."h2o_feet" LIMIT 3 OFFSET 3`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "water_level", "location"},
							Values: []suite.Row{
								{"2019-08-17T00:06:00Z", 2.116, "santa_monica"},
								{"2019-08-17T00:12:00Z", 7.887, "coyote_creek"},
								{"2019-08-17T00:12:00Z", 2.028, "santa_monica"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "limit_2_offset_2_slimit_1_water_level_time",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:42:00Z' GROUP BY *,time(12m) ORDER BY time DESC LIMIT 2 OFFSET 2 SLIMIT 1`, db, rp),
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
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
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
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "time_water_level_location_rfc3339_like",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18' AND time <= '2015-08-18 00:12:00' limit 20`, db, rp),
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
		Name:  "time_water_level_location_timestamps",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= 1439856000000000000 AND time <= 1439856720000000000 limit 20`, db, rp),
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
		Name:  "time_water_level_location_second_timestamps",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= 1439856000s AND time <= 1439856720s limit 20`, db, rp),
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
		Name:  "time_water_level_rfc3999_6m",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE time > '2015-09-18T21:24:00Z' + 6m limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "water_level"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", 8.12},
								{"2019-08-17T00:00:00Z", 2.064},
								{"2019-08-17T00:06:00Z", 8.005},
								{"2019-08-17T00:06:00Z", 2.116},
								{"2019-08-17T00:12:00Z", 7.887},
								{"2019-08-17T00:12:00Z", 2.028},
								{"2019-08-17T00:18:00Z", 7.762},
								{"2019-08-17T00:18:00Z", 2.126},
								{"2019-08-17T00:24:00Z", 7.635},
								{"2019-08-17T00:24:00Z", 2.041},
								{"2019-08-17T00:30:00Z", 7.5},
								{"2019-08-17T00:30:00Z", 2.051},
								{"2019-08-17T00:36:00Z", 7.372},
								{"2019-08-17T00:36:00Z", 2.067},
								{"2019-08-17T00:42:00Z", 7.234},
								{"2019-08-17T00:42:00Z", 2.057},
								{"2019-08-17T00:48:00Z", 7.11},
								{"2019-08-17T00:48:00Z", 1.991},
								{"2019-08-17T00:54:00Z", 6.982},
								{"2019-08-17T00:54:00Z", 2.054},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "time_water_level_timestamp_6m",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE time > 24043524m - 6m limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "water_level"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", 8.12},
								{"2019-08-17T00:00:00Z", 2.064},
								{"2019-08-17T00:06:00Z", 8.005},
								{"2019-08-17T00:06:00Z", 2.116},
								{"2019-08-17T00:12:00Z", 7.887},
								{"2019-08-17T00:12:00Z", 2.028},
								{"2019-08-17T00:18:00Z", 7.762},
								{"2019-08-17T00:18:00Z", 2.126},
								{"2019-08-17T00:24:00Z", 7.635},
								{"2019-08-17T00:24:00Z", 2.041},
								{"2019-08-17T00:30:00Z", 7.5},
								{"2019-08-17T00:30:00Z", 2.051},
								{"2019-08-17T00:36:00Z", 7.372},
								{"2019-08-17T00:36:00Z", 2.067},
								{"2019-08-17T00:42:00Z", 7.234},
								{"2019-08-17T00:42:00Z", 2.057},
								{"2019-08-17T00:48:00Z", 7.11},
								{"2019-08-17T00:48:00Z", 1.991},
								{"2019-08-17T00:54:00Z", 6.982},
								{"2019-08-17T00:54:00Z", 2.054},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "time_now_water_level_1h",
		Query: fmt.Sprintf(`SELECT "water_level" FROM "%s"."%s"."h2o_feet" WHERE time > now() - 1h limit 20`, db, rp),
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
		Name:  "time_now_level_description_1000d",
		Query: fmt.Sprintf(`SELECT "level description" FROM "%s"."%s"."h2o_feet" WHERE time > '2015-09-18T21:18:00Z' AND time < now() + 1000d limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "level description"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", "below 3 feet"},
								{"2019-08-17T00:00:00Z", "between 6 and 9 feet"},
								{"2019-08-17T00:06:00Z", "below 3 feet"},
								{"2019-08-17T00:06:00Z", "between 6 and 9 feet"},
								{"2019-08-17T00:12:00Z", "below 3 feet"},
								{"2019-08-17T00:12:00Z", "between 6 and 9 feet"},
								{"2019-08-17T00:18:00Z", "below 3 feet"},
								{"2019-08-17T00:18:00Z", "between 6 and 9 feet"},
								{"2019-08-17T00:24:00Z", "below 3 feet"},
								{"2019-08-17T00:24:00Z", "between 6 and 9 feet"},
								{"2019-08-17T00:30:00Z", "below 3 feet"},
								{"2019-08-17T00:30:00Z", "between 6 and 9 feet"},
								{"2019-08-17T00:36:00Z", "below 3 feet"},
								{"2019-08-17T00:36:00Z", "between 6 and 9 feet"},
								{"2019-08-17T00:42:00Z", "below 3 feet"},
								{"2019-08-17T00:42:00Z", "between 6 and 9 feet"},
								{"2019-08-17T00:48:00Z", "below 3 feet"},
								{"2019-08-17T00:48:00Z", "between 6 and 9 feet"},
								{"2019-08-17T00:54:00Z", "below 3 feet"},
								{"2019-08-17T00:54:00Z", "between 6 and 9 feet"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "time_fill_water_level_santa_monica_12m",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE "location"='santa_monica' AND time >= '2015-09-18T21:30:00Z' GROUP BY time(12m) fill(none) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", 2.09},
								{"2019-08-17T00:12:00Z", 2.077},
								{"2019-08-17T00:24:00Z", 2.0460000000000003},
								{"2019-08-17T00:36:00Z", 2.0620000000000003},
								{"2019-08-17T00:48:00Z", 2.0225},
								{"2019-08-17T01:00:00Z", 2.057},
								{"2019-08-17T01:12:00Z", 2.1029999999999998},
								{"2019-08-17T01:24:00Z", 2.113072073},
								{"2019-08-17T01:36:00Z", 2.159},
								{"2019-08-17T01:48:00Z", 2.377},
								{"2019-08-17T02:00:00Z", 2.4655},
								{"2019-08-17T02:12:00Z", 2.5949999999999998},
								{"2019-08-17T02:24:00Z", 2.7415000000000003},
								{"2019-08-17T02:36:00Z", 2.9415},
								{"2019-08-17T02:48:00Z", 2.9745},
								{"2019-08-17T03:00:00Z", 3.151},
								{"2019-08-17T03:12:00Z", 3.325},
								{"2019-08-17T03:24:00Z", 3.483},
								{"2019-08-17T03:36:00Z", 3.6565},
								{"2019-08-17T03:48:00Z", 3.7895},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "time_fill_water_level_180w_12m",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE "location"='santa_monica' AND time >= '2015-09-18T21:30:00Z' AND time <= now() + 180w GROUP BY time(12m) fill(none) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", 2.09},
								{"2019-08-17T00:12:00Z", 2.077},
								{"2019-08-17T00:24:00Z", 2.0460000000000003},
								{"2019-08-17T00:36:00Z", 2.0620000000000003},
								{"2019-08-17T00:48:00Z", 2.0225},
								{"2019-08-17T01:00:00Z", 2.057},
								{"2019-08-17T01:12:00Z", 2.1029999999999998},
								{"2019-08-17T01:24:00Z", 2.113072073},
								{"2019-08-17T01:36:00Z", 2.159},
								{"2019-08-17T01:48:00Z", 2.377},
								{"2019-08-17T02:00:00Z", 2.4655},
								{"2019-08-17T02:12:00Z", 2.5949999999999998},
								{"2019-08-17T02:24:00Z", 2.7415000000000003},
								{"2019-08-17T02:36:00Z", 2.9415},
								{"2019-08-17T02:48:00Z", 2.9745},
								{"2019-08-17T03:00:00Z", 3.151},
								{"2019-08-17T03:12:00Z", 3.325},
								{"2019-08-17T03:24:00Z", 3.483},
								{"2019-08-17T03:36:00Z", 3.6565},
								{"2019-08-17T03:48:00Z", 3.7895},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "time_now_fill_water_level_12m",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE "location"='santa_monica' AND time >= now() GROUP BY time(12m) fill(none) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//Regular expressions
	{
		Name:  "re_l_limit_1",
		Query: fmt.Sprintf(`SELECT /l/ FROM "%s"."%s"."h2o_feet" LIMIT 1`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "level description", "location", "water_level"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", "below 3 feet", "santa_monica", 2.064},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "re_mean_degrees",
		Query: fmt.Sprintf(`SELECT MEAN("degrees") FROM /temperature/`),
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
		Name:  "re_water_level_3",
		Query: fmt.Sprintf(`SELECT MEAN(water_level) FROM "%s"."%s"."h2o_feet" WHERE "location" =~ /[m]/ AND "water_level" > 3 limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 4.471366691627881},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "re_all_location",
		Query: fmt.Sprintf(`SELECT * FROM "%s"."%s"."h2o_feet" WHERE "location" !~ /./ limit 20`, db, rp),
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
		Name:  "re_mean_water_level_location",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE "location" =~ /./ limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 4.441931402107023},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "re_mean_water_level_location_asnta_monica",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND "level description" =~ /between/ limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 4.471366691627881},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "re_first_index_l",
		Query: fmt.Sprintf(`SELECT FIRST("index") FROM "%s"."%s"."h2o_quality" GROUP BY /l/ limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_quality",
							Columns: []string{"time", "first"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", 41},
							},
						},
						{
							Name:    "h2o_quality",
							Columns: []string{"time", "first"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", 99},
							},
						},
					},
				},
			},
		},
	},
	//Data types
	{
		Name:  "data_water_level_float_4",
		Query: fmt.Sprintf(`SELECT "water_level"::float FROM "%s"."%s"."h2o_feet" LIMIT 4`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "water_level"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", 8.12},
								{"2019-08-17T00:00:00Z", 2.064},
								{"2019-08-17T00:06:00Z", 8.005},
								{"2019-08-17T00:06:00Z", 2.116},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "data_water_level_integer_4",
		Query: fmt.Sprintf(`SELECT "water_level"::integer FROM "%s"."%s"."h2o_feet" LIMIT 4`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "water_level"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", 8},
								{"2019-08-17T00:00:00Z", 2},
								{"2019-08-17T00:06:00Z", 8},
								{"2019-08-17T00:06:00Z", 2},
							},
						},
					},
				},
			},
		},
	},
	//Merge behavior
	{
		Name:  "merge_mean_water_level",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 4.441931402107023},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "merge_mean_water_level_location",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE "location" = 'coyote_creek' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 5.3591424203039155},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "merge_mean_water_level_group_by_location",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" GROUP BY "location" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 5.3591424203039155},
							},
						},
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 3.5307120942458803},
							},
						},
					},
				},
			},
		},
	},
	//Multiple statements
	{
		Name:  "mul_mean_water_level_water_level_limit_2",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet"; SELECT "water_level" FROM "%s"."%s"."h2o_feet" LIMIT 2`, db, rp, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 4.441931402107023},
							},
						},
					},
				},
				{
					StatementId: 1,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "water_level"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", 8.12},
								{"2019-08-17T00:00:00Z", 2.064},
							},
						},
					},
				},
			},
		},
	},
	//Subqueries
	{
		Name:  "sub_sum_max_water_level_group_by_location",
		Query: fmt.Sprintf(`SELECT SUM("max") FROM (SELECT MAX("water_level") FROM "%s"."%s"."h2o_feet" GROUP BY "location") limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "sum"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 17.169},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sub_max_water_level_group_by_location",
		Query: fmt.Sprintf(`SELECT MAX("water_level") FROM "%s"."%s"."h2o_feet" GROUP BY "location" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "max"},
							Values: []suite.Row{
								{"2019-08-28T07:24:00Z", 9.964},
							},
						},
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "max"},
							Values: []suite.Row{
								{"2019-08-28T03:54:00Z", 7.205},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sub_mean_difference_pet_daycare",
		Query: fmt.Sprintf(`SELECT MEAN("difference") FROM (SELECT "cats" - "dogs" AS "difference" FROM "%s"."%s"."pet_daycare") limit 20`, db, rp),
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
		Name:  "sub_all_the_means_12m_5",
		Query: fmt.Sprintf(`SELECT "all_the_means" FROM (SELECT MEAN("water_level") AS "all_the_means" FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m) ) WHERE "all_the_means" > 5 limit 20`, db, rp),
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
		Name:  "sub_all_the_means_12m",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") AS "all_the_means" FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m) limit 20`, db, rp),
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
		Name:  "sub_sum_derivative",
		Query: fmt.Sprintf(`SELECT SUM("water_level_derivative") AS "sum_derivative" FROM (SELECT DERIVATIVE(MEAN("water_level")) AS "water_level_derivative" FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m),"location") GROUP BY "location" limit 20`, db, rp),
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
		Name:  "sub_water_level_derivative",
		Query: fmt.Sprintf(`SELECT DERIVATIVE(MEAN("water_level")) AS "water_level_derivative" FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m),"location" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
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
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	{
		Name:  "into_h2o_feet_copy_1",
		Query: fmt.Sprintf(`SELECT * FROM "%s"."%s"."h2o_feet_copy_1" limit 20`, db, rp),
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
		Name:  "into_copy2_location_coyote_creek",
		Query: fmt.Sprintf(`SELECT "water_level" INTO "where_else"."autogen"."h2o_feet_copy_2" FROM "%s"."%s"."h2o_feet" WHERE "location" = 'coyote_creek' limit 50`, db, rp),
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
		Name:  "into_h2o_feet_copy_2",
		Query: fmt.Sprintf(`SELECT * FROM "where_else"."autogen"."h2o_feet_copy_2"`),
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
		Name:  "into_all_my_averages",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") INTO "all_my_averages" FROM "%s"."%s"."h2o_feet" WHERE "location" = 'coyote_creek' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m) limit 50`, db, rp),
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
		Name:  "into_select_all_my_averages",
		Query: fmt.Sprintf(`SELECT * FROM "%s"."%s"."all_my_averages"`, db, rp),
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
		Name:  "into_where_else_12m",
		Query: fmt.Sprintf(`SELECT MEAN(*) INTO "where_else"."autogen".:MEASUREMENT FROM /.*/ WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:06:00Z' GROUP BY time(12m) limit 50`),
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
		Name:  "into_where_else_autogen",
		Query: fmt.Sprintf(`SELECT * FROM "where_else"."autogen"./.*/`),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//SHOW DATABASES
	{
		Name:  "show_databasaes",
		Query: fmt.Sprintf(`SHOW DATABASES`),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "databases",
							Columns: []string{"name"},
							Values: []suite.Row{
								{"db0"},
								{"NOAA_water_database"},
							},
						},
					},
				},
			},
		},
	},
	//SHOW MEASUREMENTS
	{
		Name:  "show_measurements_on",
		Query: fmt.Sprintf(`SHOW MEASUREMENTS ON "%s"`, db),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "measurements",
							Columns: []string{"name"},
							Values: []suite.Row{
								{"average_temperature"},
								{"h2o_feet"},
								{"h2o_pH"},
								{"h2o_quality"},
								{"h2o_temperature"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "show_measurements",
		Query: fmt.Sprintf(`SHOW MEASUREMENTS`),
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
		Name:  "show_measurements_on_with_limit_2_offset_1",
		Query: fmt.Sprintf(`SHOW MEASUREMENTS ON "%s" WITH MEASUREMENT =~ /h2o.*/ LIMIT 2 `, db),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "measurements",
							Columns: []string{"name"},
							Values: []suite.Row{
								{"h2o_feet"},
								{"h2o_pH"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "show_measurements_on_with",
		Query: fmt.Sprintf(`SHOW MEASUREMENTS ON "%s" WITH MEASUREMENT =~ /h2o.*/ WHERE "randtag"  =~ /\d/`, db),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "measurements",
							Columns: []string{"name"},
							Values: []suite.Row{
								{"h2o_quality"},
							},
						},
					},
				},
			},
		},
	},
	//SHOW FIELD KEYS
	{
		Name:  "show_field_keys_on",
		Query: fmt.Sprintf(`SHOW FIELD KEYS ON "%s"`, db),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "average_temperature",
							Columns: []string{"fieldKey", "fieldType"},
							Values: []suite.Row{
								{"degrees", "float"},
							},
						},
						{
							Name:    "h2o_feet",
							Columns: []string{"fieldKey", "fieldType"},
							Values: []suite.Row{
								{"level description", "string"},
								{"water_level", "float"},
							},
						},
						{
							Name:    "h2o_pH",
							Columns: []string{"fieldKey", "fieldType"},
							Values: []suite.Row{
								{"pH", "float"},
							},
						},
						{
							Name:    "h2o_quality",
							Columns: []string{"fieldKey", "fieldType"},
							Values: []suite.Row{
								{"index", "float"},
							},
						},
						{
							Name:    "h2o_temperature",
							Columns: []string{"fieldKey", "fieldType"},
							Values: []suite.Row{
								{"degrees", "float"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "show_field_keys",
		Query: fmt.Sprintf(`SHOW FIELD KEYS`),
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
		Name:  "show_field_keys_on_from",
		Query: fmt.Sprintf(`SHOW FIELD KEYS ON "%s" FROM "%s"."%s"."h2o_feet"`, db, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"fieldKey", "fieldType"},
							Values: []suite.Row{
								{"level description", "string"},
								{"water_level", "float"},
							},
						},
					},
				},
			},
		},
	},
	//SHOW RETENTION POLICIES
	{
		Name:  "show_rp_on_noaa",
		Query: fmt.Sprintf(`SHOW RETENTION POLICIES ON "%s"`, db),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "",
							Columns: []string{"name", "duration", "groupDuration", "replicaN", "default"},
							Values: []suite.Row{
								{"autogen", "0s", "168h0m0s", 1, false},
								{"rp0", "0s", "168h0m0s", 1, true},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "show_rp",
		Query: fmt.Sprintf(`SHOW RETENTION POLICIES`),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//SHOW TAG KEYS
	{
		Name:  "show_tag_keys_on",
		Query: fmt.Sprintf(`SHOW TAG KEYS ON "%s"`, db),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "average_temperature",
							Columns: []string{"tagKey"},
							Values: []suite.Row{
								{"location"},
							},
						},
						{
							Name:    "h2o_feet",
							Columns: []string{"tagKey"},
							Values: []suite.Row{
								{"location"},
							},
						},
						{
							Name:    "h2o_pH",
							Columns: []string{"tagKey"},
							Values: []suite.Row{
								{"location"},
							},
						},
						{
							Name:    "h2o_quality",
							Columns: []string{"tagKey"},
							Values: []suite.Row{
								{"location"},
								{"randtag"},
							},
						},
						{
							Name:    "h2o_temperature",
							Columns: []string{"tagKey"},
							Values: []suite.Row{
								{"location"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "show_tag_keys",
		Query: fmt.Sprintf(`SHOW TAG KEYS`),
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
		Name:  "show_tag_keys_1_1",
		Query: fmt.Sprintf(`SHOW TAG KEYS ON "%s" FROM "h2o_quality" LIMIT 1 OFFSET 1`, db),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_quality",
							Columns: []string{"tagKey"},
							Values: []suite.Row{
								{"randtag"},
							},
						},
					},
				},
			},
		},
	},
	//SHOW SERIES
	{
		Name:  "show_series_on_noaa",
		Query: fmt.Sprintf(`SHOW SERIES ON "%s"`, db),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "",
							Columns: []string{"key"},
							Values: []suite.Row{
								{"average_temperature,location=coyote_creek"},
								{"average_temperature,location=santa_monica"},
								{"h2o_feet,location=coyote_creek"},
								{"h2o_feet,location=santa_monica"},
								{"h2o_pH,location=coyote_creek"},
								{"h2o_pH,location=santa_monica"},
								{"h2o_quality,location=coyote_creek,randtag=1"},
								{"h2o_quality,location=coyote_creek,randtag=2"},
								{"h2o_quality,location=coyote_creek,randtag=3"},
								{"h2o_quality,location=santa_monica,randtag=1"},
								{"h2o_quality,location=santa_monica,randtag=2"},
								{"h2o_quality,location=santa_monica,randtag=3"},
								{"h2o_temperature,location=coyote_creek"},
								{"h2o_temperature,location=santa_monica"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "show_series",
		Query: fmt.Sprintf(`SHOW SERIES`),
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
		Name:  "show_series_on_noaa_from",
		Query: fmt.Sprintf(`SHOW SERIES ON "%s" FROM "%s"."%s"."h2o_quality" WHERE "location" = 'coyote_creek' LIMIT 2`, db, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "",
							Columns: []string{"key"},
							Values: []suite.Row{
								{"h2o_quality,location=coyote_creek,randtag=1"},
								{"h2o_quality,location=coyote_creek,randtag=2"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "show_series_on_noaa_now_1m",
		Query: fmt.Sprintf(`SHOW SERIES ON "%s" WHERE time < now() - 1m`, db),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "",
							Columns: []string{"key"},
							Values: []suite.Row{
								{"average_temperature,location=coyote_creek"},
								{"average_temperature,location=santa_monica"},
								{"h2o_feet,location=coyote_creek"},
								{"h2o_feet,location=santa_monica"},
								{"h2o_pH,location=coyote_creek"},
								{"h2o_pH,location=santa_monica"},
								{"h2o_quality,location=coyote_creek,randtag=1"},
								{"h2o_quality,location=coyote_creek,randtag=2"},
								{"h2o_quality,location=coyote_creek,randtag=3"},
								{"h2o_quality,location=santa_monica,randtag=1"},
								{"h2o_quality,location=santa_monica,randtag=2"},
								{"h2o_quality,location=santa_monica,randtag=3"},
								{"h2o_temperature,location=coyote_creek"},
								{"h2o_temperature,location=santa_monica"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "show_series_now_28d",
		Query: fmt.Sprintf(`SHOW SERIES ON "%s" WHERE time < now() - 28d`, db),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "",
							Columns: []string{"key"},
							Values: []suite.Row{
								{"average_temperature,location=coyote_creek"},
								{"average_temperature,location=santa_monica"},
								{"h2o_feet,location=coyote_creek"},
								{"h2o_feet,location=santa_monica"},
								{"h2o_pH,location=coyote_creek"},
								{"h2o_pH,location=santa_monica"},
								{"h2o_quality,location=coyote_creek,randtag=1"},
								{"h2o_quality,location=coyote_creek,randtag=2"},
								{"h2o_quality,location=coyote_creek,randtag=3"},
								{"h2o_quality,location=santa_monica,randtag=1"},
								{"h2o_quality,location=santa_monica,randtag=2"},
								{"h2o_quality,location=santa_monica,randtag=3"},
								{"h2o_temperature,location=coyote_creek"},
								{"h2o_temperature,location=santa_monica"},
							},
						},
					},
				},
			},
		},
	},
	//SHOW TAG VALUES
	{
		Name:  "show_tag_values_on_randtag",
		Query: fmt.Sprintf(`SHOW TAG VALUES ON "%s" WITH KEY = "randtag"`, db),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_quality",
							Columns: []string{"key", "value"},
							Values: []suite.Row{
								{"randtag", "1"},
								{"randtag", "2"},
								{"randtag", "3"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "show_tag_values_randtag",
		Query: fmt.Sprintf(`SHOW TAG VALUES WITH KEY = "randtag"`),
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
		Name:  "show_tag_values_on_location_randtag",
		Query: fmt.Sprintf(`SHOW TAG VALUES ON "%s" WITH KEY IN ("location","randtag") WHERE "randtag" =~ /./ LIMIT 3`, db),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_quality",
							Columns: []string{"key", "value"},
							Values: []suite.Row{
								{"location", "coyote_creek"},
								{"location", "santa_monica"},
								{"randtag", "1"},
							},
						},
					},
				},
			},
		},
	},
	//SHOW SHARDS
	{
		Name:  "show_shards",
		Query: fmt.Sprintf(`SHOW SHARDS`),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "db0",
							Columns: []string{"id", "database", "rp", "shard_group", "start_time", "end_time", "expiry_time", "owners"},
							Values:  []suite.Row{},
						},
						{
							Name:    "NOAA_water_database",
							Columns: []string{"id", "database", "rp", "shard_group", "start_time", "end_time", "expiry_time", "owners"},
							Values: []suite.Row{
								{2, "NOAA_water_database", "rp0", 1, "2019-08-12T00:00:00Z", "2019-08-19T00:00:00Z", "2019-08-19T00:00:00Z", "0"},
								{4, "NOAA_water_database", "rp0", 2, "2019-08-19T00:00:00Z", "2019-08-26T00:00:00Z", "2019-08-26T00:00:00Z", "0"},
								{6, "NOAA_water_database", "rp0", 3, "2019-08-26T00:00:00Z", "2019-09-02T00:00:00Z", "2019-09-02T00:00:00Z", "0"},
								{8, "NOAA_water_database", "rp0", 4, "2019-09-02T00:00:00Z", "2019-09-09T00:00:00Z", "2019-09-09T00:00:00Z", "0"},
								{10, "NOAA_water_database", "rp0", 5, "2019-09-09T00:00:00Z", "2019-09-16T00:00:00Z", "2019-09-16T00:00:00Z", "0"},
								{12, "NOAA_water_database", "rp0", 6, "2019-09-16T00:00:00Z", "2019-09-23T00:00:00Z", "2019-09-23T00:00:00Z", "0"},
							},
						},
					},
				},
			},
		},
	},
	//COUNT()
	{
		Name:  "agg_count_water_level",
		Query: fmt.Sprintf(`SELECT COUNT("water_level") FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "count"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 15258},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "agg_count_all",
		Query: fmt.Sprintf(`SELECT COUNT(*) FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "count_level description", "count_water_level"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 15258, 15258},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "agg_count_re",
		Query: fmt.Sprintf(`SELECT COUNT(/water/) FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "count_water_level"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 15258},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "agg_count_clauses",
		Query: fmt.Sprintf(`SELECT COUNT("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-17T23:48:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(12m),* fill(200) LIMIT 7 SLIMIT 1`, db, rp),
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
		Name:  "agg_count_distinct",
		Query: fmt.Sprintf(`SELECT COUNT(DISTINCT("level description")) FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "count"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 4},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "agg_count_12m",
		Query: fmt.Sprintf(`SELECT COUNT("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-09-18T21:24:00Z' AND time <= '2015-09-18T21:54:00Z' GROUP BY time(12m) limit 20`, db, rp),
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
		Name:  "agg_count_12m_800000",
		Query: fmt.Sprintf(`SELECT COUNT("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-09-18T21:24:00Z' AND time <= '2015-09-18T21:54:00Z' GROUP BY time(12m) fill(800000) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//DISTINCT()
	{
		Name:  "agg_distinct_level_description",
		Query: fmt.Sprintf(`SELECT DISTINCT("level description") FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "distinct"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", "below 3 feet"},
								{"1970-01-01T00:00:00Z", "between 6 and 9 feet"},
								{"1970-01-01T00:00:00Z", "between 3 and 6 feet"},
								{"1970-01-01T00:00:00Z", "at or greater than 9 feet"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "agg_distinct_all",
		Query: fmt.Sprintf(`SELECT DISTINCT(*) FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
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
		Name:  "agg_distinct_clauses",
		Query: fmt.Sprintf(`SELECT DISTINCT("level description") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-17T23:48:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(12m),* limit 20 SLIMIT 1`, db, rp),
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
		Name:  "agg_distinct_into",
		Query: fmt.Sprintf(`SELECT DISTINCT("level description") INTO "distincts" FROM "%s"."%s"."h2o_feet"`, db, rp),
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
		Name:  "agg_distinct_select_into",
		Query: fmt.Sprintf(`SELECT * FROM "%s"."%s"."distincts"`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//INTEGRAL()
	{
		Name:  "agg_integral_location_time_1",
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
		Name:  "agg_integral_location_time_2",
		Query: fmt.Sprintf(`SELECT INTEGRAL("water_level") FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' limit 20`, db, rp),
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
		Name:  "agg_integral_water_level_1m",
		Query: fmt.Sprintf(`SELECT INTEGRAL("water_level",1m) FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' limit 20`, db, rp),
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
		Name:  "agg_integral_all_1m",
		Query: fmt.Sprintf(`SELECT INTEGRAL(*,1m) FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' limit 20`, db, rp),
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
		Name:  "agg_integral_re",
		Query: fmt.Sprintf(`SELECT INTEGRAL(/water/,1m) FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' limit 20`, db, rp),
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
		Name:  "agg_integral_clauses",
		Query: fmt.Sprintf(`SELECT INTEGRAL("water_level",1m) FROM "%s"."%s"."h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m) LIMIT 1`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//MEAN()
	{
		Name:  "agg_mean_water_level",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 4.441931402107023},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "agg_mean_all",
		Query: fmt.Sprintf(`SELECT MEAN(*) FROM "%s"."%s"."h2o_feet"`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "mean_water_level"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 4.441931402107023},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "agg_mean_re",
		Query: fmt.Sprintf(`SELECT MEAN(/water/) FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "mean_water_level"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 4.441931402107023},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "agg_mean_clauses",
		Query: fmt.Sprintf(`SELECT MEAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-17T23:48:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(12m),* fill(9.01) LIMIT 7 SLIMIT 1`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//MEDIAN()
	{
		Name:  "agg_median_key",
		Query: fmt.Sprintf(`SELECT MEDIAN("water_level") FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "median"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 4.124},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "agg_median_all",
		Query: fmt.Sprintf(`SELECT MEDIAN(*) FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "median_water_level"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 4.124},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "agg_median_re",
		Query: fmt.Sprintf(`SELECT MEDIAN(/water/) FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "median_water_level"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 4.124},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "agg_median_clauses",
		Query: fmt.Sprintf(`SELECT MEDIAN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-17T23:48:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(12m),* fill(700) LIMIT 7 SLIMIT 1 SOFFSET 1`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//MODE()
	{
		Name:  "agg_mode_key",
		Query: fmt.Sprintf(`SELECT MODE("level description") FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "mode"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", "between 3 and 6 feet"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "agg_mode_all",
		Query: fmt.Sprintf(`SELECT MODE(*) FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "mode_level description", "mode_water_level"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", "between 3 and 6 feet", 2.69},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "agg_mode_re",
		Query: fmt.Sprintf(`SELECT MODE(/water/) FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "mode_water_level"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 2.69},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "agg_mode_clauses",
		Query: fmt.Sprintf(`SELECT MODE("level description") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-17T23:48:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(12m),* LIMIT 3 SLIMIT 1 SOFFSET 1`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//SPREAD()
	{
		Name:  "agg_spread_key",
		Query: fmt.Sprintf(`SELECT SPREAD("water_level") FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "spread"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 10.574},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "agg_spread_all",
		Query: fmt.Sprintf(` SELECT SPREAD(*) FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "spread_water_level"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 10.574},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "agg_apread_re",
		Query: fmt.Sprintf(`SELECT SPREAD(/water/) FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "spread_water_level"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 10.574},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "agg_spread_clauses",
		Query: fmt.Sprintf(`SELECT SPREAD("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-17T23:48:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(12m),* fill(18) LIMIT 3 SLIMIT 1 SOFFSET 1`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//STDDEV()
	{
		Name:  "agg_stddev_key",
		Query: fmt.Sprintf(`SELECT STDDEV("water_level") FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "stddev"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 2.2791252793623333},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "agg_stddev_all",
		Query: fmt.Sprintf(`SELECT STDDEV(*) FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "stddev_water_level"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 2.2791252793623333},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "agg_stddev_re",
		Query: fmt.Sprintf(`SELECT STDDEV(/water/) FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "stddev_water_level"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 2.2791252793623333},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "agg_stddev_clause",
		Query: fmt.Sprintf(`SELECT STDDEV("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-17T23:48:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(12m),* fill(18000) LIMIT 2 SLIMIT 1 SOFFSET 1`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//SUM()
	{
		Name:  "agg_sum_key",
		Query: fmt.Sprintf(`SELECT SUM("water_level") FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "sum"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 67774.98933334895},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "agg_sum_all",
		Query: fmt.Sprintf(`SELECT SUM(*) FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "sum_water_level"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 67774.98933334895},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "agg_sum_re",
		Query: fmt.Sprintf(`SELECT SUM(/water/) FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "sum_water_level"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 67774.98933334895},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "agg_sum_clauses",
		Query: fmt.Sprintf(`SELECT SUM("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-17T23:48:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(12m),* fill(18000) LIMIT 4 SLIMIT 1`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//BOTTOM()
	{
		Name:  "sel_bottom_key",
		Query: fmt.Sprintf(`SELECT BOTTOM("water_level",3) FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "bottom"},
							Values: []suite.Row{
								{"2019-09-02T00:42:00Z", 1.778},
								{"2019-09-02T00:48:00Z", 1.798},
								{"2019-09-02T00:54:00Z", 1.752},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_bottom_key_2_tag",
		Query: fmt.Sprintf(`SELECT BOTTOM("water_level","location",2) FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "bottom", "location"},
							Values: []suite.Row{
								{"2019-08-28T10:36:00Z", -0.243, "santa_monica"},
								{"2019-08-28T14:30:00Z", -0.61, "coyote_creek"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_bottom_water_level_4",
		Query: fmt.Sprintf(`SELECT BOTTOM("water_level",4),"location","level description" FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "bottom", "location", "level description"},
							Values: []suite.Row{
								{"2019-09-02T00:42:00Z", 1.778, "santa_monica", "below 3 feet"},
								{"2019-09-02T00:48:00Z", 1.798, "santa_monica", "below 3 feet"},
								{"2019-09-02T00:54:00Z", 1.752, "santa_monica", "below 3 feet"},
								{"2019-09-02T01:00:00Z", 1.755, "santa_monica", "below 3 feet"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_bottom_clauses",
		Query: fmt.Sprintf(`SELECT BOTTOM("water_level",3),"location" FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(24m) ORDER BY time DESC limit 20`, db, rp),
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
		Name:  "sel_bottom_water_level_2_18m",
		Query: fmt.Sprintf(`SELECT BOTTOM("water_level",2) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(18m) limit 20`, db, rp),
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
		Name:  "sel_bottom_water_level_location_3",
		Query: fmt.Sprintf(`SELECT BOTTOM("water_level","location",3) FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "bottom", "location"},
							Values: []suite.Row{
								{"2019-08-28T10:36:00Z", -0.243, "santa_monica"},
								{"2019-08-28T14:30:00Z", -0.61, "coyote_creek"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_bottom_into_bottom_water_levels",
		Query: fmt.Sprintf(`SELECT BOTTOM("water_level","location",2) INTO "bottom_water_levels" FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
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
		Name:  "sel_bottom_select_bottom_water_levels",
		Query: fmt.Sprintf(`SHOW TAG KEYS FROM "%s"."%s"."bottom_water_levels"`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//FIRST()
	{
		Name:  "sel_first_key",
		Query: fmt.Sprintf(`SELECT FIRST("level description") FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "first"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", "between 6 and 9 feet"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_first_all",
		Query: fmt.Sprintf(`SELECT FIRST(*) FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "first_level description", "first_water_level"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", "between 6 and 9 feet", 8.12},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_first_re",
		Query: fmt.Sprintf(`SELECT FIRST(/level/) FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "first_level description", "first_water_level"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", "between 6 and 9 feet", 8.12},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_first_keys_tags",
		Query: fmt.Sprintf(`SELECT FIRST("level description"),"location","water_level" FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "first", "location", "water_level"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", "between 6 and 9 feet", "coyote_creek", 8.12},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_first_clauses",
		Query: fmt.Sprintf(`SELECT FIRST("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-17T23:48:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(12m),* fill(9.01) LIMIT 4 SLIMIT 1`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//LAST()
	{
		Name:  "sel_last_key",
		Query: fmt.Sprintf(`SELECT LAST("level description") FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "last"},
							Values: []suite.Row{
								{"2019-09-17T21:42:00Z", "between 3 and 6 feet"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_last_all",
		Query: fmt.Sprintf(`SELECT LAST(*) FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "last_level description", "last_water_level"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", "between 3 and 6 feet", 4.938},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_last_re",
		Query: fmt.Sprintf(`SELECT LAST(/level/) FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "last_level description", "last_water_level"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", "between 3 and 6 feet", 4.938},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_last_keys_tags",
		Query: fmt.Sprintf(`SELECT LAST("level description"),"location","water_level" FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "last", "location", "water_level"},
							Values: []suite.Row{
								{"2019-09-17T21:42:00Z", "between 3 and 6 feet", "santa_monica", 4.938},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_last_clauses",
		Query: fmt.Sprintf(`SELECT LAST("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-17T23:48:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(12m),* fill(9.01) LIMIT 4 SLIMIT 1`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//MAX()
	{
		Name:  "sel_max_key",
		Query: fmt.Sprintf(`SELECT MAX("water_level") FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "max"},
							Values: []suite.Row{
								{"2019-08-28T07:24:00Z", 9.964},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_max_all",
		Query: fmt.Sprintf(`SELECT MAX(*) FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "max_water_level"},
							Values: []suite.Row{
								{"2019-08-28T07:24:00Z", 9.964},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_max_re",
		Query: fmt.Sprintf(`SELECT MAX(/level/) FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "max_water_level"},
							Values: []suite.Row{
								{"2019-08-28T07:24:00Z", 9.964},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_max_keys_tags",
		Query: fmt.Sprintf(`SELECT MAX("water_level"),"location","level description" FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "max", "location", "level description"},
							Values: []suite.Row{
								{"2019-08-28T07:24:00Z", 9.964, "coyote_creek", "at or greater than 9 feet"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_max_clauses",
		Query: fmt.Sprintf(`SELECT MAX("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-17T23:48:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(12m),* fill(9.01) LIMIT 4 SLIMIT 1`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//MIN()
	{
		Name:  "sel_min_key",
		Query: fmt.Sprintf(`SELECT MIN("water_level") FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "min"},
							Values: []suite.Row{
								{"2019-08-28T14:30:00Z", -0.61},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_min_all",
		Query: fmt.Sprintf(`SELECT MIN(*) FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "min_water_level"},
							Values: []suite.Row{
								{"2019-08-28T14:30:00Z", -0.61},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_min_re",
		Query: fmt.Sprintf(`SELECT MIN(/level/) FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "min_water_level"},
							Values: []suite.Row{
								{"2019-08-28T14:30:00Z", -0.61},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_min_keys_tags",
		Query: fmt.Sprintf(`SELECT MIN("water_level"),"location","level description" FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "min", "location", "level description"},
							Values: []suite.Row{
								{"2019-08-28T14:30:00Z", -0.61, "coyote_creek", "below 3 feet"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_min_clauses",
		Query: fmt.Sprintf(`SELECT MIN("water_level") FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-17T23:48:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(12m),* fill(9.01) LIMIT 4 SLIMIT 1`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//PERCENTILE()
	{
		Name:  "sel_percentile_key",
		Query: fmt.Sprintf(`SELECT PERCENTILE("water_level",5) FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "percentile"},
							Values: []suite.Row{
								{"2019-09-01T17:54:00Z", 1.122},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_percentile_all",
		Query: fmt.Sprintf(`SELECT PERCENTILE(*,5) FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "percentile_water_level"},
							Values: []suite.Row{
								{"2019-09-01T17:54:00Z", 1.122},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_percentile_re",
		Query: fmt.Sprintf(`SELECT PERCENTILE(/level/,5) FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "percentile_water_level"},
							Values: []suite.Row{
								{"2019-09-01T17:54:00Z", 1.122},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_percentile_keys_tags",
		Query: fmt.Sprintf(`SELECT PERCENTILE("water_level",5),"location","level description" FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "percentile", "location", "level description"},
							Values: []suite.Row{
								{"2019-08-30T03:42:00Z", 1.122, "coyote_creek", "below 3 feet"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_percentile_clauses",
		Query: fmt.Sprintf(`SELECT PERCENTILE("water_level",20) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-17T23:48:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(24m) fill(15) LIMIT 2`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//SAMPLE()
	{
		Name:  "sel_sample_key",
		Query: fmt.Sprintf(`ELECT SAMPLE("water_level",2) FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "sel_sample_all",
		Query: fmt.Sprintf(`SELECT SAMPLE(*,2) FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "sample_level description", "sample_water_level"},
							Values: []suite.Row{
								{"2019-08-19T20:24:00Z", nil, 4.977},
								{"2019-08-23T06:24:00Z", nil, 1.414},
								{"2019-09-01T17:00:00Z", "between 3 and 6 feet", nil},
								{"2019-09-03T23:54:00Z", "between 3 and 6 feet", nil},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_sample_re",
		Query: fmt.Sprintf(`SELECT SAMPLE(/level/,2) FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "sample_level description", "sample_water_level"},
							Values: []suite.Row{
								{"2019-08-18T07:12:00Z", "between 3 and 6 feet", nil},
								{"2019-09-07T22:00:00Z", nil, 3.471},
								{"2019-09-12T06:18:00Z", nil, 5.512},
								{"2019-09-13T14:12:00Z", "between 3 and 6 feet", nil},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_sample_keys_tags",
		Query: fmt.Sprintf(`SELECT SAMPLE("water_level",2),"location","level description" FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "sample", "location", "level description"},
							Values: []suite.Row{
								{"2019-08-23T17:54:00Z", 6.335, "coyote_creek", "between 6 and 9 feet"},
								{"2019-08-25T18:48:00Z", 2.894, "santa_monica", "below 3 feet"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_sample_clauses",
		Query: fmt.Sprintf(`SELECT SAMPLE("water_level",1) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(18m) limit 20`, db, rp),
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
		Name:  "sel_sample_clauses_2",
		Query: fmt.Sprintf(`SELECT SAMPLE("water_level",2) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(18m) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//TOP()
	{
		Name:  "sel_top_key",
		Query: fmt.Sprintf(`SELECT TOP("water_level",3) FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "top"},
							Values: []suite.Row{
								{"2019-09-02T00:00:00Z", 9.777},
								{"2019-09-02T00:06:00Z", 9.728},
								{"2019-09-02T00:12:00Z", 9.649},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_top_keys_tags",
		Query: fmt.Sprintf(`SELECT TOP("water_level","location",2) FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "top", "location"},
							Values: []suite.Row{
								{"2019-08-28T03:54:00Z", 7.205, "santa_monica"},
								{"2019-08-28T07:24:00Z", 9.964, "coyote_creek"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_top_keys_tags_fields",
		Query: fmt.Sprintf(`SELECT TOP("water_level",4),"location","level description" FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "top", "location", "level description"},
							Values: []suite.Row{
								{"2019-09-02T00:00:00Z", 9.777, "coyote_creek", "at or greater than 9 feet"},
								{"2019-09-02T00:06:00Z", 9.728, "coyote_creek", "at or greater than 9 feet"},
								{"2019-09-02T00:12:00Z", 9.649, "coyote_creek", "at or greater than 9 feet"},
								{"2019-09-02T00:18:00Z", 9.57, "coyote_creek", "at or greater than 9 feet"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_top_clauses",
		Query: fmt.Sprintf(`SELECT TOP("water_level",3),"location" FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(24m) ORDER BY time DESC limit 20`, db, rp),
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
		Name:  "sel_top_clauses_18m",
		Query: fmt.Sprintf(`SELECT TOP("water_level",2) FROM "%s"."%s"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'santa_monica' GROUP BY time(18m) limit 20`, db, rp),
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
		Name:  "sel_top_water_level_location_3",
		Query: fmt.Sprintf(`SELECT TOP("water_level","location",3) FROM "%s"."%s"."h2o_feet" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "h2o_feet",
							Columns: []string{"time", "top", "location"},
							Values: []suite.Row{
								{"2019-08-28T03:54:00Z", 7.205, "santa_monica"},
								{"2019-08-28T07:24:00Z", 9.964, "coyote_creek"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_top_into",
		Query: fmt.Sprintf(`SELECT TOP("water_level","location",2) INTO "top_water_levels" FROM "%s"."%s"."h2o_feet"`, db, rp),
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
		Name:  "sel_top_show_top_water_levels",
		Query: fmt.Sprintf(`SHOW TAG KEYS FROM "%s"."%s"."top_water_levels"`, db, rp),
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
