package traveling_info

import (
	"fmt"
	"github.com/cnosdb/cnosdb/tests/suite"
)

var cases = []suite.Step{
	//SELECT
	{
		Name:  "human_traffic_2_4",
		Query: fmt.Sprintf(`SELECT ("human_traffic" * 2) + 4 FROM "%s"."%s".information LIMIT 10 OFFSET 2000`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "human_traffic"},
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
		Name:  "human_traffic_count",
		Query: fmt.Sprintf(`SELECT COUNT("human_traffic") FROM "%s"."%s".information`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
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
								{"avg_temperature"},
								{"information"},
								{"recommended"},
								{"air_quality"},
								{"max_temperature"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "information_recommended",
		Query: fmt.Sprintf(`SELECT * FROM "%s"."%s"."information","%s"."%s"."recommended" LIMIT 10 OFFSET 1000`, db, rp, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "description", "location", "rec_level", "human_traffic"},
							Values: []suite.Row{
								{"2019-08-19T02:00:00Z", "less traffic", "shanghai", nil, 2.211},
								{"2019-08-19T02:00:00Z", "high traffic", "beijing", nil, 6.768},
								{"2019-08-19T02:06:00Z", "less traffic", "shanghai", nil, 2.188},
								{"2019-08-19T02:06:00Z", "high traffic", "beijing", nil, 6.631},
								{"2019-08-19T02:12:00Z", "less traffic", "shanghai", nil, 2.306},
								{"2019-08-19T02:12:00Z", "high traffic", "beijing", nil, 6.49},
								{"2019-08-19T02:18:00Z", "less traffic", "shanghai", nil, 2.323},
								{"2019-08-19T02:18:00Z", "high traffic", "beijing", nil, 6.358},
								{"2019-08-19T02:24:00Z", "less traffic", "shanghai", nil, 2.297},
								{"2019-08-19T02:24:00Z", "high traffic", "beijing", nil, 6.207},
							},
						},
						{
							Name:    "recommended",
							Columns: []string{"time", "description", "location", "rec_level", "human_traffic"},
							Values: []suite.Row{
								{"2019-08-19T02:00:00Z", nil, "beijing", 7, nil},
								{"2019-08-19T02:00:00Z", nil, "shanghai", 8, nil},
								{"2019-08-19T02:06:00Z", nil, "beijing", 7, nil},
								{"2019-08-19T02:06:00Z", nil, "shanghai", 8, nil},
								{"2019-08-19T02:12:00Z", nil, "beijing", 8, nil},
								{"2019-08-19T02:12:00Z", nil, "shanghai", 8, nil},
								{"2019-08-19T02:18:00Z", nil, "beijing", 7, nil},
								{"2019-08-19T02:18:00Z", nil, "shanghai", 7, nil},
								{"2019-08-19T02:24:00Z", nil, "beijing", 8, nil},
								{"2019-08-19T02:24:00Z", nil, "shanghai", 8, nil},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "information_all_fields",
		Query: fmt.Sprintf(`SELECT *::field FROM "%s"."%s".information LIMIT 10 OFFSET 3000`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "description", "human_traffic"},
							Values: []suite.Row{
								{"2019-08-23T06:00:00Z", "less traffic", 1.594},
								{"2019-08-23T06:00:00Z", "general traffic", 5.564},
								{"2019-08-23T06:06:00Z", "less traffic", 1.545},
								{"2019-08-23T06:06:00Z", "general traffic", 5.43},
								{"2019-08-23T06:12:00Z", "less traffic", 1.526},
								{"2019-08-23T06:12:00Z", "general traffic", 5.308},
								{"2019-08-23T06:18:00Z", "less traffic", 1.457},
								{"2019-08-23T06:18:00Z", "general traffic", 5.18},
								{"2019-08-23T06:24:00Z", "less traffic", 1.414},
								{"2019-08-23T06:24:00Z", "general traffic", 5.052},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "information_specific_tags_fields",
		Query: fmt.Sprintf(`SELECT "description"::field,"location"::tag,"human_traffic"::field FROM "%s"."%s".information LIMIT 10 OFFSET 5000`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "description", "location", "human_traffic"},
							Values: []suite.Row{
								{"2019-08-27T10:00:00Z", "less traffic", "shanghai", -0.062},
								{"2019-08-27T10:00:00Z", "general traffic", "beijing", 5.525},
								{"2019-08-27T10:06:00Z", "less traffic", "shanghai", -0.092},
								{"2019-08-27T10:06:00Z", "general traffic", "beijing", 5.354},
								{"2019-08-27T10:12:00Z", "less traffic", "shanghai", -0.105},
								{"2019-08-27T10:12:00Z", "general traffic", "beijing", 5.187},
								{"2019-08-27T10:18:00Z", "less traffic", "shanghai", -0.089},
								{"2019-08-27T10:18:00Z", "general traffic", "beijing", 5.02},
								{"2019-08-27T10:24:00Z", "less traffic", "shanghai", -0.112},
								{"2019-08-27T10:24:00Z", "general traffic", "beijing", 4.849},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "information_50",
		Query: fmt.Sprintf(`SELECT "description","location","human_traffic" FROM "%s"."%s".information limit 50`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "description", "location", "human_traffic"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", "less traffic", "shanghai", 2.064},
								{"2019-08-17T00:00:00Z", "high traffic", "beijing", 8.12},
								{"2019-08-17T00:06:00Z", "less traffic", "shanghai", 2.116},
								{"2019-08-17T00:06:00Z", "high traffic", "beijing", 8.005},
								{"2019-08-17T00:12:00Z", "less traffic", "shanghai", 2.028},
								{"2019-08-17T00:12:00Z", "high traffic", "beijing", 7.887},
								{"2019-08-17T00:18:00Z", "less traffic", "shanghai", 2.126},
								{"2019-08-17T00:18:00Z", "high traffic", "beijing", 7.762},
								{"2019-08-17T00:24:00Z", "less traffic", "shanghai", 2.041},
								{"2019-08-17T00:24:00Z", "high traffic", "beijing", 7.635},
								{"2019-08-17T00:30:00Z", "less traffic", "shanghai", 2.051},
								{"2019-08-17T00:30:00Z", "high traffic", "beijing", 7.5},
								{"2019-08-17T00:36:00Z", "less traffic", "shanghai", 2.067},
								{"2019-08-17T00:36:00Z", "high traffic", "beijing", 7.372},
								{"2019-08-17T00:42:00Z", "less traffic", "shanghai", 2.057},
								{"2019-08-17T00:42:00Z", "high traffic", "beijing", 7.234},
								{"2019-08-17T00:48:00Z", "less traffic", "shanghai", 1.991},
								{"2019-08-17T00:48:00Z", "high traffic", "beijing", 7.11},
								{"2019-08-17T00:54:00Z", "less traffic", "shanghai", 2.054},
								{"2019-08-17T00:54:00Z", "high traffic", "beijing", 6.982},
								{"2019-08-17T01:00:00Z", "less traffic", "shanghai", 2.018},
								{"2019-08-17T01:00:00Z", "high traffic", "beijing", 6.837},
								{"2019-08-17T01:06:00Z", "less traffic", "shanghai", 2.096},
								{"2019-08-17T01:06:00Z", "high traffic", "beijing", 6.713},
								{"2019-08-17T01:12:00Z", "less traffic", "shanghai", 2.1},
								{"2019-08-17T01:12:00Z", "high traffic", "beijing", 6.578},
								{"2019-08-17T01:18:00Z", "less traffic", "shanghai", 2.106},
								{"2019-08-17T01:18:00Z", "high traffic", "beijing", 6.44},
								{"2019-08-17T01:24:00Z", "less traffic", "shanghai", 2.126144146},
								{"2019-08-17T01:24:00Z", "high traffic", "beijing", 6.299},
								{"2019-08-17T01:30:00Z", "less traffic", "shanghai", 2.1},
								{"2019-08-17T01:30:00Z", "high traffic", "beijing", 6.168},
								{"2019-08-17T01:36:00Z", "less traffic", "shanghai", 2.136},
								{"2019-08-17T01:36:00Z", "high traffic", "beijing", 6.024},
								{"2019-08-17T01:42:00Z", "less traffic", "shanghai", 2.182},
								{"2019-08-17T01:42:00Z", "general traffic", "beijing", 5.879},
								{"2019-08-17T01:48:00Z", "less traffic", "shanghai", 2.306},
								{"2019-08-17T01:48:00Z", "general traffic", "beijing", 5.745},
								{"2019-08-17T01:54:00Z", "less traffic", "shanghai", 2.448},
								{"2019-08-17T01:54:00Z", "general traffic", "beijing", 5.617},
								{"2019-08-17T02:00:00Z", "less traffic", "shanghai", 2.464},
								{"2019-08-17T02:00:00Z", "general traffic", "beijing", 5.472},
								{"2019-08-17T02:06:00Z", "less traffic", "shanghai", 2.467},
								{"2019-08-17T02:06:00Z", "general traffic", "beijing", 5.348},
								{"2019-08-17T02:12:00Z", "less traffic", "shanghai", 2.516},
								{"2019-08-17T02:12:00Z", "general traffic", "beijing", 5.2},
								{"2019-08-17T02:18:00Z", "less traffic", "shanghai", 2.674},
								{"2019-08-17T02:18:00Z", "general traffic", "beijing", 5.072},
								{"2019-08-17T02:24:00Z", "less traffic", "shanghai", 2.684},
								{"2019-08-17T02:24:00Z", "general traffic", "beijing", 4.934},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "information_200",
		Query: fmt.Sprintf(`SELECT * FROM "%s"."%s"."information" limit 200`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "description", "location", "human_traffic"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", "less traffic", "shanghai", 2.064},
								{"2019-08-17T00:00:00Z", "high traffic", "beijing", 8.12},
								{"2019-08-17T00:06:00Z", "less traffic", "shanghai", 2.116},
								{"2019-08-17T00:06:00Z", "high traffic", "beijing", 8.005},
								{"2019-08-17T00:12:00Z", "less traffic", "shanghai", 2.028},
								{"2019-08-17T00:12:00Z", "high traffic", "beijing", 7.887},
								{"2019-08-17T00:18:00Z", "less traffic", "shanghai", 2.126},
								{"2019-08-17T00:18:00Z", "high traffic", "beijing", 7.762},
								{"2019-08-17T00:24:00Z", "less traffic", "shanghai", 2.041},
								{"2019-08-17T00:24:00Z", "high traffic", "beijing", 7.635},
								{"2019-08-17T00:30:00Z", "less traffic", "shanghai", 2.051},
								{"2019-08-17T00:30:00Z", "high traffic", "beijing", 7.5},
								{"2019-08-17T00:36:00Z", "less traffic", "shanghai", 2.067},
								{"2019-08-17T00:36:00Z", "high traffic", "beijing", 7.372},
								{"2019-08-17T00:42:00Z", "less traffic", "shanghai", 2.057},
								{"2019-08-17T00:42:00Z", "high traffic", "beijing", 7.234},
								{"2019-08-17T00:48:00Z", "less traffic", "shanghai", 1.991},
								{"2019-08-17T00:48:00Z", "high traffic", "beijing", 7.11},
								{"2019-08-17T00:54:00Z", "less traffic", "shanghai", 2.054},
								{"2019-08-17T00:54:00Z", "high traffic", "beijing", 6.982},
								{"2019-08-17T01:00:00Z", "less traffic", "shanghai", 2.018},
								{"2019-08-17T01:00:00Z", "high traffic", "beijing", 6.837},
								{"2019-08-17T01:06:00Z", "less traffic", "shanghai", 2.096},
								{"2019-08-17T01:06:00Z", "high traffic", "beijing", 6.713},
								{"2019-08-17T01:12:00Z", "less traffic", "shanghai", 2.1},
								{"2019-08-17T01:12:00Z", "high traffic", "beijing", 6.578},
								{"2019-08-17T01:18:00Z", "less traffic", "shanghai", 2.106},
								{"2019-08-17T01:18:00Z", "high traffic", "beijing", 6.44},
								{"2019-08-17T01:24:00Z", "less traffic", "shanghai", 2.126144146},
								{"2019-08-17T01:24:00Z", "high traffic", "beijing", 6.299},
								{"2019-08-17T01:30:00Z", "less traffic", "shanghai", 2.1},
								{"2019-08-17T01:30:00Z", "high traffic", "beijing", 6.168},
								{"2019-08-17T01:36:00Z", "less traffic", "shanghai", 2.136},
								{"2019-08-17T01:36:00Z", "high traffic", "beijing", 6.024},
								{"2019-08-17T01:42:00Z", "less traffic", "shanghai", 2.182},
								{"2019-08-17T01:42:00Z", "general traffic", "beijing", 5.879},
								{"2019-08-17T01:48:00Z", "less traffic", "shanghai", 2.306},
								{"2019-08-17T01:48:00Z", "general traffic", "beijing", 5.745},
								{"2019-08-17T01:54:00Z", "less traffic", "shanghai", 2.448},
								{"2019-08-17T01:54:00Z", "general traffic", "beijing", 5.617},
								{"2019-08-17T02:00:00Z", "less traffic", "shanghai", 2.464},
								{"2019-08-17T02:00:00Z", "general traffic", "beijing", 5.472},
								{"2019-08-17T02:06:00Z", "less traffic", "shanghai", 2.467},
								{"2019-08-17T02:06:00Z", "general traffic", "beijing", 5.348},
								{"2019-08-17T02:12:00Z", "less traffic", "shanghai", 2.516},
								{"2019-08-17T02:12:00Z", "general traffic", "beijing", 5.2},
								{"2019-08-17T02:18:00Z", "less traffic", "shanghai", 2.674},
								{"2019-08-17T02:18:00Z", "general traffic", "beijing", 5.072},
								{"2019-08-17T02:24:00Z", "less traffic", "shanghai", 2.684},
								{"2019-08-17T02:24:00Z", "general traffic", "beijing", 4.934},
								{"2019-08-17T02:30:00Z", "less traffic", "shanghai", 2.799},
								{"2019-08-17T02:30:00Z", "general traffic", "beijing", 4.793},
								{"2019-08-17T02:36:00Z", "less traffic", "shanghai", 2.927},
								{"2019-08-17T02:36:00Z", "general traffic", "beijing", 4.662},
								{"2019-08-17T02:42:00Z", "less traffic", "shanghai", 2.956},
								{"2019-08-17T02:42:00Z", "general traffic", "beijing", 4.534},
								{"2019-08-17T02:48:00Z", "less traffic", "shanghai", 2.927},
								{"2019-08-17T02:48:00Z", "general traffic", "beijing", 4.403},
								{"2019-08-17T02:54:00Z", "general traffic", "beijing", 4.281},
								{"2019-08-17T02:54:00Z", "general traffic", "shanghai", 3.022},
								{"2019-08-17T03:00:00Z", "general traffic", "beijing", 4.154},
								{"2019-08-17T03:00:00Z", "general traffic", "shanghai", 3.087},
								{"2019-08-17T03:06:00Z", "general traffic", "beijing", 4.029},
								{"2019-08-17T03:06:00Z", "general traffic", "shanghai", 3.215},
								{"2019-08-17T03:12:00Z", "general traffic", "beijing", 3.911},
								{"2019-08-17T03:12:00Z", "general traffic", "shanghai", 3.33},
								{"2019-08-17T03:18:00Z", "general traffic", "beijing", 3.786},
								{"2019-08-17T03:18:00Z", "general traffic", "shanghai", 3.32},
								{"2019-08-17T03:24:00Z", "general traffic", "beijing", 3.645},
								{"2019-08-17T03:24:00Z", "general traffic", "shanghai", 3.419},
								{"2019-08-17T03:30:00Z", "general traffic", "beijing", 3.524},
								{"2019-08-17T03:30:00Z", "general traffic", "shanghai", 3.547},
								{"2019-08-17T03:36:00Z", "general traffic", "beijing", 3.399},
								{"2019-08-17T03:36:00Z", "general traffic", "shanghai", 3.655},
								{"2019-08-17T03:42:00Z", "general traffic", "beijing", 3.278},
								{"2019-08-17T03:42:00Z", "general traffic", "shanghai", 3.658},
								{"2019-08-17T03:48:00Z", "general traffic", "beijing", 3.159},
								{"2019-08-17T03:48:00Z", "general traffic", "shanghai", 3.76},
								{"2019-08-17T03:54:00Z", "general traffic", "beijing", 3.048},
								{"2019-08-17T03:54:00Z", "general traffic", "shanghai", 3.819},
								{"2019-08-17T04:00:00Z", "less traffic", "beijing", 2.943},
								{"2019-08-17T04:00:00Z", "general traffic", "shanghai", 3.911},
								{"2019-08-17T04:06:00Z", "less traffic", "beijing", 2.831},
								{"2019-08-17T04:06:00Z", "general traffic", "shanghai", 4.055},
								{"2019-08-17T04:12:00Z", "less traffic", "beijing", 2.717},
								{"2019-08-17T04:12:00Z", "general traffic", "shanghai", 4.055},
								{"2019-08-17T04:18:00Z", "less traffic", "beijing", 2.625},
								{"2019-08-17T04:18:00Z", "general traffic", "shanghai", 4.124},
								{"2019-08-17T04:24:00Z", "less traffic", "beijing", 2.533},
								{"2019-08-17T04:24:00Z", "general traffic", "shanghai", 4.183},
								{"2019-08-17T04:30:00Z", "less traffic", "beijing", 2.451},
								{"2019-08-17T04:30:00Z", "general traffic", "shanghai", 4.242},
								{"2019-08-17T04:36:00Z", "less traffic", "beijing", 2.385},
								{"2019-08-17T04:36:00Z", "general traffic", "shanghai", 4.36},
								{"2019-08-17T04:42:00Z", "less traffic", "beijing", 2.339},
								{"2019-08-17T04:42:00Z", "general traffic", "shanghai", 4.501},
								{"2019-08-17T04:48:00Z", "less traffic", "beijing", 2.293},
								{"2019-08-17T04:48:00Z", "general traffic", "shanghai", 4.501},
								{"2019-08-17T04:54:00Z", "less traffic", "beijing", 2.287},
								{"2019-08-17T04:54:00Z", "general traffic", "shanghai", 4.567},
								{"2019-08-17T05:00:00Z", "less traffic", "beijing", 2.29},
								{"2019-08-17T05:00:00Z", "general traffic", "shanghai", 4.675},
								{"2019-08-17T05:06:00Z", "less traffic", "beijing", 2.313},
								{"2019-08-17T05:06:00Z", "general traffic", "shanghai", 4.642},
								{"2019-08-17T05:12:00Z", "less traffic", "beijing", 2.359},
								{"2019-08-17T05:12:00Z", "general traffic", "shanghai", 4.751},
								{"2019-08-17T05:18:00Z", "less traffic", "beijing", 2.425},
								{"2019-08-17T05:18:00Z", "general traffic", "shanghai", 4.888},
								{"2019-08-17T05:24:00Z", "less traffic", "beijing", 2.513},
								{"2019-08-17T05:24:00Z", "general traffic", "shanghai", 4.82},
								{"2019-08-17T05:30:00Z", "less traffic", "beijing", 2.608},
								{"2019-08-17T05:30:00Z", "general traffic", "shanghai", 4.8},
								{"2019-08-17T05:36:00Z", "less traffic", "beijing", 2.703},
								{"2019-08-17T05:36:00Z", "general traffic", "shanghai", 4.882},
								{"2019-08-17T05:42:00Z", "less traffic", "beijing", 2.822},
								{"2019-08-17T05:42:00Z", "general traffic", "shanghai", 4.898},
								{"2019-08-17T05:48:00Z", "less traffic", "beijing", 2.927},
								{"2019-08-17T05:48:00Z", "general traffic", "shanghai", 4.98},
								{"2019-08-17T05:54:00Z", "general traffic", "beijing", 3.054},
								{"2019-08-17T05:54:00Z", "general traffic", "shanghai", 4.997},
								{"2019-08-17T06:00:00Z", "general traffic", "beijing", 3.176},
								{"2019-08-17T06:00:00Z", "general traffic", "shanghai", 4.977},
								{"2019-08-17T06:06:00Z", "general traffic", "beijing", 3.304},
								{"2019-08-17T06:06:00Z", "general traffic", "shanghai", 4.987},
								{"2019-08-17T06:12:00Z", "general traffic", "beijing", 3.432},
								{"2019-08-17T06:12:00Z", "general traffic", "shanghai", 4.931},
								{"2019-08-17T06:18:00Z", "general traffic", "beijing", 3.57},
								{"2019-08-17T06:18:00Z", "general traffic", "shanghai", 5.02},
								{"2019-08-17T06:24:00Z", "general traffic", "beijing", 3.72},
								{"2019-08-17T06:24:00Z", "general traffic", "shanghai", 5.052},
								{"2019-08-17T06:30:00Z", "general traffic", "beijing", 3.881},
								{"2019-08-17T06:30:00Z", "general traffic", "shanghai", 5.052},
								{"2019-08-17T06:36:00Z", "general traffic", "beijing", 4.049},
								{"2019-08-17T06:36:00Z", "general traffic", "shanghai", 5.105},
								{"2019-08-17T06:42:00Z", "general traffic", "beijing", 4.209},
								{"2019-08-17T06:42:00Z", "general traffic", "shanghai", 5.089},
								{"2019-08-17T06:48:00Z", "general traffic", "beijing", 4.383},
								{"2019-08-17T06:48:00Z", "general traffic", "shanghai", 5.066},
								{"2019-08-17T06:54:00Z", "general traffic", "beijing", 4.56},
								{"2019-08-17T06:54:00Z", "general traffic", "shanghai", 5.059},
								{"2019-08-17T07:00:00Z", "general traffic", "beijing", 4.744},
								{"2019-08-17T07:00:00Z", "general traffic", "shanghai", 5.033},
								{"2019-08-17T07:06:00Z", "general traffic", "beijing", 4.915},
								{"2019-08-17T07:06:00Z", "general traffic", "shanghai", 5.039},
								{"2019-08-17T07:12:00Z", "general traffic", "beijing", 5.102},
								{"2019-08-17T07:12:00Z", "general traffic", "shanghai", 5.059},
								{"2019-08-17T07:18:00Z", "general traffic", "beijing", 5.289},
								{"2019-08-17T07:18:00Z", "general traffic", "shanghai", 4.964},
								{"2019-08-17T07:24:00Z", "general traffic", "beijing", 5.469},
								{"2019-08-17T07:24:00Z", "general traffic", "shanghai", 5.007},
								{"2019-08-17T07:30:00Z", "general traffic", "beijing", 5.643},
								{"2019-08-17T07:30:00Z", "general traffic", "shanghai", 4.921},
								{"2019-08-17T07:36:00Z", "general traffic", "beijing", 5.814},
								{"2019-08-17T07:36:00Z", "general traffic", "shanghai", 4.875},
								{"2019-08-17T07:42:00Z", "general traffic", "beijing", 5.974},
								{"2019-08-17T07:42:00Z", "general traffic", "shanghai", 4.839},
								{"2019-08-17T07:48:00Z", "general traffic", "shanghai", 4.8},
								{"2019-08-17T07:48:00Z", "high traffic", "beijing", 6.138},
								{"2019-08-17T07:54:00Z", "general traffic", "shanghai", 4.724},
								{"2019-08-17T07:54:00Z", "high traffic", "beijing", 6.293},
								{"2019-08-17T08:00:00Z", "general traffic", "shanghai", 4.547},
								{"2019-08-17T08:00:00Z", "high traffic", "beijing", 6.447},
								{"2019-08-17T08:06:00Z", "general traffic", "shanghai", 4.488},
								{"2019-08-17T08:06:00Z", "high traffic", "beijing", 6.601},
								{"2019-08-17T08:12:00Z", "general traffic", "shanghai", 4.508},
								{"2019-08-17T08:12:00Z", "high traffic", "beijing", 6.749},
								{"2019-08-17T08:18:00Z", "general traffic", "shanghai", 4.393},
								{"2019-08-17T08:18:00Z", "high traffic", "beijing", 6.893},
								{"2019-08-17T08:24:00Z", "general traffic", "shanghai", 4.334},
								{"2019-08-17T08:24:00Z", "high traffic", "beijing", 7.037},
								{"2019-08-17T08:30:00Z", "general traffic", "shanghai", 4.314},
								{"2019-08-17T08:30:00Z", "high traffic", "beijing", 7.172},
								{"2019-08-17T08:36:00Z", "general traffic", "shanghai", 4.173},
								{"2019-08-17T08:36:00Z", "high traffic", "beijing", 7.3},
								{"2019-08-17T08:42:00Z", "general traffic", "shanghai", 4.131},
								{"2019-08-17T08:42:00Z", "high traffic", "beijing", 7.428},
								{"2019-08-17T08:48:00Z", "general traffic", "shanghai", 3.996},
								{"2019-08-17T08:48:00Z", "high traffic", "beijing", 7.549},
								{"2019-08-17T08:54:00Z", "general traffic", "shanghai", 3.924},
								{"2019-08-17T08:54:00Z", "high traffic", "beijing", 7.667},
								{"2019-08-17T09:00:00Z", "general traffic", "shanghai", 3.93},
								{"2019-08-17T09:00:00Z", "high traffic", "beijing", 7.776},
								{"2019-08-17T09:06:00Z", "general traffic", "shanghai", 3.78},
								{"2019-08-17T09:06:00Z", "high traffic", "beijing", 7.874},
								{"2019-08-17T09:12:00Z", "general traffic", "shanghai", 3.773},
								{"2019-08-17T09:12:00Z", "high traffic", "beijing", 7.963},
								{"2019-08-17T09:18:00Z", "general traffic", "shanghai", 3.724},
								{"2019-08-17T09:18:00Z", "high traffic", "beijing", 8.045},
								{"2019-08-17T09:24:00Z", "general traffic", "shanghai", 3.556},
								{"2019-08-17T09:24:00Z", "high traffic", "beijing", 8.114},
								{"2019-08-17T09:30:00Z", "general traffic", "shanghai", 3.461},
								{"2019-08-17T09:30:00Z", "high traffic", "beijing", 8.166},
								{"2019-08-17T09:36:00Z", "general traffic", "shanghai", 3.373},
								{"2019-08-17T09:36:00Z", "high traffic", "beijing", 8.209},
								{"2019-08-17T09:42:00Z", "general traffic", "shanghai", 3.281},
								{"2019-08-17T09:42:00Z", "high traffic", "beijing", 8.238},
								{"2019-08-17T09:48:00Z", "general traffic", "shanghai", 3.126144146},
								{"2019-08-17T09:48:00Z", "high traffic", "beijing", 8.258},
								{"2019-08-17T09:54:00Z", "general traffic", "shanghai", 3.077},
								{"2019-08-17T09:54:00Z", "high traffic", "beijing", 8.271},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "traveling_info_database_autogen_information",
		Query: fmt.Sprintf(`SELECT * FROM "traveling_info_database"."autogen"."information" limit 20`),
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
		Name:  "traveling_info_database_information",
		Query: fmt.Sprintf(`SELECT * FROM "traveling_info_database".."information" limit 50`),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "description", "location", "human_traffic"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", "less traffic", "shanghai", 2.064},
								{"2019-08-17T00:00:00Z", "high traffic", "beijing", 8.12},
								{"2019-08-17T00:06:00Z", "less traffic", "shanghai", 2.116},
								{"2019-08-17T00:06:00Z", "high traffic", "beijing", 8.005},
								{"2019-08-17T00:12:00Z", "less traffic", "shanghai", 2.028},
								{"2019-08-17T00:12:00Z", "high traffic", "beijing", 7.887},
								{"2019-08-17T00:18:00Z", "less traffic", "shanghai", 2.126},
								{"2019-08-17T00:18:00Z", "high traffic", "beijing", 7.762},
								{"2019-08-17T00:24:00Z", "less traffic", "shanghai", 2.041},
								{"2019-08-17T00:24:00Z", "high traffic", "beijing", 7.635},
								{"2019-08-17T00:30:00Z", "less traffic", "shanghai", 2.051},
								{"2019-08-17T00:30:00Z", "high traffic", "beijing", 7.5},
								{"2019-08-17T00:36:00Z", "less traffic", "shanghai", 2.067},
								{"2019-08-17T00:36:00Z", "high traffic", "beijing", 7.372},
								{"2019-08-17T00:42:00Z", "less traffic", "shanghai", 2.057},
								{"2019-08-17T00:42:00Z", "high traffic", "beijing", 7.234},
								{"2019-08-17T00:48:00Z", "less traffic", "shanghai", 1.991},
								{"2019-08-17T00:48:00Z", "high traffic", "beijing", 7.11},
								{"2019-08-17T00:54:00Z", "less traffic", "shanghai", 2.054},
								{"2019-08-17T00:54:00Z", "high traffic", "beijing", 6.982},
								{"2019-08-17T01:00:00Z", "less traffic", "shanghai", 2.018},
								{"2019-08-17T01:00:00Z", "high traffic", "beijing", 6.837},
								{"2019-08-17T01:06:00Z", "less traffic", "shanghai", 2.096},
								{"2019-08-17T01:06:00Z", "high traffic", "beijing", 6.713},
								{"2019-08-17T01:12:00Z", "less traffic", "shanghai", 2.1},
								{"2019-08-17T01:12:00Z", "high traffic", "beijing", 6.578},
								{"2019-08-17T01:18:00Z", "less traffic", "shanghai", 2.106},
								{"2019-08-17T01:18:00Z", "high traffic", "beijing", 6.44},
								{"2019-08-17T01:24:00Z", "less traffic", "shanghai", 2.126144146},
								{"2019-08-17T01:24:00Z", "high traffic", "beijing", 6.299},
								{"2019-08-17T01:30:00Z", "less traffic", "shanghai", 2.1},
								{"2019-08-17T01:30:00Z", "high traffic", "beijing", 6.168},
								{"2019-08-17T01:36:00Z", "less traffic", "shanghai", 2.136},
								{"2019-08-17T01:36:00Z", "high traffic", "beijing", 6.024},
								{"2019-08-17T01:42:00Z", "less traffic", "shanghai", 2.182},
								{"2019-08-17T01:42:00Z", "general traffic", "beijing", 5.879},
								{"2019-08-17T01:48:00Z", "less traffic", "shanghai", 2.306},
								{"2019-08-17T01:48:00Z", "general traffic", "beijing", 5.745},
								{"2019-08-17T01:54:00Z", "less traffic", "shanghai", 2.448},
								{"2019-08-17T01:54:00Z", "general traffic", "beijing", 5.617},
								{"2019-08-17T02:00:00Z", "less traffic", "shanghai", 2.464},
								{"2019-08-17T02:00:00Z", "general traffic", "beijing", 5.472},
								{"2019-08-17T02:06:00Z", "less traffic", "shanghai", 2.467},
								{"2019-08-17T02:06:00Z", "general traffic", "beijing", 5.348},
								{"2019-08-17T02:12:00Z", "less traffic", "shanghai", 2.516},
								{"2019-08-17T02:12:00Z", "general traffic", "beijing", 5.2},
								{"2019-08-17T02:18:00Z", "less traffic", "shanghai", 2.674},
								{"2019-08-17T02:18:00Z", "general traffic", "beijing", 5.072},
								{"2019-08-17T02:24:00Z", "less traffic", "shanghai", 2.684},
								{"2019-08-17T02:24:00Z", "general traffic", "beijing", 4.934},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "location_information",
		Query: fmt.Sprintf(`SELECT "location" FROM "%s"."%s"."information" limit 20`, db, rp),
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
		Name:  "human_traffic_location_information",
		Query: fmt.Sprintf(`SELECT "human_traffic","location" FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "human_traffic", "location"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", 8.12, "beijing"},
								{"2019-08-17T00:00:00Z", 2.064, "shanghai"},
								{"2019-08-17T00:06:00Z", 8.005, "beijing"},
								{"2019-08-17T00:06:00Z", 2.116, "shanghai"},
								{"2019-08-17T00:12:00Z", 7.887, "beijing"},
								{"2019-08-17T00:12:00Z", 2.028, "shanghai"},
								{"2019-08-17T00:18:00Z", 7.762, "beijing"},
								{"2019-08-17T00:18:00Z", 2.126, "shanghai"},
								{"2019-08-17T00:24:00Z", 7.635, "beijing"},
								{"2019-08-17T00:24:00Z", 2.041, "shanghai"},
								{"2019-08-17T00:30:00Z", 7.5, "beijing"},
								{"2019-08-17T00:30:00Z", 2.051, "shanghai"},
								{"2019-08-17T00:36:00Z", 7.372, "beijing"},
								{"2019-08-17T00:36:00Z", 2.067, "shanghai"},
								{"2019-08-17T00:42:00Z", 7.234, "beijing"},
								{"2019-08-17T00:42:00Z", 2.057, "shanghai"},
								{"2019-08-17T00:48:00Z", 7.11, "beijing"},
								{"2019-08-17T00:48:00Z", 1.991, "shanghai"},
								{"2019-08-17T00:54:00Z", 6.982, "beijing"},
								{"2019-08-17T00:54:00Z", 2.054, "shanghai"},
							},
						},
					},
				},
			},
		},
	},
	//WHERE
	{
		Name:  "information_human_traffic_8",
		Query: fmt.Sprintf(`SELECT * FROM "%s"."%s"."information" WHERE "human_traffic" > 8 limit 30`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "description", "location", "human_traffic"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", "high traffic", "beijing", 8.12},
								{"2019-08-17T00:06:00Z", "high traffic", "beijing", 8.005},
								{"2019-08-17T09:18:00Z", "high traffic", "beijing", 8.045},
								{"2019-08-17T09:24:00Z", "high traffic", "beijing", 8.114},
								{"2019-08-17T09:30:00Z", "high traffic", "beijing", 8.166},
								{"2019-08-17T09:36:00Z", "high traffic", "beijing", 8.209},
								{"2019-08-17T09:42:00Z", "high traffic", "beijing", 8.238},
								{"2019-08-17T09:48:00Z", "high traffic", "beijing", 8.258},
								{"2019-08-17T09:54:00Z", "high traffic", "beijing", 8.271},
								{"2019-08-17T10:00:00Z", "high traffic", "beijing", 8.281},
								{"2019-08-17T10:06:00Z", "high traffic", "beijing", 8.284},
								{"2019-08-17T10:12:00Z", "high traffic", "beijing", 8.291},
								{"2019-08-17T10:18:00Z", "high traffic", "beijing", 8.297},
								{"2019-08-17T10:24:00Z", "high traffic", "beijing", 8.297},
								{"2019-08-17T10:30:00Z", "high traffic", "beijing", 8.287},
								{"2019-08-17T10:36:00Z", "high traffic", "beijing", 8.287},
								{"2019-08-17T10:42:00Z", "high traffic", "beijing", 8.278},
								{"2019-08-17T10:48:00Z", "high traffic", "beijing", 8.258},
								{"2019-08-17T10:54:00Z", "high traffic", "beijing", 8.222},
								{"2019-08-17T11:00:00Z", "high traffic", "beijing", 8.182},
								{"2019-08-17T11:06:00Z", "high traffic", "beijing", 8.133},
								{"2019-08-17T11:12:00Z", "high traffic", "beijing", 8.061},
								{"2019-08-17T21:42:00Z", "high traffic", "beijing", 8.022},
								{"2019-08-17T21:48:00Z", "high traffic", "beijing", 8.123},
								{"2019-08-17T21:54:00Z", "high traffic", "beijing", 8.215},
								{"2019-08-17T22:00:00Z", "high traffic", "beijing", 8.294},
								{"2019-08-17T22:06:00Z", "high traffic", "beijing", 8.373},
								{"2019-08-17T22:12:00Z", "high traffic", "beijing", 8.435},
								{"2019-08-17T22:18:00Z", "high traffic", "beijing", 8.488},
								{"2019-08-17T22:24:00Z", "high traffic", "beijing", 8.527},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "information_description_below_3_feet",
		Query: fmt.Sprintf(`SELECT * FROM "%s"."%s"."information" WHERE "description" = 'less traffic' limit 20 offset 1000`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "description", "location", "human_traffic"},
							Values: []suite.Row{
								{"2019-08-24T06:12:00Z", "less traffic", "shanghai", 1.424},
								{"2019-08-24T06:18:00Z", "less traffic", "shanghai", 1.437},
								{"2019-08-24T06:24:00Z", "less traffic", "shanghai", 1.375},
								{"2019-08-24T06:30:00Z", "less traffic", "shanghai", 1.289},
								{"2019-08-24T06:36:00Z", "less traffic", "shanghai", 1.161},
								{"2019-08-24T06:42:00Z", "less traffic", "shanghai", 1.138},
								{"2019-08-24T06:48:00Z", "less traffic", "shanghai", 1.102},
								{"2019-08-24T06:54:00Z", "less traffic", "shanghai", 1.043},
								{"2019-08-24T07:00:00Z", "less traffic", "shanghai", 0.984},
								{"2019-08-24T07:06:00Z", "less traffic", "shanghai", 0.896},
								{"2019-08-24T07:12:00Z", "less traffic", "shanghai", 0.86},
								{"2019-08-24T07:18:00Z", "less traffic", "shanghai", 0.807},
								{"2019-08-24T07:24:00Z", "less traffic", "shanghai", 0.81},
								{"2019-08-24T07:30:00Z", "less traffic", "shanghai", 0.807},
								{"2019-08-24T07:36:00Z", "less traffic", "shanghai", 0.797},
								{"2019-08-24T07:42:00Z", "less traffic", "shanghai", 0.715},
								{"2019-08-24T07:48:00Z", "less traffic", "shanghai", 0.676},
								{"2019-08-24T07:54:00Z", "less traffic", "shanghai", 0.712},
								{"2019-08-24T08:00:00Z", "less traffic", "shanghai", 0.781},
								{"2019-08-24T08:06:00Z", "less traffic", "shanghai", 0.745},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "information_human_traffic_2_11",
		Query: fmt.Sprintf(`SELECT * FROM "%s"."%s"."information" WHERE "human_traffic" + 2 > 11.9 limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "description", "location", "human_traffic"},
							Values: []suite.Row{
								{"2019-08-28T07:06:00Z", "very high traffic", "beijing", 9.902},
								{"2019-08-28T07:12:00Z", "very high traffic", "beijing", 9.938},
								{"2019-08-28T07:18:00Z", "very high traffic", "beijing", 9.957},
								{"2019-08-28T07:24:00Z", "very high traffic", "beijing", 9.964},
								{"2019-08-28T07:30:00Z", "very high traffic", "beijing", 9.954},
								{"2019-08-28T07:36:00Z", "very high traffic", "beijing", 9.941},
								{"2019-08-28T07:42:00Z", "very high traffic", "beijing", 9.925},
								{"2019-08-28T07:48:00Z", "very high traffic", "beijing", 9.902},
								{"2019-09-01T23:30:00Z", "very high traffic", "beijing", 9.902},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "information_human_traffic_location_shanghai",
		Query: fmt.Sprintf(`SELECT "human_traffic" FROM "%s"."%s"."information" WHERE "location" = 'shanghai' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "human_traffic"},
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
		Name:  "information_location_shanghai_human_traffic",
		Query: fmt.Sprintf(`SELECT "human_traffic" FROM "%s"."%s"."information" WHERE "location" <> 'shanghai' AND (human_traffic < -0.59 OR human_traffic > 9.95) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "human_traffic"},
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
		Name:  "information_time_7d",
		Query: fmt.Sprintf(`SELECT * FROM "%s"."%s"."information" WHERE time > now() - 7d limit 20`, db, rp),
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
		Name:  "information_description_9_feet",
		Query: fmt.Sprintf(`SELECT "description" FROM "%s"."%s"."information" WHERE "description" = 'very high traffic' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "description"},
							Values: []suite.Row{
								{"2019-08-25T04:00:00Z", "very high traffic"},
								{"2019-08-25T04:06:00Z", "very high traffic"},
								{"2019-08-25T04:12:00Z", "very high traffic"},
								{"2019-08-25T04:18:00Z", "very high traffic"},
								{"2019-08-25T04:24:00Z", "very high traffic"},
								{"2019-08-25T04:30:00Z", "very high traffic"},
								{"2019-08-25T04:36:00Z", "very high traffic"},
								{"2019-08-25T04:42:00Z", "very high traffic"},
								{"2019-08-25T04:48:00Z", "very high traffic"},
								{"2019-08-25T04:54:00Z", "very high traffic"},
								{"2019-08-25T05:00:00Z", "very high traffic"},
								{"2019-08-25T05:06:00Z", "very high traffic"},
								{"2019-08-25T05:12:00Z", "very high traffic"},
								{"2019-08-25T05:18:00Z", "very high traffic"},
								{"2019-08-26T04:42:00Z", "very high traffic"},
								{"2019-08-26T04:48:00Z", "very high traffic"},
								{"2019-08-26T04:54:00Z", "very high traffic"},
								{"2019-08-26T05:00:00Z", "very high traffic"},
								{"2019-08-26T05:06:00Z", "very high traffic"},
								{"2019-08-26T05:12:00Z", "very high traffic"},
							},
						},
					},
				},
			},
		},
	},
	//GROUP BY
	{
		Name:  "information_mean_human_traffic_location",
		Query: fmt.Sprintf(`SELECT MEAN("human_traffic") FROM "%s"."%s"."information" GROUP BY "location" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 5.3591424203039155},
							},
						},
						{
							Name:    "information",
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
		Name:  "air_quality_mean_air_score_location_air_level",
		Query: fmt.Sprintf(`SELECT MEAN("air_score") FROM "%s"."%s"."air_quality" GROUP BY location,air_level limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "air_quality",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 50.69033760186263},
							},
						},
						{
							Name:    "air_quality",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 49.661867544220485},
							},
						},
						{
							Name:    "air_quality",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 49.360939907550076},
							},
						},
						{
							Name:    "air_quality",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 49.132712456344585},
							},
						},
						{
							Name:    "air_quality",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 50.2937984496124},
							},
						},
						{
							Name:    "air_quality",
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
		Name:  "air_quality_mean_air_score_group_by_all",
		Query: fmt.Sprintf(`SELECT MEAN("air_score") FROM "%s"."%s"."air_quality" GROUP BY * limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "air_quality",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 50.69033760186263},
							},
						},
						{
							Name:    "air_quality",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 49.661867544220485},
							},
						},
						{
							Name:    "air_quality",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 49.360939907550076},
							},
						},
						{
							Name:    "air_quality",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 49.132712456344585},
							},
						},
						{
							Name:    "air_quality",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 50.2937984496124},
							},
						},
						{
							Name:    "air_quality",
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
		Name:  "human_traffic_location_time",
		Query: fmt.Sprintf(`SELECT "human_traffic","location" FROM "%s"."%s"."information" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' limits 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "human_traffic_time_12m_location",
		Query: fmt.Sprintf(`SELECT COUNT("human_traffic") FROM "%s"."%s"."information" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m),"location" limits 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "human_traffic_location_beijing_time",
		Query: fmt.Sprintf(`SELECT "human_traffic" FROM "%s"."%s"."information" WHERE "location"='beijing' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:18:00Z' limits 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "human_traffic_beijing_12m",
		Query: fmt.Sprintf(`SELECT COUNT("human_traffic") FROM "%s"."%s"."information" WHERE "location"='beijing' AND time >= '2015-08-18T00:06:00Z' AND time < '2015-08-18T00:18:00Z' GROUP BY time(12m) limit 20`, db, rp),
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
		Name:  "human_traffic_beijing_18m_6m",
		Query: fmt.Sprintf(`SELECT MEAN("human_traffic") FROM "%s"."%s"."information" WHERE "location"='beijing' AND time >= '2015-08-18T00:06:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(18m,6m) limit 20`, db, rp),
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
		Name:  "human_traffic_beijing_18m",
		Query: fmt.Sprintf(`SELECT MEAN("human_traffic") FROM "%s"."%s"."information" WHERE "location"='beijing' AND time >= '2015-08-18T00:06:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(18m) limit 20`, db, rp),
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
		Name:  "human_traffic_beijing_18m_12m",
		Query: fmt.Sprintf(`SELECT MEAN("human_traffic") FROM "%s"."%s"."information" WHERE "location"='beijing' AND time >= '2015-08-18T00:06:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(18m,-12m) limit 20`, db, rp),
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
		Name:  "human_traffic_beijing_12m_6m",
		Query: fmt.Sprintf(`SELECT COUNT("human_traffic") FROM "%s"."%s"."information" WHERE "location"='beijing' AND time >= '2015-08-18T00:06:00Z' AND time < '2015-08-18T00:18:00Z' GROUP BY time(12m,6m) limit 20`, db, rp),
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
		Name:  "human_traffic_beijing_12m_fill_100",
		Query: fmt.Sprintf(`SELECT MAX("human_traffic") FROM "%s"."%s"."information" WHERE "location"='beijing' AND time >= '2015-09-18T16:00:00Z' AND time <= '2015-09-18T16:42:00Z' GROUP BY time(12m) fill(100) limit 20`, db, rp),
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
		Name:  "human_traffic_beijing_12m_fill_800",
		Query: fmt.Sprintf(`SELECT MEAN("human_traffic") FROM "%s"."%s","information" WHERE "location" = 'beijing' AND time >= '2015-09-18T22:00:00Z' AND time <= '2015-09-18T22:18:00Z' GROUP BY time(12m) fill(800) limit 200`, db, rp),
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
		Name:  "human_traffic_beijing_24_12m_fill_previous",
		Query: fmt.Sprintf(`SELECT MAX("human_traffic") FROM "%s"."%s"."information" WHERE location = 'beijing' AND time >= '2015-09-18T16:24:00Z' AND time <= '2015-09-18T16:54:00Z' GROUP BY time(12m) fill(previous) limit 20`, db, rp),
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
		Name:  "human_traffic_beijing_36_12m_fill_previous",
		Query: fmt.Sprintf(`SELECT MAX("human_traffic") FROM "%s"."%s"."information" WHERE location = 'beijing' AND time >= '2015-09-18T16:36:00Z' AND time <= '2015-09-18T16:54:00Z' GROUP BY time(12m) fill(previous) limit 20`, db, rp),
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
		Name:  "time_desc_location_shanghai",
		Query: fmt.Sprintf(`SELECT "human_traffic" FROM "%s"."%s"."information" WHERE "location" = 'shanghai' ORDER BY time DESC limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "human_traffic"},
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
		Name:  "time_desc_mean_human_traffic_time",
		Query: fmt.Sprintf(`SELECT MEAN("human_traffic") FROM "%s"."%s"."information" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:42:00Z' GROUP BY time(12m) ORDER BY time DESC limit 20`, db, rp),
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
		Name:  "limit_human_traffic_location_3",
		Query: fmt.Sprintf(`SELECT "human_traffic","location" FROM "%s"."%s"."information" LIMIT 3`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "human_traffic", "location"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", 8.12, "beijing"},
								{"2019-08-17T00:00:00Z", 2.064, "shanghai"},
								{"2019-08-17T00:06:00Z", 8.005, "beijing"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "limit_mean_human_traffic_12m_2",
		Query: fmt.Sprintf(`SELECT MEAN("human_traffic") FROM "%s"."%s"."information" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:42:00Z' GROUP BY *,time(12m) LIMIT 2`, db, rp),
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
		Name:  "slimit_human_traffic_1",
		Query: fmt.Sprintf(`SELECT "human_traffic" FROM "%s"."%s"."information" GROUP BY * limit 20 SLIMIT 1`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "human_traffic"},
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
		Name:  "slimit_human_traffic_12m_1",
		Query: fmt.Sprintf(`SELECT MEAN("human_traffic") FROM "%s"."%s"."information" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:42:00Z' GROUP BY *,time(12m) SLIMIT 1`, db, rp),
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
		Name:  "limit_3_slimit_1_human_traffic",
		Query: fmt.Sprintf(`SELECT "human_traffic" FROM "%s"."%s"."information" GROUP BY * LIMIT 3 SLIMIT 1`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "human_traffic"},
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
		Name:  "limit_2_slimit_1_human_traffic_12m",
		Query: fmt.Sprintf(`SELECT MEAN("human_traffic") FROM "%s"."%s"."information" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:42:00Z' GROUP BY *,time(12m) LIMIT 2 SLIMIT 1`, db, rp),
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
		Name:  "limit_3_offset_3_human_traffic_location",
		Query: fmt.Sprintf(`SELECT "human_traffic","location" FROM "%s"."%s"."information" LIMIT 3 OFFSET 3`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "human_traffic", "location"},
							Values: []suite.Row{
								{"2019-08-17T00:06:00Z", 2.116, "shanghai"},
								{"2019-08-17T00:12:00Z", 7.887, "beijing"},
								{"2019-08-17T00:12:00Z", 2.028, "shanghai"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "limit_2_offset_2_slimit_1_human_traffic_time",
		Query: fmt.Sprintf(`SELECT MEAN("human_traffic") FROM "%s"."%s"."information" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:42:00Z' GROUP BY *,time(12m) ORDER BY time DESC LIMIT 2 OFFSET 2 SLIMIT 1`, db, rp),
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
		Name:  "slimit_1_soffset_1_human_traffic",
		Query: fmt.Sprintf(`SELECT "human_traffic" FROM "%s"."%s"."information" GROUP BY * SLIMIT 1 SOFFSET 1 limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "offset_2_slimit_1_offset_1_information_time_12m",
		Query: fmt.Sprintf(`SELECT MEAN("human_traffic") FROM "%s"."%s"."information" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:42:00Z' GROUP BY *,time(12m) ORDER BY time DESC LIMIT 2 OFFSET 2 SLIMIT 1 SOFFSET 1`, db, rp),
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
		Name:  "tz_america_chicago_location_shanghai",
		Query: fmt.Sprintf(`SELECT "human_traffic" FROM "%s"."%s"."information" WHERE "location" = 'shanghai' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:18:00Z' tz('America/Chicago') limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	//Time syntax
	{
		Name:  "time_human_traffic_location_rfc3339",
		Query: fmt.Sprintf(`SELECT "human_traffic" FROM "%s"."%s"."information" WHERE "location" = 'shanghai' AND time >= '2015-08-18T00:00:00.000000000Z' AND time <= '2015-08-18T00:12:00Z' limit 20`, db, rp),
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
		Name:  "time_human_traffic_location_rfc3339_like",
		Query: fmt.Sprintf(`SELECT "human_traffic" FROM "%s"."%s"."information" WHERE "location" = 'shanghai' AND time >= '2015-08-18' AND time <= '2015-08-18 00:12:00' limit 20`, db, rp),
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
		Name:  "time_human_traffic_location_timestamps",
		Query: fmt.Sprintf(`SELECT "human_traffic" FROM "%s"."%s"."information" WHERE "location" = 'shanghai' AND time >= 1439856000000000000 AND time <= 1439856720000000000 limit 20`, db, rp),
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
		Name:  "time_human_traffic_location_second_timestamps",
		Query: fmt.Sprintf(`SELECT "human_traffic" FROM "%s"."%s"."information" WHERE "location" = 'shanghai' AND time >= 1439856000s AND time <= 1439856720s limit 20`, db, rp),
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
		Name:  "time_human_traffic_rfc3999_6m",
		Query: fmt.Sprintf(`SELECT "human_traffic" FROM "%s"."%s"."information" WHERE time > '2015-09-18T21:24:00Z' + 6m limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "human_traffic"},
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
		Name:  "time_human_traffic_timestamp_6m",
		Query: fmt.Sprintf(`SELECT "human_traffic" FROM "%s"."%s"."information" WHERE time > 24043524m - 6m limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "human_traffic"},
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
		Name:  "time_now_human_traffic_1h",
		Query: fmt.Sprintf(`SELECT "human_traffic" FROM "%s"."%s"."information" WHERE time > now() - 1h limit 20`, db, rp),
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
		Query: fmt.Sprintf(`SELECT "description" FROM "%s"."%s"."information" WHERE time > '2015-09-18T21:18:00Z' AND time < now() + 1000d limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "description"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", "less traffic"},
								{"2019-08-17T00:00:00Z", "high traffic"},
								{"2019-08-17T00:06:00Z", "less traffic"},
								{"2019-08-17T00:06:00Z", "high traffic"},
								{"2019-08-17T00:12:00Z", "less traffic"},
								{"2019-08-17T00:12:00Z", "high traffic"},
								{"2019-08-17T00:18:00Z", "less traffic"},
								{"2019-08-17T00:18:00Z", "high traffic"},
								{"2019-08-17T00:24:00Z", "less traffic"},
								{"2019-08-17T00:24:00Z", "high traffic"},
								{"2019-08-17T00:30:00Z", "less traffic"},
								{"2019-08-17T00:30:00Z", "high traffic"},
								{"2019-08-17T00:36:00Z", "less traffic"},
								{"2019-08-17T00:36:00Z", "high traffic"},
								{"2019-08-17T00:42:00Z", "less traffic"},
								{"2019-08-17T00:42:00Z", "high traffic"},
								{"2019-08-17T00:48:00Z", "less traffic"},
								{"2019-08-17T00:48:00Z", "high traffic"},
								{"2019-08-17T00:54:00Z", "less traffic"},
								{"2019-08-17T00:54:00Z", "high traffic"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "time_fill_human_traffic_shanghai_12m",
		Query: fmt.Sprintf(`SELECT MEAN("human_traffic") FROM "%s"."%s"."information" WHERE "location"='shanghai' AND time >= '2015-09-18T21:30:00Z' GROUP BY time(12m) fill(none) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
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
		Name:  "time_fill_human_traffic_180w_12m",
		Query: fmt.Sprintf(`SELECT MEAN("human_traffic") FROM "%s"."%s"."information" WHERE "location"='shanghai' AND time >= '2015-09-18T21:30:00Z' AND time <= now() + 180w GROUP BY time(12m) fill(none) limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
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
		Name:  "time_now_fill_human_traffic_12m",
		Query: fmt.Sprintf(`SELECT MEAN("human_traffic") FROM "%s"."%s"."information" WHERE "location"='shanghai' AND time >= now() GROUP BY time(12m) fill(none) limit 20`, db, rp),
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
		Query: fmt.Sprintf(`SELECT /c/ FROM "%s"."%s"."information" LIMIT 1`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "description", "human_traffic", "location"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", "high traffic", 8.12, "beijing"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "re_mean_degree",
		Query: fmt.Sprintf(`SELECT MEAN("degree") FROM "%s"."%s"./temperature/`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "avg_temperature",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 79.98472932232272},
							},
						},
						{
							Name:    "max_temperature",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 64.98872722506226},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "re_human_traffic_3",
		Query: fmt.Sprintf(`SELECT MEAN(human_traffic) FROM "%s"."%s"."information" WHERE "location" =~ /[m]/ AND "human_traffic" > 3 limit 20`, db, rp),
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
		Name:  "re_all_location",
		Query: fmt.Sprintf(`SELECT * FROM "%s"."%s"."information" WHERE "location" !~ /./ limit 20`, db, rp),
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
		Name:  "re_mean_human_traffic_location",
		Query: fmt.Sprintf(`SELECT MEAN("human_traffic") FROM "%s"."%s"."information" WHERE "location" =~ /./ limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
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
		Name:  "re_mean_human_traffic_location_shanghai",
		Query: fmt.Sprintf(`SELECT MEAN("human_traffic") FROM "%s"."%s"."information" WHERE "location" = 'shanghai' AND "description" =~ /traffic/ limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
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
		Name:  "re_first_air_score_l",
		Query: fmt.Sprintf(`SELECT FIRST("air_score") FROM "%s"."%s"."air_quality" GROUP BY /l/ limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "air_quality",
							Columns: []string{"time", "first"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", 41},
							},
						},
						{
							Name:    "air_quality",
							Columns: []string{"time", "first"},
							Values: []suite.Row{
								{"2019-08-17T01:18:00Z", 9},
							},
						},
						{
							Name:    "air_quality",
							Columns: []string{"time", "first"},
							Values: []suite.Row{
								{"2019-08-17T00:30:00Z", 49},
							},
						},
						{
							Name:    "air_quality",
							Columns: []string{"time", "first"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", 99},
							},
						},
						{
							Name:    "air_quality",
							Columns: []string{"time", "first"},
							Values: []suite.Row{
								{"2019-08-17T00:06:00Z", 11},
							},
						},
						{
							Name:    "air_quality",
							Columns: []string{"time", "first"},
							Values: []suite.Row{
								{"2019-08-17T00:12:00Z", 65},
							},
						},
					},
				},
			},
		},
	},
	//Data types
	{
		Name:  "data_human_traffic_float_4",
		Query: fmt.Sprintf(`SELECT "human_traffic"::float FROM "%s"."%s"."information" LIMIT 4`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "human_traffic"},
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
		Name:  "data_human_traffic_integer_4",
		Query: fmt.Sprintf(`SELECT "human_traffic"::integer FROM "%s"."%s"."information" LIMIT 4`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "human_traffic"},
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
		Name:  "merge_mean_human_traffic",
		Query: fmt.Sprintf(`SELECT MEAN("human_traffic") FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
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
		Name:  "merge_mean_human_traffic_location",
		Query: fmt.Sprintf(`SELECT MEAN("human_traffic") FROM "%s"."%s"."information" WHERE "location" = 'beijing' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
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
		Name:  "merge_mean_human_traffic_group_by_location",
		Query: fmt.Sprintf(`SELECT MEAN("human_traffic") FROM "%s"."%s"."information" GROUP BY "location" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "mean"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 5.3591424203039155},
							},
						},
						{
							Name:    "information",
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
		Name:  "mul_mean_human_traffic_human_traffic_limit_2",
		Query: fmt.Sprintf(`SELECT MEAN("human_traffic") FROM "%s"."%s"."information"; SELECT "human_traffic" FROM "%s"."%s"."information" LIMIT 2`, db, rp, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
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
							Name:    "information",
							Columns: []string{"time", "human_traffic"},
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
		Name:  "sub_sum_max_human_traffic_group_by_location",
		Query: fmt.Sprintf(`SELECT SUM("max") FROM (SELECT MAX("human_traffic") FROM "%s"."%s"."information" GROUP BY "location") limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
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
		Name:  "sub_max_human_traffic_group_by_location",
		Query: fmt.Sprintf(`SELECT MAX("human_traffic") FROM "%s"."%s"."information" GROUP BY "location" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "max"},
							Values: []suite.Row{
								{"2019-08-28T07:24:00Z", 9.964},
							},
						},
						{
							Name:    "information",
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
		Query: fmt.Sprintf(`SELECT "all_the_means" FROM (SELECT MEAN("human_traffic") AS "all_the_means" FROM "%s"."%s"."information" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m) ) WHERE "all_the_means" > 5 limit 20`, db, rp),
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
		Query: fmt.Sprintf(`SELECT MEAN("human_traffic") AS "all_the_means" FROM "%s"."%s"."information" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m) limit 20`, db, rp),
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
		Query: fmt.Sprintf(`SELECT SUM("human_traffic_derivative") AS "sum_derivative" FROM (SELECT DERIVATIVE(MEAN("human_traffic")) AS "human_traffic_derivative" FROM "%s"."%s"."information" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m),"location") GROUP BY "location" limit 20`, db, rp),
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
		Name:  "sub_human_traffic_derivative",
		Query: fmt.Sprintf(`SELECT DERIVATIVE(MEAN("human_traffic")) AS "human_traffic_derivative" FROM "%s"."%s"."information" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m),"location" limit 20`, db, rp),
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
		Name:  "into_measurement_traveling_info_autogen",
		Query: fmt.Sprintf(`SELECT * INTO "copy_traveling_info_database"."autogen".:MEASUREMENT FROM "traveling_info_database"."autogen"./.* limit 20`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "into_copy1_location_beijing",
		Query: fmt.Sprintf(`SELECT "human_traffic" INTO "information_copy_1" FROM "%s"."%s"."information" WHERE "location" = 'beijing' limit 50`, db, rp),
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
		Name:  "into_information_copy_1",
		Query: fmt.Sprintf(`SELECT * FROM "%s"."%s"."information_copy_1" limit 20`, db, rp),
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
		Name:  "into_copy2_location_beijing",
		Query: fmt.Sprintf(`SELECT "human_traffic" INTO "where_else"."autogen"."information_copy_2" FROM "%s"."%s"."information" WHERE "location" = 'beijing' limit 50`, db, rp),
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
		Name:  "into_information_copy_2",
		Query: fmt.Sprintf(`SELECT * FROM "where_else"."autogen"."information_copy_2"`),
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
		Query: fmt.Sprintf(`SELECT MEAN("human_traffic") INTO "all_my_averages" FROM "%s"."%s"."information" WHERE "location" = 'beijing' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m) limit 50`, db, rp),
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
								{"traveling_info_database"},
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
								{"avg_temperature"},
								{"information"},
								{"recommended"},
								{"air_quality"},
								{"max_temperature"},
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
								{"information"},
								{"recommended"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "show_measurements_on_with",
		Query: fmt.Sprintf(`SHOW MEASUREMENTS ON "%s" WITH MEASUREMENT =~ /h2o.*/ WHERE "air_level"  =~ /d/`, db),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "measurements",
							Columns: []string{"name"},
							Values: []suite.Row{
								{"air_quality"},
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
							Name:    "avg_temperature",
							Columns: []string{"fieldKey", "fieldType"},
							Values: []suite.Row{
								{"degree", "float"},
							},
						},
						{
							Name:    "information",
							Columns: []string{"fieldKey", "fieldType"},
							Values: []suite.Row{
								{"description", "string"},
								{"human_traffic", "float"},
							},
						},
						{
							Name:    "recommended",
							Columns: []string{"fieldKey", "fieldType"},
							Values: []suite.Row{
								{"rec_level", "float"},
							},
						},
						{
							Name:    "air_quality",
							Columns: []string{"fieldKey", "fieldType"},
							Values: []suite.Row{
								{"air_score", "float"},
							},
						},
						{
							Name:    "max_temperature",
							Columns: []string{"fieldKey", "fieldType"},
							Values: []suite.Row{
								{"degree", "float"},
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
		Query: fmt.Sprintf(`SHOW FIELD KEYS ON "%s" FROM "%s"."%s"."information"`, db, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"fieldKey", "fieldType"},
							Values: []suite.Row{
								{"description", "string"},
								{"human_traffic", "float"},
							},
						},
					},
				},
			},
		},
	},
	//SHOW RETENTION POLICIES
	{
		Name:  "show_rp_on_traveling_info",
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
							Name:    "avg_temperature",
							Columns: []string{"tagKey"},
							Values: []suite.Row{
								{"location"},
							},
						},
						{
							Name:    "information",
							Columns: []string{"tagKey"},
							Values: []suite.Row{
								{"location"},
							},
						},
						{
							Name:    "recommended",
							Columns: []string{"tagKey"},
							Values: []suite.Row{
								{"location"},
							},
						},
						{
							Name:    "air_quality",
							Columns: []string{"tagKey"},
							Values: []suite.Row{
								{"location"},
								{"air_level"},
							},
						},
						{
							Name:    "max_temperature",
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
		Query: fmt.Sprintf(`SHOW TAG KEYS ON "%s" FROM "air_quality" LIMIT 1 OFFSET 1`, db),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "air_quality",
							Columns: []string{"tagKey"},
							Values: []suite.Row{
								{"air_level"},
							},
						},
					},
				},
			},
		},
	},
	//SHOW SERIES
	{
		Name:  "show_series_on_traveling_info",
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
								{"air_quality,air_level=1,location=beijing"},
								{"air_quality,air_level=1,location=shanghai"},
								{"air_quality,air_level=2,location=beijing"},
								{"air_quality,air_level=2,location=shanghai"},
								{"air_quality,air_level=3,location=beijing"},
								{"air_quality,air_level=3,location=shanghai"},
								{"avg_temperature,location=beijing"},
								{"avg_temperature,location=shanghai"},
								{"information,location=beijing"},
								{"information,location=shanghai"},
								{"max_temperature,location=beijing"},
								{"max_temperature,location=shanghai"},
								{"recommended,location=beijing"},
								{"recommended,location=shanghai"},
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
		Name:  "show_series_on_traveling_info_from",
		Query: fmt.Sprintf(`SHOW SERIES ON "%s" FROM "%s"."%s"."air_quality" WHERE "location" = 'beijing' LIMIT 2`, db, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "",
							Columns: []string{"key"},
							Values: []suite.Row{
								{"air_quality,location=beijing,air_level=1"},
								{"air_quality,location=beijing,air_level=2"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "show_series_on_traveling_info_now_1m",
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
								{"avg_temperature,location=beijing"},
								{"avg_temperature,location=shanghai"},
								{"information,location=beijing"},
								{"information,location=shanghai"},
								{"recommended,location=beijing"},
								{"recommended,location=shanghai"},
								{"air_quality,location=beijing,air_level=1"},
								{"air_quality,location=beijing,air_level=2"},
								{"air_quality,location=beijing,air_level=3"},
								{"air_quality,location=shanghai,air_level=1"},
								{"air_quality,location=shanghai,air_level=2"},
								{"air_quality,location=shanghai,air_level=3"},
								{"max_temperature,location=beijing"},
								{"max_temperature,location=shanghai"},
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
								{"avg_temperature,location=beijing"},
								{"avg_temperature,location=shanghai"},
								{"information,location=beijing"},
								{"information,location=shanghai"},
								{"recommended,location=beijing"},
								{"recommended,location=shanghai"},
								{"air_quality,location=beijing,air_level=1"},
								{"air_quality,location=beijing,air_level=2"},
								{"air_quality,location=beijing,air_level=3"},
								{"air_quality,location=shanghai,air_level=1"},
								{"air_quality,location=shanghai,air_level=2"},
								{"air_quality,location=shanghai,air_level=3"},
								{"max_temperature,location=beijing"},
								{"max_temperature,location=shanghai"},
							},
						},
					},
				},
			},
		},
	},
	//SHOW TAG VALUES
	{
		Name:  "show_tag_values_on_air_level",
		Query: fmt.Sprintf(`SHOW TAG VALUES ON "%s" WITH KEY = "air_level"`, db),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "air_quality",
							Columns: []string{"key", "value"},
							Values: []suite.Row{
								{"air_level", "1"},
								{"air_level", "2"},
								{"air_level", "3"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "show_tag_values_air_level",
		Query: fmt.Sprintf(`SHOW TAG VALUES WITH KEY = "air_level"`),
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
		Name:  "show_tag_values_on_location_air_level",
		Query: fmt.Sprintf(`SHOW TAG VALUES ON "%s" WITH KEY IN ("location","air_level") WHERE "air_level" =~ /./ LIMIT 3`, db),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "air_quality",
							Columns: []string{"key", "value"},
							Values: []suite.Row{
								{"location", "beijing"},
								{"location", "shanghai"},
								{"air_level", "1"},
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
							Name:    "traveling_info_database",
							Columns: []string{"id", "database", "rp", "shard_group", "start_time", "end_time", "expiry_time", "owners"},
							Values: []suite.Row{
								{2, "traveling_info_database", "rp0", 1, "2019-08-12T00:00:00Z", "2019-08-19T00:00:00Z", "2019-08-19T00:00:00Z", "0"},
								{4, "traveling_info_database", "rp0", 2, "2019-08-19T00:00:00Z", "2019-08-26T00:00:00Z", "2019-08-26T00:00:00Z", "0"},
								{6, "traveling_info_database", "rp0", 3, "2019-08-26T00:00:00Z", "2019-09-02T00:00:00Z", "2019-09-02T00:00:00Z", "0"},
								{8, "traveling_info_database", "rp0", 4, "2019-09-02T00:00:00Z", "2019-09-09T00:00:00Z", "2019-09-09T00:00:00Z", "0"},
								{10, "traveling_info_database", "rp0", 5, "2019-09-09T00:00:00Z", "2019-09-16T00:00:00Z", "2019-09-16T00:00:00Z", "0"},
								{12, "traveling_info_database", "rp0", 6, "2019-09-16T00:00:00Z", "2019-09-23T00:00:00Z", "2019-09-23T00:00:00Z", "0"},
							},
						},
					},
				},
			},
		},
	},
	//COUNT()
	{
		Name:  "agg_count_human_traffic",
		Query: fmt.Sprintf(`SELECT COUNT("human_traffic") FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
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
		Query: fmt.Sprintf(`SELECT COUNT(*) FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "count_description", "count_human_traffic"},
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
		Query: fmt.Sprintf(`SELECT COUNT(/traffic/) FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "count_human_traffic"},
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
		Query: fmt.Sprintf(`SELECT COUNT("human_traffic") FROM "%s"."%s"."information" WHERE time >= '2015-08-17T23:48:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(12m),* fill(200) LIMIT 7 SLIMIT 1`, db, rp),
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
		Query: fmt.Sprintf(`SELECT COUNT(DISTINCT("description")) FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
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
		Query: fmt.Sprintf(`SELECT COUNT("human_traffic") FROM "%s"."%s"."information" WHERE time >= '2015-09-18T21:24:00Z' AND time <= '2015-09-18T21:54:00Z' GROUP BY time(12m) limit 20`, db, rp),
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
		Query: fmt.Sprintf(`SELECT COUNT("human_traffic") FROM "%s"."%s"."information" WHERE time >= '2015-09-18T21:24:00Z' AND time <= '2015-09-18T21:54:00Z' GROUP BY time(12m) fill(800000) limit 20`, db, rp),
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
		Query: fmt.Sprintf(`SELECT DISTINCT("description") FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "distinct"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", "less traffic"},
								{"1970-01-01T00:00:00Z", "high traffic"},
								{"1970-01-01T00:00:00Z", "general traffic"},
								{"1970-01-01T00:00:00Z", "very high traffic"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "agg_distinct_all",
		Query: fmt.Sprintf(`SELECT DISTINCT(*) FROM "%s"."%s"."information" limit 20`, db, rp),
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
		Query: fmt.Sprintf(`SELECT DISTINCT("description") FROM "%s"."%s"."information" WHERE time >= '2015-08-17T23:48:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(12m),* limit 20 SLIMIT 1`, db, rp),
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
		Query: fmt.Sprintf(`SELECT DISTINCT("description") INTO "distincts" FROM "%s"."%s"."information"`, db, rp),
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
		Query: fmt.Sprintf(`SELECT "human_traffic" FROM "%s"."%s"."information" WHERE "location" = 'shanghai' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' limit 20`, db, rp),
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
		Query: fmt.Sprintf(`SELECT INTEGRAL("human_traffic") FROM "%s"."%s"."information" WHERE "location" = 'shanghai' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' limit 20`, db, rp),
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
		Name:  "agg_integral_human_traffic_1m",
		Query: fmt.Sprintf(`SELECT INTEGRAL("human_traffic",1m) FROM "%s"."%s"."information" WHERE "location" = 'shanghai' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' limit 20`, db, rp),
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
		Query: fmt.Sprintf(`SELECT INTEGRAL(*,1m) FROM "%s"."%s"."information" WHERE "location" = 'shanghai' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' limit 20`, db, rp),
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
		Query: fmt.Sprintf(`SELECT INTEGRAL(/human/,1m) FROM "%s"."%s"."information" WHERE "location" = 'shanghai' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' limit 20`, db, rp),
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
		Query: fmt.Sprintf(`SELECT INTEGRAL("human_traffic",1m) FROM "%s"."%s"."information" WHERE "location" = 'shanghai' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m) LIMIT 1`, db, rp),
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
		Name:  "agg_mean_human_traffic",
		Query: fmt.Sprintf(`SELECT MEAN("human_traffic") FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
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
		Query: fmt.Sprintf(`SELECT MEAN(*) FROM "%s"."%s"."information"`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "mean_human_traffic"},
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
		Query: fmt.Sprintf(`SELECT MEAN(/human/) FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "mean_human_traffic"},
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
		Query: fmt.Sprintf(`SELECT MEAN("human_traffic") FROM "%s"."%s"."information" WHERE time >= '2015-08-17T23:48:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(12m),* fill(9.01) LIMIT 7 SLIMIT 1`, db, rp),
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
		Query: fmt.Sprintf(`SELECT MEDIAN("human_traffic") FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
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
		Query: fmt.Sprintf(`SELECT MEDIAN(*) FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "median_human_traffic"},
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
		Query: fmt.Sprintf(`SELECT MEDIAN(/human/) FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "median_human_traffic"},
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
		Query: fmt.Sprintf(`SELECT MEDIAN("human_traffic") FROM "%s"."%s"."information" WHERE time >= '2015-08-17T23:48:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(12m),* fill(700) LIMIT 7 SLIMIT 1 SOFFSET 1`, db, rp),
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
		Query: fmt.Sprintf(`SELECT MODE("description") FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "mode"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", "general traffic"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "agg_mode_all",
		Query: fmt.Sprintf(`SELECT MODE(*) FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "mode_description", "mode_human_traffic"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", "general traffic", 2.69},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "agg_mode_re",
		Query: fmt.Sprintf(`SELECT MODE(/human/) FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "mode_human_traffic"},
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
		Query: fmt.Sprintf(`SELECT MODE("description") FROM "%s"."%s"."information" WHERE time >= '2015-08-17T23:48:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(12m),* LIMIT 3 SLIMIT 1 SOFFSET 1`, db, rp),
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
		Query: fmt.Sprintf(`SELECT SPREAD("human_traffic") FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
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
		Query: fmt.Sprintf(` SELECT SPREAD(*) FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "spread_human_traffic"},
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
		Query: fmt.Sprintf(`SELECT SPREAD(/human/) FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "spread_human_traffic"},
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
		Query: fmt.Sprintf(`SELECT SPREAD("human_traffic") FROM "%s"."%s"."information" WHERE time >= '2015-08-17T23:48:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(12m),* fill(18) LIMIT 3 SLIMIT 1 SOFFSET 1`, db, rp),
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
		Query: fmt.Sprintf(`SELECT STDDEV("human_traffic") FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
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
		Query: fmt.Sprintf(`SELECT STDDEV(*) FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "stddev_human_traffic"},
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
		Query: fmt.Sprintf(`SELECT STDDEV(/human/) FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "stddev_human_traffic"},
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
		Query: fmt.Sprintf(`SELECT STDDEV("human_traffic") FROM "%s"."%s"."information" WHERE time >= '2015-08-17T23:48:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(12m),* fill(18000) LIMIT 2 SLIMIT 1 SOFFSET 1`, db, rp),
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
		Query: fmt.Sprintf(`SELECT SUM("human_traffic") FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
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
		Query: fmt.Sprintf(`SELECT SUM(*) FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "sum_human_traffic"},
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
		Query: fmt.Sprintf(`SELECT SUM(/human/) FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "sum_human_traffic"},
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
		Query: fmt.Sprintf(`SELECT SUM("human_traffic") FROM "%s"."%s"."information" WHERE time >= '2015-08-17T23:48:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(12m),* fill(18000) LIMIT 4 SLIMIT 1`, db, rp),
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
		Query: fmt.Sprintf(`SELECT BOTTOM("human_traffic",3) FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
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
		Query: fmt.Sprintf(`SELECT BOTTOM("human_traffic","location",2) FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "bottom", "location"},
							Values: []suite.Row{
								{"2019-08-28T10:36:00Z", -0.243, "shanghai"},
								{"2019-08-28T14:30:00Z", -0.61, "beijing"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_bottom_human_traffic_4",
		Query: fmt.Sprintf(`SELECT BOTTOM("human_traffic",4),"location","description" FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "bottom", "location", "description"},
							Values: []suite.Row{
								{"2019-09-02T00:42:00Z", 1.778, "shanghai", "less traffic"},
								{"2019-09-02T00:48:00Z", 1.798, "shanghai", "less traffic"},
								{"2019-09-02T00:54:00Z", 1.752, "shanghai", "less traffic"},
								{"2019-09-02T01:00:00Z", 1.755, "shanghai", "less traffic"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_bottom_clauses",
		Query: fmt.Sprintf(`SELECT BOTTOM("human_traffic",3),"location" FROM "%s"."%s"."information" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(24m) ORDER BY time DESC limit 20`, db, rp),
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
		Name:  "sel_bottom_human_traffic_2_18m",
		Query: fmt.Sprintf(`SELECT BOTTOM("human_traffic",2) FROM "%s"."%s"."information" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'shanghai' GROUP BY time(18m) limit 20`, db, rp),
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
		Name:  "sel_bottom_human_traffic_location_3",
		Query: fmt.Sprintf(`SELECT BOTTOM("human_traffic","location",3) FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "bottom", "location"},
							Values: []suite.Row{
								{"2019-08-28T10:36:00Z", -0.243, "shanghai"},
								{"2019-08-28T14:30:00Z", -0.61, "beijing"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_bottom_into_bottom_human_traffics",
		Query: fmt.Sprintf(`SELECT BOTTOM("human_traffic","location",2) INTO "bottom_human_traffics" FROM "%s"."%s"."information" limit 20`, db, rp),
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
		Name:  "sel_bottom_select_bottom_human_traffics",
		Query: fmt.Sprintf(`SHOW TAG KEYS FROM "%s"."%s"."bottom_human_traffics"`, db, rp),
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
		Query: fmt.Sprintf(`SELECT FIRST("description") FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "first"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", "high traffic"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_first_all",
		Query: fmt.Sprintf(`SELECT FIRST(*) FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "first_description", "first_human_traffic"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", "high traffic", 8.12},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_first_re",
		Query: fmt.Sprintf(`SELECT FIRST(/traffic/) FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "first_human_traffic"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", 8.12},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_first_keys_tags",
		Query: fmt.Sprintf(`SELECT FIRST("description"),"location","human_traffic" FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "first", "location", "human_traffic"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", "high traffic", "beijing", 8.12},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_first_clauses",
		Query: fmt.Sprintf(`SELECT FIRST("human_traffic") FROM "%s"."%s"."information" WHERE time >= '2015-08-17T23:48:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(12m),* fill(9.01) LIMIT 4 SLIMIT 1`, db, rp),
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
		Query: fmt.Sprintf(`SELECT LAST("description") FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "last"},
							Values: []suite.Row{
								{"2019-09-17T21:42:00Z", "general traffic"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_last_all",
		Query: fmt.Sprintf(`SELECT LAST(*) FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "last_description", "last_human_traffic"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", "general traffic", 4.938},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_last_re",
		Query: fmt.Sprintf(`SELECT LAST(/traffic/) FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "last_human_traffic"},
							Values: []suite.Row{
								{"2019-09-17T21:42:00Z", 4.938},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_last_keys_tags",
		Query: fmt.Sprintf(`SELECT LAST("description"),"location","human_traffic" FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "last", "location", "human_traffic"},
							Values: []suite.Row{
								{"2019-09-17T21:42:00Z", "general traffic", "shanghai", 4.938},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_last_clauses",
		Query: fmt.Sprintf(`SELECT LAST("human_traffic") FROM "%s"."%s"."information" WHERE time >= '2015-08-17T23:48:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(12m),* fill(9.01) LIMIT 4 SLIMIT 1`, db, rp),
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
		Query: fmt.Sprintf(`SELECT MAX("human_traffic") FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
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
		Query: fmt.Sprintf(`SELECT MAX(*) FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "max_human_traffic"},
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
		Query: fmt.Sprintf(`SELECT MAX(/traffic/) FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "max_human_traffic"},
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
		Query: fmt.Sprintf(`SELECT MAX("human_traffic"),"location","description" FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "max", "location", "description"},
							Values: []suite.Row{
								{"2019-08-28T07:24:00Z", 9.964, "beijing", "very high traffic"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_max_clauses",
		Query: fmt.Sprintf(`SELECT MAX("human_traffic") FROM "%s"."%s"."information" WHERE time >= '2015-08-17T23:48:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(12m),* fill(9.01) LIMIT 4 SLIMIT 1`, db, rp),
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
		Query: fmt.Sprintf(`SELECT MIN("human_traffic") FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
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
		Query: fmt.Sprintf(`SELECT MIN(*) FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "min_human_traffic"},
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
		Query: fmt.Sprintf(`SELECT MIN(/traffic/) FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "min_human_traffic"},
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
		Query: fmt.Sprintf(`SELECT MIN("human_traffic"),"location","description" FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "min", "location", "description"},
							Values: []suite.Row{
								{"2019-08-28T14:30:00Z", -0.61, "beijing", "less traffic"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_min_clauses",
		Query: fmt.Sprintf(`SELECT MIN("human_traffic") FROM "%s"."%s"."information" WHERE time >= '2015-08-17T23:48:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(12m),* fill(9.01) LIMIT 4 SLIMIT 1`, db, rp),
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
		Query: fmt.Sprintf(`SELECT PERCENTILE("human_traffic",5) FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
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
		Query: fmt.Sprintf(`SELECT PERCENTILE(*,5) FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "percentile_human_traffic"},
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
		Query: fmt.Sprintf(`SELECT PERCENTILE(/traffic/,5) FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "percentile_human_traffic"},
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
		Query: fmt.Sprintf(`SELECT PERCENTILE("human_traffic",5),"location","description" FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "percentile", "location", "description"},
							Values: []suite.Row{
								{"2019-08-30T03:42:00Z", 1.122, "beijing", "less traffic"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_percentile_clauses",
		Query: fmt.Sprintf(`SELECT PERCENTILE("human_traffic",20) FROM "%s"."%s"."information" WHERE time >= '2015-08-17T23:48:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(24m) fill(15) LIMIT 2`, db, rp),
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
		Query: fmt.Sprintf(`ELECT SAMPLE("human_traffic",2) FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "sel_sample_all",
		Query: fmt.Sprintf(`SELECT SAMPLE(*,2) FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "sample_description", "sample_human_traffic"},
							Values: []suite.Row{
								{"2019-08-19T20:24:00Z", nil, 4.977},
								{"2019-08-23T06:24:00Z", nil, 1.414},
								{"2019-09-01T17:00:00Z", "general traffic", nil},
								{"2019-09-03T23:54:00Z", "general traffic", nil},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_sample_re",
		Query: fmt.Sprintf(`SELECT SAMPLE(/traffic/,2) FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "sample_human_traffic"},
							Values: []suite.Row{
								{"2019-08-23T08:18:00Z", 1.316},
								{"2019-08-24T21:36:00Z", 3.967},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_sample_keys_tags",
		Query: fmt.Sprintf(`SELECT SAMPLE("human_traffic",2),"location","description" FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "sample", "location", "description"},
							Values: []suite.Row{
								{"2019-08-23T17:54:00Z", 6.335, "beijing", "high traffic"},
								{"2019-08-25T18:48:00Z", 2.894, "shanghai", "less traffic"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_sample_clauses",
		Query: fmt.Sprintf(`SELECT SAMPLE("human_traffic",1) FROM "%s"."%s"."information" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'shanghai' GROUP BY time(18m) limit 20`, db, rp),
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
		Query: fmt.Sprintf(`SELECT SAMPLE("human_traffic",2) FROM "%s"."%s"."information" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'shanghai' GROUP BY time(18m) limit 20`, db, rp),
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
		Query: fmt.Sprintf(`SELECT TOP("human_traffic",3) FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
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
		Query: fmt.Sprintf(`SELECT TOP("human_traffic","location",2) FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "top", "location"},
							Values: []suite.Row{
								{"2019-08-28T03:54:00Z", 7.205, "shanghai"},
								{"2019-08-28T07:24:00Z", 9.964, "beijing"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_top_keys_tags_fields",
		Query: fmt.Sprintf(`SELECT TOP("human_traffic",4),"location","description" FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "top", "location", "description"},
							Values: []suite.Row{
								{"2019-09-02T00:00:00Z", 9.777, "beijing", "very high traffic"},
								{"2019-09-02T00:06:00Z", 9.728, "beijing", "very high traffic"},
								{"2019-09-02T00:12:00Z", 9.649, "beijing", "very high traffic"},
								{"2019-09-02T00:18:00Z", 9.57, "beijing", "very high traffic"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_top_clauses",
		Query: fmt.Sprintf(`SELECT TOP("human_traffic",3),"location" FROM "%s"."%s"."information" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(24m) ORDER BY time DESC limit 20`, db, rp),
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
		Query: fmt.Sprintf(`SELECT TOP("human_traffic",2) FROM "%s"."%s"."information" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' AND "location" = 'shanghai' GROUP BY time(18m) limit 20`, db, rp),
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
		Name:  "sel_top_human_traffic_location_3",
		Query: fmt.Sprintf(`SELECT TOP("human_traffic","location",3) FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "top", "location"},
							Values: []suite.Row{
								{"2019-08-28T03:54:00Z", 7.205, "shanghai"},
								{"2019-08-28T07:24:00Z", 9.964, "beijing"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "sel_top_into",
		Query: fmt.Sprintf(`SELECT TOP("human_traffic","location",2) INTO "top_human_traffics" FROM "%s"."%s"."information"`, db, rp),
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
		Name:  "sel_top_show_top_human_traffics",
		Query: fmt.Sprintf(`SHOW TAG KEYS FROM "%s"."%s"."top_human_traffics"`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//HOLT_WINTERS()
	{
		Name:  "pred_hw_clauses_first",
		Query: fmt.Sprintf(`SELECT HOLT_WINTERS_WITH_FIT(FIRST("human_traffic"),10,4) FROM "%s"."%s"."information" WHERE "location"='shanghai' AND time >= '2015-08-22 22:12:00' AND time <= '2015-08-28 03:00:00' GROUP BY time(379m,348m)`, db, rp),
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
		Name:  "pred_hw_first",
		Query: fmt.Sprintf(`SELECT FIRST("human_traffic") FROM "%s"."%s"."information" WHERE "location"='shanghai' and time >= '2015-08-22 22:12:00' and time <= '2015-08-28 03:00:00' GROUP BY time(379m,348m)`, db, rp),
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
		Name:  "pred_hw_select_location_time",
		Query: fmt.Sprintf(`SELECT "human_traffic" FROM "%s"."%s"."information" WHERE "location"='shanghai' AND time >= '2015-08-22 22:12:00' AND time <= '2015-08-28 03:00:00' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
				},
			},
		},
	},
	//other
	{
		Name:  "other_mean_median",
		Query: fmt.Sprintf(`SELECT MEAN("human_traffic"),MEDIAN("human_traffic") FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "mean", "median"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 4.441931402107023, 4.124},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "other_mode_mode",
		Query: fmt.Sprintf(`SELECT MODE("human_traffic"),MODE("description") FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "mode", "mode_1"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 2.69, "general traffic"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "other_min",
		Query: fmt.Sprintf(`SELECT MIN("human_traffic") FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
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
		Name:  "other_max",
		Query: fmt.Sprintf(`SELECT MAX("human_traffic") FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
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
		Name:  "other_mean_as",
		Query: fmt.Sprintf(`SELECT MEAN("human_traffic") AS "dream_name" FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "dream_name"},
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
		Name:  "other_mean",
		Query: fmt.Sprintf(`SELECT MEAN("human_traffic") FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
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
		Name:  "other_median_as_mode_as",
		Query: fmt.Sprintf(`SELECT MEDIAN("human_traffic") AS "med_wat",MODE("human_traffic") AS "mode_wat" FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "med_wat", "mode_wat"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 4.124, 2.69},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "other_median_mode",
		Query: fmt.Sprintf(`SELECT MEDIAN("human_traffic"),MODE("human_traffic") FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "median", "mode"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 4.124, 2.69},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "other_sum",
		Query: fmt.Sprintf(`SELECT SUM("human_traffic") FROM "%s"."%s"."information"`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
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
		Name:  "other_sum_time",
		Query: fmt.Sprintf(`SELECT SUM("human_traffic") FROM "%s"."%s"."information" WHERE time >= '2015-08-18T00:00:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "sum"},
							Values: []suite.Row{
								{"2015-08-18T00:00:00Z", 67774.98933334895},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "other_sum_time_12m",
		Query: fmt.Sprintf(`SELECT SUM("human_traffic") FROM "%s"."%s"."information" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:18:00Z' GROUP BY time(12m) limit 20`, db, rp),
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
		Name:  "other_sum_location",
		Query: fmt.Sprintf(`SELECT SUM("human_traffic"),"location" FROM "%s"."%s"."information" limit 20`, db, rp),
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
		Name:  "other_max_time",
		Query: fmt.Sprintf(`SELECT MAX("human_traffic") FROM "%s"."%s"."information" WHERE time >= '2015-08-18T00:00:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
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
		Name:  "other_first_all",
		Query: fmt.Sprintf(`SELECT FIRST(*) FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "first_description", "first_human_traffic"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", "high traffic", 8.12},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "other_max_all",
		Query: fmt.Sprintf(`SELECT MAX(*) FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "max_human_traffic"},
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
		Name:  "other_max_min",
		Query: fmt.Sprintf(`SELECT MAX("human_traffic"),MIN("human_traffic") FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "max", "min"},
							Values: []suite.Row{
								{"1970-01-01T00:00:00Z", 9.964, -0.61},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "other_max_min_time",
		Query: fmt.Sprintf(`SELECT MAX("human_traffic"),MIN("human_traffic") FROM "%s"."%s"."information" WHERE time >= '2015-08-18T00:00:00Z' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "max", "min"},
							Values: []suite.Row{
								{"2015-08-18T00:00:00Z", 9.964, -0.61},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "other_max_time_12m",
		Query: fmt.Sprintf(`SELECT MAX("human_traffic") FROM "%s"."%s"."information" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:18:00Z' GROUP BY time(12m) limit 20`, db, rp),
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
