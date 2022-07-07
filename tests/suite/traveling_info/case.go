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
								{"2019-08-27T10:00:00Z", "general traffic", "beijing", 5.525},
								{"2019-08-27T10:00:00Z", "less traffic", "shanghai", -0.062},
								{"2019-08-27T10:06:00Z", "general traffic", "beijing", 5.354},
								{"2019-08-27T10:06:00Z", "less traffic", "shanghai", -0.092},
								{"2019-08-27T10:12:00Z", "general traffic", "beijing", 5.187},
								{"2019-08-27T10:12:00Z", "less traffic", "shanghai", -0.105},
								{"2019-08-27T10:18:00Z", "general traffic", "beijing", 5.02},
								{"2019-08-27T10:18:00Z", "less traffic", "shanghai", -0.089},
								{"2019-08-27T10:24:00Z", "general traffic", "beijing", 4.849},
								{"2019-08-27T10:24:00Z", "less traffic", "shanghai", -0.112},
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
								{"air_quality"},
								{"avg_temperature"},
								{"information"},
								{"max_temperature"},
								{"recommended"},
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
							Columns: []string{"time", "description", "human_traffic", "location", "rec_level"},
							Values: []suite.Row{
								{"2019-08-19T02:00:00Z", "high traffic", 6.768, "beijing", nil},
								{"2019-08-19T02:00:00Z", "less traffic", 2.211, "shanghai", nil},
								{"2019-08-19T02:06:00Z", "high traffic", 6.631, "beijing", nil},
								{"2019-08-19T02:06:00Z", "less traffic", 2.188, "shanghai", nil},
								{"2019-08-19T02:12:00Z", "high traffic", 6.49, "beijing", nil},
								{"2019-08-19T02:12:00Z", "less traffic", 2.306, "shanghai", nil},
								{"2019-08-19T02:18:00Z", "high traffic", 6.358, "beijing", nil},
								{"2019-08-19T02:18:00Z", "less traffic", 2.323, "shanghai", nil},
								{"2019-08-19T02:24:00Z", "high traffic", 6.207, "beijing", nil},
								{"2019-08-19T02:24:00Z", "less traffic", 2.297, "shanghai", nil},
							},
						},
						{
							Name:    "recommended",
							Columns: []string{"time", "description", "human_traffic", "location", "rec_level"},
							Values: []suite.Row{
								{"2019-08-19T02:00:00Z", nil, nil, "beijing", 7},
								{"2019-08-19T02:00:00Z", nil, nil, "shanghai", 8},
								{"2019-08-19T02:06:00Z", nil, nil, "beijing", 7},
								{"2019-08-19T02:06:00Z", nil, nil, "shanghai", 8},
								{"2019-08-19T02:12:00Z", nil, nil, "beijing", 8},
								{"2019-08-19T02:12:00Z", nil, nil, "shanghai", 8},
								{"2019-08-19T02:18:00Z", nil, nil, "beijing", 7},
								{"2019-08-19T02:18:00Z", nil, nil, "shanghai", 7},
								{"2019-08-19T02:24:00Z", nil, nil, "beijing", 8},
								{"2019-08-19T02:24:00Z", nil, nil, "shanghai", 8},
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
								{"2019-08-17T00:00:00Z", "high traffic", "beijing", 8.12},
								{"2019-08-17T00:00:00Z", "less traffic", "shanghai", 2.064},
								{"2019-08-17T00:06:00Z", "high traffic", "beijing", 8.005},
								{"2019-08-17T00:06:00Z", "less traffic", "shanghai", 2.116},
								{"2019-08-17T00:12:00Z", "high traffic", "beijing", 7.887},
								{"2019-08-17T00:12:00Z", "less traffic", "shanghai", 2.028},
								{"2019-08-17T00:18:00Z", "high traffic", "beijing", 7.762},
								{"2019-08-17T00:18:00Z", "less traffic", "shanghai", 2.126},
								{"2019-08-17T00:24:00Z", "high traffic", "beijing", 7.635},
								{"2019-08-17T00:24:00Z", "less traffic", "shanghai", 2.041},
								{"2019-08-17T00:30:00Z", "high traffic", "beijing", 7.5},
								{"2019-08-17T00:30:00Z", "less traffic", "shanghai", 2.051},
								{"2019-08-17T00:36:00Z", "high traffic", "beijing", 7.372},
								{"2019-08-17T00:36:00Z", "less traffic", "shanghai", 2.067},
								{"2019-08-17T00:42:00Z", "high traffic", "beijing", 7.234},
								{"2019-08-17T00:42:00Z", "less traffic", "shanghai", 2.057},
								{"2019-08-17T00:48:00Z", "high traffic", "beijing", 7.11},
								{"2019-08-17T00:48:00Z", "less traffic", "shanghai", 1.991},
								{"2019-08-17T00:54:00Z", "high traffic", "beijing", 6.982},
								{"2019-08-17T00:54:00Z", "less traffic", "shanghai", 2.054},
								{"2019-08-17T01:00:00Z", "high traffic", "beijing", 6.837},
								{"2019-08-17T01:00:00Z", "less traffic", "shanghai", 2.018},
								{"2019-08-17T01:06:00Z", "high traffic", "beijing", 6.713},
								{"2019-08-17T01:06:00Z", "less traffic", "shanghai", 2.096},
								{"2019-08-17T01:12:00Z", "high traffic", "beijing", 6.578},
								{"2019-08-17T01:12:00Z", "less traffic", "shanghai", 2.1},
								{"2019-08-17T01:18:00Z", "high traffic", "beijing", 6.44},
								{"2019-08-17T01:18:00Z", "less traffic", "shanghai", 2.106},
								{"2019-08-17T01:24:00Z", "high traffic", "beijing", 6.299},
								{"2019-08-17T01:24:00Z", "less traffic", "shanghai", 2.126144146},
								{"2019-08-17T01:30:00Z", "high traffic", "beijing", 6.168},
								{"2019-08-17T01:30:00Z", "less traffic", "shanghai", 2.1},
								{"2019-08-17T01:36:00Z", "high traffic", "beijing", 6.024},
								{"2019-08-17T01:36:00Z", "less traffic", "shanghai", 2.136},
								{"2019-08-17T01:42:00Z", "general traffic", "beijing", 5.879},
								{"2019-08-17T01:42:00Z", "less traffic", "shanghai", 2.182},
								{"2019-08-17T01:48:00Z", "general traffic", "beijing", 5.745},
								{"2019-08-17T01:48:00Z", "less traffic", "shanghai", 2.306},
								{"2019-08-17T01:54:00Z", "general traffic", "beijing", 5.617},
								{"2019-08-17T01:54:00Z", "less traffic", "shanghai", 2.448},
								{"2019-08-17T02:00:00Z", "general traffic", "beijing", 5.472},
								{"2019-08-17T02:00:00Z", "less traffic", "shanghai", 2.464},
								{"2019-08-17T02:06:00Z", "general traffic", "beijing", 5.348},
								{"2019-08-17T02:06:00Z", "less traffic", "shanghai", 2.467},
								{"2019-08-17T02:12:00Z", "general traffic", "beijing", 5.2},
								{"2019-08-17T02:12:00Z", "less traffic", "shanghai", 2.516},
								{"2019-08-17T02:18:00Z", "general traffic", "beijing", 5.072},
								{"2019-08-17T02:18:00Z", "less traffic", "shanghai", 2.674},
								{"2019-08-17T02:24:00Z", "general traffic", "beijing", 4.934},
								{"2019-08-17T02:24:00Z", "less traffic", "shanghai", 2.684},
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
							Columns: []string{"time", "description", "human_traffic", "location"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", "high traffic", 8.12, "beijing"},
								{"2019-08-17T00:00:00Z", "less traffic", 2.064, "shanghai"},
								{"2019-08-17T00:06:00Z", "high traffic", 8.005, "beijing"},
								{"2019-08-17T00:06:00Z", "less traffic", 2.116, "shanghai"},
								{"2019-08-17T00:12:00Z", "high traffic", 7.887, "beijing"},
								{"2019-08-17T00:12:00Z", "less traffic", 2.028, "shanghai"},
								{"2019-08-17T00:18:00Z", "high traffic", 7.762, "beijing"},
								{"2019-08-17T00:18:00Z", "less traffic", 2.126, "shanghai"},
								{"2019-08-17T00:24:00Z", "high traffic", 7.635, "beijing"},
								{"2019-08-17T00:24:00Z", "less traffic", 2.041, "shanghai"},
								{"2019-08-17T00:30:00Z", "high traffic", 7.5, "beijing"},
								{"2019-08-17T00:30:00Z", "less traffic", 2.051, "shanghai"},
								{"2019-08-17T00:36:00Z", "high traffic", 7.372, "beijing"},
								{"2019-08-17T00:36:00Z", "less traffic", 2.067, "shanghai"},
								{"2019-08-17T00:42:00Z", "high traffic", 7.234, "beijing"},
								{"2019-08-17T00:42:00Z", "less traffic", 2.057, "shanghai"},
								{"2019-08-17T00:48:00Z", "high traffic", 7.11, "beijing"},
								{"2019-08-17T00:48:00Z", "less traffic", 1.991, "shanghai"},
								{"2019-08-17T00:54:00Z", "high traffic", 6.982, "beijing"},
								{"2019-08-17T00:54:00Z", "less traffic", 2.054, "shanghai"},
								{"2019-08-17T01:00:00Z", "high traffic", 6.837, "beijing"},
								{"2019-08-17T01:00:00Z", "less traffic", 2.018, "shanghai"},
								{"2019-08-17T01:06:00Z", "high traffic", 6.713, "beijing"},
								{"2019-08-17T01:06:00Z", "less traffic", 2.096, "shanghai"},
								{"2019-08-17T01:12:00Z", "high traffic", 6.578, "beijing"},
								{"2019-08-17T01:12:00Z", "less traffic", 2.1, "shanghai"},
								{"2019-08-17T01:18:00Z", "high traffic", 6.44, "beijing"},
								{"2019-08-17T01:18:00Z", "less traffic", 2.106, "shanghai"},
								{"2019-08-17T01:24:00Z", "high traffic", 6.299, "beijing"},
								{"2019-08-17T01:24:00Z", "less traffic", 2.126144146, "shanghai"},
								{"2019-08-17T01:30:00Z", "high traffic", 6.168, "beijing"},
								{"2019-08-17T01:30:00Z", "less traffic", 2.1, "shanghai"},
								{"2019-08-17T01:36:00Z", "high traffic", 6.024, "beijing"},
								{"2019-08-17T01:36:00Z", "less traffic", 2.136, "shanghai"},
								{"2019-08-17T01:42:00Z", "general traffic", 5.879, "beijing"},
								{"2019-08-17T01:42:00Z", "less traffic", 2.182, "shanghai"},
								{"2019-08-17T01:48:00Z", "general traffic", 5.745, "beijing"},
								{"2019-08-17T01:48:00Z", "less traffic", 2.306, "shanghai"},
								{"2019-08-17T01:54:00Z", "general traffic", 5.617, "beijing"},
								{"2019-08-17T01:54:00Z", "less traffic", 2.448, "shanghai"},
								{"2019-08-17T02:00:00Z", "general traffic", 5.472, "beijing"},
								{"2019-08-17T02:00:00Z", "less traffic", 2.464, "shanghai"},
								{"2019-08-17T02:06:00Z", "general traffic", 5.348, "beijing"},
								{"2019-08-17T02:06:00Z", "less traffic", 2.467, "shanghai"},
								{"2019-08-17T02:12:00Z", "general traffic", 5.2, "beijing"},
								{"2019-08-17T02:12:00Z", "less traffic", 2.516, "shanghai"},
								{"2019-08-17T02:18:00Z", "general traffic", 5.072, "beijing"},
								{"2019-08-17T02:18:00Z", "less traffic", 2.674, "shanghai"},
								{"2019-08-17T02:24:00Z", "general traffic", 4.934, "beijing"},
								{"2019-08-17T02:24:00Z", "less traffic", 2.684, "shanghai"},
								{"2019-08-17T02:30:00Z", "general traffic", 4.793, "beijing"},
								{"2019-08-17T02:30:00Z", "less traffic", 2.799, "shanghai"},
								{"2019-08-17T02:36:00Z", "general traffic", 4.662, "beijing"},
								{"2019-08-17T02:36:00Z", "less traffic", 2.927, "shanghai"},
								{"2019-08-17T02:42:00Z", "general traffic", 4.534, "beijing"},
								{"2019-08-17T02:42:00Z", "less traffic", 2.956, "shanghai"},
								{"2019-08-17T02:48:00Z", "general traffic", 4.403, "beijing"},
								{"2019-08-17T02:48:00Z", "less traffic", 2.927, "shanghai"},
								{"2019-08-17T02:54:00Z", "general traffic", 4.281, "beijing"},
								{"2019-08-17T02:54:00Z", "general traffic", 3.022, "shanghai"},
								{"2019-08-17T03:00:00Z", "general traffic", 4.154, "beijing"},
								{"2019-08-17T03:00:00Z", "general traffic", 3.087, "shanghai"},
								{"2019-08-17T03:06:00Z", "general traffic", 4.029, "beijing"},
								{"2019-08-17T03:06:00Z", "general traffic", 3.215, "shanghai"},
								{"2019-08-17T03:12:00Z", "general traffic", 3.911, "beijing"},
								{"2019-08-17T03:12:00Z", "general traffic", 3.33, "shanghai"},
								{"2019-08-17T03:18:00Z", "general traffic", 3.786, "beijing"},
								{"2019-08-17T03:18:00Z", "general traffic", 3.32, "shanghai"},
								{"2019-08-17T03:24:00Z", "general traffic", 3.645, "beijing"},
								{"2019-08-17T03:24:00Z", "general traffic", 3.419, "shanghai"},
								{"2019-08-17T03:30:00Z", "general traffic", 3.524, "beijing"},
								{"2019-08-17T03:30:00Z", "general traffic", 3.547, "shanghai"},
								{"2019-08-17T03:36:00Z", "general traffic", 3.399, "beijing"},
								{"2019-08-17T03:36:00Z", "general traffic", 3.655, "shanghai"},
								{"2019-08-17T03:42:00Z", "general traffic", 3.278, "beijing"},
								{"2019-08-17T03:42:00Z", "general traffic", 3.658, "shanghai"},
								{"2019-08-17T03:48:00Z", "general traffic", 3.159, "beijing"},
								{"2019-08-17T03:48:00Z", "general traffic", 3.76, "shanghai"},
								{"2019-08-17T03:54:00Z", "general traffic", 3.048, "beijing"},
								{"2019-08-17T03:54:00Z", "general traffic", 3.819, "shanghai"},
								{"2019-08-17T04:00:00Z", "general traffic", 3.911, "shanghai"},
								{"2019-08-17T04:00:00Z", "less traffic", 2.943, "beijing"},
								{"2019-08-17T04:06:00Z", "general traffic", 4.055, "shanghai"},
								{"2019-08-17T04:06:00Z", "less traffic", 2.831, "beijing"},
								{"2019-08-17T04:12:00Z", "general traffic", 4.055, "shanghai"},
								{"2019-08-17T04:12:00Z", "less traffic", 2.717, "beijing"},
								{"2019-08-17T04:18:00Z", "general traffic", 4.124, "shanghai"},
								{"2019-08-17T04:18:00Z", "less traffic", 2.625, "beijing"},
								{"2019-08-17T04:24:00Z", "general traffic", 4.183, "shanghai"},
								{"2019-08-17T04:24:00Z", "less traffic", 2.533, "beijing"},
								{"2019-08-17T04:30:00Z", "general traffic", 4.242, "shanghai"},
								{"2019-08-17T04:30:00Z", "less traffic", 2.451, "beijing"},
								{"2019-08-17T04:36:00Z", "general traffic", 4.36, "shanghai"},
								{"2019-08-17T04:36:00Z", "less traffic", 2.385, "beijing"},
								{"2019-08-17T04:42:00Z", "general traffic", 4.501, "shanghai"},
								{"2019-08-17T04:42:00Z", "less traffic", 2.339, "beijing"},
								{"2019-08-17T04:48:00Z", "general traffic", 4.501, "shanghai"},
								{"2019-08-17T04:48:00Z", "less traffic", 2.293, "beijing"},
								{"2019-08-17T04:54:00Z", "general traffic", 4.567, "shanghai"},
								{"2019-08-17T04:54:00Z", "less traffic", 2.287, "beijing"},
								{"2019-08-17T05:00:00Z", "general traffic", 4.675, "shanghai"},
								{"2019-08-17T05:00:00Z", "less traffic", 2.29, "beijing"},
								{"2019-08-17T05:06:00Z", "general traffic", 4.642, "shanghai"},
								{"2019-08-17T05:06:00Z", "less traffic", 2.313, "beijing"},
								{"2019-08-17T05:12:00Z", "general traffic", 4.751, "shanghai"},
								{"2019-08-17T05:12:00Z", "less traffic", 2.359, "beijing"},
								{"2019-08-17T05:18:00Z", "general traffic", 4.888, "shanghai"},
								{"2019-08-17T05:18:00Z", "less traffic", 2.425, "beijing"},
								{"2019-08-17T05:24:00Z", "general traffic", 4.82, "shanghai"},
								{"2019-08-17T05:24:00Z", "less traffic", 2.513, "beijing"},
								{"2019-08-17T05:30:00Z", "general traffic", 4.8, "shanghai"},
								{"2019-08-17T05:30:00Z", "less traffic", 2.608, "beijing"},
								{"2019-08-17T05:36:00Z", "general traffic", 4.882, "shanghai"},
								{"2019-08-17T05:36:00Z", "less traffic", 2.703, "beijing"},
								{"2019-08-17T05:42:00Z", "general traffic", 4.898, "shanghai"},
								{"2019-08-17T05:42:00Z", "less traffic", 2.822, "beijing"},
								{"2019-08-17T05:48:00Z", "general traffic", 4.98, "shanghai"},
								{"2019-08-17T05:48:00Z", "less traffic", 2.927, "beijing"},
								{"2019-08-17T05:54:00Z", "general traffic", 4.997, "shanghai"},
								{"2019-08-17T05:54:00Z", "general traffic", 3.054, "beijing"},
								{"2019-08-17T06:00:00Z", "general traffic", 4.977, "shanghai"},
								{"2019-08-17T06:00:00Z", "general traffic", 3.176, "beijing"},
								{"2019-08-17T06:06:00Z", "general traffic", 4.987, "shanghai"},
								{"2019-08-17T06:06:00Z", "general traffic", 3.304, "beijing"},
								{"2019-08-17T06:12:00Z", "general traffic", 4.931, "shanghai"},
								{"2019-08-17T06:12:00Z", "general traffic", 3.432, "beijing"},
								{"2019-08-17T06:18:00Z", "general traffic", 5.02, "shanghai"},
								{"2019-08-17T06:18:00Z", "general traffic", 3.57, "beijing"},
								{"2019-08-17T06:24:00Z", "general traffic", 5.052, "shanghai"},
								{"2019-08-17T06:24:00Z", "general traffic", 3.72, "beijing"},
								{"2019-08-17T06:30:00Z", "general traffic", 5.052, "shanghai"},
								{"2019-08-17T06:30:00Z", "general traffic", 3.881, "beijing"},
								{"2019-08-17T06:36:00Z", "general traffic", 5.105, "shanghai"},
								{"2019-08-17T06:36:00Z", "general traffic", 4.049, "beijing"},
								{"2019-08-17T06:42:00Z", "general traffic", 5.089, "shanghai"},
								{"2019-08-17T06:42:00Z", "general traffic", 4.209, "beijing"},
								{"2019-08-17T06:48:00Z", "general traffic", 5.066, "shanghai"},
								{"2019-08-17T06:48:00Z", "general traffic", 4.383, "beijing"},
								{"2019-08-17T06:54:00Z", "general traffic", 5.059, "shanghai"},
								{"2019-08-17T06:54:00Z", "general traffic", 4.56, "beijing"},
								{"2019-08-17T07:00:00Z", "general traffic", 5.033, "shanghai"},
								{"2019-08-17T07:00:00Z", "general traffic", 4.744, "beijing"},
								{"2019-08-17T07:06:00Z", "general traffic", 5.039, "shanghai"},
								{"2019-08-17T07:06:00Z", "general traffic", 4.915, "beijing"},
								{"2019-08-17T07:12:00Z", "general traffic", 5.059, "shanghai"},
								{"2019-08-17T07:12:00Z", "general traffic", 5.102, "beijing"},
								{"2019-08-17T07:18:00Z", "general traffic", 4.964, "shanghai"},
								{"2019-08-17T07:18:00Z", "general traffic", 5.289, "beijing"},
								{"2019-08-17T07:24:00Z", "general traffic", 5.007, "shanghai"},
								{"2019-08-17T07:24:00Z", "general traffic", 5.469, "beijing"},
								{"2019-08-17T07:30:00Z", "general traffic", 4.921, "shanghai"},
								{"2019-08-17T07:30:00Z", "general traffic", 5.643, "beijing"},
								{"2019-08-17T07:36:00Z", "general traffic", 4.875, "shanghai"},
								{"2019-08-17T07:36:00Z", "general traffic", 5.814, "beijing"},
								{"2019-08-17T07:42:00Z", "general traffic", 4.839, "shanghai"},
								{"2019-08-17T07:42:00Z", "general traffic", 5.974, "beijing"},
								{"2019-08-17T07:48:00Z", "general traffic", 4.8, "shanghai"},
								{"2019-08-17T07:48:00Z", "high traffic", 6.138, "beijing"},
								{"2019-08-17T07:54:00Z", "general traffic", 4.724, "shanghai"},
								{"2019-08-17T07:54:00Z", "high traffic", 6.293, "beijing"},
								{"2019-08-17T08:00:00Z", "general traffic", 4.547, "shanghai"},
								{"2019-08-17T08:00:00Z", "high traffic", 6.447, "beijing"},
								{"2019-08-17T08:06:00Z", "general traffic", 4.488, "shanghai"},
								{"2019-08-17T08:06:00Z", "high traffic", 6.601, "beijing"},
								{"2019-08-17T08:12:00Z", "general traffic", 4.508, "shanghai"},
								{"2019-08-17T08:12:00Z", "high traffic", 6.749, "beijing"},
								{"2019-08-17T08:18:00Z", "general traffic", 4.393, "shanghai"},
								{"2019-08-17T08:18:00Z", "high traffic", 6.893, "beijing"},
								{"2019-08-17T08:24:00Z", "general traffic", 4.334, "shanghai"},
								{"2019-08-17T08:24:00Z", "high traffic", 7.037, "beijing"},
								{"2019-08-17T08:30:00Z", "general traffic", 4.314, "shanghai"},
								{"2019-08-17T08:30:00Z", "high traffic", 7.172, "beijing"},
								{"2019-08-17T08:36:00Z", "general traffic", 4.173, "shanghai"},
								{"2019-08-17T08:36:00Z", "high traffic", 7.3, "beijing"},
								{"2019-08-17T08:42:00Z", "general traffic", 4.131, "shanghai"},
								{"2019-08-17T08:42:00Z", "high traffic", 7.428, "beijing"},
								{"2019-08-17T08:48:00Z", "general traffic", 3.996, "shanghai"},
								{"2019-08-17T08:48:00Z", "high traffic", 7.549, "beijing"},
								{"2019-08-17T08:54:00Z", "general traffic", 3.924, "shanghai"},
								{"2019-08-17T08:54:00Z", "high traffic", 7.667, "beijing"},
								{"2019-08-17T09:00:00Z", "general traffic", 3.93, "shanghai"},
								{"2019-08-17T09:00:00Z", "high traffic", 7.776, "beijing"},
								{"2019-08-17T09:06:00Z", "general traffic", 3.78, "shanghai"},
								{"2019-08-17T09:06:00Z", "high traffic", 7.874, "beijing"},
								{"2019-08-17T09:12:00Z", "general traffic", 3.773, "shanghai"},
								{"2019-08-17T09:12:00Z", "high traffic", 7.963, "beijing"},
								{"2019-08-17T09:18:00Z", "general traffic", 3.724, "shanghai"},
								{"2019-08-17T09:18:00Z", "high traffic", 8.045, "beijing"},
								{"2019-08-17T09:24:00Z", "general traffic", 3.556, "shanghai"},
								{"2019-08-17T09:24:00Z", "high traffic", 8.114, "beijing"},
								{"2019-08-17T09:30:00Z", "general traffic", 3.461, "shanghai"},
								{"2019-08-17T09:30:00Z", "high traffic", 8.166, "beijing"},
								{"2019-08-17T09:36:00Z", "general traffic", 3.373, "shanghai"},
								{"2019-08-17T09:36:00Z", "high traffic", 8.209, "beijing"},
								{"2019-08-17T09:42:00Z", "general traffic", 3.281, "shanghai"},
								{"2019-08-17T09:42:00Z", "high traffic", 8.238, "beijing"},
								{"2019-08-17T09:48:00Z", "general traffic", 3.126144146, "shanghai"},
								{"2019-08-17T09:48:00Z", "high traffic", 8.258, "beijing"},
								{"2019-08-17T09:54:00Z", "general traffic", 3.077, "shanghai"},
								{"2019-08-17T09:54:00Z", "high traffic", 8.271, "beijing"},
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
							Columns: []string{"time", "description", "human_traffic", "location"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", "high traffic", 8.12, "beijing"},
								{"2019-08-17T00:00:00Z", "less traffic", 2.064, "shanghai"},
								{"2019-08-17T00:06:00Z", "high traffic", 8.005, "beijing"},
								{"2019-08-17T00:06:00Z", "less traffic", 2.116, "shanghai"},
								{"2019-08-17T00:12:00Z", "high traffic", 7.887, "beijing"},
								{"2019-08-17T00:12:00Z", "less traffic", 2.028, "shanghai"},
								{"2019-08-17T00:18:00Z", "high traffic", 7.762, "beijing"},
								{"2019-08-17T00:18:00Z", "less traffic", 2.126, "shanghai"},
								{"2019-08-17T00:24:00Z", "high traffic", 7.635, "beijing"},
								{"2019-08-17T00:24:00Z", "less traffic", 2.041, "shanghai"},
								{"2019-08-17T00:30:00Z", "high traffic", 7.5, "beijing"},
								{"2019-08-17T00:30:00Z", "less traffic", 2.051, "shanghai"},
								{"2019-08-17T00:36:00Z", "high traffic", 7.372, "beijing"},
								{"2019-08-17T00:36:00Z", "less traffic", 2.067, "shanghai"},
								{"2019-08-17T00:42:00Z", "high traffic", 7.234, "beijing"},
								{"2019-08-17T00:42:00Z", "less traffic", 2.057, "shanghai"},
								{"2019-08-17T00:48:00Z", "high traffic", 7.11, "beijing"},
								{"2019-08-17T00:48:00Z", "less traffic", 1.991, "shanghai"},
								{"2019-08-17T00:54:00Z", "high traffic", 6.982, "beijing"},
								{"2019-08-17T00:54:00Z", "less traffic", 2.054, "shanghai"},
								{"2019-08-17T01:00:00Z", "high traffic", 6.837, "beijing"},
								{"2019-08-17T01:00:00Z", "less traffic", 2.018, "shanghai"},
								{"2019-08-17T01:06:00Z", "high traffic", 6.713, "beijing"},
								{"2019-08-17T01:06:00Z", "less traffic", 2.096, "shanghai"},
								{"2019-08-17T01:12:00Z", "high traffic", 6.578, "beijing"},
								{"2019-08-17T01:12:00Z", "less traffic", 2.1, "shanghai"},
								{"2019-08-17T01:18:00Z", "high traffic", 6.44, "beijing"},
								{"2019-08-17T01:18:00Z", "less traffic", 2.106, "shanghai"},
								{"2019-08-17T01:24:00Z", "high traffic", 6.299, "beijing"},
								{"2019-08-17T01:24:00Z", "less traffic", 2.126144146, "shanghai"},
								{"2019-08-17T01:30:00Z", "high traffic", 6.168, "beijing"},
								{"2019-08-17T01:30:00Z", "less traffic", 2.1, "shanghai"},
								{"2019-08-17T01:36:00Z", "high traffic", 6.024, "beijing"},
								{"2019-08-17T01:36:00Z", "less traffic", 2.136, "shanghai"},
								{"2019-08-17T01:42:00Z", "general traffic", 5.879, "beijing"},
								{"2019-08-17T01:42:00Z", "less traffic", 2.182, "shanghai"},
								{"2019-08-17T01:48:00Z", "general traffic", 5.745, "beijing"},
								{"2019-08-17T01:48:00Z", "less traffic", 2.306, "shanghai"},
								{"2019-08-17T01:54:00Z", "general traffic", 5.617, "beijing"},
								{"2019-08-17T01:54:00Z", "less traffic", 2.448, "shanghai"},
								{"2019-08-17T02:00:00Z", "general traffic", 5.472, "beijing"},
								{"2019-08-17T02:00:00Z", "less traffic", 2.464, "shanghai"},
								{"2019-08-17T02:06:00Z", "general traffic", 5.348, "beijing"},
								{"2019-08-17T02:06:00Z", "less traffic", 2.467, "shanghai"},
								{"2019-08-17T02:12:00Z", "general traffic", 5.2, "beijing"},
								{"2019-08-17T02:12:00Z", "less traffic", 2.516, "shanghai"},
								{"2019-08-17T02:18:00Z", "general traffic", 5.072, "beijing"},
								{"2019-08-17T02:18:00Z", "less traffic", 2.674, "shanghai"},
								{"2019-08-17T02:24:00Z", "general traffic", 4.934, "beijing"},
								{"2019-08-17T02:24:00Z", "less traffic", 2.684, "shanghai"},
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
							Columns: []string{"time", "description", "human_traffic", "location"},
							Values: []suite.Row{
								{"2019-08-17T00:00:00Z", "high traffic", 8.12, "beijing"},
								{"2019-08-17T00:06:00Z", "high traffic", 8.005, "beijing"},
								{"2019-08-17T09:18:00Z", "high traffic", 8.045, "beijing"},
								{"2019-08-17T09:24:00Z", "high traffic", 8.114, "beijing"},
								{"2019-08-17T09:30:00Z", "high traffic", 8.166, "beijing"},
								{"2019-08-17T09:36:00Z", "high traffic", 8.209, "beijing"},
								{"2019-08-17T09:42:00Z", "high traffic", 8.238, "beijing"},
								{"2019-08-17T09:48:00Z", "high traffic", 8.258, "beijing"},
								{"2019-08-17T09:54:00Z", "high traffic", 8.271, "beijing"},
								{"2019-08-17T10:00:00Z", "high traffic", 8.281, "beijing"},
								{"2019-08-17T10:06:00Z", "high traffic", 8.284, "beijing"},
								{"2019-08-17T10:12:00Z", "high traffic", 8.291, "beijing"},
								{"2019-08-17T10:18:00Z", "high traffic", 8.297, "beijing"},
								{"2019-08-17T10:24:00Z", "high traffic", 8.297, "beijing"},
								{"2019-08-17T10:30:00Z", "high traffic", 8.287, "beijing"},
								{"2019-08-17T10:36:00Z", "high traffic", 8.287, "beijing"},
								{"2019-08-17T10:42:00Z", "high traffic", 8.278, "beijing"},
								{"2019-08-17T10:48:00Z", "high traffic", 8.258, "beijing"},
								{"2019-08-17T10:54:00Z", "high traffic", 8.222, "beijing"},
								{"2019-08-17T11:00:00Z", "high traffic", 8.182, "beijing"},
								{"2019-08-17T11:06:00Z", "high traffic", 8.133, "beijing"},
								{"2019-08-17T11:12:00Z", "high traffic", 8.061, "beijing"},
								{"2019-08-17T21:42:00Z", "high traffic", 8.022, "beijing"},
								{"2019-08-17T21:48:00Z", "high traffic", 8.123, "beijing"},
								{"2019-08-17T21:54:00Z", "high traffic", 8.215, "beijing"},
								{"2019-08-17T22:00:00Z", "high traffic", 8.294, "beijing"},
								{"2019-08-17T22:06:00Z", "high traffic", 8.373, "beijing"},
								{"2019-08-17T22:12:00Z", "high traffic", 8.435, "beijing"},
								{"2019-08-17T22:18:00Z", "high traffic", 8.488, "beijing"},
								{"2019-08-17T22:24:00Z", "high traffic", 8.527, "beijing"},
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
							Columns: []string{"time", "description", "human_traffic", "location"},
							Values: []suite.Row{
								{"2019-08-24T06:12:00Z", "less traffic", 1.424, "shanghai"},
								{"2019-08-24T06:18:00Z", "less traffic", 1.437, "shanghai"},
								{"2019-08-24T06:24:00Z", "less traffic", 1.375, "shanghai"},
								{"2019-08-24T06:30:00Z", "less traffic", 1.289, "shanghai"},
								{"2019-08-24T06:36:00Z", "less traffic", 1.161, "shanghai"},
								{"2019-08-24T06:42:00Z", "less traffic", 1.138, "shanghai"},
								{"2019-08-24T06:48:00Z", "less traffic", 1.102, "shanghai"},
								{"2019-08-24T06:54:00Z", "less traffic", 1.043, "shanghai"},
								{"2019-08-24T07:00:00Z", "less traffic", 0.984, "shanghai"},
								{"2019-08-24T07:06:00Z", "less traffic", 0.896, "shanghai"},
								{"2019-08-24T07:12:00Z", "less traffic", 0.86, "shanghai"},
								{"2019-08-24T07:18:00Z", "less traffic", 0.807, "shanghai"},
								{"2019-08-24T07:24:00Z", "less traffic", 0.81, "shanghai"},
								{"2019-08-24T07:30:00Z", "less traffic", 0.807, "shanghai"},
								{"2019-08-24T07:36:00Z", "less traffic", 0.797, "shanghai"},
								{"2019-08-24T07:42:00Z", "less traffic", 0.715, "shanghai"},
								{"2019-08-24T07:48:00Z", "less traffic", 0.676, "shanghai"},
								{"2019-08-24T07:54:00Z", "less traffic", 0.712, "shanghai"},
								{"2019-08-24T08:00:00Z", "less traffic", 0.781, "shanghai"},
								{"2019-08-24T08:06:00Z", "less traffic", 0.745, "shanghai"},
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
							Columns: []string{"time", "description", "human_traffic", "location"},
							Values: []suite.Row{
								{"2019-08-28T07:06:00Z", "very high traffic", 9.902, "beijing"},
								{"2019-08-28T07:12:00Z", "very high traffic", 9.938, "beijing"},
								{"2019-08-28T07:18:00Z", "very high traffic", 9.957, "beijing"},
								{"2019-08-28T07:24:00Z", "very high traffic", 9.964, "beijing"},
								{"2019-08-28T07:30:00Z", "very high traffic", 9.954, "beijing"},
								{"2019-08-28T07:36:00Z", "very high traffic", 9.941, "beijing"},
								{"2019-08-28T07:42:00Z", "very high traffic", 9.925, "beijing"},
								{"2019-08-28T07:48:00Z", "very high traffic", 9.902, "beijing"},
								{"2019-09-01T23:30:00Z", "very high traffic", 9.902, "beijing"},
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
								{"1970-01-01T00:00:00Z", 3.5307120942458807},
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
								{"1970-01-01T00:00:00Z", 49.132712456344585},
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
								{"1970-01-01T00:00:00Z", 50.2937984496124},
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
								{"1970-01-01T00:00:00Z", 49.132712456344585},
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
								{"1970-01-01T00:00:00Z", 50.2937984496124},
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
								{"2019-08-17T00:00:00Z", "high traffic"},
								{"2019-08-17T00:00:00Z", "less traffic"},
								{"2019-08-17T00:06:00Z", "high traffic"},
								{"2019-08-17T00:06:00Z", "less traffic"},
								{"2019-08-17T00:12:00Z", "high traffic"},
								{"2019-08-17T00:12:00Z", "less traffic"},
								{"2019-08-17T00:18:00Z", "high traffic"},
								{"2019-08-17T00:18:00Z", "less traffic"},
								{"2019-08-17T00:24:00Z", "high traffic"},
								{"2019-08-17T00:24:00Z", "less traffic"},
								{"2019-08-17T00:30:00Z", "high traffic"},
								{"2019-08-17T00:30:00Z", "less traffic"},
								{"2019-08-17T00:36:00Z", "high traffic"},
								{"2019-08-17T00:36:00Z", "less traffic"},
								{"2019-08-17T00:42:00Z", "high traffic"},
								{"2019-08-17T00:42:00Z", "less traffic"},
								{"2019-08-17T00:48:00Z", "high traffic"},
								{"2019-08-17T00:48:00Z", "less traffic"},
								{"2019-08-17T00:54:00Z", "high traffic"},
								{"2019-08-17T00:54:00Z", "less traffic"},
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
								{"1970-01-01T00:00:00Z", 3.5307120942458807},
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
								{"air_quality"},
								{"avg_temperature"},
								{"information"},
								{"max_temperature"},
								{"recommended"},
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
		Query: fmt.Sprintf(`SHOW MEASUREMENTS ON "%s" WITH MEASUREMENT =~ /temp/ LIMIT 2 `, db),
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
								{"max_temperature"},
							},
						},
					},
				},
			},
		},
	},
	{
		Name:  "show_measurements_on_with",
		Query: fmt.Sprintf(`SHOW MEASUREMENTS ON "%s" WITH MEASUREMENT =~ /air.*/ WHERE "air_level"  =~ /d/`, db),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series:      []suite.Series{},
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
							Name:    "air_quality",
							Columns: []string{"fieldKey", "fieldType"},
							Values: []suite.Row{
								{"air_score", "float"},
							},
						},
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
							Name:    "max_temperature",
							Columns: []string{"fieldKey", "fieldType"},
							Values: []suite.Row{
								{"degree", "float"},
							},
						},
						{
							Name:    "recommended",
							Columns: []string{"fieldKey", "fieldType"},
							Values: []suite.Row{
								{"rec_level", "float"},
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
							Name:    "air_quality",
							Columns: []string{"tagKey"},
							Values: []suite.Row{
								{"air_level"},
								{"location"},
							},
						},
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
							Name:    "max_temperature",
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
								{"location"},
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
								{"air_quality,air_level=1,location=beijing"},
								{"air_quality,air_level=2,location=beijing"},
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
								{"1970-01-01T00:00:00Z", "high traffic"},
								{"1970-01-01T00:00:00Z", "less traffic"},
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
								{"2019-09-16T00:24:00Z", 1.854, "shanghai", "less traffic"},
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
								{"2019-08-17T00:00:00Z", "less traffic"},
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
								{"1970-01-01T00:00:00Z", "less traffic", 8.12},
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
								{"2019-08-17T00:00:00Z", "less traffic", "shanghai", 2.064},
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
								{"2019-08-24T10:18:00Z", 1.122, "beijing", "less traffic"},
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
		Query: fmt.Sprintf(`SELECT MEDIAN("human_traffic") AS "med_hum",MODE("human_traffic") AS "mode_hum" FROM "%s"."%s"."information" limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{
				{
					StatementId: 0,
					Series: []suite.Series{
						{
							Name:    "information",
							Columns: []string{"time", "med_hum", "mode_hum"},
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
								{"1970-01-01T00:00:00Z", "less traffic", 8.12},
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
								{"2019-08-23T06:00:00Z", "general traffic", 5.564},
								{"2019-08-23T06:00:00Z", "less traffic", 1.594},
								{"2019-08-23T06:06:00Z", "general traffic", 5.43},
								{"2019-08-23T06:06:00Z", "less traffic", 1.545},
								{"2019-08-23T06:12:00Z", "general traffic", 5.308},
								{"2019-08-23T06:12:00Z", "less traffic", 1.526},
								{"2019-08-23T06:18:00Z", "general traffic", 5.18},
								{"2019-08-23T06:18:00Z", "less traffic", 1.457},
								{"2019-08-23T06:24:00Z", "general traffic", 5.052},
								{"2019-08-23T06:24:00Z", "less traffic", 1.414},
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
}
