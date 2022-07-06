package traveling_info

import (
	"fmt"
	"github.com/cnosdb/cnosdb/tests/suite"
)

var cases_sample = []suite.Step{
	{
		//Values[0] time: 2019-08-20T15:24:00Z!=2019-08-17T23:06:00Z
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
								{"2019-08-20T15:24:00Z", "less traffic", nil},
								{"2019-08-31T06:54:00Z", nil, 4.324},
								{"2019-09-05T16:36:00Z", "high traffic", nil},
								{"2019-09-13T01:42:00Z", nil, 3.698},
							},
						},
					},
				},
			},
		},
	},
	{
		//Values[0] time: 2019-08-30T20:06:00Z!=2019-08-24T09:00:00Z
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
								{"2019-08-30T20:06:00Z", 4.941},
								{"2019-09-03T17:06:00Z", 4.042},
							},
						},
					},
				},
			},
		},
	},
	{
		//Values[0] time: 2019-08-30T01:00:00Z!=2019-09-04T23:30:00Z
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
								{"2019-08-30T01:00:00Z", 4.97, "beijing", "general traffic"},
								{"2019-09-02T08:24:00Z", 4.639, "shanghai", "general traffic"},
							},
						},
					},
				},
			},
		},
	},
}
