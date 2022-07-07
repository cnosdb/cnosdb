package traveling_info

import (
	"fmt"
	"github.com/cnosdb/cnosdb/tests/suite"
)

var cases_write = []suite.Step{
	//Basic Syntax
	{
		Name:  "basic_syntax_cq_basic",
		Query: fmt.Sprintf(`CREATE CONTINUOUS QUERY "cq_basic" ON "transportation" BEGIN SELECT mean("passengers") INTO "average_passengers" FROM "%s"."%s"."bus_data" GROUP BY time(1h) END`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "basic_syntax_cq_basic_average_passengers",
		Query: fmt.Sprintf(`SELECT * FROM "%s"."%s"."average_passengers"`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "basic_syntax_cq_basic_rp",
		Query: fmt.Sprintf(`CREATE CONTINUOUS QUERY "cq_basic_rp" ON "transportation" BEGIN  SELECT mean("passengers") INTO "transportation"."three_weeks"."average_passengers" FROM "%s"."%s"."bus_data" GROUP BY time(1h) END`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "basic_syntax_trans_three_weeks_average_passengers",
		Query: fmt.Sprintf(`SELECT * FROM "transportation"."three_weeks"."average_passengers"`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "basic_syntax_cq_basic_br",
		Query: fmt.Sprintf(`CREATE CONTINUOUS QUERY "cq_basic_br" ON "transportation" BEGIN SELECT mean(*) INTO "downsampled_transportation"."autogen".:MEASUREMENT FROM /.*/ GROUP BY time(30m),* END`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "basic_syntax_down_trans_autogen_bus_data",
		Query: fmt.Sprintf(`SELECT * FROM "downsampled_transportation.""autogen.""bus_data`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "basic_syntax_cq_basic_offset",
		Query: fmt.Sprintf(`CREATE CONTINUOUS QUERY "cq_basic_offset" ON "transportation" BEGIN SELECT mean("passengers") INTO "average_passengers" FROM "%s"."%s"."bus_data" GROUP BY time(1h,15m) END`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "basic_syntax_cq_basic_offset_average_passengers",
		Query: fmt.Sprintf(`SELECT * FROM "%s"."%s"."average_passengers"`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	//Advanced Syntax
	{
		Name:  "adv_syntax_cq_advanced_every",
		Query: fmt.Sprintf(`CREATE CONTINUOUS QUERY "cq_advanced_every" ON "transportation" RESAMPLE EVERY 30m BEGIN SELECT mean("passengers") INTO "average_passengers" FROM "%s"."%s"."bus_data" GROUP BY time(1h) END`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "adv_syntax_advanced_every_select",
		Query: fmt.Sprintf(`SELECT * FROM "%s"."%s"."average_passengers"`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "adv_syntax_cq_advanced_for",
		Query: fmt.Sprintf(`CREATE CONTINUOUS QUERY "cq_advanced_for" ON "transportation" RESAMPLE FOR 1h BEGIN SELECT mean("passengers") INTO "average_passengers" FROM "%s"."%s"."bus_data" GROUP BY time(30m) END`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "adv_syntax_cq_advanced_for_select",
		Query: fmt.Sprintf(`SELECT * FROM "%s"."%s"."average_passengers"`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "adv_syntax_cq_advanced_every_for",
		Query: fmt.Sprintf(`CREATE CONTINUOUS QUERY "cq_advanced_every_for" ON "transportation" RESAMPLE EVERY 1h FOR 90m BEGIN SELECT mean("passengers") INTO "average_passengers" FROM "%s"."%s"."bus_data" GROUP BY time(30m) END`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "adv_syntax_cq_advanced_every_for_select",
		Query: fmt.Sprintf(`SELECT * FROM "%s"."%s"."average_passengers"`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "adv_syntax_cq_advanced_for_fill",
		Query: fmt.Sprintf(`CREATE CONTINUOUS QUERY "cq_advanced_for_fill" ON "transportation" RESAMPLE FOR 2h BEGIN SELECT mean("passengers") INTO "average_passengers" FROM "%s"."%s"."bus_data" GROUP BY time(1h) fill(1000) END`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "adv_syntax_cq_advanced_for_fill_select",
		Query: fmt.Sprintf(`SELECT * FROM "%s"."%s"."average_passengers"`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	//CQ Management
	{
		Name:  "cq_show_continuous_queries",
		Query: fmt.Sprintf(`SHOW CONTINUOUS QUERIES`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "cq_drop_idle_hands_telegraf",
		Query: fmt.Sprintf(`DROP CONTINUOUS QUERY "idle_hands" ON "telegraf"`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "cq_select_mean_bees_30m_20",
		Query: fmt.Sprintf(`SELECT mean("bees") FROM "%s"."%s"."farm" GROUP BY time(30m) HAVING mean("bees") > 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "cq_create_bee_cq",
		Query: fmt.Sprintf(`CREATE CONTINUOUS QUERY "bee_cq" ON "mydb" BEGIN SELECT mean("bees") AS "mean_bees" INTO "aggregate_bees" FROM "%s"."%s"."farm" GROUP BY time(30m) END`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "cq_select_mean_bees_20",
		Query: fmt.Sprintf(`SELECT "mean_bees" FROM "%s"."%s"."aggregate_bees" WHERE "mean_bees" > 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "cq_select_mean_count_bees_30m",
		Query: fmt.Sprintf(`SELECT mean(count("bees")) FROM "%s"."%s"."farm" GROUP BY time(30m)`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "cq_bee_cq_30m",
		Query: fmt.Sprintf(`CREATE CONTINUOUS QUERY "bee_cq" ON "mydb" BEGIN SELECT count("bees") AS "count_bees" INTO "aggregate_bees" FROM "%s"."%s"."farm" GROUP BY time(30m) END`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "cq_select_mean_count_bees",
		Query: fmt.Sprintf(""),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
}
