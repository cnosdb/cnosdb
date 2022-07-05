package traveling_info

import (
	"fmt"
	"github.com/cnosdb/cnosdb/tests/suite"
)

var cases_math_gen = []suite.Step{
	//Addition
	{
		Name:  "math_add_a_5",
		Query: fmt.Sprintf(`SELECT "A" + 5 FROM "%s"."%s"."add"`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "math_add_a_5_10",
		Query: fmt.Sprintf(`SELECT * FROM "add" WHERE "A" + 5 > 10`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "math_a_b",
		Query: fmt.Sprintf(`SELECT "A" + "B" FROM "add"`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "math_a_b_10",
		Query: fmt.Sprintf(`SELECT * FROM "add" WHERE "A" + "B" >= 10`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	//Subtraction
	{
		Name:  "math_sub_1_a",
		Query: fmt.Sprintf(`SELECT 1 - "A" FROM "sub"`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "math_sub_1_a_3",
		Query: fmt.Sprintf(`SELECT * FROM "sub" WHERE 1 - "A" <= 3`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "math_sub_a_b",
		Query: fmt.Sprintf(`SELECT "A" - "B" FROM "sub"`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "math_sub_a_b_1",
		Query: fmt.Sprintf(`SELECT * FROM "sub" WHERE "A" - "B" <= 1`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	//Multiplication
	{
		Name:  "math_mult_10_a",
		Query: fmt.Sprintf(`SELECT 10 * "A" FROM "mult"`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "math_mult_a_10_20",
		Query: fmt.Sprintf(`SELECT * FROM "mult" WHERE "A" * 10 >= 20`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "math_mult_a_b_c",
		Query: fmt.Sprintf(`SELECT "A" * "B" * "C" FROM "mult"`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "math_mult_a_b_80",
		Query: fmt.Sprintf(`SELECT * FROM "mult" WHERE "A" * "B" <= 80`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "math_mult_add",
		Query: fmt.Sprintf(`SELECT 10 * ("A" + "B" + "C") FROM "mult"`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "math_mult_sub",
		Query: fmt.Sprintf(`SELECT 10 * ("A" - "B" - "C") FROM "mult"`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "math_mult_add_sub",
		Query: fmt.Sprintf(`SELECT 10 * ("A" + "B" - "C") FROM "mult"`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	//Division
	{
		Name:  "math_div_10_a",
		Query: fmt.Sprintf(`SELECT 10 / "A" FROM "div"`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "math_div_a_10_2",
		Query: fmt.Sprintf(`SELECT * FROM "div" WHERE "A" / 10 <= 2`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "math_div_a_b",
		Query: fmt.Sprintf(`SELECT "A" / "B" FROM "div"`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "math_div_a_b_10",
		Query: fmt.Sprintf(`SELECT * FROM "div" WHERE "A" / "B" >= 10`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "math_div_add_mult",
		Query: fmt.Sprintf(`SELECT 10 / ("A" + "B" + "C") FROM "mult"`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	//Modulo
	{
		Name:  "math_mod_b_2",
		Query: fmt.Sprintf(`SELECT "B" % 2 FROM "modulo"`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "math_mod_b_2_0",
		Query: fmt.Sprintf(`SELECT "B" FROM "modulo" WHERE "B" % 2 = 0`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "math_mod_a_b",
		Query: fmt.Sprintf(`SELECT "A" % "B" FROM "modulo"`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "math_mod_a_b_0",
		Query: fmt.Sprintf(`SELECT "A" FROM "modulo" WHERE "A" % "B" = 0`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	//Bitwise AND
	{
		Name:  "math_and_a_255",
		Query: fmt.Sprintf(`SELECT "A" & 255 FROM "bitfields"`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "math_and_a_b_bit",
		Query: fmt.Sprintf(`SELECT "A" & "B" FROM "bitfields"`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "math_and_15_0",
		Query: fmt.Sprintf(`SELECT * FROM "data" WHERE "bitfield" & 15 > 0`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "math_and_a_b_bool",
		Query: fmt.Sprintf(`SELECT "A" & "B" FROM "booleans"`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "math_and_a_true_b_bool",
		Query: fmt.Sprintf(`SELECT ("A" ^ true) & "B" FROM "booleans"`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	//Bitwise OR
	{
		Name:  "math_or_a_5",
		Query: fmt.Sprintf(`SELECT "A" | 5 FROM "bitfields"`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "math_or_a_b",
		Query: fmt.Sprintf(`SELECT "A" | "B" FROM "bitfields"`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "math_or_12_12",
		Query: fmt.Sprintf(`SELECT * FROM "data" WHERE "bitfield" | 12 = 12`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	//Bitwise Exclusive-OR
	{
		Name:  "math_xor_a_255",
		Query: fmt.Sprintf(`SELECT "A" ^ 255 FROM "bitfields"`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "math_xor_a_b",
		Query: fmt.Sprintf(`SELECT "A" ^ "B" FROM "bitfields"`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	{
		Name:  "math_xor_bit_6_0",
		Query: fmt.Sprintf(`SELECT * FROM "data" WHERE "bitfield" ^ 6 > 0`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
	//other
	{
		Name:  "math_fun_mean",
		Query: fmt.Sprintf(`SELECT 10 * mean("value") FROM "cpu"`),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	},
}
