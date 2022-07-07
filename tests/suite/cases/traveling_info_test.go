package cases

import (
	"fmt"
	"github.com/cnosdb/cnosdb/tests/suite"
	"github.com/cnosdb/cnosdb/tests/suite/traveling_info"
	"testing"
)

func TestTravelingInfo(t *testing.T) {
	n := traveling_info.TravelingInfo{S: server, T: t}
	n.Load()
	n.Test()
}

const (
	db = "traveling_info_database"
	rp = "rp0"
)

func TestGenCode_1(t *testing.T) {

	n := traveling_info.TravelingInfo{S: server, T: t}
	n.Load()
	s := suite.Step{
		Name:  "h2o_feet_level_description_9_feet",
		Query: fmt.Sprintf(`SELECT "level description" FROM "%s"."%s"."h2o_feet" WHERE "level description" = 'at or greater than 9 feet' limit 20`, db, rp),
		Result: suite.Results{
			Results: []suite.Result{},
		},
	}
	s.ResCode("", server)
}

func TestGenCode_n(t *testing.T) {

	n := traveling_info.TravelingInfo{S: server, T: t}
	n.Load()
	var steps = [...]suite.Step{
		{
			Name:  "information_all_fields",
			Query: fmt.Sprintf(`SELECT *::field FROM "%s"."%s".information LIMIT 10 OFFSET 3000`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
		{
			Name:  "information_location_shanghai_human_traffic",
			Query: fmt.Sprintf(`SELECT "human_traffic" FROM "%s"."%s"."information" WHERE "location" <> 'shanghai' AND (human_traffic < -0.59 OR human_traffic > 9.95) limit 20`, db, rp),
			Result: suite.Results{
				Results: []suite.Result{},
			},
		},
	}
	var i int
	for i = 0; i < len(steps); i++ {
		fmt.Printf(steps[i].Name)
		fmt.Printf("\n")
		steps[i].ResCode("", server)
	}
}
