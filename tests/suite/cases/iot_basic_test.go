package cases

import (
	"encoding/json"
	"fmt"
	"github.com/cnosdb/cnosdb/tests/suite"
	iot2 "github.com/cnosdb/cnosdb/tests/suite/iot"
	"testing"
	"time"
)

func TestBaseWrite(t *testing.T) {
	g := iot2.Generator{
		Server:   server,
		Parallel: 10,
		Scale:    10,
		Seed:     123,
		Interval: 20 * time.Minute,
		Start:    time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		End:      time.Date(2020, 2, 1, 0, 0, 0, 0, time.UTC),
	}
	g.Init()
	g.Run()
	r := suite.Results{
		Results: []suite.Result{
			{
				StatementId: 0,
				Series: []suite.Series{
					{
						Name: "readings",
						Columns: []string{"time", "device_version", "driver", "elevation", "fleet", "fuel_consumption",
							"grade", "heading", "latitude", "longitude", "model", "name", "velocity"},
						Values: []suite.Row{
							{"2020-01-01T00:20:00Z", "v1.0", "Rodney", 484, "South", 2.8, 2, 88, 28.07564, 179.53388, "H-2", "truck_1", 0},
							{"2020-01-01T00:20:00Z", "v1.0", "Trish", 423, "East", 7.3, 0, 154, 63.05325, 121.79727, "H-2", "truck_7", 0},
							{"2020-01-01T00:20:00Z", "v1.0", "Trish", 371, "West", 12.4, 0, 255, 79.05027, 89.01063, "G-2000", "truck_4", 4},
							{"2020-01-01T00:20:00Z", "v1.5", "Albert", 435, "West", 7, 0, 144, 21.54052, 131.83325, "G-2000", "truck_5", 0},
							{"2020-01-01T00:20:00Z", "v2.0", "Derek", 315, "West", 13.5, 0, 246, 37.12601, 82.44311, "G-2000", "truck_6", 2},
							// 5
							{"2020-01-01T00:20:00Z", "v2.0", "Derek", 342, "East", 5.3, 0, 330, 42.91851, 179.34986, "G-2000", "truck_2", 4},
							{"2020-01-01T00:20:00Z", "v2.0", "Derek", 263, "East", 22.1, 0, 272, 78.64091, 29.67632, "G-2000", "truck_8", 0},
							{"2020-01-01T00:20:00Z", "v2.3", "Albert", 456, "East", 9.9, 0, 173, 64.14688, 3.16587, "H-2", "truck_3", 0},
							{"2020-01-01T00:20:00Z", "v2.3", "Rodney", 371, "West", 8.4, 0, 326, 18.10829, 139.47116, "H-2", "truck_0", 0},
						},
					},
				},
			},
		},
	}
	d := suite.Results{
		Results: []suite.Result{
			{
				StatementId: 0,
				Series: []suite.Series{
					{
						Name: "diagnostics",
						Columns: []string{"time", "current_load", "device_version", "driver", "fleet", "fuel_capacity",
							"fuel_state", "load_capacity", "model", "name", "nominal_fuel_consumption", "status"},
						Values: []suite.Row{
							{"2020-01-01T00:20:00Z", 335, "v1.0", "Rodney", "South", 150, 0.9, 1500, "H-2", "truck_1", 12, 0},
							{"2020-01-01T00:20:00Z", 1495, "v2.3", "Rodney", "West", 150, 0.9, 1500, "H-2", "truck_0", 12, 0},
							{"2020-01-01T00:20:00Z", 3524, "v2.3", "Albert", "East", 150, 0.9, 1500, "H-2", "truck_3", 12, 0},
							{"2020-01-01T00:20:00Z", 65, "v2.0", "Derek", "West", 300, 0.9, 5000, "G-2000", "truck_6", 19, 0},
							{"2020-01-01T00:20:00Z", 2139, "v2.0", "Derek", "East", 300, 0.9, 5000, "G-2000", "truck_8", 19, 0},
							// 5
							{"2020-01-01T00:20:00Z", 4663, "v2.0", "Derek", "East", 300, 0.9, 5000, "G-2000", "truck_2", 19, 0},
							{"2020-01-01T00:20:00Z", 1224, "v1.5", "Albert", "West", 300, 0.9, 5000, "G-2000", "truck_5", 19, 0},
							{"2020-01-01T00:20:00Z", 2364, "v1.0", "Trish", "West", 300, 0.9, 5000, "G-2000", "truck_4", 19, 0},
							{"2020-01-01T00:20:00Z", 3298, "v1.0", "Trish", "East", 150, 0.9, 1500, "H-2", "truck_7", 12, 0},
						},
					},
				},
			},
		},
	}

	res, _ := server.Query(`select * from db0.."readings" where time='2020-01-01T00:20:00Z'`)
	fmt.Println(res)
	rr := suite.Results{}
	if err := json.Unmarshal([]byte(res), &rr); err != nil {
		t.Error(err)
	}
	rr.AssertEqual(t, r)

	res, _ = server.Query(`select * from db0.."diagnostics" where time='2020-01-01T00:20:00Z'`)
	fmt.Println(res)
	dd := suite.Results{}
	if err := json.Unmarshal([]byte(res), &dd); err != nil {
		t.Error(err)
	}
	dd.AssertEqual(t, d)
}
