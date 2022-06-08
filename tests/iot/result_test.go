package iot

import (
	"encoding/json"
	"testing"
)

func TestResults_Marshal(t *testing.T) {
	res := Results{}
	data := []byte(`{"results":[{"statement_id":0,"series":[{"name":"readings","columns":["time","device_version","driver","elevation","fleet","fuel_consumption","grade","heading","latitude","longitude","model","name","velocity"],"values":[["2020-01-01T00:20:00Z","v1.0","Rodney",484,"South",2.8,2,88,28.07564,179.53388,"H-2","truck_1",0],["2020-01-01T00:20:00Z","v1.0","Trish",423,"East",7.3,0,154,63.05325,121.79727,"H-2","truck_7",0],["2020-01-01T00:20:00Z","v1.0","Trish",371,"West",12.4,0,255,79.05027,89.01063,"G-2000","truck_4",4],["2020-01-01T00:20:00Z","v1.5","Albert",435,"West",7,0,144,21.54052,131.83325,"G-2000","truck_5",0],["2020-01-01T00:20:00Z","v2.0","Derek",315,"West",13.5,0,246,37.12601,82.44311,"G-2000","truck_6",2],["2020-01-01T00:20:00Z","v2.0","Derek",342,"East",5.3,0,330,42.91851,179.34986,"G-2000","truck_2",4],["2020-01-01T00:20:00Z","v2.0","Derek",263,"East",22.1,0,272,78.64091,29.67632,"G-2000","truck_8",0],["2020-01-01T00:20:00Z","v2.3","Albert",456,"East",9.9,0,173,64.14688,3.16587,"H-2","truck_3",0],["2020-01-01T00:20:00Z","v2.3","Rodney",371,"West",8.4,0,326,18.10829,139.47116,"H-2","truck_0",0]]}]}]}`)
	if err := json.Unmarshal(data, &res); err != nil {
		t.Error(err)
	}
	if b, err := json.Marshal(res); err != nil {
		t.Error(err)
	} else {
		data2 := []byte(`{"results":[{"statement_id":0,"series":[{"name":"readings","columns":["time","device_version","driver","elevation","fleet","fuel_consumption","grade","heading","latitude","longitude","model","name","velocity"],"values":[["2020-01-01T00:20:00Z","v1.0","Rodney",484,"South",2.8,2,88,28.07564,179.53388,"H-2","truck_1",0],["2020-01-01T00:20:00Z","v1.0","Trish",423,"East",7.3,0,154,63.05325,121.79727,"H-2","truck_7",0],["2020-01-01T00:20:00Z","v1.0","Trish",371,"West",12.4,0,255,79.05027,89.01063,"G-2000","truck_4",4],["2020-01-01T00:20:00Z","v1.5","Albert",435,"West",7,0,144,21.54052,131.83325,"G-2000","truck_5",0],["2020-01-01T00:20:00Z","v2.0","Derek",315,"West",13.5,0,246,37.12601,82.44311,"G-2000","truck_6",2],["2020-01-01T00:20:00Z","v2.0","Derek",342,"East",5.3,0,330,42.91851,179.34986,"G-2000","truck_2",4],["2020-01-01T00:20:00Z","v2.0","Derek",263,"East",22.1,0,272,78.64091,29.67632,"G-2000","truck_8",0],["2020-01-01T00:20:00Z","v2.3","Albert",456,"East",9.9,0,173,64.14688,3.16587,"H-2","truck_3",0],["2020-01-01T00:20:00Z","v2.3","Rodney",371,"West",8.4,0,326,18.10829,139.47116,"H-2","truck_0",0]]}]}]`)
		AssertEqual(t, string(b), string(data))
		AssertNotEqual(t, string(b), string(data2))
	}
}
