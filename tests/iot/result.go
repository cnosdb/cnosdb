package iot

import (
	"fmt"
	"math"
	"testing"
)

type Row []interface{}

func (r Row) Equal(columns []string, a Row, num int) bool {
	if len(columns) != len(a) {
		panic("Should be equal")
	}
	if len(r) != len(a) {
		fmt.Printf("len(Values[%d]): %d!=%d \n", num, len(r), len(a))
		return false
	}
	for i := 0; i < len(r); i++ {
		switch columns[i] {
		case "time", "device_version", "driver", "fleet", "model", "name":
			// string
			x := r[i].(string)
			y := a[i].(string)
			if x != y {
				fmt.Printf("Values[%d] %s: %s!=%s \n", i, columns[i], x, y)
				return false
			}
		case
			/* Readings */
			"latitude", "longitude", "elevation", "velocity",
			"heading", "grade", "fuel_consumption",
			/* Diagnostics */
			"load_capacity", "fuel_capacity", "nominal_fuel_consumption",
			"current_load", "fuel_state", "status":
			x := toFloat64(r[i])
			y := toFloat64(a[i])
			if math.Abs(x-y) > 0.000001 {
				fmt.Printf("Values[%d] %s: %f!=%f \n", i, columns[i], x, y)
			}
		default:
			panic(columns[i])
		}
	}
	return true
}

type Series struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Values  []Row    `json:"values"`
}

type Result struct {
	StatementId int      `json:"statement_id"`
	Series      []Series `json:"series"`
}

type Results struct {
	Results []Result `json:"results"`
}

func (r *Results) Equal(a Results) bool {
	if len(r.Results) != len(a.Results) {
		fmt.Printf("len(Results): %d!=%d \n", len(r.Results), len(a.Results))
		return false
	}
	for i := 0; i < len(r.Results); i++ {
		if r.Results[i].StatementId != a.Results[i].StatementId {
			fmt.Printf("StatementId: %d!=%d \n", r.Results[i].StatementId, a.Results[i].StatementId)
			return false
		}
		if len(r.Results[i].Series) != len(a.Results[i].Series) {
			fmt.Printf("len(Series): %d!=%d \n", len(r.Results[i].Series), len(a.Results[i].Series))
			return false
		}
		for j := 0; j < len(r.Results[i].Series); j++ {
			// Name
			if r.Results[i].Series[j].Name != a.Results[i].Series[j].Name {
				fmt.Printf("Name: %s!=%s \n", r.Results[i].Series[j].Name, a.Results[i].Series[j].Name)
				return false
			}
			// Columns
			if len(r.Results[i].Series[j].Columns) != len(a.Results[i].Series[j].Columns) {
				fmt.Printf("len(Columns): %d!=%d \n",
					len(r.Results[i].Series[j].Columns), len(a.Results[i].Series[j].Columns))
				return false
			}
			for k := 0; k < len(r.Results[i].Series[j].Columns); k++ {
				if r.Results[i].Series[j].Columns[k] != a.Results[i].Series[j].Columns[k] {
					fmt.Printf("Columns[%d]: %s!=%s \n", k,
						r.Results[i].Series[j].Columns[k], a.Results[i].Series[j].Columns[k])
					return false
				}
			}
			// Values
			if len(r.Results[i].Series[j].Values) != len(a.Results[i].Series[j].Values) {
				fmt.Printf("len(Values): %d!=%d \n",
					len(r.Results[i].Series[j].Values), len(a.Results[i].Series[j].Values))
				return false
			}
			for l := 0; l < len(r.Results[i].Series[j].Values); l++ {
				if len(r.Results[i].Series[j].Values[l]) != len(a.Results[i].Series[j].Values[l]) {
					fmt.Printf("len(Values[%d]): %d!=%d \n", l,
						len(r.Results[i].Series[j].Values[l]), len(a.Results[i].Series[j].Values[l]))
					return false
				}
				x := r.Results[i].Series[j].Values[l]
				y := a.Results[i].Series[j].Values[l]
				if !x.Equal(r.Results[i].Series[j].Columns, y, l) {
					return false
				}
			}
		}
	}
	return true
}

func (r *Results) AssertEqual(t *testing.T, a Results) {
	if !r.Equal(a) {
		fmt.Print("A: ")
		fmt.Println(r)
		fmt.Print("B: ")
		fmt.Println(a)
		t.Error("A should be equal to B.")
	}
}

func (r *Results) AssertNotEqual(t *testing.T, a Results) {
	if r.Equal(a) {
		fmt.Print("A: ")
		fmt.Println(r)
		fmt.Print("B: ")
		fmt.Println(a)
		t.Error("A should be not equal to B.")
	}
}

func toFloat64(a interface{}) float64 {
	switch a.(type) {
	case float64:
		return a.(float64)
	case float32:
		return float64(a.(float32))
	case int:
		return float64(a.(int))
	default:
		panic(a)
	}
}
