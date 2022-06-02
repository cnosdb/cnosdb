package iot

import "time"

type generator struct {
	Scale    int // Total number of trucks
	Seed     int
	Interval time.Duration
	Start    time.Time
	End      time.Time
	trucks   []truck
}
