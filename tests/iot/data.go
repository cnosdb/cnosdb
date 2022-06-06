package iot

import (
	"time"
)

type generator struct {
	Parallel int
	Scale    int // Total number of trucks
	Seed     int64
	Interval time.Duration
	Start    time.Time
	End      time.Time
	trucks   []truck
}
