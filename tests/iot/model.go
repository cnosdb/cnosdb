package iot

import (
	"fmt"
	"math"
	"math/rand"
	"time"
)

type tag struct {
	Key   string
	Value string
}
type field struct {
	Key   string
	Value float64
}

type truckPoint struct {
	Timestamp time.Time
	Tags      struct {
		Name          tag
		Fleet         tag
		Driver        tag
		Model         tag
		DeviceVersion tag
	}
	Measurements struct {
		Readings struct {
			Name            string
			Latitude        field
			Longitude       field
			Elevation       field
			Velocity        field
			Heading         field
			Grade           field
			FuelConsumption field // Actually
		}
		Diagnostics struct {
			Name            string
			LoadCapacity    field
			FuelCapacity    field
			FuelConsumption field // Expected
			CurrentLoad     field
			FuelState       field
			Status          field
		}
	}
}

type truckNum int

func randChoice[T any](s []T) T {
	return s[rand.Intn(len(s))]
}

func newTruck(ts time.Time, i truckNum, currentLoad float64) truckPoint {
	type model struct {
		Name            string
		LoadCapacity    float64
		FuelCapacity    float64
		FuelConsumption float64
	}

	const (
		truckNameFmt       = "truck_%d"
		maxLatitude        = 90.0
		maxLongitude       = 180.0
		maxElevation       = 5000.0
		maxVelocity        = 100
		maxHeading         = 360.0
		maxGrade           = 100.0
		maxFuelConsumption = 50
		maxFuel            = 1.0
		maxLoad            = 5000.0
		loadChangeChance   = 0.05
	)

	fleetChoices := []string{
		"East",
		"West",
		"North",
		"South",
	}
	driverChoices := []string{
		"Derek",
		"Rodney",
		"Albert",
		"Andy",
		"Seth",
		"Trish",
	}
	modelChoices := []model{
		{
			Name:            "F-150",
			LoadCapacity:    2000,
			FuelCapacity:    200,
			FuelConsumption: 15,
		},
		{
			Name:            "G-2000",
			LoadCapacity:    5000,
			FuelCapacity:    300,
			FuelConsumption: 19,
		},
		{
			Name:            "H-2",
			LoadCapacity:    1500,
			FuelCapacity:    150,
			FuelConsumption: 12,
		},
	}
	deviceVersionChoices := []string{
		"v1.0",
		"v1.5",
		"v2.0",
		"v2.3",
	}

	var t truckPoint
	m := randChoice(modelChoices)
	t.Timestamp = ts
	t.Tags.Name = tag{"name", fmt.Sprintf(truckNameFmt, i)}
	t.Tags.Fleet = tag{"fleet", randChoice(fleetChoices)}
	t.Tags.Driver = tag{"driver", randChoice(driverChoices)}
	t.Tags.Model = tag{"model", m.Name}
	t.Tags.DeviceVersion = tag{"device_version", randChoice(deviceVersionChoices)}

	t.Measurements.Readings.Name = "readings"
	t.Measurements.Readings.Latitude = field{"latitude",
		readingsField(maxLatitude, -0.005, 0.005, -90.0, 90.0, 5)}
	t.Measurements.Readings.Longitude = field{"longitude",
		readingsField(maxLongitude, -0.005, 0.005, -180, 180, 5)}
	t.Measurements.Readings.Elevation = field{"elevation",
		readingsField(500, -10, 10, 0, maxElevation, 0)}
	t.Measurements.Readings.Velocity = field{"velocity",
		readingsField(0, -10, 10, 0, maxVelocity, 0)}
	t.Measurements.Readings.Heading = field{"heading",
		readingsField(maxHeading, -5, 5, 0, maxHeading, 0)}
	t.Measurements.Readings.Grade = field{"grade",
		readingsField(0, -5, 5, 0, maxGrade, 0)}
	t.Measurements.Readings.FuelConsumption = field{"fuel_consumption",
		readingsField(maxFuelConsumption/2, -5, 5, 0, maxFuelConsumption, 1)}

	t.Measurements.Diagnostics.Name = "diagnostics"
	t.Measurements.Diagnostics.LoadCapacity = field{"load_capacity", m.LoadCapacity}
	t.Measurements.Diagnostics.FuelCapacity = field{"fuel_capacity", m.FuelCapacity}
	t.Measurements.Diagnostics.FuelConsumption = field{"nominal_fuel_consumption", m.FuelConsumption}
	t.Measurements.Diagnostics.CurrentLoad = field{"current_load",
		newCurrentLoad(currentLoad, maxLoad, 1-loadChangeChance)}
	t.Measurements.Diagnostics.FuelState = field{"fuel_state",
		fuelState(maxFuel, -0.001, 0, 0, maxFuel, 1)}
	t.Measurements.Diagnostics.Status = field{"status",
		readingsField(0, 0, 1, 0, 5, 0)}

	return t
}

// u is uniform
func u(low, high float64) float64 {
	x := rand.Float64()
	x *= high - low
	x += low
	return x
}

// crw is clamped random walk
func crw(step, min, max, state float64) float64 {
	state += step
	if state > max {
		return max
	}
	if state < min {
		return min
	}
	return state
}

// fp is float precision
func fp(step, precision float64) float64 {
	precision = math.Pow(10, precision)
	return float64(int(step*precision)) / precision
}

func newCurrentLoad(origin, max, threshold float64) float64 {
	if u(0, 1) > threshold || origin == 0 {
		return fp(u(0, max), 0)
	}
	return origin
}

func readingsField(state, low, high, min, max, precision float64) float64 {
	return fp(crw(u(low, high), min, max, rand.Float64()*state), precision)
}

func fuelState(state, low, high, min, max, precision float64) float64 {
	c := crw(u(low, high), min, max, state)
	if c == min {
		c = max
	}
	return fp(c, precision)
}
