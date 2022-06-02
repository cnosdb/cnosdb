package iot

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"strconv"
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

type truck struct {
	Tags struct {
		Name          tag
		Fleet         tag
		Driver        tag
		Model         tag
		DeviceVersion tag
	}
	Timestamp    time.Time
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
	readings    bytes.Buffer
	diagnostics bytes.Buffer
}

type truckNum int

func (t *truck) Init(i truckNum) {
	type model struct {
		Name            string
		LoadCapacity    float64
		FuelCapacity    float64
		FuelConsumption float64
	}

	const (
		truckNameFmt = "truck_%d"
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
	m := randChoice(modelChoices)
	t.Tags.Name = tag{"name", fmt.Sprintf(truckNameFmt, i)}
	t.Tags.Fleet = tag{"fleet", randChoice(fleetChoices)}
	t.Tags.Driver = tag{"driver", randChoice(driverChoices)}
	t.Tags.Model = tag{"model", m.Name}
	t.Tags.DeviceVersion = tag{"device_version", randChoice(deviceVersionChoices)}
	t.Measurements.Readings.Name = "readings"
	t.Measurements.Diagnostics.Name = "diagnostics"
	t.Measurements.Diagnostics.LoadCapacity = field{"load_capacity", m.LoadCapacity}
	t.Measurements.Diagnostics.FuelCapacity = field{"fuel_capacity", m.FuelCapacity}
	t.Measurements.Diagnostics.FuelConsumption = field{"nominal_fuel_consumption", m.FuelConsumption}
}

func (t *truck) New(ts time.Time) {
	const (
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

	t.Timestamp = ts

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

	t.Measurements.Diagnostics.CurrentLoad = field{"current_load",
		newCurrentLoad(t.Measurements.Diagnostics.CurrentLoad.Value, maxLoad, 1-loadChangeChance)}
	t.Measurements.Diagnostics.FuelState = field{"fuel_state",
		fuelState(maxFuel, -0.001, 0, 0, maxFuel, 1)}
	t.Measurements.Diagnostics.Status = field{"status",
		readingsField(0, 0, 1, 0, 5, 0)}
}

func (t *truck) Serialize() (readings, diagnostics []byte) {
	t.readings.Reset()
	t.diagnostics.Reset()
	writeTags := func(buf *bytes.Buffer) {
		buf.WriteString(",")
		buf.WriteString(t.Tags.Name.Key)
		buf.WriteString("=")
		buf.WriteString(t.Tags.Name.Value)
		buf.WriteString(",")
		buf.WriteString(t.Tags.Fleet.Key)
		buf.WriteString("=")
		buf.WriteString(t.Tags.Fleet.Value)
		buf.WriteString(",")
		buf.WriteString(t.Tags.Model.Key)
		buf.WriteString("=")
		buf.WriteString(t.Tags.Model.Value)
		buf.WriteString(",")
		buf.WriteString(t.Tags.DeviceVersion.Key)
		buf.WriteString("=")
		buf.WriteString(t.Tags.DeviceVersion.Value)
		buf.WriteString(" ")
	}
	ff64 := func(f float64) string { return strconv.FormatFloat(f, 'f', -1, 64) }
	r := &t.Measurements.Readings
	t.readings.WriteString(r.Name)
	writeTags(&t.readings)
	t.readings.WriteString(r.Latitude.Key)
	t.readings.WriteString("=")
	t.readings.WriteString(ff64(r.Latitude.Value))
	t.readings.WriteString(",")
	t.readings.WriteString(r.Longitude.Key)
	t.readings.WriteString("=")
	t.readings.WriteString(ff64(r.Longitude.Value))
	t.readings.WriteString(",")
	t.readings.WriteString(r.Elevation.Key)
	t.readings.WriteString("=")
	t.readings.WriteString(ff64(r.Elevation.Value))
	t.readings.WriteString(",")
	t.readings.WriteString(r.Velocity.Key)
	t.readings.WriteString("=")
	t.readings.WriteString(ff64(r.Velocity.Value))
	t.readings.WriteString(",")
	t.readings.WriteString(r.Heading.Key)
	t.readings.WriteString("=")
	t.readings.WriteString(ff64(r.Heading.Value))
	t.readings.WriteString(",")
	t.readings.WriteString(r.Grade.Key)
	t.readings.WriteString("=")
	t.readings.WriteString(ff64(r.Grade.Value))
	t.readings.WriteString(",")
	t.readings.WriteString(r.FuelConsumption.Key)
	t.readings.WriteString("=")
	t.readings.WriteString(ff64(r.FuelConsumption.Value))
	t.readings.WriteString(" ")
	t.readings.WriteString(strconv.FormatInt(t.Timestamp.UTC().UnixNano(), 10))

	d := &t.Measurements.Diagnostics
	t.diagnostics.WriteString(d.Name)
	writeTags(&t.diagnostics)
	t.diagnostics.WriteString(d.LoadCapacity.Key)
	t.diagnostics.WriteString("=")
	t.diagnostics.WriteString(ff64(d.LoadCapacity.Value))
	t.diagnostics.WriteString(",")
	t.diagnostics.WriteString(d.FuelCapacity.Key)
	t.diagnostics.WriteString("=")
	t.diagnostics.WriteString(ff64(d.FuelCapacity.Value))
	t.diagnostics.WriteString(",")
	t.diagnostics.WriteString(d.FuelConsumption.Key)
	t.diagnostics.WriteString("=")
	t.diagnostics.WriteString(ff64(d.FuelConsumption.Value))
	t.diagnostics.WriteString(",")
	t.diagnostics.WriteString(d.CurrentLoad.Key)
	t.diagnostics.WriteString("=")
	t.diagnostics.WriteString(ff64(d.CurrentLoad.Value))
	t.diagnostics.WriteString(",")
	t.diagnostics.WriteString(d.FuelState.Key)
	t.diagnostics.WriteString("=")
	t.diagnostics.WriteString(ff64(d.FuelState.Value))
	t.diagnostics.WriteString(",")
	t.diagnostics.WriteString(d.Status.Key)
	t.diagnostics.WriteString("=")
	t.diagnostics.WriteString(ff64(d.Status.Value))
	t.diagnostics.WriteString(" ")
	t.diagnostics.WriteString(strconv.FormatInt(t.Timestamp.UTC().UnixNano(), 10))

	return t.readings.Bytes(), t.diagnostics.Bytes()
}

func randChoice[T any](s []T) T {
	return s[rand.Intn(len(s))]
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
