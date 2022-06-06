package iot

import (
	"bytes"
	"fmt"
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
	Num  int
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

type truckGen struct {
	Num  int
	Seed int64
	r    random
	t    truck
}

func (g *truckGen) Init() {
	g.r = random{Seed: g.Seed}
	g.r.Init()

	type model struct {
		LoadCapacity    float64
		FuelCapacity    float64
		FuelConsumption float64
	}
	const truckNameFmt = "truck_%d"

	fleetChoices := []string{"East", "West", "North", "South"}
	driverChoices := []string{"Derek", "Rodney", "Albert", "Andy", "Seth", "Trish"}
	modelChoices := []string{"F-150", "G-2000", "H-2"}
	modelMap := map[string]model{
		"F-150":  {LoadCapacity: 2000, FuelCapacity: 200, FuelConsumption: 15},
		"G-2000": {LoadCapacity: 5000, FuelCapacity: 300, FuelConsumption: 19},
		"H-2":    {LoadCapacity: 1500, FuelCapacity: 150, FuelConsumption: 12},
	}
	deviceVersionChoices := []string{"v1.0", "v1.5", "v2.0", "v2.3"}

	m := g.r.Choice(modelChoices)
	mo := modelMap[m]
	g.t.Tags.Name = tag{"name", fmt.Sprintf(truckNameFmt, g.Num)}
	g.t.Tags.Fleet = tag{"fleet", g.r.Choice(fleetChoices)}
	g.t.Tags.Driver = tag{"driver", g.r.Choice(driverChoices)}
	g.t.Tags.Model = tag{"model", m}
	g.t.Tags.DeviceVersion = tag{"device_version", g.r.Choice(deviceVersionChoices)}
	g.t.Measurements.Readings.Name = "readings"
	g.t.Measurements.Diagnostics.Name = "diagnostics"
	g.t.Measurements.Diagnostics.LoadCapacity = field{"load_capacity", mo.LoadCapacity}
	g.t.Measurements.Diagnostics.FuelCapacity = field{"fuel_capacity", mo.FuelCapacity}
	g.t.Measurements.Diagnostics.FuelConsumption = field{"nominal_fuel_consumption", mo.FuelConsumption}
}

func (g *truckGen) New(ts time.Time) (readings, diagnostics []byte) {
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

	g.t.Timestamp = ts

	g.t.Measurements.Readings.Latitude = field{"latitude",
		g.r.ReadingsField(maxLatitude, -0.005, 0.005, -90.0, 90.0, 5)}
	g.t.Measurements.Readings.Longitude = field{"longitude",
		g.r.ReadingsField(maxLongitude, -0.005, 0.005, -180, 180, 5)}
	g.t.Measurements.Readings.Elevation = field{"elevation",
		g.r.ReadingsField(500, -10, 10, 0, maxElevation, 0)}
	g.t.Measurements.Readings.Velocity = field{"velocity",
		g.r.ReadingsField(0, -10, 10, 0, maxVelocity, 0)}
	g.t.Measurements.Readings.Heading = field{"heading",
		g.r.ReadingsField(maxHeading, -5, 5, 0, maxHeading, 0)}
	g.t.Measurements.Readings.Grade = field{"grade",
		g.r.ReadingsField(0, -5, 5, 0, maxGrade, 0)}
	g.t.Measurements.Readings.FuelConsumption = field{"fuel_consumption",
		g.r.ReadingsField(maxFuelConsumption/2, -5, 5, 0, maxFuelConsumption, 1)}

	g.t.Measurements.Diagnostics.CurrentLoad = field{"current_load",
		g.r.CurrentLoad(g.t.Measurements.Diagnostics.CurrentLoad.Value, maxLoad, 1-loadChangeChance)}
	g.t.Measurements.Diagnostics.FuelState = field{"fuel_state",
		g.r.FuelState(maxFuel, -0.001, 0, 0, maxFuel, 1)}
	g.t.Measurements.Diagnostics.Status = field{"status",
		g.r.ReadingsField(0, 0, 1, 0, 5, 0)}

	return g.t.Serialize()
}
