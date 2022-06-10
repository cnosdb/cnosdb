package monitor_test

import (
	"github.com/BurntSushi/toml"
	"github.com/cnosdb/cnosdb/monitor"
	"testing"
	"time"
)

func TestConfig_Parse(t *testing.T) {
	// Parse configuration.
	var c monitor.Config
	if _, err := toml.Decode(`
store-enabled=true
store-database="db"
store-interval="15m"
`, &c); err != nil {
		t.Fatal(err)
	}

	// Validate configuration.
	if !c.StoreEnabled {
		t.Fatalf("unexpected store-enabled: %v", c.StoreEnabled)
	} else if c.StoreDatabase != "db" {
		t.Fatalf("unexpected store-database: %s", c.StoreDatabase)
	} else if time.Duration(c.StoreInterval) != 15*time.Minute {
		t.Fatalf("unexpected store-interval:  %s", c.StoreInterval)
	}
}

func TestConfig_Validate(t *testing.T) {
	// NewConfig must validate correctly.
	c := monitor.NewConfig()
	if err := c.Validate(); err != nil {
		t.Fatalf("unexpected validation error: %s", err)
	}

	// Non-positive duration is invalid.
	c = monitor.NewConfig()
	c.StoreInterval *= 0
	if err := c.Validate(); err == nil {
		t.Fatalf("unexpected successful validation for %#v", c)
	}

	// Empty database is invalid.
	c = monitor.NewConfig()
	c.StoreDatabase = ""
	if err := c.Validate(); err == nil {
		t.Fatalf("unexpected successful validation for %#v", c)
	}
}
