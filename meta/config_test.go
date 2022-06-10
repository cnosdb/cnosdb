package meta_test

import (
	"github.com/BurntSushi/toml"
	"github.com/cnosdb/cnosdb/meta"
	"testing"
)

func TestConfig_Parse(t *testing.T) {
	// Parse configuration.
	var c meta.Config
	if _, err := toml.Decode(`
dir = "/tmp/foo"
hostname = "localhost"
retention-autocreate = true
`, &c); err != nil {
		t.Fatal(err)
	}

	// Validate configuration.
	if c.Dir != "/tmp/foo" {
		t.Fatalf("unexpected dir: %s", c.Dir)
	} else if c.Hostname != "localhost" {
		t.Fatalf("unexpected hostname: %v", c.Hostname)
	} else if !c.RetentionAutoCreate {
		t.Fatalf("unexpected retention-auto-create value")
	}
}
