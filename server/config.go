package server

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/cnosdatabase/cnosdb/meta"
	"github.com/cnosdatabase/cnosdb/monitor"
	"github.com/cnosdatabase/cnosdb/pkg/logger"
	"github.com/cnosdatabase/cnosdb/pkg/tlsconfig"
	"github.com/cnosdatabase/cnosdb/server/continuous_querier"
	"github.com/cnosdatabase/cnosdb/server/coordinator"
	"github.com/cnosdatabase/cnosdb/server/hh"
	"github.com/cnosdatabase/cnosdb/server/precreator"
	"github.com/cnosdatabase/cnosdb/server/rp"
	"github.com/cnosdatabase/cnosdb/server/subscriber"
	itoml "github.com/cnosdatabase/common/pkg/toml"
	"github.com/cnosdatabase/db/tsdb"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

const (
	// DefaultTCPBindAddress is the default address for various RPC services.
	DefaultTCPBindAddress = "127.0.0.1:8088"
	DefaultCluster        = false
)

type Config struct {
	// BindAddress is the address that all TCP services use (Raft, Snapshot, Cluster, etc.)
	BindAddress string `toml:"bind-address"`
	Cluster     bool   `toml:"cluster"`

	Meta            *meta.Config
	Data            tsdb.Config
	Coordinator     coordinator.Config
	RetentionPolicy rp.Config
	Precreator      precreator.Config

	Monitor         monitor.Config
	Subscriber      subscriber.Config
	HTTPD           HTTPConfig
	Log             *logger.Config
	ContinuousQuery continuous_querier.Config
	HintedHandoff   hh.Config
	TLS             tlsconfig.Config
}

// NewConfig returns an instance of Config with reasonable defaults.
func NewConfig() *Config {
	c := &Config{}
	c.BindAddress = DefaultTCPBindAddress
	c.Cluster = DefaultCluster
	c.Meta = meta.NewConfig()
	c.Data = tsdb.NewConfig()
	c.Coordinator = coordinator.NewConfig()
	c.Precreator = precreator.NewConfig()

	c.Monitor = monitor.NewConfig()
	c.HTTPD = NewHTTPConfig()
	c.Log = logger.NewDefaultLogConfig()

	c.ContinuousQuery = continuous_querier.NewConfig()
	c.RetentionPolicy = rp.NewConfig()

	return c
}

// NewDemoConfig returns the config that runs when no config is specified.
func NewDemoConfig() (*Config, error) {
	c := NewConfig()

	var homeDir string
	// By default, store meta and data files in current users home directory
	u, err := user.Current()
	if err == nil {
		homeDir = u.HomeDir
	} else if os.Getenv("HOME") != "" {
		homeDir = os.Getenv("HOME")
	} else {
		return nil, fmt.Errorf("failed to determine current user for storage")
	}

	c.Meta.Dir = filepath.Join(homeDir, ".cnosdb/meta")
	c.Data.Dir = filepath.Join(homeDir, ".cnosdb/data")
	c.Data.WALDir = filepath.Join(homeDir, ".cnosdb/wal")

	return c, nil
}

// FromTomlFile loads the config from a TOML file.
func (c *Config) FromTomlFile(fpath string) error {
	bs, err := ioutil.ReadFile(fpath)
	if err != nil {
		return err
	}

	// Handle any potential Byte-Order-Marks that may be in the config file.
	// This is for Windows compatibility only.

	bom := unicode.BOMOverride(transform.Nop)
	bs, _, err = transform.Bytes(bom, bs)
	if err != nil {
		return err
	}
	return c.FromToml(string(bs))
}

// FromToml loads the config from TOML.
func (c *Config) FromToml(input string) error {
	// Replace deprecated [cluster] with [coordinator]
	re := regexp.MustCompile(`(?m)^\s*\[cluster\]`)
	input = re.ReplaceAllStringFunc(input, func(in string) string {
		in = strings.TrimSpace(in)
		out := "[coordinator]"
		log.Printf("deprecated config option %s replaced with %s; %s will not be supported in a future release\n", in, out, in)
		return out
	})

	_, err := toml.Decode(input, c)
	return err
}

// Validate returns an error if the config is invalid.
func (c *Config) Validate() error {
	if err := c.Data.Validate(); err != nil {
		return err
	}

	if err := c.Monitor.Validate(); err != nil {
		return err
	}

	if err := c.ContinuousQuery.Validate(); err != nil {
		return err
	}

	if err := c.HintedHandoff.Validate(); err != nil {
		return err
	}

	if err := c.RetentionPolicy.Validate(); err != nil {
		return err
	}

	if err := c.Precreator.Validate(); err != nil {
		return err
	}

	if err := c.Subscriber.Validate(); err != nil {
		return err
	}

	if err := c.TLS.Validate(); err != nil {
		return err
	}

	return nil
}

// ApplyEnvOverrides apply the environment configuration on top of the config.
func (c *Config) ApplyEnvOverrides(getenv func(string) string) error {
	return itoml.ApplyEnvOverrides(getenv, "CNOSDB", c)
}
