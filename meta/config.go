package meta

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/cnosdb/cnosdb/pkg/logger"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

const (
	// DefaultLoggingEnabled determines if log messages are printed for the meta service.
	DefaultLoggingEnabled = true
)

// Config represents the meta configuration.
type Config struct {
	Dir                 string `toml:"dir"`
	RetentionAutoCreate bool   `toml:"retention-autocreate"`
	Hostname            string `toml:"hostname"`
	HTTPD               *ServerConfig
	Log                 *logger.Config
}

// NewConfig builds a new configuration with default values.
func NewConfig() *Config {
	return &Config{
		RetentionAutoCreate: true,
	}
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

	c.Dir = filepath.Join(homeDir, ".cnosdb/meta")

	return c, nil
}

// NewDemoClusterConfig returns the config with meta server config
// that runs when no config is specified.
func NewDemoClusterConfig() (*Config, error) {
	c, err := NewDemoConfig()
	if err != nil {
		return nil, err
	}

	c.HTTPD = NewServerConfig()
	c.Log = logger.NewDefaultLogConfig()

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

func (c *Config) Validate() error {
	if c.Dir == "" {
		return errors.New("Meta.Dir must be specified")
	}
	return nil
}
