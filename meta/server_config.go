package meta

import (
	"github.com/cnosdb/cnosdb/vend/common/monitor/diagnostics"
	"net"
	"time"

	log "github.com/cnosdb/cnosdb/pkg/logger"
	tls "github.com/cnosdb/cnosdb/pkg/tlsconfig"
	"github.com/cnosdb/cnosdb/vend/common/pkg/toml"
)

const (

	// DefaultHostname is the default hostname if one is not provided.
	DefaultHostname = "localhost"

	// DefaultHTTPBindAddress is the default address to bind the API to.
	DefaultHTTPBindAddress = ":8091"

	// DefaultHeartbeatTimeout is the default heartbeat timeout for the store.
	DefaultHeartbeatTimeout = 1000 * time.Millisecond

	// DefaultElectionTimeout is the default election timeout for the store.
	DefaultElectionTimeout = 1000 * time.Millisecond

	// DefaultLeaderLeaseTimeout is the default leader lease for the store.
	DefaultLeaderLeaseTimeout = 500 * time.Millisecond

	// DefaultCommitTimeout is the default commit timeout for the store.
	DefaultCommitTimeout = 50 * time.Millisecond

	// DefaultLeaseDuration is the default duration for leases.
	DefaultLeaseDuration = 60 * time.Second
)

type ServerConfig struct {
	LoggingEnabled bool `toml:"logging-enabled"`

	Log *log.Config

	// RemoteHostname is the hostname portion to use when registering meta node
	// addresses.  This hostname must be resolvable from other nodes.

	// HTTPBindAddress is the bind address for the metaservice HTTP API
	HTTPBindAddress  string `toml:"http-bind-address"`
	HTTPSEnabled     bool   `toml:"https-enabled"`
	HTTPSCertificate string `toml:"https-certificate"`

	ElectionTimeout    toml.Duration `toml:"election-timeout"`
	HeartbeatTimeout   toml.Duration `toml:"heartbeat-timeout"`
	LeaderLeaseTimeout toml.Duration `toml:"leader-lease-timeout"`
	CommitTimeout      toml.Duration `toml:"commit-timeout"`
	ClusterTracing     bool          `toml:"cluster-tracing"`
	LeaseDuration      toml.Duration `toml:"lease-duration"`

	TLS *tls.Config `toml:"-"`
}

// NewServerConfig builds a new configuration with default values.
func NewServerConfig() *ServerConfig {
	sc := &ServerConfig{
		LoggingEnabled:     DefaultLoggingEnabled,
		HTTPBindAddress:    DefaultHTTPBindAddress,
		ElectionTimeout:    toml.Duration(DefaultElectionTimeout),
		HeartbeatTimeout:   toml.Duration(DefaultHeartbeatTimeout),
		LeaderLeaseTimeout: toml.Duration(DefaultLeaderLeaseTimeout),
		CommitTimeout:      toml.Duration(DefaultCommitTimeout),
		LeaseDuration:      toml.Duration(DefaultLeaseDuration),
	}

	return sc
}

func (c *ServerConfig) defaultHost(addr string) string {
	address, err := DefaultHost(DefaultHostname, addr)
	if nil != err {
		return addr
	}
	return address
}

func DefaultHost(hostname, addr string) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}

	if host == "" || host == "0.0.0.0" || host == "::" {
		return net.JoinHostPort(hostname, port), nil
	}
	return addr, nil
}

// Diagnostics returns a diagnostics representation of a subset of the ServerConfig.
func (c ServerConfig) Diagnostics() (*diagnostics.Diagnostics, error) {
	return diagnostics.RowFromMap(map[string]interface{}{
		"logging-enabled":      c.LoggingEnabled,
		"http-bind-address":    c.HTTPBindAddress,
		"https-enabled":        c.HTTPSEnabled,
		"https-certificate":    c.HTTPSCertificate,
		"election-timeout":     c.ElectionTimeout,
		"heartbeat-timeout":    c.HeartbeatTimeout,
		"leader-lease-timeout": c.LeaderLeaseTimeout,
		"commit-timeout":       c.CommitTimeout,
		"cluster-tracing":      c.ClusterTracing,
	}), nil
}

// ApplyEnvOverrides apply the environment configuration on top of the config.
func (c *Config) ApplyEnvOverrides(getenv func(string) string) error {
	return toml.ApplyEnvOverrides(getenv, "CNOSDB", c)
}
