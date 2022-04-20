package server

import (
	"errors"
	"fmt"
	mainserver "github.com/cnosdb/cnosdb/server"
	"os"
	"time"

	"github.com/cnosdb/cnosdb/meta"
	"github.com/cnosdb/cnosdb/vend/db/tsdb"
	"go.uber.org/zap"
)

type Interface interface {
	Open(path string) error
	Close()
	MetaClient() MetaClient
	TSDBConfig() tsdb.Config
	Logger() *zap.Logger
}

type MetaClient interface {
	Database(name string) *meta.DatabaseInfo
	NodeID() uint64
	DropDatabase(name string) error
	RetentionPolicy(database, name string) (rpi *meta.RetentionPolicyInfo, err error)
	ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error)
	CreateRetentionPolicy(database string, spec *meta.RetentionPolicySpec, makeDefault bool) (*meta.RetentionPolicyInfo, error)
	UpdateRetentionPolicy(database, name string, rpu *meta.RetentionPolicyUpdate, makeDefault bool) error
	CreateDatabase(name string) (*meta.DatabaseInfo, error)
	CreateDatabaseWithRetentionPolicy(name string, spec *meta.RetentionPolicySpec) (*meta.DatabaseInfo, error)
	DeleteShardGroup(database, policy string, id uint64) error
	CreateShardGroup(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error)
	// NodeShardGroupsByTimeRange returns a list of all shard groups on a database and policy
	// that may contain data for the specified time range and limits the Shards to the current node only.
	// Shard groups are sorted by start time.
	NodeShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error)
}

func NewSingleServer() *singleServer {
	return &singleServer{logger: zap.NewNop()}
}

type singleServer struct {
	logger   *zap.Logger
	config   *mainserver.Config
	noClient bool
	client   *meta.Client
	mc       MetaClient
}

func (s *singleServer) Open(path string) (err error) {
	s.config, err = s.parseConfig(path)
	if err != nil {
		return err
	}

	// Validate the configuration.
	if err = s.config.Validate(); err != nil {
		return fmt.Errorf("validate config: %s", err)
	}

	if s.noClient {
		return nil
	}

	s.client = meta.NewClient(s.config.Meta)
	if err = s.client.Open(); err != nil {
		s.client = nil
		return err
	}
	s.mc = &ossMetaClient{s.client}
	return nil
}

func (s *singleServer) Close() {
	if s.client != nil {
		s.client.Close()
		s.client = nil
	}
}

func (s *singleServer) MetaClient() MetaClient  { return s.mc }
func (s *singleServer) TSDBConfig() tsdb.Config { return s.config.Data }
func (s *singleServer) Logger() *zap.Logger     { return s.logger }
func (s *singleServer) NodeID() uint64          { return 0 }

// ParseConfig parses the config at path.
// It returns a demo configuration if path is blank.
func (s *singleServer) parseConfig(path string) (*mainserver.Config, error) {
	path = s.resolvePath(path)
	// Use demo configuration if no config path is specified.
	if path == "" {
		return nil, errors.New("missing config file")
	}

	config := mainserver.NewConfig()
	if err := config.FromTomlFile(path); err != nil {
		return nil, err
	}

	return config, nil
}

func (s *singleServer) resolvePath(path string) string {
	if path != "" {
		if path == os.DevNull {
			return ""
		}
		return path
	}

	for _, p := range []string{
		os.ExpandEnv("${HOME}/.influxdb/influxdb.conf"),
		"/etc/influxdb/influxdb.conf",
	} {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return ""
}

type ossMetaClient struct {
	*meta.Client
}

func (*ossMetaClient) NodeID() uint64 { return 0 }

func (c *ossMetaClient) NodeShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error) {
	return c.ShardGroupsByTimeRange(database, policy, min, max)
}
