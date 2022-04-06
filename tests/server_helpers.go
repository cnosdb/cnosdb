// This package is a set of convenience helpers and structs to make integration testing easier
package tests

import (
	"github.com/cnosdb/cnosdb/vend/common/pkg/toml"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/cnosdb/cnosdb/meta"
	"github.com/cnosdb/cnosdb/server"
	"github.com/cnosdb/cnosdb/vend/db/models"
)

var verboseServerLogs bool
var indexType string
var cleanupData bool
var seed int64

// Server represents a test wrapper for run.Server.
type Server interface {
	URL() string
	TcpAddr() string
	Open() error
	SetLogOutput(w io.Writer)
	Close()
	Closed() bool

	CreateDatabase(db string) (*meta.DatabaseInfo, error)
	CreateDatabaseAndRetentionPolicy(db string, rp *meta.RetentionPolicySpec, makeDefault bool) error
	CreateSubscription(database, rp, name, mode string, destinations []string) error
	DropDatabase(db string) error
	Reset() error

	Query(query string) (results string, err error)
	QueryWithParams(query string, values url.Values) (results string, err error)

	Write(db, rp, body string, params url.Values) (results string, err error)
	MustWrite(db, rp, body string, params url.Values) string
	WritePoints(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, user meta.User, points []models.Point) error
}

func NewServer(c *Config) {

}

type Config struct {
	rootPath string
	*server.Config
}

func NewConfig() *Config {
	root, err := os.MkdirTemp("", "tests-cnosdb-")
	if err != nil {
		panic(err)
	}

	c := &Config{rootPath: root, Config: server.NewConfig()}
	c.BindAddress = "127.0.0.1:0"
	c.Coordinator.WriteTimeout = toml.Duration(30 * time.Second)

	c.Meta.Dir = filepath.Join(c.rootPath, "meta")

	c.Data.Dir = filepath.Join(c.rootPath, "data")
	c.Data.WALDir = filepath.Join(c.rootPath, "wal")
	c.Data.QueryLogEnabled = verboseServerLogs
	c.Data.TraceLoggingEnabled = verboseServerLogs
	c.Data.Index = indexType

	c.HTTPD.Enabled = true
	c.HTTPD.BindAddress = "127.0.0.1:0"
	c.HTTPD.LogEnabled = verboseServerLogs

	c.Monitor.StoreEnabled = false

	return c
}