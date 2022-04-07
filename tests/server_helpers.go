// This package is a set of convenience helpers and structs to make integration testing easier
package tests

import (
	"github.com/cnosdb/cnosdb/meta"
	"github.com/cnosdb/cnosdb/vend/db/models"
	"io"
	"net/url"
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