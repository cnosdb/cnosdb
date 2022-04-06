// This package is a set of convenience helpers and structs to make integration testing easier
package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/cnosdb/cnosdb/meta"
	"github.com/cnosdb/cnosdb/server"
	"github.com/cnosdb/cnosdb/vend/common/pkg/toml"
	"github.com/cnosdb/cnosdb/vend/db/models"
)

var verboseServerLogs bool
var indexType string
var cleanupData bool
var seed int64

// Server represents a test wrapper for server.Server.
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

// NewServer returns a new instance of Server
func NewServer(c *Config) {
	//srv:= server.NewServer(c.Config)
	//s :=
}

//
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

// LocalServer is a Server that is running in-proess and can be accessed directly
type LocalServer struct {
	mu sync.RWMutex
	*server.Server

	*client
	Config *Config
}


type client struct {
	URLFn func() string
}

func (c *client) URL() string {
	return c.URLFn()
}

// Query executes a query against the server and returns the results.
func (s *client) Query(query string) (results string, err error) {
	return s.QueryWithParams(query, nil)
}

// MustQuery executes a query against the server and returns the results.
func (s *client) MustQuery(query string) string {
	results, err := s.Query(query)
	if err != nil {
		panic(err)
	}
	return results
}

func (s *client) QueryWithParams(query string, values url.Values) (results string, err error) {
	var v url.Values
	if values == nil {
		v = url.Values{}
	} else {
		v, _ = url.ParseQuery(values.Encode())
	}
	v.Set("q", query)
	return s.HTTPPost(s.URL()+"/query?"+v.Encode(), nil)
}

func (s *client) MustQueryWithParams(query string, values url.Values) string {
	results, err := s.QueryWithParams(query, values)
	if err != nil {
		panic(err)
	}
	return results
}


// TOOLS
// HTTPGet makes an HTTP GET request to the server and returns the response.
func (s *client) HTTPGet(url string) (results string, err error) {
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	body := strings.TrimSpace(string(MustReadAll(resp.Body)))
	switch resp.StatusCode {
	case http.StatusBadRequest:
		if !expectPattern(".*error parsing query*.", body) {
			return "", fmt.Errorf("unexpected status code: code=%d, body=%s", resp.StatusCode, body)
		}
		return body, nil
	case http.StatusOK:
		return body, nil
	default:
		return "", fmt.Errorf("unexpected status code: code=%d, body=%s", resp.StatusCode, body)
	}
}

// HTTPPost makes an HTTP POST request to the server and returns the response.
func (s *client) HTTPPost(url string, content []byte) (results string, err error) {
	buf := bytes.NewBuffer(content)
	resp, err := http.Post(url, "application/json", buf)
	if err != nil {
		return "", err
	}
	body := strings.TrimSpace(string(MustReadAll(resp.Body)))
	switch resp.StatusCode {
	case http.StatusBadRequest:
		if !expectPattern(".*error parsing query*.", body) {
			return "", fmt.Errorf("unexpected status code: code=%d, body=%s", resp.StatusCode, body)
		}
		return body, nil
	case http.StatusOK, http.StatusNoContent:
		return body, nil
	default:
		return "", fmt.Errorf("unexpected status code: code=%d, body=%s", resp.StatusCode, body)
	}
}



type WriteError struct {
	body       string
	statusCode int
}

func (wr WriteError) StatusCode() int {
	return wr.statusCode
}

func (wr WriteError) Body() string {
	return wr.body
}

func (wr WriteError) Error() string {
	return fmt.Sprintf("invalid status code: code=%d, body=%s", wr.statusCode, wr.body)
}

// Write executes a write against the server and returns the results.
func (s *client) Write(db, rp, body string, params url.Values) (results string, err error) {
	if params == nil {
		params = url.Values{}
	}
	if params.Get("db") == "" {
		params.Set("db", db)
	}
	if params.Get("rp") == "" {
		params.Set("rp", rp)
	}
	resp, err := http.Post(s.URL()+"/write?"+params.Encode(), "", strings.NewReader(body))
	if err != nil {
		return "", err
	} else if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return "", WriteError{statusCode: resp.StatusCode, body: string(MustReadAll(resp.Body))}
	}
	return string(MustReadAll(resp.Body)), nil
}

// MustWrite executes a write to the server. Panic on error.
func (s *client) MustWrite(db, rp, body string, params url.Values) string {
	results, err := s.Write(db, rp, body, params)
	if err != nil {
		panic(err)
	}
	return results
}

func NewRetentionPolicySpec(name string, rf int, duration time.Duration) *meta.RetentionPolicySpec {
	return &meta.RetentionPolicySpec{Name: name, ReplicaN: &rf, Duration: &duration}
}

func maxInt64() string {
	maxInt64, _ := json.Marshal(^int64(0))
	return string(maxInt64)
}

func now() time.Time {
	return time.Now().UTC()
}

func yesterday() time.Time {
	return now().Add(-1 * time.Hour * 24)
}

func mustParseTime(layout, value string) time.Time {
	tm, err := time.Parse(layout, value)
	if err != nil {
		panic(err)
	}
	return tm
}

func mustParseLocation(tzname string) *time.Location {
	loc, err := time.LoadLocation(tzname)
	if err != nil {
		panic(err)
	}
	return loc
}

// MustReadAll reads r. Panic on error.
func MustReadAll(r io.Reader) []byte {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		panic(err)
	}
	return b
}

func RemoteEnabled() bool {
	return os.Getenv("URL") != ""
}

func expectPattern(exp, act string) bool {
	return regexp.MustCompile(exp).MatchString(act)
}