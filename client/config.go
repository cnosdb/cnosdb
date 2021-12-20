package client

import (
	"crypto/tls"
	"net/http"
	"net/url"
	"time"
)

// HTTPConfig is the config data needed to create an HTTP Client.
type HTTPConfig struct {
	// Addr should be of the form "http://host:port"
	// or "http://[ipv6-host%zone]:port".
	Addr string

	// Username is the cnosdb username, optional.
	Username string

	// Password is the cnosdb password, optional.
	Password string

	// UserAgent is the http User Agent, defaults to "CnosDBClient".
	UserAgent string

	// Timeout for cnosdb writes, defaults to no timeout.
	Timeout time.Duration

	// InsecureSkipVerify gets passed to the http client, if true, it will
	// skip https certificate verification. Defaults to false.
	InsecureSkipVerify bool

	// TLSConfig allows the user to set their own TLS config for the HTTP
	// Client. If set, this option overrides InsecureSkipVerify.
	TLSConfig *tls.Config

	// Proxy configures the Proxy function on the HTTP client.
	Proxy func(r *http.Request) (*url.URL, error)
}

// BatchPointsConfig is the config data needed to create an instance of the BatchPoints struct.
type BatchPointsConfig struct {
	// Precision is the write precision of the points, defaults to "ns".
	Precision string

	// Database is the database to write points to.
	Database string

	// RetentionPolicy is the retention policy of the points.
	RetentionPolicy string

	// Write consistency is the number of servers required to confirm write.
	WriteConsistency string
}
