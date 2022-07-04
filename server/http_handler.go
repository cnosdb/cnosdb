package server

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cnosdb/cnosdb/pkg/errors"

	"github.com/cnosdb/cnosdb/meta"
	"github.com/cnosdb/cnosdb/monitor"
	"github.com/cnosdb/cnosdb/pkg/logger"
	"github.com/cnosdb/cnosdb/pkg/prometheus"
	"github.com/cnosdb/cnosdb/pkg/uuid"
	"github.com/cnosdb/cnosdb/vend/cnosql"
	"github.com/cnosdb/cnosdb/vend/common/monitor/diagnostics"
	"github.com/cnosdb/cnosdb/vend/db/models"
	"github.com/cnosdb/cnosdb/vend/db/query"
	"github.com/cnosdb/cnosdb/vend/db/tsdb"
	"github.com/cnosdb/cnosdb/vend/storage"

	"github.com/dgrijalva/jwt-go"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/gorilla/mux"
	"github.com/prometheus/prometheus/prompb"
	"go.uber.org/zap"
)

const (
	headerContentType = "Content-Type"
	contentTypeJSON   = "application/json"

	headerRequestID = "X-Request-Id"
	headerErrorMsg  = "X-CnosDB-Error"
)

// AuthenticationMethod 鉴权方式
type AuthenticationMethod int

// 目前支持的鉴权方式
const (
	// UserAuthentication 基于 basic authentication 进行鉴权
	UserAuthentication AuthenticationMethod = iota

	// BearerAuthentication 基于 JWT 进行鉴权
	BearerAuthentication
)

const (
	// DefaultChunkSize specifies the maximum number of points that will
	// be read before sending results back to the engine.
	//
	// This has no relation to the number of bytes that are returned.
	DefaultChunkSize = 10000

	DefaultDebugRequestsInterval = 10 * time.Second

	MaxDebugRequestsInterval = 6 * time.Hour
)

// statistics gathered by the httpd package.
const (
	statRequest                      = "req"                  // Number of HTTP requests served.
	statQueryRequest                 = "queryReq"             // Number of query requests served.
	statWriteRequest                 = "writeReq"             // Number of write requests serverd.
	statPingRequest                  = "pingReq"              // Number of ping requests served.
	statStatusRequest                = "statusReq"            // Number of status requests served.
	statWriteRequestBytesReceived    = "writeReqBytes"        // Sum of all bytes in write requests.
	statQueryRequestBytesTransmitted = "queryRespBytes"       // Sum of all bytes returned in query reponses.
	statPointsWrittenOK              = "pointsWrittenOK"      // Number of points written OK.
	statValuesWrittenOK              = "valuesWrittenOK"      // Number of values (fields) written OK.
	statPointsWrittenDropped         = "pointsWrittenDropped" // Number of points dropped by the storage engine.
	statPointsWrittenFail            = "pointsWrittenFail"    // Number of points that failed to be written.
	statAuthFail                     = "authFail"             // Number of authentication failures.
	statRequestDuration              = "reqDurationNs"        // Number of (wall-time) nanoseconds spent inside requests.
	statQueryRequestDuration         = "queryReqDurationNs"   // Number of (wall-time) nanoseconds spent inside query requests.
	statWriteRequestDuration         = "writeReqDurationNs"   // Number of (wall-time) nanoseconds spent inside write requests.
	statRequestsActive               = "reqActive"            // Number of currently active requests.
	statWriteRequestsActive          = "writeReqActive"       // Number of currently active write requests.
	statClientError                  = "clientError"          // Number of HTTP responses due to client error.
	statServerError                  = "serverError"          // Number of HTTP responses due to server error.
	statRecoveredPanics              = "recoveredPanics"      // Number of panics recovered by HTTP Handler.
	statPromWriteRequest             = "promWriteReq"         // Number of write requests to the prometheus endpoint.
	statPromReadRequest              = "promReadReq"          // Number of read requests to the prometheus endpoint.
)

// 如果环境变量 CNOSDB_PANIC_CRASH 值已设置，并且为 true
// 那么系统在处理请求时将不会试图 recover 任何 panic
var willCrash bool

func init() {
	var err error
	if willCrash, err = strconv.ParseBool(os.Getenv(query.PanicCrashEnv)); err != nil {
		willCrash = false
	}
}

// route 定义 HTTP 谓词的路由，以及处理方式等属性
type route struct {
	Name           string
	Method         string
	Path           string
	Gzipped        bool
	LoggingEnabled bool
	HandlerFunc    interface{}
}

type QueryAuthorizer interface {
	AuthorizeQuery(u meta.User, query *cnosql.Query, database string) (query.FineAuthorizer, error)
	AuthorizeDatabase(u meta.User, priv cnosql.Privilege, database string) error
}

// userQueryAuthorizer binds the QueryAuthorizer with a specific user for consumption by the query engine.
type userQueryAuthorizer struct {
	auth QueryAuthorizer
	user meta.User
}

func (a *userQueryAuthorizer) AuthorizeDatabase(p cnosql.Privilege, name string) bool {
	return a.auth.AuthorizeDatabase(a.user, p, name) == nil
}

// Handler http 请求的处理逻辑
type Handler struct {
	Version string

	config     *HTTPConfig
	metaClient meta.MetaClient

	router *mux.Router

	stats *Statistics

	QueryAuthorizer QueryAuthorizer

	WriteAuthorizer interface {
		AuthorizeWrite(username, database string) error
	}

	QueryExecutor *query.Executor

	StorageStore *storage.Store

	Monitor interface {
		Statistics(tags map[string]string) ([]*monitor.Statistic, error)
		Diagnostics() (map[string]*diagnostics.Diagnostics, error)
	}

	PointsWriter interface {
		WritePoints(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, user meta.User, points []models.Point) error
	}

	requestTracker *RequestTracker
	writeThrottler *Throttler

	logger       *zap.Logger
	accessLogger *log.Logger
}

// 创建 Handler 的实例，并设置 router
func NewHandler(conf *HTTPConfig) *Handler {
	h := &Handler{
		config:         conf,
		router:         mux.NewRouter(),
		stats:          &Statistics{},
		requestTracker: NewRequestTracker(),
		logger:         zap.NewNop(),
		accessLogger:   log.New(os.Stderr, "[httpd] ", 0),
	}

	if metaConfig, err := meta.NewDemoConfig(); err == nil {
		h.metaClient = meta.NewClient(metaConfig)
	} else {
		logger.Error(fmt.Sprintf("unable to create metaclient %s", err))
	}

	if err := h.metaClient.Load(); err != nil {
		logger.Error(fmt.Sprintf("unable to load metadata %s", err))
	}

	h.writeThrottler = NewThrottler(conf.MaxConcurrentWriteLimit, conf.MaxEnqueuedWriteLimit)
	h.writeThrottler.EnqueueTimeout = conf.EnqueuedWriteTimeout

	h.AddRoutes([]route{
		{
			"query-options", http.MethodOptions, "/query", false, true,
			h.serveOptions,
		},
		{
			"metajson", http.MethodGet, "/metajson", true, true,
			h.serveMetaJson,
		},
		{
			"query", http.MethodPost, "/query", false, true,
			h.serveQuery,
		},
		{
			"query", http.MethodGet, "/query", false, true,
			h.serveQuery,
		},
		{
			"ping", http.MethodGet, "/ping", false, true,
			h.servePing,
		},
		{
			"ping-head", http.MethodHead, "/ping", false, true,
			h.servePing,
		},
		{
			"write-options", http.MethodOptions, "/write", false, true,
			h.serveOptions,
		},
		{
			"write", http.MethodPost, "/write", true, true,
			h.serveWrite,
		},
		{
			"prometheus-read", // Prometheus remote read
			"POST", "/api/v1/prom/read", true, true, h.servePromRead,
		},
		{
			"prometheus-write", // Prometheus remote write
			"POST", "/api/v1/prom/write", false, true, h.servePromWrite,
		},
	}...)

	return h
}

func (h *Handler) Open() {
	if h.config.LogEnabled {
		path := "stderr"

		if h.config.AccessLogPath != "" {
			f, err := os.OpenFile(h.config.AccessLogPath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
			if err != nil {
				h.logger.Error("unable to open access log, falling back to stderr",
					zap.Error(err), zap.String("path", h.config.AccessLogPath))
				return
			}
			h.accessLogger = log.New(f, "", 0)
			path = h.config.AccessLogPath
		}
		h.logger.Info("opened HTTP access log", zap.String("path", path))
	}
}

// 响应 HTTP 请求
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("X-CnosDB-Version", h.Version)
	h.router.ServeHTTP(w, r)
}

// serveOptions returns an empty response to comply with OPTIONS pre-flight requests
func (h *Handler) serveOptions(w http.ResponseWriter, r *http.Request) {
	writeHeader(w, http.StatusNoContent)
}

func (h *Handler) serveMetaJson(w http.ResponseWriter, r *http.Request) {
	b, err := json.Marshal(h.metaClient.Data())
	if err != nil {
		h.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Add("Content-Type", "application/octet-stream")
	w.Write(b)
}

// serveQuery parses an incoming query and, if valid, executes the query.
func (h *Handler) serveQuery(w http.ResponseWriter, r *http.Request, user meta.User) {
	atomic.AddInt64(&h.stats.QueryRequests, 1)
	defer func(start time.Time) {
		atomic.AddInt64(&h.stats.QueryRequestDuration, time.Since(start).Nanoseconds())
	}(time.Now())
	h.requestTracker.Add(r, user)

	// Retrieve the underlying ResponseWriter or initialize our own.
	rw, ok := w.(ResponseWriter)
	if !ok {
		rw = NewResponseWriter(w, r)
	}

	// Retrieve the node id the query should be executed on.
	nodeID, _ := strconv.ParseUint(r.FormValue("node_id"), 10, 64)

	var qr io.Reader
	// Attempt to read the form value from the "q" form value.
	if qp := strings.TrimSpace(r.FormValue("q")); qp != "" {
		qr = strings.NewReader(qp)
	} else if r.MultipartForm != nil && r.MultipartForm.File != nil {
		// If we have a multipart/form-data, try to retrieve a file from 'q'.
		if fhs := r.MultipartForm.File["q"]; len(fhs) > 0 {
			f, err := fhs[0].Open()
			if err != nil {
				writeError(rw, err.Error())
				return
			}
			defer f.Close()
			qr = f
		}
	}

	if qr == nil {
		writeError(rw, `missing required parameter "q"`)
		return
	}

	epoch := strings.TrimSpace(r.FormValue("epoch"))

	p := cnosql.NewParser(qr)
	db := r.FormValue("db")

	// Sanitize the request query params so it doesn't show up in the response logger.
	// Do this before anything else so a parsing error doesn't leak passwords.
	sanitize(r)

	// Parse the parameters
	rawParams := r.FormValue("params")
	if rawParams != "" {
		var params map[string]interface{}
		decoder := json.NewDecoder(strings.NewReader(rawParams))
		decoder.UseNumber()
		if err := decoder.Decode(&params); err != nil {
			writeError(rw, fmt.Sprintf("error parsing query parameters: %s", err.Error()))
			return
		}

		// Convert json.Number into int64 and float64 values
		for k, v := range params {
			if v, ok := v.(json.Number); ok {
				var err error
				if strings.Contains(string(v), ".") {
					params[k], err = v.Float64()
				} else {
					params[k], err = v.Int64()
				}

				if err != nil {
					writeError(rw, fmt.Sprintf("error parsing json value: %s", err.Error()))
					return
				}
			}
		}
		p.SetParams(params)
	}

	// Parse query from query string.
	q, err := p.ParseQuery()
	if err != nil {
		writeError(rw, fmt.Sprintf("error parsing query: %s", err.Error()))
		return
	}

	// Check authorization.
	var fineAuthorizer query.FineAuthorizer
	if h.config.AuthEnabled {
		var err error
		if fineAuthorizer, err = h.QueryAuthorizer.AuthorizeQuery(user, q, db); err != nil {
			if authErr, ok := err.(meta.ErrAuthorize); ok {
				h.logger.Info("Unauthorized request",
					zap.String("user", authErr.User),
					zap.Stringer("query", authErr.Query),
					logger.Database(authErr.Database))
			} else {
				h.logger.Info("Error authorizing query", zap.Error(err))
			}
			writeErrorWithCode(rw, "error authorizing query: "+err.Error(), http.StatusForbidden)
			return
		}
	} else {
		fineAuthorizer = query.OpenAuthorizer
	}

	// Parse chunk size. Use default if not provided or unparsable.
	chunked := r.FormValue("chunked") == "true"
	chunkSize := DefaultChunkSize
	if chunked {
		if n, err := strconv.ParseInt(r.FormValue("chunk_size"), 10, 64); err == nil && int(n) > 0 {
			chunkSize = int(n)
		}
	}

	// Parse whether this is an async command.
	async := r.FormValue("async") == "true"

	opts := query.ExecutionOptions{
		Database:        db,
		RetentionPolicy: r.FormValue("rp"),
		ChunkSize:       chunkSize,
		ReadOnly:        r.Method == "GET",
		NodeID:          nodeID,
		Authorizer:      fineAuthorizer,
	}

	if h.config.AuthEnabled {
		// The current user determines the authorized actions.
		opts.CoarseAuthorizer = &userQueryAuthorizer{
			auth: h.QueryAuthorizer,
			user: user,
		}
	} else {
		opts.CoarseAuthorizer = query.OpenCoarseAuthorizer
	}

	// Make sure if the client disconnects we signal the query to abort
	var closing chan struct{}
	if !async {
		closing = make(chan struct{})
		if notifier, ok := w.(http.CloseNotifier); ok {
			// CloseNotify() is not guaranteed to send a notification when the query
			// is closed. Use this channel to signal that the query is finished to
			// prevent lingering goroutines that may be stuck.
			done := make(chan struct{})
			defer close(done)

			notify := notifier.CloseNotify()
			go func() {
				// Wait for either the request to finish
				// or for the client to disconnect
				select {
				case <-done:
				case <-notify:
					close(closing)
				}
			}()
			opts.AbortCh = done
		} else {
			defer close(closing)
		}
	}

	// Execute query.
	results := h.QueryExecutor.ExecuteQuery(q, opts, closing)

	// If we are running in async mode, open a goroutine to drain the results
	// and return with a StatusNoContent.
	if async {
		go h.async(q, results)
		writeHeader(w, http.StatusNoContent)
		return
	}

	// if we're not chunking, this will be the in memory buffer for all results before sending to client
	resp := Response{Results: make([]*query.Result, 0)}

	// Status header is OK once this point is reached.
	// Attempt to flush the header immediately so the client gets the header information
	// and knows the query was accepted.
	writeHeader(rw, http.StatusOK)
	if w, ok := w.(http.Flusher); ok {
		w.Flush()
	}

	// pull all results from the channel
	rows := 0
	for r := range results {
		// Ignore nil results.
		if r == nil {
			continue
		}

		// if requested, convert result timestamps to epoch
		if epoch != "" {
			convertToEpoch(r, epoch)
		}

		// Write out result immediately if chunked.
		if chunked {
			n, _ := rw.WriteResponse(Response{
				Results: []*query.Result{r},
			})
			atomic.AddInt64(&h.stats.QueryRequestBytesTransmitted, int64(n))
			w.(http.Flusher).Flush()
			continue
		}

		// Limit the number of rows that can be returned in a non-chunked
		// response.  This is to prevent the server from going OOM when
		// returning a large response.  If you want to return more than the
		// default chunk size, then use chunking to process multiple blobs.
		// Iterate through the series in this result to count the rows and
		// truncate any rows we shouldn't return.
		if h.config.MaxRowLimit > 0 {
			for i, series := range r.Series {
				n := h.config.MaxRowLimit - rows
				if n < len(series.Values) {
					// We have reached the maximum number of values. Truncate
					// the values within this row.
					series.Values = series.Values[:n]
					// Since this was truncated, it will always be a partial return.
					// Add this so the client knows we truncated the response.
					series.Partial = true
				}
				rows += len(series.Values)

				if rows >= h.config.MaxRowLimit {
					// Drop any remaining series since we have already reached the row limit.
					if i < len(r.Series) {
						r.Series = r.Series[:i+1]
					}
					break
				}
			}
		}

		// It's not chunked so buffer results in memory.
		// Results for statements need to be combined together.
		// We need to check if this new result is for the same statement as
		// the last result, or for the next statement
		l := len(resp.Results)
		if l == 0 {
			resp.Results = append(resp.Results, r)
		} else if resp.Results[l-1].StatementID == r.StatementID {
			if r.Err != nil {
				resp.Results[l-1] = r
				continue
			}

			cr := resp.Results[l-1]
			rowsMerged := 0
			if len(cr.Series) > 0 {
				lastSeries := cr.Series[len(cr.Series)-1]

				for _, row := range r.Series {
					if !lastSeries.SameSeries(row) {
						// Next row is for a different series than last.
						break
					}
					// Values are for the same series, so append them.
					lastSeries.Values = append(lastSeries.Values, row.Values...)
					lastSeries.Partial = row.Partial
					rowsMerged++
				}
			}

			// Append remaining rows as new rows.
			r.Series = r.Series[rowsMerged:]
			cr.Series = append(cr.Series, r.Series...)
			cr.Messages = append(cr.Messages, r.Messages...)
			cr.Partial = r.Partial
		} else {
			resp.Results = append(resp.Results, r)
		}

		// Drop out of this loop and do not process further results when we hit the row limit.
		if h.config.MaxRowLimit > 0 && rows >= h.config.MaxRowLimit {
			// If the result is marked as partial, remove that partial marking
			// here. While the series is partial and we would normally have
			// tried to return the rest in the next chunk, we are not using
			// chunking and are truncating the series so we don't want to
			// signal to the client that we plan on sending another JSON blob
			// with another result.  The series, on the other hand, still
			// returns partial true if it was truncated or had more data to
			// send in a future chunk.
			r.Partial = false
			break
		}
	}

	// If it's not chunked we buffered everything in memory, so write it out
	if !chunked {
		n, _ := rw.WriteResponse(resp)
		atomic.AddInt64(&h.stats.QueryRequestBytesTransmitted, int64(n))
	}
}

// servePing returns a simple response to let the client know the server is running.
func (h *Handler) servePing(w http.ResponseWriter, r *http.Request) {
	verbose := r.URL.Query().Get("verbose")
	atomic.AddInt64(&h.stats.PingRequests, 1)

	if verbose != "" && verbose != "0" && verbose != "false" {
		writeHeader(w, http.StatusOK)
		b, _ := json.Marshal(map[string]string{"version": h.Version})
		w.Write(b)
	} else {
		writeHeader(w, http.StatusNoContent)
	}
}

// async drains the results from an async query and logs a message if it fails.
func (h *Handler) async(q *cnosql.Query, results <-chan *query.Result) {
	for r := range results {
		// Drain the results and do nothing with them.
		// If it fails, log the failure so there is at least a record of it.
		if r.Err != nil {
			// Do not log when a statement was not executed since there would
			// have been an earlier error that was already logged.
			if r.Err == query.ErrNotExecuted {
				continue
			}
			h.logger.Info("Error while running async query",
				zap.Stringer("query", q),
				zap.Error(r.Err))
		}
	}
}

// serveWrite receives incoming series data in line protocol format and writes it to the database.
func (h *Handler) serveWrite(w http.ResponseWriter, r *http.Request, user meta.User) {
	atomic.AddInt64(&h.stats.WriteRequests, 1)
	atomic.AddInt64(&h.stats.ActiveWriteRequests, 1)
	defer func(start time.Time) {
		atomic.AddInt64(&h.stats.ActiveWriteRequests, -1)
		atomic.AddInt64(&h.stats.WriteRequestDuration, time.Since(start).Nanoseconds())
	}(time.Now())
	h.requestTracker.Add(r, user)

	precision := r.URL.Query().Get("precision")
	switch precision {
	case "", "n", "ns", "u", "ms", "s", "m", "h":
		// it's valid
	default:
		writeError(w, fmt.Sprintf("invalid precision %q (use n, u, ms, s, m or h)", precision))
		return
	}

	database := r.URL.Query().Get("db")
	retentionPolicy := r.URL.Query().Get("rp")

	if database == "" {
		writeError(w, "database is required")
		return
	}

	if di := h.metaClient.Database(database); di == nil {
		writeErrorWithCode(w, fmt.Sprintf("database not found: %q", database), http.StatusNotFound)
		return
	}

	if h.config.AuthEnabled {
		if user == nil {
			writeErrorWithCode(w, fmt.Sprintf("user is required to write to database %q", database), http.StatusForbidden)
			return
		}

		if err := h.WriteAuthorizer.AuthorizeWrite(user.ID(), database); err != nil {
			writeErrorWithCode(w, fmt.Sprintf("%q user is not authorized to write to database %q", user.ID(), database), http.StatusForbidden)
			return
		}
	}

	body := r.Body
	if h.config.MaxBodySize > 0 {
		body = truncateReader(body, int64(h.config.MaxBodySize))
	}

	// Handle gzip decoding of the body
	if r.Header.Get("Content-Encoding") == "gzip" {
		b, err := gzip.NewReader(r.Body)
		if err != nil {
			writeError(w, err.Error())
			return
		}
		defer b.Close()
		body = b
	}

	var bs []byte
	if r.ContentLength > 0 {
		if h.config.MaxBodySize > 0 && r.ContentLength > int64(h.config.MaxBodySize) {
			writeErrorWithCode(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
			return
		}

		// This will just be an initial hint for the gzip reader, as the
		// bytes.Buffer will grow as needed when ReadFrom is called
		bs = make([]byte, 0, r.ContentLength)
	}
	buf := bytes.NewBuffer(bs)

	_, err := buf.ReadFrom(body)
	if err != nil {
		if err == errTruncated {
			writeErrorWithCode(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
			return
		}

		if h.config.WriteTracing {
			h.logger.Info("Write Handler unable to read bytes from request body")
		}
		writeError(w, err.Error())
		return
	}
	atomic.AddInt64(&h.stats.WriteRequestBytesReceived, int64(buf.Len()))

	if h.config.WriteTracing {
		h.logger.Info("Write body received by Handler", zap.ByteString("body", buf.Bytes()))
	}

	points, parseError := models.ParsePointsWithPrecision(buf.Bytes(), time.Now().UTC(), precision)
	// Not points parsed correctly so return the error now
	if parseError != nil && len(points) == 0 {
		if parseError.Error() == "EOF" {
			writeHeader(w, http.StatusOK)
			return
		}
		writeError(w, parseError.Error())
		return
	}

	// Determine required consistency level.
	level := r.URL.Query().Get("consistency")
	consistency := models.ConsistencyLevelOne
	if level != "" {
		var err error
		consistency, err = models.ParseConsistencyLevel(level)
		if err != nil {
			writeError(w, err.Error())
			return
		}
	}

	// Write points.
	if err := h.PointsWriter.WritePoints(database, retentionPolicy, consistency, user, points); errors.IsClientError(err) {
		atomic.AddInt64(&h.stats.PointsWrittenFail, int64(len(points)))
		writeError(w, err.Error())
		return
	} else if errors.IsAuthorizationError(err) {
		atomic.AddInt64(&h.stats.PointsWrittenFail, int64(len(points)))
		writeErrorWithCode(w, err.Error(), http.StatusForbidden)
		return
	} else if werr, ok := err.(tsdb.PartialWriteError); ok {
		atomic.AddInt64(&h.stats.PointsWrittenOK, int64(len(points)-werr.Dropped))
		atomic.AddInt64(&h.stats.PointsWrittenDropped, int64(werr.Dropped))
		writeError(w, werr.Error())
		return
	} else if err != nil {
		atomic.AddInt64(&h.stats.PointsWrittenFail, int64(len(points)))
		writeErrorWithCode(w, err.Error(), http.StatusInternalServerError)
		return
	} else if parseError != nil {
		// We wrote some of the points
		atomic.AddInt64(&h.stats.PointsWrittenOK, int64(len(points)))
		// The other points failed to parse which means the client sent invalid line protocol.  We return a 400
		// response code as well as the lines that failed to parse.
		writeError(w, tsdb.PartialWriteError{Reason: parseError.Error()}.Error())
		return
	}

	atomic.AddInt64(&h.stats.PointsWrittenOK, int64(len(points)))
	writeHeader(w, http.StatusNoContent)
}

// Statistics maintains statistics for the httpd service.
type Statistics struct {
	Requests                     int64
	CQRequests                   int64
	QueryRequests                int64
	WriteRequests                int64
	PingRequests                 int64
	StatusRequests               int64
	WriteRequestBytesReceived    int64
	QueryRequestBytesTransmitted int64
	PointsWrittenOK              int64
	ValuesWrittenOK              int64
	PointsWrittenDropped         int64
	PointsWrittenFail            int64
	AuthenticationFailures       int64
	RequestDuration              int64
	QueryRequestDuration         int64
	WriteRequestDuration         int64
	ActiveRequests               int64
	ActiveWriteRequests          int64
	ClientErrors                 int64
	ServerErrors                 int64
	RecoveredPanics              int64
	PromWriteRequests            int64
	PromReadRequests             int64
}

// Statistics returns statistics for periodic monitoring.
func (h *Handler) Statistics(tags map[string]string) []models.Statistic {
	return []models.Statistic{{
		Name: "httpd",
		Tags: tags,
		Values: map[string]interface{}{
			statRequest:                      atomic.LoadInt64(&h.stats.Requests),
			statQueryRequest:                 atomic.LoadInt64(&h.stats.QueryRequests),
			statWriteRequest:                 atomic.LoadInt64(&h.stats.WriteRequests),
			statPingRequest:                  atomic.LoadInt64(&h.stats.PingRequests),
			statStatusRequest:                atomic.LoadInt64(&h.stats.StatusRequests),
			statWriteRequestBytesReceived:    atomic.LoadInt64(&h.stats.WriteRequestBytesReceived),
			statQueryRequestBytesTransmitted: atomic.LoadInt64(&h.stats.QueryRequestBytesTransmitted),
			statPointsWrittenOK:              atomic.LoadInt64(&h.stats.PointsWrittenOK),
			statValuesWrittenOK:              atomic.LoadInt64(&h.stats.ValuesWrittenOK),
			statPointsWrittenDropped:         atomic.LoadInt64(&h.stats.PointsWrittenDropped),
			statPointsWrittenFail:            atomic.LoadInt64(&h.stats.PointsWrittenFail),
			statAuthFail:                     atomic.LoadInt64(&h.stats.AuthenticationFailures),
			statRequestDuration:              atomic.LoadInt64(&h.stats.RequestDuration),
			statQueryRequestDuration:         atomic.LoadInt64(&h.stats.QueryRequestDuration),
			statWriteRequestDuration:         atomic.LoadInt64(&h.stats.WriteRequestDuration),
			statRequestsActive:               atomic.LoadInt64(&h.stats.ActiveRequests),
			statWriteRequestsActive:          atomic.LoadInt64(&h.stats.ActiveWriteRequests),
			statClientError:                  atomic.LoadInt64(&h.stats.ClientErrors),
			statServerError:                  atomic.LoadInt64(&h.stats.ServerErrors),
			statRecoveredPanics:              atomic.LoadInt64(&h.stats.RecoveredPanics),
			statPromWriteRequest:             atomic.LoadInt64(&h.stats.PromWriteRequests),
			statPromReadRequest:              atomic.LoadInt64(&h.stats.PromReadRequests),
		},
	}}
}

// AddRoutes sets the provided routes on the Handler.
func (h *Handler) AddRoutes(routes ...route) {
	for _, r := range routes {
		var handler http.Handler
		if hf, ok := r.HandlerFunc.(func(http.ResponseWriter, *http.Request, meta.User)); ok {
			handler = WrapWithAuthenticate(hf, h.config, h.metaClient)
		}
		if hf, ok := r.HandlerFunc.(func(http.ResponseWriter, *http.Request)); ok {
			handler = http.HandlerFunc(hf)
		}
		if handler == nil {
			panic(fmt.Sprintf("route is not a 'serveAuthenticateFunc' or 'http.HandlerFunc': %+v", r))
		}

		if r.Method == http.MethodPost {
			switch r.Path {
			case "/write", "/api/v1/prom/write":
				handler = h.writeThrottler.WrapWithThrottler(handler)
			}
		}

		handler = WrapWithResponseWriter(handler)
		if r.Gzipped {
			handler = WrapWithGzipResponseWriter(handler)
		}

		handler = WrapWithCors(handler)
		handler = WrapWithRequestID(handler)

		if h.config.LogEnabled && r.LoggingEnabled {
			handler = h.WrapWithLogger(handler, h.config.AccessLogStatusFilters)
		}
		handler = WrapWithRecovery(handler)

		h.router.Handle(r.Path, handler).Methods(r.Method).Name(r.Name)
	}
}

// WrapWithCors responds to incoming requests and adds the appropriate cors headers
func WrapWithCors(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); origin != "" {
			w.Header().Set(`Access-Control-Allow-Origin`, origin)
			w.Header().Set(`Access-Control-Allow-Methods`, strings.Join([]string{
				`DELETE`,
				`GET`,
				`OPTIONS`,
				`POST`,
				`PUT`,
			}, ", "))

			w.Header().Set(`Access-Control-Allow-Headers`, strings.Join([]string{
				`Accept`,
				`Accept-Encoding`,
				`Authorization`,
				`Content-Length`,
				`Content-Type`,
				`X-CSRF-Token`,
				`X-HTTPD-Method-Override`,
			}, ", "))

			w.Header().Set(`Access-Control-Expose-Headers`, strings.Join([]string{
				`Date`,
				`X-CnosDB-Version`,
				`X-CnosDB-Build`,
			}, ", "))
		}

		if r.Method == "OPTIONS" {
			return
		}

		inner.ServeHTTP(w, r)
	})
}

// WrapWithRequestID
func WrapWithRequestID(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rid := r.Header.Get(headerRequestID)

		// If X-Request-Id is empty then generate a v1 UUID.
		if rid == "" {
			rid = uuid.TimeUUID().String()
		}

		// Set the request ID on the response headers.
		w.Header().Set(headerRequestID, rid)

		inner.ServeHTTP(w, r)
	})
}

// WrapWithLogger
func (h *Handler) WrapWithLogger(inner http.Handler, filters []StatusFilter) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		l := &ResponseLogger{w: w}
		inner.ServeHTTP(l, r)

		if StatusFilters(filters).Match(l.Status()) {
			h.accessLogger.Println(buildLogLine(l, r, start))
		}

		// Log server errors.
		if l.Status()/100 == 5 {
			errStr := l.Header().Get(headerErrorMsg)
			if errStr != "" {
				logger.Error(fmt.Sprintf("[%d] - %q", l.Status(), errStr))
			}
		}
	})
}

// WrapWithRecovery
func WrapWithRecovery(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		l := &ResponseLogger{w: w}

		defer func() {
			if err := recover(); err != nil {
				logLine := buildLogLine(l, r, start)
				logLine = fmt.Sprintf("%s [panic:%s] %s", logLine, err, debug.Stack())
				logger.Error(logLine)
				http.Error(w, http.StatusText(http.StatusInternalServerError), 500)
				//atomic.AddInt64(&h.stats.RecoveredPanics, 1) // Capture the panic in _internal stats.

				if willCrash {
					logger.Error("\n\n=====\nAll goroutines now follow:")
					buf := debug.Stack()
					logger.Error(fmt.Sprintf("%s\n", buf))
					os.Exit(1) // If we panic then the Go server will recover.
				}
			}
		}()

		inner.ServeHTTP(l, r)
	})
}

type credentials struct {
	Method   AuthenticationMethod
	Username string
	Password string
	Token    string
}

// parseCredentials parses a request and returns the authentication credentials.
// The credentials may be present as URL query params, or as a Basic
// Authentication header.
// As params: http://127.0.0.1/query?u=username&p=password
// As basic auth: http://username:password@127.0.0.1
// As Bearer token in Authorization header: Bearer <JWT_TOKEN_BLOB>
// As Token in Authorization header: Token <username:password>
func parseCredentials(r *http.Request) (*credentials, error) {
	q := r.URL.Query()

	// Check for username and password in URL params.
	if u, p := q.Get("u"), q.Get("p"); u != "" && p != "" {
		return &credentials{
			Method:   UserAuthentication,
			Username: u,
			Password: p,
		}, nil
	}

	// 请求头 Authorization
	if s := r.Header.Get("Authorization"); s != "" {
		// Bearer 格式的 Token
		strs := strings.Split(s, " ")
		if len(strs) == 2 {
			switch strs[0] {
			case "Bearer":
				return &credentials{
					Method: BearerAuthentication,
					Token:  strs[1],
				}, nil
			case "Token":
				if u, p, ok := parseToken(strs[1]); ok {
					return &credentials{
						Method:   UserAuthentication,
						Username: u,
						Password: p,
					}, nil
				}
			}
		}

		// Check for basic auth.
		if u, p, ok := r.BasicAuth(); ok {
			return &credentials{
				Method:   UserAuthentication,
				Username: u,
				Password: p,
			}, nil
		}
	}

	return nil, fmt.Errorf("unable to parse authentication credentials")
}

func parseToken(token string) (user, pass string, ok bool) {
	s := strings.IndexByte(token, ':')
	if s < 0 {
		return
	}
	return token[:s], token[s+1:], true
}

// convertToEpoch converts result timestamps from time.Time to the specified epoch.
func convertToEpoch(r *query.Result, epoch string) {
	divisor := int64(1)

	switch epoch {
	case "u":
		divisor = int64(time.Microsecond)
	case "ms":
		divisor = int64(time.Millisecond)
	case "s":
		divisor = int64(time.Second)
	case "m":
		divisor = int64(time.Minute)
	case "h":
		divisor = int64(time.Hour)
	}

	for _, s := range r.Series {
		for _, v := range s.Values {
			if ts, ok := v[0].(time.Time); ok {
				v[0] = ts.UnixNano() / divisor
			}
		}
	}
}

func (h *Handler) servePromRead(w http.ResponseWriter, r *http.Request, user meta.User) {
	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		h.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req prompb.ReadRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Query the DB and create a ReadResponse for Prometheus
	db := r.FormValue("db")
	rp := r.FormValue("rp")

	readRequest, err := prometheus.ReadRequestToCnosDBStorageRequest(&req, db, rp)
	if err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	respond := func(resp *prompb.ReadResponse) {
		data, err := proto.Marshal(resp)
		if err != nil {
			h.httpError(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Header().Set("Content-Encoding", "snappy")

		compressed = snappy.Encode(nil, data)
		if _, err := w.Write(compressed); err != nil {
			h.httpError(w, err.Error(), http.StatusInternalServerError)
			return
		}

		atomic.AddInt64(&h.stats.QueryRequestBytesTransmitted, int64(len(compressed)))
	}

	ctx := context.Background()
	rs, err := h.StorageStore.ReadFilter(ctx, readRequest)
	if err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := &prompb.ReadResponse{
		Results: []*prompb.QueryResult{{}},
	}

	if rs == nil {
		respond(resp)
		return
	}
	defer rs.Close()

	for rs.Next() {
		cur := rs.Cursor()
		if cur == nil {
			// no data for series key + field combination
			continue
		}

		tags := prometheus.RemoveCnosDBSystemTags(rs.Tags())
		var unsupportedCursor string
		switch cur := cur.(type) {
		case tsdb.FloatArrayCursor:
			var series *prompb.TimeSeries
			for {
				a := cur.Next()
				if a.Len() == 0 {
					break
				}

				// We have some data for this series.
				if series == nil {
					series = &prompb.TimeSeries{
						Labels: prometheus.ModelTagsToLabelPairs(tags),
					}
				}

				for i, ts := range a.Timestamps {
					series.Samples = append(series.Samples, prompb.Sample{
						Value:     a.Values[i],
						Timestamp: ts / int64(time.Millisecond),
					})
				}
			}

			// There was data for the series.
			if series != nil {
				resp.Results[0].Timeseries = append(resp.Results[0].Timeseries, series)
			}
		case tsdb.IntegerArrayCursor:
			unsupportedCursor = "int64"
		case tsdb.UnsignedArrayCursor:
			unsupportedCursor = "uint"
		case tsdb.BooleanArrayCursor:
			unsupportedCursor = "bool"
		case tsdb.StringArrayCursor:
			unsupportedCursor = "string"
		default:
			panic(fmt.Sprintf("unreachable: %T", cur))
		}
		cur.Close()

		if len(unsupportedCursor) > 0 {
			h.logger.Info("Prometheus can't read cursor",
				zap.String("cursor_type", unsupportedCursor),
				zap.Stringer("series", tags),
			)
		}
	}

	respond(resp)
}

// servePromWrite receives data in the Prometheus remote write protocol and writes it to the database
func (h *Handler) servePromWrite(w http.ResponseWriter, r *http.Request, user meta.User) {
	atomic.AddInt64(&h.stats.WriteRequests, 1)
	atomic.AddInt64(&h.stats.ActiveWriteRequests, 1)
	atomic.AddInt64(&h.stats.PromWriteRequests, 1)
	defer func(start time.Time) {
		atomic.AddInt64(&h.stats.ActiveWriteRequests, -1)
		atomic.AddInt64(&h.stats.WriteRequestDuration, time.Since(start).Nanoseconds())
	}(time.Now())
	h.requestTracker.Add(r, user)

	database := r.URL.Query().Get("db")
	if database == "" {
		h.httpError(w, "database is required", http.StatusBadRequest)
		return
	}

	if di := h.metaClient.Database(database); di == nil {
		h.httpError(w, fmt.Sprintf("database not found: %q", database), http.StatusNotFound)
		return
	}

	if h.config.AuthEnabled {
		if user == nil {
			h.httpError(w, fmt.Sprintf("%q user is authorized to write to database %q", user.ID(), database), http.StatusForbidden)
			return
		}
	}

	body := r.Body
	if h.config.MaxBodySize > 0 {
		body = truncateReader(body, int64(h.config.MaxBodySize))
	}

	var bs []byte
	if r.ContentLength > 0 {
		if h.config.MaxBodySize > 0 && r.ContentLength > int64(h.config.MaxBodySize) {
			h.httpError(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
			return
		}

		// This will just be an initial hint for the reader, as the
		// bytes.Buffer will grow as needed when ReadFrom is called
		bs = make([]byte, 0, r.ContentLength)
	}
	buf := bytes.NewBuffer(bs)

	_, err := buf.ReadFrom(body)
	if err != nil {
		if err == errTruncated {
			h.httpError(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
			return
		}

		if h.config.WriteTracing {
			h.logger.Info("Prom write handler unable to read bytes from request body")
		}
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}
	atomic.AddInt64(&h.stats.WriteRequestBytesReceived, int64(buf.Len()))

	if h.config.WriteTracing {
		h.logger.Info("Prom write body received by handler", zap.ByteString("body", buf.Bytes()))
	}

	reqBuf, err := snappy.Decode(nil, buf.Bytes())
	if err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Convert the Prometheus remote write request to CnosDB Points
	var req prompb.WriteRequest
	if err := req.Unmarshal(reqBuf); err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	points, err := prometheus.WriteRequestToPoints(&req)
	if err != nil {
		if h.config.WriteTracing {
			h.logger.Info("Prom write handler", zap.Error(err))
		}

		// Check if the error was from something other than dropping invalid values.
		if _, ok := err.(prometheus.DroppedValuesError); !ok {
			h.httpError(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	// Determine required consistency level.
	level := r.URL.Query().Get("consistency")
	consistency := models.ConsistencyLevelOne
	if level != "" {
		consistency, err = models.ParseConsistencyLevel(level)
		if err != nil {
			h.httpError(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	// Write points.
	if err := h.PointsWriter.WritePoints(database, r.URL.Query().Get("rp"), consistency, user, points); errors.IsClientError(err) {
		atomic.AddInt64(&h.stats.PointsWrittenFail, int64(len(points)))
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	} else if errors.IsAuthorizationError(err) {
		atomic.AddInt64(&h.stats.PointsWrittenFail, int64(len(points)))
		h.httpError(w, err.Error(), http.StatusForbidden)
		return
	} else if werr, ok := err.(tsdb.PartialWriteError); ok {
		atomic.AddInt64(&h.stats.PointsWrittenOK, int64(len(points)-werr.Dropped))
		atomic.AddInt64(&h.stats.PointsWrittenDropped, int64(werr.Dropped))
		h.httpError(w, werr.Error(), http.StatusBadRequest)
		return
	} else if err != nil {
		atomic.AddInt64(&h.stats.PointsWrittenFail, int64(len(points)))
		h.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	atomic.AddInt64(&h.stats.PointsWrittenOK, int64(len(points)))
	h.writeHeader(w, http.StatusNoContent)
}

func (h *Handler) writeHeader(w http.ResponseWriter, code int) {
	switch code / 100 {
	case 4:
		atomic.AddInt64(&h.stats.ClientErrors, 1)
	case 5:
		atomic.AddInt64(&h.stats.ServerErrors, 1)
	}
}

type serveAuthenticateFunc func(http.ResponseWriter, *http.Request, meta.User)

// WrapWithAuthenticate wraps a Handler and ensures that if user credentials are passed in
// an attempt is made to authenticate that user. If authentication fails, an error is returned.
//
// There is one exception: if there are no users in the system, authentication is not required. This
// is to facilitate bootstrapping of a system with authentication enabled.
func WrapWithAuthenticate(inner serveAuthenticateFunc, conf *HTTPConfig, metaCli meta.MetaClient) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return early if we are not authenticating
		if !conf.AuthEnabled {
			inner(w, r, nil)
			return
		}
		var user meta.User

		if metaCli.AdminUserExists() {
			creds, err := parseCredentials(r)
			if err != nil {
				//atomic.AddInt64(&h.stats.AuthenticationFailures, 1)
				writeErrorUnauthorized(w, err.Error(), conf.Realm)
				return
			}

			switch creds.Method {
			case UserAuthentication:
				if creds.Username == "" {
					//atomic.AddInt64(&h.stats.AuthenticationFailures, 1)
					writeErrorUnauthorized(w, "username required", conf.Realm)
					return
				}

				user, err = metaCli.Authenticate(creds.Username, creds.Password)
				if err != nil {
					//atomic.AddInt64(&h.stats.AuthenticationFailures, 1)
					writeErrorUnauthorized(w, "authorization failed", conf.Realm)
					return
				}
			case BearerAuthentication:
				keyLookupFn := func(token *jwt.Token) (interface{}, error) {
					// Check for expected signing method.
					if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
						return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
					}
					return []byte(conf.SharedSecret), nil
				}

				// Parse and validate the token.
				token, err := jwt.Parse(creds.Token, keyLookupFn)
				if err != nil {
					writeErrorUnauthorized(w, err.Error(), conf.Realm)
					return
				} else if !token.Valid {
					writeErrorUnauthorized(w, "invalid token", conf.Realm)
					return
				}

				claims, ok := token.Claims.(jwt.MapClaims)
				if !ok {
					writeErrorUnauthorized(w, "problem authenticating token", conf.Realm)
					logger.Info("Could not assert JWT token claims as jwt.MapClaims")
					return
				}

				// Make sure an expiration was set on the token.
				if exp, ok := claims["exp"].(float64); !ok || exp <= 0.0 {
					writeErrorUnauthorized(w, "token expiration required", conf.Realm)
					return
				}

				// Get the username from the token.
				username, ok := claims["username"].(string)
				if !ok {
					writeErrorUnauthorized(w, "username in token must be a string", conf.Realm)
					return
				} else if username == "" {
					writeErrorUnauthorized(w, "token must contain a username", conf.Realm)
					return
				}

				// Lookup user in the metastore.
				if user, err = metaCli.User(username); err != nil {
					writeErrorUnauthorized(w, err.Error(), conf.Realm)
					return
				} else if user == nil {
					writeErrorUnauthorized(w, meta.ErrUserNotFound.Error(), conf.Realm)
					return
				}
			default:
				writeErrorUnauthorized(w, "unsupported authentication", conf.Realm)
			}

		}
		inner(w, r, user)
	})
}

// Throttler represents an HTTP throttler that limits the number of concurrent
// requests being processed as well as the number of enqueued requests.
type Throttler struct {
	current  chan struct{}
	enqueued chan struct{}

	// Maximum amount of time requests can wait in queue.
	// Must be set before adding middleware.
	EnqueueTimeout time.Duration

	Logger *zap.Logger
}

// NewThrottler returns a new instance of Throttler that limits to concurrentN.
// requests processed at a time and maxEnqueueN requests waiting to be processed.
func NewThrottler(concurrentN, maxEnqueueN int) *Throttler {
	return &Throttler{
		current:  make(chan struct{}, concurrentN),
		enqueued: make(chan struct{}, concurrentN+maxEnqueueN),
		Logger:   zap.NewNop(),
	}
}

// WrapWithThrottler wraps h in a middleware Handler that throttles requests.
func (t *Throttler) WrapWithThrottler(h http.Handler) http.Handler {
	timeout := t.EnqueueTimeout

	// Return original Handler if concurrent requests is zero.
	if cap(t.current) == 0 {
		return h
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Start a timer to limit enqueued request times.
		var timerCh <-chan time.Time
		if timeout > 0 {
			timer := time.NewTimer(timeout)
			defer timer.Stop()
			timerCh = timer.C
		}

		// Wait for a spot in the queue.
		if cap(t.enqueued) > cap(t.current) {
			select {
			case t.enqueued <- struct{}{}:
				defer func() { <-t.enqueued }()
			default:
				t.Logger.Warn("request throttled, queue full", zap.Duration("d", timeout))
				http.Error(w, "request throttled, queue full", http.StatusServiceUnavailable)
				return
			}
		}

		// First check if we can immediately send in to current because there is
		// available capacity. This helps reduce racyness in tests.
		select {
		case t.current <- struct{}{}:
		default:
			// Wait for a spot in the list of concurrent requests, but allow checking the timeout.
			select {
			case t.current <- struct{}{}:
			case <-timerCh:
				t.Logger.Warn("request throttled, exceeds timeout", zap.Duration("d", timeout))
				http.Error(w, "request throttled, exceeds timeout", http.StatusServiceUnavailable)
				return
			}
		}
		defer func() { <-t.current }()

		// Execute request.
		h.ServeHTTP(w, r)
	})
}

func WrapWithResponseWriter(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w = NewResponseWriter(w, r)
		inner.ServeHTTP(w, r)
	})
}

// WrapWithGzipResponseWriter determines if the client can accept compressed responses, and encodes accordingly.
func WrapWithGzipResponseWriter(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			inner.ServeHTTP(w, r)
			return
		}

		gw := &lazyGzipResponseWriter{ResponseWriter: w, Writer: w}

		if f, ok := w.(http.Flusher); ok {
			gw.Flusher = f
		}

		r.Context().Done()
		if cn, ok := w.(http.CloseNotifier); ok {
			gw.CloseNotifier = cn
		}

		defer gw.Close()

		inner.ServeHTTP(gw, r)
	})
}
