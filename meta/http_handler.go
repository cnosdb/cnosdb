package meta

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	internal "github.com/cnosdb/cnosdb/meta/internal"
	"github.com/cnosdb/cnosdb/pkg/logger"
	"github.com/cnosdb/cnosdb/pkg/uuid"


	"github.com/gogo/protobuf/proto"
	"github.com/gorilla/mux"
	"github.com/hashicorp/raft"
	"go.uber.org/zap"
)

// route 定义 HTTP 谓词的路由，以及处理方式等属性
type route struct {
	Name           string
	Method         string
	Path           string
	Gzipped        bool
	LoggingEnabled bool
	HandlerFunc    interface{}
}

type Handler struct {
	Version string

	config *ServerConfig

	router *mux.Router

	logger       *zap.Logger
	pprofEnabled bool
	store        interface {
		afterIndex(index uint64) <-chan struct{}
		index() uint64
		leader() string
		leaderHTTP() string
		snapshot() (*Data, error)
		apply(b []byte) error
		joinCluster(peers []string) (*NodeInfo, error)
		addMetaNode(n *NodeInfo) (*NodeInfo, error)
		removeMetaNode(n *NodeInfo) (*NodeInfo, error)
		metaServersHTTP() []string
		otherMetaServersHTTP() []string
		peers() []string
		getNode() *NodeInfo
	}
	s *Server

	mu      sync.RWMutex
	closing chan struct{}
	leases  *Leases
}

// 创建 Handler 的实例，并设置 router
func NewHandler(conf *ServerConfig) *Handler {
	h := &Handler{
		config: conf,
		router: mux.NewRouter(),
	}

	h.AddRoutes([]route{
		{
			"snapshot", http.MethodGet, "/", true, true,
			h.serveSnapshot,
		},
		{
			"ping", http.MethodGet, "/ping", true, true,
			h.servePing,
		},
		{
			"node", http.MethodGet, "/node", true, true,
			h.serveGetNode,
		},
		{
			"lease", http.MethodGet, "/lease", true, true,
			h.serveLease,
		},
		{
			"peers", http.MethodGet, "/peers", true, true,
			h.servePeers,
		},
		{
			"meta-servers", http.MethodGet, "/meta-servers", true, true,
			h.serveMetaServers,
		},
		{
			"execute", http.MethodPost, "/execute", true, true,
			h.serveExecute,
		},
		{
			"join-cluster", http.MethodPost, "/join-cluster", true, true,
			h.serveJoinCluster,
		},
		{
			"add-meta", http.MethodPost, "/add-meta", true, true,
			h.serveAddMeta,
		},
		{
			"remove-meta", http.MethodPost, "/remove-meta", true, true,
			h.serveRemoveMeta,
		},
	}...)

	return h
}

func (h *Handler) Open() {

}

// 响应 HTTP 请求
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.router.ServeHTTP(w, r)
}

func validateCommand(b []byte) error {
	// Ensure command can be deserialized before applying.
	if err := proto.Unmarshal(b, &internal.Command{}); err != nil {
		return fmt.Errorf("unable to unmarshal command: %s", err)
	}

	return nil
}

// serveSnapshot is a long polling http connection to server cache updates
func (h *Handler) serveSnapshot(w http.ResponseWriter, r *http.Request) {
	if h.isClosed() {
		h.httpError(fmt.Errorf("server closed"), w, http.StatusInternalServerError)
		return
	}

	// get the current index that client has
	index, err := strconv.ParseUint(r.URL.Query().Get("index"), 10, 64)
	if err != nil {
		http.Error(w, "error parsing index", http.StatusBadRequest)
	}

	select {
	case <-h.store.afterIndex(index):
		// Send updated snapshot to client.
		ss, err := h.store.snapshot()
		if err != nil {
			h.httpError(err, w, http.StatusInternalServerError)
			return
		}
		b, err := ss.MarshalBinary()
		if err != nil {
			h.httpError(err, w, http.StatusInternalServerError)
			return
		}
		w.Header().Add("Content-Type", "application/octet-stream")
		w.Write(b)
		return
	case <-w.(http.CloseNotifier).CloseNotify():
		// Client closed the connection so we're done.
		return
	case <-h.closing:
		h.httpError(fmt.Errorf("server closed"), w, http.StatusInternalServerError)
		return
	}
}

// servePing will return if the server is up, or if specified will check the status
// of the other meta-servers as well
func (h *Handler) servePing(w http.ResponseWriter, r *http.Request) {
	// if they're not asking to check all servers, just return who we think
	// the leader is
	if r.URL.Query().Get("all") == "" {
		w.Write([]byte(h.store.leader()))
		return
	}

	leader := h.store.leader()
	healthy := true
	for _, n := range h.store.otherMetaServersHTTP() {
		scheme := "http://"
		if h.config.HTTPSEnabled {
			scheme = "https://"
		}
		url := scheme + n + "/ping"

		resp, err := http.Get(url)
		if err != nil {
			healthy = false
			break
		}

		defer resp.Body.Close()
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			healthy = false
			break
		}

		if leader != string(b) {
			healthy = false
			break
		}
	}

	if healthy {
		w.Write([]byte(h.store.leader()))
		return
	}

	h.httpError(fmt.Errorf("one or more meta-servers not up"), w, http.StatusInternalServerError)
}

// serveGetNode will return if the server node info
func (h *Handler) serveGetNode(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")
	enc := json.NewEncoder(w)

	if err := enc.Encode(h.store.getNode()); err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
	}
}

func (h *Handler) serveLease(w http.ResponseWriter, r *http.Request) {
	var name, nodeIDStr string
	q := r.URL.Query()

	// Get the requested lease name.
	name = q.Get("name")
	if name == "" {
		http.Error(w, "lease name required", http.StatusBadRequest)
		return
	}

	// Get the ID of the requesting node.
	nodeIDStr = q.Get("nodeid")
	if nodeIDStr == "" {
		http.Error(w, "node ID required", http.StatusBadRequest)
		return
	}

	// Redirect to leader if necessary.
	leader := h.store.leaderHTTP()
	if leader != h.s.remoteAddr(h.s.httpAddr) {
		if leader == "" {
			// No cluster leader. Client will have to try again later.
			h.httpError(errors.New("no leader"), w, http.StatusServiceUnavailable)
			return
		}
		scheme := "http://"
		if h.config.HTTPSEnabled {
			scheme = "https://"
		}

		leader = scheme + leader + "/lease?" + q.Encode()
		http.Redirect(w, r, leader, http.StatusTemporaryRedirect)
		return
	}

	// Convert node ID to an int.
	nodeID, err := strconv.ParseUint(nodeIDStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid node ID", http.StatusBadRequest)
		return
	}

	// Try to acquire the requested lease.
	// Always returns a lease. err determins if we own it.
	l, err := h.leases.Acquire(name, nodeID)
	// Marshal the lease to JSON.
	b, e := json.Marshal(l)
	if e != nil {
		h.httpError(e, w, http.StatusInternalServerError)
		return
	}
	// Write HTTP status.
	if err != nil {
		// Another node owns the lease.
		w.WriteHeader(http.StatusConflict)
	} else {
		// Lease successfully acquired.
		w.WriteHeader(http.StatusOK)
	}
	// Write the lease data.
	w.Header().Add("Content-Type", "application/json")
	w.Write(b)
	return
}

func (h *Handler) servePeers(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")
	enc := json.NewEncoder(w)

	if err := enc.Encode(h.store.peers()); err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
	}
}

func (h *Handler) serveMetaServers(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")
	enc := json.NewEncoder(w)

	h.store.leaderHTTP()
	if err := enc.Encode(h.store.metaServersHTTP()); err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
	}
}

func (h *Handler) serveExecute(w http.ResponseWriter, r *http.Request) {
	if h.isClosed() {
		h.httpError(fmt.Errorf("server closed"), w, http.StatusServiceUnavailable)
		return
	}

	// Read the command from the request body.
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
		return
	}

	// Make sure it's a valid command.
	if err := validateCommand(body); err != nil {
		h.httpError(err, w, http.StatusBadRequest)
		return
	}

	// Apply the command to the store.
	var resp *internal.Response
	if err := h.store.apply(body); err != nil {
		// If we aren't the leader, redirect client to the leader.
		if err == raft.ErrNotLeader {
			l := h.store.leaderHTTP()
			if l == "" {
				// No cluster leader. Client will have to try again later.
				h.httpError(errors.New("no leader"), w, http.StatusServiceUnavailable)
				return
			}
			scheme := "http://"
			if h.config.HTTPSEnabled {
				scheme = "https://"
			}

			l = scheme + l + "/execute"
			http.Redirect(w, r, l, http.StatusTemporaryRedirect)
			return
		}

		// Error wasn't a leadership error so pass it back to client.
		resp = &internal.Response{
			OK:    proto.Bool(false),
			Error: proto.String(err.Error()),
		}
	} else {
		// Apply was successful. Return the new store index to the client.
		resp = &internal.Response{
			OK:    proto.Bool(false),
			Index: proto.Uint64(h.store.index()),
		}
	}

	// Marshal the response.
	b, err := proto.Marshal(resp)
	if err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
		return
	}

	// Send response to client.
	w.Header().Add("Content-Type", "application/octet-stream")
	w.Write(b)
}

func (h *Handler) serveJoinCluster(w http.ResponseWriter, r *http.Request) {
	if h.isClosed() {
		h.httpError(fmt.Errorf("server closed"), w, http.StatusServiceUnavailable)
		return
	}

	// Read the command from the request body.
	peers := []string{}
	if err := json.NewDecoder(r.Body).Decode(&peers); err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
		return
	}

	node, err := h.store.joinCluster(peers)
	if err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
		return
	}

	// Return the node with newly assigned ID as json
	w.Header().Add("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(node); err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
	}
}

func (h *Handler) serveAddMeta(w http.ResponseWriter, r *http.Request) {
	if h.isClosed() {
		h.httpError(fmt.Errorf("server closed"), w, http.StatusServiceUnavailable)
		return
	}

	// Read the command from the request body.
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
		return
	}

	n := &NodeInfo{}
	if err := json.Unmarshal(body, n); err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
		return
	}

	node, err := h.store.addMetaNode(n)
	if err == raft.ErrNotLeader {
		l := h.store.leaderHTTP()
		if l == "" {
			// No cluster leader. Client will have to try again later.
			h.httpError(errors.New("no leader"), w, http.StatusServiceUnavailable)
			return
		}
		scheme := "http://"
		if h.config.HTTPSEnabled {
			scheme = "https://"
		}

		l = scheme + l + "/add-meta"
		http.Redirect(w, r, l, http.StatusTemporaryRedirect)
		return
	}

	if err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
		return
	}

	// Return the node with newly assigned ID as json
	w.Header().Add("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(node); err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
	}
}

func (h *Handler) serveRemoveMeta(w http.ResponseWriter, r *http.Request) {
	if h.isClosed() {
		h.httpError(fmt.Errorf("server closed"), w, http.StatusServiceUnavailable)
		return
	}

	// Read the command from the request body.
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
		return
	}

	n := &NodeInfo{}
	if err := json.Unmarshal(body, n); err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
		return
	}

	node, err := h.store.removeMetaNode(n)
	if err == raft.ErrNotLeader {
		l := h.store.leaderHTTP()
		if l == "" {
			// No cluster leader. Client will have to try again later.
			h.httpError(errors.New("no leader"), w, http.StatusServiceUnavailable)
			return
		}
		scheme := "http://"
		if h.config.HTTPSEnabled {
			scheme = "https://"
		}

		l = scheme + l + "/del-meta"
		http.Redirect(w, r, l, http.StatusTemporaryRedirect)
		return
	}

	if err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
		return
	}

	// Return the node with newly assigned ID as json
	w.Header().Add("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(node); err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
	}
}

func (h *Handler) isClosed() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	select {
	case <-h.closing:
		return true
	default:
		return false
	}
}

// AddRoutes sets the provided routes on the Handler.
func (h *Handler) AddRoutes(routes ...route) {
	for _, r := range routes {
		var handler http.Handler
		if hf, ok := r.HandlerFunc.(func(http.ResponseWriter, *http.Request)); ok {
			handler = http.HandlerFunc(hf)
		}
		if r.Gzipped {
			handler = WrapWithGzipResponseWriter(handler)
		}

		handler = WrapWithVersionHeader(handler, h.Version)
		handler = WrapWithRequestID(handler)

		if h.config.LoggingEnabled && r.LoggingEnabled {
			handler = h.WrapWithLogger(handler, r.Name)
		}
		handler = WrapWithRecovery(handler, r.Name)

		h.router.Handle(r.Path, handler).Methods(r.Method).Name(r.Name)
	}
}

// WrapWithVersionHeader
func WrapWithVersionHeader(inner http.Handler, version string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Add("X-CnosDB-Version", version)

		inner.ServeHTTP(w, r)
	})
}

// WrapWithRequestID
func WrapWithRequestID(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		uid := uuid.TimeUUID()
		r.Header.Set("Request-Id", uid.String())
		w.Header().Set("Request-Id", r.Header.Get("Request-Id"))

		inner.ServeHTTP(w, r)
	})
}

// WrapWithLogger
func (h *Handler) WrapWithLogger(inner http.Handler, name string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		l := &ResponseLogger{w: w}
		inner.ServeHTTP(l, r)

		h.logger.Info(buildLogLine(l, r, start))
	})
}

// WrapWithRecovery
func WrapWithRecovery(inner http.Handler, name string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		l := &ResponseLogger{w: w}

		defer func() {
			if err := recover(); err != nil {
				b := make([]byte, 1024)
				runtime.Stack(b, false)
				logLine := buildLogLine(l, r, start)
				logLine = fmt.Sprintf("%s [panic:%s]\n%s", logLine, err, string(b))
				logger.Error(logLine)
			}
		}()

		inner.ServeHTTP(l, r)
	})
}

type gzipResponseWriter struct {
	io.Writer
	http.ResponseWriter
}

func (w gzipResponseWriter) Write(b []byte) (int, error) {
	return w.Writer.Write(b)
}

func (w gzipResponseWriter) Flush() {
	w.Writer.(*gzip.Writer).Flush()
}

func (w gzipResponseWriter) CloseNotify() <-chan bool {
	return w.ResponseWriter.(http.CloseNotifier).CloseNotify()
}

// WrapWithGzipResponseWriter determines if the client can accept compressed responses, and encodes accordingly.
func WrapWithGzipResponseWriter(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			inner.ServeHTTP(w, r)
			return
		}

		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		defer gz.Close()
		gzw := gzipResponseWriter{Writer: gz, ResponseWriter: w}
		inner.ServeHTTP(gzw, r)
	})
}

func (h *Handler) httpError(err error, w http.ResponseWriter, status int) {
	if h.config.LoggingEnabled {
		h.logger.Error("http error", zap.Error(err))
	}
	http.Error(w, err.Error(), status)
}
