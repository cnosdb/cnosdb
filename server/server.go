package server

import (
	"encoding/json"
	"fmt"

	"io"
	"math"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"strings"
	"time"

	"github.com/cnosdb/cnosdb"
	"github.com/cnosdb/cnosdb/meta"
	"github.com/cnosdb/cnosdb/monitor"
	"github.com/cnosdb/cnosdb/pkg/logger"
	"github.com/cnosdb/cnosdb/pkg/network"
	"github.com/cnosdb/cnosdb/pkg/utils"
	"github.com/cnosdb/cnosdb/server/continuous_querier"
	"github.com/cnosdb/cnosdb/server/coordinator"
	"github.com/cnosdb/cnosdb/server/hh"
	"github.com/cnosdb/cnosdb/server/snapshotter"
	"github.com/cnosdb/cnosdb/server/subscriber"
	"github.com/cnosdb/cnosdb/vend/db/models"
	"github.com/cnosdb/cnosdb/vend/db/query"
	"github.com/cnosdb/cnosdb/vend/db/tsdb"
	"github.com/cnosdb/cnosdb/vend/storage"
	// Initialize the engine package
	_ "github.com/cnosdb/cnosdb/vend/db/tsdb/engine"
	// Initialize the index package
	_ "github.com/cnosdb/cnosdb/vend/db/tsdb/index"

	"github.com/pkg/errors"
	"github.com/soheilhy/cmux"
	"go.uber.org/zap"
)

const NodeMuxHeader = "node"

type Server struct {
	Config *Config

	err     chan error
	closing chan struct{}

	listener     net.Listener
	httpMux      cmux.CMux
	httpListener net.Listener
	tcpMux       cmux.CMux
	tcpListener  net.Listener

	httpHandler http.Handler
	httpServer  *http.Server

	Node       *cnosdb.Node
	NewNode    bool
	metaServer *meta.Server
	meta.MetaClient

	TSDBStore                *tsdb.Store
	queryExecutor            *query.Executor
	PointsWriter             *coordinator.PointsWriter
	shardWriter              *coordinator.ShardWriter
	hintedHandoff            *hh.Service
	subscriber               *subscriber.Service
	continuousQuerierService *continuous_querier.Service

	coordinatorService *coordinator.Service
	snapshotterService *snapshotter.Service

	services []interface {
		WithLogger(log *zap.Logger)
		Open() error
		Close() error
	}

	monitor *monitor.Monitor

	// Profiling
	CPUProfile            string
	CPUProfileWriteCloser io.WriteCloser
	MemProfile            string
	MemProfileWriteCloser io.WriteCloser

	Logger *zap.Logger
}

func NewServer(c *Config) *Server {
	s := &Server{
		Config:  c,
		err:     make(chan error),
		closing: make(chan struct{}),
		Logger:  logger.BgLogger(),
	}

	return s
}

func (s *Server) Open() error {
	if err := s.initMetaStore(); err != nil {
		return err
	}

	if err := s.initTCPServer(); err != nil {
		return err
	}

	go s.startNodeServer()

	if err := s.initMetaClient(); err != nil {
		return err
	}

	if err := s.initTSDBStore(); err != nil {
		return err
	}

	if err := s.initHTTPServer(); err != nil {
		return err
	}

	if err := s.initMonitor(); err != nil {
		return err
	}

	if err := s.initContinueQuery(); err != nil {
		return err
	}

	if err := s.openServices(); err != nil {
		return err
	}

	go s.startHTTPServer()

	return nil
}

func (s *Server) Close() {
	for _, service := range s.services {
		_ = service.Close()
	}

	if s.PointsWriter != nil {
		_ = s.PointsWriter.Close()
	}

	if s.queryExecutor != nil {
		_ = s.queryExecutor.Close()
	}

	// Close the TSDBStore, no more reads or writes at this point
	if s.TSDBStore != nil {
		_ = s.TSDBStore.Close()
	}

	if s.MetaClient != nil {
		_ = s.MetaClient.Close()
	}

	_ = s.httpListener.Close()
	s.httpMux.Close()

	if s.continuousQuerierService != nil {
		_ = s.continuousQuerierService.Close()
	}

	close(s.closing)
}

// Err returns an error channel that multiplexes all out of band errors received from all services.
func (s *Server) Err() <-chan error { return s.err }

func (s *Server) initMetaStore() error {
	if err := os.MkdirAll(s.Config.Meta.Dir, 0777); err != nil {
		return fmt.Errorf("mkdir all: %s", err)
	}

	if node, err := cnosdb.LoadNode(s.Config.Meta.Dir, ""); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		s.NewNode = true
		s.Node = cnosdb.NewNode(s.Config.Meta.Dir)
	} else {
		s.Node = node
	}

	return nil
}

func (s *Server) initTSDBStore() error {
	s.monitor = monitor.New(s, s.Config.Monitor)

	s.TSDBStore = tsdb.NewStore(s.Config.Data.Dir)
	s.TSDBStore.EngineOptions.Config = s.Config.Data

	s.TSDBStore.EngineOptions.EngineVersion = s.Config.Data.Engine
	s.TSDBStore.EngineOptions.IndexVersion = s.Config.Data.Index

	s.shardWriter = coordinator.NewShardWriter(time.Duration(s.Config.Coordinator.ShardWriterTimeout),
		s.Config.Coordinator.MaxRemoteWriteConnections)
	s.shardWriter.MetaClient = s.MetaClient

	s.hintedHandoff = hh.NewService(s.Config.HintedHandoff, s.shardWriter, s.MetaClient)
	s.hintedHandoff.Monitor = s.monitor

	s.PointsWriter = coordinator.NewPointsWriter()
	s.PointsWriter.WriteTimeout = time.Duration(s.Config.Coordinator.WriteTimeout)
	s.PointsWriter.MetaClient = s.MetaClient
	s.PointsWriter.HintedHandoff = s.hintedHandoff
	s.PointsWriter.TSDBStore = s.TSDBStore
	s.PointsWriter.ShardWriter = s.shardWriter
	s.PointsWriter.Node = s.Node

	s.subscriber = subscriber.NewService(s.Config.Subscriber)
	s.subscriber.MetaClient = s.MetaClient

	s.queryExecutor = query.NewExecutor()
	s.queryExecutor.StatementExecutor = &coordinator.StatementExecutor{
		MetaClient:  s.MetaClient,
		TaskManager: s.queryExecutor.TaskManager,
		TSDBStore:   s.TSDBStore,
		ShardMapper: &coordinator.LocalShardMapper{
			MetaClient: s.MetaClient,
			TSDBStore: coordinator.LocalTSDBStore{
				Store: s.TSDBStore,
			},
		},
		Monitor:           s.monitor,
		PointsWriter:      s.PointsWriter,
		MaxSelectPointN:   s.Config.Coordinator.MaxSelectPointN,
		MaxSelectSeriesN:  s.Config.Coordinator.MaxSelectSeriesN,
		MaxSelectBucketsN: s.Config.Coordinator.MaxSelectBucketsN,
	}
	s.queryExecutor.TaskManager.QueryTimeout = time.Duration(s.Config.Coordinator.QueryTimeout)
	s.queryExecutor.TaskManager.LogQueriesAfter = time.Duration(s.Config.Coordinator.LogQueriesAfter)
	s.queryExecutor.TaskManager.MaxConcurrentQueries = s.Config.Coordinator.MaxConcurrentQueries

	s.coordinatorService = coordinator.NewService(s.Config.Coordinator)
	s.coordinatorService.TSDBStore = s.TSDBStore
	s.coordinatorService.MetaClient = s.MetaClient

	s.snapshotterService = snapshotter.NewService()
	s.snapshotterService.TSDBStore = s.TSDBStore
	s.snapshotterService.MetaClient = s.MetaClient

	// Open TSDB store.
	if err := s.TSDBStore.Open(); err != nil {
		return fmt.Errorf("open tsdb store: %s", err)
	}

	// Open the points writer service
	if err := s.PointsWriter.Open(); err != nil {
		return fmt.Errorf("open points writer: %s", err)
	}

	// Open the hinted-handoff service
	if err := s.hintedHandoff.Open(); err != nil {
		return fmt.Errorf("open hinted-handoff: %s", err)
	}

	// Open the subscriber service
	if err := s.subscriber.Open(); err != nil {
		return fmt.Errorf("open subscriber: %s", err)
	}

	for _, service := range s.services {
		if err := service.Open(); err != nil {
			return fmt.Errorf("open service: %s", err)
		}
	}

	return nil
}

func (s *Server) initHTTPServer() error {
	ln, err := net.Listen("tcp", s.Config.HTTPD.BindAddress)
	if err != nil {
		return fmt.Errorf("listen: %s", err)
	}
	s.listener = ln

	s.httpMux = cmux.New(s.listener)
	s.httpListener = s.httpMux.Match(cmux.HTTP1Fast())

	h := NewHandler(&s.Config.HTTPD)
	h.Version = "0.0.0"
	h.metaClient = s.MetaClient
	h.QueryAuthorizer = meta.NewQueryAuthorizer(s.MetaClient)
	h.WriteAuthorizer = meta.NewWriteAuthorizer(s.MetaClient)
	h.QueryExecutor = s.queryExecutor
	h.StorageStore = storage.NewStore(s.TSDBStore, s.MetaClient)
	h.Monitor = s.monitor
	h.PointsWriter = s.PointsWriter
	h.logger = logger.BgLogger()
	h.Open()

	s.httpHandler = h

	return nil
}

func (s *Server) initTCPServer() error {
	tcpLn, err := net.Listen("tcp", s.Config.BindAddress)
	if err != nil {
		return fmt.Errorf("listen: %s", err)
	}

	s.tcpMux = cmux.New(tcpLn)
	s.tcpListener = network.ListenString(s.tcpMux, NodeMuxHeader)

	return nil
}

func (s *Server) initMonitor() error {
	s.monitor.MetaClient = s.MetaClient
	s.monitor.PointsWriter = (*monitorPointsWriter)(s.PointsWriter)
	return s.monitor.Open()
}

func (s *Server) openServices() error {
	s.coordinatorService.Listener = network.ListenString(s.tcpMux, coordinator.MuxHeader)
	if err := s.coordinatorService.Open(); err != nil {
		return fmt.Errorf("open coordinator service: %s", err)
	}

	s.snapshotterService.Listener = network.ListenString(s.tcpMux, snapshotter.MuxHeader)
	if err := s.snapshotterService.Open(); err != nil {
		return fmt.Errorf("open snapshotter service: %s", err)
	}

	if err := s.continuousQuerierService.Open(); err != nil {
		return fmt.Errorf("open continuous query service: %s", err)
	}

	return nil
}

func (s *Server) initMetaClient() error {
	var metaCli meta.MetaClient
	if s.Config.Cluster == false {
		metaCli = meta.NewClient(s.Config.Meta)
	} else {
		s.Logger.Info("waiting to be added to cluster")
		metaCli = meta.NewRemoteClient()
		for {
			if len(s.Node.Peers) == 0 {
				time.Sleep(time.Second)
				continue
			}
			metaCli.SetMetaServers(s.Node.Peers)
			break
		}
		s.Logger.Info("joined cluster", zap.String("peers", strings.Join(s.Node.Peers, ",")))
	}
	s.MetaClient = metaCli

	// s.metaClient.SetTLS(s.metaUseTLS)

	if err := s.MetaClient.Open(); err != nil {
		return err
	}

	// if the node ID is > 0 then we need to initialize the metaclient
	if s.Node.ID > 0 {
		s.MetaClient.WaitForDataChanged()
	}

	return nil
}

func (s *Server) startHTTPServer() {
	srv := http.NewServeMux()
	srv.Handle("/", s.httpHandler)

	srv.HandleFunc("/debug/pprof/", pprof.Index)
	srv.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	srv.HandleFunc("/debug/pprof/profile", pprof.Profile)
	srv.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	srv.HandleFunc("/debug/pprof/trace", pprof.Trace)

	s.httpServer = &http.Server{Addr: s.Config.HTTPD.BindAddress, Handler: srv}

	go utils.WithRecovery(func() {
		err := s.httpServer.Serve(s.httpListener)
		s.Logger.Error("http server error", zap.Error(err))
	}, nil)

	if err := s.httpMux.Serve(); err != nil {
		s.Logger.Error("start http/tcp server error", zap.Error(err))
	}
}

const RequestClusterJoin = 0x01

type Request struct {
	Type  uint8
	Peers []string
}

func (s *Server) startNodeServer() {
	go func() {
		for {
			// Wait for next connection.
			conn, err := s.tcpListener.Accept()
			if err != nil && strings.Contains(err.Error(), "connection closed") {
				s.Logger.Error("DATA node listener closed")
			} else if err != nil {
				s.Logger.Error("Error accepting DATA node request", zap.Error(err))
				continue
			}

			var r Request
			if err := json.NewDecoder(conn).Decode(&r); err != nil {
				s.Logger.Error("Error reading request", zap.Error(err))
			}

			switch r.Type {
			case RequestClusterJoin:
				if !s.NewNode {
					conn.Close()
					continue
				}

				if len(r.Peers) == 0 {
					s.Logger.Error("Invalid MetaServerInfo: empty Peers")
					conn.Close()
					continue
				}

				s.joinCluster(conn, r.Peers)

			default:
				s.Logger.Error(fmt.Sprintf("request type unknown: %v", r.Type))
			}
			conn.Close()
		}
	}()

	if err := s.tcpMux.Serve(); err != nil {
		s.Logger.Error("start node server error", zap.Error(err))
	}
}

func (s *Server) joinCluster(conn net.Conn, peers []string) {
	metaClient := meta.NewRemoteClient()
	metaClient.SetMetaServers(peers)
	if err := metaClient.Open(); err != nil {
		s.Logger.Error("error open MetaClient", zap.Error(err))
		return
	}

	// if the node ID is > 0 then we need to initialize the metaclient
	if s.Node.ID > 0 {
		metaClient.WaitForDataChanged()
	}

	// If we've already created a data node for our id, we're done
	if _, err := metaClient.DataNode(s.Node.ID); err == nil {
		metaClient.Close()
		return
	}

	n, err := metaClient.CreateDataNode(s.HTTPAddr(), s.TCPAddr())
	for err != nil {
		s.Logger.Error("unable to create data node. retry in 1s", zap.Error(err))
		time.Sleep(time.Second)
		n, err = metaClient.CreateDataNode(s.HTTPAddr(), s.TCPAddr())
	}
	metaClient.Close()

	s.Node.ID = n.ID
	s.Node.Peers = peers

	if err := s.Node.Save(""); err != nil {
		s.Logger.Error("error save node", zap.Error(err))
		return
	}
	s.NewNode = false

	if err := json.NewEncoder(conn).Encode(n); err != nil {
		s.Logger.Error("error writing response", zap.Error(err))
	}

}

// HTTPAddr returns the HTTP address used by other nodes for HTTP queries and writes.
func (s *Server) HTTPAddr() string {
	return s.remoteAddr(s.Config.HTTPD.BindAddress)
}

// TCPAddr returns the TCP address used by other nodes for cluster communication.
func (s *Server) TCPAddr() string {
	return s.remoteAddr(s.Config.BindAddress)
}

func (s *Server) remoteAddr(addr string) string {
	hostname, err := meta.DefaultHost(s.Config.Hostname, addr)
	if err != nil {
		return addr
	}
	return hostname
}

// Statistics returns statistics for the services running in the Server.
func (s *Server) Statistics(tags map[string]string) []models.Statistic {
	var statistics []models.Statistic
	statistics = append(statistics, s.queryExecutor.Statistics(tags)...)
	statistics = append(statistics, s.TSDBStore.Statistics(tags)...)
	statistics = append(statistics, s.PointsWriter.Statistics(tags)...)
	for _, srv := range s.services {
		if m, ok := srv.(monitor.Reporter); ok {
			statistics = append(statistics, m.Statistics(tags)...)
		}
	}
	return statistics
}

func (s *Server) initContinueQuery() error {
	if !s.Config.ContinuousQuery.Enabled {
		return fmt.Errorf("open continue query service failed. ")
	}

	s.continuousQuerierService = continuous_querier.NewService(s.Config.ContinuousQuery)
	s.continuousQuerierService.MetaClient = s.MetaClient
	s.continuousQuerierService.QueryExecutor = s.queryExecutor
	s.continuousQuerierService.Monitor = s.monitor
	return nil
}

func writeHeader(w http.ResponseWriter, code int) {
	w.WriteHeader(code)
}

func writeErrorUnauthorized(w http.ResponseWriter, errMsg string, realm string) {
	w.Header().Set("WWW-Authenticate", fmt.Sprintf("Basic realm=\"%s\"", realm))
	w.Header().Add(headerContentType, contentTypeJSON)
	writeHeader(w, http.StatusUnauthorized)

	response := Response{Err: errors.New(errMsg)}
	b, _ := json.Marshal(response)
	_, _ = w.Write(b)
}

func writeError(w http.ResponseWriter, errMsg string) {
	writeErrorWithCode(w, errMsg, http.StatusBadRequest)
}

func writeErrorWithCode(w http.ResponseWriter, errMsg string, code int) {
	if code/100 != 2 {
		sz := math.Min(float64(len(errMsg)), 1024.0)
		w.Header().Set(headerErrorMsg, errMsg[:int(sz)])
	}

	w.Header().Add(headerContentType, contentTypeJSON)
	writeHeader(w, code)

	response := Response{Err: errors.New(errMsg)}
	b, _ := json.Marshal(response)
	_, _ = w.Write(b)
}

// httpError writes an error to the client in a standard format.
func (h *Handler) httpError(w http.ResponseWriter, errmsg string, code int) {
	if code == http.StatusUnauthorized {
		// If an unauthorized header will be sent back, add a WWW-Authenticate header
		// as an authorization challenge.
		w.Header().Set("WWW-Authenticate", fmt.Sprintf("Basic realm=\"%s\"", h.config.Realm))
	} else if code/100 != 2 {
		sz := math.Min(float64(len(errmsg)), 1024.0)
		w.Header().Set("X-CnosDB-Error", errmsg[:int(sz)])
	}
	response := Response{Err: errors.New(errmsg)}
	if rw, ok := w.(ResponseWriter); ok {
		h.writeHeader(w, code)
		rw.WriteResponse(response)
		return
	}

	// Default implementation if the response writer hasn't been replaced
	// with our special response writer type.
	w.Header().Add("Content-Type", "application/json")
	h.writeHeader(w, code)
	b, _ := json.Marshal(response)
	w.Write(b)
}

func writeJson(w http.ResponseWriter, data interface{}) {
	js, err := json.MarshalIndent(data, "", " ")
	if err != nil {
		writeErrorWithCode(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// write response
	w.Header().Set(headerContentType, contentTypeJSON)
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(js)
}

// Response represents a list of statement results.
type Response struct {
	Results []*query.Result
	Err     error
}

// MarshalJSON encodes a Response struct into JSON.
func (r Response) MarshalJSON() ([]byte, error) {
	// Define a struct that outputs "error" as a string.
	var o struct {
		Results []*query.Result `json:"results,omitempty"`
		Err     string          `json:"error,omitempty"`
	}

	// Copy fields to output struct.
	o.Results = r.Results
	if r.Err != nil {
		o.Err = r.Err.Error()
	}

	return json.Marshal(&o)
}

// UnmarshalJSON decodes the data into the Response struct.
func (r *Response) UnmarshalJSON(b []byte) error {
	var o struct {
		Results []*query.Result `json:"results,omitempty"`
		Err     string          `json:"error,omitempty"`
	}

	err := json.Unmarshal(b, &o)
	if err != nil {
		return err
	}
	r.Results = o.Results
	if o.Err != "" {
		r.Err = errors.New(o.Err)
	}
	return nil
}

// Error returns the first error from any statement.
// Returns nil if no errors occurred on any statements.
func (r *Response) Error() error {
	if r.Err != nil {
		return r.Err
	}
	for _, rr := range r.Results {
		if rr != nil {
			return rr.Err
		}
	}
	return nil
}

// monitorPointsWriter is a wrapper around `coordinator.PointsWriter` that helps
// to prevent a circular dependency between the `cluster` and `monitor` packages.
type monitorPointsWriter coordinator.PointsWriter

func (pw *monitorPointsWriter) WritePoints(database, retentionPolicy string, points models.Points) error {

	return (*coordinator.PointsWriter)(pw).WritePointsPrivileged(database, retentionPolicy, models.ConsistencyLevelAny, points)
}
