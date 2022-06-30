package server

import (
	"archive/tar"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"strings"
	"time"

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
	"github.com/cnosdb/cnosdb/usage_client"
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

var startTime time.Time

func init() {
	startTime = time.Now().UTC()
}

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

	Node       *meta.Node
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

	//this field is nil.We don't append services to it.
	services []interface {
		WithLogger(log *zap.Logger)
		Open() error
		Close() error
	}

	monitor *monitor.Monitor

	// Server reporting and registration
	reportingDisabled bool

	// Profiling
	CPUProfile            string
	CPUProfileWriteCloser io.WriteCloser
	MemProfile            string
	MemProfileWriteCloser io.WriteCloser

	Logger *zap.Logger
}

func NewServer(c *Config) *Server {
	s := &Server{
		Config:            c,
		err:               make(chan error),
		closing:           make(chan struct{}),
		Logger:            logger.L(),
		reportingDisabled: c.ReportingDisabled,
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

	// Start the reporting service, if not disabled.
	if !s.reportingDisabled {
		go s.startServerReporting()
	}
	go s.startHTTPServer()

	return nil
}

func (s *Server) Close() {

	if s.listener != nil {
		_ = s.listener.Close()
	}

	//services is no use,It's nil.
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

	if s.hintedHandoff != nil {
		_ = s.hintedHandoff.Close()
	}

	if s.monitor != nil {
		_ = s.monitor.Close()
	}

	//if s.snapshotterService != nil {
	//	_ = s.snapshotterService.Close()
	//}

	//_ = s.tcpListener.Close()
	//s.tcpMux.Close()
	//
	//_ = s.httpListener.Close()
	//s.httpMux.Close()
	//
	//_ = s.httpServer.Close()

	close(s.closing)
}

// Err returns an error channel that multiplexes all out of band errors received from all services.
func (s *Server) Err() <-chan error { return s.err }

func (s *Server) initMetaStore() error {
	if err := os.MkdirAll(s.Config.Meta.Dir, 0777); err != nil {
		return fmt.Errorf("mkdir all: %s", err)
	}

	if node, err := meta.LoadNode(s.Config.Meta.Dir, ""); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		s.Node = meta.NewNode(s.Config.Meta.Dir)
	} else {
		s.Node = node
	}

	return nil
}

func (s *Server) initTSDBStore() error {
	s.monitor = monitor.New(s, s.Config.Monitor)

	s.TSDBStore = tsdb.NewStore(s.Config.Data.Dir)
	s.TSDBStore.WithLogger(s.Logger)
	s.TSDBStore.EngineOptions.Config = s.Config.Data

	s.TSDBStore.EngineOptions.EngineVersion = s.Config.Data.Engine
	s.TSDBStore.EngineOptions.IndexVersion = s.Config.Data.Index

	s.shardWriter = coordinator.NewShardWriter(time.Duration(s.Config.Coordinator.ShardWriterTimeout),
		s.Config.Coordinator.MaxRemoteWriteConnections)
	s.shardWriter.MetaClient = s.MetaClient

	s.hintedHandoff = hh.NewService(s.Config.HintedHandoff, s.shardWriter, s.MetaClient)
	s.hintedHandoff.WithLogger(s.Logger)
	s.hintedHandoff.Monitor = s.monitor

	s.PointsWriter = coordinator.NewPointsWriter()
	s.PointsWriter.WithLogger(s.Logger)
	s.PointsWriter.WriteTimeout = time.Duration(s.Config.Coordinator.WriteTimeout)
	s.PointsWriter.MetaClient = s.MetaClient
	s.PointsWriter.HintedHandoff = s.hintedHandoff
	s.PointsWriter.TSDBStore = s.TSDBStore
	s.PointsWriter.ShardWriter = s.shardWriter
	s.PointsWriter.Node = s.Node

	s.subscriber = subscriber.NewService(s.Config.Subscriber)
	s.subscriber.WithLogger(s.Logger)
	s.subscriber.MetaClient = s.MetaClient

	s.queryExecutor = query.NewExecutor()
	s.queryExecutor.WithLogger(s.Logger)
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
	s.coordinatorService.WithLogger(s.Logger)
	s.coordinatorService.TSDBStore = s.TSDBStore
	s.coordinatorService.MetaClient = s.MetaClient

	s.snapshotterService = snapshotter.NewService()
	s.snapshotterService.WithLogger(s.Logger)
	s.snapshotterService.TSDBStore = s.TSDBStore
	s.snapshotterService.MetaClient = s.MetaClient
	s.snapshotterService.Node = s.Node

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
	h.logger = s.Logger
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
	if !s.Config.Cluster {
		metaCli = meta.NewClient(s.Config.Meta)
		metaCli.WithLogger(s.Logger)
	} else {
		s.Logger.Info("waiting to be added to cluster")
		metaCli = meta.NewRemoteClient()
		metaCli.WithLogger(s.Logger)
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
	s.MetaClient.WithLogger(s.Logger)

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
		s.Logger.Info("http server stop", zap.Error(err))
	}, nil)

	if err := s.httpMux.Serve(); err != nil {
		s.Logger.Info("start to stop http/tcp server", zap.Error(err))
	}
}

const (
	RequestClusterJoin uint8 = iota
	RequestUpdateDataNode
	RequestReplaceDataNode
	RequestClusterPreJoin
)

type NodeRequest struct {
	Type     uint8
	NodeAddr string
	OldAddr  string
	Peers    []string
}

type NodeResponse struct {
	StatusCode uint32
	Message    string
}

func (s *Server) startNodeServer() {
	go func() {
		for {
			// Wait for next connection.
			conn, err := s.tcpListener.Accept()
			if err != nil {
				s.Logger.Error("Error accepting DATA node request", zap.Error(err))
				continue
			}

			go func() {
				defer conn.Close()
				if err := s.handleConn(conn); err != nil {
					s.Logger.Info("node service handle conn error", zap.Error(err))
				}
			}()
		}
	}()

	if err := s.tcpMux.Serve(); err != nil {
		s.Logger.Error("start node server error", zap.Error(err))
	}
}

func (s *Server) handleConn(conn net.Conn) error {
	var req NodeRequest
	if err := json.NewDecoder(conn).Decode(&req); err != nil {
		return fmt.Errorf("Error reading request %s", err.Error())
	}

	switch req.Type {
	case RequestClusterJoin:
		return s.handleClusterJoin(conn, req.Peers)
	case RequestUpdateDataNode:
		return s.handleUpdateDataNode(conn, req.Peers, req.OldAddr)
	case RequestReplaceDataNode:
		go s.handleReplaceDataNode(req.NodeAddr)
		io.WriteString(conn, "Processing ......")
		return nil
	case RequestClusterPreJoin:
		return s.handleClusterPreJoin(conn, req.Peers)
	default:
		return fmt.Errorf("node service request type unknown: %v", req.Type)
	}
}

func (s *Server) handleClusterJoin(conn net.Conn, peers []string) error {
	if len(s.Node.Peers) > 0 {
		return fmt.Errorf("Node is already in cluster %v", s.Node.Peers)
	}

	if len(peers) == 0 {
		return fmt.Errorf("Invalid MetaServerInfo: empty Peers")
	}

	s.joinCluster(conn, peers)

	return nil
}

func (s *Server) handleClusterPreJoin(conn net.Conn, peers []string) error {
	if len(peers) == 0 {
		return fmt.Errorf("Invalid MetaServerInfo: empty Peers")
	}

	s.Node.ID = 0
	s.Node.Peers = peers
	rsp := NodeResponse{
		StatusCode: 0,
	}
	if err := json.NewEncoder(conn).Encode(rsp); err != nil {
		s.Logger.Error("error writing response", zap.Error(err))
	}

	return nil
}

func (s *Server) handleReplaceDataNode(destHost string) {
	s.Logger.Info("raplace data command ",
		zap.String("Local", s.TCPAddr()),
		zap.String("Destination", destHost))

	req := &NodeRequest{
		Type:  RequestClusterPreJoin,
		Peers: s.Node.Peers,
	}
	_, err := s.doRequest(destHost, req)
	if err != nil {
		s.Logger.Error("pre join cluster request error", zap.Error(err))
		return
	}
	time.Sleep(time.Second * 3)

	data := s.MetaClient.Data()
	shardList := data.DataNodeContainShardsByID(s.Node.ID)
	for _, shardID := range shardList {
		data := s.MetaClient.Data()
		dbName, rp, shardInfo := data.ShardDBRetentionAndInfo(shardID)
		if !shardInfo.OwnedBy(s.Node.ID) {
			continue
		}
		if s := s.TSDBStore.Shard(shardID); s == nil {
			continue
		}

		reader, writer := io.Pipe()
		go func() {
			if err := s.TSDBStore.BackupShard(shardID, time.Time{}, writer); err != nil {
				writer.CloseWithError(err)
				s.Logger.Error("Error backup Shard", zap.Uint64("shardID", shardID), zap.Error(err))
			} else {
				writer.Close()
			}
		}()

		tr := tar.NewReader(reader)
		client := snapshotter.NewClient(destHost)
		if err := client.UploadShard(shardID, shardID, dbName, rp, tr); err != nil {
			reader.CloseWithError(err)
			s.Logger.Error("Error upload Shard", zap.Uint64("shardID", shardID), zap.Error(err))
			return
		} else {
			reader.Close()
		}

		s.Logger.Info("Success Copy Shard ", zap.Uint64("ShardID", shardID), zap.String("Host", destHost))
	}

	req = &NodeRequest{
		Type:    RequestUpdateDataNode,
		Peers:   s.Node.Peers,
		OldAddr: s.TCPAddr(),
	}
	rsp, err := s.doRequest(destHost, req)
	if err != nil {
		s.Logger.Error("Decode response error", zap.Error(err))
		return
	}

	s.Node.ID = 0
	s.Node.Peers = nil
	s.Node.Save("")

	s.Logger.Info("update data node response ", zap.Int("status", int(rsp.StatusCode)), zap.String("message", rsp.Message))
}

func (s *Server) handleUpdateDataNode(conn net.Conn, peers []string, oldAddr string) error {
	if len(peers) == 0 {
		return fmt.Errorf("Invalid MetaServerInfo: empty Peers")
	}

	metaClient := meta.NewRemoteClient()
	metaClient.SetMetaServers(peers)
	if err := metaClient.Open(); err != nil {
		s.Logger.Error("error open MetaClient", zap.Error(err))
		return err
	}
	defer metaClient.Close()

	nodeInfo, err := metaClient.DataNodeByTCPHost(oldAddr)
	if err != nil {
		s.Logger.Error("can't find node : ", zap.String("host", oldAddr), zap.Error(err))
		return err
	}

	if err := metaClient.UpdateDataNodeAddr(nodeInfo.ID, s.HTTPAddr(), s.TCPAddr()); err != nil {
		s.Logger.Error("unable to update data node. retry in 1s", zap.Error(err))
		return err
	}

	s.Node.ID = nodeInfo.ID
	s.Node.Peers = peers
	if err := s.Node.Save(""); err != nil {
		s.Logger.Error("error save node", zap.Error(err))
		return err
	}

	rsp := NodeResponse{StatusCode: 0}
	if err := json.NewEncoder(conn).Encode(rsp); err != nil {
		s.Logger.Error("error writing response", zap.Error(err))
		return err
	}

	return nil
}

func (s *Server) doRequest(host string, req *NodeRequest) (*NodeResponse, error) {
	// Connect to node service.
	conn, err := network.Dial("tcp", host, NodeMuxHeader)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return nil, fmt.Errorf("Encode node request: %s", err)
	}

	rsp := &NodeResponse{}
	if err := json.NewDecoder(conn).Decode(rsp); err != nil {
		s.Logger.Error("Decode response error", zap.Error(err))
		return nil, err
	}

	return rsp, nil
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

	if err := json.NewEncoder(conn).Encode(n); err != nil {
		s.Logger.Error("error writing response", zap.Error(err))
	}

}

func (s *Server) URL() string {
	return "http://" + s.httpListener.Addr().String()
}

// HTTPAddr returns the HTTP address used by other nodes for HTTP queries and writes.
//todo: Get dynamic address
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
	s.continuousQuerierService.WithLogger(s.Logger)
	s.continuousQuerierService.MetaClient = s.MetaClient
	s.continuousQuerierService.QueryExecutor = s.queryExecutor
	s.continuousQuerierService.Monitor = s.monitor
	return nil
}

func (s *Server) startServerReporting() {
	s.reportServer()

	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-s.closing:
			return
		case <-ticker.C:
			s.reportServer()
		}
	}
}

func (s *Server) reportServer() {
	dbs := s.MetaClient.Databases()
	numDatabases := len(dbs)

	var (
		numMeasurements int64
		numSeries       int64
	)

	for _, db := range dbs {
		name := db.Name
		// Use the context.Background() to avoid timing out on this.
		n, err := s.TSDBStore.SeriesCardinality(context.Background(), name)
		if err != nil {
			s.Logger.Error(fmt.Sprintf("Unable to get series cardinality for database %s: %v", name, err))
		} else {
			numSeries += n
		}

		// Use the context.Background() to avoid timing out on this.
		n, err = s.TSDBStore.MeasurementsCardinality(context.Background(), name)
		if err != nil {
			s.Logger.Error(fmt.Sprintf("Unable to get measurement cardinality for database %s: %v", name, err))
		} else {
			numMeasurements += n
		}
	}

	clusterID := s.MetaClient.ClusterID()
	cl := usage_client.New("")
	usage := usage_client.Usage{
		Product: "cnosdb",
		Data: []usage_client.UsageData{
			{
				Values: usage_client.Values{
					"os":               runtime.GOOS,
					"arch":             runtime.GOARCH,
					"version":          s.monitor.Version,
					"cluster_id":       fmt.Sprintf("%v", clusterID),
					"num_series":       numSeries,
					"num_measurements": numMeasurements,
					"num_databases":    numDatabases,
					"uptime":           time.Since(startTime).Seconds(),
				},
			},
		},
	}

	s.Logger.Info("Sending usage statistics to cnosdb official website")

	go func() {
		_, err := cl.Save(usage)
		if err != nil {
			logger.Debug(fmt.Sprintf("Unable to send message to usage_server because %s", err))
		}
	}()
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
