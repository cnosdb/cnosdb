package meta

import (
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"os"

	"github.com/cnosdb/cnosdb/pkg/logger"
	"github.com/cnosdb/cnosdb/pkg/network"
	"github.com/cnosdb/cnosdb/pkg/utils"

	"github.com/soheilhy/cmux"
	"go.uber.org/zap"
)

const RaftMuxHeader = "raft"

type Server struct {
	Config   *Config
	httpAddr string
	raftAddr string

	listener net.Listener
	mux      cmux.CMux
	httpMux  net.Listener
	raftMux  net.Listener

	httpHandler http.Handler
	httpServer  *http.Server

	Node  *Node
	store *store

	services []interface {
		WithLogger(log *zap.Logger)
		Open() error
		Close() error
	}

	logger *zap.Logger
}

func NewServer(c *Config) *Server {
	s := &Server{
		Config: c,
		logger: logger.L(),
	}

	return s
}

func (s *Server) Open(ln net.Listener) error {
	if err := s.initFileSystem(); err != nil {
		return err
	}

	s.initMetaStore()

	if err := s.initNetwork(ln); err != nil {
		return err
	}

	go s.startHTTPServer()

	if err := s.store.open(s.raftMux); err != nil {
		return err
	}

	return nil
}

func (s *Server) Close() {
	_ = s.store.close()

	_ = s.httpMux.Close()
	_ = s.raftMux.Close()
	s.mux.Close()
}

func (s *Server) initFileSystem() error {
	if err := os.MkdirAll(s.Config.Dir, 0777); err != nil {
		return fmt.Errorf("mkdir all: %s", err)
	}

	if _, err := LoadNode(s.Config.Dir, "meta.json"); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		s.Node = NewNode(s.Config.Dir)
	}

	return nil
}

func (s *Server) initMetaStore() {
	httpAddr := s.remoteAddr(s.Config.HTTPD.HTTPBindAddress)
	tcpAddr := s.remoteAddr(s.Config.HTTPD.HTTPBindAddress)
	s.store = newStore(s.Config, httpAddr, tcpAddr)
	s.store.withLogger(s.logger)
	s.store.node = s.Node
}

func (s *Server) initNetwork(ln net.Listener) error {
	ln, err := net.Listen("tcp", s.Config.HTTPD.HTTPBindAddress)
	if err != nil {
		return fmt.Errorf("listen: %s", err)
	}
	s.listener = ln

	s.mux = cmux.New(s.listener)
	s.httpMux = s.mux.Match(cmux.HTTP1Fast())
	s.raftMux = network.ListenString(s.mux, RaftMuxHeader)

	h := NewHandler(s.Config.HTTPD)
	h.Version = "0.0.0"
	h.logger = s.logger
	h.store = s.store
	h.Open()

	s.httpHandler = h

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

	s.httpServer = &http.Server{Addr: s.Config.HTTPD.HTTPBindAddress, Handler: srv}

	go utils.WithRecovery(func() {
		err := s.httpServer.Serve(s.httpMux)
		s.logger.Error("http server error", zap.Error(err))
	}, nil)

	if err := s.mux.Serve(); err != nil {
		s.logger.Error("start http/tcp server error", zap.Error(err))
	}
}

func (s *Server) remoteAddr(addr string) string {
	hostname := s.Config.Hostname
	if hostname == "" {
		hostname = DefaultHostname
	}
	remote, err := DefaultHost(hostname, addr)
	if err != nil {
		return addr
	}
	return remote
}
func autoAssignPort(addr string) bool {
	_, p, _ := net.SplitHostPort(addr)
	return p == "0"
}

func combineHostAndAssignedPort(ln net.Listener, autoAddr string) (string, error) {
	host, _, err := net.SplitHostPort(autoAddr)
	if err != nil {
		return "", err
	}
	_, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		return "", err
	}
	return net.JoinHostPort(host, port), nil
}
