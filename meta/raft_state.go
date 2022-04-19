package meta

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cnosdb/cnosdb/pkg/network"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"go.uber.org/zap"
)

// Raft configuration.
const (
	raftLogCacheSize      = 512
	raftSnapshotsRetained = 2
	raftTransportMaxPool  = 3
	raftTransportTimeout  = 10 * time.Second
)

// raftState is a consensus strategy that uses a local raft implementation for
// consensus operations.
type raftState struct {
	wg        sync.WaitGroup
	config    *ServerConfig
	closing   chan struct{}
	raft      *raft.Raft
	transport *raft.NetworkTransport
	raftStore *raftboltdb.BoltStore
	raftLayer *raftLayer
	ln        net.Listener
	addr      string
	logger    *zap.Logger
	path      string
}

func newRaftState(c *ServerConfig, addr string) *raftState {
	return &raftState{
		config: c,
		addr:   addr,
		logger: zap.NewNop(),
	}
}

func (r *raftState) withLogger(log *zap.Logger) {
	r.logger = log.With(zap.String("service", "raft-state"))
}

func (r *raftState) open(s *store, ln net.Listener) error {
	r.ln = ln
	r.closing = make(chan struct{})

	// Setup raft configuration.
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(s.raftAddr)
	config.LogOutput = ioutil.Discard

	if r.config.ClusterTracing {
		//config.Logger = hclog.New(&hclog.LoggerOptions{
		//	Name:   "raft-state",
		//	Level:  hclog.Info,
		//	Output: zap.NewStdLog(r.logger).Writer(),
		//})
		config.Logger = &hclogWrapper{
			logger: r.logger,
			name:   "raft-state",
			level:  hclog.Info,
		}
	}
	config.HeartbeatTimeout = time.Duration(r.config.HeartbeatTimeout)
	config.ElectionTimeout = time.Duration(r.config.ElectionTimeout)
	config.LeaderLeaseTimeout = time.Duration(r.config.LeaderLeaseTimeout)
	config.CommitTimeout = time.Duration(r.config.CommitTimeout)
	// Since we actually never call `removePeer` this is safe.
	// If in the future we decide to call remove peer we have to re-evaluate how to handle this
	config.ShutdownOnRemove = false

	// Build raft layer to multiplex listener.
	r.raftLayer = newRaftLayer(r.addr, r.ln)

	// Create a transport layer
	r.transport = raft.NewNetworkTransport(r.raftLayer, 3, 10*time.Second, config.LogOutput)

	// Create the log store and stable store.
	store, err := raftboltdb.NewBoltStore(filepath.Join(r.path, "raft.db"))
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}
	r.raftStore = store

	// Create the snapshot store.
	snapshots, err := raft.NewFileSnapshotStore(r.path, raftSnapshotsRetained, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Create raft log.
	ra, err := raft.NewRaft(config, (*storeFSM)(s), store, store, snapshots, r.transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	r.raft = ra

	if configFuture := ra.GetConfiguration(); configFuture.Error() != nil {
		r.logger.Info("failed to get raft configuration", zap.Error(configFuture.Error()))
		return configFuture.Error()
	} else {
		newConfig := configFuture.Configuration()
		if newConfig.Servers == nil || len(newConfig.Servers) == 0 {
			r.logger.Info("bootstrap needed")
			configuration := raft.Configuration{
				Servers: []raft.Server{
					{
						ID:      config.LocalID,
						Address: r.transport.LocalAddr(),
					},
				},
			}
			r.logger.Info("bootstrapping new raft cluster")
			ra.BootstrapCluster(configuration)
		} else {
			r.logger.Info("no bootstrap needed")
		}
	}

	r.wg.Add(1)
	go r.logLeaderChanges()

	return nil
}

func (r *raftState) logLeaderChanges() {
	defer r.wg.Done()
	// Logs our current state (Node at 1.2.3.4:8088 [Follower])
	r.logger.Info(r.raft.String())

	for {
		select {
		case <-r.closing:
			return
		case <-r.raft.LeaderCh():
			peers, err := r.peers()
			if err != nil {
				r.logger.Info("failed to lookup peers", zap.Error(err))
			}
			r.logger.Info(r.raft.String(), zap.Strings("peers", peers))
		}
	}
}

func (r *raftState) close() error {
	if r == nil {
		return nil
	}
	if r.closing != nil {
		close(r.closing)
	}
	r.wg.Wait()

	if r.transport != nil {
		r.transport.Close()
		r.transport = nil
	}

	// Shutdown raft.
	if r.raft != nil {
		if err := r.raft.Shutdown().Error(); err != nil {
			return err
		}
		r.raft = nil
	}

	if r.raftStore != nil {
		r.raftStore.Close()
		r.raftStore = nil
	}

	return nil
}

// apply applies a serialized command to the raft log.
func (r *raftState) apply(b []byte) error {
	// Apply to raft log.
	f := r.raft.Apply(b, 0)
	if err := f.Error(); err != nil {
		return err
	}

	// Return response if it's an error.
	// No other non-nil objects should be returned.
	resp := f.Response()
	if err, ok := resp.(error); ok {
		return err
	}
	if resp != nil {
		panic(fmt.Sprintf("unexpected response: %#v", resp))
	}

	return nil
}

func (r *raftState) lastIndex() uint64 {
	return r.raft.LastIndex()
}

func (r *raftState) snapshot() error {
	future := r.raft.Snapshot()
	return future.Error()
}

// addVoter instead of addPeer, adds addr to the list of peers in the cluster.
func (r *raftState) addVoter(addr string) error {
	serverAddr := raft.ServerAddress(addr)

	var servers []raft.Server
	if configFuture := r.raft.GetConfiguration(); configFuture.Error() != nil {
		r.logger.Info("failed to get raft configuration", zap.Error(configFuture.Error()))
		return configFuture.Error()
	} else {
		servers = configFuture.Configuration().Servers
	}

	for _, srv := range servers {
		if srv.Address == serverAddr {
			return nil
		}
	}

	if fut := r.raft.AddVoter(raft.ServerID(addr), raft.ServerAddress(addr), 0, 0); fut.Error() != nil {
		return fut.Error()
	}
	return nil
}

// removeVoter instead of removePeer removes addr from the list of peers in the cluster.
func (r *raftState) removeVoter(addr string) error {
	// Only do this on the leader
	if !r.isLeader() {
		return raft.ErrNotLeader
	}

	serverAddr := raft.ServerAddress(addr)

	var servers []raft.Server
	if cfu := r.raft.GetConfiguration(); cfu.Error() != nil {
		r.logger.Info("failed to get raft configuration", zap.Error(cfu.Error()))
		return cfu.Error()
	} else {
		servers = cfu.Configuration().Servers
	}

	var srv raft.Server
	var exists bool
	for _, srv = range servers {
		if srv.Address == serverAddr {
			exists = true
			break
		}
	}

	if !exists {
		return nil
	}

	if fut := r.raft.RemoveServer(srv.ID, 0, 0); fut.Error() != nil {
		return fut.Error()
	}
	return nil
}

func (r *raftState) peers() ([]string, error) {

	if configFuture := r.raft.GetConfiguration(); configFuture.Error() != nil {
		r.logger.Info("failed to get raft configuration", zap.Error(configFuture.Error()))
		return []string{}, configFuture.Error()
	} else {
		peers := []string{}
		for _, srv := range configFuture.Configuration().Servers {
			peers = append(peers, string(srv.Address))
		}
		return peers, nil
	}
}

func (r *raftState) leader() string {
	if r.raft == nil {
		return ""
	}

	return string(r.raft.Leader())
}

func (r *raftState) isLeader() bool {
	if r.raft == nil {
		return false
	}
	return r.raft.State() == raft.Leader
}

// raftLayer wraps the connection so it can be re-used for forwarding.
type raftLayer struct {
	addr   *raftLayerAddr
	ln     net.Listener
	conn   chan net.Conn
	closed chan struct{}
}

type raftLayerAddr struct {
	addr string
}

func (r *raftLayerAddr) Network() string {
	return "tcp"
}

func (r *raftLayerAddr) String() string {
	return r.addr
}

// newRaftLayer returns a new instance of raftLayer.
func newRaftLayer(addr string, ln net.Listener) *raftLayer {
	return &raftLayer{
		addr:   &raftLayerAddr{addr},
		ln:     ln,
		conn:   make(chan net.Conn),
		closed: make(chan struct{}),
	}
}

// Addr returns the local address for the layer.
func (l *raftLayer) Addr() net.Addr {
	return l.addr
}

// Dial creates a new network connection.
func (l *raftLayer) Dial(addr raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	//return net.DialTimeout("tcp", string(addr), timeout)
	return network.DialTimeout("tcp", string(addr), RaftMuxHeader, timeout)
}

// Accept waits for the next connection.
func (l *raftLayer) Accept() (net.Conn, error) {
	return l.ln.Accept()
}

// Close closes the layer.
func (l *raftLayer) Close() error { return l.ln.Close() }

type hclogWrapper struct {
	logger *zap.Logger
	name   string
	level  hclog.Level
}

func (l *hclogWrapper) renderSlice(v reflect.Value) string {
	var buf bytes.Buffer

	buf.WriteRune('[')

	for i := 0; i < v.Len(); i++ {
		if i > 0 {
			buf.WriteString(", ")
		}

		sv := v.Index(i)

		var val string

		switch sv.Kind() {
		case reflect.String:
			val = sv.String()
		case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64:
			val = strconv.FormatInt(sv.Int(), 10)
		case reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			val = strconv.FormatUint(sv.Uint(), 10)
		default:
			val = fmt.Sprintf("%v", sv.Interface())
		}

		if strings.ContainsAny(val, " \t\n\r") {
			buf.WriteByte('"')
			buf.WriteString(val)
			buf.WriteByte('"')
		} else {
			buf.WriteString(val)
		}
	}

	buf.WriteRune(']')

	return buf.String()
}

func (l *hclogWrapper) renderArgs(args ...interface{}) (hclog.CapturedStacktrace, []zap.Field) {
	var logFields []zap.Field

	var stacktrace hclog.CapturedStacktrace

	if args != nil && len(args) > 0 {
		if len(args)%2 != 0 {
			cs, ok := args[len(args)-1].(hclog.CapturedStacktrace)
			if ok {
				args = args[:len(args)-1]
				stacktrace = cs
			} else {
				extra := args[len(args)-1]
				args = append(args[:len(args)-1], hclog.MissingKey, extra)
			}
		}

	FOR:
		for i := 0; i < len(args); i = i + 2 {
			var (
				val zap.Field
			)

			var key string
			switch st := args[i].(type) {
			case string:
				key = st
			default:
				key = fmt.Sprintf("%s", st)
			}

			switch st := args[i+1].(type) {
			case string:
				val = zap.String(key, st)
			case int:
				val = zap.Int(key, st)
			case int64:
				val = zap.Int64(key, st)
			case int32:
				val = zap.Int32(key, st)
			case int16:
				val = zap.Int16(key, st)
			case int8:
				val = zap.Int8(key, st)
			case uint:
				val = zap.Uint(key, st)
			case uint64:
				val = zap.Uint64(key, st)
			case uint32:
				val = zap.Uint32(key, st)
			case uint16:
				val = zap.Uint16(key, st)
			case uint8:
				val = zap.Uint8(key, st)
			case hclog.CapturedStacktrace:
				stacktrace = st
				continue FOR
			case hclog.Format:
				val = zap.String(key, fmt.Sprintf(st[0].(string), st[1:]...))
			default:
				v := reflect.ValueOf(st)
				if v.Kind() == reflect.Slice {
					val = zap.String(key, l.renderSlice(v))
				} else {
					val = zap.String(key, fmt.Sprintf("%v", st))
				}
			}

			logFields = append(logFields, val)
		}
	}

	return stacktrace, logFields
}

func (l *hclogWrapper) Log(level hclog.Level, msg string, args ...interface{}) {
	stacktrace, logFields := l.renderArgs(args)

	switch level {
	case hclog.Trace, hclog.Debug:
		l.logger.Debug(msg, logFields...)

		if stacktrace != "" {
			l.logger.Debug(string(stacktrace))
		}
	case hclog.Info:
		l.logger.Info(msg, logFields...)

		if stacktrace != "" {
			l.logger.Info(string(stacktrace))
		}
	case hclog.Warn:
		l.logger.Warn(msg, logFields...)

		if stacktrace != "" {
			l.logger.Warn(string(stacktrace))
		}
	case hclog.Error:
		l.logger.Error(msg, logFields...)

		if stacktrace != "" {
			l.logger.Error(string(stacktrace))
		}
	default:
		l.logger.Info(msg, logFields...)

		if stacktrace != "" {
			l.logger.Info(string(stacktrace))
		}
	}
}

func (l *hclogWrapper) Trace(msg string, args ...interface{}) {
	l.Log(hclog.Trace, msg, args...)
}

func (l *hclogWrapper) Debug(msg string, args ...interface{}) {
	l.Log(hclog.Debug, msg, args...)
}

func (l *hclogWrapper) Info(msg string, args ...interface{}) {
	l.Log(hclog.Info, msg, args...)
}

func (l *hclogWrapper) Warn(msg string, args ...interface{}) {
	l.Log(hclog.Warn, msg, args...)
}

func (l *hclogWrapper) Error(msg string, args ...interface{}) {
	l.Log(hclog.Error, msg, args...)
}

func (l *hclogWrapper) IsTrace() bool { return l.level == hclog.Trace }

func (l *hclogWrapper) IsDebug() bool { return l.level == hclog.Debug }

func (l *hclogWrapper) IsInfo() bool { return l.level == hclog.Info }

func (l *hclogWrapper) IsWarn() bool { return l.level == hclog.Warn }

func (l *hclogWrapper) IsError() bool { return l.level == hclog.Error }

func (l *hclogWrapper) ImpliedArgs() []interface{} { return []interface{}{} }

func (l *hclogWrapper) With(args ...interface{}) hclog.Logger {
	_, fields := l.renderArgs(args...)
	return &hclogWrapper{
		logger: l.logger.With(fields...),
	}
}

func (l *hclogWrapper) Name() string { return l.name }

func (l *hclogWrapper) Named(name string) hclog.Logger {
	sl := *l

	if sl.name != "" {
		sl.name = sl.name + "." + name
	} else {
		sl.name = name
	}

	return &sl
}

func (l *hclogWrapper) ResetNamed(name string) hclog.Logger {
	sl := *l

	sl.name = name

	return &sl
}

func (l *hclogWrapper) SetLevel(level hclog.Level) {
	atomic.StoreInt32((*int32)(&l.level), int32(level))
}

func (l *hclogWrapper) StandardLogger(opts *hclog.StandardLoggerOptions) *log.Logger {
	return zap.NewStdLog(l.logger)
}

func (l *hclogWrapper) StandardWriter(opts *hclog.StandardLoggerOptions) io.Writer {
	return ioutil.Discard
}
