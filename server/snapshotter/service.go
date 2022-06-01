// Package snapshotter provides the meta snapshot service.
package snapshotter

import (
	"archive/tar"
	"bytes"
	"encoding"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/cnosdb/cnosdb"
	"github.com/cnosdb/cnosdb/meta"
	"github.com/cnosdb/cnosdb/pkg/network"
	"github.com/cnosdb/cnosdb/vend/db/tsdb"

	"go.uber.org/zap"
)

const (
	// MuxHeader is the header byte used for the TCP muxer.
	MuxHeader = "snapshotter"

	// BackupMagicHeader is the first 8 bytes used to identify and validate
	// a metastore backup file
	BackupMagicHeader = 0x6b6d657461 //kmeta
)

// Service manages the listener for the snapshot endpoint.
type Service struct {
	wg sync.WaitGroup

	Node *cnosdb.Node

	MetaClient interface {
		encoding.BinaryMarshaler
		Database(name string) *meta.DatabaseInfo
		Data() meta.Data
		SetData(data *meta.Data) error
		TruncateShardGroups(t time.Time) error
		UpdateShardOwners(shardID uint64, addOwners []uint64, delOwners []uint64) error
	}

	TSDBStore interface {
		BackupShard(id uint64, since time.Time, w io.Writer) error
		ExportShard(id uint64, ExportStart time.Time, ExportEnd time.Time, w io.Writer) error
		Shard(id uint64) *tsdb.Shard
		ShardRelativePath(id uint64) (string, error)
		SetShardEnabled(shardID uint64, enabled bool) error
		RestoreShard(id uint64, r io.Reader) error
		CreateShard(database, retentionPolicy string, shardID uint64, enabled bool) error
		DeleteShard(shardID uint64) error
	}

	Listener      net.Listener
	Logger        *zap.Logger
	copyingShards map[string]*Record
}

type Record struct {
	quit chan int
}

// NewService returns a new instance of Service.
func NewService() *Service {
	return &Service{
		Logger:        zap.NewNop(),
		copyingShards: make(map[string]*Record),
	}
}

// Open starts the service.
func (s *Service) Open() error {
	s.Logger.Info("Starting snapshot service")

	s.wg.Add(1)
	go s.serve()
	return nil
}

// Close implements the Service interface.
func (s *Service) Close() error {
	if s.Listener != nil {
		if err := s.Listener.Close(); err != nil {
			return err
		}
	}
	s.wg.Wait()
	return nil
}

// WithLogger sets the logger on the service.
func (s *Service) WithLogger(log *zap.Logger) {
	s.Logger = log.With(zap.String("service", "snapshot"))
}

// serve serves snapshot requests from the listener.
func (s *Service) serve() {
	defer s.wg.Done()

	s.Logger.Info("snapshotter service start")

	for {
		// Wait for next connection.
		conn, err := s.Listener.Accept()
		if err != nil && strings.Contains(err.Error(), "connection closed") {
			s.Logger.Info("Listener closed")
			return
		} else if err != nil {
			s.Logger.Info("Error accepting snapshot request", zap.Error(err))
			continue
		}

		// Handle connection in separate goroutine.
		s.wg.Add(1)
		go func(conn net.Conn) {
			defer s.wg.Done()
			defer conn.Close()
			if err := s.handleConn(conn); err != nil {
				s.Logger.Info("snapshotter service handle conn error", zap.Error(err))
			}
		}(conn)
	}
}

// handleConn processes conn. This is run in a separate goroutine.
func (s *Service) handleConn(conn net.Conn) error {
	var typ [1]byte

	if _, err := io.ReadFull(conn, typ[:]); err != nil {
		return err
	}

	if RequestType(typ[0]) == RequestShardUpdate {
		return s.updateShardsLive(conn)
	}

	r, bytes, err := s.readRequest(conn)
	if err != nil {
		return fmt.Errorf("read request: %s", err)
	}

	switch RequestType(typ[0]) {
	case RequestShardBackup:
		if err := s.TSDBStore.BackupShard(r.ShardID, r.Since, conn); err != nil {
			return err
		}
	case RequestShardExport:
		if err := s.TSDBStore.ExportShard(r.ShardID, r.ExportStart, r.ExportEnd, conn); err != nil {
			return err
		}
	case RequestMetastoreBackup:
		if err := s.writeMetaStore(conn); err != nil {
			return err
		}
	case RequestDatabaseInfo:
		return s.writeDatabaseInfo(conn, r.BackupDatabase)
	case RequestRetentionPolicyInfo:
		return s.writeRetentionPolicyInfo(conn, r.BackupDatabase, r.BackupRetentionPolicy)
	case RequestMetaStoreUpdate:
		return s.updateMetaStore(conn, bytes, r.BackupDatabase, r.RestoreDatabase, r.BackupRetentionPolicy, r.RestoreRetentionPolicy)
	case RequestCopyShard:
		return s.copyShardToDest(conn, r.CopyShardDestHost, r.ShardID)
	case RequestRemoveShard:
		return s.removeShardCopy(conn, r.ShardID)
	case RequestTruncateShards:
		return s.truncateShardGroups(conn, r.DelaySecond)
	case RequestKillCopyShard:
		return s.killCopyShard(conn, r.CopyShardDestHost, r.ShardID)
	default:
		return fmt.Errorf("snapshotter request type unknown: %v", r.Type)
	}

	return nil
}

func (s *Service) updateShardsLive(conn net.Conn) error {
	var sidBytes [8]byte
	if _, err := io.ReadFull(conn, sidBytes[:]); err != nil {
		return err
	}
	sid := binary.BigEndian.Uint64(sidBytes[:])

	metaData := s.MetaClient.Data()
	dbName, rp, _ := metaData.ShardDBRetentionAndInfo(sid)
	if err := s.TSDBStore.CreateShard(dbName, rp, sid, true); err != nil {
		return err
	}

	if err := s.TSDBStore.SetShardEnabled(sid, false); err != nil {
		return err
	}
	defer s.TSDBStore.SetShardEnabled(sid, true)

	return s.TSDBStore.RestoreShard(sid, conn)
}

func (s *Service) updateMetaStore(conn net.Conn, bits []byte, backupDBName, restoreDBName, backupRPName, restoreRPName string) error {
	md := meta.Data{}
	err := md.UnmarshalBinary(bits)
	if err != nil {
		if err := s.respondIDMap(conn, map[uint64]uint64{}); err != nil {
			return err
		}
		return fmt.Errorf("failed to decode meta: %s", err)
	}

	data := s.MetaClient.Data()

	IDMap, newDBs, err := data.ImportData(md, backupDBName, restoreDBName, backupRPName, restoreRPName)
	if err != nil {
		if err := s.respondIDMap(conn, map[uint64]uint64{}); err != nil {
			return err
		}
		return err
	}

	err = s.MetaClient.SetData(&data)
	if err != nil {
		return err
	}

	err = s.createNewDBShards(data, newDBs)
	if err != nil {
		return err
	}

	err = s.respondIDMap(conn, IDMap)
	return err
}

// copy a shard to remote host
func (s *Service) copyShardToDest(conn net.Conn, destHost string, shardID uint64) error {
	localAddr := s.Listener.Addr().String()
	s.Logger.Info("copy shard command ",
		zap.String("Local", localAddr),
		zap.String("Dest", destHost),
		zap.Uint64("ShardID", shardID))

	data := s.MetaClient.Data()
	info := data.DataNodeByAddr(destHost)
	if info == nil {
		io.WriteString(conn, "Can't found data node: "+destHost)
		return nil
	}

	dbName, rp, shardInfo := data.ShardDBRetentionAndInfo(shardID)

	if !shardInfo.OwnedBy(s.Node.ID) {
		io.WriteString(conn, "Can't found shard in: "+localAddr)
		return nil
	}

	if shardInfo.OwnedBy(info.ID) {
		io.WriteString(conn, fmt.Sprintf("Shard %d already in %s", shardID, destHost))
		return nil
	}

	key := fmt.Sprintf("%s_%s_%d", localAddr, destHost, shardID)
	if _, ok := s.copyingShards[key]; ok {
		io.WriteString(conn, fmt.Sprintf("The Shard %d from %s to %s is copying, please wait", shardID, localAddr, destHost))
		return nil
	}

	quit := make(chan int, 1)
	s.copyingShards[key] = &Record{quit: quit}
	go func(quit chan int) {
		reader, writer := io.Pipe()
		defer reader.Close()

		go func() {
			defer writer.Close()
			if err := s.TSDBStore.BackupShard(shardID, time.Time{}, writer); err != nil {
				s.Logger.Error("Error backup shard", zap.Error(err))
			}
		}()
		go func() {
			tr := tar.NewReader(reader)
			client := NewClient(destHost)
			if err := client.UploadShard(shardID, shardID, dbName, rp, tr); err != nil {
				s.Logger.Error("Error upload shard", zap.Error(err))
				return
			}

			if err := s.MetaClient.UpdateShardOwners(shardID, []uint64{info.ID}, nil); err != nil {
				s.Logger.Error("Error update owner", zap.Error(err))
				return
			}

			s.Logger.Info("Success Copy Shard ", zap.Uint64("ShardID", shardID), zap.String("Host", destHost))
			delete(s.copyingShards, key)
			quit <- 1
			close(quit)
		}()

		select {
		case _, ok := <-quit:
			s.Logger.Info("receive a quit single", zap.Bool("Exit normally", ok), zap.Uint64("ShardID", shardID), zap.String("Host", destHost))
			return
		}

	}(quit)

	io.WriteString(conn, "Copying ......")

	return nil
}

// kill a copy-shard command
func (s *Service) killCopyShard(conn net.Conn, destHost string, shardID uint64) error {
	localAddr := s.Listener.Addr().String()
	s.Logger.Info("kill copy shard command ",
		zap.String("Local", localAddr),
		zap.String("Dest", destHost),
		zap.Uint64("ShardID", shardID))

	data := s.MetaClient.Data()
	info := data.DataNodeByAddr(destHost)
	if info == nil {
		io.WriteString(conn, "Can't found data node: "+destHost)
		return nil
	}

	key := fmt.Sprintf("%s_%s_%d", localAddr, destHost, shardID)
	if _, ok := s.copyingShards[key]; !ok {
		io.WriteString(conn, fmt.Sprintf("The Copy Shard %d from %s to %s is not exist", shardID, localAddr, destHost))
		return nil
	}

	record := s.copyingShards[key]
	if record == nil {
		io.WriteString(conn, fmt.Sprintf("The Copy Shard %d from %s to %s is not exist or it's finished", shardID, localAddr, destHost))
		return nil
	}

	close(record.quit)

	delete(s.copyingShards, key)

	request := &Request{
		Type:    RequestRemoveShard,
		ShardID: shardID,
	}

	destconn, err := network.Dial("tcp", destHost, MuxHeader)
	if err != nil {
		return err
	}
	defer destconn.Close()

	_, err = destconn.Write([]byte{byte(request.Type)})
	if err != nil {
		return err
	}

	if err := json.NewEncoder(destconn).Encode(request); err != nil {
		return fmt.Errorf("encode snapshot request: %s", err)
	}

	bytes, err := ioutil.ReadAll(destconn)
	if string(bytes) != "Success " || err != nil {
		io.WriteString(conn, "The Copy Shard is killed, but delete shard in the destHost failed: "+err.Error())
		return nil
	}

	io.WriteString(conn, "Kill Copy Shard Succeeded")

	return nil
}

// remove a shard replication
func (s *Service) removeShardCopy(conn net.Conn, shardID uint64) error {
	localAddr := s.Listener.Addr().String()
	s.Logger.Info("copy shard command ",
		zap.String("Local", localAddr),
		zap.Uint64("ShardID", shardID))

	if err := s.TSDBStore.DeleteShard(shardID); err != nil {
		io.WriteString(conn, err.Error())
		return err
	}

	if err := s.MetaClient.UpdateShardOwners(shardID, nil, []uint64{s.Node.ID}); err != nil {
		io.WriteString(conn, err.Error())
		return err
	}
	io.WriteString(conn, "Success ")
	return nil
}

// remove a shard replication
func (s *Service) truncateShardGroups(conn net.Conn, delaySecond int) error {
	localAddr := s.Listener.Addr().String()
	s.Logger.Info("truncate shards command ",
		zap.String("Local", localAddr),
		zap.Int("Delay second", delaySecond))

	timestamp := time.Now().Add(time.Duration(delaySecond) * time.Second).UTC()
	if err := s.MetaClient.TruncateShardGroups(timestamp); err != nil {
		io.WriteString(conn, err.Error())
		return err
	}
	io.WriteString(conn, "Success ")
	return nil
}

// iterate over a list of newDB's that should have just been added to the metadata
// If the db was not created in the metadata return an error.
// None of the shards should exist on a new DB, and CreateShard protects against double-creation.
func (s *Service) createNewDBShards(data meta.Data, newDBs []string) error {
	for _, restoreDBName := range newDBs {
		dbi := data.Database(restoreDBName)
		if dbi == nil {
			return fmt.Errorf("db %s not found when creating new db shards", restoreDBName)
		}
		for _, rpi := range dbi.RetentionPolicies {
			for _, sgi := range rpi.ShardGroups {
				for _, shard := range sgi.Shards {
					err := s.TSDBStore.CreateShard(restoreDBName, rpi.Name, shard.ID, true)
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

// send the IDMapping based on the metadata from the source server vs the shard ID
// metadata on this server.  Sends back [BackupMagicHeader,0] if there's no mapped
// values, signaling that nothing should be imported.
func (s *Service) respondIDMap(conn net.Conn, IDMap map[uint64]uint64) error {
	npairs := len(IDMap)
	// 2 information ints, then npairs of 8byte ints.
	numBytes := make([]byte, (npairs+1)*16)

	binary.BigEndian.PutUint64(numBytes[:8], BackupMagicHeader)
	binary.BigEndian.PutUint64(numBytes[8:16], uint64(npairs))
	next := 16
	for k, v := range IDMap {
		binary.BigEndian.PutUint64(numBytes[next:next+8], k)
		binary.BigEndian.PutUint64(numBytes[next+8:next+16], v)
		next += 16
	}

	_, err := conn.Write(numBytes[:])
	return err
}

func (s *Service) writeMetaStore(conn net.Conn) error {
	// Retrieve and serialize the current meta data.
	metaBlob, err := s.MetaClient.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal meta: %s", err)
	}

	var nodeBytes bytes.Buffer
	if err := json.NewEncoder(&nodeBytes).Encode(s.Node); err != nil {
		return err
	}

	var numBytes [24]byte

	binary.BigEndian.PutUint64(numBytes[:8], BackupMagicHeader)
	binary.BigEndian.PutUint64(numBytes[8:16], uint64(len(metaBlob)))
	binary.BigEndian.PutUint64(numBytes[16:24], uint64(nodeBytes.Len()))

	// backup header followed by meta blob length
	if _, err := conn.Write(numBytes[:16]); err != nil {
		return err
	}

	if _, err := conn.Write(metaBlob); err != nil {
		return err
	}

	if _, err := conn.Write(numBytes[16:24]); err != nil {
		return err
	}

	if _, err := nodeBytes.WriteTo(conn); err != nil {
		return err
	}
	return nil
}

// writeDatabaseInfo will write the relative paths of all shards in the database on
// this server into the connection.
func (s *Service) writeDatabaseInfo(conn net.Conn, database string) error {
	res := Response{}
	dbs := []meta.DatabaseInfo{}
	if database != "" {
		db := s.MetaClient.Database(database)
		if db == nil {
			return cnosdb.ErrDatabaseNotFound(database)
		}
		dbs = append(dbs, *db)
	} else {
		// we'll allow collecting info on all databases
		dbs = s.MetaClient.Data().Databases
	}

	for _, db := range dbs {
		for _, rp := range db.RetentionPolicies {
			for _, sg := range rp.ShardGroups {
				for _, sh := range sg.Shards {
					// ignore if the shard isn't on the server
					if s.TSDBStore.Shard(sh.ID) == nil {
						continue
					}

					path, err := s.TSDBStore.ShardRelativePath(sh.ID)
					if err != nil {
						return err
					}

					res.Paths = append(res.Paths, path)
				}
			}
		}
	}
	if err := json.NewEncoder(conn).Encode(res); err != nil {
		return fmt.Errorf("encode response: %s", err.Error())
	}

	return nil
}

// writeDatabaseInfo will write the relative paths of all shards in the retention policy on
// this server into the connection
func (s *Service) writeRetentionPolicyInfo(conn net.Conn, database, retentionPolicy string) error {
	res := Response{}
	db := s.MetaClient.Database(database)
	if db == nil {
		return cnosdb.ErrDatabaseNotFound(database)
	}

	var ret *meta.RetentionPolicyInfo

	for _, rp := range db.RetentionPolicies {
		if rp.Name == retentionPolicy {
			ret = &rp
			break
		}
	}

	if ret == nil {
		return cnosdb.ErrRetentionPolicyNotFound(retentionPolicy)
	}

	for _, sg := range ret.ShardGroups {
		for _, sh := range sg.Shards {
			// ignore if the shard isn't on the server
			if s.TSDBStore.Shard(sh.ID) == nil {
				continue
			}

			path, err := s.TSDBStore.ShardRelativePath(sh.ID)
			if err != nil {
				return err
			}

			res.Paths = append(res.Paths, path)
		}
	}

	if err := json.NewEncoder(conn).Encode(res); err != nil {
		return fmt.Errorf("encode resonse: %s", err.Error())
	}

	return nil
}

// readRequest unmarshals a request object from the conn.
func (s *Service) readRequest(conn net.Conn) (Request, []byte, error) {
	var r Request
	d := json.NewDecoder(conn)

	if err := d.Decode(&r); err != nil {
		return r, nil, err
	}

	bits := make([]byte, r.UploadSize+1)

	if r.UploadSize > 0 {

		remainder := d.Buffered()

		n, err := remainder.Read(bits)
		if err != nil && err != io.EOF {
			return r, bits, err
		}

		// it is a bit random but sometimes the Json decoder will consume all the bytes and sometimes
		// it will leave a few behind.
		if err != io.EOF && n < int(r.UploadSize+1) {
			_, err = conn.Read(bits[n:])
		}

		if err != nil && err != io.EOF {
			return r, bits, err
		}
		// the JSON encoder on the client side seems to write an extra byte, so trim that off the front.
		return r, bits[1:], nil
	}

	return r, bits, nil
}

// RequestType indicates the typeof snapshot request.
type RequestType uint8

const (
	// RequestShardBackup represents a request for a shard backup.
	RequestShardBackup RequestType = iota

	// RequestMetastoreBackup represents a request to back up the metastore.
	RequestMetastoreBackup

	// RequestSeriesFileBackup represents a request to back up the database series file.
	RequestSeriesFileBackup

	// RequestDatabaseInfo represents a request for database info.
	RequestDatabaseInfo

	// RequestRetentionPolicyInfo represents a request for retention policy info.
	RequestRetentionPolicyInfo

	// RequestShardExport represents a request to export Shard data.  Similar to a backup, but shards
	// may be filtered based on the start/end times on each block.
	RequestShardExport

	// RequestMetaStoreUpdate represents a request to upload a metafile that will be used to do a live update
	// to the existing metastore.
	RequestMetaStoreUpdate

	// RequestShardUpdate will initiate the upload of a shard data tar file
	// and have the engine import the data.
	RequestShardUpdate

	// RequestCopyShard represents a request for copy a shard to dest host
	RequestCopyShard

	// RequestRemoveShard represents a request for remove a shard copy
	RequestRemoveShard

	RequestCopyShardStatus
	RequestKillCopyShard
	RequestTruncateShards
)

// Request represents a request for a specific backup or for information
// about the shards on this server for a database or retention policy.
type Request struct {
	Type                   RequestType
	CopyShardDestHost      string
	BackupDatabase         string
	RestoreDatabase        string
	BackupRetentionPolicy  string
	RestoreRetentionPolicy string
	ShardID                uint64
	Since                  time.Time
	ExportStart            time.Time
	ExportEnd              time.Time
	UploadSize             int64
	DelaySecond            int
}

// Response contains the relative paths for all the shards on this server
// that are in the requested database or retention policy.
type Response struct {
	Paths []string
}

type CopyShardInfo struct {
	ShardID   uint64
	SrcHost   string
	DestHost  string
	Database  string
	Retention string
	Status    string
	StartTime time.Time
}
