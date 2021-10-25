package meta

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/cnosdatabase/cnosdb"
	internal "github.com/cnosdatabase/cnosdb/meta/internal"
	"github.com/cnosdatabase/cnosql"
	"github.com/cnosdatabase/db/models"
	"github.com/cnosdatabase/db/query"
	"github.com/gogo/protobuf/proto"
)

//go:generate protoc --gogo_out=. internal/meta.proto

const (
	// DefaultTimeToLiveReplicaN is the default value of TimeToLiveInfo.ReplicaN.
	DefaultTimeToLiveReplicaN = 1

	// DefaultTimeToLiveDuration is the default value of TimeToLiveInfo.Duration.
	DefaultTimeToLiveDuration = time.Duration(0)

	// DefaultTimeToLiveName is the default name for auto generated time-to-lives.
	DefaultTimeToLiveName = "autogen"

	// MinTimeToLiveDuration represents the minimum duration for a time-to-live.
	MinTimeToLiveDuration = time.Hour

	// MaxNameLen is the maximum length of a database or time-to-live name.
	// CnosDB uses the name for the directory name on disk.
	MaxNameLen = 255
)

// Data represents the top level collection of all metadata.
type Data struct {
	Term      uint64 // associated raft term
	Index     uint64 // associated raft index
	ClusterID uint64
	MetaNodes []NodeInfo
	DataNodes []NodeInfo
	Databases []DatabaseInfo
	Users     []UserInfo

	// adminUserExists provides a constant time mechanism for determining
	// if there is at least one admin user.
	adminUserExists bool

	MaxNodeID   uint64
	MaxRegionID uint64
	MaxShardID  uint64
}

// MetaNode returns a node by id.
func (data *Data) MetaNode(id uint64) *NodeInfo {
	for i := range data.MetaNodes {
		if data.MetaNodes[i].ID == id {
			return &data.MetaNodes[i]
		}
	}
	return nil
}

// CreateMetaNode will add a new meta node to the metastore
func (data *Data) CreateMetaNode(httpAddr, tcpAddr string) error {
	// Ensure a node with the same host doesn't already exist.
	for _, n := range data.MetaNodes {
		if n.Host == httpAddr {
			return ErrNodeExists
		}
	}

	// If an existing data node exists with the same TCPHost address,
	// then these nodes are actually the same so re-use the existing ID
	var existingID uint64
	for _, n := range data.DataNodes {
		if n.TCPHost == tcpAddr {
			existingID = n.ID
			break
		}
	}

	// We didn't find and existing data node ID, so assign a new ID
	// to this meta node.
	if existingID == 0 {
		data.MaxNodeID++
		existingID = data.MaxNodeID
	}

	// Append new node.
	data.MetaNodes = append(data.MetaNodes, NodeInfo{
		ID:      existingID,
		Host:    httpAddr,
		TCPHost: tcpAddr,
	})

	sort.Sort(NodeInfos(data.MetaNodes))
	return nil
}

// SetMetaNode will update the information for the single meta
// node or create a new metanode. If there are more than 1 meta
// nodes already, an error will be returned
func (data *Data) SetMetaNode(httpAddr, tcpAddr string) error {
	if len(data.MetaNodes) > 1 {
		return fmt.Errorf("can't set meta node when there are more than 1 in the metastore")
	}

	if len(data.MetaNodes) == 0 {
		return data.CreateMetaNode(httpAddr, tcpAddr)
	}

	data.MetaNodes[0].Host = httpAddr
	data.MetaNodes[0].TCPHost = tcpAddr

	return nil
}

// DeleteMetaNode will remove the meta node from the store
func (data *Data) DeleteMetaNode(id uint64) error {
	// Node has to be larger than 0 to be real
	if id == 0 {
		return ErrNodeIDRequired
	}

	var nodes []NodeInfo
	for _, n := range data.MetaNodes {
		if n.ID == id {
			continue
		}
		nodes = append(nodes, n)
	}

	if len(nodes) == len(data.MetaNodes) {
		return ErrNodeNotFound
	}

	data.MetaNodes = nodes
	return nil
}

// DataNode returns a node by id.
func (data *Data) DataNode(id uint64) *NodeInfo {
	for i := range data.DataNodes {
		if data.DataNodes[i].ID == id {
			return &data.DataNodes[i]
		}
	}
	return nil
}

// CreateDataNode adds a node to the metadata.
func (data *Data) CreateDataNode(host, tcpHost string) error {
	// Ensure a node with the same host doesn't already exist.
	for _, n := range data.DataNodes {
		if n.TCPHost == tcpHost {
			return ErrNodeExists
		}
	}

	// If an existing meta node exists with the same TCPHost address,
	// then these nodes are actually the same so re-use the existing ID
	var existingID uint64
	for _, n := range data.MetaNodes {
		if n.TCPHost == tcpHost {
			existingID = n.ID
			break
		}
	}

	// We didn't find an existing node, so assign it a new node ID
	if existingID == 0 {
		data.MaxNodeID++
		existingID = data.MaxNodeID
	}

	// Append new node.
	data.DataNodes = append(data.DataNodes, NodeInfo{
		ID:      existingID,
		Host:    host,
		TCPHost: tcpHost,
	})
	sort.Sort(NodeInfos(data.DataNodes))

	return nil
}

// setDataNode adds a data node with a pre-specified nodeID.
// this should only be used when the cluster is upgrading from 0.9 to 0.10
func (data *Data) setDataNode(nodeID uint64, host, tcpHost string) error {
	// Ensure a node with the same host doesn't already exist.
	for _, n := range data.DataNodes {
		if n.Host == host {
			return ErrNodeExists
		}
	}

	// Append new node.
	data.DataNodes = append(data.DataNodes, NodeInfo{
		ID:      nodeID,
		Host:    host,
		TCPHost: tcpHost,
	})

	return nil
}

// DeleteDataNode removes a node from the Meta store.
//
// If necessary, DeleteDataNode reassigns ownership of any shards that
// would otherwise become orphaned by the removal of the node from the
// cluster.
func (data *Data) DeleteDataNode(id uint64) error {
	var nodes []NodeInfo

	// Remove the data node from the store's list.
	for _, n := range data.DataNodes {
		if n.ID != id {
			nodes = append(nodes, n)
		}
	}

	if len(nodes) == len(data.DataNodes) {
		return ErrNodeNotFound
	}
	data.DataNodes = nodes

	// Remove node id from all shard infos
	for di, db := range data.Databases {
		for ti, ttl := range db.TimeToLives {
			for ri, rg := range ttl.Regions {
				var (
					nodeOwnerFreqs = make(map[int]int)
					orphanedShards []ShardInfo
				)
				// Look through all shards in the region and
				// determine (1) if a shard no longer has any owners
				// (orphaned); (2) if all shards in the region
				// are orphaned; and (3) the number of shards in this
				// region owned by each data node in the cluster.
				for si, sh := range rg.Shards {
					// Track of how many shards in the region are
					// owned by each data node in the cluster.
					var nodeIdx = -1
					for oi, owner := range sh.Owners {
						if owner.NodeID == id {
							nodeIdx = oi
						}
						nodeOwnerFreqs[int(owner.NodeID)]++
					}

					if nodeIdx > -1 {
						// Data node owns shard, so relinquish ownership
						// and set new owners on the shard.
						sh.Owners = append(sh.Owners[:nodeIdx], sh.Owners[nodeIdx+1:]...)
						data.Databases[di].TimeToLives[ti].Regions[ri].Shards[si].Owners = sh.Owners
					}

					// Shard no longer owned. Will need reassigning
					// an owner.
					if len(sh.Owners) == 0 {
						orphanedShards = append(orphanedShards, sh)
					}
				}

				// Mark the region as deleted if it has no shards,
				// or all of its shards are orphaned.
				if len(rg.Shards) == 0 || len(orphanedShards) == len(rg.Shards) {
					data.Databases[di].TimeToLives[ti].Regions[ri].DeletedAt = time.Now().UTC()
					continue
				}

				// Reassign any orphaned shards. Delete the node we're
				// dropping from the list of potential new owners.
				delete(nodeOwnerFreqs, int(id))

				for _, orphan := range orphanedShards {
					newOwnerID, err := newShardOwner(orphan, nodeOwnerFreqs)
					if err != nil {
						return err
					}

					for si, s := range rg.Shards {
						if s.ID == orphan.ID {
							rg.Shards[si].Owners = append(rg.Shards[si].Owners, ShardOwner{NodeID: newOwnerID})
							data.Databases[di].TimeToLives[ti].Regions[ri].Shards = rg.Shards
							break
						}
					}

				}
			}
		}
	}
	return nil
}

// newShardOwner sets the owner of the provided shard to the data node
// that currently owns the fewest number of shards. If multiple nodes
// own the same (fewest) number of shards, then one of those nodes
// becomes the new shard owner.
func newShardOwner(s ShardInfo, ownerFreqs map[int]int) (uint64, error) {
	var (
		minId   = -1
		minFreq int
	)

	for id, freq := range ownerFreqs {
		if minId == -1 || freq < minFreq {
			minId, minFreq = int(id), freq
		}
	}

	if minId < 0 {
		return 0, fmt.Errorf("cannot reassign shard %d due to lack of data nodes", s.ID)
	}

	// Update the shard owner frequencies and set the new owner on the
	// shard.
	ownerFreqs[minId]++
	return uint64(minId), nil
}

// Database returns a DatabaseInfo by the database name.
func (data *Data) Database(name string) *DatabaseInfo {
	for i := range data.Databases {
		if data.Databases[i].Name == name {
			return &data.Databases[i]
		}
	}
	return nil
}

// CreateDatabase creates a new database.
// It returns an error if name is blank or if a database with the same name already exists.
func (data *Data) CreateDatabase(name string) error {
	if name == "" {
		return ErrDatabaseNameRequired
	} else if len(name) > MaxNameLen {
		return ErrNameTooLong
	} else if data.Database(name) != nil {
		return nil
	}

	// Append new node.
	data.Databases = append(data.Databases, DatabaseInfo{Name: name})

	return nil
}

// DropDatabase removes a database by name. It does not return an error
// if the database cannot be found.
func (data *Data) DropDatabase(name string) error {
	for i := range data.Databases {
		if data.Databases[i].Name == name {
			data.Databases = append(data.Databases[:i], data.Databases[i+1:]...)

			// Remove all user privileges associated with this database.
			for i := range data.Users {
				delete(data.Users[i].Privileges, name)
			}
			break
		}
	}
	return nil
}

// TimeToLive returns a time-to-live for a database by name.
func (data *Data) TimeToLive(database, name string) (*TimeToLiveInfo, error) {
	di := data.Database(database)
	if di == nil {
		return nil, cnosdb.ErrDatabaseNotFound(database)
	}

	for i := range di.TimeToLives {
		if di.TimeToLives[i].Name == name {
			return &di.TimeToLives[i], nil
		}
	}
	return nil, nil
}

// CreateTimeToLive creates a new time-to-live on a database.
// It returns an error if name is blank or if the database does not exist.
func (data *Data) CreateTimeToLive(database string, ttli *TimeToLiveInfo, makeDefault bool) error {
	// Validate time-to-live.
	if ttli == nil {
		return ErrTimeToLiveRequired
	} else if ttli.Name == "" {
		return ErrTimeToLiveNameRequired
	} else if len(ttli.Name) > MaxNameLen {
		return ErrNameTooLong
	} else if ttli.ReplicaN < 1 {
		return ErrReplicationFactorTooLow
	}

	// Normalise ShardDuration before comparing to any existing
	// time-to-lives. The client is supposed to do this, but
	// do it again to verify input.
	ttli.RegionDuration = normalisedShardDuration(ttli.RegionDuration, ttli.Duration)

	if ttli.Duration > 0 && ttli.Duration < ttli.RegionDuration {
		return ErrIncompatibleDurations
	}

	// Find database.
	di := data.Database(database)
	if di == nil {
		return cnosdb.ErrDatabaseNotFound(database)
	} else if ttl := di.TimeToLive(ttli.Name); ttl != nil {
		// Time-to-live with that name already exists. Make sure they're the same.
		if ttl.ReplicaN != ttli.ReplicaN || ttl.Duration != ttli.Duration || ttl.RegionDuration != ttli.RegionDuration {
			return ErrTimeToLiveExists
		}
		// if they want to make it default, and it's not the default, it's not an identical command so it's an error
		if makeDefault && di.DefaultTimeToLive != ttli.Name {
			return ErrTimeToLiveConflict
		}
		return nil
	}

	// Append copy of new time-to-live.
	di.TimeToLives = append(di.TimeToLives, *ttli)

	// Set the default if needed
	if makeDefault {
		di.DefaultTimeToLive = ttli.Name
	}

	return nil
}

// DropTimeToLive removes a time-to-live from a database by name.
func (data *Data) DropTimeToLive(database, name string) error {
	// Find database.
	di := data.Database(database)
	if di == nil {
		// no database? no problem
		return nil
	}

	// Remove from list.
	for i := range di.TimeToLives {
		if di.TimeToLives[i].Name == name {
			di.TimeToLives = append(di.TimeToLives[:i], di.TimeToLives[i+1:]...)
			break
		}
	}

	return nil
}

// TimeToLiveUpdate represents time-to-live fields to be updated.
type TimeToLiveUpdate struct {
	Name           *string
	Duration       *time.Duration
	ReplicaN       *int
	RegionDuration *time.Duration
}

// SetName sets the TimeToLiveUpdate.Name.
func (ttlu *TimeToLiveUpdate) SetName(v string) { ttlu.Name = &v }

// SetDuration sets the TimeToLiveUpdate.Duration.
func (ttlu *TimeToLiveUpdate) SetDuration(v time.Duration) { ttlu.Duration = &v }

// SetReplicaN sets the TimeToLiveUpdate.ReplicaN.
func (ttlu *TimeToLiveUpdate) SetReplicaN(v int) { ttlu.ReplicaN = &v }

// SetRegionDuration sets the TimeToLiveUpdate.RegionDuration.
func (ttlu *TimeToLiveUpdate) SetRegionDuration(v time.Duration) { ttlu.RegionDuration = &v }

// UpdateTimeToLive updates an existing time-to-live.
func (data *Data) UpdateTimeToLive(database, name string, ttlu *TimeToLiveUpdate, makeDefault bool) error {
	// Find database.
	di := data.Database(database)
	if di == nil {
		return cnosdb.ErrDatabaseNotFound(database)
	}

	// Find time-to-live.
	ttli := di.TimeToLive(name)
	if ttli == nil {
		return cnosdb.ErrTimeToLiveNotFound(name)
	}

	// Ensure new time-to-live doesn't match an existing time-to-live.
	if ttlu.Name != nil && *ttlu.Name != name && di.TimeToLive(*ttlu.Name) != nil {
		return ErrTimeToLiveNameExists
	}

	// Enforce duration of at least MinTimeToLiveDuration
	if ttlu.Duration != nil && *ttlu.Duration < MinTimeToLiveDuration && *ttlu.Duration != 0 {
		return ErrTimeToLiveDurationTooLow
	}

	// Enforce duration is at least the shard duration
	if (ttlu.Duration != nil && *ttlu.Duration > 0 &&
		((ttlu.RegionDuration != nil && *ttlu.Duration < *ttlu.RegionDuration) ||
			(ttlu.RegionDuration == nil && *ttlu.Duration < ttli.RegionDuration))) ||
		(ttlu.Duration == nil && ttli.Duration > 0 &&
			ttlu.RegionDuration != nil && ttli.Duration < *ttlu.RegionDuration) {
		return ErrIncompatibleDurations
	}

	// Update fields.
	if ttlu.Name != nil {
		ttli.Name = *ttlu.Name
	}
	if ttlu.Duration != nil {
		ttli.Duration = *ttlu.Duration
	}
	if ttlu.ReplicaN != nil {
		ttli.ReplicaN = *ttlu.ReplicaN
	}
	if ttlu.RegionDuration != nil {
		ttli.RegionDuration = normalisedShardDuration(*ttlu.RegionDuration, ttli.Duration)
	}

	if di.DefaultTimeToLive != ttli.Name && makeDefault {
		di.DefaultTimeToLive = ttli.Name
	}

	return nil
}

// SetDefaultTimeToLive sets the default time-to-live for a database.
func (data *Data) SetDefaultTimeToLive(database, name string) error {
	// Find database and verify time-to-live exists.
	di := data.Database(database)
	if di == nil {
		return cnosdb.ErrDatabaseNotFound(database)
	} else if di.TimeToLive(name) == nil {
		return cnosdb.ErrTimeToLiveNotFound(name)
	}

	// Set default time-to-live.
	di.DefaultTimeToLive = name

	return nil
}

// DropShard removes a shard by ID.
//
// DropShard won't return an error if the shard can't be found, which
// allows the command to be re-run in the case that the meta store
// succeeds but a data node fails.
func (data *Data) DropShard(id uint64) {
	found := -1
	for dbidx, dbi := range data.Databases {
		for ttlidx, ttli := range dbi.TimeToLives {
			for sgidx, rg := range ttli.Regions {
				for sidx, s := range rg.Shards {
					if s.ID == id {
						found = sidx
						break
					}
				}

				if found > -1 {
					shards := rg.Shards
					data.Databases[dbidx].TimeToLives[ttlidx].Regions[sgidx].Shards = append(shards[:found], shards[found+1:]...)

					if len(shards) == 1 {
						// We just deleted the last shard in the region.
						data.Databases[dbidx].TimeToLives[ttlidx].Regions[sgidx].DeletedAt = time.Now()
					}
					return
				}
			}
		}
	}
}

// Regions returns a list of all regions on a database and time-to-live.
func (data *Data) Regions(database, ttl string) ([]RegionInfo, error) {
	// Find time-to-live.
	ttli, err := data.TimeToLive(database, ttl)
	if err != nil {
		return nil, err
	} else if ttli == nil {
		return nil, cnosdb.ErrTimeToLiveNotFound(ttl)
	}
	regions := make([]RegionInfo, 0, len(ttli.Regions))
	for _, g := range ttli.Regions {
		if g.Deleted() {
			continue
		}
		regions = append(regions, g)
	}
	return regions, nil
}

// RegionsByTimeRange returns a list of all regions on a database and time-to-live that may contain data
// for the specified time range. Regions are sorted by start time.
func (data *Data) RegionsByTimeRange(database, ttl string, tmin, tmax time.Time) ([]RegionInfo, error) {
	// Find time-to-live.
	ttli, err := data.TimeToLive(database, ttl)
	if err != nil {
		return nil, err
	} else if ttli == nil {
		return nil, cnosdb.ErrTimeToLiveNotFound(ttl)
	}
	regions := make([]RegionInfo, 0, len(ttli.Regions))
	for _, g := range ttli.Regions {
		if g.Deleted() || !g.Overlaps(tmin, tmax) {
			continue
		}
		regions = append(regions, g)
	}
	return regions, nil
}

// RegionByTimestamp returns the region on a database and time-to-live for a given timestamp.
func (data *Data) RegionByTimestamp(database, ttl string, timestamp time.Time) (*RegionInfo, error) {
	// Find time-to-live.
	ttli, err := data.TimeToLive(database, ttl)
	if err != nil {
		return nil, err
	} else if ttli == nil {
		return nil, cnosdb.ErrTimeToLiveNotFound(ttl)
	}

	return ttli.RegionByTimestamp(timestamp), nil
}

// CreateRegion creates a region on a database and time-to-live for a given timestamp.
func (data *Data) CreateRegionDeprecated(database, ttl string, timestamp time.Time) error {
	// Find time-to-live.
	ttli, err := data.TimeToLive(database, ttl)
	if err != nil {
		return err
	} else if ttli == nil {
		return cnosdb.ErrTimeToLiveNotFound(ttl)
	}

	// Verify that region doesn't already exist for this timestamp.
	if ttli.RegionByTimestamp(timestamp) != nil {
		return nil
	}

	// Create the region.
	data.MaxRegionID++
	sgi := RegionInfo{}
	sgi.ID = data.MaxRegionID
	sgi.StartTime = timestamp.Truncate(ttli.RegionDuration).UTC()
	sgi.EndTime = sgi.StartTime.Add(ttli.RegionDuration).UTC()
	if sgi.EndTime.After(time.Unix(0, models.MaxNanoTime)) {
		// Region range is [start, end) so add one to the max time.
		sgi.EndTime = time.Unix(0, models.MaxNanoTime+1)
	}

	data.MaxShardID++
	sgi.Shards = []ShardInfo{
		{ID: data.MaxShardID},
	}

	// Time-to-live has a new region, so update the time-to-live. Regions
	// must be stored in sorted order, as other parts of the system
	// assume this to be the case.
	ttli.Regions = append(ttli.Regions, sgi)
	sort.Sort(RegionInfos(ttli.Regions))

	return nil
}

// CreateRegion creates a region on a database and time-to-live for a given timestamp.
func (data *Data) CreateRegion(database, ttl string, timestamp time.Time) error {
	singleMode := false
	dataNodeCount := len(data.DataNodes)
	if dataNodeCount == 0 {
		dataNodeCount = 1
		singleMode = true
	}

	// Find time-to-live.
	ttli, err := data.TimeToLive(database, ttl)
	if err != nil {
		return err
	} else if ttli == nil {
		return cnosdb.ErrTimeToLiveNotFound(ttl)
	}

	// Verify that region doesn't already exist for this timestamp.
	if ttli.RegionByTimestamp(timestamp) != nil {
		return nil
	}

	// Require at least one replica but no more replicas than nodes.
	replicaN := ttli.ReplicaN
	if replicaN == 0 {
		replicaN = 1
	} else if replicaN > dataNodeCount {
		replicaN = dataNodeCount
	}

	// Determine shard count by node count divided by replication factor.
	// This will ensure nodes will get distributed across nodes evenly and
	// replicated the correct number of times.
	shardN := dataNodeCount / replicaN

	// Create the region.
	data.MaxRegionID++
	sgi := RegionInfo{}
	sgi.ID = data.MaxRegionID
	sgi.StartTime = timestamp.Truncate(ttli.RegionDuration).UTC()
	sgi.EndTime = sgi.StartTime.Add(ttli.RegionDuration).UTC()
	if sgi.EndTime.After(time.Unix(0, models.MaxNanoTime)) {
		// Region range is [start, end) so add one to the max time.
		sgi.EndTime = time.Unix(0, models.MaxNanoTime+1)
	}

	data.MaxShardID++
	sgi.Shards = []ShardInfo{
		{ID: data.MaxShardID},
	}

	// Create shards on the group.
	sgi.Shards = make([]ShardInfo, shardN)
	for i := range sgi.Shards {
		data.MaxShardID++
		sgi.Shards[i] = ShardInfo{ID: data.MaxShardID}
	}

	if singleMode {
		for i := range sgi.Shards {
			si := &sgi.Shards[i]
			for j := 0; j < replicaN; j++ {
				si.Owners = append(si.Owners, ShardOwner{NodeID: 0})
			}
		}
	} else {
		// Assign data nodes to shards via round robin.
		// Start from a repeatably "random" place in the node list.
		nodeIndex := int(data.Index % uint64(dataNodeCount))
		for i := range sgi.Shards {
			si := &sgi.Shards[i]
			for j := 0; j < replicaN; j++ {
				nodeID := data.DataNodes[nodeIndex%dataNodeCount].ID
				si.Owners = append(si.Owners, ShardOwner{NodeID: nodeID})
				nodeIndex++
			}
		}
	}

	// Time-to-live has a new region, so update the time-to-live. Regions
	// must be stored in sorted order, as other parts of the system
	// assume this to be the case.
	ttli.Regions = append(ttli.Regions, sgi)
	sort.Sort(RegionInfos(ttli.Regions))

	return nil
}

// DeleteRegion removes a region from a database and time-to-live by id.
func (data *Data) DeleteRegion(database, ttl string, id uint64) error {
	// Find time-to-live.
	ttli, err := data.TimeToLive(database, ttl)
	if err != nil {
		return err
	} else if ttli == nil {
		return cnosdb.ErrTimeToLiveNotFound(ttl)
	}

	// Find region by ID and set its deletion timestamp.
	for i := range ttli.Regions {
		if ttli.Regions[i].ID == id {
			ttli.Regions[i].DeletedAt = time.Now().UTC()
			return nil
		}
	}

	return ErrRegionNotFound
}

// CreateContinuousQuery adds a named continuous query to a database.
func (data *Data) CreateContinuousQuery(database, name, query string) error {
	di := data.Database(database)
	if di == nil {
		return cnosdb.ErrDatabaseNotFound(database)
	}

	// Ensure the name doesn't already exist.
	for _, cq := range di.ContinuousQueries {
		if cq.Name == name {
			// If the query string is the same, we'll silently return,
			// otherwise we'll assume the user might be trying to
			// overwrite an existing CQ with a different query.
			if strings.ToLower(cq.Query) == strings.ToLower(query) {
				return nil
			}
			return ErrContinuousQueryExists
		}
	}

	// Append new query.
	di.ContinuousQueries = append(di.ContinuousQueries, ContinuousQueryInfo{
		Name:  name,
		Query: query,
	})

	return nil
}

// DropContinuousQuery removes a continuous query.
func (data *Data) DropContinuousQuery(database, name string) error {
	di := data.Database(database)
	if di == nil {
		return nil
	}

	for i := range di.ContinuousQueries {
		if di.ContinuousQueries[i].Name == name {
			di.ContinuousQueries = append(di.ContinuousQueries[:i], di.ContinuousQueries[i+1:]...)
			return nil
		}
	}
	return nil
}

// validateURL returns an error if the URL does not have a port or uses a scheme other than UDP or HTTP.
func validateURL(input string) error {
	u, err := url.Parse(input)
	if err != nil {
		return ErrInvalidSubscriptionURL(input)
	}

	if u.Scheme != "udp" && u.Scheme != "http" && u.Scheme != "https" {
		return ErrInvalidSubscriptionURL(input)
	}

	_, port, err := net.SplitHostPort(u.Host)
	if err != nil || port == "" {
		return ErrInvalidSubscriptionURL(input)
	}

	return nil
}

// CreateSubscription adds a named subscription to a database and time-to-live.
func (data *Data) CreateSubscription(database, ttl, name, mode string, destinations []string) error {
	for _, d := range destinations {
		if err := validateURL(d); err != nil {
			return err
		}
	}

	ttli, err := data.TimeToLive(database, ttl)
	if err != nil {
		return err
	} else if ttli == nil {
		return cnosdb.ErrTimeToLiveNotFound(ttl)
	}

	// Ensure the name doesn't already exist.
	for i := range ttli.Subscriptions {
		if ttli.Subscriptions[i].Name == name {
			return ErrSubscriptionExists
		}
	}

	// Append new query.
	ttli.Subscriptions = append(ttli.Subscriptions, SubscriptionInfo{
		Name:         name,
		Mode:         mode,
		Destinations: destinations,
	})

	return nil
}

// DropSubscription removes a subscription.
func (data *Data) DropSubscription(database, ttl, name string) error {
	ttli, err := data.TimeToLive(database, ttl)
	if err != nil {
		return err
	} else if ttli == nil {
		return cnosdb.ErrTimeToLiveNotFound(ttl)
	}

	for i := range ttli.Subscriptions {
		if ttli.Subscriptions[i].Name == name {
			ttli.Subscriptions = append(ttli.Subscriptions[:i], ttli.Subscriptions[i+1:]...)
			return nil
		}
	}
	return ErrSubscriptionNotFound
}

func (data *Data) user(username string) *UserInfo {
	for i := range data.Users {
		if data.Users[i].Name == username {
			return &data.Users[i]
		}
	}
	return nil
}

// User returns a user by username.
func (data *Data) User(username string) User {
	u := data.user(username)
	if u == nil {
		// prevent non-nil interface with nil pointer
		return nil
	}
	return u
}

// CreateUser creates a new user.
func (data *Data) CreateUser(name, hash string, admin bool) error {
	// Ensure the user doesn't already exist.
	if name == "" {
		return ErrUsernameRequired
	} else if data.User(name) != nil {
		return ErrUserExists
	}

	// Append new user.
	data.Users = append(data.Users, UserInfo{
		Name:  name,
		Hash:  hash,
		Admin: admin,
	})

	// We know there is now at least one admin user.
	if admin {
		data.adminUserExists = true
	}

	return nil
}

// DropUser removes an existing user by name.
func (data *Data) DropUser(name string) error {
	for i := range data.Users {
		if data.Users[i].Name == name {
			wasAdmin := data.Users[i].Admin
			data.Users = append(data.Users[:i], data.Users[i+1:]...)

			// Maybe we dropped the only admin user?
			if wasAdmin {
				data.adminUserExists = data.hasAdminUser()
			}
			return nil
		}
	}

	return ErrUserNotFound
}

// UpdateUser updates the password hash of an existing user.
func (data *Data) UpdateUser(name, hash string) error {
	for i := range data.Users {
		if data.Users[i].Name == name {
			data.Users[i].Hash = hash
			return nil
		}
	}
	return ErrUserNotFound
}

// SetPrivilege sets a privilege for a user on a database.
func (data *Data) SetPrivilege(name, database string, p cnosql.Privilege) error {
	ui := data.user(name)
	if ui == nil {
		return ErrUserNotFound
	}

	if data.Database(database) == nil {
		return cnosdb.ErrDatabaseNotFound(database)
	}

	if ui.Privileges == nil {
		ui.Privileges = make(map[string]cnosql.Privilege)
	}
	ui.Privileges[database] = p

	return nil
}

// SetAdminPrivilege sets the admin privilege for a user.
func (data *Data) SetAdminPrivilege(name string, admin bool) error {
	ui := data.user(name)
	if ui == nil {
		return ErrUserNotFound
	}

	ui.Admin = admin

	// We could have promoted or revoked the only admin. Check if an admin
	// user exists.
	data.adminUserExists = data.hasAdminUser()
	return nil
}

// AdminUserExists returns true if an admin user exists.
func (data Data) AdminUserExists() bool {
	return data.adminUserExists
}

// UserPrivileges gets the privileges for a user.
func (data *Data) UserPrivileges(name string) (map[string]cnosql.Privilege, error) {
	ui := data.user(name)
	if ui == nil {
		return nil, ErrUserNotFound
	}

	return ui.Privileges, nil
}

// UserPrivilege gets the privilege for a user on a database.
func (data *Data) UserPrivilege(name, database string) (*cnosql.Privilege, error) {
	ui := data.user(name)
	if ui == nil {
		return nil, ErrUserNotFound
	}

	for db, p := range ui.Privileges {
		if db == database {
			return &p, nil
		}
	}

	return cnosql.NewPrivilege(cnosql.NoPrivileges), nil
}

// Clone returns a copy of data with a new version.
func (data *Data) Clone() *Data {
	other := *data

	// Copy nodes.
	if data.DataNodes != nil {
		other.DataNodes = make([]NodeInfo, len(data.DataNodes))
		for i := range data.DataNodes {
			other.DataNodes[i] = data.DataNodes[i].clone()
		}
	}

	if data.MetaNodes != nil {
		other.MetaNodes = make([]NodeInfo, len(data.MetaNodes))
		for i := range data.MetaNodes {
			other.MetaNodes[i] = data.MetaNodes[i].clone()
		}
	}

	if data.Databases != nil {
		other.Databases = make([]DatabaseInfo, len(data.Databases))
		for i := range data.Databases {
			other.Databases[i] = data.Databases[i].clone()
		}
	}

	if data.Users != nil {
		other.Users = make([]UserInfo, len(data.Users))
		for i := range data.Users {
			other.Users[i] = data.Users[i].clone()
		}
	}

	return &other
}

// marshal serializes data to a protobuf representation.
func (data *Data) marshal() *internal.Data {
	pb := &internal.Data{
		Term:      proto.Uint64(data.Term),
		Index:     proto.Uint64(data.Index),
		ClusterID: proto.Uint64(data.ClusterID),

		MaxNodeID:   proto.Uint64(data.MaxNodeID),
		MaxRegionID: proto.Uint64(data.MaxRegionID),
		MaxShardID:  proto.Uint64(data.MaxShardID),
	}

	pb.DataNodes = make([]*internal.NodeInfo, len(data.DataNodes))
	for i := range data.DataNodes {
		pb.DataNodes[i] = data.DataNodes[i].marshal()
	}

	pb.MetaNodes = make([]*internal.NodeInfo, len(data.MetaNodes))
	for i := range data.MetaNodes {
		pb.MetaNodes[i] = data.MetaNodes[i].marshal()
	}

	pb.Databases = make([]*internal.DatabaseInfo, len(data.Databases))
	for i := range data.Databases {
		pb.Databases[i] = data.Databases[i].marshal()
	}

	pb.Users = make([]*internal.UserInfo, len(data.Users))
	for i := range data.Users {
		pb.Users[i] = data.Users[i].marshal()
	}

	return pb
}

// unmarshal deserializes from a protobuf representation.
func (data *Data) unmarshal(pb *internal.Data) {
	data.Term = pb.GetTerm()
	data.Index = pb.GetIndex()
	data.ClusterID = pb.GetClusterID()

	data.MaxNodeID = pb.GetMaxNodeID()
	data.MaxRegionID = pb.GetMaxRegionID()
	data.MaxShardID = pb.GetMaxShardID()

	data.DataNodes = make([]NodeInfo, len(pb.GetDataNodes()))
	for i, x := range pb.GetDataNodes() {
		data.DataNodes[i].unmarshal(x)
	}

	data.MetaNodes = make([]NodeInfo, len(pb.GetMetaNodes()))
	for i, x := range pb.GetMetaNodes() {
		data.MetaNodes[i].unmarshal(x)
	}

	data.Databases = make([]DatabaseInfo, len(pb.GetDatabases()))
	for i, x := range pb.GetDatabases() {
		data.Databases[i].unmarshal(x)
	}

	data.Users = make([]UserInfo, len(pb.GetUsers()))
	for i, x := range pb.GetUsers() {
		data.Users[i].unmarshal(x)
	}

	// Exhaustively determine if there is an admin user. The marshalled cache
	// value may not be correct.
	data.adminUserExists = data.hasAdminUser()
}

// MarshalBinary encodes the metadata to a binary format.
func (data *Data) MarshalBinary() ([]byte, error) {
	return proto.Marshal(data.marshal())
}

// UnmarshalBinary decodes the object from a binary format.
func (data *Data) UnmarshalBinary(buf []byte) error {
	var pb internal.Data
	if err := proto.Unmarshal(buf, &pb); err != nil {
		return err
	}
	data.unmarshal(&pb)
	return nil
}

// TruncateRegions truncates any region that could contain timestamps beyond t.
func (data *Data) TruncateRegions(t time.Time) {
	for i := range data.Databases {
		dbi := &data.Databases[i]

		for j := range dbi.TimeToLives {
			ttli := &dbi.TimeToLives[j]

			for k := range ttli.Regions {
				sgi := &ttli.Regions[k]

				if !t.Before(sgi.EndTime) || sgi.Deleted() || (sgi.Truncated() && sgi.TruncatedAt.Before(t)) {
					continue
				}

				if !t.After(sgi.StartTime) {
					// future region
					sgi.TruncatedAt = sgi.StartTime
				} else {
					sgi.TruncatedAt = t
				}
			}
		}
	}
}

// hasAdminUser exhaustively checks for the presence of at least one admin
// user.
func (data *Data) hasAdminUser() bool {
	for _, u := range data.Users {
		if u.Admin {
			return true
		}
	}
	return false
}

// ImportData imports selected data into the current metadata.
// if non-empty, backupDBName, restoreDBName, backupTTLName, restoreTTLName can be used to select DB metadata from other,
// and to assign a new name to the imported data.  Returns a map of shard ID's in the old metadata to new shard ID's
// in the new metadata, along with a list of new databases created, both of which can assist in the import of existing
// shard data during a database restore.
func (data *Data) ImportData(other Data, backupDBName, restoreDBName, backupTTLName, restoreTTLName string) (map[uint64]uint64, []string, error) {
	shardIDMap := make(map[uint64]uint64)
	if backupDBName != "" {
		dbName, err := data.importOneDB(other, backupDBName, restoreDBName, backupTTLName, restoreTTLName, shardIDMap)
		if err != nil {
			return nil, nil, err
		}

		return shardIDMap, []string{dbName}, nil
	}

	// if no backupDBName then we'll try to import all the DB's.  If one of them fails, we'll mark the whole
	// operation a failure and return an error.
	var newDBs []string
	for _, dbi := range other.Databases {
		if dbi.Name == "_internal" {
			continue
		}
		dbName, err := data.importOneDB(other, dbi.Name, "", "", "", shardIDMap)
		if err != nil {
			return nil, nil, err
		}
		newDBs = append(newDBs, dbName)
	}
	return shardIDMap, newDBs, nil
}

// importOneDB imports a single database/time-to-live from an external metadata object, renaming them if new names are provided.
func (data *Data) importOneDB(other Data, backupDBName, restoreDBName, backupTTLName, restoreTTLName string, shardIDMap map[uint64]uint64) (string, error) {

	dbPtr := other.Database(backupDBName)
	if dbPtr == nil {
		return "", fmt.Errorf("imported metadata does not have datbase named %s", backupDBName)
	}

	if restoreDBName == "" {
		restoreDBName = backupDBName
	}

	if data.Database(restoreDBName) != nil {
		return "", errors.New("database already exists")
	}

	// change the names if we want/need to
	err := data.CreateDatabase(restoreDBName)
	if err != nil {
		return "", err
	}
	dbImport := data.Database(restoreDBName)

	if backupTTLName != "" {
		ttlPtr := dbPtr.TimeToLive(backupTTLName)

		if ttlPtr != nil {
			ttlImport := ttlPtr.clone()
			if restoreTTLName == "" {
				restoreTTLName = backupTTLName
			}
			ttlImport.Name = restoreTTLName
			dbImport.TimeToLives = []TimeToLiveInfo{ttlImport}
			dbImport.DefaultTimeToLive = restoreTTLName
		} else {
			return "", fmt.Errorf("time to live not found in meta backup: %s.%s", backupDBName, backupTTLName)
		}

	} else { // import all time-to-lives without renaming
		dbImport.DefaultTimeToLive = dbPtr.DefaultTimeToLive
		if dbPtr.TimeToLives != nil {
			dbImport.TimeToLives = make([]TimeToLiveInfo, len(dbPtr.TimeToLives))
			for i := range dbPtr.TimeToLives {
				dbImport.TimeToLives[i] = dbPtr.TimeToLives[i].clone()
			}
		}

	}

	// renumber the regions and shards for the new time to live(ies)
	for _, ttlImport := range dbImport.TimeToLives {
		for j, sgImport := range ttlImport.Regions {
			data.MaxRegionID++
			ttlImport.Regions[j].ID = data.MaxRegionID
			for k := range sgImport.Shards {
				data.MaxShardID++
				shardIDMap[sgImport.Shards[k].ID] = data.MaxShardID
				sgImport.Shards[k].ID = data.MaxShardID
				// OSS doesn't use Owners but if we are importing this from Enterprise, we'll want to clear it out
				// to avoid any issues if they ever export this DB again to bring back to Enterprise.
				sgImport.Shards[k].Owners = []ShardOwner{}
			}
		}
	}

	return restoreDBName, nil
}

// NodeInfo represents information about a single node in the cluster.
type NodeInfo struct {
	ID      uint64
	Host    string
	TCPHost string
}

// NodeInfos is a slice of NodeInfo used for sorting
type NodeInfos []NodeInfo

// clone returns a deep copy of NodeInfo.
func (n NodeInfo) clone() NodeInfo { return n }

// Len implements sort.Interface.
func (n NodeInfos) Len() int { return len(n) }

// Swap implements sort.Interface.
func (n NodeInfos) Swap(i, j int) { n[i], n[j] = n[j], n[i] }

// Less implements sort.Interface.
func (n NodeInfos) Less(i, j int) bool { return n[i].ID < n[j].ID }

// marshal serializes to a protobuf representation.
func (n NodeInfo) marshal() *internal.NodeInfo {
	pb := &internal.NodeInfo{}
	pb.ID = proto.Uint64(n.ID)
	pb.Host = proto.String(n.Host)
	pb.TCPHost = proto.String(n.TCPHost)
	return pb
}

// unmarshal deserializes from a protobuf representation.
func (n *NodeInfo) unmarshal(pb *internal.NodeInfo) {
	n.ID = pb.GetID()
	n.Host = pb.GetHost()
	n.TCPHost = pb.GetTCPHost()
}

// DatabaseInfo represents information about a database in the system.
type DatabaseInfo struct {
	Name              string
	DefaultTimeToLive string
	TimeToLives       []TimeToLiveInfo
	ContinuousQueries []ContinuousQueryInfo
}

// TimeToLive returns a time-to-live by name.
func (di DatabaseInfo) TimeToLive(name string) *TimeToLiveInfo {
	if name == "" {
		if di.DefaultTimeToLive == "" {
			return nil
		}
		name = di.DefaultTimeToLive
	}

	for i := range di.TimeToLives {
		if di.TimeToLives[i].Name == name {
			return &di.TimeToLives[i]
		}
	}
	return nil
}

// ShardInfos returns a list of all shards' info for the database.
func (di DatabaseInfo) ShardInfos() []ShardInfo {
	shards := map[uint64]*ShardInfo{}
	for i := range di.TimeToLives {
		for j := range di.TimeToLives[i].Regions {
			rg := di.TimeToLives[i].Regions[j]
			// Skip deleted regions
			if rg.Deleted() {
				continue
			}
			for k := range rg.Shards {
				si := &di.TimeToLives[i].Regions[j].Shards[k]
				shards[si.ID] = si
			}
		}
	}

	infos := make([]ShardInfo, 0, len(shards))
	for _, info := range shards {
		infos = append(infos, *info)
	}

	return infos
}

// clone returns a deep copy of di.
func (di DatabaseInfo) clone() DatabaseInfo {
	other := di

	if di.TimeToLives != nil {
		other.TimeToLives = make([]TimeToLiveInfo, len(di.TimeToLives))
		for i := range di.TimeToLives {
			other.TimeToLives[i] = di.TimeToLives[i].clone()
		}
	}

	// Copy continuous queries.
	if di.ContinuousQueries != nil {
		other.ContinuousQueries = make([]ContinuousQueryInfo, len(di.ContinuousQueries))
		for i := range di.ContinuousQueries {
			other.ContinuousQueries[i] = di.ContinuousQueries[i].clone()
		}
	}

	return other
}

// marshal serializes to a protobuf representation.
func (di DatabaseInfo) marshal() *internal.DatabaseInfo {
	pb := &internal.DatabaseInfo{}
	pb.Name = proto.String(di.Name)
	pb.DefaultTimeToLive = proto.String(di.DefaultTimeToLive)

	pb.TimeToLives = make([]*internal.TimeToLiveInfo, len(di.TimeToLives))
	for i := range di.TimeToLives {
		pb.TimeToLives[i] = di.TimeToLives[i].marshal()
	}

	pb.ContinuousQueries = make([]*internal.ContinuousQueryInfo, len(di.ContinuousQueries))
	for i := range di.ContinuousQueries {
		pb.ContinuousQueries[i] = di.ContinuousQueries[i].marshal()
	}
	return pb
}

// unmarshal deserializes from a protobuf representation.
func (di *DatabaseInfo) unmarshal(pb *internal.DatabaseInfo) {
	di.Name = pb.GetName()
	di.DefaultTimeToLive = pb.GetDefaultTimeToLive()

	if len(pb.GetTimeToLives()) > 0 {
		di.TimeToLives = make([]TimeToLiveInfo, len(pb.GetTimeToLives()))
		for i, x := range pb.GetTimeToLives() {
			di.TimeToLives[i].unmarshal(x)
		}
	}

	if len(pb.GetContinuousQueries()) > 0 {
		di.ContinuousQueries = make([]ContinuousQueryInfo, len(pb.GetContinuousQueries()))
		for i, x := range pb.GetContinuousQueries() {
			di.ContinuousQueries[i].unmarshal(x)
		}
	}
}

// TimeToLiveSpec represents the specification for a new time-to-live.
type TimeToLiveSpec struct {
	Name           string
	ReplicaN       *int
	Duration       *time.Duration
	RegionDuration time.Duration
}

// NewTimeToLiveInfo creates a new time-to-live info from the specification.
func (s *TimeToLiveSpec) NewTimeToLiveInfo() *TimeToLiveInfo {
	return DefaultTimeToLiveInfo().Apply(s)
}

// Matches checks if this time-to-live specification matches
// an existing time-to-live.
func (s *TimeToLiveSpec) Matches(ttli *TimeToLiveInfo) bool {
	if ttli == nil {
		return false
	} else if s.Name != "" && s.Name != ttli.Name {
		return false
	} else if s.Duration != nil && *s.Duration != ttli.Duration {
		return false
	} else if s.ReplicaN != nil && *s.ReplicaN != ttli.ReplicaN {
		return false
	}

	// Normalise ShardDuration before comparing to any existing time-to-live.
	// Normalize with the time-to-live info's duration instead of the spec
	// since they should be the same and we're performing a comparison.
	sgDuration := normalisedShardDuration(s.RegionDuration, ttli.Duration)
	return sgDuration == ttli.RegionDuration
}

// marshal serializes to a protobuf representation.
func (s *TimeToLiveSpec) marshal() *internal.TimeToLiveSpec {
	pb := &internal.TimeToLiveSpec{}
	if s.Name != "" {
		pb.Name = proto.String(s.Name)
	}
	if s.Duration != nil {
		pb.Duration = proto.Int64(int64(*s.Duration))
	}
	if s.RegionDuration > 0 {
		pb.RegionDuration = proto.Int64(int64(s.RegionDuration))
	}
	if s.ReplicaN != nil {
		pb.ReplicaN = proto.Uint32(uint32(*s.ReplicaN))
	}
	return pb
}

// unmarshal deserializes from a protobuf representation.
func (s *TimeToLiveSpec) unmarshal(pb *internal.TimeToLiveSpec) {
	if pb.Name != nil {
		s.Name = pb.GetName()
	}
	if pb.Duration != nil {
		duration := time.Duration(pb.GetDuration())
		s.Duration = &duration
	}
	if pb.RegionDuration != nil {
		s.RegionDuration = time.Duration(pb.GetRegionDuration())
	}
	if pb.ReplicaN != nil {
		replicaN := int(pb.GetReplicaN())
		s.ReplicaN = &replicaN
	}
}

// MarshalBinary encodes TimeToLiveSpec to a binary format.
func (s *TimeToLiveSpec) MarshalBinary() ([]byte, error) {
	return proto.Marshal(s.marshal())
}

// UnmarshalBinary decodes TimeToLiveSpec from a binary format.
func (s *TimeToLiveSpec) UnmarshalBinary(data []byte) error {
	var pb internal.TimeToLiveSpec
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	s.unmarshal(&pb)
	return nil
}

// TimeToLiveInfo represents metadata about a time-to-live.
type TimeToLiveInfo struct {
	Name           string
	ReplicaN       int
	Duration       time.Duration
	RegionDuration time.Duration
	Regions        []RegionInfo
	Subscriptions  []SubscriptionInfo
}

// NewTimeToLiveInfo returns a new instance of TimeToLiveInfo
// with default replication and duration.
func NewTimeToLiveInfo(name string) *TimeToLiveInfo {
	return &TimeToLiveInfo{
		Name:     name,
		ReplicaN: DefaultTimeToLiveReplicaN,
		Duration: DefaultTimeToLiveDuration,
	}
}

// DefaultTimeToLiveInfo returns a new instance of TimeToLiveInfo
// with default name, replication, and duration.
func DefaultTimeToLiveInfo() *TimeToLiveInfo {
	return NewTimeToLiveInfo(DefaultTimeToLiveName)
}

// Apply applies a specification to the time-to-live info.
func (ttli *TimeToLiveInfo) Apply(spec *TimeToLiveSpec) *TimeToLiveInfo {
	ttl := &TimeToLiveInfo{
		Name:           ttli.Name,
		ReplicaN:       ttli.ReplicaN,
		Duration:       ttli.Duration,
		RegionDuration: ttli.RegionDuration,
	}
	if spec.Name != "" {
		ttl.Name = spec.Name
	}
	if spec.ReplicaN != nil {
		ttl.ReplicaN = *spec.ReplicaN
	}
	if spec.Duration != nil {
		ttl.Duration = *spec.Duration
	}
	ttl.RegionDuration = normalisedShardDuration(spec.RegionDuration, ttl.Duration)
	return ttl
}

// RegionByTimestamp returns the region in the time-to-live that contains the timestamp,
// or nil if no region matches.
func (ttli *TimeToLiveInfo) RegionByTimestamp(timestamp time.Time) *RegionInfo {
	for i := range ttli.Regions {
		sgi := &ttli.Regions[i]
		if sgi.Contains(timestamp) && !sgi.Deleted() && (!sgi.Truncated() || timestamp.Before(sgi.TruncatedAt)) {
			return &ttli.Regions[i]
		}
	}

	return nil
}

// ExpiredRegions returns the Regions which are considered expired, for the given time.
func (ttli *TimeToLiveInfo) ExpiredRegions(t time.Time) []*RegionInfo {
	var regions = make([]*RegionInfo, 0)
	for i := range ttli.Regions {
		if ttli.Regions[i].Deleted() {
			continue
		}
		if ttli.Duration != 0 && ttli.Regions[i].EndTime.Add(ttli.Duration).Before(t) {
			regions = append(regions, &ttli.Regions[i])
		}
	}
	return regions
}

// DeletedRegions returns the Regions which are marked as deleted.
func (ttli *TimeToLiveInfo) DeletedRegions() []*RegionInfo {
	var regions = make([]*RegionInfo, 0)
	for i := range ttli.Regions {
		if ttli.Regions[i].Deleted() {
			regions = append(regions, &ttli.Regions[i])
		}
	}
	return regions
}

// marshal serializes to a protobuf representation.
func (ttli *TimeToLiveInfo) marshal() *internal.TimeToLiveInfo {
	pb := &internal.TimeToLiveInfo{
		Name:           proto.String(ttli.Name),
		ReplicaN:       proto.Uint32(uint32(ttli.ReplicaN)),
		Duration:       proto.Int64(int64(ttli.Duration)),
		RegionDuration: proto.Int64(int64(ttli.RegionDuration)),
	}

	pb.Regions = make([]*internal.RegionInfo, len(ttli.Regions))
	for i, sgi := range ttli.Regions {
		pb.Regions[i] = sgi.marshal()
	}

	pb.Subscriptions = make([]*internal.SubscriptionInfo, len(ttli.Subscriptions))
	for i, sub := range ttli.Subscriptions {
		pb.Subscriptions[i] = sub.marshal()
	}

	return pb
}

// unmarshal deserializes from a protobuf representation.
func (ttli *TimeToLiveInfo) unmarshal(pb *internal.TimeToLiveInfo) {
	ttli.Name = pb.GetName()
	ttli.ReplicaN = int(pb.GetReplicaN())
	ttli.Duration = time.Duration(pb.GetDuration())
	ttli.RegionDuration = time.Duration(pb.GetRegionDuration())

	if len(pb.GetRegions()) > 0 {
		ttli.Regions = make([]RegionInfo, len(pb.GetRegions()))
		for i, x := range pb.GetRegions() {
			ttli.Regions[i].unmarshal(x)
		}
	}
	if len(pb.GetSubscriptions()) > 0 {
		ttli.Subscriptions = make([]SubscriptionInfo, len(pb.GetSubscriptions()))
		for i, x := range pb.GetSubscriptions() {
			ttli.Subscriptions[i].unmarshal(x)
		}
	}
}

// clone returns a deep copy of ttli.
func (ttli TimeToLiveInfo) clone() TimeToLiveInfo {
	other := ttli

	if ttli.Regions != nil {
		other.Regions = make([]RegionInfo, len(ttli.Regions))
		for i := range ttli.Regions {
			other.Regions[i] = ttli.Regions[i].clone()
		}
	}

	return other
}

// MarshalBinary encodes ttli to a binary format.
func (ttli *TimeToLiveInfo) MarshalBinary() ([]byte, error) {
	return proto.Marshal(ttli.marshal())
}

// UnmarshalBinary decodes ttli from a binary format.
func (ttli *TimeToLiveInfo) UnmarshalBinary(data []byte) error {
	var pb internal.TimeToLiveInfo
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	ttli.unmarshal(&pb)
	return nil
}

// regionDuration returns the default duration for a region based on a time-to-live duration.
func regionDuration(d time.Duration) time.Duration {
	if d >= 180*24*time.Hour || d == 0 { // 6 months or 0
		return 7 * 24 * time.Hour
	} else if d >= 2*24*time.Hour { // 2 days
		return 1 * 24 * time.Hour
	}
	return 1 * time.Hour
}

// normalisedShardDuration returns normalised shard duration based on a time-to-live duration.
func normalisedShardDuration(sgd, d time.Duration) time.Duration {
	// If it is zero, it likely wasn't specified, so we default to the region duration
	if sgd == 0 {
		return regionDuration(d)
	}
	// If it was specified, but it's less than the MinTimeToLiveDuration, then normalize
	// to the MinTimeToLiveDuration
	if sgd < MinTimeToLiveDuration {
		return regionDuration(MinTimeToLiveDuration)
	}
	return sgd
}

// RegionInfo represents metadata about a region. The DeletedAt field is important
// because it makes it clear that a Region has been marked as deleted, and allow the system
// to be sure that a Region is not simply missing. If the DeletedAt is set, the system can
// safely delete any associated shards.
type RegionInfo struct {
	ID          uint64
	StartTime   time.Time
	EndTime     time.Time
	DeletedAt   time.Time
	Shards      []ShardInfo
	TruncatedAt time.Time
}

// RegionInfos implements sort.Interface on []RegionInfo, based
// on the StartTime field.
type RegionInfos []RegionInfo

// Len implements sort.Interface.
func (a RegionInfos) Len() int { return len(a) }

// Swap implements sort.Interface.
func (a RegionInfos) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// Less implements sort.Interface.
func (a RegionInfos) Less(i, j int) bool {
	iEnd := a[i].EndTime
	if a[i].Truncated() {
		iEnd = a[i].TruncatedAt
	}

	jEnd := a[j].EndTime
	if a[j].Truncated() {
		jEnd = a[j].TruncatedAt
	}

	if iEnd.Equal(jEnd) {
		return a[i].StartTime.Before(a[j].StartTime)
	}

	return iEnd.Before(jEnd)
}

// Contains returns true iif StartTime ≤ t < EndTime.
func (sgi *RegionInfo) Contains(t time.Time) bool {
	return !t.Before(sgi.StartTime) && t.Before(sgi.EndTime)
}

// Overlaps returns whether the region contains data for the time range between min and max
func (sgi *RegionInfo) Overlaps(min, max time.Time) bool {
	return !sgi.StartTime.After(max) && sgi.EndTime.After(min)
}

// Deleted returns whether this Region has been deleted.
func (sgi *RegionInfo) Deleted() bool {
	return !sgi.DeletedAt.IsZero()
}

// Truncated returns true if this Region has been truncated (no new writes).
func (sgi *RegionInfo) Truncated() bool {
	return !sgi.TruncatedAt.IsZero()
}

// clone returns a deep copy of sgi.
func (sgi RegionInfo) clone() RegionInfo {
	other := sgi

	if sgi.Shards != nil {
		other.Shards = make([]ShardInfo, len(sgi.Shards))
		for i := range sgi.Shards {
			other.Shards[i] = sgi.Shards[i].clone()
		}
	}

	return other
}

type hashIDer interface {
	HashID() uint64
}

// ShardFor returns the ShardInfo for a Point or other hashIDer.
func (sgi *RegionInfo) ShardFor(p hashIDer) ShardInfo {
	if len(sgi.Shards) == 1 {
		return sgi.Shards[0]
	}
	return sgi.Shards[p.HashID()%uint64(len(sgi.Shards))]
}

// marshal serializes to a protobuf representation.
func (sgi *RegionInfo) marshal() *internal.RegionInfo {
	pb := &internal.RegionInfo{
		ID:        proto.Uint64(sgi.ID),
		StartTime: proto.Int64(MarshalTime(sgi.StartTime)),
		EndTime:   proto.Int64(MarshalTime(sgi.EndTime)),
		DeletedAt: proto.Int64(MarshalTime(sgi.DeletedAt)),
	}

	if !sgi.TruncatedAt.IsZero() {
		pb.TruncatedAt = proto.Int64(MarshalTime(sgi.TruncatedAt))
	}

	pb.Shards = make([]*internal.ShardInfo, len(sgi.Shards))
	for i := range sgi.Shards {
		pb.Shards[i] = sgi.Shards[i].marshal()
	}

	return pb
}

// unmarshal deserializes from a protobuf representation.
func (sgi *RegionInfo) unmarshal(pb *internal.RegionInfo) {
	sgi.ID = pb.GetID()
	if i := pb.GetStartTime(); i == 0 {
		sgi.StartTime = time.Unix(0, 0).UTC()
	} else {
		sgi.StartTime = UnmarshalTime(i)
	}
	if i := pb.GetEndTime(); i == 0 {
		sgi.EndTime = time.Unix(0, 0).UTC()
	} else {
		sgi.EndTime = UnmarshalTime(i)
	}
	sgi.DeletedAt = UnmarshalTime(pb.GetDeletedAt())

	if pb != nil && pb.TruncatedAt != nil {
		sgi.TruncatedAt = UnmarshalTime(pb.GetTruncatedAt())
	}

	if len(pb.GetShards()) > 0 {
		sgi.Shards = make([]ShardInfo, len(pb.GetShards()))
		for i, x := range pb.GetShards() {
			sgi.Shards[i].unmarshal(x)
		}
	}
}

// ShardInfo represents metadata about a shard.
type ShardInfo struct {
	ID     uint64
	Owners []ShardOwner
}

// OwnedBy determines whether the shard's owner IDs includes nodeID.
func (si ShardInfo) OwnedBy(nodeID uint64) bool {
	for _, so := range si.Owners {
		if so.NodeID == nodeID {
			return true
		}
	}
	return false
}

// clone returns a deep copy of si.
func (si ShardInfo) clone() ShardInfo {
	other := si

	if si.Owners != nil {
		other.Owners = make([]ShardOwner, len(si.Owners))
		for i := range si.Owners {
			other.Owners[i] = si.Owners[i].clone()
		}
	}

	return other
}

// marshal serializes to a protobuf representation.
func (si ShardInfo) marshal() *internal.ShardInfo {
	pb := &internal.ShardInfo{
		ID: proto.Uint64(si.ID),
	}

	pb.Owners = make([]*internal.ShardOwner, len(si.Owners))
	for i := range si.Owners {
		pb.Owners[i] = si.Owners[i].marshal()
	}

	return pb
}

// UnmarshalBinary decodes the object from a binary format.
func (si *ShardInfo) UnmarshalBinary(buf []byte) error {
	var pb internal.ShardInfo
	if err := proto.Unmarshal(buf, &pb); err != nil {
		return err
	}
	si.unmarshal(&pb)
	return nil
}

// unmarshal deserializes from a protobuf representation.
func (si *ShardInfo) unmarshal(pb *internal.ShardInfo) {
	si.ID = pb.GetID()

	// If deprecated "OwnerIDs" exists then convert it to "Owners" format.
	if len(pb.GetOwnerIDs()) > 0 {
		si.Owners = make([]ShardOwner, len(pb.GetOwnerIDs()))
		for i, x := range pb.GetOwnerIDs() {
			si.Owners[i].unmarshal(&internal.ShardOwner{
				NodeID: proto.Uint64(x),
			})
		}
	} else if len(pb.GetOwners()) > 0 {
		si.Owners = make([]ShardOwner, len(pb.GetOwners()))
		for i, x := range pb.GetOwners() {
			si.Owners[i].unmarshal(x)
		}
	}
}

// SubscriptionInfo holds the subscription information.
type SubscriptionInfo struct {
	Name         string
	Mode         string
	Destinations []string
}

// marshal serializes to a protobuf representation.
func (si SubscriptionInfo) marshal() *internal.SubscriptionInfo {
	pb := &internal.SubscriptionInfo{
		Name: proto.String(si.Name),
		Mode: proto.String(si.Mode),
	}

	pb.Destinations = make([]string, len(si.Destinations))
	for i := range si.Destinations {
		pb.Destinations[i] = si.Destinations[i]
	}
	return pb
}

// unmarshal deserializes from a protobuf representation.
func (si *SubscriptionInfo) unmarshal(pb *internal.SubscriptionInfo) {
	si.Name = pb.GetName()
	si.Mode = pb.GetMode()

	if len(pb.GetDestinations()) > 0 {
		si.Destinations = make([]string, len(pb.GetDestinations()))
		copy(si.Destinations, pb.GetDestinations())
	}
}

// ShardOwner represents a node that owns a shard.
type ShardOwner struct {
	NodeID uint64 // if NodeID is 0 , the Shard is a local shard
}

// clone returns a deep copy of so.
func (so ShardOwner) clone() ShardOwner {
	return so
}

// marshal serializes to a protobuf representation.
func (so ShardOwner) marshal() *internal.ShardOwner {
	return &internal.ShardOwner{
		NodeID: proto.Uint64(so.NodeID),
	}
}

// unmarshal deserializes from a protobuf representation.
func (so *ShardOwner) unmarshal(pb *internal.ShardOwner) {
	so.NodeID = pb.GetNodeID()
}

// ContinuousQueryInfo represents metadata about a continuous query.
type ContinuousQueryInfo struct {
	Name  string
	Query string
}

// clone returns a deep copy of cqi.
func (cqi ContinuousQueryInfo) clone() ContinuousQueryInfo { return cqi }

// marshal serializes to a protobuf representation.
func (cqi ContinuousQueryInfo) marshal() *internal.ContinuousQueryInfo {
	return &internal.ContinuousQueryInfo{
		Name:  proto.String(cqi.Name),
		Query: proto.String(cqi.Query),
	}
}

// unmarshal deserializes from a protobuf representation.
func (cqi *ContinuousQueryInfo) unmarshal(pb *internal.ContinuousQueryInfo) {
	cqi.Name = pb.GetName()
	cqi.Query = pb.GetQuery()
}

var _ query.FineAuthorizer = (*UserInfo)(nil)

// UserInfo represents metadata about a user in the system.
type UserInfo struct {
	// User's name.
	Name string

	// Hashed password.
	Hash string

	// Whether the user is an admin, i.e. allowed to do everything.
	Admin bool

	// Map of database name to granted privilege.
	Privileges map[string]cnosql.Privilege
}

type User interface {
	query.FineAuthorizer
	ID() string
	AuthorizeUnrestricted() bool
}

func (u *UserInfo) ID() string {
	return u.Name
}

// AuthorizeDatabase returns true if the user is authorized for the given privilege on the given database.
func (ui *UserInfo) AuthorizeDatabase(privilege cnosql.Privilege, database string) bool {
	if ui.Admin || privilege == cnosql.NoPrivileges {
		return true
	}
	p, ok := ui.Privileges[database]
	return ok && (p == privilege || p == cnosql.AllPrivileges)
}

// AuthorizeSeriesRead is used to limit access per-series (enterprise only)
func (u *UserInfo) AuthorizeSeriesRead(database string, metric []byte, tags models.Tags) bool {
	return true
}

// AuthorizeSeriesWrite is used to limit access per-series (enterprise only)
func (u *UserInfo) AuthorizeSeriesWrite(database string, metric []byte, tags models.Tags) bool {
	return true
}

// IsOpen is a method on FineAuthorizer to indicate all fine auth is permitted and short circuit some checks.
func (u *UserInfo) IsOpen() bool {
	return true
}

// AuthorizeUnrestricted allows admins to shortcut access checks.
func (u *UserInfo) AuthorizeUnrestricted() bool {
	return u.Admin
}

// clone returns a deep copy of si.
func (ui UserInfo) clone() UserInfo {
	other := ui

	if ui.Privileges != nil {
		other.Privileges = make(map[string]cnosql.Privilege)
		for k, v := range ui.Privileges {
			other.Privileges[k] = v
		}
	}

	return other
}

// marshal serializes to a protobuf representation.
func (ui UserInfo) marshal() *internal.UserInfo {
	pb := &internal.UserInfo{
		Name:  proto.String(ui.Name),
		Hash:  proto.String(ui.Hash),
		Admin: proto.Bool(ui.Admin),
	}

	for database, privilege := range ui.Privileges {
		pb.Privileges = append(pb.Privileges, &internal.UserPrivilege{
			Database:  proto.String(database),
			Privilege: proto.Int32(int32(privilege)),
		})
	}

	return pb
}

// unmarshal deserializes from a protobuf representation.
func (ui *UserInfo) unmarshal(pb *internal.UserInfo) {
	ui.Name = pb.GetName()
	ui.Hash = pb.GetHash()
	ui.Admin = pb.GetAdmin()

	ui.Privileges = make(map[string]cnosql.Privilege)
	for _, p := range pb.GetPrivileges() {
		ui.Privileges[p.GetDatabase()] = cnosql.Privilege(p.GetPrivilege())
	}
}

// Lease represents a lease held on a resource.
type Lease struct {
	Name       string    `json:"name"`
	Expiration time.Time `json:"expiration"`
	Owner      uint64    `json:"owner"`
}

// Leases is a concurrency-safe collection of leases keyed by name.
type Leases struct {
	mu sync.Mutex
	m  map[string]*Lease
	d  time.Duration
}

// NewLeases returns a new instance of Leases.
func NewLeases(d time.Duration) *Leases {
	return &Leases{
		m: make(map[string]*Lease),
		d: d,
	}
}

// Acquire acquires a lease with the given name for the given nodeID.
// If the lease doesn't exist or exists but is expired, a valid lease is returned.
// If nodeID already owns the named and unexpired lease, the lease expiration is extended.
// If a different node owns the lease, an error is returned.
func (leases *Leases) Acquire(name string, nodeID uint64) (*Lease, error) {
	leases.mu.Lock()
	defer leases.mu.Unlock()

	l := leases.m[name]
	if l != nil {
		if time.Now().After(l.Expiration) || l.Owner == nodeID {
			l.Expiration = time.Now().Add(leases.d)
			l.Owner = nodeID
			return l, nil
		}
		return l, errors.New("another node has the lease")
	}

	l = &Lease{
		Name:       name,
		Expiration: time.Now().Add(leases.d),
		Owner:      nodeID,
	}

	leases.m[name] = l

	return l, nil
}

// MarshalTime converts t to nanoseconds since epoch. A zero time returns 0.
func MarshalTime(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

// UnmarshalTime converts nanoseconds since epoch to time.
// A zero value returns a zero time.
func UnmarshalTime(v int64) time.Time {
	if v == 0 {
		return time.Time{}
	}
	return time.Unix(0, v).UTC()
}

// ValidName checks to see if the given name can would be valid for DB/TTL name
func ValidName(name string) bool {
	for _, r := range name {
		if !unicode.IsPrint(r) {
			return false
		}
	}

	return name != "" &&
		name != "." &&
		name != ".." &&
		!strings.ContainsAny(name, `/\`)
}
