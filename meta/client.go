// Package meta provides control over meta data for CnosDB,
// such as controlling databases, time-to-lives, users, etc.
package meta

import (
	"bytes"
	crand "crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/cnosdatabase/cnosdb"
	"github.com/cnosdatabase/cnosql"
	"github.com/cnosdatabase/db/logger"
	"github.com/cnosdatabase/db/pkg/file"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
)

const (
	// SaltBytes is the number of bytes used for salts.
	SaltBytes = 32

	metaFile = "meta.db"

	// RegionDeletedExpiration is the amount of time before a region info will be removed from cached
	// data after it has been marked deleted (2 weeks).
	RegionDeletedExpiration = -2 * 7 * 24 * time.Hour
)

var (
	// ErrServiceUnavailable is returned when the meta service is unavailable.
	ErrServiceUnavailable = errors.New("meta service unavailable")

	// ErrService is returned when the meta service returns an error.
	ErrService = errors.New("meta service error")
)

type MetaClient interface {
	Open() error
	Close() error

	NodeID() uint64
	ClusterID() uint64

	Ping(checkAllMetaServers bool) error
	AcquireLease(name string) (*Lease, error)
	SetMetaServers([]string)

	DataNode(id uint64) (*NodeInfo, error)
	DataNodes() ([]NodeInfo, error)
	CreateDataNode(httpAddr, tcpAddr string) (*NodeInfo, error)
	DataNodeByHTTPHost(httpAddr string) (*NodeInfo, error)
	DataNodeByTCPHost(tcpAddr string) (*NodeInfo, error)
	DeleteDataNode(id uint64) error

	MetaNodes() ([]NodeInfo, error)
	MetaNodeByAddr(addr string) *NodeInfo
	CreateMetaNode(httpAddr, tcpAddr string) (*NodeInfo, error)
	DeleteMetaNode(id uint64) error

	Database(name string) *DatabaseInfo
	Databases() []DatabaseInfo
	CreateDatabase(name string) (*DatabaseInfo, error)
	CreateDatabaseWithTimeToLive(name string, spec *TimeToLiveSpec) (*DatabaseInfo, error)
	DropDatabase(name string) error

	CreateTimeToLive(database string, spec *TimeToLiveSpec, makeDefault bool) (*TimeToLiveInfo, error)
	TimeToLive(database, name string) (ttli *TimeToLiveInfo, err error)
	DropTimeToLive(database, name string) error
	SetDefaultTimeToLive(database, name string) error
	UpdateTimeToLive(database, name string, ttlu *TimeToLiveUpdate, makeDefault bool) error

	Users() []UserInfo
	UserCount() int
	User(name string) (User, error)
	CreateUser(name, password string, admin bool) (User, error)
	UpdateUser(name, password string) error
	DropUser(name string) error

	SetPrivilege(username, database string, p cnosql.Privilege) error
	SetAdminPrivilege(username string, admin bool) error
	UserPrivileges(username string) (map[string]cnosql.Privilege, error)
	UserPrivilege(username, database string) (*cnosql.Privilege, error)
	AdminUserExists() bool
	Authenticate(username, password string) (User, error)

	ShardIDs() []uint64
	RegionsByTimeRange(database, ttl string, min, max time.Time) (a []RegionInfo, err error)
	ShardsByTimeRange(sources cnosql.Sources, tmin, tmax time.Time) (a []ShardInfo, err error)
	DropShard(id uint64) error
	TruncateRegions(t time.Time) error
	PruneRegions() error
	CreateRegion(database, ttl string, timestamp time.Time) (*RegionInfo, error)
	DeleteRegion(database, ttl string, id uint64) error
	PrecreateRegions(from, to time.Time) error
	ShardOwner(shardID uint64) (database, ttl string, sgi *RegionInfo)

	CreateContinuousQuery(database, name, query string) error
	DropContinuousQuery(database, name string) error

	CreateSubscription(database, ttl, name, mode string, destinations []string) error
	DropSubscription(database, ttl, name string) error

	SetData(data *Data) error
	Data() Data
	WaitForDataChanged() chan struct{}

	Load() error
	MarshalBinary() ([]byte, error)
	WithLogger(log *zap.Logger)
}

var _ MetaClient = &Client{}

// Client is used to execute commands on and read data from
// a meta service cluster.
type Client struct {
	logger *zap.Logger
	nodeID uint64

	mu        sync.RWMutex
	closing   chan struct{}
	changed   chan struct{}
	cacheData *Data

	// Authentication cache.
	authCache map[string]authUser

	path string

	timeToLiveAutoCreate bool
}

type authUser struct {
	bhash string
	salt  []byte
	hash  []byte
}

// NewClient returns a new *
func NewClient(config *Config) *Client {
	return &Client{
		cacheData: &Data{
			ClusterID: uint64(rand.Int63()),
			Index:     1,
		},
		closing:              make(chan struct{}),
		changed:              make(chan struct{}),
		logger:               zap.NewNop(),
		authCache:            make(map[string]authUser),
		path:                 config.Dir,
		timeToLiveAutoCreate: config.TimeToLiveAutoCreate,
	}
}

// Open a connection to a meta service cluster.
func (c *Client) Open() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Try to load from disk
	if err := c.Load(); err != nil {
		return err
	}

	// If this is a brand new instance, persist to disk immediatly.
	if c.cacheData.Index == 1 {
		if err := snapshot(c.path, c.cacheData); err != nil {
			return err
		}
	}

	return nil
}

// Close the meta service cluster connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if t, ok := http.DefaultTransport.(*http.Transport); ok {
		t.CloseIdleConnections()
	}

	select {
	case <-c.closing:
		return nil
	default:
		close(c.closing)
	}

	return nil
}

// NodeID returns the client's node ID.
func (c *Client) NodeID() uint64 { return c.nodeID }

// ClusterID returns the ID of the cluster it's connected to.
func (c *Client) ClusterID() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.cacheData.ClusterID
}

func (c *Client) Ping(checkAllMetaServers bool) error { return nil }

// AcquireLease attempts to acquire the specified lease.
func (c *Client) AcquireLease(name string) (*Lease, error) {
	l := Lease{
		Name:       name,
		Expiration: time.Now().Add(DefaultLeaseDuration),
	}
	return &l, nil
}

func (c *Client) SetMetaServers([]string) {
	// Do nothing
}

func (c *Client) DataNode(id uint64) (*NodeInfo, error)                      { return nil, nil }
func (c *Client) DataNodes() ([]NodeInfo, error)                             { return nil, nil }
func (c *Client) CreateDataNode(httpAddr, tcpAddr string) (*NodeInfo, error) { return nil, nil }
func (c *Client) DataNodeByHTTPHost(httpAddr string) (*NodeInfo, error)      { return nil, nil }
func (c *Client) DataNodeByTCPHost(tcpAddr string) (*NodeInfo, error)        { return nil, nil }
func (c *Client) DeleteDataNode(id uint64) error                             { return nil }
func (c *Client) MetaNodes() ([]NodeInfo, error)                             { return nil, nil }
func (c *Client) MetaNodeByAddr(addr string) *NodeInfo                       { return nil }
func (c *Client) CreateMetaNode(httpAddr, tcpAddr string) (*NodeInfo, error) { return nil, nil }
func (c *Client) DeleteMetaNode(id uint64) error                             { return nil }

// Database returns info for the requested database.
func (c *Client) Database(name string) *DatabaseInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, d := range c.cacheData.Databases {
		if d.Name == name {
			return &d
		}
	}

	return nil
}

// Databases returns a list of all database infos.
func (c *Client) Databases() []DatabaseInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	dbs := c.cacheData.Databases
	if dbs == nil {
		return []DatabaseInfo{}
	}
	return dbs
}

// CreateDatabase creates a database or returns it if it already exists.
func (c *Client) CreateDatabase(name string) (*DatabaseInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()

	if db := data.Database(name); db != nil {
		return db, nil
	}

	if err := data.CreateDatabase(name); err != nil {
		return nil, err
	}

	// create default time-to-live
	if c.timeToLiveAutoCreate {
		ttli := DefaultTimeToLiveInfo()
		if err := data.CreateTimeToLive(name, ttli, true); err != nil {
			return nil, err
		}
	}

	db := data.Database(name)

	if err := c.commit(data); err != nil {
		return nil, err
	}

	return db, nil
}

// CreateDatabaseWithTimeToLive creates a database with the specified
// time-to-live.
//
// When creating a database with a time-to-live, the time-to-live will
// always be set to default. Therefore if the caller provides a time-to-live
// that already exists on the database, but that time-to-live is not the
// default one, an error will be returned.
//
// This call is only idempotent when the caller provides the exact same
// time-to-live, and that time-to-live is already the default for the
// database.
//
func (c *Client) CreateDatabaseWithTimeToLive(name string, spec *TimeToLiveSpec) (*DatabaseInfo, error) {
	if spec == nil {
		return nil, errors.New("CreateDatabaseWithTimeToLive called with nil spec")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()

	if spec.Duration != nil && *spec.Duration < MinTimeToLiveDuration && *spec.Duration != 0 {
		return nil, ErrTimeToLiveDurationTooLow
	}

	db := data.Database(name)
	if db == nil {
		if err := data.CreateDatabase(name); err != nil {
			return nil, err
		}
		db = data.Database(name)
	}

	// No existing time-to-lives, so we can create the provided time-to-live as
	// the new default ttl.
	ttli := spec.NewTimeToLiveInfo()
	if len(db.TimeToLives) == 0 {
		if err := data.CreateTimeToLive(name, ttli, true); err != nil {
			return nil, err
		}
	} else if !spec.Matches(db.TimeToLive(ttli.Name)) {
		// In this case we already have a time-to-live on the database and
		// the provided time-to-live does not match it. Therefore, this call
		// is not idempotent and we need to return an error.
		return nil, ErrTimeToLiveConflict
	}

	// If a non-default time-to-live was passed in that already exists then
	// it's an error regardless of if the exact same time-to-live is
	// provided. CREATE DATABASE WITH TTL should only be used to
	// create DEFAULT time-to-lives.
	if db.DefaultTimeToLive != ttli.Name {
		return nil, ErrTimeToLiveConflict
	}

	// Commit the changes.
	if err := c.commit(data); err != nil {
		return nil, err
	}

	// Refresh the database info.
	db = data.Database(name)

	return db, nil
}

// DropDatabase deletes a database.
func (c *Client) DropDatabase(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()

	if err := data.DropDatabase(name); err != nil {
		return err
	}

	if err := c.commit(data); err != nil {
		return err
	}

	return nil
}

// CreateTimeToLive creates a time-to-live on the specified database.
func (c *Client) CreateTimeToLive(database string, spec *TimeToLiveSpec, makeDefault bool) (*TimeToLiveInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()

	if spec.Duration != nil && *spec.Duration < MinTimeToLiveDuration && *spec.Duration != 0 {
		return nil, ErrTimeToLiveDurationTooLow
	}

	ttl := spec.NewTimeToLiveInfo()
	if err := data.CreateTimeToLive(database, ttl, makeDefault); err != nil {
		return nil, err
	}

	if err := c.commit(data); err != nil {
		return nil, err
	}

	return ttl, nil
}

// TimeToLive returns the requested time-to-live info.
func (c *Client) TimeToLive(database, name string) (ttli *TimeToLiveInfo, err error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	db := c.cacheData.Database(database)
	if db == nil {
		return nil, cnosdb.ErrDatabaseNotFound(database)
	}

	return db.TimeToLive(name), nil
}

// DropTimeToLive drops a time-to-live from a database.
func (c *Client) DropTimeToLive(database, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()

	if err := data.DropTimeToLive(database, name); err != nil {
		return err
	}

	if err := c.commit(data); err != nil {
		return err
	}

	return nil
}

// SetDefaultTimeToLive sets a database's default time to live.
func (c *Client) SetDefaultTimeToLive(database, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()

	if err := data.SetDefaultTimeToLive(database, name); err != nil {
		return err
	}

	if err := c.commit(data); err != nil {
		return err
	}

	return nil
}

// UpdateTimeToLive updates a time-to-live.
func (c *Client) UpdateTimeToLive(database, name string, ttlu *TimeToLiveUpdate, makeDefault bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()

	if err := data.UpdateTimeToLive(database, name, ttlu, makeDefault); err != nil {
		return err
	}

	if err := c.commit(data); err != nil {
		return err
	}

	return nil
}

// Users returns a slice of UserInfo representing the currently known users.
func (c *Client) Users() []UserInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	users := c.cacheData.Users

	if users == nil {
		return []UserInfo{}
	}
	return users
}

// User returns the user with the given name, or ErrUserNotFound.
func (c *Client) User(name string) (User, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, u := range c.cacheData.Users {
		if u.Name == name {
			return &u, nil
		}
	}

	return nil, ErrUserNotFound
}

// bcryptCost is the cost associated with generating password with bcrypt.
// This setting is lowered during testing to improve test suite performance.
var bcryptCost = bcrypt.DefaultCost

// hashWithSalt returns a salted hash of password using salt.
func (c *Client) hashWithSalt(salt []byte, password string) []byte {
	hasher := sha256.New()
	hasher.Write(salt)
	hasher.Write([]byte(password))
	return hasher.Sum(nil)
}

// saltedHash returns a salt and salted hash of password.
func (c *Client) saltedHash(password string) (salt, hash []byte, err error) {
	salt = make([]byte, SaltBytes)
	if _, err := io.ReadFull(crand.Reader, salt); err != nil {
		return nil, nil, err
	}

	return salt, c.hashWithSalt(salt, password), nil
}

// CreateUser adds a user with the given name and password and admin status.
func (c *Client) CreateUser(name, password string, admin bool) (User, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()

	// See if the user already exists.
	if u := data.user(name); u != nil {
		if err := bcrypt.CompareHashAndPassword([]byte(u.Hash), []byte(password)); err != nil || u.Admin != admin {
			return nil, ErrUserExists
		}
		return u, nil
	}

	// Hash the password before serializing it.
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcryptCost)
	if err != nil {
		return nil, err
	}

	if err := data.CreateUser(name, string(hash), admin); err != nil {
		return nil, err
	}

	u := data.user(name)

	if err := c.commit(data); err != nil {
		return nil, err
	}

	return u, nil
}

// UpdateUser updates the password of an existing user.
func (c *Client) UpdateUser(name, password string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()

	// Hash the password before serializing it.
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcryptCost)
	if err != nil {
		return err
	}

	if err := data.UpdateUser(name, string(hash)); err != nil {
		return err
	}

	delete(c.authCache, name)

	return c.commit(data)
}

// DropUser removes the user with the given name.
func (c *Client) DropUser(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()

	if err := data.DropUser(name); err != nil {
		return err
	}

	if err := c.commit(data); err != nil {
		return err
	}

	return nil
}

// SetPrivilege sets a privilege for the given user on the given database.
func (c *Client) SetPrivilege(username, database string, p cnosql.Privilege) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()

	if err := data.SetPrivilege(username, database, p); err != nil {
		return err
	}

	if err := c.commit(data); err != nil {
		return err
	}

	return nil
}

// SetAdminPrivilege sets or unsets admin privilege to the given username.
func (c *Client) SetAdminPrivilege(username string, admin bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()

	if err := data.SetAdminPrivilege(username, admin); err != nil {
		return err
	}

	if err := c.commit(data); err != nil {
		return err
	}

	return nil
}

// UserPrivileges returns the privileges for a user mapped by database name.
func (c *Client) UserPrivileges(username string) (map[string]cnosql.Privilege, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	p, err := c.cacheData.UserPrivileges(username)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// UserPrivilege returns the privilege for the given user on the given database.
func (c *Client) UserPrivilege(username, database string) (*cnosql.Privilege, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	p, err := c.cacheData.UserPrivilege(username, database)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// AdminUserExists returns true if any user has admin privilege.
func (c *Client) AdminUserExists() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cacheData.AdminUserExists()
}

// Authenticate returns a UserInfo if the username and password match an existing entry.
func (c *Client) Authenticate(username, password string) (User, error) {
	// Find user.
	c.mu.RLock()
	userInfo := c.cacheData.user(username)
	c.mu.RUnlock()
	if userInfo == nil {
		return nil, ErrUserNotFound
	}

	// Check the local auth cache first.
	c.mu.RLock()
	au, ok := c.authCache[username]
	c.mu.RUnlock()
	if ok {
		// verify the password using the cached salt and hash
		if bytes.Equal(c.hashWithSalt(au.salt, password), au.hash) {
			return userInfo, nil
		}

		// fall through to requiring a full bcrypt hash for invalid passwords
	}

	// Compare password with user hash.
	if err := bcrypt.CompareHashAndPassword([]byte(userInfo.Hash), []byte(password)); err != nil {
		return nil, ErrAuthenticate
	}

	// generate a salt and hash of the password for the cache
	salt, hashed, err := c.saltedHash(password)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.authCache[username] = authUser{salt: salt, hash: hashed, bhash: userInfo.Hash}
	c.mu.Unlock()
	return userInfo, nil
}

// UserCount returns the number of users stored.
func (c *Client) UserCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.cacheData.Users)
}

// ShardIDs returns a list of all shard ids.
func (c *Client) ShardIDs() []uint64 {
	c.mu.RLock()

	var a []uint64
	for _, dbi := range c.cacheData.Databases {
		for _, ttli := range dbi.TimeToLives {
			for _, sgi := range ttli.Regions {
				for _, si := range sgi.Shards {
					a = append(a, si.ID)
				}
			}
		}
	}
	c.mu.RUnlock()
	sort.Sort(uint64Slice(a))
	return a
}

// RegionsByTimeRange returns a list of all regions on a database and time-to-live that may contain data
// for the specified time range. Regions are sorted by start time.
func (c *Client) RegionsByTimeRange(database, ttl string, min, max time.Time) (a []RegionInfo, err error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Find time-to-live.
	ttli, err := c.cacheData.TimeToLive(database, ttl)
	if err != nil {
		return nil, err
	} else if ttli == nil {
		return nil, cnosdb.ErrTimeToLiveNotFound(ttl)
	}
	groups := make([]RegionInfo, 0, len(ttli.Regions))
	for _, g := range ttli.Regions {
		if g.Deleted() || !g.Overlaps(min, max) {
			continue
		}
		groups = append(groups, g)
	}
	return groups, nil
}

// ShardsByTimeRange returns a slice of shards that may contain data in the time range.
func (c *Client) ShardsByTimeRange(sources cnosql.Sources, tmin, tmax time.Time) (a []ShardInfo, err error) {
	m := make(map[*ShardInfo]struct{})
	for _, mm := range sources.Metrics() {
		groups, err := c.RegionsByTimeRange(mm.Database, mm.TimeToLive, tmin, tmax)
		if err != nil {
			return nil, err
		}
		for _, g := range groups {
			for i := range g.Shards {
				m[&g.Shards[i]] = struct{}{}
			}
		}
	}

	a = make([]ShardInfo, 0, len(m))
	for sh := range m {
		a = append(a, *sh)
	}

	return a, nil
}

// DropShard deletes a shard by ID.
func (c *Client) DropShard(id uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()
	data.DropShard(id)
	return c.commit(data)
}

// TruncateRegions truncates any region that could contain timestamps beyond t.
func (c *Client) TruncateRegions(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()
	data.TruncateRegions(t)
	return c.commit(data)
}

// PruneRegions remove deleted regions from the data store.
func (c *Client) PruneRegions() error {
	var changed bool
	expiration := time.Now().Add(RegionDeletedExpiration)
	c.mu.Lock()
	defer c.mu.Unlock()
	data := c.cacheData.Clone()
	for i, d := range data.Databases {
		for j, ttl := range d.TimeToLives {
			var remainingRegions []RegionInfo
			for _, sgi := range ttl.Regions {
				if sgi.DeletedAt.IsZero() || !expiration.After(sgi.DeletedAt) {
					remainingRegions = append(remainingRegions, sgi)
					continue
				}
				changed = true
			}
			data.Databases[i].TimeToLives[j].Regions = remainingRegions
		}
	}
	if changed {
		return c.commit(data)
	}
	return nil
}

// CreateRegion creates a region on a database and time-to-live for a given timestamp.
func (c *Client) CreateRegion(database, ttl string, timestamp time.Time) (*RegionInfo, error) {
	// Check under a read-lock
	c.mu.RLock()
	if rg, _ := c.cacheData.RegionByTimestamp(database, ttl, timestamp); rg != nil {
		c.mu.RUnlock()
		return rg, nil
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check again under the write lock
	data := c.cacheData.Clone()
	if rg, _ := data.RegionByTimestamp(database, ttl, timestamp); rg != nil {
		return rg, nil
	}

	sgi, err := createRegion(data, database, ttl, timestamp)
	if err != nil {
		return nil, err
	}

	if err := c.commit(data); err != nil {
		return nil, err
	}

	return sgi, nil
}

func createRegion(data *Data, database, ttl string, timestamp time.Time) (*RegionInfo, error) {
	// It is the responsibility of the caller to check if it exists before calling this method.
	if rg, _ := data.RegionByTimestamp(database, ttl, timestamp); rg != nil {
		return nil, ErrRegionExists
	}

	if err := data.CreateRegion(database, ttl, timestamp); err != nil {
		return nil, err
	}

	ttli, err := data.TimeToLive(database, ttl)
	if err != nil {
		return nil, err
	} else if ttli == nil {
		return nil, errors.New("time-to-live deleted after region created")
	}

	sgi := ttli.RegionByTimestamp(timestamp)
	return sgi, nil
}

// DeleteRegion removes a region from a database and time-to-live by id.
func (c *Client) DeleteRegion(database, ttl string, id uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()

	if err := data.DeleteRegion(database, ttl, id); err != nil {
		return err
	}

	if err := c.commit(data); err != nil {
		return err
	}

	return nil
}

// PrecreateRegions creates regions whose endtime is before the 'to' time passed in, but
// is yet to expire before 'from'. This is to avoid the need for these shards to be created when data
// for the corresponding time range arrives. Shard creation involves Raft consensus, and precreation
// avoids taking the hit at write-time.
func (c *Client) PrecreateRegions(from, to time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	data := c.cacheData.Clone()
	var changed bool

	for _, di := range data.Databases {
		for _, ttl := range di.TimeToLives {
			if len(ttl.Regions) == 0 {
				// No data was ever written to this region, or all groups have been deleted.
				continue
			}
			g := ttl.Regions[len(ttl.Regions)-1] // Get the last region in time.
			if !g.Deleted() && g.EndTime.Before(to) && g.EndTime.After(from) {
				// Region is not deleted, will end before the future time, but is still yet to expire.
				// This last check is important, so the system doesn't create shards groups wholly
				// in the past.

				// Create successive region.
				nextRegionTime := g.EndTime.Add(1 * time.Nanosecond)
				// if it already exists, continue
				if rg, _ := data.RegionByTimestamp(di.Name, ttl.Name, nextRegionTime); rg != nil {
					c.logger.Info("region already exists",
						logger.Region(rg.ID),
						logger.Database(di.Name),
						logger.TimeToLive(ttl.Name))
					continue
				}
				newGroup, err := createRegion(data, di.Name, ttl.Name, nextRegionTime)
				if err != nil {
					c.logger.Info("Failed to precreate successive region",
						zap.Uint64("group_id", g.ID), zap.Error(err))
					continue
				}
				changed = true
				c.logger.Info("New region successfully precreated",
					logger.Region(newGroup.ID),
					logger.Database(di.Name),
					logger.TimeToLive(ttl.Name))
			}
		}
	}

	if changed {
		if err := c.commit(data); err != nil {
			return err
		}
	}

	return nil
}

// ShardOwner returns the owning region info for a specific shard.
func (c *Client) ShardOwner(shardID uint64) (database, ttl string, sgi *RegionInfo) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, dbi := range c.cacheData.Databases {
		for _, ttli := range dbi.TimeToLives {
			for _, g := range ttli.Regions {
				if g.Deleted() {
					continue
				}

				for _, sh := range g.Shards {
					if sh.ID == shardID {
						database = dbi.Name
						ttl = ttli.Name
						sgi = &g
						return
					}
				}
			}
		}
	}
	return
}

// CreateContinuousQuery saves a continuous query with the given name for the given database.
func (c *Client) CreateContinuousQuery(database, name, query string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()

	if err := data.CreateContinuousQuery(database, name, query); err != nil {
		return err
	}

	if err := c.commit(data); err != nil {
		return err
	}

	return nil
}

// DropContinuousQuery removes the continuous query with the given name on the given database.
func (c *Client) DropContinuousQuery(database, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()

	if err := data.DropContinuousQuery(database, name); err != nil {
		return err
	}

	if err := c.commit(data); err != nil {
		return err
	}

	return nil
}

// CreateSubscription creates a subscription against the given database and time-to-live.
func (c *Client) CreateSubscription(database, ttl, name, mode string, destinations []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()

	if err := data.CreateSubscription(database, ttl, name, mode, destinations); err != nil {
		return err
	}

	if err := c.commit(data); err != nil {
		return err
	}

	return nil
}

// DropSubscription removes the named subscription from the given database and time-to-live.
func (c *Client) DropSubscription(database, ttl, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()

	if err := data.DropSubscription(database, ttl, name); err != nil {
		return err
	}

	if err := c.commit(data); err != nil {
		return err
	}

	return nil
}

// SetData overwrites the underlying data in the meta store.
func (c *Client) SetData(data *Data) error {
	c.mu.Lock()

	d := data.Clone()

	if err := c.commit(d); err != nil {
		return err
	}

	c.mu.Unlock()

	return nil
}

// Data returns a clone of the underlying data in the meta store.
func (c *Client) Data() Data {
	c.mu.RLock()
	defer c.mu.RUnlock()
	d := c.cacheData.Clone()
	return *d
}

// WaitForDataChanged returns a channel that will get closed when
// the metastore data has changed.
func (c *Client) WaitForDataChanged() chan struct{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.changed
}

// commit writes data to the underlying store.
// This method assumes c's mutex is already locked.
func (c *Client) commit(data *Data) error {
	data.Index++

	// try to write to disk before updating in memory
	if err := snapshot(c.path, data); err != nil {
		return err
	}

	// update in memory
	c.cacheData = data

	// close channels to signal changes
	close(c.changed)
	c.changed = make(chan struct{})

	return nil
}

// MarshalBinary returns a binary representation of the underlying data.
func (c *Client) MarshalBinary() ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cacheData.MarshalBinary()
}

// WithLogger sets the logger for the
func (c *Client) WithLogger(log *zap.Logger) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.logger = log.With(zap.String("service", "meta-client"))
}

// snapshot saves the current meta data to disk.
func snapshot(path string, data *Data) error {
	filename := filepath.Join(path, metaFile)
	tmpFile := filename + "tmp"

	f, err := os.Create(tmpFile)
	if err != nil {
		return err
	}
	defer f.Close()

	var d []byte
	if b, err := data.MarshalBinary(); err != nil {
		return err
	} else {
		d = b
	}

	if _, err := f.Write(d); err != nil {
		return err
	}

	if err = f.Sync(); err != nil {
		return err
	}

	//close file handle before renaming to support Windows
	if err = f.Close(); err != nil {
		return err
	}

	return file.RenameFile(tmpFile, filename)
}

// Load loads the current meta data from disk.
func (c *Client) Load() error {
	file := filepath.Join(c.path, metaFile)

	f, err := os.Open(file)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	data, err := ioutil.ReadAll(f)
	if err != nil {
		return err
	}

	if err := c.cacheData.UnmarshalBinary(data); err != nil {
		return err
	}
	return nil
}

type uint64Slice []uint64

func (a uint64Slice) Len() int           { return len(a) }
func (a uint64Slice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a uint64Slice) Less(i, j int) bool { return a[i] < a[j] }
