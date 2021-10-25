package coordinator

import (
	"time"

	"github.com/cnosdatabase/cnosdb/meta"
	"github.com/cnosdatabase/cnosql"
)

// MetaClient is an interface for accessing meta data.
type MetaClient interface {
	CreateContinuousQuery(database, name, query string) error
	CreateDatabase(name string) (*meta.DatabaseInfo, error)
	CreateDatabaseWithTimeToLive(name string, spec *meta.TimeToLiveSpec) (*meta.DatabaseInfo, error)
	CreateTimeToLive(database string, spec *meta.TimeToLiveSpec, makeDefault bool) (*meta.TimeToLiveInfo, error)
	CreateSubscription(database, ttl, name, mode string, destinations []string) error
	CreateUser(name, password string, admin bool) (meta.User, error)
	Database(name string) *meta.DatabaseInfo
	Databases() []meta.DatabaseInfo
	DataNode(id uint64) (*meta.NodeInfo, error)
	DataNodes() ([]meta.NodeInfo, error)
	DeleteDataNode(id uint64) error
	MetaNodes() ([]meta.NodeInfo, error)
	DeleteMetaNode(id uint64) error
	DropShard(id uint64) error
	DropContinuousQuery(database, name string) error
	DropDatabase(name string) error
	DropTimeToLive(database, name string) error
	DropSubscription(database, ttl, name string) error
	DropUser(name string) error
	RegionsByTimeRange(database, ttl string, min, max time.Time) (a []meta.RegionInfo, err error)
	SetAdminPrivilege(username string, admin bool) error
	SetDefaultTimeToLive(database, name string) error
	SetPrivilege(username, database string, p cnosql.Privilege) error
	ShardsByTimeRange(sources cnosql.Sources, tmin, tmax time.Time) (a []meta.ShardInfo, err error)
	TimeToLive(database, name string) (ttl *meta.TimeToLiveInfo, err error)
	TruncateRegions(t time.Time) error
	UpdateTimeToLive(database, name string, ttlu *meta.TimeToLiveUpdate, makeDefault bool) error
	UpdateUser(name, password string) error
	UserPrivilege(username, database string) (*cnosql.Privilege, error)
	UserPrivileges(username string) (map[string]cnosql.Privilege, error)
	Users() []meta.UserInfo
}
