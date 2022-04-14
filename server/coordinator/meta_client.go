package coordinator

import (
	"time"

	"github.com/cnosdb/cnosdb/meta"
	"github.com/cnosdb/cnosdb/vend/cnosql"
)

// MetaClient is an interface for accessing meta data.
type MetaClient interface {
	CreateContinuousQuery(database, name, query string) error
	CreateDatabase(name string) (*meta.DatabaseInfo, error)
	CreateDatabaseWithRetentionPolicy(name string, spec *meta.RetentionPolicySpec) (*meta.DatabaseInfo, error)
	CreateRetentionPolicy(database string, spec *meta.RetentionPolicySpec, makeDefault bool) (*meta.RetentionPolicyInfo, error)
	CreateSubscription(database, rp, name, mode string, destinations []string) error
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
	DropRetentionPolicy(database, name string) error
	DropSubscription(database, rp, name string) error
	DropUser(name string) error
	ShardGroupsByTimeRange(database, rp string, min, max time.Time) (a []meta.ShardGroupInfo, err error)
	SetAdminPrivilege(username string, admin bool) error
	SetDefaultRetentionPolicy(database, name string) error
	SetPrivilege(username, database string, p cnosql.Privilege) error
	ShardsByTimeRange(sources cnosql.Sources, tmin, tmax time.Time) (a []meta.ShardInfo, err error)
	RetentionPolicy(database, name string) (rp *meta.RetentionPolicyInfo, err error)
	TruncateShardGroups(t time.Time) error
	UpdateRetentionPolicy(database, name string, rpu *meta.RetentionPolicyUpdate, makeDefault bool) error
	UpdateUser(name, password string) error
	UserPrivilege(username, database string) (*cnosql.Privilege, error)
	UserPrivileges(username string) (map[string]cnosql.Privilege, error)
	Users() []meta.UserInfo
}
