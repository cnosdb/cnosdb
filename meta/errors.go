package meta

import (
	"errors"
	"fmt"
)

var (
	// ErrStoreOpen is returned when opening an already open store.
	ErrStoreOpen = errors.New("store already open")

	// ErrStoreClosed is returned when closing an already closed store.
	ErrStoreClosed = errors.New("raft store already closed")
)

var (
	// ErrNodeExists is returned when creating an already existing node.
	ErrNodeExists = errors.New("node already exists")

	// ErrNodeNotFound is returned when mutating a node that doesn't exist.
	ErrNodeNotFound = errors.New("node not found")

	// ErrNodesRequired is returned when at least one node is required for an operation.
	// This occurs when creating a region.
	ErrNodesRequired = errors.New("at least one node required")

	// ErrNodeIDRequired is returned when using a zero node id.
	ErrNodeIDRequired = errors.New("node id must be greater than 0")

	// ErrNodeUnableToDropFinalNode is returned if the node being dropped is the last
	// node in the cluster
	ErrNodeUnableToDropFinalNode = errors.New("unable to drop the final node in a cluster")
)

var (
	// ErrDatabaseExists is returned when creating an already existing database.
	ErrDatabaseExists = errors.New("database already exists")

	// ErrDatabaseNotExists is returned when operating on a not existing database.
	ErrDatabaseNotExists = errors.New("database does not exist")

	// ErrDatabaseNameRequired is returned when creating a database without a name.
	ErrDatabaseNameRequired = errors.New("database name required")

	// ErrNameTooLong is returned when attempting to create a database or
	// time-to-live with a name that is too long.
	ErrNameTooLong = errors.New("name too long")

	// ErrInvalidName is returned when attempting to create a database or time-to-live with an invalid name
	ErrInvalidName = errors.New("invalid name")
)

var (
	// ErrTimeToLiveExists is returned when creating an already existing time-to-live.
	ErrTimeToLiveExists = errors.New("time-to-live already exists")

	// ErrTimeToLiveNotFound is returned when an expected time-to-live wasn't found.
	ErrTimeToLiveNotFound = errors.New("time-to-live not found")

	// ErrTimeToLiveDefault is returned when attempting a prohibited operation
	// on a default time-to-live.
	ErrTimeToLiveDefault = errors.New("time-to-live is default")

	// ErrTimeToLiveRequired is returned when a time-to-live is required
	// by an operation, but a nil time-to-live was passed.
	ErrTimeToLiveRequired = errors.New("time-to-live required")

	// ErrTimeToLiveNameRequired is returned when creating a time-to-live without a name.
	ErrTimeToLiveNameRequired = errors.New("time-to-live name required")

	// ErrTimeToLiveNameExists is returned when renaming a time-to-live to
	// the same name as another existing time-to-live.
	ErrTimeToLiveNameExists = errors.New("time-to-live name already exists")

	// ErrTimeToLiveDurationTooLow is returned when updating a time-to-live
	// that has a duration lower than the allowed minimum.
	ErrTimeToLiveDurationTooLow = fmt.Errorf("time-to-live duration must be at least %s", MinTimeToLiveDuration)

	// ErrTimeToLiveConflict is returned when creating a time-to-live conflicts
	// with an existing time-to-live.
	ErrTimeToLiveConflict = errors.New("time-to-live conflicts with an existing time-to-live")

	// ErrIncompatibleDurations is returned when creating or updating a
	// time-to-live that has a duration lower than the current shard
	// duration.
	ErrIncompatibleDurations = errors.New("time-to-live duration must be greater than the shard duration")

	// ErrReplicationFactorTooLow is returned when the replication factor is not in an
	// acceptable range.
	ErrReplicationFactorTooLow = errors.New("replication factor must be greater than 0")
)

var (
	// ErrRegionExists is returned when creating an already existing region.
	ErrRegionExists = errors.New("region already exists")

	// ErrRegionNotFound is returned when mutating a region that doesn't exist.
	ErrRegionNotFound = errors.New("region not found")

	// ErrShardNotReplicated is returned if the node requested to be dropped has
	// the last copy of a shard present and the force keyword was not used
	ErrShardNotReplicated = errors.New("shard not replicated")
)

var (
	// ErrContinuousQueryExists is returned when creating an already existing continuous query.
	ErrContinuousQueryExists = errors.New("continuous query already exists")

	// ErrContinuousQueryNotFound is returned when removing a continuous query that doesn't exist.
	ErrContinuousQueryNotFound = errors.New("continuous query not found")
)

var (
	// ErrSubscriptionExists is returned when creating an already existing subscription.
	ErrSubscriptionExists = errors.New("subscription already exists")

	// ErrSubscriptionNotFound is returned when removing a subscription that doesn't exist.
	ErrSubscriptionNotFound = errors.New("subscription not found")
)

// ErrInvalidSubscriptionURL is returned when the subscription's destination URL is invalid.
func ErrInvalidSubscriptionURL(url string) error {
	return fmt.Errorf("invalid subscription URL: %s", url)
}

var (
	// ErrUserExists is returned when creating an already existing user.
	ErrUserExists = errors.New("user already exists")

	// ErrUserNotFound is returned when mutating a user that doesn't exist.
	ErrUserNotFound = errors.New("user not found")

	// ErrUsernameRequired is returned when creating a user without a username.
	ErrUsernameRequired = errors.New("username required")

	// ErrAuthenticate is returned when authentication fails.
	ErrAuthenticate = errors.New("authentication failed")
)
