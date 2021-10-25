package meta

import (
	"fmt"
	"io"
	"io/ioutil"
	"time"

	internal "github.com/cnosdatabase/cnosdb/meta/internal"
	"github.com/cnosdatabase/cnosql"
	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/raft"
)

// storeFSM represents the finite state machine used by Store to interact with Raft.
type storeFSM store

func (fsm *storeFSM) Apply(l *raft.Log) interface{} {
	var cmd internal.Command
	if err := proto.Unmarshal(l.Data, &cmd); err != nil {
		panic(fmt.Errorf("cannot marshal command: %x", l.Data))
	}

	// Lock the store.
	s := (*store)(fsm)

	s.mu.Lock()
	defer s.mu.Unlock()

	err := func() interface{} {
		switch cmd.GetType() {
		case internal.Command_RemovePeerCommand:
			return fsm.applyRemovePeerCommand(&cmd)
		case internal.Command_CreateNodeCommand:
			// create node was in < 0.10.0 servers, we need the peers
			// list to convert to the appropriate data/meta nodes now
			peers, err := s.raftState.peers()
			if err != nil {
				return err
			}
			return fsm.applyCreateNodeCommand(&cmd, peers)
		case internal.Command_DeleteNodeCommand:
			return fsm.applyDeleteNodeCommand(&cmd)
		case internal.Command_CreateDatabaseCommand:
			return fsm.applyCreateDatabaseCommand(&cmd)
		case internal.Command_DropDatabaseCommand:
			return fsm.applyDropDatabaseCommand(&cmd)
		case internal.Command_CreateTimeToLiveCommand:
			return fsm.applyCreateTimeToLiveCommand(&cmd)
		case internal.Command_DropTimeToLiveCommand:
			return fsm.applyDropTimeToLiveCommand(&cmd)
		case internal.Command_SetDefaultTimeToLiveCommand:
			return fsm.applySetDefaultTimeToLiveCommand(&cmd)
		case internal.Command_UpdateTimeToLiveCommand:
			return fsm.applyUpdateTimeToLiveCommand(&cmd)
		case internal.Command_CreateRegionCommand:
			return fsm.applyCreateRegionCommand(&cmd)
		case internal.Command_DeleteRegionCommand:
			return fsm.applyDeleteRegionCommand(&cmd)
		case internal.Command_CreateContinuousQueryCommand:
			return fsm.applyCreateContinuousQueryCommand(&cmd)
		case internal.Command_DropContinuousQueryCommand:
			return fsm.applyDropContinuousQueryCommand(&cmd)
		case internal.Command_CreateSubscriptionCommand:
			return fsm.applyCreateSubscriptionCommand(&cmd)
		case internal.Command_DropSubscriptionCommand:
			return fsm.applyDropSubscriptionCommand(&cmd)
		case internal.Command_CreateUserCommand:
			return fsm.applyCreateUserCommand(&cmd)
		case internal.Command_DropUserCommand:
			return fsm.applyDropUserCommand(&cmd)
		case internal.Command_UpdateUserCommand:
			return fsm.applyUpdateUserCommand(&cmd)
		case internal.Command_SetPrivilegeCommand:
			return fsm.applySetPrivilegeCommand(&cmd)
		case internal.Command_SetAdminPrivilegeCommand:
			return fsm.applySetAdminPrivilegeCommand(&cmd)
		case internal.Command_SetDataCommand:
			return fsm.applySetDataCommand(&cmd)
		case internal.Command_UpdateNodeCommand:
			return fsm.applyUpdateNodeCommand(&cmd)
		case internal.Command_CreateMetaNodeCommand:
			return fsm.applyCreateMetaNodeCommand(&cmd)
		case internal.Command_DeleteMetaNodeCommand:
			return fsm.applyDeleteMetaNodeCommand(&cmd, s)
		case internal.Command_SetMetaNodeCommand:
			return fsm.applySetMetaNodeCommand(&cmd)
		case internal.Command_CreateDataNodeCommand:
			return fsm.applyCreateDataNodeCommand(&cmd)
		case internal.Command_DeleteDataNodeCommand:
			return fsm.applyDeleteDataNodeCommand(&cmd)
		default:
			panic(fmt.Errorf("cannot apply command: %x", l.Data))
		}
	}()

	// Copy term and index to new metadata.
	fsm.data.Term = l.Term
	fsm.data.Index = l.Index

	// signal that the data changed
	close(s.dataChanged)
	s.dataChanged = make(chan struct{})

	return err
}

func (fsm *storeFSM) applyRemovePeerCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_RemovePeerCommand_Command)
	v := ext.(*internal.RemovePeerCommand)

	addr := v.GetAddr()

	// Only do this if you are the leader
	if fsm.raftState.isLeader() {
		//Remove that node from the peer
		fsm.logger.Printf("removing voter: %s", addr)
		if err := fsm.raftState.removeVoter(addr); err != nil {
			fsm.logger.Printf("error removing peer: %s", err)
		}
	}

	return nil
}

func (fsm *storeFSM) applyCreateNodeCommand(cmd *internal.Command, peers []string) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_CreateNodeCommand_Command)
	v := ext.(*internal.CreateNodeCommand)

	// Copy data and update.
	other := fsm.data.Clone()

	// CreateNode is a command from < 0.10.0 clusters. Every node in
	// those clusters would be a data node and only the nodes that are
	// in the list of peers would be meta nodes
	isMeta := false
	for _, p := range peers {
		if v.GetHost() == p {
			isMeta = true
			break
		}
	}

	if isMeta {
		if err := other.CreateMetaNode(v.GetHost(), v.GetHost()); err != nil {
			return err
		}
	}

	// Get the only meta node
	if len(other.MetaNodes) == 1 {
		metaNode := other.MetaNodes[0]

		if err := other.setDataNode(metaNode.ID, v.GetHost(), v.GetHost()); err != nil {
			return err
		}
	} else {
		if err := other.CreateDataNode(v.GetHost(), v.GetHost()); err != nil {
			return err
		}
	}

	// If the cluster ID hasn't been set then use the command's random number.
	if other.ClusterID == 0 {
		other.ClusterID = uint64(v.GetRand())
	}

	fsm.data = other
	return nil
}

// applyUpdateNodeCommand was in < 0.10.0, noop this now
func (fsm *storeFSM) applyUpdateNodeCommand(cmd *internal.Command) interface{} {
	return nil
}

func (fsm *storeFSM) applyUpdateDataNodeCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_CreateNodeCommand_Command)
	v := ext.(*internal.UpdateDataNodeCommand)

	// Copy data and update.
	other := fsm.data.Clone()

	node := other.DataNode(v.GetID())
	if node == nil {
		return ErrNodeNotFound
	}

	node.Host = v.GetHost()
	node.TCPHost = v.GetTCPHost()

	fsm.data = other
	return nil
}

// applyDeleteNodeCommand is from < 0.10.0. no op for this one
func (fsm *storeFSM) applyDeleteNodeCommand(cmd *internal.Command) interface{} {
	return nil
}

func (fsm *storeFSM) applyCreateDatabaseCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_CreateDatabaseCommand_Command)
	v := ext.(*internal.CreateDatabaseCommand)

	// Copy data and update.
	other := fsm.data.Clone()
	if err := other.CreateDatabase(v.GetName()); err != nil {
		return err
	}

	s := (*store)(fsm)
	if ttli := v.GetTimeToLive(); ttli != nil {
		if err := other.CreateTimeToLive(v.GetName(), &TimeToLiveInfo{
			Name:           ttli.GetName(),
			ReplicaN:       int(ttli.GetReplicaN()),
			Duration:       time.Duration(ttli.GetDuration()),
			RegionDuration: time.Duration(ttli.GetRegionDuration()),
		}, true); err != nil {
			if err == ErrTimeToLiveExists {
				return ErrTimeToLiveConflict
			}
			return err
		}
	} else if s.config.TimeToLiveAutoCreate {
		// Read node count.
		// Time to live must be fully replicated.
		replicaN := len(other.DataNodes)
		if replicaN > maxAutoCreatedTimeToLiveReplicaN {
			replicaN = maxAutoCreatedTimeToLiveReplicaN
		} else if replicaN < 1 {
			replicaN = 1
		}

		// Create a time to live.
		ttli := NewTimeToLiveInfo(autoCreateTimeToLiveName)
		ttli.ReplicaN = replicaN
		ttli.Duration = autoCreateTimeToLivePeriod
		if err := other.CreateTimeToLive(v.GetName(), ttli, true); err != nil {
			return err
		}
	}

	fsm.data = other

	return nil
}

func (fsm *storeFSM) applyDropDatabaseCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_DropDatabaseCommand_Command)
	v := ext.(*internal.DropDatabaseCommand)

	// Copy data and update.
	other := fsm.data.Clone()
	if err := other.DropDatabase(v.GetName()); err != nil {
		return err
	}
	fsm.data = other

	return nil
}

func (fsm *storeFSM) applyCreateTimeToLiveCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_CreateTimeToLiveCommand_Command)
	v := ext.(*internal.CreateTimeToLiveCommand)
	pb := v.GetTimeToLive()

	// Copy data and update.
	other := fsm.data.Clone()
	if err := other.CreateTimeToLive(v.GetDatabase(),
		&TimeToLiveInfo{
			Name:           pb.GetName(),
			ReplicaN:       int(pb.GetReplicaN()),
			Duration:       time.Duration(pb.GetDuration()),
			RegionDuration: time.Duration(pb.GetRegionDuration()),
		}, false); err != nil {
		return err
	}
	fsm.data = other

	return nil
}

func (fsm *storeFSM) applyDropTimeToLiveCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_DropTimeToLiveCommand_Command)
	v := ext.(*internal.DropTimeToLiveCommand)

	// Copy data and update.
	other := fsm.data.Clone()
	if err := other.DropTimeToLive(v.GetDatabase(), v.GetName()); err != nil {
		return err
	}
	fsm.data = other

	return nil
}

func (fsm *storeFSM) applySetDefaultTimeToLiveCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_SetDefaultTimeToLiveCommand_Command)
	v := ext.(*internal.SetDefaultTimeToLiveCommand)

	// Copy data and update.
	other := fsm.data.Clone()
	if err := other.SetDefaultTimeToLive(v.GetDatabase(), v.GetName()); err != nil {
		return err
	}
	fsm.data = other

	return nil
}

func (fsm *storeFSM) applyUpdateTimeToLiveCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_UpdateTimeToLiveCommand_Command)
	v := ext.(*internal.UpdateTimeToLiveCommand)

	// Create update object.
	rpu := TimeToLiveUpdate{Name: v.NewName}
	if v.Duration != nil {
		value := time.Duration(v.GetDuration())
		rpu.Duration = &value
	}
	if v.ReplicaN != nil {
		value := int(v.GetReplicaN())
		rpu.ReplicaN = &value
	}

	// Copy data and update.
	other := fsm.data.Clone()
	if err := other.UpdateTimeToLive(v.GetDatabase(), v.GetName(), &rpu, false); err != nil {
		return err
	}
	fsm.data = other

	return nil
}

func (fsm *storeFSM) applyCreateRegionCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_CreateRegionCommand_Command)
	v := ext.(*internal.CreateRegionCommand)

	// Copy data and update.
	other := fsm.data.Clone()
	if err := other.CreateRegion(v.GetDatabase(), v.GetTimeToLive(), time.Unix(0, v.GetTimestamp())); err != nil {
		return err
	}
	fsm.data = other

	return nil
}

func (fsm *storeFSM) applyDeleteRegionCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_DeleteRegionCommand_Command)
	v := ext.(*internal.DeleteRegionCommand)

	// Copy data and update.
	other := fsm.data.Clone()
	if err := other.DeleteRegion(v.GetDatabase(), v.GetTimeToLive(), v.GetRegionID()); err != nil {
		return err
	}
	fsm.data = other

	return nil
}

func (fsm *storeFSM) applyCreateContinuousQueryCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_CreateContinuousQueryCommand_Command)
	v := ext.(*internal.CreateContinuousQueryCommand)

	// Copy data and update.
	other := fsm.data.Clone()
	if err := other.CreateContinuousQuery(v.GetDatabase(), v.GetName(), v.GetQuery()); err != nil {
		return err
	}
	fsm.data = other

	return nil
}

func (fsm *storeFSM) applyDropContinuousQueryCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_DropContinuousQueryCommand_Command)
	v := ext.(*internal.DropContinuousQueryCommand)

	// Copy data and update.
	other := fsm.data.Clone()
	if err := other.DropContinuousQuery(v.GetDatabase(), v.GetName()); err != nil {
		return err
	}
	fsm.data = other

	return nil
}

func (fsm *storeFSM) applyCreateSubscriptionCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_CreateSubscriptionCommand_Command)
	v := ext.(*internal.CreateSubscriptionCommand)

	// Copy data and update.
	other := fsm.data.Clone()
	if err := other.CreateSubscription(v.GetDatabase(), v.GetTimeToLive(), v.GetName(), v.GetMode(), v.GetDestinations()); err != nil {
		return err
	}
	fsm.data = other

	return nil
}

func (fsm *storeFSM) applyDropSubscriptionCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_DropSubscriptionCommand_Command)
	v := ext.(*internal.DropSubscriptionCommand)

	// Copy data and update.
	other := fsm.data.Clone()
	if err := other.DropSubscription(v.GetDatabase(), v.GetTimeToLive(), v.GetName()); err != nil {
		return err
	}
	fsm.data = other

	return nil
}

func (fsm *storeFSM) applyCreateUserCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_CreateUserCommand_Command)
	v := ext.(*internal.CreateUserCommand)

	// Copy data and update.
	other := fsm.data.Clone()
	if err := other.CreateUser(v.GetName(), v.GetHash(), v.GetAdmin()); err != nil {
		return err
	}
	fsm.data = other

	return nil
}

func (fsm *storeFSM) applyDropUserCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_DropUserCommand_Command)
	v := ext.(*internal.DropUserCommand)

	// Copy data and update.
	other := fsm.data.Clone()
	if err := other.DropUser(v.GetName()); err != nil {
		return err
	}
	fsm.data = other
	return nil
}

func (fsm *storeFSM) applyUpdateUserCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_UpdateUserCommand_Command)
	v := ext.(*internal.UpdateUserCommand)

	// Copy data and update.
	other := fsm.data.Clone()
	if err := other.UpdateUser(v.GetName(), v.GetHash()); err != nil {
		return err
	}
	fsm.data = other
	return nil
}

func (fsm *storeFSM) applySetPrivilegeCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_SetPrivilegeCommand_Command)
	v := ext.(*internal.SetPrivilegeCommand)

	// Copy data and update.
	other := fsm.data.Clone()
	if err := other.SetPrivilege(v.GetUsername(), v.GetDatabase(), cnosql.Privilege(v.GetPrivilege())); err != nil {
		return err
	}
	fsm.data = other
	return nil
}

func (fsm *storeFSM) applySetAdminPrivilegeCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_SetAdminPrivilegeCommand_Command)
	v := ext.(*internal.SetAdminPrivilegeCommand)

	// Copy data and update.
	other := fsm.data.Clone()
	if err := other.SetAdminPrivilege(v.GetUsername(), v.GetAdmin()); err != nil {
		return err
	}
	fsm.data = other
	return nil
}

func (fsm *storeFSM) applySetDataCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_SetDataCommand_Command)
	v := ext.(*internal.SetDataCommand)

	// Overwrite data.
	fsm.data = &Data{}
	fsm.data.unmarshal(v.GetData())

	return nil
}

func (fsm *storeFSM) applyCreateMetaNodeCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_CreateMetaNodeCommand_Command)
	v := ext.(*internal.CreateMetaNodeCommand)

	other := fsm.data.Clone()
	other.CreateMetaNode(v.GetHTTPAddr(), v.GetTCPAddr())

	// If the cluster ID hasn't been set then use the command's random number.
	if other.ClusterID == 0 {
		other.ClusterID = uint64(v.GetRand())
	}

	fsm.data = other
	return nil
}

func (fsm *storeFSM) applySetMetaNodeCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_SetMetaNodeCommand_Command)
	v := ext.(*internal.SetMetaNodeCommand)

	other := fsm.data.Clone()
	other.SetMetaNode(v.GetHTTPAddr(), v.GetTCPAddr())

	// If the cluster ID hasn't been set then use the command's random number.
	if other.ClusterID == 0 {
		other.ClusterID = uint64(v.GetRand())
	}

	fsm.data = other
	return nil
}

func (fsm *storeFSM) applyDeleteMetaNodeCommand(cmd *internal.Command, s *store) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_DeleteMetaNodeCommand_Command)
	v := ext.(*internal.DeleteMetaNodeCommand)

	other := fsm.data.Clone()
	node := other.MetaNode(v.GetID())
	if node == nil {
		return ErrNodeNotFound
	}

	if err := s.leave(node); err != nil && err != raft.ErrNotLeader {
		return err
	}

	if err := other.DeleteMetaNode(v.GetID()); err != nil {
		return err
	}
	fsm.data = other
	return nil
}

func (fsm *storeFSM) applyCreateDataNodeCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_CreateDataNodeCommand_Command)
	v := ext.(*internal.CreateDataNodeCommand)

	other := fsm.data.Clone()

	// Get the only meta node
	if len(other.MetaNodes) == 1 && len(other.DataNodes) == 0 {
		metaNode := other.MetaNodes[0]

		if err := other.setDataNode(metaNode.ID, v.GetHTTPAddr(), v.GetTCPAddr()); err != nil {
			return err
		}
	} else {
		other.CreateDataNode(v.GetHTTPAddr(), v.GetTCPAddr())
	}
	fsm.data = other
	return nil
}

func (fsm *storeFSM) applyDeleteDataNodeCommand(cmd *internal.Command) interface{} {
	ext, _ := proto.GetExtension(cmd, internal.E_DeleteDataNodeCommand_Command)
	v := ext.(*internal.DeleteDataNodeCommand)

	other := fsm.data.Clone()
	if err := other.DeleteDataNode(v.GetID()); err != nil {
		return err
	}
	fsm.data = other
	return nil
}

func (fsm *storeFSM) Snapshot() (raft.FSMSnapshot, error) {
	s := (*store)(fsm)
	s.mu.Lock()
	defer s.mu.Unlock()

	return &storeFSMSnapshot{Data: (*store)(fsm).data}, nil
}

func (fsm *storeFSM) Restore(r io.ReadCloser) error {
	// Read all bytes.
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	// Decode metadata.
	data := &Data{}
	if err := data.UnmarshalBinary(b); err != nil {
		return err
	}

	// Set metadata on store.
	// NOTE: No lock because Hashicorp Raft doesn't call Restore concurrently
	// with any other function.
	fsm.data = data

	return nil
}

type storeFSMSnapshot struct {
	Data *Data
}

func (s *storeFSMSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode data.
		p, err := s.Data.MarshalBinary()
		if err != nil {
			return err
		}

		// Write data to sink.
		if _, err := sink.Write(p); err != nil {
			return err
		}

		// Close the sink.
		if err := sink.Close(); err != nil {
			return err
		}

		return nil
	}()

	if err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

// Release is invoked when we are finished with the snapshot
func (s *storeFSMSnapshot) Release() {}
