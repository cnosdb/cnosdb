use async_trait::async_trait;
use models::schema::query_info::QueryInfo;
use spi::query::datasource::stream::checker::StreamCheckerManagerRef;
use spi::query::dispatcher::QueryStatus;
use spi::query::execution::{Output, QueryExecution, QueryStateMachineRef};
use spi::query::logical_planner::DDLPlan;
use spi::QueryResult;

use self::alter_tenant::AlterTenantTask;
use self::alter_user::AlterUserTask;
use self::create_external_table::CreateExternalTableTask;
use self::create_role::CreateRoleTask;
use self::create_stream_table::CreateStreamTableTask;
use self::create_table::CreateTableTask;
use self::create_tenant::CreateTenantTask;
use self::create_user::CreateUserTask;
use self::drop_database_object::DropDatabaseObjectTask;
use self::drop_global_object::DropGlobalObjectTask;
use self::drop_tenant_object::DropTenantObjectTask;
use self::grant_revoke::GrantRevokeTask;
use self::recover_database::RecoverDatabaseTask;
use self::recover_tenant::RecoverTenantTask;
use self::replica_add::ReplicaAddTask;
use self::replica_destroy::ReplicaDestroyTask;
use self::replica_promote::ReplicaPromoteTask;
use self::replica_remove::ReplicaRemoveTask;
use self::show_replica::ShowReplicasTask;
use crate::execution::ddl::alter_database::AlterDatabaseTask;
use crate::execution::ddl::alter_table::AlterTableTask;
use crate::execution::ddl::checksum_group::ChecksumGroupTask;
use crate::execution::ddl::compact_vnode::CompactVnodeTask;
use crate::execution::ddl::copy_vnode::CopyVnodeTask;
use crate::execution::ddl::create_database::CreateDatabaseTask;
use crate::execution::ddl::drop_vnode::DropVnodeTask;
use crate::execution::ddl::move_node::MoveVnodeTask;

mod alter_database;
mod alter_table;
mod alter_tenant;
mod alter_user;
mod checksum_group;
mod compact_vnode;
mod copy_vnode;
mod create_database;
mod create_external_table;
mod create_role;
mod create_stream_table;
mod create_table;
mod create_tenant;
mod create_user;
mod drop_database_object;
mod drop_global_object;
mod drop_tenant_object;
mod drop_vnode;
mod grant_revoke;
mod move_node;
mod recover_database;
mod recover_tenant;
mod replica_add;
mod replica_destroy;
mod replica_promote;
mod replica_remove;
mod show_replica;

/// Traits that DDL tasks should implement
#[async_trait]
trait DDLDefinitionTask: Send + Sync {
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> QueryResult<Output>;
}

pub struct DDLExecution {
    task_factory: DDLDefinitionTaskFactory,
    query_state_machine: QueryStateMachineRef,
}

impl DDLExecution {
    pub fn new(
        query_state_machine: QueryStateMachineRef,
        stream_checker_manager: StreamCheckerManagerRef,
        plan: DDLPlan,
    ) -> Self {
        Self {
            task_factory: DDLDefinitionTaskFactory {
                stream_checker_manager,
                plan,
            },
            query_state_machine,
        }
    }
}

#[async_trait]
impl QueryExecution for DDLExecution {
    // execute ddl task
    // This logic usually does not change
    async fn start(&self) -> QueryResult<Output> {
        let query_state_machine = &self.query_state_machine;

        query_state_machine.begin_schedule();

        let _span_recorder = self
            .query_state_machine
            .session
            .get_child_span("execute ddl");

        let result = self
            .task_factory
            .create_task()
            .execute(query_state_machine.clone())
            .await;

        query_state_machine.end_schedule();

        result
    }

    fn cancel(&self) -> QueryResult<()> {
        // ddl ignore
        Ok(())
    }

    fn info(&self) -> QueryInfo {
        let qsm = &self.query_state_machine;
        QueryInfo::new(
            qsm.query_id,
            qsm.query.content().to_string(),
            *qsm.session.tenant_id(),
            qsm.session.tenant().to_string(),
            qsm.session.default_database().to_string(),
            qsm.session.user().clone(),
            qsm.coord.node_id(),
        )
    }

    fn status(&self) -> QueryStatus {
        QueryStatus::new(
            self.query_state_machine.state().clone(),
            self.query_state_machine.duration(),
        )
    }
}

struct DDLDefinitionTaskFactory {
    stream_checker_manager: StreamCheckerManagerRef,
    plan: DDLPlan,
}

impl DDLDefinitionTaskFactory {
    // According to different statement types, construct the corresponding task
    // If you add ddl operations, you usually need to modify here
    fn create_task(&self) -> Box<dyn DDLDefinitionTask> {
        match &self.plan {
            DDLPlan::CreateExternalTable(sub_plan) => {
                Box::new(CreateExternalTableTask::new(sub_plan.clone()))
            }
            DDLPlan::DropDatabaseObject(sub_plan) => {
                Box::new(DropDatabaseObjectTask::new(sub_plan.clone()))
            }
            DDLPlan::DropTenantObject(sub_plan) => {
                Box::new(DropTenantObjectTask::new(sub_plan.clone()))
            }
            DDLPlan::DropGlobalObject(sub_plan) => {
                Box::new(DropGlobalObjectTask::new(sub_plan.clone()))
            }
            DDLPlan::CreateTable(sub_plan) => Box::new(CreateTableTask::new(sub_plan.clone())),
            DDLPlan::CreateDatabase(sub_plan) => {
                Box::new(CreateDatabaseTask::new(sub_plan.clone()))
            }
            DDLPlan::CreateTenant(sub_plan) => Box::new(CreateTenantTask::new(*sub_plan.clone())),
            DDLPlan::CreateUser(sub_plan) => Box::new(CreateUserTask::new(sub_plan.clone())),
            DDLPlan::CreateRole(sub_plan) => Box::new(CreateRoleTask::new(sub_plan.clone())),
            DDLPlan::AlterDatabase(sub_plan) => Box::new(AlterDatabaseTask::new(sub_plan.clone())),
            DDLPlan::AlterTable(sub_plan) => Box::new(AlterTableTask::new(sub_plan.clone())),
            DDLPlan::AlterTenant(sub_plan) => Box::new(AlterTenantTask::new(sub_plan.clone())),
            DDLPlan::AlterUser(sub_plan) => Box::new(AlterUserTask::new(sub_plan.clone())),
            DDLPlan::GrantRevoke(sub_plan) => Box::new(GrantRevokeTask::new(sub_plan.clone())),
            DDLPlan::DropVnode(sub_plan) => Box::new(DropVnodeTask::new(sub_plan.clone())),
            DDLPlan::CopyVnode(sub_plan) => Box::new(CopyVnodeTask::new(sub_plan.clone())),
            DDLPlan::MoveVnode(sub_plan) => Box::new(MoveVnodeTask::new(sub_plan.clone())),
            DDLPlan::CompactVnode(sub_plan) => Box::new(CompactVnodeTask::new(sub_plan.clone())),
            DDLPlan::ChecksumGroup(sub_plan) => {
                Box::new(ChecksumGroupTask::new(sub_plan.clone(), self.plan.schema()))
            }
            DDLPlan::CreateStreamTable(sub_plan) => {
                let checker = self.stream_checker_manager.checker(&sub_plan.stream_type);

                Box::new(CreateStreamTableTask::new(checker, sub_plan.clone()))
            }
            DDLPlan::RecoverDatabase(sub_plan) => {
                Box::new(RecoverDatabaseTask::new(sub_plan.clone()))
            }
            DDLPlan::RecoverTenant(sub_plan) => Box::new(RecoverTenantTask::new(sub_plan.clone())),
            DDLPlan::ShowReplicas => Box::new(ShowReplicasTask::new()),
            DDLPlan::ReplicaDestroy(sub_plan) => {
                Box::new(ReplicaDestroyTask::new(sub_plan.clone()))
            }
            DDLPlan::ReplicaAdd(sub_plan) => Box::new(ReplicaAddTask::new(sub_plan.clone())),
            DDLPlan::ReplicaRemove(sub_plan) => Box::new(ReplicaRemoveTask::new(sub_plan.clone())),
            DDLPlan::ReplicaPromote(sub_plan) => {
                Box::new(ReplicaPromoteTask::new(sub_plan.clone()))
            }
        }
    }
}
