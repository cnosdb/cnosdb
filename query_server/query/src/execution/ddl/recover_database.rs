use async_trait::async_trait;
use meta::error::MetaError;
use models::oid::Identifier;
use models::schema::resource_info::{ResourceInfo, ResourceOperator, ResourceStatus};
use snafu::ResultExt;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::RecoverDatabase;
use spi::{MetaSnafu, QueryError, QueryResult};

use super::DDLDefinitionTask;

pub struct RecoverDatabaseTask {
    stmt: RecoverDatabase,
}

impl RecoverDatabaseTask {
    #[inline(always)]
    pub fn new(stmt: RecoverDatabase) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for RecoverDatabaseTask {
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> QueryResult<Output> {
        let RecoverDatabase {
            ref tenant_name,
            ref db_name,
            ref if_exist,
        } = self.stmt;

        let meta = query_state_machine
            .meta
            .tenant_meta(tenant_name)
            .await
            .ok_or_else(|| QueryError::Meta {
                source: MetaError::TenantNotFound {
                    tenant: tenant_name.to_string(),
                },
            })?;

        // first, cancel drop task
        let mut resourceinfo = ResourceInfo::new(
            (*meta.tenant().id(), db_name.to_string()),
            tenant_name.clone() + "-" + db_name,
            ResourceOperator::DropDatabase(tenant_name.clone(), db_name.clone()),
            &None,
            query_state_machine.coord.node_id(),
        );
        resourceinfo.set_status(ResourceStatus::Cancel);
        resourceinfo.set_comment("");

        query_state_machine
            .meta
            .write_resourceinfo(resourceinfo.get_name(), resourceinfo.clone())
            .await
            .context(MetaSnafu)?;

        // second, set hidden to FALSE
        if (meta.set_db_is_hidden(tenant_name, db_name, false).await).is_err() && !if_exist {
            return Err(QueryError::Meta {
                source: MetaError::DatabaseNotFound {
                    database: db_name.to_string(),
                },
            });
        }

        Ok(Output::Nil(()))
    }
}
