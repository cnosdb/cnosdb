use async_trait::async_trait;
use meta::error::MetaError;
use snafu::ResultExt;
use spi::query::execution::{
    // ExecutionError,
    Output,
    QueryStateMachineRef,
};
use spi::query::logical_planner::DropSubscription;
use spi::{MetaSnafu, QueryError, Result};

use crate::execution::ddl::DDLDefinitionTask;

pub struct DropSubscriptionTask {
    stmt: DropSubscription,
}

impl DropSubscriptionTask {
    pub fn new(stmt: DropSubscription) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for DropSubscriptionTask {
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> Result<Output> {
        let tenant_manager = query_state_machine.meta.tenant_manager();

        let tenant_name = query_state_machine.session.tenant();
        let meta = tenant_manager
            .tenant_meta(tenant_name)
            .await
            .ok_or_else(|| QueryError::Meta {
                source: MetaError::TenantNotFound {
                    tenant: tenant_name.to_string(),
                },
            })?;

        let DropSubscription {
            if_exist,
            ref subs_name,
            ref db_name,
        } = self.stmt;

        let res = meta.drop_subscription(db_name, subs_name).await;

        if if_exist {
            return Ok(Output::Nil(()));
        }

        res.map(|_| Output::Nil(())).context(MetaSnafu)
    }
}
