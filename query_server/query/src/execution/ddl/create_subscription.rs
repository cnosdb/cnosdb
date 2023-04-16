use async_trait::async_trait;
use meta::error::MetaError;
use models::meta_data::SubscriptionInfo;
use spi::query::execution::{
    // ExecutionError,
    Output,
    QueryStateMachineRef,
};
use spi::query::logical_planner::CreateSubscription;
use spi::{QueryError, Result};

use crate::execution::ddl::DDLDefinitionTask;

pub struct CreateSubscriptionTask {
    stmt: CreateSubscription,
}

impl CreateSubscriptionTask {
    pub fn new(stmt: CreateSubscription) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for CreateSubscriptionTask {
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
        let CreateSubscription {
            ref subs_name,
            ref db_name,
            ref level,
            ref addrs,
        } = self.stmt;

        // todo : remove table name and add expr for filter subscription data
        let subs_info = SubscriptionInfo {
            consistency_level: *level as u8,
            name: subs_name.clone(),
            _table_name: "".to_string(),
            destinations: addrs.clone(),
        };

        meta.create_subscription(db_name, &subs_info).await?;
        Ok(Output::Nil(()))
    }
}
