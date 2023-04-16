use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use meta::error::MetaError;
use models::consistency_level::ConsistencyLevel;
// use spi::query::execution::spi::DatafusionSnafu;
// use spi::query::spi::MetaSnafu;
use spi::query::execution::{
    // ExecutionError,
    Output,
    QueryStateMachineRef,
};
use spi::query::logical_planner::ShowSubscription;
use spi::Result;

use crate::execution::ddl::DDLDefinitionTask;

pub struct ShowSubscriptionTask {
    stmt: ShowSubscription,
}

impl ShowSubscriptionTask {
    pub fn new(stmt: ShowSubscription) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for ShowSubscriptionTask {
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> Result<Output> {
        let tenant = query_state_machine.session.tenant();
        let client = query_state_machine
            .meta
            .tenant_manager()
            .tenant_meta(tenant)
            .await
            .ok_or(MetaError::TenantNotFound {
                tenant: tenant.to_string(),
            })?;

        let tenant_data = client.get_data();
        let db_info =
            tenant_data
                .dbs
                .get(&self.stmt.db_name)
                .ok_or(MetaError::DatabaseNotFound {
                    database: self.stmt.db_name.clone(),
                })?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("Subscription", DataType::Utf8, false),
            Field::new("DESTINATIONS", DataType::Utf8, false),
            Field::new("Concurrency", DataType::Utf8, false),
        ]));

        let data = db_info.subs.iter().map(|(name, info)| {
            (
                name.clone(),
                info.destinations.join(","),
                (ConsistencyLevel::from(info.consistency_level)).to_string(),
            )
        });

        let mut subscriptions = Vec::with_capacity(data.len());
        let mut destinations = Vec::with_capacity(data.len());
        let mut concurrency = Vec::with_capacity(data.len());

        data.for_each(|datum| {
            subscriptions.push(datum.0);
            destinations.push(datum.1);
            concurrency.push(datum.2);
        });

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(subscriptions)),
                Arc::new(StringArray::from(destinations)),
                Arc::new(StringArray::from(concurrency)),
            ],
        )?;
        Ok(Output::StreamData(schema, vec![batch]))
    }
}
