use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{StringArray, UInt32Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use meta::error::MetaError;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::recordbatch::RecordBatchStreamWrapper;
use spi::Result;

use crate::execution::ddl::DDLDefinitionTask;

pub struct ShowReplicasTask {}

impl ShowReplicasTask {
    pub fn new() -> Self {
        ShowReplicasTask {}
    }
}

#[async_trait]
impl DDLDefinitionTask for ShowReplicasTask {
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> Result<Output> {
        show_replica(query_state_machine).await
    }
}

async fn show_replica(machine: QueryStateMachineRef) -> Result<Output> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("replica_id", DataType::UInt32, false),
        Field::new("location", DataType::Utf8, false),
        Field::new("database", DataType::Utf8, false),
        Field::new("start_time", DataType::Utf8, false),
        Field::new("end_time", DataType::Utf8, false),
    ]));

    let mut location_list = Vec::new();
    let mut replica_id_list = Vec::new();
    let mut database_list = Vec::new();
    let mut start_time_list = Vec::new();
    let mut end_time_list = Vec::new();

    let tenant = machine.session.tenant();
    let client = machine
        .meta
        .tenant_meta(tenant)
        .await
        .ok_or(MetaError::TenantNotFound {
            tenant: tenant.to_string(),
        })?;

    let databases = client.list_databases()?;

    for (db_name, db_info) in databases {
        for bucket in db_info.buckets {
            for replica in bucket.shard_group {
                replica_id_list.push(replica.id);
                database_list.push(db_name.clone());

                let start_time_nanos = models::schema::timestamp_convert(
                    *db_info.schema.config.precision_or_default(),
                    models::schema::Precision::NS,
                    bucket.start_time,
                )
                .unwrap_or_default();
                start_time_list.push(timestamp_to_string(start_time_nanos));
                let end_time_nanos = models::schema::timestamp_convert(
                    *db_info.schema.config.precision_or_default(),
                    models::schema::Precision::NS,
                    bucket.end_time,
                )
                .unwrap_or_default();
                end_time_list.push(timestamp_to_string(end_time_nanos));

                let mut temp_locations = Vec::new();
                for vnode in replica.vnodes {
                    let mut temp = format!("{:?}", vnode.node_id);
                    if replica.leader_vnode_id == vnode.id {
                        temp = format!("{:?}*", vnode.node_id);
                    }
                    temp_locations.push(temp);
                }
                location_list.push(temp_locations.join(","));
            }
        }
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt32Array::from(replica_id_list)),
            Arc::new(StringArray::from(location_list)),
            Arc::new(StringArray::from(database_list)),
            Arc::new(StringArray::from(start_time_list)),
            Arc::new(StringArray::from(end_time_list)),
        ],
    )?;

    Ok(Output::StreamData(Box::pin(RecordBatchStreamWrapper::new(
        schema,
        vec![batch],
    ))))
}

fn timestamp_to_string(nanos: i64) -> String {
    if let Some(datetime) = chrono::NaiveDateTime::from_timestamp_nanos(nanos) {
        let utc_datetime = datetime.and_utc();

        format!("{}", utc_datetime)
    } else {
        nanos.to_string()
    }
}
