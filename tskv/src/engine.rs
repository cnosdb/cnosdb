use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use async_trait::async_trait;
use models::{FieldId, SeriesId, Timestamp};
use protos::kv_service::{WritePointsRpcRequest, WritePointsRpcResponse};
use crate::{Options, TimeRange, TsKv};
use crate::error::Result;
use crate::tsm::DataBlock;


pub type EngineRef = Arc<dyn Engine>;

#[async_trait]
pub trait Engine: Send + Sync + Debug {
    async fn write(&self, write_batch: WritePointsRpcRequest) -> Result<WritePointsRpcResponse>;
    fn read(&self, sids: Vec<SeriesId>, time_range: &TimeRange, fields: Vec<FieldId>)-> HashMap<SeriesId, HashMap<FieldId, Vec<DataBlock>>> ;
    async fn delete_series(&self, sids: Vec<SeriesId>, min: Timestamp, max: Timestamp) -> Result<()>;
}