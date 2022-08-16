use crate::error::Result;
use crate::index::IndexResult;
use crate::tsm::DataBlock;
use crate::{Options, TimeRange, TsKv};
use async_trait::async_trait;
use models::{FieldId, FieldInfo, SeriesId, SeriesKey, Tag, Timestamp};
use protos::kv_service::{WritePointsRpcRequest, WritePointsRpcResponse};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

pub type EngineRef = Arc<dyn Engine>;

#[async_trait]
pub trait Engine: Send + Sync + Debug {
    async fn write(&self, write_batch: WritePointsRpcRequest) -> Result<WritePointsRpcResponse>;
    fn read(
        &self,
        sids: Vec<SeriesId>,
        time_range: &TimeRange,
        fields: Vec<FieldId>,
    ) -> HashMap<SeriesId, HashMap<FieldId, Vec<DataBlock>>>;
    async fn delete_series(
        &self,
        sids: Vec<SeriesId>,
        min: Timestamp,
        max: Timestamp,
    ) -> Result<()>;
    fn get_table_schema(&self, tab: &String) -> Result<Option<Vec<FieldInfo>>>;
    async fn get_series_id_list(&self, tab: &String, tags: &Vec<Tag>) -> IndexResult<Vec<u64>>;
    fn get_series_key(&self, sid: u64) -> IndexResult<Option<SeriesKey>>;
}
