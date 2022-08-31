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

    async fn write_from_wal(
        &self,
        write_batch: WritePointsRpcRequest,
        seq: u64,
    ) -> Result<WritePointsRpcResponse>;

    fn read(
        &self,
        db: &String,
        sids: Vec<SeriesId>,
        time_range: &TimeRange,
        fields: Vec<u32>,
    ) -> HashMap<SeriesId, HashMap<u32, Vec<DataBlock>>>;

    async fn delete_series(
        &self,
        db: &String,
        sids: Vec<SeriesId>,
        min: Timestamp,
        max: Timestamp,
    ) -> Result<()>;

    fn get_table_schema(&self, db: &String, tab: &String) -> Result<Option<Vec<FieldInfo>>>;

    async fn get_series_id_list(
        &self,
        db: &String,
        tab: &String,
        tags: &Vec<Tag>,
    ) -> IndexResult<Vec<u64>>;
    fn get_series_key(&self, db: &String, sid: u64) -> IndexResult<Option<SeriesKey>>;
}

#[derive(Debug, Default)]
pub struct MockEngine {}

#[async_trait]
impl Engine for MockEngine {
    async fn write(&self, write_batch: WritePointsRpcRequest) -> Result<WritePointsRpcResponse> {
        todo!()
    }

    async fn write_from_wal(
        &self,
        write_batch: WritePointsRpcRequest,
        seq: u64,
    ) -> Result<WritePointsRpcResponse> {
        todo!()
    }

    fn read(
        &self,
        db: &String,
        sids: Vec<SeriesId>,
        time_range: &TimeRange,
        fields: Vec<u32>,
    ) -> HashMap<SeriesId, HashMap<u32, Vec<DataBlock>>> {
        todo!()
    }

    async fn delete_series(
        &self,
        db: &String,
        sids: Vec<SeriesId>,
        min: Timestamp,
        max: Timestamp,
    ) -> Result<()> {
        todo!()
    }

    fn get_table_schema(&self, db: &String, tab: &String) -> Result<Option<Vec<FieldInfo>>> {
        todo!()
    }

    async fn get_series_id_list(
        &self,
        db: &String,
        tab: &String,
        tags: &Vec<Tag>,
    ) -> IndexResult<Vec<u64>> {
        todo!()
    }

    fn get_series_key(&self, db: &String, sid: u64) -> IndexResult<Option<SeriesKey>> {
        todo!()
    }
}
