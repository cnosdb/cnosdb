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
use tracing::log::info;

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
        fields: Vec<FieldId>,
    ) -> HashMap<SeriesId, HashMap<FieldId, Vec<DataBlock>>>;

    fn drop_database(&self, database: &str) -> Result<()>;

    fn drop_table(&self, database: &str, table: &str) -> Result<()>;

    fn delete_series(
        &self,
        db: &String,
        sids: &[SeriesId],
        field_ids: &[FieldId],
        time_range: &TimeRange,
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
        fields: Vec<FieldId>,
    ) -> HashMap<SeriesId, HashMap<FieldId, Vec<DataBlock>>> {
        todo!()
    }

    fn drop_database(&self, database: &str) -> Result<()> {
        println!("drop_database {:?}", database);
        Ok(())
    }

    fn drop_table(&self, database: &str, table: &str) -> Result<()> {
        println!("drop_table db:{:?}, table:{:?}", database, table);
        Ok(())
    }

    fn delete_series(
        &self,
        db: &String,
        sids: &[SeriesId],
        field_ids: &[FieldId],
        time_range: &TimeRange,
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
