use crate::error::Result;
use crate::index::IndexResult;
use crate::tseries_family::SuperVersion;
use crate::tsm::DataBlock;
use crate::{Options, TimeRange, TsKv};
use async_trait::async_trait;
use datafusion::prelude::Column;
use models::codec::Encoding;
use models::predicate::domain::{ColumnDomains, PredicateRef};
use models::schema::{DatabaseSchema, TableColumn, TableSchema, TskvTableSchema};
use models::{ColumnId, FieldId, FieldInfo, SeriesId, SeriesKey, Tag, Timestamp, ValueType};
use protos::{
    kv_service::{WritePointsRpcRequest, WritePointsRpcResponse, WriteRowsRpcRequest},
    models as fb_models,
};
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::sync::Arc;
use trace::{debug, info};

pub type EngineRef = Arc<dyn Engine>;

#[async_trait]
pub trait Engine: Send + Sync + Debug {
    async fn write(&self, write_batch: WritePointsRpcRequest) -> Result<WritePointsRpcResponse>;

    async fn write_from_wal(
        &self,
        write_batch: WritePointsRpcRequest,
        seq: u64,
    ) -> Result<WritePointsRpcResponse>;

    fn create_database(&self, schema: &DatabaseSchema) -> Result<()>;

    fn alter_database(&self, schema: &DatabaseSchema) -> Result<()>;

    fn get_db_schema(&self, name: &str) -> Option<DatabaseSchema>;

    fn drop_database(&self, database: &str) -> Result<()>;

    fn create_table(&self, schema: &TableSchema) -> Result<()>;

    fn drop_table(&self, database: &str, table: &str) -> Result<()>;

    fn list_databases(&self) -> Result<Vec<String>>;

    fn list_tables(&self, database: &str) -> Result<Vec<String>>;

    fn add_table_column(&self, database: &str, table: &str, column: TableColumn) -> Result<()>;

    fn drop_table_column(&self, database: &str, table: &str, column: &str) -> Result<()>;

    fn change_table_column(
        &self,
        database: &str,
        table: &str,
        column_name: &str,
        new_column: &TableColumn,
    ) -> Result<()>;

    fn delete_series(
        &self,
        db: &str,
        sids: &[SeriesId],
        field_ids: &[ColumnId],
        time_range: &TimeRange,
    ) -> Result<()>;

    fn get_table_schema(&self, db: &str, tab: &str) -> Result<Option<TableSchema>>;

    fn get_series_id_by_filter(
        &self,
        db: &str,
        tab: &str,
        filter: &ColumnDomains<String>,
    ) -> IndexResult<Vec<u64>>;
    fn get_series_id_list(&self, db: &str, tab: &str, tags: &[Tag]) -> IndexResult<Vec<u64>>;
    fn get_series_key(&self, db: &str, sid: SeriesId) -> IndexResult<Option<SeriesKey>>;
    fn get_db_version(&self, db: &str) -> Result<Option<Arc<SuperVersion>>>;
}

#[derive(Debug, Default)]
pub struct MockEngine {}

#[async_trait]
impl Engine for MockEngine {
    async fn write(&self, write_batch: WritePointsRpcRequest) -> Result<WritePointsRpcResponse> {
        debug!("writing point");
        let points = Arc::new(write_batch.points);
        let fb_points = flatbuffers::root::<fb_models::Points>(&points).unwrap();

        debug!("writed point: {:?}", fb_points);

        Ok(WritePointsRpcResponse {
            version: write_batch.version,
            points: vec![],
        })
    }

    async fn write_from_wal(
        &self,
        write_batch: WritePointsRpcRequest,
        seq: u64,
    ) -> Result<WritePointsRpcResponse> {
        debug!("write point");
        Ok(WritePointsRpcResponse {
            version: write_batch.version,
            points: vec![],
        })
    }

    fn drop_database(&self, database: &str) -> Result<()> {
        println!("drop_database.sql {:?}", database);
        Ok(())
    }

    fn create_table(&self, schema: &TableSchema) -> Result<()> {
        todo!()
    }

    fn create_database(&self, schema: &DatabaseSchema) -> Result<()> {
        Ok(())
    }

    fn list_databases(&self) -> Result<Vec<String>> {
        todo!()
    }

    fn list_tables(&self, database: &str) -> Result<Vec<String>> {
        todo!()
    }

    fn get_db_schema(&self, name: &str) -> Option<DatabaseSchema> {
        Some(DatabaseSchema::new(name))
    }

    fn drop_table(&self, database: &str, table: &str) -> Result<()> {
        println!("drop_table db:{:?}, table:{:?}", database, table);
        Ok(())
    }

    fn delete_series(
        &self,
        db: &str,
        sids: &[SeriesId],
        field_ids: &[ColumnId],
        time_range: &TimeRange,
    ) -> Result<()> {
        todo!()
    }

    fn get_table_schema(&self, db: &str, tab: &str) -> Result<Option<TableSchema>> {
        debug!("get_table_schema db:{:?}, table:{:?}", db, tab);
        Ok(Some(TableSchema::TsKvTableSchema(TskvTableSchema::new(
            db.to_string(),
            tab.to_string(),
            Default::default(),
        ))))
    }

    fn get_series_id_by_filter(
        &self,
        db: &str,
        tab: &str,
        filter: &ColumnDomains<String>,
    ) -> IndexResult<Vec<u64>> {
        Ok(vec![])
    }

    fn get_series_id_list(&self, db: &str, tab: &str, tags: &[Tag]) -> IndexResult<Vec<u64>> {
        Ok(vec![])
    }

    fn get_series_key(&self, db: &str, sid: u64) -> IndexResult<Option<SeriesKey>> {
        Ok(None)
    }

    fn get_db_version(&self, db: &str) -> Result<Option<Arc<SuperVersion>>> {
        todo!()
    }

    fn alter_database(&self, schema: &DatabaseSchema) -> Result<()> {
        todo!()
    }

    fn add_table_column(&self, database: &str, table: &str, column: TableColumn) -> Result<()> {
        todo!()
    }

    fn drop_table_column(&self, database: &str, table: &str, column: &str) -> Result<()> {
        todo!()
    }

    fn change_table_column(
        &self,
        database: &str,
        table: &str,
        column_name: &str,
        new_column: &TableColumn,
    ) -> Result<()> {
        todo!()
    }
}
