#![allow(dead_code, unused_variables)]

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use models::meta_data::VnodeId;
use models::predicate::domain::{ColumnDomains, TimeRange};
use models::schema::{Precision, TableColumn};
use models::{ColumnId, SeriesId, SeriesKey};
use protos::kv_service::{WritePointsRequest, WritePointsResponse};
use protos::models as fb_models;
use trace::{debug, SpanContext};

use crate::error::Result;
use crate::kv_option::StorageOptions;
use crate::summary::VersionEdit;
use crate::tseries_family::SuperVersion;
use crate::{Engine, TseriesFamilyId};

#[derive(Debug, Default)]
pub struct MockEngine {}

#[async_trait]
impl Engine for MockEngine {
    async fn write(
        &self,
        _span_ctx: Option<&SpanContext>,
        id: u32,
        precision: Precision,
        write_batch: WritePointsRequest,
    ) -> Result<WritePointsResponse> {
        debug!("writing point");
        let points = Arc::new(write_batch.points);
        let fb_points = flatbuffers::root::<fb_models::Points>(&points).unwrap();

        debug!("writed point: {:?}", fb_points);

        Ok(WritePointsResponse { points_number: 0 })
    }

    async fn write_from_wal(
        &self,
        id: u32,
        precision: Precision,
        write_batch: WritePointsRequest,
        seq: u64,
    ) -> Result<()> {
        debug!("write point");
        Ok(())
    }

    async fn remove_tsfamily(&self, tenant: &str, database: &str, id: u32) -> Result<()> {
        Ok(())
    }

    async fn remove_tsfamily_from_wal(
        &self,
        tenant: &str,
        database: &str,
        vnode_id: VnodeId,
    ) -> Result<()> {
        Ok(())
    }

    async fn flush_tsfamily(&self, tenant: &str, database: &str, id: u32) -> Result<()> {
        Ok(())
    }

    async fn drop_database(&self, tenant: &str, database: &str) -> Result<()> {
        println!("drop_database.sql {:?}", database);
        Ok(())
    }

    async fn drop_table(&self, tenant: &str, database: &str, table: &str) -> Result<()> {
        println!("drop_table db:{:?}, table:{:?}", database, table);
        Ok(())
    }

    async fn drop_table_from_wal(&self, tenant: &str, database: &str, table: &str) -> Result<()> {
        Ok(())
    }

    // fn create_table(&self, schema: &TskvTableSchema) -> Result<()> {
    //     todo!()
    // }

    // fn create_database(&self, schema: &DatabaseSchema) -> Result<Arc<RwLock<Database>>> {
    //     todo!()
    // }

    // fn list_databases(&self) -> Result<Vec<String>> {
    //     todo!()
    // }

    // fn list_tables(&self, tenant: &str, database: &str) -> Result<Vec<String>> {
    //     todo!()
    // }

    // fn get_db_schema(&self, tenant: &str, name: &str) -> Result<Option<DatabaseSchema>> {
    //     Ok(Some(DatabaseSchema::new(tenant, name)))
    // }

    async fn delete_series(
        &self,
        tenant: &str,
        database: &str,
        series_ids: &[SeriesId],
        field_ids: &[ColumnId],
        time_range: &TimeRange,
    ) -> Result<()> {
        todo!()
    }

    async fn get_series_id_by_filter(
        &self,
        tenant: &str,
        db: &str,
        tab: &str,
        id: SeriesId,
        filter: &ColumnDomains<String>,
    ) -> Result<Vec<SeriesId>> {
        Ok(vec![])
    }

    async fn get_series_key(
        &self,
        tenant: &str,
        db: &str,
        vnode_id: u32,
        sid: u32,
    ) -> Result<Option<SeriesKey>> {
        Ok(None)
    }

    async fn get_db_version(
        &self,
        tenant: &str,
        db: &str,
        vnode_id: u32,
    ) -> Result<Option<Arc<SuperVersion>>> {
        todo!()
    }

    async fn get_vnode_summary(
        &self,
        tenant: &str,
        database: &str,
        vnode_id: u32,
    ) -> Result<Option<VersionEdit>> {
        todo!()
    }

    async fn apply_vnode_summary(
        &self,
        tenant: &str,
        database: &str,
        vnode_id: u32,
        summary: VersionEdit,
    ) -> Result<()> {
        todo!()
    }

    // fn alter_database(&self, schema: &DatabaseSchema) -> Result<()> {
    //     todo!()
    // }

    async fn add_table_column(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        column: TableColumn,
    ) -> Result<()> {
        todo!()
    }

    async fn drop_table_column(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        column: &str,
    ) -> Result<()> {
        todo!()
    }

    async fn change_table_column(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        column_name: &str,
        new_column: TableColumn,
    ) -> Result<()> {
        todo!()
    }

    fn get_storage_options(&self) -> Arc<StorageOptions> {
        todo!()
    }

    async fn drop_vnode(&self, id: TseriesFamilyId) -> Result<()> {
        todo!()
    }

    async fn compact(&self, vnode_ids: Vec<TseriesFamilyId>) -> Result<()> {
        todo!()
    }

    async fn get_vnode_hash_tree(&self, vnode_ids: VnodeId) -> Result<RecordBatch> {
        todo!()
    }

    async fn close(&self) {}

    async fn prepare_copy_vnode(&self, tenant: &str, database: &str, vnode_id: u32) -> Result<()> {
        todo!()
    }
}
