#![allow(dead_code, unused_variables)]

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use models::meta_data::VnodeId;
use models::predicate::domain::ColumnDomains;
use models::{SeriesId, SeriesKey};

use crate::error::TskvResult;
use crate::kv_option::StorageOptions;
use crate::tsfamily::super_version::SuperVersion;
use crate::vnode_store::VnodeStorage;
use crate::{Engine, TseriesFamilyId};

#[derive(Debug, Default)]
pub struct MockEngine {}

#[async_trait]
impl Engine for MockEngine {
    async fn get_tsfamily(&self, vnode_id: VnodeId) -> Option<VnodeStorage> {
        todo!()
    }

    async fn open_tsfamily(
        &self,
        tenant: &str,
        db_name: &str,
        vnode_id: VnodeId,
    ) -> TskvResult<VnodeStorage> {
        todo!()
    }

    async fn remove_tsfamily(&self, tenant: &str, database: &str, id: u32) -> TskvResult<()> {
        Ok(())
    }

    async fn flush_tsfamily(
        &self,
        tenant: &str,
        database: &str,
        id: u32,
        trigger_compact: bool,
    ) -> TskvResult<()> {
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

    async fn get_series_id_by_filter(
        &self,
        tenant: &str,
        db: &str,
        tab: &str,
        id: SeriesId,
        filter: &ColumnDomains<String>,
    ) -> TskvResult<Vec<SeriesId>> {
        Ok(vec![])
    }

    async fn get_series_key(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        vnode_id: VnodeId,
        series_id: &[SeriesId],
    ) -> TskvResult<Vec<SeriesKey>> {
        Ok(vec![])
    }

    async fn get_db_version(
        &self,
        tenant: &str,
        db: &str,
        vnode_id: u32,
    ) -> TskvResult<Option<Arc<SuperVersion>>> {
        todo!()
    }

    // fn alter_database(&self, schema: &DatabaseSchema) -> Result<()> {
    //     todo!()
    // }

    fn get_storage_options(&self) -> Arc<StorageOptions> {
        todo!()
    }

    async fn compact(&self, vnode_ids: Vec<TseriesFamilyId>) -> TskvResult<()> {
        todo!()
    }

    async fn get_vnode_hash_tree(&self, vnode_ids: VnodeId) -> TskvResult<RecordBatch> {
        todo!()
    }

    async fn close(&self) {}
}
