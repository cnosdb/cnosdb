use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use models::predicate::domain::{ColumnDomains, TimeRange};
use models::schema::TableColumn;
use models::{ColumnId, SeriesId, SeriesKey};
use protos::kv_service::{WritePointsRequest, WritePointsResponse};

use crate::error::Result;
use crate::kv_option::StorageOptions;
use crate::tseries_family::SuperVersion;
use crate::{TseriesFamilyId, VersionEdit};

pub type EngineRef = Arc<dyn Engine>;

#[async_trait]
pub trait Engine: Send + Sync + Debug {
    async fn write(&self, id: u32, write_batch: WritePointsRequest) -> Result<WritePointsResponse>;

    async fn write_from_wal(
        &self,
        id: u32,
        write_batch: WritePointsRequest,
        seq: u64,
    ) -> Result<()>;

    async fn drop_database(&self, tenant: &str, database: &str) -> Result<()>;

    async fn drop_table(&self, tenant: &str, database: &str, table: &str) -> Result<()>;

    async fn remove_tsfamily(&self, tenant: &str, database: &str, id: u32) -> Result<()>;

    async fn flush_tsfamily(&self, tenant: &str, database: &str, id: u32) -> Result<()>;

    async fn add_table_column(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        column: TableColumn,
    ) -> Result<()>;

    async fn drop_table_column(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        column: &str,
    ) -> Result<()>;

    async fn change_table_column(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        column_name: &str,
        new_column: TableColumn,
    ) -> Result<()>;

    async fn delete_series(
        &self,
        tenant: &str,
        database: &str,
        series_ids: &[SeriesId],
        field_ids: &[ColumnId],
        time_range: &TimeRange,
    ) -> Result<()>;

    async fn get_series_id_by_filter(
        &self,
        tenant: &str,
        db: &str,
        tab: &str,
        vnode_id: SeriesId,
        filter: &ColumnDomains<String>,
    ) -> Result<Vec<SeriesId>>;

    async fn get_series_key(
        &self,
        tenant: &str,
        db: &str,
        vnode_id: u32,
        sid: SeriesId,
    ) -> Result<Option<SeriesKey>>;

    async fn get_db_version(
        &self,
        tenant: &str,
        db: &str,
        vnode_id: u32,
    ) -> Result<Option<Arc<SuperVersion>>>;

    fn get_storage_options(&self) -> Arc<StorageOptions>;

    async fn get_vnode_summary(
        &self,
        tenant: &str,
        database: &str,
        vnode_id: u32,
    ) -> Result<Option<VersionEdit>>;

    async fn apply_vnode_summary(
        &self,
        tenant: &str,
        database: &str,
        vnode_id: u32,
        summary: VersionEdit,
    ) -> Result<()>;

    async fn drop_vnode(&self, id: TseriesFamilyId) -> Result<()>;

    async fn compact(&self, vnode_ids: Vec<TseriesFamilyId>) -> Result<()>;

    async fn close(&self);
}
