#![allow(dead_code)]
#![allow(unreachable_patterns)]
#![feature(maybe_uninit_array_assume_init)]
#![feature(maybe_uninit_uninit_array)]

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
pub use compaction::check::vnode_table_checksum_schema;
use datafusion::arrow::record_batch::RecordBatch;
use models::meta_data::VnodeId;
use models::predicate::domain::{ColumnDomains, TimeRange};
use models::schema::{Precision, TableColumn};
use models::{ColumnId, SeriesId, SeriesKey};
use protos::kv_service::{WritePointsRequest, WritePointsResponse};
use trace::SpanContext;

pub use crate::error::{Error, Result};
pub use crate::kv_option::Options;
use crate::kv_option::StorageOptions;
pub use crate::kvcore::TsKv;
pub use crate::summary::{print_summary_statistics, Summary, VersionEdit};
use crate::tseries_family::SuperVersion;
pub use crate::tsm::print_tsm_statistics;
pub use crate::wal::print_wal_statistics;

pub mod byte_utils;
mod compaction;
mod compute;
mod context;
pub mod database;
pub mod engine_mock;
pub mod error;
pub mod file_system;
pub mod file_utils;
pub mod index;
pub mod kv_option;
mod kvcore;
mod memcache;
// TODO supposedly private
pub mod reader;
mod record_file;
mod schema;
mod summary;
mod tseries_family;
pub mod tsm;
mod tsm2;
mod version_set;
mod wal;

pub type ColumnFileId = u64;
type TseriesFamilyId = u32;
type LevelId = u32;

/// Returns the normalized tenant of a WritePointsRequest
pub fn tenant_name_from_request(req: &protos::kv_service::WritePointsRequest) -> String {
    match &req.meta {
        Some(meta) => meta.tenant.clone(),
        None => models::schema::DEFAULT_CATALOG.to_string(),
    }
}

pub type EngineRef = Arc<dyn Engine>;

#[async_trait]
pub trait Engine: Send + Sync + Debug {
    /// Tskv engine write the gRPC message `WritePointsRequest`(which contains
    /// the tenant, user, database, some tables, and each table has some rows)
    /// into a `Vnode` managed by engine.
    ///
    /// - span_ctx - The trace span.
    /// - vnode_id - ID of the storage unit(caches and files).
    /// - precision - The timestamp precision of table rows.
    ///
    /// Data will be written to the write-ahead-log first.
    async fn write(
        &self,
        span_ctx: Option<&SpanContext>,
        vnode_id: VnodeId,
        precision: Precision,
        write_batch: WritePointsRequest,
    ) -> Result<WritePointsResponse>;

    /// Remove all storage unit(caches and files) in specified database,
    /// then remove directory of the database.
    async fn drop_database(&self, tenant: &str, database: &str) -> Result<()>;

    /// Delete all data of a table.
    async fn drop_table(&self, tenant: &str, database: &str, table: &str) -> Result<()>;

    /// Remove the storage unit(caches and files) managed by engine,
    /// then remove directory of the storage unit.
    async fn remove_tsfamily(&self, tenant: &str, database: &str, vnode_id: VnodeId) -> Result<()>;

    /// Mark the storage unit as `Copying` and flush caches.
    async fn prepare_copy_vnode(
        &self,
        tenant: &str,
        database: &str,
        vnode_id: VnodeId,
    ) -> Result<()>;

    /// Flush all caches of the storage unit into a file.
    async fn flush_tsfamily(&self, tenant: &str, database: &str, vnode_id: VnodeId) -> Result<()>;

    // TODO this method is not completed,
    async fn add_table_column(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        column: TableColumn,
    ) -> Result<()>;

    // TODO this method is not completed,
    async fn drop_table_column(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        column: &str,
    ) -> Result<()>;

    // TODO this method is not completed,
    async fn change_table_column(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        column_name: &str,
        new_column: TableColumn,
    ) -> Result<()>;

    /// Modify the name of the tag type column of the specified table
    ///
    /// TODO Could specify vnode id, because the current interface may include modifying multiple vnodes, but atomicity cannot be guaranteed.
    async fn rename_tag(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        tag_name: &str,
        new_tag_name: &str,
    ) -> Result<()>;

    // TODO this method is not completed,
    async fn delete_series(
        &self,
        tenant: &str,
        database: &str,
        series_ids: &[SeriesId],
        field_ids: &[ColumnId],
        time_range: &TimeRange,
    ) -> Result<()>;

    /// Read index of a storage unit, find series ids that matches the filter.
    async fn get_series_id_by_filter(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        vnode_id: VnodeId,
        filter: &ColumnDomains<String>,
    ) -> Result<Vec<SeriesId>>;

    /// Read index of a storage unit, get `SeriesKey` of the geiven series id.
    async fn get_series_key(
        &self,
        tenant: &str,
        database: &str,
        vnode_id: VnodeId,
        series_id: SeriesId,
    ) -> Result<Option<SeriesKey>>;

    /// Get a `SuperVersion` that contains the latest version of caches and files
    /// of the storage unit.
    async fn get_db_version(
        &self,
        tenant: &str,
        database: &str,
        vnode_id: u32,
    ) -> Result<Option<Arc<SuperVersion>>>;

    /// Get the storage options which was used to install the engine.
    fn get_storage_options(&self) -> Arc<StorageOptions>;

    /// Get the summary(information of files) of the storae unit.
    async fn get_vnode_summary(
        &self,
        tenant: &str,
        database: &str,
        vnode_id: u32,
    ) -> Result<Option<VersionEdit>>;

    /// Try to build a new storage unit from the summary(information of files),
    /// if it already exists, delete first.
    async fn apply_vnode_summary(
        &self,
        tenant: &str,
        database: &str,
        vnode_id: u32,
        summary: VersionEdit,
    ) -> Result<()>;

    // TODO this method is the same as remove_tsfamily and not be referenced,
    // we can delete it.
    #[deprecated]
    async fn drop_vnode(&self, id: TseriesFamilyId) -> Result<()>;

    /// For the specified storage units, flush all caches into files, then compact
    /// files into larger files.
    async fn compact(&self, vnode_ids: Vec<TseriesFamilyId>) -> Result<()>;

    /// Get a compressed hash_tree(ID and checksum of each vnode) of engine.
    async fn get_vnode_hash_tree(&self, vnode_id: VnodeId) -> Result<RecordBatch>;

    /// Close all background jobs of engine.
    async fn close(&self);
}

pub mod test {
    pub use crate::memcache::test::{get_one_series_cache_data, put_rows_to_cache};
}
