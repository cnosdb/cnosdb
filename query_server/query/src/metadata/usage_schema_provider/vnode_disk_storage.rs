use std::sync::Arc;

use datafusion::datasource::TableProvider;
use spi::query::session::SessionCtx;
use spi::Result;

use crate::metadata::usage_schema_provider::{
    create_usage_schema_view_table, UsageSchemaTableFactory,
};
use crate::metadata::TableHandleProviderRef;

pub const USAGE_SCHEMA_VNODE_DISK_STORAGE: &str = "vnode_disk_storage";

pub struct VnodeDiskStorage {}

impl UsageSchemaTableFactory for VnodeDiskStorage {
    fn table_name(&self) -> &'static str {
        USAGE_SCHEMA_VNODE_DISK_STORAGE
    }

    fn create(
        &self,
        session: &SessionCtx,
        base_table_provider: &TableHandleProviderRef,
    ) -> Result<Arc<dyn TableProvider>> {
        create_usage_schema_view_table(
            session,
            base_table_provider,
            USAGE_SCHEMA_VNODE_DISK_STORAGE,
        )
    }
}
