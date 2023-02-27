use std::sync::Arc;

use coordinator::service::CoordinatorRef;
use datafusion::datasource::TableProvider;
use meta::MetaClientRef;
use models::auth::user::User;
use spi::Result;

use crate::metadata::usage_schema_provider::{
    create_usage_schema_view_table, UsageSchemaTableFactory,
};

pub const USAGE_SCHEMA_DISK_STORAGE: &str = "disk_storage";
pub struct DiskStorage {}

impl UsageSchemaTableFactory for DiskStorage {
    fn table_name(&self) -> &'static str {
        USAGE_SCHEMA_DISK_STORAGE
    }

    fn create(
        &self,
        user: &User,
        coord: CoordinatorRef,
        meta: MetaClientRef,
        default_catalog: MetaClientRef,
    ) -> Result<Arc<dyn TableProvider>> {
        create_usage_schema_view_table(
            user,
            coord,
            meta,
            USAGE_SCHEMA_DISK_STORAGE,
            default_catalog,
        )
    }
}
