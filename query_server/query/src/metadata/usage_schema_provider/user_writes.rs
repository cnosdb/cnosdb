use std::sync::Arc;

use coordinator::service::CoordinatorRef;
use datafusion::datasource::TableProvider;
use meta::model::MetaClientRef;
use models::auth::user::User;

use crate::metadata::usage_schema_provider::{
    create_usage_schema_view_table, UsageSchemaTableFactory,
};

pub const USAGE_SCHEMA_USER_WRITES: &str = "user_writes";

pub struct UserWrites {}

impl UsageSchemaTableFactory for UserWrites {
    fn table_name(&self) -> &str {
        USAGE_SCHEMA_USER_WRITES
    }
    fn create(
        &self,
        user: &User,
        coord: CoordinatorRef,
        meta: MetaClientRef,
        default_catalog: MetaClientRef,
    ) -> spi::Result<Arc<dyn TableProvider>> {
        create_usage_schema_view_table(user, coord, meta, USAGE_SCHEMA_USER_WRITES, default_catalog)
    }
}
