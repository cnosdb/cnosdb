use std::sync::Arc;

use coordinator::service::CoordinatorRef;
use datafusion::datasource::TableProvider;
use meta::model::MetaClientRef;
use models::auth::user::User;
use spi::Result;

use crate::metadata::usage_schema_provider::{
    create_usage_schema_view_table, UsageSchemaTableFactory,
};

pub const USAGE_SCHEMA_COORD_DATA_IN: &str = "coord_data_in";

pub struct CoordDataIn {}

impl UsageSchemaTableFactory for CoordDataIn {
    fn table_name(&self) -> &str {
        USAGE_SCHEMA_COORD_DATA_IN
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
            USAGE_SCHEMA_COORD_DATA_IN,
            default_catalog,
        )
    }
}
