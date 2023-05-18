use std::sync::Arc;

use datafusion::datasource::TableProvider;
use spi::query::session::SessionCtx;

use crate::metadata::usage_schema_provider::{
    create_usage_schema_view_table, UsageSchemaTableFactory,
};
use crate::metadata::TableHandleProviderRef;

pub const USAGE_SCHEMA_COORD_DATA_OUT: &str = "coord_data_out";

pub struct CoordDataOut {}

impl UsageSchemaTableFactory for CoordDataOut {
    fn table_name(&self) -> &str {
        USAGE_SCHEMA_COORD_DATA_OUT
    }
    fn create(
        &self,
        session: &SessionCtx,
        base_table_provider: &TableHandleProviderRef,
    ) -> spi::Result<Arc<dyn TableProvider>> {
        create_usage_schema_view_table(session, base_table_provider, USAGE_SCHEMA_COORD_DATA_OUT)
    }
}
