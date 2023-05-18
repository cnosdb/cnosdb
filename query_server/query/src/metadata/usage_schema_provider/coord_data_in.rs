use std::sync::Arc;

use datafusion::datasource::TableProvider;
use spi::query::session::SessionCtx;
use spi::Result;

use crate::metadata::usage_schema_provider::{
    create_usage_schema_view_table, UsageSchemaTableFactory,
};
use crate::metadata::TableHandleProviderRef;

pub const USAGE_SCHEMA_COORD_DATA_IN: &str = "coord_data_in";

pub struct CoordDataIn {}

impl UsageSchemaTableFactory for CoordDataIn {
    fn table_name(&self) -> &str {
        USAGE_SCHEMA_COORD_DATA_IN
    }
    fn create(
        &self,
        session: &SessionCtx,
        base_table_provider: &TableHandleProviderRef,
    ) -> Result<Arc<dyn TableProvider>> {
        create_usage_schema_view_table(session, base_table_provider, USAGE_SCHEMA_COORD_DATA_IN)
    }
}
