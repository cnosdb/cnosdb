use std::sync::Arc;

use datafusion::datasource::TableProvider;
use spi::query::session::SessionCtx;
use spi::Result;

use crate::metadata::usage_schema_provider::{
    create_usage_schema_view_table, UsageSchemaTableFactory,
};
use crate::metadata::TableHandleProviderRef;

pub const USAGE_SCHEMA_COORD_DATA_IN: &str = "coord_data_in";
pub const USAGE_SCHEMA_SQL_DATA_IN: &str = "sql_data_in";
pub const USAGE_SCHEMA_WRITE_DATA_IN: &str = "write_data_in";
pub const USAGE_SCHEMA_SQL_WRITE_ROW: &str = "sql_write_row";
pub const USAGE_SCHEMA_SQL_POINTS_DATA_IN: &str = "sql_points_data_in";

pub struct CoordDataIn {}
pub struct SQLDataIn {}
pub struct WriteDataIn {}
pub struct SQLWriteRow {}
pub struct SQLPointsDataIn {}

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

impl UsageSchemaTableFactory for SQLDataIn {
    fn table_name(&self) -> &str {
        USAGE_SCHEMA_SQL_DATA_IN
    }
    fn create(
        &self,
        session: &SessionCtx,
        base_table_provider: &TableHandleProviderRef,
    ) -> Result<Arc<dyn TableProvider>> {
        create_usage_schema_view_table(session, base_table_provider, USAGE_SCHEMA_SQL_DATA_IN)
    }
}

impl UsageSchemaTableFactory for WriteDataIn {
    fn table_name(&self) -> &str {
        USAGE_SCHEMA_WRITE_DATA_IN
    }
    fn create(
        &self,
        session: &SessionCtx,
        base_table_provider: &TableHandleProviderRef,
    ) -> Result<Arc<dyn TableProvider>> {
        create_usage_schema_view_table(session, base_table_provider, USAGE_SCHEMA_WRITE_DATA_IN)
    }
}

impl UsageSchemaTableFactory for SQLWriteRow {
    fn table_name(&self) -> &str {
        USAGE_SCHEMA_SQL_WRITE_ROW
    }
    fn create(
        &self,
        session: &SessionCtx,
        base_table_provider: &TableHandleProviderRef,
    ) -> Result<Arc<dyn TableProvider>> {
        create_usage_schema_view_table(session, base_table_provider, USAGE_SCHEMA_SQL_WRITE_ROW)
    }
}

impl UsageSchemaTableFactory for SQLPointsDataIn {
    fn table_name(&self) -> &str {
        USAGE_SCHEMA_SQL_POINTS_DATA_IN
    }
    fn create(
        &self,
        session: &SessionCtx,
        base_table_provider: &TableHandleProviderRef,
    ) -> Result<Arc<dyn TableProvider>> {
        create_usage_schema_view_table(
            session,
            base_table_provider,
            USAGE_SCHEMA_SQL_POINTS_DATA_IN,
        )
    }
}
