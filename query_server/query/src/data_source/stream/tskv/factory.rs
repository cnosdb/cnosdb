use std::sync::Arc;

use coordinator::service::CoordinatorRef;
use datafusion::prelude::Column;
use meta::error::MetaError;
use meta::MetaClientRef;
use models::schema::StreamTable;
use spi::query::datasource::stream::{StreamProviderFactory, StreamProviderRef};
use spi::QueryError;

use super::provider::TskvStreamProvider;
use super::{get_target_db_name, get_target_table_name};
use crate::extension::EVENT_TIME_COLUMN;

pub const TSKV_STREAM_PROVIDER: &str = "tskv";

pub struct TskvStreamProviderFactory {
    client: CoordinatorRef,
}

impl TskvStreamProviderFactory {
    pub fn new(client: CoordinatorRef) -> Self {
        Self { client }
    }
}

impl StreamProviderFactory for TskvStreamProviderFactory {
    fn create(
        &self,
        meta: MetaClientRef,
        table: &StreamTable,
    ) -> Result<StreamProviderRef, QueryError> {
        let options = table.extra_options();
        let event_time_column =
            options
                .get(EVENT_TIME_COLUMN)
                .ok_or_else(|| QueryError::MissingTableOptions {
                    option_name: EVENT_TIME_COLUMN.into(),
                    table_name: table.name().into(),
                })?;

        let target_db = get_target_db_name(options).unwrap_or_else(|| table.db());
        let target_table = get_target_table_name(table.name(), options)?;

        let table_schema = meta
            .get_tskv_table_schema(target_db, target_table)?
            .ok_or_else(|| MetaError::TableNotFound {
                table: target_table.into(),
            })?;

        let used_schema = if table.schema().fields().is_empty() {
            table_schema.to_arrow_schema()
        } else {
            table.schema()
        };

        Ok(Arc::new(TskvStreamProvider::try_new(
            self.client.clone(),
            Column::from_qualified_name(event_time_column),
            table_schema,
            used_schema,
        )?))
    }
}
