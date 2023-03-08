use std::sync::Arc;

use coordinator::service::CoordinatorRef;
use datafusion::prelude::Column;
use meta::error::MetaError;
use meta::MetaClientRef;
use models::schema::StreamTable;
use spi::query::datasource::stream::{StreamProviderFactory, StreamProviderRef};
use spi::QueryError;

use super::provider::TskvStreamProvider;
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
        let event_time_column = options.get(EVENT_TIME_COLUMN).ok_or_else(|| {
            QueryError::EventTimeColumnNotSpecified {
                name: table.name().into(),
            }
        })?;

        let table_schema = meta
            .get_tskv_table_schema(table.db(), table.name())?
            .ok_or_else(|| MetaError::TableNotFound {
                table: table.name().into(),
            })?;

        Ok(Arc::new(TskvStreamProvider::new(
            self.client.clone(),
            Column::from_qualified_name(event_time_column),
            table_schema,
        )))
    }
}
