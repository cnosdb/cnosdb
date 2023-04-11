use std::sync::Arc;

use coordinator::service::CoordinatorRef;
use meta::error::MetaError;
use meta::model::MetaClientRef;
use models::schema::StreamTable;
use spi::query::datasource::stream::checker::SchemaChecker;
use spi::query::datasource::stream::{StreamProviderFactory, StreamProviderRef};
use spi::QueryError;

use super::provider::TskvStreamProvider;
use super::{get_target_db_name, get_target_table_name, STREAM_DB_KEY, STREAM_TABLE_KEY};
use crate::data_source::batch::tskv::ClusterTable;
use crate::data_source::split::SplitManagerRef;

pub const TSKV_STREAM_PROVIDER: &str = "tskv";

pub struct TskvStreamProviderFactory {
    client: CoordinatorRef,
    split_manager: SplitManagerRef,
}

impl TskvStreamProviderFactory {
    pub fn new(client: CoordinatorRef, split_manager: SplitManagerRef) -> Self {
        Self {
            client,
            split_manager,
        }
    }
}

impl SchemaChecker<StreamTable> for TskvStreamProviderFactory {
    fn check(&self, client: &MetaClientRef, table: &StreamTable) -> Result<(), QueryError> {
        if table.stream_type() != TSKV_STREAM_PROVIDER {
            return Err(QueryError::Internal { reason: format!("The {TSKV_STREAM_PROVIDER} stream data source cannot handle the {} stream table", table.stream_type()) });
        }

        let table_name = table.name();
        let options = table.extra_options();

        let target_db =
            options
                .get(STREAM_DB_KEY)
                .ok_or_else(|| QueryError::MissingTableOptions {
                    option_name: STREAM_DB_KEY.to_string(),
                    table_name: table_name.to_string(),
                })?;

        let target_table =
            options
                .get(STREAM_TABLE_KEY)
                .ok_or_else(|| QueryError::MissingTableOptions {
                    option_name: STREAM_TABLE_KEY.to_string(),
                    table_name: table_name.to_string(),
                })?;

        let target_table = client
            .get_tskv_table_schema(target_db, target_table)?
            .ok_or_else(|| QueryError::InvalidTableOption {
                option_name: format!("{STREAM_DB_KEY} | {STREAM_TABLE_KEY}"),
                table_name: format!("{target_db}.{target_table}"),
                reason: "Table not found.".to_string(),
            })?;

        // Make sure that the columns specified in [`StreamTable`] must be included in the target table
        let target_schema = target_table.to_arrow_schema();
        for f in table.schema().fields() {
            target_schema.field_with_name(f.name())?;
        }

        Ok(())
    }
}

impl StreamProviderFactory for TskvStreamProviderFactory {
    fn create(
        &self,
        meta: MetaClientRef,
        table: &StreamTable,
    ) -> Result<StreamProviderRef, QueryError> {
        let options = table.extra_options();
        let watermark = table.watermark();

        let target_db = get_target_db_name(options).unwrap_or_else(|| table.db());
        let target_table = get_target_table_name(table.name(), options)?;

        let database_info =
            meta.get_db_info(target_db)?
                .ok_or_else(|| MetaError::DatabaseNotFound {
                    database: target_db.into(),
                })?;

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

        let table = Arc::new(ClusterTable::new(
            self.client.clone(),
            self.split_manager.clone(),
            Arc::new(database_info),
            table_schema,
        ));

        Ok(Arc::new(TskvStreamProvider::new(
            watermark.clone(),
            table,
            used_schema,
        )))
    }
}
