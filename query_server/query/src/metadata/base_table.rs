use std::sync::Arc;

use coordinator::service::CoordinatorRef;
use datafusion::common::Result as DFResult;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::error::DataFusionError;
use meta::model::MetaClientRef;
use models::schema::TableSchema;
use spi::query::datasource::stream::StreamProviderManagerRef;

use super::TableHandleProvider;
use crate::data_source::batch::tskv::ClusterTable;
use crate::data_source::split::SplitManagerRef;
use crate::data_source::table_source::TableHandle;

pub struct BaseTableProvider {
    coord: CoordinatorRef,
    split_manager: SplitManagerRef,
    meta_client: MetaClientRef,
    stream_provider_manager: StreamProviderManagerRef,
}

impl BaseTableProvider {
    pub fn new(
        coord: CoordinatorRef,
        split_manager: SplitManagerRef,
        meta_client: MetaClientRef,
        stream_provider_manager: StreamProviderManagerRef,
    ) -> Self {
        Self {
            coord,
            split_manager,
            meta_client,
            stream_provider_manager,
        }
    }
}

impl TableHandleProvider for BaseTableProvider {
    fn build_table_handle(&self, database_name: &str, table_name: &str) -> DFResult<TableHandle> {
        let table_handle: TableHandle = match self
            .meta_client
            .get_table_schema(database_name, table_name)
            .map_err(|e| DataFusionError::External(Box::new(e)))?
        {
            Some(table) => match table {
                TableSchema::TsKvTableSchema(schema) => Arc::new(ClusterTable::new(
                    self.coord.clone(),
                    self.split_manager.clone(),
                    self.meta_client.clone(),
                    schema,
                ))
                .into(),
                TableSchema::ExternalTableSchema(schema) => {
                    let table_path = ListingTableUrl::parse(&schema.location)?;
                    let options = schema.table_options()?;
                    let config = ListingTableConfig::new(table_path)
                        .with_listing_options(options)
                        .with_schema(Arc::new(schema.schema.clone()));
                    Arc::new(ListingTable::try_new(config)?).into()
                }
                TableSchema::StreamTableSchema(table) => self
                    .stream_provider_manager
                    .create_provider(self.meta_client.clone(), table.as_ref())
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
                    .into(),
            },
            None => {
                return Err(DataFusionError::Plan(format!(
                    "Table not found, tenant: {} db: {}, table: {}",
                    self.meta_client.tenant_name(),
                    database_name,
                    table_name,
                )));
            }
        };

        Ok(table_handle)
    }
}
