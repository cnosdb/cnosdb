use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::datasource::file_format::avro::{AvroFormat, DEFAULT_AVRO_EXTENSION};
use datafusion::datasource::file_format::csv::{CsvFormat, DEFAULT_CSV_EXTENSION};
use datafusion::datasource::file_format::json::{JsonFormat, DEFAULT_JSON_EXTENSION};
use datafusion::datasource::file_format::parquet::{ParquetFormat, DEFAULT_PARQUET_EXTENSION};
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::context::SessionState;
use datafusion::prelude::SessionConfig;
use datafusion::sql::TableReference;
use snafu::ResultExt;
use spi::catalog::MetadataError;
use spi::query::execution;
use spi::query::execution::ExecutionError;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::{CSVOptions, CreateExternalTable, FileDescriptor};

use super::DDLDefinitionTask;

pub struct CreateExternalTableTask {
    stmt: CreateExternalTable,
}

impl CreateExternalTableTask {
    #[inline(always)]
    pub fn new(stmt: CreateExternalTable) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for CreateExternalTableTask {
    async fn execute(
        &self,
        query_state_machine: QueryStateMachineRef,
    ) -> Result<Output, ExecutionError> {
        let CreateExternalTable {
            ref name,
            ref if_not_exists,
            ..
        } = self.stmt;

        let table_ref: TableReference = name.as_str().into();
        let table = query_state_machine.catalog.table_provider(table_ref);

        match (if_not_exists, table) {
            // do not create if exists
            (true, Ok(_)) => Ok(Output::Nil(())),
            // Report an error if it exists
            (false, Ok(_)) => Err(MetadataError::TableAlreadyExists {
                table_name: name.clone(),
            })
            .context(execution::MetadataSnafu),
            // does not exist, create
            (_, Err(_)) => {
                create_exernal_table(&self.stmt, query_state_machine).await?;
                Ok(Output::Nil(()))
            }
        }
    }
}

async fn create_exernal_table(
    stmt: &CreateExternalTable,
    query_state_machine: QueryStateMachineRef,
) -> Result<(), ExecutionError> {
    let CreateExternalTable {
        ref name,
        ref location,
        ..
    } = stmt;

    let state = query_state_machine.session.inner().state();

    let (provided_schema, options) = construct_listing_table_options(stmt, &state.config);

    let listing_table = construct_listing_table(location, options, provided_schema, &state).await?;

    query_state_machine
        .catalog
        .create_table(name, Arc::new(listing_table))
        .context(execution::MetadataSnafu)?;

    Ok(())
}

fn construct_listing_table_options(
    stmt: &CreateExternalTable,
    config: &SessionConfig,
) -> (Option<Arc<Schema>>, ListingOptions) {
    let CreateExternalTable {
        ref schema,
        ref file_descriptor,
        ref table_partition_cols,
        ..
    } = stmt;

    let (file_format, file_extension) = construct_file_format_and_extension(file_descriptor);

    // TODO make schema in CreateExternalTable optional instead of empty
    let provided_schema = if schema.fields().is_empty() {
        None
    } else {
        Some(Arc::new(schema.as_ref().to_owned().into()))
    };

    let options = ListingOptions {
        format: file_format,
        collect_stat: false,
        file_extension: file_extension.to_owned(),
        target_partitions: config.target_partitions,
        table_partition_cols: table_partition_cols.clone(),
    };

    (provided_schema, options)
}

/// construct a table that uses the listing feature of the object store to
/// find the files to be processed
/// This is async because it might need to resolve the schema.
async fn construct_listing_table(
    table_path: impl AsRef<str>,
    options: ListingOptions,
    provided_schema: Option<SchemaRef>,
    state: &SessionState,
) -> Result<ListingTable, ExecutionError> {
    let table_path = ListingTableUrl::parse(table_path).context(execution::ExternalSnafu)?;
    let resolved_schema = match provided_schema {
        None => options
            .infer_schema(state, &table_path)
            .await
            .context(execution::ExternalSnafu)?,
        Some(s) => s,
    };
    let config = ListingTableConfig::new(table_path)
        .with_listing_options(options)
        .with_schema(resolved_schema);
    let table = ListingTable::try_new(config).context(execution::ExternalSnafu)?;

    Ok(table)
}

fn construct_file_format_and_extension(
    file_descriptor: &FileDescriptor,
) -> (Arc<dyn FileFormat>, &str) {
    match file_descriptor {
        FileDescriptor::NdJson => (
            Arc::new(JsonFormat::default()) as Arc<dyn FileFormat>,
            DEFAULT_JSON_EXTENSION,
        ),
        FileDescriptor::Parquet => (
            Arc::new(ParquetFormat::default()) as Arc<dyn FileFormat>,
            DEFAULT_PARQUET_EXTENSION,
        ),
        FileDescriptor::CSV(CSVOptions {
            has_header,
            delimiter,
        }) => (
            Arc::new(
                CsvFormat::default()
                    .with_has_header(*has_header)
                    .with_delimiter(*delimiter as u8),
            ) as Arc<dyn FileFormat>,
            DEFAULT_CSV_EXTENSION,
        ),
        FileDescriptor::Avro => (
            Arc::new(AvroFormat::default()) as Arc<dyn FileFormat>,
            DEFAULT_AVRO_EXTENSION,
        ),
    }
}
