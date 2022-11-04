use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::datasource::file_format::avro::AvroFormat;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::file_type::{FileCompressionType, FileType};
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::CreateExternalTable;
use datafusion::prelude::SessionConfig;
use datafusion::sql::TableReference;
use snafu::ResultExt;
use spi::catalog::MetadataError;
use spi::query::execution::ExecutionError;
use spi::query::execution::{self, ExternalSnafu};
use spi::query::execution::{Output, QueryStateMachineRef};

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

    let (provided_schema, options) =
        construct_listing_table_options(stmt, &state.config).context(ExternalSnafu)?;

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
) -> Result<(Option<Arc<Schema>>, ListingOptions), DataFusionError> {
    let CreateExternalTable {
        ref schema,
        ref table_partition_cols,
        ..
    } = stmt;

    let (file_format, file_extension) = construct_file_format_and_extension(stmt)?;

    // TODO make schema in CreateExternalTable optional instead of empty
    let provided_schema = if schema.fields().is_empty() {
        None
    } else {
        Some(Arc::new(schema.as_ref().to_owned().into()))
    };

    let options = ListingOptions {
        format: file_format,
        collect_stat: false,
        file_extension,
        target_partitions: config.target_partitions,
        table_partition_cols: table_partition_cols.clone(),
    };

    Ok((provided_schema, options))
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
    plan: &CreateExternalTable,
) -> Result<(Arc<dyn FileFormat>, String), DataFusionError> {
    let file_compression_type =
        match FileCompressionType::from_str(plan.file_compression_type.as_str()) {
            Ok(t) => t,
            Err(_) => Err(DataFusionError::Execution(
                "Only known FileCompressionTypes can be ListingTables!".to_string(),
            ))?,
        };

    let file_type = match FileType::from_str(plan.file_type.as_str()) {
        Ok(t) => t,
        Err(_) => Err(DataFusionError::Execution(
            "Only known FileTypes can be ListingTables!".to_string(),
        ))?,
    };

    let file_extension = file_type.get_ext_with_compression(file_compression_type.to_owned())?;

    let file_format: Arc<dyn FileFormat> = match file_type {
        FileType::CSV => Arc::new(
            CsvFormat::default()
                .with_has_header(plan.has_header)
                .with_delimiter(plan.delimiter as u8)
                .with_file_compression_type(file_compression_type),
        ),
        FileType::PARQUET => Arc::new(ParquetFormat::default()),
        FileType::AVRO => Arc::new(AvroFormat::default()),
        FileType::JSON => {
            Arc::new(JsonFormat::default().with_file_compression_type(file_compression_type))
        }
    };

    Ok((file_format, file_extension))
}
