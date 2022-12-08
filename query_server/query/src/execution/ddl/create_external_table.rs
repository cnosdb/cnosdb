use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::TableReference;
use datafusion::datasource::file_format::avro::AvroFormat;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::file_type::{FileCompressionType, FileType};
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTableUrl};
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::CreateExternalTable;

use meta::meta_client::MetaError;
use models::schema::{ExternalTableSchema, TableSchema};
use snafu::ResultExt;
use spi::query::execution::MetadataSnafu;

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
        let table_name = table_ref.resolve(
            query_state_machine.session.tenant(),
            query_state_machine.session.default_database(),
        );
        let client = query_state_machine
            .meta
            .tenant_manager()
            .tenant_meta(table_name.catalog)
            .ok_or(MetaError::TenantNotFound {
                tenant: table_name.catalog.to_string(),
            })
            .context(MetadataSnafu)?;

        let table = client
            .get_external_table_schema(table_name.schema, table_name.table)
            .context(MetadataSnafu)?;

        match (if_not_exists, table) {
            // do not create if exists
            (true, Some(_)) => Ok(Output::Nil(())),
            // Report an error if it exists
            (false, Some(_)) => Err(MetaError::TableAlreadyExists {
                table_name: name.clone(),
            })
            .context(execution::MetadataSnafu),
            // does not exist, create
            (_, None) => {
                create_exernal_table(&self.stmt, query_state_machine).await?;
                Ok(Output::Nil(()))
            }
        }
    }
}

#[allow(dead_code)]
async fn create_exernal_table(
    stmt: &CreateExternalTable,
    query_state_machine: QueryStateMachineRef,
) -> Result<(), ExecutionError> {
    let state = query_state_machine.session.inner().state();

    let schema = build_table_schema(
        stmt,
        query_state_machine.session.tenant().to_string(),
        query_state_machine.session.default_database().to_string(),
        &state,
    )
    .await?;

    let tenant = query_state_machine.session.tenant();
    let client = query_state_machine
        .meta
        .tenant_manager()
        .tenant_meta(tenant)
        .ok_or(MetaError::TenantNotFound {
            tenant: tenant.to_string(),
        })
        .context(MetadataSnafu)?;

    client
        .create_table(&TableSchema::ExternalTableSchema(schema))
        .context(MetadataSnafu)
}

async fn build_table_schema(
    stmt: &CreateExternalTable,
    tenant: String,
    db: String,
    state: &SessionState,
) -> Result<ExternalTableSchema, ExecutionError> {
    let options =
        build_external_table_config(stmt, state.config.target_partitions).context(ExternalSnafu)?;

    let schema = construct_listing_table_schema(stmt, state, &options)
        .await?
        .deref()
        .clone();

    let schema = ExternalTableSchema {
        tenant,
        db,
        name: stmt.name.clone(),
        location: stmt.location.clone(),
        file_type: stmt.file_type.clone(),
        file_compression_type: stmt.file_compression_type.clone(),
        target_partitions: state.config.target_partitions,
        table_partition_cols: stmt.table_partition_cols.clone(),
        has_header: stmt.has_header,
        delimiter: stmt.delimiter as u8,
        schema,
    };
    Ok(schema)
}

async fn construct_listing_table_schema(
    stmt: &CreateExternalTable,
    state: &SessionState,
    options: &ListingOptions,
) -> Result<SchemaRef, ExecutionError> {
    let CreateExternalTable {
        ref schema,
        ref location,
        ..
    } = stmt;

    // TODO make schema in CreateExternalTable optional instead of empty
    let provided_schema = if schema.fields().is_empty() {
        None
    } else {
        Some(Arc::new(schema.as_ref().to_owned().into()))
    };

    let table_path = ListingTableUrl::parse(location).context(ExternalSnafu)?;
    Ok(match provided_schema {
        None => options
            .infer_schema(state, &table_path)
            .await
            .context(execution::ExternalSnafu)?,
        Some(s) => s,
    })
}

fn build_external_table_config(
    stmt: &CreateExternalTable,
    target_partitions: usize,
) -> Result<ListingOptions, DataFusionError> {
    let file_format: Arc<dyn FileFormat> = match FileType::from_str(&stmt.file_type)? {
        FileType::CSV => Arc::new(
            CsvFormat::default()
                .with_has_header(stmt.has_header)
                .with_delimiter(stmt.delimiter as u8)
                .with_file_compression_type(
                    FileCompressionType::from_str(&stmt.file_compression_type).map_err(|_| {
                        DataFusionError::Execution(
                            "Only known FileCompressionTypes can be ListingTables!".to_string(),
                        )
                    })?,
                ),
        ),
        FileType::PARQUET => Arc::new(ParquetFormat::default()),
        FileType::AVRO => Arc::new(AvroFormat::default()),
        FileType::JSON => Arc::new(JsonFormat::default().with_file_compression_type(
            FileCompressionType::from_str(&stmt.file_compression_type)?,
        )),
    };

    let file_type = match FileType::from_str(stmt.file_type.as_str()) {
        Ok(t) => t,
        Err(_) => Err(DataFusionError::Execution(
            "Only known FileTypes can be ListingTables!".to_string(),
        ))?,
    };
    Ok(ListingOptions {
        format: file_format,
        collect_stat: false,
        file_extension: file_type
            .get_ext_with_compression(stmt.file_compression_type.to_owned().parse()?)?,
        target_partitions,
        table_partition_cols: stmt.table_partition_cols.clone(),
    })
}
