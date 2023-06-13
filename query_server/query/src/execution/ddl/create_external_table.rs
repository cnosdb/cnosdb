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
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::CreateExternalTable;
use lazy_static::__Deref;
use meta::error::MetaError;
use models::schema::{ExternalTableSchema, TableSchema};
use snafu::ResultExt;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::{DatafusionSnafu, Result};

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
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> Result<Output> {
        let CreateExternalTable {
            ref name,
            ref if_not_exists,
            ..
        } = self.stmt;
        let unresolved_name = name.to_string();
        let table_name = TableReference::parse_str(&unresolved_name).resolve(
            query_state_machine.session.tenant(),
            query_state_machine.session.default_database(),
        );
        let client = query_state_machine
            .meta
            .tenant_meta(&table_name.catalog)
            .await
            .ok_or(MetaError::TenantNotFound {
                tenant: table_name.catalog.to_string(),
            })?;
        // .context(spi::MetaSnafu)?;

        let table = client.get_external_table_schema(&table_name.schema, &table_name.table)?;
        // .context(spi::MetaSnafu)?;

        match (if_not_exists, table) {
            // do not create if exists
            (true, Some(_)) => Ok(Output::Nil(())),
            // Report an error if it exists
            (false, Some(_)) => Err(MetaError::TableAlreadyExists {
                table_name: name.to_string(),
            })?,
            // .context(spi::MetaSnafu),
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
) -> Result<()> {
    let state = query_state_machine.session.inner().state();

    let schema = build_table_schema(
        stmt,
        query_state_machine.session.tenant().to_string(),
        query_state_machine.session.default_database().to_string(),
        &state,
    )
    .await?;

    let tenant = query_state_machine.session.tenant();
    let client =
        query_state_machine
            .meta
            .tenant_meta(tenant)
            .await
            .ok_or(MetaError::TenantNotFound {
                tenant: tenant.to_string(),
            })?;
    // .context(MetaSnafu)?;

    Ok(client
        .create_table(&TableSchema::ExternalTableSchema(Arc::new(schema)))
        .await?)
    // .context(MetaSnafu)
}

async fn build_table_schema(
    stmt: &CreateExternalTable,
    tenant: String,
    db: String,
    state: &SessionState,
) -> Result<ExternalTableSchema> {
    let options = build_external_table_config(stmt, state.config().target_partitions())?;

    let schema = construct_listing_table_schema(stmt, state, &options).await?;

    let schema = ExternalTableSchema {
        tenant,
        db,
        name: stmt.name.to_string(),
        location: stmt.location.clone(),
        file_type: stmt.file_type.clone(),
        file_compression_type: stmt.file_compression_type.to_string(),
        target_partitions: state.config().target_partitions(),
        table_partition_cols: Default::default(),
        has_header: stmt.has_header,
        delimiter: stmt.delimiter as u8,
        schema: schema.deref().clone(),
    };
    Ok(schema)
}

async fn construct_listing_table_schema(
    stmt: &CreateExternalTable,
    state: &SessionState,
    options: &ListingOptions,
) -> Result<SchemaRef> {
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

    let table_path = ListingTableUrl::parse(location).context(DatafusionSnafu)?;
    Ok(match provided_schema {
        None => options
            .infer_schema(state, &table_path)
            .await
            .context(spi::DatafusionSnafu)?,
        Some(s) => s,
    })
}

fn build_external_table_config(
    stmt: &CreateExternalTable,
    target_partitions: usize,
) -> Result<ListingOptions> {
    let file_compression_type = FileCompressionType::from(stmt.file_compression_type);
    let file_type = FileType::from_str(stmt.file_type.as_str())?;
    let file_extension = file_type.get_ext_with_compression(file_compression_type.to_owned())?;
    let file_format: Arc<dyn FileFormat> = match file_type {
        FileType::CSV => Arc::new(
            CsvFormat::default()
                .with_has_header(stmt.has_header)
                .with_delimiter(stmt.delimiter as u8)
                .with_file_compression_type(file_compression_type),
        ),
        FileType::PARQUET => Arc::new(ParquetFormat::default()),
        FileType::AVRO => Arc::new(AvroFormat::default()),
        FileType::JSON => {
            Arc::new(JsonFormat::default().with_file_compression_type(file_compression_type))
        }
    };

    let options = ListingOptions::new(file_format)
        .with_file_extension(file_extension)
        .with_target_partitions(target_partitions);

    Ok(options)
}
