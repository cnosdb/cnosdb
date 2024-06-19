use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
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
use models::schema::external_table_schema::ExternalTableSchema;
use models::schema::table_schema::TableSchema;
use snafu::ResultExt;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::{DatafusionSnafu, MetaSnafu, QueryError, QueryResult};

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
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> QueryResult<Output> {
        let CreateExternalTable {
            ref name,
            ref if_not_exists,
            ..
        } = self.stmt;
        let table = name.table();
        let database = name.schema().ok_or_else(|| QueryError::Internal {
            reason: "couldn't resolve database".to_string(),
        })?;
        let tenant = name.catalog().ok_or_else(|| QueryError::Internal {
            reason: "couldn't resolve tenant".to_string(),
        })?;
        let client = query_state_machine
            .meta
            .tenant_meta(tenant)
            .await
            .ok_or_else(|| MetaError::TenantNotFound {
                tenant: tenant.to_string(),
            })
            .context(MetaSnafu)?;

        let table = client
            .get_external_table_schema(database, table)
            .context(MetaSnafu)?;

        match (if_not_exists, table) {
            // do not create if exists
            (true, Some(_)) => Ok(Output::Nil(())),
            // Report an error if it exists
            (false, Some(_)) => Err(MetaError::TableAlreadyExists {
                table_name: name.to_string(),
            })
            .context(MetaSnafu)?,
            // does not exist, create
            (_, None) => {
                create_external_table(&self.stmt, query_state_machine).await?;
                Ok(Output::Nil(()))
            }
        }
    }
}

#[allow(dead_code)]
async fn create_external_table(
    stmt: &CreateExternalTable,
    query_state_machine: QueryStateMachineRef,
) -> QueryResult<()> {
    let schema = build_table_schema(stmt, query_state_machine.session.inner()).await?;

    let client = query_state_machine
        .meta
        .tenant_meta(schema.tenant.as_str())
        .await
        .ok_or_else(|| MetaError::TenantNotFound {
            tenant: schema.tenant.clone(),
        })
        .context(MetaSnafu)?;

    client
        .create_table(&TableSchema::ExternalTableSchema(Arc::new(schema)))
        .await
        .context(MetaSnafu)
}

async fn build_table_schema(
    stmt: &CreateExternalTable,
    state: &SessionState,
) -> QueryResult<ExternalTableSchema> {
    let db = stmt.name.schema().ok_or_else(|| QueryError::Internal {
        reason: "couldn't resolve database".to_string(),
    })?;
    let tenant = stmt.name.catalog().ok_or_else(|| QueryError::Internal {
        reason: "couldn't resolve tenant".to_string(),
    })?;

    let schema = construct_listing_table_schema(stmt, state).await?;

    let schema = ExternalTableSchema {
        tenant: tenant.to_string(),
        db: db.to_string(),
        name: stmt.name.table().to_string(),
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
) -> QueryResult<SchemaRef> {
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
    let options = build_external_table_config(stmt, state.config().target_partitions())?;

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
) -> QueryResult<ListingOptions> {
    let file_compression_type = FileCompressionType::from(stmt.file_compression_type);
    let file_type = FileType::from_str(stmt.file_type.as_str())?;
    let file_format: Arc<dyn FileFormat> = match file_type {
        FileType::CSV => Arc::new(
            CsvFormat::default()
                .with_has_header(stmt.has_header)
                .with_delimiter(stmt.delimiter as u8)
                .with_file_compression_type(file_compression_type),
        ),
        FileType::PARQUET => Arc::new(ParquetFormat::default()),
        FileType::AVRO => Arc::new(AvroFormat),
        FileType::JSON => {
            Arc::new(JsonFormat::default().with_file_compression_type(file_compression_type))
        }
        FileType::ARROW => {
            return Err(QueryError::NotImplemented {
                err: "Build arrow external table config".to_string(),
            })
        }
    };

    let options = ListingOptions::new(file_format).with_target_partitions(target_partitions);

    Ok(options)
}
