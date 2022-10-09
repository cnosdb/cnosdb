use crate::execution::ddl::DDLDefinitionTask;
use crate::metadata::MetaDataRef;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::sql::TableReference;
use models::schema::{ColumnType, TableFiled, TableSchema, TIME_FIELD};
use models::ValueType;
use snafu::ResultExt;
use spi::catalog::MetadataError;
use spi::query::execution;
use spi::query::execution::{ExecutionError, Output, QueryStateMachineRef};
use spi::query::logical_planner::CreateTable;
use std::collections::BTreeMap;
use std::sync::Arc;

pub struct CreateTableTask {
    stmt: CreateTable,
}

impl CreateTableTask {
    pub fn new(stmt: CreateTable) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for CreateTableTask {
    async fn execute(
        &self,
        catalog: MetaDataRef,
        _query_state_machine: QueryStateMachineRef,
    ) -> Result<Output, ExecutionError> {
        let CreateTable {
            ref name,
            ref if_not_exists,
            ..
        } = self.stmt;

        let table_ref: TableReference = name.as_str().into();
        let table = catalog.table_provider(table_ref);

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
                create_table(&self.stmt, catalog).await?;
                Ok(Output::Nil(()))
            }
        }
    }
}

async fn create_table(stmt: &CreateTable, catalog: MetaDataRef) -> Result<(), ExecutionError> {
    let CreateTable {
        schema, name, tags, ..
    } = stmt;
    let mut kv_fields = BTreeMap::new();
    let mut get_time = false;
    for (i, tag) in tags.iter().enumerate() {
        let kv_field = TableFiled::new((i + 1) as u64, tag.clone(), ColumnType::Tag, 0);
        kv_fields.insert(tag.clone(), kv_field);
    }

    for (i, field) in schema.fields().iter().enumerate() {
        let field_name = field.name();
        let id = if field_name == TIME_FIELD {
            get_time = true;
            0
        } else if get_time == false {
            kv_fields.len() + i + 1
        } else {
            kv_fields.len() + i
        };
        let kv_field = TableFiled::new(
            id as u64,
            field_name.clone(),
            data_type_to_column_type(field.data_type()),
            codec_name_to_codec(
                schema
                    .metadata()
                    .get(field_name)
                    .unwrap_or(&"DEFAULT".to_string()),
            ),
        );
        kv_fields.insert(field_name.clone(), kv_field);
    }

    let table_schema = TableSchema::new(catalog.schema_name(), name.clone(), kv_fields);
    catalog
        .create_table(name, Arc::new(table_schema))
        .context(execution::MetadataSnafu)?;

    Ok(())
}

fn data_type_to_column_type(data_type: &DataType) -> ColumnType {
    match data_type {
        DataType::Timestamp(TimeUnit::Nanosecond, None) => ColumnType::Time,
        DataType::Int64 => ColumnType::Field(ValueType::Integer),
        DataType::UInt64 => ColumnType::Field(ValueType::Unsigned),
        DataType::Float64 => ColumnType::Field(ValueType::Float),
        DataType::Utf8 => ColumnType::Field(ValueType::String),
        DataType::Boolean => ColumnType::Field(ValueType::Boolean),
        _ => ColumnType::Field(ValueType::Unknown),
    }
}

fn codec_name_to_codec(codec_name: &str) -> u8 {
    match codec_name {
        "DEFAULT" => 0,
        "NULL" => 1,
        "DELTA" => 2,
        "QUANTILE" => 3,
        "GZIP" => 4,
        "BZIP" => 5,
        "GORILLA" => 6,
        "SNAPPY" => 7,
        "ZSTD" => 8,
        "ZLIB" => 9,
        "BITPACK" => 10,
        _ => 15,
    }
}
