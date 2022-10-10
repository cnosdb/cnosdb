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
                create_table(&self.stmt, catalog)?;
                Ok(Output::Nil(()))
            }
        }
    }
}

fn create_table(
    stmt: &CreateTable,
    catalog: MetaDataRef,
) -> Result<Arc<TableSchema>, ExecutionError> {
    let CreateTable {
        schema, name, tags, ..
    } = stmt;
    let mut kv_fields = BTreeMap::new();
    let mut get_time = false;
    for (i, tag) in tags.iter().enumerate() {
        let kv_field = TableFiled::new((i + 1) as u64, tag.clone(), ColumnType::Tag, 0);
        kv_fields.insert(tag.clone(), kv_field);
    }

    let start = kv_fields.len();
    for (i, field) in schema.fields().iter().enumerate() {
        let field_name = field.name();
        let id = if field_name == TIME_FIELD {
            get_time = true;
            0
        } else if !get_time {
            start + i + 1
        } else {
            start + i
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
    let table_schema = Arc::new(table_schema);
    catalog
        .create_table(name, table_schema.clone())
        .context(execution::MetadataSnafu)?;

    Ok(table_schema)
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

#[cfg(test)]
mod test {
    use crate::execution::ddl::create_table::create_table;
    use crate::extension::expr::load_all_functions;
    use crate::function::simple_func_manager::SimpleFunctionMetadataManager;
    use crate::metadata::{LocalCatalogMeta, MetaData};
    use crate::sql::parser::ExtParser;
    use crate::sql::planner::SqlPlaner;
    use config::get_config;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::error::DataFusionError;
    use datafusion::logical_expr::{AggregateUDF, ScalarUDF, TableSource};
    use datafusion::sql::planner::ContextProvider;
    use datafusion::sql::TableReference;
    use spi::query::logical_planner::{DDLPlan, Plan};
    use std::result;
    use std::sync::Arc;
    use tokio::runtime;
    use tskv::engine::Engine;
    use tskv::{kv_option, TsKv};

    pub type Result<T> = result::Result<T, DataFusionError>;

    #[derive(Debug)]
    struct MockContext {}

    impl ContextProvider for MockContext {
        fn get_table_provider(&self, _name: TableReference) -> Result<Arc<dyn TableSource>> {
            unimplemented!()
        }

        fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
            unimplemented!()
        }

        fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
            unimplemented!()
        }

        fn get_variable_type(&self, _: &[String]) -> Option<DataType> {
            unimplemented!()
        }
    }

    #[test]
    fn test_create_table() {
        let global_config = get_config("../../config/config.toml");
        let opt = kv_option::Options::from(&global_config);
        let rt = Arc::new(runtime::Runtime::new().unwrap());
        let (_rt, tskv) =
            rt.block_on(async { (rt.clone(), TsKv::open(opt, rt.clone()).await.unwrap()) });
        let tskv = Arc::new(tskv);
        let mut function_manager = SimpleFunctionMetadataManager::default();
        load_all_functions(&mut function_manager).unwrap();
        let meta = Arc::new(LocalCatalogMeta::new_with_default(
            tskv.clone(),
            Arc::new(function_manager),
        ));
        let sql = "CREATE TABLE IF NOT EXISTS test\
            (column1 BIGINT CODEC(DELTA),\
            column2 STRING CODEC(GZIP),\
            column3 BIGINT UNSIGNED CODEC(NULL),\
            column4 BOOLEAN,\
            column5 DOUBLE CODEC(GORILLA),\
            TAGS(column6, column7))";
        let mut statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        let test = MockContext {};
        let planner = SqlPlaner::new(test);
        let plan = planner
            .statement_to_plan(statements.pop_back().unwrap())
            .unwrap();
        let plan = match plan {
            Plan::Query(_) => panic!("not possible"),
            Plan::DDL(plan) => plan,
        };
        let plan = match plan {
            DDLPlan::Drop(_) => panic!("not possible"),
            DDLPlan::CreateExternalTable(_) => panic!("not possible"),
            DDLPlan::CreateTable(plan) => plan,
        };

        create_table(&plan, meta.clone()).unwrap();
        let ans = format!("{:?}", tskv.get_table_schema(&meta.schema_name(), "test"));
        let expected = r#"Ok(Some(TableSchema { db: "public", name: "test", schema_id: 0, fields: {"column1": TableFiled { id: 3, name: "column1", column_type: Field(Integer), codec: 2 }, "column2": TableFiled { id: 4, name: "column2", column_type: Field(String), codec: 4 }, "column3": TableFiled { id: 5, name: "column3", column_type: Field(Unsigned), codec: 1 }, "column4": TableFiled { id: 6, name: "column4", column_type: Field(Boolean), codec: 0 }, "column5": TableFiled { id: 7, name: "column5", column_type: Field(Float), codec: 6 }, "column6": TableFiled { id: 1, name: "column6", column_type: Tag, codec: 0 }, "column7": TableFiled { id: 2, name: "column7", column_type: Tag, codec: 0 }, "time": TableFiled { id: 0, name: "time", column_type: Time, codec: 0 }} }))"#;
        assert_eq!(ans, expected);
    }
}
