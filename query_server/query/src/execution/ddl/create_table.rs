use crate::execution::ddl::DDLDefinitionTask;
use async_trait::async_trait;
use datafusion::sql::TableReference;
use models::schema::TableSchema;
use snafu::ResultExt;
use spi::catalog::{MetaDataRef, MetadataError};
use spi::query::execution;
use spi::query::execution::{ExecutionError, Output, QueryStateMachineRef};
use spi::query::logical_planner::CreateTable;
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
        query_state_machine: QueryStateMachineRef,
    ) -> Result<Output, ExecutionError> {
        let CreateTable {
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
                create_table(&self.stmt, query_state_machine.catalog.clone())?;
                Ok(Output::Nil(()))
            }
        }
    }
}

fn create_table(
    stmt: &CreateTable,
    catalog: MetaDataRef,
) -> Result<Arc<TableSchema>, ExecutionError> {
    let CreateTable { name, .. } = stmt;
    let table_schema = Arc::new(build_schema(stmt, catalog.clone()));
    catalog
        .create_table(name, table_schema.clone())
        .context(execution::MetadataSnafu)?;

    Ok(table_schema)
}

fn build_schema(stmt: &CreateTable, catalog: MetaDataRef) -> TableSchema {
    let CreateTable { schema, name, .. } = stmt;

    let table: TableReference = name.as_str().into();
    let catalog_name = catalog.catalog_name();
    let schema_name = catalog.schema_name();
    let table_ref = table.resolve(&catalog_name, &schema_name);

    TableSchema::new(
        table_ref.schema.to_string(),
        table.table().to_string(),
        schema.to_owned(),
    )
}

#[cfg(test)]
mod test {
    use crate::execution::ddl::create_table::create_table;
    use crate::extension::expr::load_all_functions;
    use crate::function::simple_func_manager::SimpleFunctionMetadataManager;
    use crate::metadata::LocalCatalogMeta;
    use crate::sql::parser::ExtParser;
    use crate::sql::planner::SqlPlaner;
    use config::get_config;
    use coordinator::meta_client_mock::MockMetaManager;
    use coordinator::service::MockCoordinator;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::error::DataFusionError;
    use datafusion::logical_expr::{AggregateUDF, ScalarUDF, TableSource};
    use datafusion::sql::planner::ContextProvider;
    use datafusion::sql::TableReference;
    use spi::query::execution::QueryStateMachine;
    use spi::query::logical_planner::{DDLPlan, Plan};
    use spi::query::session::IsiphoSessionCtxFactory;
    use spi::service::protocol::{ContextBuilder, Query, UserInfo};
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
        let meta = Arc::new(
            LocalCatalogMeta::new_with_default(
                tskv.clone(),
                Arc::new(MockCoordinator::default()),
                Arc::new(function_manager),
            )
            .unwrap(),
        );
        let context = ContextBuilder::new(UserInfo {
            user: "".to_string(),
            password: "".to_string(),
        })
        .with_database(Some("public".to_string()))
        .build();
        let query = Query::new(context, "1".to_string());
        let factory = IsiphoSessionCtxFactory::default();
        let session = factory.create_isipho_session_ctx(query.context().clone());
        let query_state_machine = Arc::new(QueryStateMachine::begin(query, session, meta));
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
            DDLPlan::CreateTable(plan) => plan,
            _ => panic!("not possible"),
        };

        create_table(&plan, query_state_machine.catalog.clone()).unwrap();
        let table = tskv
            .get_table_schema(&query_state_machine.catalog.schema_name(), "test")
            .unwrap()
            .unwrap();
        let ans = format!("{:?}, {:?}", table.name, table.columns(),);

        let expected = "\"test\", [TableColumn { id: 0, name: \"time\", column_type: Time, codec: 0 }, TableColumn { id: 1, name: \"column6\", column_type: Tag, codec: 0 }, TableColumn { id: 2, name: \"column7\", column_type: Tag, codec: 0 }, TableColumn { id: 3, name: \"column1\", column_type: Field(Integer), codec: 2 }, TableColumn { id: 4, name: \"column2\", column_type: Field(String), codec: 4 }, TableColumn { id: 5, name: \"column3\", column_type: Field(Unsigned), codec: 1 }, TableColumn { id: 6, name: \"column4\", column_type: Field(Boolean), codec: 0 }, TableColumn { id: 7, name: \"column5\", column_type: Field(Float), codec: 6 }]";
        assert_eq!(ans, expected);
    }
}
