use datafusion::sql::planner::{ContextProvider, SqlToRel};
use snafu::ResultExt;
use spi::query::ast::{DropObject, ExtStatement};
use spi::query::logical_planner::{DropPlan, LogicalPlanner, Plan, QueryPlan};
use spi::query::session::IsiphoSessionCtx;
use sqlparser::ast::Statement;

use spi::query::LogicalPlannerSnafu;
use spi::query::Result;

/// CnosDB SQL query planner
#[derive(Debug)]
pub struct SqlPlaner<S> {
    schema_provider: S,
}

impl<S: ContextProvider> SqlPlaner<S> {
    /// Create a new query planner
    pub fn new(schema_provider: S) -> Self {
        SqlPlaner { schema_provider }
    }

    /// Generate a logical plan from an  Extent SQL statement
    fn statement_to_plan(&self, statement: ExtStatement) -> Result<Plan> {
        match statement {
            ExtStatement::SqlStatement(stmt) => self.df_sql_to_plan(*stmt),
            ExtStatement::CreateTable(_) => todo!(),
            ExtStatement::CreateDatabase(_) => todo!(),
            ExtStatement::CreateUser(_) => todo!(),
            ExtStatement::Drop(s) => self.drop_object_to_plan(s),
            ExtStatement::DropUser(_) => todo!(),
            ExtStatement::DescribeTable(_) => todo!(),
            ExtStatement::DescribeDatabase(_) => todo!(),
            ExtStatement::ShowDatabases => todo!(),
            ExtStatement::ShowTables => todo!(),
        }
    }

    fn df_sql_to_plan(&self, stmt: Statement) -> Result<Plan> {
        match stmt {
            Statement::Query(_) => {
                let df_planner = SqlToRel::new(&self.schema_provider);
                let df_plan = df_planner
                    .sql_statement_to_plan(stmt)
                    .context(LogicalPlannerSnafu)?;
                Ok(Plan::Query(QueryPlan { df_plan }))
            }
            _ => {
                unimplemented!()
            }
        }
    }

    fn drop_object_to_plan(&self, stmt: DropObject) -> Result<Plan> {
        Ok(Plan::Drop(DropPlan {
            if_exist: stmt.if_exist,
            object_name: stmt.object_name,
            obj_type: stmt.obj_type,
        }))
    }
}

impl<S: ContextProvider> LogicalPlanner for SqlPlaner<S> {
    fn create_logical_plan(
        &self,
        statement: ExtStatement,
        _session: &IsiphoSessionCtx,
    ) -> Result<Plan> {
        self.statement_to_plan(statement)
    }
}

#[cfg(test)]
mod tests {
    use crate::extension::datafusion::parser::ExtParser;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::logical_expr::{AggregateUDF, ScalarUDF, TableSource};
    use datafusion::sql::planner::ContextProvider;
    use datafusion::sql::TableReference;
    use std::any::Any;
    use std::sync::Arc;

    use super::*;
    use datafusion::error::Result;

    #[derive(Debug)]
    struct MockContext {}

    impl ContextProvider for MockContext {
        fn get_table_provider(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
            let schema = match name.table() {
                "test_tb" => Ok(Schema::new(vec![
                    Field::new("field_int", DataType::Int32, false),
                    Field::new("field_string", DataType::Utf8, false),
                ])),
                _ => {
                    unimplemented!("use test_tb for test")
                }
            };
            match schema {
                Ok(tb) => Ok(Arc::new(TestTable::new(Arc::new(tb)))),
                Err(e) => Err(e),
            }
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
    struct TestTable {
        table_schema: SchemaRef,
    }

    impl TestTable {
        fn new(table_schema: SchemaRef) -> Self {
            Self { table_schema }
        }
    }

    impl TableSource for TestTable {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            self.table_schema.clone()
        }
    }
    #[test]
    fn test_select() {
        let sql = "select * from test_tb";
        let mut statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        let test = MockContext {};
        let planner = SqlPlaner::new(test);
        let plan = planner
            .statement_to_plan(statements.pop_back().unwrap())
            .unwrap();
        println!("{:?}", plan);
    }

    #[test]
    fn test_drop() {
        let sql = "drop table if exists test_tb";
        let mut statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        let test = MockContext {};
        let planner = SqlPlaner::new(test);
        let plan = planner
            .statement_to_plan(statements.pop_back().unwrap())
            .unwrap();
        println!("{:?}", plan);
    }
}
