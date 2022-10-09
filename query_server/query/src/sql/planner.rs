use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::TableSource;
use datafusion::logical_plan::plan::{Analyze, Explain, Extension};
use datafusion::logical_plan::{
    DFField, FileType, LogicalPlan, LogicalPlanBuilder, PlanType, ToDFSchema, ToStringifiedPlan,
};
use datafusion::logical_plan::{DFSchema, DFSchemaRef};
use datafusion::prelude::{cast, col, lit, Expr};
use datafusion::scalar::ScalarValue;
use datafusion::sql::parser::CreateExternalTable as AstCreateExternalTable;
use datafusion::sql::planner::{ContextProvider, SqlToRel};
use datafusion::sql::TableReference;
use hashbrown::HashMap;
use snafu::ResultExt;
use spi::query::ast::{ColumnOption, CreateTable as ASTCreateTable, DropObject, ExtStatement};
use spi::query::logical_planner::{
    self, affected_row_expr, CSVOptions, CreateExternalTable, CreateTable, DDLPlan, DropPlan,
    ExternalSnafu, FileDescriptor, LogicalPlanner, LogicalPlannerError, Plan, QueryPlan,
    MISMATCHED_COLUMNS, MISSING_COLUMN,
};
use spi::query::session::IsiphoSessionCtx;
use sqlparser::ast::{Ident, ObjectName, Query};
use std::collections::HashMap as MetaDataHashmap;

use spi::query::logical_planner::Result;
use spi::query::UNEXPECTED_EXTERNAL_PLAN;
use sqlparser::ast::{DataType as SQLDataType, Statement};
use trace::debug;

use crate::extension::logical::plan_node::table_writer::TableWriterPlanNode;

const TIMESTAMP_CODEC: [&str; 4] = ["DEFAULT", "NULL", "DELTA", "QUANTILE"];
const BIGINT_CODEC: [&str; 4] = ["DEFAULT", "NULL", "DELTA", "QUANTILE"];
const UNSIGNED_BIGINT_CODEC: [&str; 4] = ["DEFAULT", "NULL", "DELTA", "QUANTILE"];
const DOUBLE_CODEC: [&str; 4] = ["DEFAULT", "NULL", "GORILLA", "QUANTILE"];
const STRING_CODEC: [&str; 7] = ["DEFAULT", "NULL", "GZIP", "BZIP", "ZSTD", "SNAPPY", "ZLIB"];
const BOOLEAN_CODEC: [&str; 3] = ["DEFAULT", "NULL", "BITPACK"];

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
    pub(crate) fn statement_to_plan(&self, statement: ExtStatement) -> Result<Plan> {
        match statement {
            ExtStatement::SqlStatement(stmt) => self.df_sql_to_plan(*stmt),
            ExtStatement::CreateExternalTable(stmt) => self.external_table_to_plan(stmt),
            ExtStatement::CreateTable(stmt) => self.table_to_plan(stmt),
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
                    .context(ExternalSnafu)?;
                Ok(Plan::Query(QueryPlan { df_plan }))
            }
            Statement::Explain {
                verbose,
                statement,
                analyze,
                describe_alias: _,
            } => self.explain_statement_to_plan(verbose, analyze, *statement),
            Statement::Insert {
                table_name: ref sql_object_name,
                columns: ref sql_column_names,
                source,
                ..
            } => self.insert_to_plan(sql_object_name, sql_column_names, source),
            _ => Err(LogicalPlannerError::NotImplemented {
                err: stmt.to_string(),
            }),
        }
    }

    /// Generate a plan for EXPLAIN ... that will print out a plan
    ///
    pub fn explain_statement_to_plan(
        &self,
        verbose: bool,
        analyze: bool,
        statement: Statement,
    ) -> Result<Plan> {
        let plan = self.df_sql_to_plan(statement)?;

        let input_df_plan = match plan {
            Plan::Query(query) => Arc::new(query.df_plan),
            _ => {
                return Err(LogicalPlannerError::NotImplemented {
                    err: "explain non-query statement.".to_string(),
                })
            }
        };

        let schema = LogicalPlan::explain_schema()
            .to_dfschema_ref()
            .context(ExternalSnafu)?;

        let df_plan = if analyze {
            LogicalPlan::Analyze(Analyze {
                verbose,
                input: input_df_plan,
                schema,
            })
        } else {
            let stringified_plans =
                vec![input_df_plan.to_stringified(PlanType::InitialLogicalPlan)];
            LogicalPlan::Explain(Explain {
                verbose,
                plan: input_df_plan,
                stringified_plans,
                schema,
            })
        };

        Ok(Plan::Query(QueryPlan { df_plan }))
    }

    /// Add a projection operation (if necessary)
    /// 1. Iterate over all fields of the table
    ///   1.1. Construct the col expression
    ///   1.2. Check if the current field exists in columns
    ///     1.2.1. does not exist: add cast(null as target_type) expression to save
    ///     1.2.1. Exist: save if the type matches, add cast(expr as target_type) to save if it does not exist
    fn add_projection_between_source_and_insert_node_if_necessary(
        &self,
        target_table: Arc<dyn TableSource>,
        source_plan: LogicalPlan,
        insert_columns: Vec<String>,
    ) -> Result<LogicalPlan> {
        let insert_col_name_with_source_field_tuples: Vec<(&String, &DFField)> = insert_columns
            .iter()
            .zip(source_plan.schema().fields())
            .collect();

        debug!(
            "Insert col name with source field tuples: {:?}",
            insert_col_name_with_source_field_tuples
        );
        debug!("Target table: {:?}", target_table.schema());

        let assignments: Vec<Expr> = target_table
            .schema()
            .fields()
            .iter()
            .map(|column| {
                let target_column_name = column.name();
                let target_column_data_type = column.data_type();

                let expr = if let Some((_, source_field)) = insert_col_name_with_source_field_tuples
                    .iter()
                    .find(|(insert_col_name, _)| *insert_col_name == target_column_name)
                {
                    // insert column exists in the target table
                    if source_field.data_type() == target_column_data_type {
                        // save if type matches col(source_field_name)
                        col(source_field.name())
                    } else {
                        // Add cast(source_col as target_type) if it doesn't exist
                        cast(col(source_field.name()), target_column_data_type.clone())
                    }
                } else {
                    // The specified column in the target table is missing from the insert
                    // then add cast(null as target_type)
                    cast(lit(ScalarValue::Null), target_column_data_type.clone())
                };

                expr.alias(target_column_name)
            })
            .collect();

        LogicalPlanBuilder::from(source_plan)
            .project_with_alias(assignments, None)
            .context(logical_planner::ExternalSnafu)?
            .build()
            .context(logical_planner::ExternalSnafu)
    }

    fn insert_to_plan(
        &self,
        sql_object_name: &ObjectName,
        sql_column_names: &[Ident],
        source: Box<Query>,
    ) -> Result<Plan> {
        // Transform subqueries
        let source_plan = SqlToRel::new(&self.schema_provider)
            .query_to_plan(*source, &mut HashMap::new())
            .context(logical_planner::ExternalSnafu)?;

        let table_name = normalize_sql_object_name(sql_object_name);
        let columns = sql_column_names
            .iter()
            .map(normalize_ident)
            .collect::<Vec<String>>();

        // Get the metadata of the target table
        let target_table = self.get_table_metadata(table_name.to_string())?;
        let insert_columns = self.extract_column_names(columns.as_ref(), target_table.clone());

        // Check if the plan is legal
        semantic_check(insert_columns.as_ref(), &source_plan, target_table.clone())?;

        let final_source_logical_plan = self
            .add_projection_between_source_and_insert_node_if_necessary(
                target_table.clone(),
                source_plan,
                insert_columns,
            )?;

        // output variable for insert operation
        let affected_row_expr = affected_row_expr();

        // construct table writer logical node
        let node = Arc::new(TableWriterPlanNode::new(
            table_name,
            target_table,
            Arc::new(final_source_logical_plan),
            vec![affected_row_expr],
        ));

        Ok(Plan::Query(QueryPlan {
            df_plan: LogicalPlan::Extension(Extension { node }),
        }))
    }

    fn drop_object_to_plan(&self, stmt: DropObject) -> Result<Plan> {
        Ok(Plan::DDL(DDLPlan::Drop(DropPlan {
            if_exist: stmt.if_exist,
            object_name: stmt.object_name,
            obj_type: stmt.obj_type,
        })))
    }

    /// Generate a logical plan from a CREATE EXTERNAL TABLE statement
    pub fn external_table_to_plan(&self, statement: AstCreateExternalTable) -> Result<Plan> {
        let df_planner = SqlToRel::new(&self.schema_provider);

        let logical_plan = df_planner
            .external_table_to_plan(statement)
            .context(ExternalSnafu)?;

        if let LogicalPlan::CreateExternalTable(datafusion::logical_plan::CreateExternalTable {
            schema,
            name,
            location,
            file_type,
            has_header,
            delimiter,
            table_partition_cols,
            if_not_exists,
        }) = logical_plan
        {
            let file_descriptor = match file_type {
                FileType::NdJson => FileDescriptor::NdJson,
                FileType::Parquet => FileDescriptor::Parquet,
                FileType::CSV => FileDescriptor::CSV(CSVOptions {
                    has_header,
                    delimiter,
                }),
                FileType::Avro => FileDescriptor::Avro,
            };

            return Ok(Plan::DDL(DDLPlan::CreateExternalTable(
                CreateExternalTable {
                    schema,
                    name,
                    location,
                    file_descriptor,
                    table_partition_cols,
                    if_not_exists,
                },
            )));
        }

        Err(DataFusionError::Internal(
            UNEXPECTED_EXTERNAL_PLAN.to_string(),
        ))
        .context(ExternalSnafu)
    }

    pub fn table_to_plan(&self, statement: ASTCreateTable) -> Result<Plan> {
        let ASTCreateTable {
            name,
            if_not_exists,
            columns,
        } = statement;
        let mut fields = vec![];
        let mut tags_name = vec![];
        let mut field_name_codec_algo = MetaDataHashmap::new();
        for column in columns {
            if column.is_tag {
                tags_name.push(column.name.value);
            } else {
                fields.push(DFField::new(
                    Some(&name),
                    &column.name.value,
                    self.make_data_type(&column.data_type)?,
                    true,
                ));
                if field_name_codec_algo.get(&column.name.value) != None {
                    return Err(LogicalPlannerError::External {
                        source: DataFusionError::Internal(
                            "column name should different from each other".to_string(),
                        ),
                    });
                }
                self.check_column(&column)?;
                field_name_codec_algo.insert(column.name.value, column.codec);
            }
        }

        let table_schema = DFSchemaRef::new(
            DFSchema::new_with_metadata(fields, field_name_codec_algo).context(ExternalSnafu)?,
        );

        Ok(Plan::DDL(DDLPlan::CreateTable(CreateTable {
            schema: table_schema,
            name,
            if_not_exists,
            tags: tags_name,
        })))
    }

    fn make_data_type(&self, data_type: &SQLDataType) -> Result<DataType> {
        match data_type {
            // todo : should support get time unit for database
            SQLDataType::Timestamp => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
            SQLDataType::BigInt(_) => Ok(DataType::Int64),
            SQLDataType::UnsignedBigInt(_) => Ok(DataType::UInt64),
            SQLDataType::Double => Ok(DataType::Float64),
            SQLDataType::String => Ok(DataType::Utf8),
            SQLDataType::Boolean => Ok(DataType::Boolean),
            _ => Err(LogicalPlannerError::External {
                source: DataFusionError::Internal(format!("Unexpected data type {}", data_type)),
            }),
        }
    }

    fn check_column(&self, column: &ColumnOption) -> Result<()> {
        let is_ok = match column.data_type {
            SQLDataType::Timestamp => {
                TIMESTAMP_CODEC.contains(&column.codec.to_uppercase().as_str())
            }
            SQLDataType::BigInt(_) => BIGINT_CODEC.contains(&column.codec.to_uppercase().as_str()),
            SQLDataType::UnsignedBigInt(_) => {
                UNSIGNED_BIGINT_CODEC.contains(&column.codec.to_uppercase().as_str())
            }
            SQLDataType::Double => DOUBLE_CODEC.contains(&column.codec.to_uppercase().as_str()),
            SQLDataType::String => STRING_CODEC.contains(&column.codec.to_uppercase().as_str()),
            SQLDataType::Boolean => BOOLEAN_CODEC.contains(&column.codec.to_uppercase().as_str()),
            _ => false,
        };
        if !is_ok {
            return Err(LogicalPlannerError::External {
                source: DataFusionError::Internal(format!(
                    "Unsupported codec type {} for {}",
                    column.codec, column.data_type
                )),
            });
        }
        Ok(())
    }

    fn extract_column_names(
        &self,
        columns: &[String],
        target_table_source_ref: Arc<dyn TableSource>,
    ) -> Vec<String> {
        if columns.is_empty() {
            target_table_source_ref
                .schema()
                .fields()
                .iter()
                .map(|e| e.name().clone())
                .collect()
        } else {
            columns.to_vec()
        }
    }

    fn get_table_metadata(&self, table_name: String) -> Result<Arc<dyn TableSource>> {
        let table_ref = TableReference::from(table_name.as_str());
        self.schema_provider
            .get_table_provider(table_ref)
            .context(logical_planner::ExternalSnafu)
    }
}

fn semantic_check(
    insert_columns: &[String],
    source_plan: &LogicalPlan,
    target_table: Arc<dyn TableSource>,
) -> Result<()> {
    let target_table_schema = target_table.schema();
    let target_table_fields = target_table_schema.fields();

    let source_field_num = source_plan.schema().fields().len();
    let insert_field_num = insert_columns.len();
    let target_table_field_num = target_table_fields.len();

    if insert_field_num > source_field_num {
        return Err(LogicalPlannerError::Semantic {
            err: MISMATCHED_COLUMNS.to_string(),
        });
    }

    if insert_field_num == 0 && source_field_num != target_table_field_num {
        return Err(LogicalPlannerError::Semantic {
            err: MISMATCHED_COLUMNS.to_string(),
        });
    }
    // The target table must contain all insert fields
    for insert_col in insert_columns {
        target_table_fields
            .iter()
            .find(|e| e.name() == insert_col)
            .ok_or_else(|| LogicalPlannerError::Semantic {
                err: format!(
                    "{} {}, expected: {}",
                    MISSING_COLUMN,
                    insert_col,
                    target_table_fields
                        .iter()
                        .map(|e| e.name().as_str())
                        .collect::<Vec<&str>>()
                        .join(",")
                ),
            })?;
    }

    Ok(())
}

/// Normalize a SQL object name
fn normalize_sql_object_name(sql_object_name: &ObjectName) -> String {
    sql_object_name
        .0
        .iter()
        .map(normalize_ident)
        .collect::<Vec<String>>()
        .join(".")
}

// Normalize an identifier to a lowercase string unless the identifier is quoted.
pub fn normalize_ident(id: &Ident) -> String {
    match id.quote_style {
        Some(_) => id.value.clone(),
        None => id.value.to_ascii_lowercase(),
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
    use crate::sql::parser::ExtParser;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::logical_expr::{AggregateUDF, ScalarUDF, TableSource};
    use datafusion::sql::planner::ContextProvider;
    use datafusion::sql::TableReference;
    use std::any::Any;
    use std::ops::Deref;
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

    #[test]
    fn test_create_table() {
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
        let ans = format!("{:?}", plan);
        let expected = r#"DDL(CreateTable(CreateTable { schema: DFSchema { fields: [DFField { qualifier: Some("test"), field: Field { name: "time", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: None } }, DFField { qualifier: Some("test"), field: Field { name: "column1", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: None } }, DFField { qualifier: Some("test"), field: Field { name: "column2", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: None } }, DFField { qualifier: Some("test"), field: Field { name: "column3", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: None } }, DFField { qualifier: Some("test"), field: Field { name: "column4", data_type: Boolean, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: None } }, DFField { qualifier: Some("test"), field: Field { name: "column5", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: None } }], metadata: {"column5": "GORILLA", "column3": "NULL", "column4": "DEFAULT", "column2": "GZIP", "column1": "DELTA", "time": "DEFAULT"} }, name: "test", if_not_exists: true, tags: ["column6", "column7"] }))"#;
        assert_eq!(ans[0..1022], expected[0..1022]);
        assert_eq!(ans[1186..1223], expected[1186..1223]);
    }

    #[test]
    fn test_insert_select() {
        let sql = "insert test_tb(field_int, field_string)
                         select column1, column2
                         from
                         (values
                             (7, '7a'));";
        let mut statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        let test = MockContext {};
        let planner = SqlPlaner::new(test);
        let plan = planner
            .statement_to_plan(statements.pop_back().unwrap())
            .unwrap();

        match plan {
            Plan::Query(QueryPlan {
                df_plan: LogicalPlan::Extension(Extension { node }),
            }) => match node.as_any().downcast_ref::<TableWriterPlanNode>() {
                Some(TableWriterPlanNode {
                    target_table_name, ..
                }) => {
                    assert_eq!(target_table_name.deref(), "test_tb");
                }
                _ => panic!(),
            },
            _ => panic!(),
        }
    }
}
