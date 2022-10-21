use std::sync::Arc;

use datafusion::error::DataFusionError;
use datafusion::logical_expr::TableSource;
use datafusion::logical_plan::plan::{Analyze, Explain, Extension};
use datafusion::logical_plan::{
    DFField, FileType, LogicalPlan, LogicalPlanBuilder, PlanType, ToDFSchema, ToStringifiedPlan,
};
use datafusion::prelude::{cast, col, lit, Expr};
use datafusion::scalar::ScalarValue;
use datafusion::sql::parser::CreateExternalTable as AstCreateExternalTable;
use datafusion::sql::planner::{ContextProvider, SqlToRel};
use datafusion::sql::TableReference;
use hashbrown::HashMap;
use models::schema::{ColumnType, TableColumn};
use models::utils::SeqIdGenerator;
use models::{ColumnId, ValueType};
use snafu::ResultExt;
use spi::query::ast::{
    ColumnOption, CreateDatabase as ASTCreateDatabase, CreateTable as ASTCreateTable,
    DatabaseOptions as ASTDatabaseOptions, DescribeDatabase as DescribeDatabaseOptions,
    DescribeTable as DescribeTableOptions, DropObject, ExtStatement,
};
use spi::query::logical_planner::{
    self, affected_row_expr, CSVOptions, CreateDatabase, CreateExternalTable, CreateTable, DDLPlan,
    DescribeDatabase, DescribeTable, DropPlan, ExternalSnafu, FileDescriptor, LogicalPlanner,
    LogicalPlannerError, Plan, QueryPlan, MISMATCHED_COLUMNS, MISSING_COLUMN,
};
use spi::query::session::IsiphoSessionCtx;
use sqlparser::ast::{Ident, ObjectName, Query};

use models::codec::{
    codec_name_to_codec, BIGINT_CODEC, BOOLEAN_CODEC, DOUBLE_CODEC, STRING_CODEC, TIMESTAMP_CODEC,
    UNSIGNED_BIGINT_CODEC,
};
use models::schema::{DatabaseOptions, Duration, Precision};
use spi::query::logical_planner::Result;
use spi::query::UNEXPECTED_EXTERNAL_PLAN;
use sqlparser::ast::{DataType as SQLDataType, Statement};
use trace::debug;

use crate::extension::logical::plan_node::table_writer::TableWriterPlanNode;

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
            ExtStatement::CreateDatabase(stmt) => self.database_to_plan(stmt),
            ExtStatement::CreateUser(_) => todo!(),
            ExtStatement::Drop(s) => self.drop_object_to_plan(s),
            ExtStatement::DropUser(_) => todo!(),
            ExtStatement::DescribeTable(stmt) => self.table_to_describe(stmt),
            ExtStatement::DescribeDatabase(stmt) => self.database_to_describe(stmt),
            ExtStatement::ShowDatabases() => self.database_to_show(),
            ExtStatement::ShowTables(stmt) => self.table_to_show(stmt),
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
        let id_generator = SeqIdGenerator::default();
        // all col: time col, tag col, field col
        // sys inner time column
        let mut schema: Vec<TableColumn> = Vec::with_capacity(columns.len() + 1);

        let time_col = TableColumn::new_time_column(id_generator.next_id() as ColumnId);
        // Append time column at the start
        schema.push(time_col);

        for column_opt in columns {
            self.check_column(&column_opt)?;

            let col_id = id_generator.next_id() as ColumnId;

            let col = if column_opt.is_tag {
                TableColumn::new_tag_column(col_id, normalize_ident(&column_opt.name))
            } else {
                TableColumn::new(
                    col_id,
                    normalize_ident(&column_opt.name),
                    self.make_data_type(&column_opt.data_type)?,
                    codec_name_to_codec(&column_opt.codec),
                )
            };

            schema.push(col)
        }

        Ok(Plan::DDL(DDLPlan::CreateTable(CreateTable {
            schema,
            name,
            if_not_exists,
        })))
    }

    fn database_to_describe(&self, statement: DescribeDatabaseOptions) -> Result<Plan> {
        Ok(Plan::DDL(DDLPlan::DescribeDatabase(DescribeDatabase {
            database_name: statement.database_name,
        })))
    }

    fn table_to_describe(&self, opts: DescribeTableOptions) -> Result<Plan> {
        Ok(Plan::DDL(DDLPlan::DescribeTable(DescribeTable {
            table_name: opts.table_name,
        })))
    }

    fn database_to_show(&self) -> Result<Plan> {
        Ok(Plan::DDL(DDLPlan::ShowDatabase()))
    }

    fn table_to_show(&self, database: String) -> Result<Plan> {
        Ok(Plan::DDL(DDLPlan::ShowTable(database)))
    }

    fn database_to_plan(&self, stmt: ASTCreateDatabase) -> Result<Plan> {
        let ASTCreateDatabase {
            name,
            if_not_exists,
            options,
        } = stmt;
        let options = self.make_database_option(options)?;
        Ok(Plan::DDL(DDLPlan::CreateDatabase(CreateDatabase {
            name,
            if_not_exists,
            options,
        })))
    }

    fn make_database_option(&self, options: ASTDatabaseOptions) -> Result<DatabaseOptions> {
        let mut plan_options = DatabaseOptions::default();
        if let Some(ttl) = options.ttl {
            plan_options.ttl = self.str_to_duration(&ttl)?;
        }
        if let Some(replica) = options.replica {
            plan_options.replica = replica
        }
        if let Some(shard_num) = options.shard_num {
            plan_options.shard_num = shard_num
        }
        if let Some(vnode_duration) = options.vnode_duration {
            plan_options.vnode_duration = self.str_to_duration(&vnode_duration)?
        }
        if let Some(precision) = options.precision {
            plan_options.precision = match Precision::new(&precision) {
                None => {
                    return Err(LogicalPlannerError::Semantic {
                        err: format!(
                            "{} is not a valid precision, use like 'ms', 'us', 'ns'",
                            precision
                        ),
                    })
                }
                Some(v) => v,
            }
        }
        Ok(plan_options)
    }

    fn str_to_duration(&self, text: &str) -> Result<Duration> {
        let duration = match Duration::new(text) {
            None => {
                return Err(LogicalPlannerError::Semantic {
                    err: format!(
                        "{} is not a valid precision, use like 'ms', 'us', 'ns'",
                        text
                    ),
                })
            }
            Some(v) => v,
        };
        Ok(duration)
    }

    fn make_data_type(&self, data_type: &SQLDataType) -> Result<ColumnType> {
        match data_type {
            // todo : should support get time unit for database
            SQLDataType::Timestamp => Ok(ColumnType::Time),
            SQLDataType::BigInt(_) => Ok(ColumnType::Field(ValueType::Integer)),
            SQLDataType::UnsignedBigInt(_) => Ok(ColumnType::Field(ValueType::Unsigned)),
            SQLDataType::Double => Ok(ColumnType::Field(ValueType::Float)),
            SQLDataType::String => Ok(ColumnType::Field(ValueType::String)),
            SQLDataType::Boolean => Ok(ColumnType::Field(ValueType::Boolean)),
            _ => Err(LogicalPlannerError::Semantic {
                err: format!("Unexpected data type {}", data_type),
            }),
        }
    }

    fn check_column(&self, column: &ColumnOption) -> Result<()> {
        // tag无压缩，直接返回
        if column.is_tag {
            return Ok(());
        }
        // 数据类型 -> 压缩方式 校验
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
            return Err(LogicalPlannerError::Semantic {
                err: format!(
                    "Unsupported codec type {} for {}",
                    column.codec, column.data_type
                ),
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
        let expected = "DDL(CreateTable(CreateTable { schema: [TableColumn { id: 0, name: \"time\", column_type: Time, codec: 0 }, TableColumn { id: 1, name: \"column6\", column_type: Tag, codec: 0 }, TableColumn { id: 2, name: \"column7\", column_type: Tag, codec: 0 }, TableColumn { id: 3, name: \"column1\", column_type: Field(Integer), codec: 2 }, TableColumn { id: 4, name: \"column2\", column_type: Field(String), codec: 4 }, TableColumn { id: 5, name: \"column3\", column_type: Field(Unsigned), codec: 1 }, TableColumn { id: 6, name: \"column4\", column_type: Field(Boolean), codec: 0 }, TableColumn { id: 7, name: \"column5\", column_type: Field(Float), codec: 6 }], name: \"test\", if_not_exists: true }))";
        assert_eq!(ans, expected);
    }

    #[test]
    fn test_create_database() {
        let sql = "CREATE DATABASE test WITH TTL '10' SHARD 5 VNODE_DURATION '3d' REPLICA 10 PRECISION 'us';";
        let mut statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        let test = MockContext {};
        let planner = SqlPlaner::new(test);
        let plan = planner
            .statement_to_plan(statements.pop_back().unwrap())
            .unwrap();
        let ans = format!("{:?}", plan);
        let expected = r#"DDL(CreateDatabase(CreateDatabase { name: "test", if_not_exists: false, options: DatabaseOptions { ttl: Duration { time_num: 10, unit: Day }, shard_num: 5, vnode_duration: Duration { time_num: 3, unit: Day }, replica: 10, precision: US } }))"#;
        assert_eq!(ans, expected);
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
