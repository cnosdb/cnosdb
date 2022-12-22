use datafusion::arrow::datatypes::DataType;
use std::collections::{HashMap, HashSet};
use std::iter;
use std::option::Option;
use std::sync::Arc;

use datafusion::common::{Column, DFField, DFSchema, ToDFSchema};
use datafusion::datasource::{source_as_provider, TableProvider};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::logical_plan::Analyze;
use datafusion::logical_expr::utils::expr_to_columns;
use datafusion::logical_expr::{
    cast, lit, BinaryExpr, BuiltinScalarFunction, Case, EmptyRelation, Explain, Expr, Extension,
    LogicalPlan, LogicalPlanBuilder, Operator, PlanType, Projection, TableSource,
    ToStringifiedPlan, Union,
};
use datafusion::scalar::ScalarValue;
use datafusion::sql::parser::CreateExternalTable as AstCreateExternalTable;
use datafusion::sql::planner::SqlToRel;
use datafusion::sql::sqlparser::ast::{
    DataType as SQLDataType, Expr as ASTExpr, Ident, ObjectName, Offset, OrderByExpr, Query,
    Statement,
};
use datafusion::sql::sqlparser::parser::ParserError;
use datafusion::sql::TableReference;
use meta::error::MetaError;
use models::auth::privilege::{
    DatabasePrivilege, GlobalPrivilege, Privilege, TenantObjectPrivilege,
};
use models::auth::role::{SystemTenantRole, TenantRoleIdentifier};
use models::object_reference::ObjectReference;
use models::oid::{Identifier, Oid};
use models::schema::{ColumnType, TableColumn, TskvTableSchema, TskvTableSchemaRef};
use models::utils::SeqIdGenerator;
use models::{ColumnId, ValueType};
use snafu::ResultExt;
use spi::query::ast::{
    AlterDatabase as ASTAlterDatabase, AlterTable as ASTAlterTable,
    AlterTableAction as ASTAlterTableAction, AlterTenantOperation, AlterUserOperation,
    ChecksumGroup as ASTChecksumGroup, ColumnOption, CompactVnode as ASTCompactVnode,
    CopyVnode as ASTCopyVnode, CreateDatabase as ASTCreateDatabase, CreateTable as ASTCreateTable,
    DatabaseOptions as ASTDatabaseOptions, DescribeDatabase as DescribeDatabaseOptions,
    DescribeTable as DescribeTableOptions, DropVnode as ASTDropVnode, ExtStatement,
    MoveVnode as ASTMoveVnode, ShowSeries as ASTShowSeries, ShowTagBody,
    ShowTagValues as ASTShowTagValues, With,
};
use spi::query::logical_planner::{
    affected_row_expr, merge_affected_row_expr, sql_options_to_tenant_options,
    sql_options_to_user_options, AlterDatabase, AlterTable, AlterTableAction, AlterTenant,
    AlterTenantAction, AlterTenantAddUser, AlterTenantSetUser, AlterUser, AlterUserAction,
    ChecksumGroup, CompactVnode, CopyVnode, CreateDatabase, CreateRole, CreateTable, CreateTenant,
    CreateUser, DDLPlan, DatabaseObjectType, DescribeDatabase, DescribeTable, DropDatabaseObject,
    DropGlobalObject, DropTenantObject, DropVnode, GlobalObjectType, GrantRevoke, LogicalPlanner,
    MoveVnode, Plan, PlanWithPrivileges, QueryPlan, SYSPlan, TenantObjectType,
};
use spi::query::session::IsiphoSessionCtx;
use spi::DatafusionSnafu;
use spi::QueryError;

use models::schema::{DatabaseOptions, Duration, Precision};
use spi::query::{ast, UNEXPECTED_EXTERNAL_PLAN};
use spi::Result;
use trace::{debug, warn};

use crate::extension::logical::plan_node::table_writer::TableWriterPlanNode;
use crate::metadata::{ContextProviderExtension, DatabaseSet, CLUSTER_SCHEMA, INFORMATION_SCHEMA};
use crate::sql::parser::{merge_object_name, normalize_ident, normalize_sql_object_name};
use crate::table::ClusterTable;

/// CnosDB SQL query planner
#[derive(Debug)]
pub struct SqlPlaner<S> {
    schema_provider: S,
}

impl<S: ContextProviderExtension> SqlPlaner<S> {
    /// Create a new query planner
    pub fn new(schema_provider: S) -> Self {
        SqlPlaner { schema_provider }
    }

    /// Generate a logical plan from an  Extent SQL statement
    pub(crate) fn statement_to_plan(
        &self,
        statement: ExtStatement,
        session: &IsiphoSessionCtx,
    ) -> Result<PlanWithPrivileges> {
        match statement {
            ExtStatement::SqlStatement(stmt) => self.df_sql_to_plan(*stmt, session),
            ExtStatement::CreateExternalTable(stmt) => self.external_table_to_plan(stmt, session),
            ExtStatement::CreateTable(stmt) => self.create_table_to_plan(stmt, session),
            ExtStatement::CreateDatabase(stmt) => self.database_to_plan(stmt, session),
            ExtStatement::CreateTenant(stmt) => self.create_tenant_to_plan(stmt),
            ExtStatement::CreateUser(stmt) => self.create_user_to_plan(stmt),
            ExtStatement::CreateRole(stmt) => self.create_role_to_plan(stmt, session),
            ExtStatement::DropDatabaseObject(s) => self.drop_database_object_to_plan(s, session),
            ExtStatement::DropTenantObject(s) => self.drop_tenant_object_to_plan(s, session),
            ExtStatement::DropGlobalObject(s) => self.drop_global_object_to_plan(s),
            ExtStatement::DescribeTable(stmt) => self.table_to_describe(stmt, session),
            ExtStatement::DescribeDatabase(stmt) => self.database_to_describe(stmt, session),
            ExtStatement::ShowDatabases() => self.database_to_show(session),
            ExtStatement::ShowTables(stmt) => self.table_to_show(stmt, session),
            ExtStatement::AlterDatabase(stmt) => self.database_to_alter(stmt, session),
            ExtStatement::ShowSeries(stmt) => self.show_series_to_plan(*stmt, session),
            ExtStatement::Explain(stmt) => self.explain_statement_to_plan(
                stmt.analyze,
                stmt.verbose,
                *stmt.ext_statement,
                session,
            ),
            ExtStatement::ShowTagValues(stmt) => self.show_tag_values(*stmt, session),
            ExtStatement::AlterTable(stmt) => self.table_to_alter(stmt, session),
            ExtStatement::AlterTenant(stmt) => self.alter_tenant_to_plan(stmt),
            ExtStatement::AlterUser(stmt) => self.alter_user_to_plan(stmt),
            ExtStatement::GrantRevoke(stmt) => self.grant_revoke_to_plan(stmt, session),
            // system statement
            ExtStatement::ShowQueries => {
                let plan = Plan::SYSTEM(SYSPlan::ShowQueries);
                // TODO privileges
                Ok(PlanWithPrivileges {
                    plan,
                    privileges: vec![],
                })
            }
            // vnode statement
            ExtStatement::DropVnode(stmt) => self.drop_vnode_to_plan(stmt),
            ExtStatement::CopyVnode(stmt) => self.copy_vnode_to_plan(stmt),
            ExtStatement::MoveVnode(stmt) => self.move_vnode_to_plan(stmt),
            ExtStatement::CompactVnode(stmt) => self.compact_vnode_to_plan(stmt),
            ExtStatement::ChecksumGroup(stmt) => self.checksum_group_to_plan(stmt),
        }
    }

    fn df_sql_to_plan(
        &self,
        stmt: Statement,
        session: &IsiphoSessionCtx,
    ) -> Result<PlanWithPrivileges> {
        match stmt {
            Statement::Query(_) => {
                let df_planner = SqlToRel::new(&self.schema_provider);
                let df_plan = df_planner.sql_statement_to_plan(stmt)?;
                let plan = Plan::Query(QueryPlan { df_plan });

                // privileges
                let access_databases = self.schema_provider.reset_access_databases();
                let privileges = databases_privileges(
                    DatabasePrivilege::Read,
                    *session.tenant_id(),
                    access_databases,
                );
                Ok(PlanWithPrivileges { plan, privileges })
            }
            Statement::Insert {
                table_name: ref sql_object_name,
                columns: ref sql_column_names,
                source,
                ..
            } => self.insert_to_plan(sql_object_name, sql_column_names, source, session),
            Statement::Kill { id, .. } => {
                let plan = Plan::SYSTEM(SYSPlan::KillQuery(id.into()));
                // TODO privileges
                Ok(PlanWithPrivileges {
                    plan,
                    privileges: vec![],
                })
            }
            _ => Err(QueryError::NotImplemented {
                err: stmt.to_string(),
            }),
        }
    }

    /// Generate a plan for EXPLAIN ... that will print out a plan
    pub fn explain_statement_to_plan(
        &self,
        verbose: bool,
        analyze: bool,
        statement: ExtStatement,
        session: &IsiphoSessionCtx,
    ) -> Result<PlanWithPrivileges> {
        let PlanWithPrivileges { plan, privileges } = self.statement_to_plan(statement, session)?;

        let input_df_plan = match plan {
            Plan::Query(query) => Arc::new(query.df_plan),
            _ => {
                return Err(QueryError::NotImplemented {
                    err: "explain non-query statement.".to_string(),
                })
            }
        };

        let schema = LogicalPlan::explain_schema().to_dfschema_ref()?;

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

        let plan = Plan::Query(QueryPlan { df_plan });

        Ok(PlanWithPrivileges { plan, privileges })
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
                        Expr::Column(source_field.qualified_column())
                    } else {
                        // Add cast(source_col as target_type) if it doesn't exist
                        cast(
                            Expr::Column(source_field.qualified_column()),
                            target_column_data_type.clone(),
                        )
                    }
                } else {
                    // The specified column in the target table is missing from the insert
                    // then add cast(null as target_type)
                    cast(lit(ScalarValue::Null), target_column_data_type.clone())
                };

                expr.alias(target_column_name)
            })
            .collect();

        debug!("assignments: {:?}", &assignments);

        Ok(LogicalPlan::Projection(
            Projection::try_new(assignments, Arc::new(source_plan), None)?, // .context(spi::DatafusionSnafu)?,
        ))
    }

    fn insert_to_plan(
        &self,
        sql_object_name: &ObjectName,
        sql_column_names: &[Ident],
        source: Box<Query>,
        session: &IsiphoSessionCtx,
    ) -> Result<PlanWithPrivileges> {
        // Transform subqueries
        let source_plan =
            SqlToRel::new(&self.schema_provider).query_to_plan(*source, &mut HashMap::new())?;

        // save database read privileges
        // This operation must be done before fetching the target table metadata
        let mut read_privileges = databases_privileges(
            DatabasePrivilege::Read,
            *session.tenant_id(),
            self.schema_provider.reset_access_databases(),
        );

        let table_name = normalize_sql_object_name(sql_object_name);
        let columns = sql_column_names
            .iter()
            .map(normalize_ident)
            .collect::<Vec<String>>();

        // Get the metadata of the target table
        let target_table = self.get_table_source(&table_name)?;
        let insert_columns = self.extract_column_names(columns.as_ref(), target_table.clone());

        // Check if the plan is legal
        semantic_check(insert_columns.as_ref(), &source_plan, target_table.clone())?;

        let final_source_logical_plan = self
            .add_projection_between_source_and_insert_node_if_necessary(
                target_table.clone(),
                source_plan,
                insert_columns,
            )?;

        let df_plan = table_write_plan_node(table_name, target_table, final_source_logical_plan)?;

        debug!("Insert plan:\n{}", df_plan.display_indent_schema());

        let plan = Plan::Query(QueryPlan { df_plan });

        // privileges
        let mut write_privileges = databases_privileges(
            DatabasePrivilege::Write,
            *session.tenant_id(),
            self.schema_provider.reset_access_databases(),
        );
        write_privileges.append(&mut read_privileges);
        Ok(PlanWithPrivileges {
            plan,
            privileges: write_privileges,
        })
    }

    fn drop_database_object_to_plan(
        &self,
        stmt: ast::DropDatabaseObject,
        session: &IsiphoSessionCtx,
    ) -> Result<PlanWithPrivileges> {
        let ast::DropDatabaseObject {
            ref object_name,
            if_exist,
            ref obj_type,
        } = stmt;
        // get the current tenant id from the session
        let tenant_id = *session.tenant_id();

        let (plan, privilege) = match obj_type {
            DatabaseObjectType::Table => {
                let table = normalize_sql_object_name(object_name);
                let object_ref = ObjectReference::from(table.as_str());
                let resolved_object_ref = object_ref.resolve(session.default_database());
                let database_name = resolved_object_ref.parent;
                (
                    DDLPlan::DropDatabaseObject(DropDatabaseObject {
                        if_exist,
                        object_name: normalize_sql_object_name(object_name),
                        obj_type: DatabaseObjectType::Table,
                    }),
                    Privilege::TenantObject(
                        TenantObjectPrivilege::Database(
                            DatabasePrivilege::Write,
                            Some(database_name.to_string()),
                        ),
                        Some(tenant_id),
                    ),
                )
            }
        };

        Ok(PlanWithPrivileges {
            plan: Plan::DDL(plan),
            privileges: vec![privilege],
        })
    }

    fn drop_tenant_object_to_plan(
        &self,
        stmt: ast::DropTenantObject,
        session: &IsiphoSessionCtx,
    ) -> Result<PlanWithPrivileges> {
        let ast::DropTenantObject {
            ref object_name,
            if_exist,
            ref obj_type,
        } = stmt;
        // get the current tenant id from the session
        let tenant_name = session.tenant();
        let tenant_id = *session.tenant_id();

        let (plan, privilege) = match obj_type {
            TenantObjectType::Database => {
                let database_name = normalize_ident(object_name);
                (
                    DDLPlan::DropTenantObject(DropTenantObject {
                        tenant_name: tenant_name.to_string(),
                        name: database_name.clone(),
                        if_exist,
                        obj_type: TenantObjectType::Database,
                    }),
                    Privilege::TenantObject(
                        TenantObjectPrivilege::Database(
                            DatabasePrivilege::Full,
                            Some(database_name),
                        ),
                        Some(tenant_id),
                    ),
                )
            }
            TenantObjectType::Role => {
                let role_name = normalize_ident(object_name);
                (
                    DDLPlan::DropTenantObject(DropTenantObject {
                        tenant_name: tenant_name.to_string(),
                        name: role_name,
                        if_exist,
                        obj_type: TenantObjectType::Role,
                    }),
                    Privilege::TenantObject(TenantObjectPrivilege::RoleFull, Some(tenant_id)),
                )
            }
        };

        Ok(PlanWithPrivileges {
            plan: Plan::DDL(plan),
            privileges: vec![privilege],
        })
    }

    fn drop_global_object_to_plan(
        &self,
        stmt: ast::DropGlobalObject,
    ) -> Result<PlanWithPrivileges> {
        let ast::DropGlobalObject {
            ref object_name,
            if_exist,
            ref obj_type,
        } = stmt;

        let (plan, privilege) = match obj_type {
            GlobalObjectType::Tenant => {
                let tenant_name = normalize_ident(object_name);
                (
                    DDLPlan::DropGlobalObject(DropGlobalObject {
                        if_exist,
                        name: tenant_name,
                        obj_type: GlobalObjectType::Tenant,
                    }),
                    Privilege::Global(GlobalPrivilege::Tenant(None)),
                )
            }
            GlobalObjectType::User => {
                let user_name = normalize_ident(object_name);
                (
                    DDLPlan::DropGlobalObject(DropGlobalObject {
                        if_exist,
                        name: user_name,
                        obj_type: GlobalObjectType::User,
                    }),
                    Privilege::Global(GlobalPrivilege::User(None)),
                )
            }
        };

        Ok(PlanWithPrivileges {
            plan: Plan::DDL(plan),
            privileges: vec![privilege],
        })
    }

    /// Generate a logical plan from a CREATE EXTERNAL TABLE statement
    pub fn external_table_to_plan(
        &self,
        statement: AstCreateExternalTable,
        session: &IsiphoSessionCtx,
    ) -> Result<PlanWithPrivileges> {
        let df_planner = SqlToRel::new(&self.schema_provider);

        let table_name = statement.name.clone();
        let (database_name, _) = extract_database_table_name(table_name.as_str(), session);

        let logical_plan = df_planner.external_table_to_plan(statement)?;

        if let LogicalPlan::CreateExternalTable(plan) = logical_plan {
            let plan = Plan::DDL(DDLPlan::CreateExternalTable(plan));
            // privileges
            return Ok(PlanWithPrivileges {
                plan,
                privileges: vec![Privilege::TenantObject(
                    TenantObjectPrivilege::Database(DatabasePrivilege::Full, Some(database_name)),
                    Some(*session.tenant_id()),
                )],
            });
        }

        Err(DataFusionError::Internal(
            UNEXPECTED_EXTERNAL_PLAN.to_string(),
        ))
        .context(DatafusionSnafu)
    }

    pub fn create_table_to_plan(
        &self,
        statement: ASTCreateTable,
        session: &IsiphoSessionCtx,
    ) -> Result<PlanWithPrivileges> {
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
            let col_id = id_generator.next_id() as ColumnId;
            let column = Self::column_opt_to_table_column(column_opt, col_id)?;
            schema.push(column);
        }

        let mut column_name = HashSet::new();
        for col in schema.iter() {
            if !column_name.insert(col.name.as_str()) {
                return Err(QueryError::SameColumnName {
                    column: col.name.to_string(),
                });
            }
        }

        let table_name = normalize_sql_object_name(&name);
        let (database_name, _) = extract_database_table_name(table_name.as_str(), session);

        let plan = Plan::DDL(DDLPlan::CreateTable(CreateTable {
            schema,
            name: table_name,
            if_not_exists,
        }));

        // privilege
        Ok(PlanWithPrivileges {
            plan,
            privileges: vec![Privilege::TenantObject(
                TenantObjectPrivilege::Database(DatabasePrivilege::Full, Some(database_name)),
                Some(*session.tenant_id()),
            )],
        })
    }

    fn column_opt_to_table_column(column_opt: ColumnOption, id: ColumnId) -> Result<TableColumn> {
        Self::check_column_encoding(&column_opt)?;

        let col = if column_opt.is_tag {
            TableColumn::new_tag_column(id, normalize_ident(&column_opt.name))
        } else {
            let name = normalize_ident(&column_opt.name);
            let column_type = Self::make_data_type(&name, &column_opt.data_type)?;
            TableColumn::new(
                id,
                name,
                column_type,
                column_opt.encoding.unwrap_or_default(),
            )
        };
        Ok(col)
    }

    fn database_to_describe(
        &self,
        statement: DescribeDatabaseOptions,
        session: &IsiphoSessionCtx,
    ) -> Result<PlanWithPrivileges> {
        // get the current tenant id from the session
        let database_name = normalize_sql_object_name(&statement.database_name);

        let plan = Plan::DDL(DDLPlan::DescribeDatabase(DescribeDatabase {
            database_name: database_name.to_string(),
        }));
        // privileges
        let tenant_id = *session.tenant_id();
        let privilege = Privilege::TenantObject(
            TenantObjectPrivilege::Database(DatabasePrivilege::Read, Some(database_name)),
            Some(tenant_id),
        );
        Ok(PlanWithPrivileges {
            plan,
            privileges: vec![privilege],
        })
    }

    fn table_to_describe(
        &self,
        opts: DescribeTableOptions,
        session: &IsiphoSessionCtx,
    ) -> Result<PlanWithPrivileges> {
        let table_name = normalize_sql_object_name(&opts.table_name);
        let (database_name, _) = extract_database_table_name(&table_name, session);

        let plan = Plan::DDL(DDLPlan::DescribeTable(DescribeTable { table_name }));
        // privileges
        Ok(PlanWithPrivileges {
            plan,
            privileges: vec![Privilege::TenantObject(
                TenantObjectPrivilege::Database(DatabasePrivilege::Read, Some(database_name)),
                Some(*session.tenant_id()),
            )],
        })
    }

    fn table_to_alter(
        &self,
        statement: ASTAlterTable,
        session: &IsiphoSessionCtx,
    ) -> Result<PlanWithPrivileges> {
        let table_name = normalize_sql_object_name(&statement.table_name);
        let table_schema = self.get_tskv_schema(&table_name)?;

        let alter_action = match statement.alter_action {
            ASTAlterTableAction::AddColumn { column } => {
                let table_column = Self::column_opt_to_table_column(column, ColumnId::default())?;
                if table_schema.contains_column(&table_column.name) {
                    return Err(QueryError::ColumnAlreadyExists {
                        table: table_schema.name.to_string(),
                        column: table_column.name,
                    });
                }
                AlterTableAction::AddColumn { table_column }
            }
            ASTAlterTableAction::DropColumn { ref column_name } => {
                let column_name = normalize_ident(column_name);
                let table_column = table_schema.column(&column_name).ok_or_else(|| {
                    QueryError::ColumnNotExists {
                        column: column_name.to_string(),
                        table: table_schema.name.to_string(),
                    }
                })?;

                if table_column.column_type.is_tag() {
                    return Err(QueryError::DropTag {
                        column: table_column.name.to_string(),
                    });
                }

                if table_column.column_type.is_field() && table_schema.field_num() == 1 {
                    return Err(QueryError::AtLeastOneField);
                }

                if table_column.column_type.is_time() {
                    return Err(QueryError::AtLeastOneTag);
                }

                AlterTableAction::DropColumn { column_name }
            }

            ASTAlterTableAction::AlterColumnEncoding {
                ref column_name,
                encoding,
            } => {
                let column_name = normalize_ident(column_name);
                let column = table_schema.column(&column_name).ok_or_else(|| {
                    QueryError::ColumnNotExists {
                        column: column_name.to_string(),
                        table: table_schema.name.to_string(),
                    }
                })?;
                if column.column_type.is_tag() {
                    return Err(QueryError::TagNotSupportCompression);
                }

                if column.column_type.is_time() {
                    return Err(QueryError::TimeColumnAlter);
                }

                let mut new_column = column.clone();
                new_column.encoding = encoding;

                AlterTableAction::AlterColumn {
                    column_name,
                    new_column,
                }
            }
        };
        let plan = Plan::DDL(DDLPlan::AlterTable(AlterTable {
            table_name,
            alter_action,
        }));

        // privileges
        Ok(PlanWithPrivileges {
            plan,
            privileges: vec![Privilege::TenantObject(
                TenantObjectPrivilege::Database(
                    DatabasePrivilege::Full,
                    Some(table_schema.db.clone()),
                ),
                Some(*session.tenant_id()),
            )],
        })
    }

    fn database_to_show(&self, session: &IsiphoSessionCtx) -> Result<PlanWithPrivileges> {
        let plan = Plan::DDL(DDLPlan::ShowDatabases());
        // privileges
        let tenant_id = *session.tenant_id();
        let privilege = Privilege::TenantObject(
            TenantObjectPrivilege::Database(DatabasePrivilege::Read, None),
            Some(tenant_id),
        );
        Ok(PlanWithPrivileges {
            plan,
            privileges: vec![privilege],
        })
    }

    fn table_to_show(
        &self,
        database: Option<ObjectName>,
        session: &IsiphoSessionCtx,
    ) -> Result<PlanWithPrivileges> {
        let db_name = database.map(|db_name| normalize_sql_object_name(&db_name));
        let plan = Plan::DDL(DDLPlan::ShowTables(db_name.clone()));
        // privileges
        Ok(PlanWithPrivileges {
            plan,
            privileges: vec![Privilege::TenantObject(
                TenantObjectPrivilege::Database(DatabasePrivilege::Read, db_name),
                Some(*session.tenant_id()),
            )],
        })
    }

    fn show_tag_body(
        &self,
        session: &IsiphoSessionCtx,
        body: ShowTagBody,
        projection_function: impl FnOnce(
            &TskvTableSchema,
            LogicalPlanBuilder,
            bool,
        ) -> Result<LogicalPlan>,
    ) -> Result<PlanWithPrivileges> {
        // merge db a.b table b.c to a.b.c
        let table = match merge_object_name(body.database_name.clone(), Some(body.table.clone())) {
            Some(table) => table,
            None => {
                return Err(QueryError::DBTableConflict {
                    db: body.database_name.unwrap_or(ObjectName(vec![])).to_string(),
                    table: body.table.to_string(),
                })
            }
        };

        let table_name = normalize_sql_object_name(&table);
        let table_source = self.get_table_source(&table_name)?;
        let table_schema = self.get_tskv_schema(&table_name)?;
        let table_df_schema = table_source.schema().to_dfschema_ref()?;
        let sql_to_rel = SqlToRel::new(&self.schema_provider);

        // build from
        let mut plan_builder =
            LogicalPlanBuilder::scan(&table_schema.name, table_source.clone(), None)?;

        // build where
        let selection = match body.selection {
            Some(expr) => Some(
                sql_to_rel.sql_to_rex(expr, &table_df_schema, &mut HashMap::new())?, // .context(spi::DatafusionSnafu)?,
            ),
            None => None,
        };

        // get all columns in expr
        let mut columns = HashSet::new();
        if let Some(selection) = &selection {
            expr_to_columns(selection, &mut columns).context(spi::DatafusionSnafu)?;
        }

        // check column
        check_show_series_expr(&columns, &table_schema)?;

        if let Some(selection) = selection {
            plan_builder = plan_builder.filter(selection)?;
        }

        // get where has time column
        let where_contain_time = columns
            .iter()
            .flat_map(|c: &Column| table_schema.column(&c.name))
            .any(|c: &TableColumn| c.column_type.is_time());

        // build projection
        let plan = projection_function(&table_schema, plan_builder, where_contain_time)?;

        // build order by
        let mut plan_builder = self.order_by(body.order_by, plan)?;

        // build limit
        if body.limit.is_some() || body.offset.is_some() {
            plan_builder =
                self.limit_offset_to_plan(body.limit, body.offset, plan_builder, &table_df_schema)?;
        }

        let df_plan = plan_builder
            .build()
            .context(DatafusionSnafu)?;
        let db_name = &table_schema.db;

        Ok(PlanWithPrivileges {
            plan: Plan::Query(QueryPlan { df_plan }),
            privileges: vec![Privilege::TenantObject(
                TenantObjectPrivilege::Database(DatabasePrivilege::Read, Some(db_name.to_string())),
                Some(*session.tenant_id()),
            )],
        })
    }

    fn show_series_to_plan(
        &self,
        stmt: ASTShowSeries,
        session: &IsiphoSessionCtx,
    ) -> Result<PlanWithPrivileges> {
        self.show_tag_body(session, stmt.body, show_series_projection)
    }

    fn show_tag_values(
        &self,
        stmt: ASTShowTagValues,
        session: &IsiphoSessionCtx,
    ) -> Result<PlanWithPrivileges> {
        // merge db a.b table b.c to a.b.c
        self.show_tag_body(
            session,
            stmt.body,
            |schema, plan_builder, where_contain_time| {
                show_tag_value_projections(schema, plan_builder, where_contain_time, stmt.with)
            },
        )
    }

    fn database_to_plan(
        &self,
        stmt: ASTCreateDatabase,
        session: &IsiphoSessionCtx,
    ) -> Result<PlanWithPrivileges> {
        let ASTCreateDatabase {
            name,
            if_not_exists,
            options,
        } = stmt;

        let name = normalize_sql_object_name(&name);

        // check if system database
        if name.eq_ignore_ascii_case(CLUSTER_SCHEMA)
            || name.eq_ignore_ascii_case(INFORMATION_SCHEMA)
        {
            return Err(QueryError::Meta {
                source: MetaError::DatabaseAlreadyExists { database: name },
            });
        }

        let options = self.make_database_option(options)?;
        let plan = Plan::DDL(DDLPlan::CreateDatabase(CreateDatabase {
            name,
            if_not_exists,
            options,
        }));
        // privileges
        let tenant_id = *session.tenant_id();
        let privilege = Privilege::TenantObject(
            TenantObjectPrivilege::Database(DatabasePrivilege::Write, None),
            Some(tenant_id),
        );
        Ok(PlanWithPrivileges {
            plan,
            privileges: vec![privilege],
        })
    }

    fn order_by(&self, exprs: Vec<OrderByExpr>, plan: LogicalPlan) -> Result<LogicalPlanBuilder> {
        if exprs.is_empty() {
            return Ok(LogicalPlanBuilder::from(plan));
        }
        let schema = plan.schema();
        let sql_to_rel = SqlToRel::new(&self.schema_provider);

        let mut sort_exprs = Vec::new();
        for OrderByExpr {
            expr,
            asc,
            nulls_first,
        } in exprs
        {
            let expr = sql_to_rel.sql_to_rex(expr, schema, &mut HashMap::new())?;
            // .context(spi::DatafusionSnafu)?;
            let asc = asc.unwrap_or(true);
            let sort_expr = Expr::Sort {
                expr: Box::new(expr),
                asc,
                // when asc is true, by default nulls last to be consistent with postgres
                // postgres rule: https://www.postgresql.org/docs/current/queries-order.html
                nulls_first: nulls_first.unwrap_or(!asc),
            };
            sort_exprs.push(sort_expr);
        }

        LogicalPlanBuilder::from(plan)
            .sort(sort_exprs)
            .context(spi::DatafusionSnafu)
    }

    fn limit_offset_to_plan(
        &self,
        limit: Option<ASTExpr>,
        offset: Option<Offset>,
        plan_builder: LogicalPlanBuilder,
        schema: &DFSchema,
    ) -> Result<LogicalPlanBuilder> {
        let sql_to_rel = SqlToRel::new(&self.schema_provider);
        let skip = match offset {
            Some(offset) => match sql_to_rel
                .sql_to_rex(offset.value, schema, &mut HashMap::new())
                .context(spi::DatafusionSnafu)?
            {
                Expr::Literal(ScalarValue::Int64(Some(m))) => {
                    if m < 0 {
                        return Err(QueryError::OffsetBtZero { provide: m });
                    } else {
                        m as usize
                    }
                }
                _ => return Err(QueryError::OffsetConstant),
            },
            None => 0,
        };

        let fetch = match limit {
            Some(exp) => match sql_to_rel
                .sql_to_rex(exp, schema, &mut HashMap::new())
                .context(spi::DatafusionSnafu)?
            {
                Expr::Literal(ScalarValue::Int64(Some(n))) => {
                    if n < 0 {
                        return Err(QueryError::LimitBtZero { provide: n });
                    } else {
                        Some(n as usize)
                    }
                }
                _ => return Err(QueryError::LimitConstant),
            },
            None => None,
        };

        let plan_builder = plan_builder.limit(skip, fetch)?;
        Ok(plan_builder)
    }

    fn database_to_alter(
        &self,
        stmt: ASTAlterDatabase,
        session: &IsiphoSessionCtx,
    ) -> Result<PlanWithPrivileges> {
        let ASTAlterDatabase { name, options } = stmt;
        let options = self.make_database_option(options)?;
        let database_name = normalize_sql_object_name(&name);
        let plan = Plan::DDL(DDLPlan::AlterDatabase(AlterDatabase {
            database_name: database_name.to_string(),
            database_options: options,
        }));
        // privileges
        let tenant_id = *session.tenant_id();
        let privilege = Privilege::TenantObject(
            TenantObjectPrivilege::Database(DatabasePrivilege::Full, Some(database_name)),
            Some(tenant_id),
        );
        Ok(PlanWithPrivileges {
            plan,
            privileges: vec![privilege],
        })
    }

    fn make_database_option(&self, options: ASTDatabaseOptions) -> Result<DatabaseOptions> {
        let mut plan_options = DatabaseOptions::default();
        if let Some(ttl) = options.ttl {
            plan_options.with_ttl(self.str_to_duration(&ttl)?);
        }
        if let Some(replica) = options.replica {
            plan_options.with_replica(replica);
        }
        if let Some(shard_num) = options.shard_num {
            plan_options.with_shard_num(shard_num);
        }
        if let Some(vnode_duration) = options.vnode_duration {
            plan_options.with_vnode_duration(self.str_to_duration(&vnode_duration)?);
        }
        if let Some(precision) = options.precision {
            plan_options.with_precision(Precision::new(&precision).ok_or(QueryError::Parser {
                source: ParserError::ParserError(format!(
                    "{} is not a valid precision, use like 'ms', 'us', 'ns'",
                    precision
                )),
            })?);
        }
        Ok(plan_options)
    }

    fn str_to_duration(&self, text: &str) -> Result<Duration> {
        Duration::new(text).ok_or_else(|| QueryError::Parser {
            source: ParserError::ParserError(format!(
                "{} is not a valid precision, use like 'ms', 'us', 'ns'",
                text
            )),
        })
    }

    fn make_data_type(column_name: &str, data_type: &SQLDataType) -> Result<ColumnType> {
        match data_type {
            // todo : should support get time unit for database
            SQLDataType::Timestamp(_) => Ok(ColumnType::Time),
            SQLDataType::BigInt(_) => Ok(ColumnType::Field(ValueType::Integer)),
            SQLDataType::UnsignedBigInt(_) => Ok(ColumnType::Field(ValueType::Unsigned)),
            SQLDataType::Double => Ok(ColumnType::Field(ValueType::Float)),
            SQLDataType::String => Ok(ColumnType::Field(ValueType::String)),
            SQLDataType::Boolean => Ok(ColumnType::Field(ValueType::Boolean)),
            _ => Err(QueryError::DataType {
                column: column_name.to_string(),
                data_type: data_type.to_string(),
            }),
        }
    }

    fn check_column_encoding(column: &ColumnOption) -> Result<()> {
        // tag无压缩，直接返回
        if column.is_tag {
            return Ok(());
        }
        // 数据类型 -> 压缩方式 校验
        let encoding = column.encoding.unwrap_or_default();
        let is_ok = match column.data_type {
            SQLDataType::Timestamp(_) => encoding.is_timestamp_encoding(),
            SQLDataType::BigInt(_) => encoding.is_bigint_encoding(),
            SQLDataType::UnsignedBigInt(_) => encoding.is_unsigned_encoding(),
            SQLDataType::Double => encoding.is_double_encoding(),
            SQLDataType::String => encoding.is_string_encoding(),
            SQLDataType::Boolean => encoding.is_bool_encoding(),
            _ => false,
        };
        if !is_ok {
            return Err(QueryError::EncodingType {
                encoding_type: column.encoding.unwrap_or_default(),
                data_type: column.data_type.to_string(),
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

    fn get_table_source(&self, table_name: &str) -> Result<Arc<dyn TableSource>> {
        let table_ref = TableReference::from(table_name);
        self.schema_provider
            .get_table_provider(table_ref)
            .context(spi::DatafusionSnafu)
    }

    fn get_table_provider(&self, table_name: &str) -> Result<Arc<dyn TableProvider>> {
        let table_source = self.get_table_source(table_name)?;
        source_as_provider(&table_source)
            .map_err(|_| MetaError::CommonError {
                msg: format!(
                    "can't convert table source {} to table provider ",
                    table_name
                ),
            })
            .map_err(|e| QueryError::Meta { source: e })
        // .context(MetaSnafu)
    }

    fn create_tenant_to_plan(&self, stmt: ast::CreateTenant) -> Result<PlanWithPrivileges> {
        let ast::CreateTenant {
            name,
            if_not_exists,
            with_options,
        } = stmt;

        let name = normalize_ident(&name);
        let options = sql_options_to_tenant_options(with_options)?;

        let privileges = vec![Privilege::Global(GlobalPrivilege::Tenant(None))];

        let plan = Plan::DDL(DDLPlan::CreateTenant(Box::new(CreateTenant {
            name,
            if_not_exists,
            options,
        })));

        Ok(PlanWithPrivileges { plan, privileges })
    }

    fn create_user_to_plan(&self, stmt: ast::CreateUser) -> Result<PlanWithPrivileges> {
        let ast::CreateUser {
            name,
            if_not_exists,
            with_options,
        } = stmt;

        let name = normalize_ident(&name);
        let options = sql_options_to_user_options(with_options)?;

        let privileges = vec![Privilege::Global(GlobalPrivilege::User(None))];

        let plan = Plan::DDL(DDLPlan::CreateUser(CreateUser {
            name,
            if_not_exists,
            options,
        }));

        Ok(PlanWithPrivileges { plan, privileges })
    }

    fn create_role_to_plan(
        &self,
        stmt: ast::CreateRole,
        session: &IsiphoSessionCtx,
    ) -> Result<PlanWithPrivileges> {
        let ast::CreateRole {
            name,
            if_not_exists,
            inherit,
        } = stmt;

        let role_name = normalize_ident(&name);
        let tenant_name = session.tenant();
        let tenant_id = *session.tenant_id();

        let inherit_tenant_role = inherit
            .map(|ref e| {
                SystemTenantRole::try_from(normalize_ident(e).as_str())
                    .map_err(|err| QueryError::Semantic { err })
            })
            .unwrap_or(Ok(SystemTenantRole::Member))?;

        let privilege = Privilege::TenantObject(TenantObjectPrivilege::RoleFull, Some(tenant_id));

        let plan = Plan::DDL(DDLPlan::CreateRole(CreateRole {
            tenant_name: tenant_name.to_string(),
            name: role_name,
            if_not_exists,
            inherit_tenant_role,
        }));

        Ok(PlanWithPrivileges {
            plan,
            privileges: vec![privilege],
        })
    }

    fn construct_alter_tenant_action_with_privilege(
        &self,
        operation: AlterTenantOperation,
        tenant_id: Oid,
    ) -> Result<(AlterTenantAction, Privilege<Oid>)> {
        let alter_tenant_action_with_privileges = match operation {
            AlterTenantOperation::AddUser(ref user, ref role) => {
                // user_id: Oid,
                // role: TenantRole<Oid>,
                // tenant_id: Oid,
                let privilege =
                    Privilege::TenantObject(TenantObjectPrivilege::MemberFull, Some(tenant_id));

                let user_name = normalize_ident(user);
                // 查询用户信息，不存在直接报错咯
                // fn user(
                //     &self,
                //     name: &str
                // ) -> Result<Option<UserDesc>>;
                let user_desc = self.schema_provider.get_user(&user_name)?;
                // .context(MetaSnafu)?;
                let user_id = *user_desc.id();

                let role_name = normalize_ident(role);
                let role = SystemTenantRole::try_from(role_name.as_str())
                    .ok()
                    .map(TenantRoleIdentifier::System)
                    .unwrap_or_else(|| TenantRoleIdentifier::Custom(role_name));

                (
                    AlterTenantAction::AddUser(AlterTenantAddUser { user_id, role }),
                    privilege,
                )
            }
            AlterTenantOperation::SetUser(ref user, ref role) => {
                // user_id: Oid,
                // role: TenantRole<Oid>,
                // tenant_id: Oid,
                let privilege =
                    Privilege::TenantObject(TenantObjectPrivilege::MemberFull, Some(tenant_id));

                let user_name = normalize_ident(user);
                // 查询用户信息，不存在直接报错咯
                // fn user(
                //     &self,
                //     name: &str
                // ) -> Result<Option<UserDesc>>;
                let user_desc = self.schema_provider.get_user(&user_name)?;
                // .context(MetaSnafu)?;
                let user_id = *user_desc.id();

                let role_name = normalize_ident(role);
                let role = SystemTenantRole::try_from(role_name.as_str())
                    .ok()
                    .map(TenantRoleIdentifier::System)
                    .unwrap_or_else(|| TenantRoleIdentifier::Custom(role_name));

                (
                    AlterTenantAction::SetUser(AlterTenantSetUser { user_id, role }),
                    privilege,
                )
            }
            AlterTenantOperation::RemoveUser(ref user) => {
                // user_id: Oid,
                // tenant_id: Oid
                let privilege =
                    Privilege::TenantObject(TenantObjectPrivilege::MemberFull, Some(tenant_id));

                let user_name = normalize_ident(user);
                // 查询用户信息，不存在直接报错咯
                // fn user(
                //     &self,
                //     name: &str
                // ) -> Result<Option<UserDesc>>;
                let user_desc = self.schema_provider.get_user(&user_name)?;
                // .context(MetaSnafu)?;
                let user_id = *user_desc.id();

                (AlterTenantAction::RemoveUser(user_id), privilege)
            }
            AlterTenantOperation::Set(sql_option) => {
                // tenant_id: Oid
                let privilege = Privilege::Global(GlobalPrivilege::Tenant(Some(tenant_id)));

                let tenant_options = sql_options_to_tenant_options(vec![sql_option])?;
                // .map_err(|err| QueryError::Semantic { err })?;

                (AlterTenantAction::Set(Box::new(tenant_options)), privilege)
            }
        };

        Ok(alter_tenant_action_with_privileges)
    }

    fn alter_tenant_to_plan(&self, stmt: ast::AlterTenant) -> Result<PlanWithPrivileges> {
        let ast::AlterTenant {
            ref name,
            operation,
        } = stmt;

        let tenant_name = normalize_ident(name);
        // 查询租户信息，不存在直接报错咯
        // fn tenant(
        //     &self,
        //     name: &str
        // ) -> Result<Tenant>;
        let tenant = self.schema_provider.get_tenant(&tenant_name)?;
        // .context(MetaSnafu)?;

        let (alter_tenant_action, privilege) =
            self.construct_alter_tenant_action_with_privilege(operation, *tenant.id())?;

        let plan = Plan::DDL(DDLPlan::AlterTenant(AlterTenant {
            tenant_name,
            alter_tenant_action,
        }));

        Ok(PlanWithPrivileges {
            plan,
            privileges: vec![privilege],
        })
    }

    fn alter_user_to_plan(&self, stmt: ast::AlterUser) -> Result<PlanWithPrivileges> {
        let ast::AlterUser { name, operation } = stmt;

        let user_name = normalize_ident(&name);
        // 查询用户信息，不存在直接报错咯
        // fn user(
        //     &self,
        //     name: &str
        // ) -> Result<Option<UserDesc>>;
        let user_desc = self.schema_provider.get_user(&user_name)?;
        // .context(MetaSnafu)?;
        let user_id = *user_desc.id();

        let alter_user_action = match operation {
            AlterUserOperation::RenameTo(ref new_name) => {
                AlterUserAction::RenameTo(normalize_ident(new_name))
            }
            AlterUserOperation::Set(sql_option) => {
                let user_options = sql_options_to_user_options(vec![sql_option])?;
                // .map_err(|err| QueryError::Semantic { err })?;

                AlterUserAction::Set(user_options)
            }
        };

        let privileges = vec![Privilege::Global(GlobalPrivilege::User(Some(user_id)))];

        let plan = Plan::DDL(DDLPlan::AlterUser(AlterUser {
            user_name,
            alter_user_action,
        }));

        Ok(PlanWithPrivileges { plan, privileges })
    }

    fn grant_revoke_to_plan(
        &self,
        stmt: ast::GrantRevoke,
        session: &IsiphoSessionCtx,
    ) -> Result<PlanWithPrivileges> {
        // database_id: Oid,
        // privilege: DatabasePrivilege,
        // role_name: &str,
        // tenant_id: &Oid,
        let ast::GrantRevoke {
            is_grant,
            privileges,
            role_name,
        } = stmt;

        let role_name = normalize_ident(&role_name);
        let tenant_name = session.tenant();
        let tenant_id = *session.tenant_id();

        if SystemTenantRole::try_from(role_name.as_str()).is_ok() {
            let err = QueryError::SystemRoleModification;
            warn!("{}", err.to_string());
            return Err(err);
        }

        let database_privileges = privileges
            .iter()
            .map(|ast::Privilege { action, database }| {
                let database_privilege = match action {
                    ast::Action::Read => DatabasePrivilege::Read,
                    ast::Action::Write => DatabasePrivilege::Write,
                    ast::Action::All => DatabasePrivilege::Full,
                };
                let database_name = normalize_ident(database);

                (database_privilege, database_name)
            })
            .collect::<Vec<(DatabasePrivilege, String)>>();

        let privileges = vec![Privilege::TenantObject(
            TenantObjectPrivilege::RoleFull,
            Some(tenant_id),
        )];

        let plan = Plan::DDL(DDLPlan::GrantRevoke(GrantRevoke {
            is_grant,
            database_privileges,
            tenant_name: tenant_name.to_string(),
            role_name,
        }));

        Ok(PlanWithPrivileges { plan, privileges })
    }

    fn drop_vnode_to_plan(&self, stmt: ASTDropVnode) -> Result<PlanWithPrivileges> {
        let ASTDropVnode { vnode_id } = stmt;

        let plan = Plan::DDL(DDLPlan::DropVnode(DropVnode { vnode_id }));
        Ok(PlanWithPrivileges {
            plan,
            privileges: vec![Privilege::Global(GlobalPrivilege::System)],
        })
    }

    fn copy_vnode_to_plan(&self, stmt: ASTCopyVnode) -> Result<PlanWithPrivileges> {
        let ASTCopyVnode { vnode_id, node_id } = stmt;

        let plan = Plan::DDL(DDLPlan::CopyVnode(CopyVnode { vnode_id, node_id }));
        Ok(PlanWithPrivileges {
            plan,
            privileges: vec![Privilege::Global(GlobalPrivilege::System)],
        })
    }

    fn move_vnode_to_plan(&self, stmt: ASTMoveVnode) -> Result<PlanWithPrivileges> {
        let ASTMoveVnode { vnode_id, node_id } = stmt;

        let plan = Plan::DDL(DDLPlan::MoveVnode(MoveVnode { vnode_id, node_id }));
        Ok(PlanWithPrivileges {
            plan,
            privileges: vec![Privilege::Global(GlobalPrivilege::System)],
        })
    }

    fn compact_vnode_to_plan(&self, stmt: ASTCompactVnode) -> Result<PlanWithPrivileges> {
        let ASTCompactVnode { vnode_ids } = stmt;

        let plan = Plan::DDL(DDLPlan::CompactVnode(CompactVnode { vnode_ids }));
        Ok(PlanWithPrivileges {
            plan,
            privileges: vec![Privilege::Global(GlobalPrivilege::System)],
        })
    }

    fn checksum_group_to_plan(&self, stmt: ASTChecksumGroup) -> Result<PlanWithPrivileges> {
        let ASTChecksumGroup { replication_set_id } = stmt;

        let plan = Plan::DDL(DDLPlan::ChecksumGroup(ChecksumGroup { replication_set_id }));
        Ok(PlanWithPrivileges {
            plan,
            privileges: vec![Privilege::Global(GlobalPrivilege::System)],
        })
    }

    fn get_tskv_schema(&self, table_name: &str) -> Result<TskvTableSchemaRef> {
        Ok(self
            .get_table_provider(table_name)?
            .as_any()
            .downcast_ref::<ClusterTable>()
            .ok_or_else(|| MetaError::TableNotFound {
                table: table_name.to_string(),
            })?
            // .context(MetaSnafu)?
            .table_schema())
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
        return Err(QueryError::MismatchColumns);
    }

    if insert_field_num == 0 && source_field_num != target_table_field_num {
        return Err(QueryError::MismatchColumns);
    }
    // The target table must contain all insert fields
    for insert_col in insert_columns {
        target_table_fields
            .iter()
            .find(|e| e.name() == insert_col)
            .ok_or_else(|| QueryError::MissingColumn {
                insert_col: insert_col.to_string(),
                fields: target_table_fields
                    .iter()
                    .map(|e| e.name().as_str())
                    .collect::<Vec<&str>>()
                    .join(","),
            })?;
    }

    Ok(())
}

// check
// show series can't include field column
fn check_show_series_expr(columns: &HashSet<Column>, table_schema: &TskvTableSchema) -> Result<()> {
    for column in columns.iter() {
        match table_schema.column(&column.name) {
            Some(table_column) => {
                if table_column.column_type.is_field() {
                    return Err(QueryError::ShowSeriesWhereContainsField {
                        column: column.to_string(),
                    });
                }
            }

            None => {
                return Err(QueryError::ColumnNotExists {
                    column: column.to_string(),
                    table: table_schema.name.to_string(),
                })
            }
        }
    }

    Ok(())
}

fn show_series_projection(
    table_schema: &TskvTableSchema,
    mut plan_builder: LogicalPlanBuilder,
    where_contain_time: bool,
) -> Result<LogicalPlan> {
    let tags = table_schema
        .columns()
        .iter()
        .filter(|c| c.column_type.is_tag())
        .collect::<Vec<&TableColumn>>();

    // If the time column is included,
    //   all field columns will be scanned at rewrite_tag_scan,
    //   so this projection needs to be added
    if where_contain_time {
        let exp = tags
            .iter()
            .map(|tag| table_column_to_expr(table_schema, tag))
            .collect::<Vec<Expr>>();
        plan_builder = plan_builder.project(exp).context(spi::DatafusionSnafu)?;
    };

    plan_builder = plan_builder.distinct().context(spi::DatafusionSnafu)?;

    // concat tag_key=tag_value projection
    let tag_concat_expr_iter = tags.iter().map(|tag| {
        let column_expr = Box::new(Expr::Column(Column::new(
            Some(&table_schema.name),
            &tag.name,
        )));
        let is_null_expr = Box::new(column_expr.clone().is_null());
        let when_then_expr = vec![(is_null_expr, Box::new(lit(ScalarValue::Null)))];
        let else_expr = Some(Box::new(Expr::BinaryExpr(BinaryExpr {
            left: Box::new(lit(format!("{}=", &tag.name))),
            op: Operator::StringConcat,
            right: column_expr,
        })));
        Expr::Case(Case::new(None, when_then_expr, else_expr))
    });

    let concat_ws_args = iter::once(lit(","))
        .chain(iter::once(lit(&table_schema.name)))
        .chain(tag_concat_expr_iter)
        .collect::<Vec<Expr>>();
    let concat_ws = Expr::ScalarFunction {
        fun: BuiltinScalarFunction::ConcatWithSeparator,
        args: concat_ws_args,
    }
    .alias("key");
    plan_builder
        .project(iter::once(concat_ws))?
        .build()
        .context(spi::DatafusionSnafu)
}

fn show_tag_value_projections(
    table_schema: &TskvTableSchema,
    mut plan_builder: LogicalPlanBuilder,
    where_contain_time: bool,
    with: With,
) -> Result<LogicalPlan> {
    let mut tag_key_filter: Box<dyn FnMut(&TableColumn) -> bool> = match with {
        With::Equal(ident) => Box::new(move |column| normalize_ident(&ident).eq(&column.name)),
        With::UnEqual(ident) => Box::new(move |column| normalize_ident(&ident).ne(&column.name)),
        With::In(idents) => Box::new(move |column| {
            idents
                .iter()
                .map(normalize_ident)
                .any(|name| column.name.eq(&name))
        }),
        With::NotIn(idents) => Box::new(move |column| {
            idents
                .iter()
                .map(normalize_ident)
                .all(|name| column.name.ne(&name))
        }),
        _ => {
            return Err(QueryError::NotImplemented {
                err: "Not implemented Match, UnMatch".to_string(),
            })
        }
    };

    let tags = table_schema
        .columns()
        .iter()
        .filter(|column| column.column_type.is_tag())
        .filter(|column| tag_key_filter(column))
        .collect::<Vec<&TableColumn>>();

    if tags.is_empty() {
        return Ok(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(
                DFSchema::new_with_metadata(
                    vec![
                        DFField::new(None, "key", DataType::Utf8, false),
                        DFField::new(None, "value", DataType::Utf8, false),
                    ],
                    HashMap::new(),
                )
                .context(spi::DatafusionSnafu)?,
            ),
        }));
    }

    // If the time column is included,
    //   all field columns will be scanned at rewrite_tag_scan,
    //   so this projection needs to be added
    if where_contain_time {
        let exprs = tags
            .iter()
            .map(|tag| table_column_to_expr(table_schema, tag))
            .collect::<Vec<Expr>>();

        plan_builder = plan_builder.project(exprs).context(spi::DatafusionSnafu)?;
    };

    plan_builder = plan_builder.distinct().context(spi::DatafusionSnafu)?;

    let mut projections = Vec::new();
    for tag in tags {
        let key_column = lit(&tag.name).alias("key");
        let value_column = table_column_to_expr(table_schema, tag).alias("value");
        let projection = plan_builder.project(vec![key_column, value_column.clone()])?;
        let filter_expr = value_column.is_not_null();
        let projection = projection.filter(filter_expr)?.build()?;
        projections.push(Arc::new(projection));
    }
    let df_schema = projections[0].schema().clone();

    let union = LogicalPlan::Union(Union {
        inputs: projections,
        schema: df_schema,
        alias: None,
    });

    Ok(union)
}

fn table_column_to_expr(table_schema: &TskvTableSchema, column: &TableColumn) -> Expr {
    Expr::Column(Column::new(
        Some(table_schema.name.to_string()),
        column.name.to_string(),
    ))
}

fn table_write_plan_node(
    table_name: String,
    target_table: Arc<dyn TableSource>,
    input: LogicalPlan,
) -> std::result::Result<LogicalPlan, DataFusionError> {
    // output variable for insert operation
    let expr = input
        .schema()
        .fields()
        .iter()
        .last()
        .map(|e| Expr::Column(e.qualified_column()));

    debug_assert!(
        expr.is_some(),
        "invalid table write node's input logical plan"
    );

    let expr = unsafe { expr.unwrap_unchecked() };

    let affected_row_expr = affected_row_expr(expr);

    // construct table writer logical node
    let node = Arc::new(TableWriterPlanNode::try_new(
        table_name,
        target_table,
        Arc::new(input),
        vec![affected_row_expr],
    )?);

    let df_plan = LogicalPlan::Extension(Extension { node });

    let group_expr: Vec<Expr> = vec![];

    LogicalPlanBuilder::from(df_plan)
        .aggregate(group_expr, vec![merge_affected_row_expr()])?
        .build()
}

impl<S: ContextProviderExtension> LogicalPlanner for SqlPlaner<S> {
    fn create_logical_plan(
        &self,
        statement: ExtStatement,
        session: &IsiphoSessionCtx,
    ) -> Result<Plan> {
        let PlanWithPrivileges { plan, privileges } = self.statement_to_plan(statement, session)?;
        // check privileges
        let privileges_str = privileges
            .iter()
            .map(|e| format!("{:?}", e))
            .collect::<Vec<String>>()
            .join(",");
        debug!("logical_plan's privileges: [{}]", privileges_str);

        // check privilege
        for p in privileges.iter() {
            if !session.user().check_privilege(p) {
                return Err(QueryError::InsufficientPrivileges {
                    privilege: format!("{}", p),
                });
            }
        }

        Ok(plan)
    }
}

fn databases_privileges(
    db_priv: DatabasePrivilege,
    tenant_id: Oid,
    databases: DatabaseSet,
) -> Vec<Privilege<Oid>> {
    databases
        .dbs()
        .into_iter()
        .cloned()
        .map(|db| {
            Privilege::TenantObject(
                TenantObjectPrivilege::Database(db_priv.clone(), Some(db)),
                Some(tenant_id),
            )
        })
        .collect()
}

fn extract_database_table_name(full_name: &str, session: &IsiphoSessionCtx) -> (String, String) {
    let table_ref = TableReference::from(full_name);
    let resloved_table = table_ref.resolve(session.tenant(), session.default_database());
    let table_name = resloved_table.table.to_string();
    let database_name = resloved_table.schema.to_string();

    (database_name, table_name)
}

#[cfg(test)]
mod tests {
    use crate::sql::parser::ExtParser;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::logical_expr::{Aggregate, AggregateUDF, ScalarUDF, TableSource};
    use datafusion::sql::planner::ContextProvider;
    use datafusion::sql::TableReference;
    use models::auth::user::{User, UserDesc, UserOptions};
    use models::schema::Tenant;
    use spi::query::session::IsiphoSessionCtxFactory;
    use spi::service::protocol::ContextBuilder;
    use std::any::Any;
    use std::ops::Deref;
    use std::sync::Arc;

    use super::*;
    use datafusion::error::Result;
    use meta::error::MetaError;
    use models::codec::Encoding;

    #[derive(Debug)]
    struct MockContext {}

    impl ContextProviderExtension for MockContext {
        fn get_user(&self, _name: &str) -> std::result::Result<UserDesc, MetaError> {
            todo!()
        }

        fn get_tenant(&self, _name: &str) -> std::result::Result<Tenant, MetaError> {
            todo!()
        }

        fn reset_access_databases(&self) -> crate::metadata::DatabaseSet {
            todo!()
        }
    }

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

    fn session() -> IsiphoSessionCtx {
        let user_desc = UserDesc::new(
            0_u128,
            "test_name".to_string(),
            UserOptions::default(),
            false,
        );
        let user = User::new(user_desc, HashSet::default());
        let context = ContextBuilder::new(user).build();
        IsiphoSessionCtxFactory::default().create_isipho_session_ctx(context, 0_u128)
    }

    #[test]
    fn test_drop() {
        let sql = "drop table if exists test_tb";
        let mut statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        let test = MockContext {};
        let planner = SqlPlaner::new(test);
        let plan = planner
            .statement_to_plan(statements.pop_back().unwrap(), &session())
            .unwrap();
        if let Plan::DDL(DDLPlan::DropDatabaseObject(drop)) = plan.plan {
            println!("{:?}", drop);
        } else {
            panic!("expected drop plan")
        }
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
            .statement_to_plan(statements.pop_back().unwrap(), &session())
            .unwrap();
        if let Plan::DDL(DDLPlan::CreateTable(create)) = plan.plan {
            assert_eq!(
                create,
                CreateTable {
                    schema: vec![
                        TableColumn {
                            id: 0,
                            name: "time".to_string(),
                            column_type: ColumnType::Time,
                            encoding: Encoding::Default
                        },
                        TableColumn {
                            id: 1,
                            name: "column6".to_string(),
                            column_type: ColumnType::Tag,
                            encoding: Encoding::Default,
                        },
                        TableColumn {
                            id: 2,
                            name: "column7".to_string(),
                            column_type: ColumnType::Tag,
                            encoding: Encoding::Default,
                        },
                        TableColumn {
                            id: 3,
                            name: "column1".to_string(),
                            column_type: ColumnType::Field(ValueType::Integer),
                            encoding: Encoding::Delta,
                        },
                        TableColumn {
                            id: 4,
                            name: "column2".to_string(),
                            column_type: ColumnType::Field(ValueType::String),
                            encoding: Encoding::Gzip,
                        },
                        TableColumn {
                            id: 5,
                            name: "column3".to_string(),
                            column_type: ColumnType::Field(ValueType::Unsigned),
                            encoding: Encoding::Null,
                        },
                        TableColumn {
                            id: 6,
                            name: "column4".to_string(),
                            column_type: ColumnType::Field(ValueType::Boolean),
                            encoding: Encoding::Default,
                        },
                        TableColumn {
                            id: 7,
                            name: "column5".to_string(),
                            column_type: ColumnType::Field(ValueType::Float),
                            encoding: Encoding::Gorilla,
                        }
                    ],
                    name: "test".to_string(),
                    if_not_exists: true
                }
            );
        } else {
            panic!("expected create table plan")
        }
    }

    #[test]
    fn test_create_database() {
        let sql = "CREATE DATABASE test WITH TTL '10' SHARD 5 VNODE_DURATION '3d' REPLICA 10 PRECISION 'us';";
        let mut statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        let test = MockContext {};
        let planner = SqlPlaner::new(test);
        let plan = planner
            .statement_to_plan(statements.pop_back().unwrap(), &session())
            .unwrap();
        if let Plan::DDL(DDLPlan::CreateDatabase(create)) = plan.plan {
            let ans = format!("{:?}", create);
            println!("{ans}");
            let expected = r#"CreateDatabase { name: "test", if_not_exists: false, options: DatabaseOptions { ttl: Some(Duration { time_num: 10, unit: Day }), shard_num: Some(5), vnode_duration: Some(Duration { time_num: 3, unit: Day }), replica: Some(10), precision: Some(US) } }"#;
            assert_eq!(ans, expected);
        } else {
            panic!("expected create table plan")
        }
    }

    #[test]
    fn test_create_table_filed_name_same() {
        let sql = "CREATE TABLE air (visibility DOUBLE,temperature DOUBLE,pressure DOUBLE,pressure DOUBLE,TAGS(station));";
        let mut statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        let test = MockContext {};
        let planner = SqlPlaner::new(test);
        let error = planner
            .statement_to_plan(statements.pop_back().unwrap(), &session())
            .err()
            .unwrap();
        assert!(matches!(error, QueryError::SameColumnName {column} if column.eq("pressure")));
    }

    #[test]
    fn test_create_table_tag_name_same() {
        let sql = "CREATE TABLE air (visibility DOUBLE,temperature DOUBLE,presssure DOUBLE,TAGS(station,station));";
        let mut statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        let test = MockContext {};
        let planner = SqlPlaner::new(test);
        let error = planner
            .statement_to_plan(statements.pop_back().unwrap(), &session())
            .err()
            .unwrap();
        assert!(matches!(error, QueryError::SameColumnName {column} if column.eq("station")));
    }

    #[test]
    fn test_create_table_tag_field_same_name() {
        let sql = "CREATE TABLE air (visibility DOUBLE,temperature DOUBLE,pressure DOUBLE,TAGS(station,pressure));";
        let mut statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        let test = MockContext {};
        let planner = SqlPlaner::new(test);
        let error = planner
            .statement_to_plan(statements.pop_back().unwrap(), &session())
            .err()
            .unwrap();
        assert!(matches!(error, QueryError::SameColumnName {column} if column.eq("pressure")));
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
            .statement_to_plan(statements.pop_back().unwrap(), &session())
            .unwrap();

        match plan.plan {
            Plan::Query(QueryPlan {
                df_plan: LogicalPlan::Aggregate(Aggregate { input, .. }),
            }) => match input.as_ref() {
                LogicalPlan::Extension(Extension { node }) => {
                    match node.as_any().downcast_ref::<TableWriterPlanNode>() {
                        Some(TableWriterPlanNode {
                            target_table_name, ..
                        }) => {
                            assert_eq!(target_table_name.deref(), "test_tb");
                        }
                        _ => panic!(),
                    }
                }
                _ => panic!(),
            },
            _ => panic!(),
        }
    }
}
