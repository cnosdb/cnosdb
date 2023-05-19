use std::collections::{HashMap, HashSet};
use std::iter;
use std::option::Option;
use std::sync::Arc;

use async_recursion::async_recursion;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::arrow::error::ArrowError;
use datafusion::common::parsers::CompressionTypeVariant;
use datafusion::common::{
    Column, DFField, DFSchema, OwnedTableReference, Result as DFResult, ToDFSchema,
};
use datafusion::datasource::file_format::avro::AvroFormat;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::file_type::FileType;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::expr::Sort;
use datafusion::logical_expr::logical_plan::Analyze;
use datafusion::logical_expr::utils::expr_to_columns;
use datafusion::logical_expr::{
    lit, BinaryExpr, BuiltinScalarFunction, Case, CreateExternalTable as PlanCreateExternalTable,
    EmptyRelation, Explain, Expr, LogicalPlan, LogicalPlanBuilder, Operator, PlanType,
    SubqueryAlias, TableSource, ToStringifiedPlan, Union,
};
use datafusion::prelude::{col, SessionConfig};
use datafusion::scalar::ScalarValue;
use datafusion::sql::parser::CreateExternalTable as AstCreateExternalTable;
use datafusion::sql::planner::{object_name_to_table_reference, PlannerContext, SqlToRel};
use datafusion::sql::sqlparser::ast::{
    DataType as SQLDataType, Expr as ASTExpr, Ident, ObjectName, Offset, OrderByExpr, Query,
    SqlOption, Statement, TableAlias, TableFactor, TimezoneInfo,
};
use datafusion::sql::sqlparser::parser::ParserError;
use datafusion::sql::TableReference;
use lazy_static::__Deref;
use meta::error::MetaError;
use models::auth::privilege::{
    DatabasePrivilege, GlobalPrivilege, Privilege, TenantObjectPrivilege,
};
use models::auth::role::{SystemTenantRole, TenantRoleIdentifier};
use models::auth::user::User;
use models::object_reference::{Resolve, ResolvedTable};
use models::oid::{Identifier, Oid};
use models::schema::{
    ColumnType, DatabaseOptions, Duration, Precision, TableColumn, Tenant, TskvTableSchema,
    TskvTableSchemaRef, Watermark, DEFAULT_CATALOG, DEFAULT_DATABASE, TIME_FIELD,
};
use models::utils::SeqIdGenerator;
use models::{ColumnId, ValueType};
use object_store::ObjectStore;
use spi::query::ast;
use spi::query::ast::{
    AlterDatabase as ASTAlterDatabase, AlterTable as ASTAlterTable,
    AlterTableAction as ASTAlterTableAction, AlterTenantOperation, AlterUserOperation,
    ChecksumGroup as ASTChecksumGroup, ColumnOption, CompactVnode as ASTCompactVnode,
    CopyIntoTable, CopyTarget, CopyVnode as ASTCopyVnode, CreateDatabase as ASTCreateDatabase,
    CreateTable as ASTCreateTable, DatabaseOptions as ASTDatabaseOptions,
    DescribeDatabase as DescribeDatabaseOptions, DescribeTable as DescribeTableOptions,
    DropVnode as ASTDropVnode, ExtStatement, MoveVnode as ASTMoveVnode,
    ShowSeries as ASTShowSeries, ShowTagBody, ShowTagValues as ASTShowTagValues, UriLocation, With,
};
use spi::query::datasource::{self, UriSchema};
use spi::query::logical_planner::{
    parse_connection_options, sql_option_to_alter_tenant_action, sql_options_to_map,
    sql_options_to_tenant_options, sql_options_to_user_options,
    unset_option_to_alter_tenant_action, AlterDatabase, AlterTable, AlterTableAction, AlterTenant,
    AlterTenantAction, AlterTenantAddUser, AlterTenantSetUser, AlterUser, AlterUserAction,
    ChecksumGroup, CompactVnode, CopyOptions, CopyOptionsBuilder, CopyVnode, CreateDatabase,
    CreateRole, CreateStreamTable, CreateTable, CreateTenant, CreateUser, DDLPlan,
    DatabaseObjectType, DescribeDatabase, DescribeTable, DropDatabaseObject, DropGlobalObject,
    DropTenantObject, DropVnode, FileFormatOptions, FileFormatOptionsBuilder, GlobalObjectType,
    GrantRevoke, LogicalPlanner, MoveVnode, Plan, PlanWithPrivileges, QueryPlan, SYSPlan,
    TenantObjectType,
};
use spi::query::session::SessionCtx;
use spi::{QueryError, Result};
use trace::{debug, warn};
use url::Url;

use crate::data_source::source_downcast_adapter;
use crate::data_source::stream::{get_event_time_column, get_watermark_delay};
use crate::data_source::table_source::{TableHandle, TableSourceAdapter, TEMP_LOCATION_TABLE_NAME};
use crate::extension::logical::logical_plan_builder::LogicalPlanBuilderExt;
use crate::metadata::{ContextProviderExtension, DatabaseSet, CLUSTER_SCHEMA, INFORMATION_SCHEMA};

/// CnosDB SQL query planner
pub struct SqlPlanner<'a, S: ContextProviderExtension> {
    schema_provider: &'a S,
    df_planner: SqlToRel<'a, S>,
}

#[async_trait]
impl<'a, S: ContextProviderExtension + Send + Sync> LogicalPlanner for SqlPlanner<'a, S> {
    async fn create_logical_plan(
        &self,
        statement: ExtStatement,
        session: &SessionCtx,
    ) -> Result<Plan> {
        let PlanWithPrivileges { plan, privileges } =
            self.statement_to_plan(statement, session).await?;
        check_privilege(session.user(), privileges)?;
        Ok(plan)
    }
}

impl<'a, S: ContextProviderExtension + Send + Sync + 'a> SqlPlanner<'a, S> {
    /// Create a new query planner
    pub fn new(schema_provider: &'a S) -> Self {
        SqlPlanner {
            schema_provider,
            df_planner: SqlToRel::new(schema_provider),
        }
    }

    /// Generate a logical plan from an  Extent SQL statement
    #[async_recursion]
    pub(crate) async fn statement_to_plan(
        &self,
        statement: ExtStatement,
        session: &SessionCtx,
    ) -> Result<PlanWithPrivileges> {
        match statement {
            ExtStatement::SqlStatement(stmt) => self.df_sql_to_plan(*stmt, session).await,
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
            ExtStatement::Explain(stmt) => {
                self.explain_statement_to_plan(
                    stmt.analyze,
                    stmt.verbose,
                    *stmt.ext_statement,
                    session,
                )
                .await
            }
            ExtStatement::ShowTagValues(stmt) => self.show_tag_values(*stmt, session),
            ExtStatement::AlterTable(stmt) => self.table_to_alter(stmt, session),
            ExtStatement::AlterTenant(stmt) => self.alter_tenant_to_plan(stmt).await,
            ExtStatement::AlterUser(stmt) => self.alter_user_to_plan(stmt).await,
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
            ExtStatement::Copy(stmt) => self.copy_to_plan(stmt, session).await,
            // vnode statement
            ExtStatement::DropVnode(stmt) => self.drop_vnode_to_plan(stmt),
            ExtStatement::CopyVnode(stmt) => self.copy_vnode_to_plan(stmt),
            ExtStatement::MoveVnode(stmt) => self.move_vnode_to_plan(stmt),
            ExtStatement::CompactVnode(stmt) => self.compact_vnode_to_plan(stmt),
            ExtStatement::ChecksumGroup(stmt) => self.checksum_group_to_plan(stmt),
            ExtStatement::CreateStream(_) => Err(QueryError::NotImplemented {
                err: "CreateStream Planner.".to_string(),
            }),
            ExtStatement::DropStream(_) => Err(QueryError::NotImplemented {
                err: "DropStream Planner.".to_string(),
            }),
            ExtStatement::ShowStreams(_) => Err(QueryError::NotImplemented {
                err: "ShowStreams Planner.".to_string(),
            }),
            ExtStatement::CreateStreamTable(stmt) => {
                self.create_stream_table_to_plan(stmt, session)
            }
        }
    }

    async fn df_sql_to_plan(
        &self,
        stmt: Statement,
        session: &SessionCtx,
    ) -> Result<PlanWithPrivileges> {
        match stmt {
            Statement::Query(_) => {
                let df_plan = self.df_planner.sql_statement_to_plan(stmt)?;
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
                table_name: sql_object_name,
                columns: sql_column_names,
                source,
                ..
            } => {
                self.insert_to_plan(sql_object_name, sql_column_names, source, session)
                    .await
            }
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
    async fn explain_statement_to_plan(
        &self,
        analyze: bool,
        verbose: bool,
        statement: ExtStatement,
        session: &SessionCtx,
    ) -> Result<PlanWithPrivileges> {
        let PlanWithPrivileges { plan, privileges } =
            self.statement_to_plan(statement, session).await?;

        let input_df_plan = match plan {
            Plan::Query(query) => Arc::new(query.df_plan),
            _ => {
                return Err(QueryError::NotImplemented {
                    err: "explain non-query statement.".to_string(),
                });
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
                logical_optimization_succeeded: false,
            })
        };

        let plan = Plan::Query(QueryPlan { df_plan });

        Ok(PlanWithPrivileges { plan, privileges })
    }

    async fn insert_to_plan(
        &self,
        sql_object_name: ObjectName,
        sql_column_names: Vec<Ident>,
        source: Box<Query>,
        session: &SessionCtx,
    ) -> Result<PlanWithPrivileges> {
        // Transform subqueries
        let source_plan = self
            .df_planner
            .sql_statement_to_plan(Statement::Query(source))?;

        // save database read privileges
        // This operation must be done before fetching the target table metadata
        let mut read_privileges = databases_privileges(
            DatabasePrivilege::Read,
            *session.tenant_id(),
            self.schema_provider.reset_access_databases(),
        );

        let table_ref = normalize_sql_object_name(sql_object_name)?;
        let columns = sql_column_names
            .into_iter()
            .map(normalize_ident)
            .collect::<Vec<String>>();

        // Get the metadata of the target table
        let target_table = self.get_table_source(table_ref.clone())?;

        let build_plan_func = || {
            LogicalPlanBuilder::from(source_plan)
                .write(target_table, table_ref.table(), columns.as_ref())?
                .build()
        };

        let df_plan = build_plan_func()?;

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
        session: &SessionCtx,
    ) -> Result<PlanWithPrivileges> {
        let ast::DropDatabaseObject {
            object_name,
            if_exist,
            ref obj_type,
        } = stmt;
        // get the current tenant id from the session
        let tenant_id = *session.tenant_id();

        let (plan, privilege) = match obj_type {
            DatabaseObjectType::Table => {
                let table = object_name_to_resolved_table(session, object_name)?;
                let database_name = table.database().to_string();
                (
                    DDLPlan::DropDatabaseObject(DropDatabaseObject {
                        if_exist,
                        object_name: table,
                        obj_type: DatabaseObjectType::Table,
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
        };

        Ok(PlanWithPrivileges {
            plan: Plan::DDL(plan),
            privileges: vec![privilege],
        })
    }

    fn drop_tenant_object_to_plan(
        &self,
        stmt: ast::DropTenantObject,
        session: &SessionCtx,
    ) -> Result<PlanWithPrivileges> {
        let ast::DropTenantObject {
            object_name,
            if_exist,
            ref obj_type,
        } = stmt;
        // get the current tenant id from the session
        let tenant_name = session.tenant();
        let tenant_id = *session.tenant_id();

        let (plan, privilege) = match obj_type {
            TenantObjectType::Database => {
                let database_name = normalize_ident(object_name);
                if database_name.eq(DEFAULT_DATABASE) {
                    return Err(QueryError::ForbidDropDatabase {
                        name: database_name,
                    });
                }
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
            object_name,
            if_exist,
            ref obj_type,
        } = stmt;

        let (plan, privilege) = match obj_type {
            GlobalObjectType::Tenant => {
                let tenant_name = normalize_ident(object_name);
                if tenant_name == DEFAULT_CATALOG {
                    return Err(QueryError::ForbidDropTenant { name: tenant_name });
                }

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

    /// Copy from DataFusion
    fn df_external_table_to_plan(
        &self,
        statement: AstCreateExternalTable,
    ) -> Result<PlanCreateExternalTable> {
        let definition = Some(statement.to_string());
        let AstCreateExternalTable {
            name,
            columns,
            file_type,
            has_header,
            delimiter,
            location,
            table_partition_cols,
            if_not_exists,
            file_compression_type,
            options,
            order_exprs,
        } = statement;

        // semantic checks
        if file_type == "PARQUET" && !columns.is_empty() {
            Err(DataFusionError::Plan(
                "Column definitions can not be specified for PARQUET files.".into(),
            ))?;
        }

        if file_type != "CSV"
            && file_type != "JSON"
            && file_compression_type != CompressionTypeVariant::UNCOMPRESSED
        {
            Err(DataFusionError::Plan(
                "File compression type can be specified for CSV/JSON files.".into(),
            ))?;
        }

        let schema = self.df_planner.build_schema(columns)?;
        let df_schema = schema.to_dfschema_ref()?;

        let ordered_exprs =
            self.df_planner
                .build_order_by(order_exprs, &df_schema, &mut PlannerContext::new())?;

        // External tables do not support schemas at the moment, so the name is just a table name
        let name = OwnedTableReference::bare(name);

        Ok(PlanCreateExternalTable {
            schema: df_schema,
            name,
            location,
            file_type,
            has_header,
            delimiter,
            table_partition_cols,
            if_not_exists,
            definition,
            file_compression_type,
            options,
            order_exprs: ordered_exprs,
        })
    }

    /// Generate a logical plan from a CREATE EXTERNAL TABLE statement
    pub fn external_table_to_plan(
        &self,
        statement: AstCreateExternalTable,
        session: &SessionCtx,
    ) -> Result<PlanWithPrivileges> {
        let table_name = statement.name.clone();
        let (database_name, _) = extract_database_table_name(table_name.as_str(), session);

        let logical_plan = self.df_external_table_to_plan(statement)?;

        let plan = Plan::DDL(DDLPlan::CreateExternalTable(logical_plan));
        // privileges
        Ok(PlanWithPrivileges {
            plan,
            privileges: vec![Privilege::TenantObject(
                TenantObjectPrivilege::Database(DatabasePrivilege::Full, Some(database_name)),
                Some(*session.tenant_id()),
            )],
        })
    }

    pub fn create_table_to_plan(
        &self,
        statement: ASTCreateTable,
        session: &SessionCtx,
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

        let resolved_table = object_name_to_resolved_table(session, name)?;
        let database_name = resolved_table.database().to_string();
        let unit: TimeUnit = self.get_db_precision(&database_name)?.into();

        let time_col =
            TableColumn::new_time_column(id_generator.next_id() as ColumnId, unit.clone());
        // Append time column at the start
        schema.push(time_col);

        for column_opt in columns {
            let col_id = id_generator.next_id() as ColumnId;
            let column = self.column_opt_to_table_column(column_opt, col_id, unit.clone())?;
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

        let plan = Plan::DDL(DDLPlan::CreateTable(CreateTable {
            schema,
            name: resolved_table,
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

    fn column_opt_to_table_column(
        &self,
        column_opt: ColumnOption,
        id: ColumnId,
        unit: TimeUnit,
    ) -> Result<TableColumn> {
        Self::check_column_encoding(&column_opt)?;

        let col = if column_opt.is_tag {
            TableColumn::new_tag_column(id, normalize_ident(column_opt.name))
        } else {
            let name = normalize_ident(column_opt.name);
            let column_type = self.make_data_type(&name, &column_opt.data_type, unit)?;
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
        session: &SessionCtx,
    ) -> Result<PlanWithPrivileges> {
        // get the current tenant id from the session
        let database_name = normalize_ident(statement.database_name);

        let plan = Plan::DDL(DDLPlan::DescribeDatabase(DescribeDatabase {
            database_name: database_name.clone(),
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
        session: &SessionCtx,
    ) -> Result<PlanWithPrivileges> {
        let table_name = object_name_to_resolved_table(session, opts.table_name)?;
        let database_name = table_name.database().to_string();

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
        session: &SessionCtx,
    ) -> Result<PlanWithPrivileges> {
        let table_ref = normalize_sql_object_name(statement.table_name)?;
        let table_name = table_ref
            .clone()
            .resolve_object(session.tenant(), session.default_database())?;
        let table_schema = self.get_tskv_schema(table_ref)?;

        let time_unit = if let ColumnType::Time(ref unit) = table_schema
            .column(TIME_FIELD)
            .ok_or(QueryError::CommonError {
                msg: "schema missing time column".to_string(),
            })?
            .column_type
        {
            unit.clone()
        } else {
            return Err(QueryError::CommonError {
                msg: "time column type not match".to_string(),
            });
        };

        let alter_action = match statement.alter_action {
            ASTAlterTableAction::AddColumn { column } => {
                let mut table_column =
                    self.column_opt_to_table_column(column, ColumnId::default(), time_unit)?;
                if table_schema.contains_column(&table_column.name) {
                    return Err(QueryError::ColumnAlreadyExists {
                        table: table_schema.name.to_string(),
                        column: table_column.name,
                    });
                }
                table_column.id = table_schema.next_column_id();
                AlterTableAction::AddColumn { table_column }
            }
            ASTAlterTableAction::DropColumn { column_name } => {
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
                column_name,
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

    fn database_to_show(&self, session: &SessionCtx) -> Result<PlanWithPrivileges> {
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
        database: Option<Ident>,
        session: &SessionCtx,
    ) -> Result<PlanWithPrivileges> {
        let db_name = database.map(normalize_ident);
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
        session: &SessionCtx,
        body: ShowTagBody,
        projection_function: impl FnOnce(
            &TskvTableSchema,
            LogicalPlanBuilder,
            bool,
        ) -> Result<LogicalPlan>,
    ) -> Result<PlanWithPrivileges> {
        let ShowTagBody {
            database_name,
            table,
            selection,
            order_by,
            limit,
            offset,
        } = body;

        let table_name = database_name
            .map(|e| ObjectName(vec![e, table.clone()]))
            .unwrap_or_else(|| ObjectName(vec![table]));
        let table_ref = normalize_sql_object_name(table_name)?;

        let table_schema = self.get_tskv_schema(table_ref.clone())?;

        let (source_plan, _) =
            self.create_table_relation(table_ref, None, &mut Default::default())?;

        // build from
        let mut plan_builder = LogicalPlanBuilder::from(source_plan);

        // build where
        let selection = match selection {
            Some(expr) => Some(self.df_planner.sql_to_expr(
                expr,
                plan_builder.schema(),
                &mut Default::default(),
            )?),
            None => None,
        };

        // get all columns in expr
        let mut columns = HashSet::new();
        if let Some(selection) = &selection {
            expr_to_columns(selection, &mut columns)?;
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
        let mut plan_builder = self.order_by(order_by, plan)?;

        // build limit
        if limit.is_some() || offset.is_some() {
            plan_builder = self.limit_offset_to_plan(limit, offset, plan_builder)?;
        }

        let df_plan = plan_builder.build()?;
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
        session: &SessionCtx,
    ) -> Result<PlanWithPrivileges> {
        self.show_tag_body(session, stmt.body, show_series_projection)
    }

    fn show_tag_values(
        &self,
        stmt: ASTShowTagValues,
        session: &SessionCtx,
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
        session: &SessionCtx,
    ) -> Result<PlanWithPrivileges> {
        let ASTCreateDatabase {
            name,
            if_not_exists,
            options,
        } = stmt;

        let name = normalize_ident(name);

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

        let mut sort_exprs = Vec::new();
        for OrderByExpr {
            expr,
            asc,
            nulls_first,
        } in exprs
        {
            let expr = self
                .df_planner
                .sql_to_expr(expr, schema, &mut Default::default())?;
            let asc = asc.unwrap_or(true);
            let sort_expr = Expr::Sort(Sort {
                expr: Box::new(expr),
                asc,
                // when asc is true, by default nulls last to be consistent with postgres
                // postgres rule: https://www.postgresql.org/docs/current/queries-order.html
                nulls_first: nulls_first.unwrap_or(!asc),
            });
            sort_exprs.push(sort_expr);
        }

        Ok(LogicalPlanBuilder::from(plan).sort(sort_exprs)?)
    }

    fn limit_offset_to_plan(
        &self,
        limit: Option<ASTExpr>,
        offset: Option<Offset>,
        plan_builder: LogicalPlanBuilder,
    ) -> Result<LogicalPlanBuilder> {
        let skip = match offset {
            Some(offset) => {
                match self.df_planner.sql_to_expr(
                    offset.value,
                    plan_builder.schema(),
                    &mut Default::default(),
                )? {
                    Expr::Literal(ScalarValue::Int64(Some(m))) => {
                        if m < 0 {
                            return Err(QueryError::Semantic {
                                err: format!("OFFSET must be >= 0, '{}' was provided.", m),
                            });
                        } else {
                            m as usize
                        }
                    }
                    _ => {
                        return Err(QueryError::Semantic {
                            err: "The OFFSET clause must be a constant of BIGINT type".to_string(),
                        });
                    }
                }
            }
            None => 0,
        };

        let fetch = match limit {
            Some(exp) => match self.df_planner.sql_to_expr(
                exp,
                plan_builder.schema(),
                &mut Default::default(),
            )? {
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
        session: &SessionCtx,
    ) -> Result<PlanWithPrivileges> {
        let ASTAlterDatabase { name, options } = stmt;
        let options = self.make_database_option(options)?;
        if options.precision().is_some() {
            return Err(QueryError::Semantic {
                err: "Can not alter database precision".to_string(),
            });
        }
        let database_name = normalize_ident(name);
        let plan = Plan::DDL(DDLPlan::AlterDatabase(AlterDatabase {
            database_name: database_name.clone(),
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

    fn make_data_type(
        &self,
        column_name: &str,
        data_type: &SQLDataType,
        time_unit: TimeUnit,
    ) -> Result<ColumnType> {
        match data_type {
            // todo : should support get time unit for database
            SQLDataType::Timestamp(_, TimezoneInfo::None) => Ok(ColumnType::Time(time_unit)),
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
            SQLDataType::Timestamp(_, _) => encoding.is_timestamp_encoding(),
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

    fn get_db_precision(&self, name: &str) -> Result<Precision> {
        let precision = self.schema_provider.get_db_precision(name)?;
        Ok(precision)
    }

    fn get_table_source(&self, table_name: TableReference) -> Result<Arc<dyn TableSource>> {
        Ok(self.schema_provider.get_table_source(table_name)?)
    }

    fn create_tenant_to_plan(&self, stmt: ast::CreateTenant) -> Result<PlanWithPrivileges> {
        let ast::CreateTenant {
            name,
            if_not_exists,
            with_options,
        } = stmt;

        let name = normalize_ident(name);
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

        let name = normalize_ident(name);
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
        session: &SessionCtx,
    ) -> Result<PlanWithPrivileges> {
        let ast::CreateRole {
            name,
            if_not_exists,
            inherit,
        } = stmt;

        let role_name = normalize_ident(name);
        let tenant_name = session.tenant();
        let tenant_id = *session.tenant_id();

        let inherit_tenant_role = inherit
            .map(|e| {
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

    async fn construct_alter_tenant_action_with_privilege(
        &self,
        tenant: Tenant,
        operation: AlterTenantOperation,
    ) -> Result<(AlterTenantAction, Privilege<Oid>)> {
        let alter_tenant_action_with_privileges = match operation {
            AlterTenantOperation::AddUser(user, role) => {
                // user_id: Oid,
                // role: TenantRole<Oid>,
                // tenant_id: Oid,
                let privilege =
                    Privilege::TenantObject(TenantObjectPrivilege::MemberFull, Some(*tenant.id()));

                let user_name = normalize_ident(user);
                // 查询用户信息，不存在直接报错咯
                // fn user(
                //     &self,
                //     name: &str
                // ) -> Result<Option<UserDesc>>;
                let user_desc = self.schema_provider.get_user(&user_name).await?;
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
            AlterTenantOperation::SetUser(user, role) => {
                // user_id: Oid,
                // role: TenantRole<Oid>,
                // tenant_id: Oid,
                let privilege =
                    Privilege::TenantObject(TenantObjectPrivilege::MemberFull, Some(*tenant.id()));

                let user_name = normalize_ident(user);
                // 查询用户信息，不存在直接报错咯
                // fn user(
                //     &self,
                //     name: &str
                // ) -> Result<Option<UserDesc>>;
                let user_desc = self.schema_provider.get_user(&user_name).await?;
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
            AlterTenantOperation::RemoveUser(user) => {
                // user_id: Oid,
                // tenant_id: Oid
                let privilege =
                    Privilege::TenantObject(TenantObjectPrivilege::MemberFull, Some(*tenant.id()));

                let user_name = normalize_ident(user);
                // 查询用户信息，不存在直接报错咯
                // fn user(
                //     &self,
                //     name: &str
                // ) -> Result<Option<UserDesc>>;
                let user_desc = self.schema_provider.get_user(&user_name).await?;
                let user_id = *user_desc.id();

                (AlterTenantAction::RemoveUser(user_id), privilege)
            }
            AlterTenantOperation::Set(sql_option) => {
                sql_option_to_alter_tenant_action(tenant, sql_option)?
            }
            AlterTenantOperation::UnSet(ident) => {
                unset_option_to_alter_tenant_action(tenant, ident)?
            }
        };

        Ok(alter_tenant_action_with_privileges)
    }

    async fn alter_tenant_to_plan(&self, stmt: ast::AlterTenant) -> Result<PlanWithPrivileges> {
        let ast::AlterTenant { name, operation } = stmt;

        let tenant_name = normalize_ident(name);
        // 查询租户信息，不存在直接报错咯
        // fn tenant(
        //     &self,
        //     name: &str
        // ) -> Result<Tenant>;
        let tenant = self.schema_provider.get_tenant(&tenant_name).await?;

        let (alter_tenant_action, privilege) = self
            .construct_alter_tenant_action_with_privilege(tenant, operation)
            .await?;

        let plan = Plan::DDL(DDLPlan::AlterTenant(AlterTenant {
            tenant_name,
            alter_tenant_action,
        }));

        Ok(PlanWithPrivileges {
            plan,
            privileges: vec![privilege],
        })
    }

    async fn alter_user_to_plan(&self, stmt: ast::AlterUser) -> Result<PlanWithPrivileges> {
        let ast::AlterUser { name, operation } = stmt;

        let user_name = normalize_ident(name);
        // 查询用户信息，不存在直接报错咯
        // fn user(
        //     &self,
        //     name: &str
        // ) -> Result<Option<UserDesc>>;
        let user_desc = self.schema_provider.get_user(&user_name).await?;
        let user_id = *user_desc.id();

        let alter_user_action = match operation {
            AlterUserOperation::RenameTo(new_name) => {
                AlterUserAction::RenameTo(normalize_ident(new_name))
            }
            AlterUserOperation::Set(sql_option) => {
                let user_options = sql_options_to_user_options(vec![sql_option])?;

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
        session: &SessionCtx,
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

        let role_name = normalize_ident(role_name);
        let tenant_name = session.tenant();
        let tenant_id = *session.tenant_id();

        if SystemTenantRole::try_from(role_name.as_str()).is_ok() {
            let err = QueryError::SystemRoleModification;
            warn!("{}", err.to_string());
            return Err(err);
        }

        let database_privileges = privileges
            .into_iter()
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

    fn create_stream_table_to_plan(
        &self,
        stmt: Statement,
        session: &SessionCtx,
    ) -> Result<PlanWithPrivileges> {
        if let Statement::CreateTable {
            if_not_exists,
            name,
            columns,
            with_options,
            engine,
            ..
        } = stmt
        {
            let stream_type = engine
                .ok_or_else(|| QueryError::Analyzer {
                    err: "Engine not found.".to_string(),
                })?
                .to_ascii_lowercase();

            let resolved_table = object_name_to_resolved_table(session, name)?;
            let database_name = resolved_table.database().to_string();

            let extra_options = sql_options_to_map(&with_options);

            let watermark = Watermark {
                column: get_event_time_column(resolved_table.table(), &extra_options)?.into(),
                delay: get_watermark_delay(resolved_table.table(), &extra_options)?
                    .unwrap_or_default(),
            };

            let schema = self.df_planner.build_schema(columns)?;

            let plan = Plan::DDL(DDLPlan::CreateStreamTable(CreateStreamTable {
                if_not_exists,
                name: resolved_table,
                schema,
                stream_type,
                watermark,
                extra_options,
            }));

            // privilege
            return Ok(PlanWithPrivileges {
                plan,
                privileges: vec![Privilege::TenantObject(
                    TenantObjectPrivilege::Database(DatabasePrivilege::Full, Some(database_name)),
                    Some(*session.tenant_id()),
                )],
            });
        }

        Err(QueryError::Internal {
            reason: format!("CreateStreamTable: {stmt}"),
        })
    }

    fn get_tskv_schema(&self, table_ref: TableReference) -> Result<TskvTableSchemaRef> {
        let source = self.get_table_source(table_ref.clone())?;
        let adapter = source_downcast_adapter(&source)?;
        let result = match adapter.table_handle() {
            TableHandle::Tskv(e) => Ok(e.table_schema()),
            _ => Err(MetaError::TableNotFound {
                table: table_ref.to_string(),
            }),
        };

        Ok(result?)
    }

    async fn copy_to_plan(
        &self,
        stmt: ast::Copy,
        session: &SessionCtx,
    ) -> Result<PlanWithPrivileges> {
        let ast::Copy {
            copy_target,
            file_format_options,
            copy_options,
        } = stmt;

        let file_format_options = FileFormatOptionsBuilder::default()
            .apply_options(file_format_options)?
            .build();

        let copy_options = CopyOptionsBuilder::default()
            .apply_options(copy_options)?
            .build();

        let tenant_id = *session.tenant_id();

        match copy_target {
            CopyTarget::IntoTable(stmt) => {
                // .   TableWriter
                //         ListingTable
                let (external_location_table, target_table, insert_columns) = self
                    .build_source_and_target_table(session, stmt, file_format_options, copy_options)
                    .await?;

                let plan = build_copy_into_table_plan(
                    external_location_table,
                    target_table.clone(),
                    insert_columns.as_ref(),
                )?;

                Ok(PlanWithPrivileges {
                    plan,
                    privileges: vec![Privilege::TenantObject(
                        TenantObjectPrivilege::Database(
                            DatabasePrivilege::Write,
                            Some(target_table.database_name().into()),
                        ),
                        Some(tenant_id),
                    )],
                })
            }
            CopyTarget::IntoLocation(stmt) => {
                // .   TableWriterPlanNode
                //         Plan.....
                let plan = self
                    .copy_into_location(session, stmt, file_format_options)
                    .await?;

                let database_set = self.schema_provider.reset_access_databases();
                let privileges =
                    databases_privileges(DatabasePrivilege::Read, tenant_id, database_set);
                Ok(PlanWithPrivileges { plan, privileges })
            }
        }
    }

    /// Construct an external file as an external table
    ///
    /// Get target table‘s metadata and insert columns
    async fn build_source_and_target_table(
        &self,
        session: &SessionCtx,
        stmt: CopyIntoTable,
        file_format_options: FileFormatOptions,
        copy_options: CopyOptions,
    ) -> Result<(Arc<dyn TableSource>, Arc<TableSourceAdapter>, Vec<String>)> {
        let CopyIntoTable {
            location,
            table_name,
            columns,
        } = stmt;

        let UriLocation {
            path,
            connection_options,
        } = location;

        let CopyOptions { auto_infer_schema } = copy_options;

        let table_path = ListingTableUrl::parse(path)?;
        let insert_columns = columns.into_iter().map(normalize_ident).collect::<Vec<_>>();

        // 1. Build and register object store
        build_and_register_object_store(
            &table_path,
            connection_options,
            session.inner().runtime_env(),
        )?;

        // 2. Get the metadata of the target table
        let table_name = normalize_sql_object_name(table_name)?;
        let target_table_source = self.schema_provider.get_table_source(table_name)?;

        // 3. According to the external path, construct the external table
        let default_schema = if auto_infer_schema {
            None
        } else {
            let schema = target_table_source.schema();
            if insert_columns.is_empty() {
                // Use the schema of the insert table directly
                Some(schema)
            } else {
                // projection with specific columns
                // e.g. COPY INTO inner_csv_v2(time, tag1, tag2, bigint_c, string_c, ubigint_c, boolean_c, double_c)
                let indices = insert_columns
                    .iter()
                    .map(|e| schema.index_of(e))
                    .collect::<std::result::Result<Vec<usize>, ArrowError>>()?;
                Some(Arc::new(schema.project(&indices)?))
            }
        };
        let external_location_table_source = build_external_location_table_source(
            session,
            table_path,
            default_schema,
            file_format_options,
            session.inner().copied_config(),
        )
        .await?;

        Ok((
            external_location_table_source,
            target_table_source,
            insert_columns,
        ))
    }

    async fn copy_into_location(
        &self,
        session: &SessionCtx,
        stmt: ast::CopyIntoLocation,
        file_format_options: FileFormatOptions,
    ) -> Result<Plan> {
        let ast::CopyIntoLocation { from, location } = stmt;
        let UriLocation {
            path,
            connection_options,
        } = location;

        let table_path = ListingTableUrl::parse(path)?;

        // 1. Build and register object store
        build_and_register_object_store(
            &table_path,
            connection_options,
            session.inner().runtime_env(),
        )?;

        // 2. build source plan
        let source_plan = self.create_relation(from, &mut Default::default())?;
        let source_schem = SchemaRef::new(source_plan.schema().deref().into());

        // 3. According to the external path, construct the external table
        let target_table = build_external_location_table_source(
            session,
            table_path,
            Some(source_schem),
            file_format_options,
            session.inner().copied_config(),
        )
        .await?;

        // 4. build final plan
        let df_plan = LogicalPlanBuilder::from(source_plan)
            .write(target_table, TEMP_LOCATION_TABLE_NAME, Default::default())?
            .build()?;

        Ok(Plan::Query(QueryPlan { df_plan }))
    }

    fn create_table_relation(
        &self,
        table_ref: OwnedTableReference,
        alias: Option<TableAlias>,
        ctes: &mut HashMap<String, LogicalPlan>,
    ) -> DFResult<(LogicalPlan, Option<TableAlias>)> {
        let table_name = table_ref.to_string();
        let cte = ctes.get(&table_name);
        Ok((
            match (
                cte,
                self.schema_provider.get_table_provider(table_ref.clone()),
            ) {
                (Some(cte_plan), _) => Ok(cte_plan.clone()),
                (_, Ok(provider)) => LogicalPlanBuilder::scan(table_ref, provider, None)?.build(),
                (None, Err(e)) => Err(e),
            }?,
            alias,
        ))
    }

    fn create_relation(
        &self,
        relation: TableFactor,
        ctes: &mut HashMap<String, LogicalPlan>,
    ) -> DFResult<LogicalPlan> {
        let (plan, alias) = match relation {
            TableFactor::Table { name, alias, .. } => {
                // normalize name and alias
                let table_ref = normalize_sql_object_name(name)?;
                self.create_table_relation(table_ref, alias, ctes)?
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let logical_plan = self
                    .df_planner
                    .sql_statement_to_plan(Statement::Query(subquery))?;
                (logical_plan, alias)
            }
            // @todo Support TableFactory::TableFunction?
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported ast node {:?} in create_relation",
                    relation
                )));
            }
        };
        if let Some(alias) = alias {
            self.apply_table_alias(plan, alias)
        } else {
            Ok(plan)
        }
    }

    /// Apply the given TableAlias to the top-level projection.
    fn apply_table_alias(&self, plan: LogicalPlan, alias: TableAlias) -> DFResult<LogicalPlan> {
        let apply_name_plan =
            LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(plan, normalize_ident(alias.name))?);

        self.apply_expr_alias(apply_name_plan, alias.columns)
    }

    fn apply_expr_alias(&self, plan: LogicalPlan, idents: Vec<Ident>) -> DFResult<LogicalPlan> {
        if idents.is_empty() {
            Ok(plan)
        } else if idents.len() != plan.schema().fields().len() {
            Err(DataFusionError::Plan(format!(
                "Source table contains {} columns but only {} names given as column alias",
                plan.schema().fields().len(),
                idents.len(),
            )))
        } else {
            let fields = plan.schema().fields().clone();
            LogicalPlanBuilder::from(plan)
                .project(
                    fields
                        .iter()
                        .zip(idents.into_iter())
                        .map(|(field, ident)| col(field.name()).alias(normalize_ident(ident))),
                )?
                .build()
        }
    }
}

fn build_copy_into_table_plan(
    external_location_table: Arc<dyn TableSource>,
    target_table: Arc<TableSourceAdapter>,
    insert_columns: &[String],
) -> DFResult<Plan> {
    let df_plan =
        LogicalPlanBuilder::scan(TEMP_LOCATION_TABLE_NAME, external_location_table, None)?
            .write(
                target_table.clone(),
                target_table.table_name(),
                insert_columns,
            )?
            .build()?;

    debug!("Copy into table plan:\n{}", df_plan.display_indent_schema());

    Ok(Plan::Query(QueryPlan { df_plan }))
}

fn build_and_register_object_store(
    table_path: &ListingTableUrl,
    connection_options: Vec<SqlOption>,
    runtime_env: Arc<RuntimeEnv>,
) -> Result<()> {
    let url: &Url = table_path.as_ref();
    let bucket = url.host_str();
    let schema = table_path.scheme();

    trace::debug!(
        "Build object store for path: {:?}, bucket: {:?}, options: {:?}",
        table_path,
        bucket,
        connection_options
    );

    // local file will not object_store
    if let Some(object_store) = build_object_store(schema, bucket, connection_options)? {
        debug!(
            "Register object store, schema: {}, bucket: {}",
            schema,
            bucket.unwrap_or_default()
        );
        runtime_env.register_object_store(url, object_store);
    }

    Ok(())
}

fn build_object_store(
    schema: &str,
    bucket: Option<&str>,
    connection_options: Vec<SqlOption>,
) -> Result<Option<Arc<dyn ObjectStore>>> {
    let uri_schema = UriSchema::from(schema);
    let parsed_connection_options =
        parse_connection_options(&uri_schema, bucket, connection_options)?;

    Ok(datasource::build_object_store(parsed_connection_options)?)
}

async fn build_external_location_table_source(
    ctx: &SessionCtx,
    table_path: ListingTableUrl,
    default_schema: Option<SchemaRef>,
    file_format_options: FileFormatOptions,
    session_config: SessionConfig,
) -> datafusion::common::Result<Arc<dyn TableSource>> {
    let (file_extension, file_format) = build_file_extension_and_format(file_format_options)?;
    let external_location_table = build_listing_table(
        &ctx.inner().state(),
        table_path,
        default_schema,
        file_extension,
        file_format,
        session_config,
    )
    .await?;

    let external_location_table_source = Arc::new(TableSourceAdapter::try_new(
        TableReference::bare(TEMP_LOCATION_TABLE_NAME),
        "tmp",
        TEMP_LOCATION_TABLE_NAME,
        external_location_table,
    )?);

    Ok(external_location_table_source)
}

async fn build_listing_table(
    ctx: &SessionState,
    table_path: ListingTableUrl,
    default_schema: Option<SchemaRef>,
    file_extension: String,
    file_format: Arc<dyn FileFormat>,
    session_config: SessionConfig,
) -> datafusion::common::Result<Arc<ListingTable>> {
    let options = ListingOptions::new(file_format)
        .with_collect_stat(session_config.collect_statistics())
        .with_file_extension(file_extension)
        .with_target_partitions(session_config.target_partitions());

    let schema = if let Some(schema) = default_schema {
        schema
    } else {
        debug!("Not has default schema, infer schema, path: {}", table_path);
        options.infer_schema(ctx, &table_path).await?
    };

    debug!("ListingTable schema: {:?}", schema);

    let config = ListingTableConfig::new(table_path)
        .with_listing_options(options)
        // Use the schema of the target table
        .with_schema(schema);

    Ok(Arc::new(ListingTable::try_new(config)?))
}

fn build_file_extension_and_format(
    file_format_options: FileFormatOptions,
) -> datafusion::common::Result<(String, Arc<dyn FileFormat>)> {
    let FileFormatOptions {
        file_type,
        delimiter,
        with_header,
        file_compression_type,
    } = file_format_options;
    let file_extension = file_type.get_ext_with_compression(file_compression_type.to_owned())?;
    let file_format: Arc<dyn FileFormat> = match file_type {
        FileType::CSV => Arc::new(
            CsvFormat::default()
                .with_has_header(with_header)
                .with_delimiter(delimiter as u8)
                .with_file_compression_type(file_compression_type),
        ),
        FileType::PARQUET => Arc::new(ParquetFormat::default()),
        FileType::AVRO => Arc::new(AvroFormat::default()),
        FileType::JSON => {
            Arc::new(JsonFormat::default().with_file_compression_type(file_compression_type))
        }
    };

    Ok((file_extension, file_format))
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
                });
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
        let exp = tags.iter().map(|tag| col(&tag.name)).collect::<Vec<Expr>>();
        plan_builder = plan_builder.project(exp)?.distinct()?;
    };

    // concat tag_key=tag_value projection
    let tag_concat_expr_iter = tags.iter().map(|tag| {
        let column_expr = Box::new(Expr::Column(Column::new_unqualified(&tag.name)));
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
    Ok(plan_builder.project(iter::once(concat_ws))?.build()?)
}

fn show_tag_value_projections(
    table_schema: &TskvTableSchema,
    mut plan_builder: LogicalPlanBuilder,
    where_contain_time: bool,
    with: With,
) -> Result<LogicalPlan> {
    let mut tag_key_filter: Box<dyn FnMut(&TableColumn) -> bool> = match with {
        With::Equal(ident) => {
            Box::new(move |column| normalize_ident(ident.clone()).eq(&column.name))
        }
        With::UnEqual(ident) => {
            Box::new(move |column| normalize_ident(ident.clone()).ne(&column.name))
        }
        With::In(idents) => Box::new(move |column| {
            idents
                .clone()
                .into_iter()
                .map(normalize_ident)
                .any(|name| column.name.eq(&name))
        }),
        With::NotIn(idents) => Box::new(move |column| {
            idents
                .clone()
                .into_iter()
                .map(normalize_ident)
                .all(|name| column.name.ne(&name))
        }),
        _ => {
            return Err(QueryError::NotImplemented {
                err: "Not implemented Match, UnMatch".to_string(),
            });
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
            schema: Arc::new(DFSchema::new_with_metadata(
                vec![
                    DFField::new_unqualified("key", DataType::Utf8, false),
                    DFField::new_unqualified("value", DataType::Utf8, false),
                ],
                HashMap::new(),
            )?),
        }));
    }

    // If the time column is included,
    //   all field columns will be scanned at rewrite_tag_scan,
    //   so this projection needs to be added
    if where_contain_time {
        let exprs = tags
            .iter()
            .map(|tag| col(Column::new_unqualified(&tag.name)))
            .collect::<Vec<Expr>>();

        plan_builder = plan_builder.project(exprs)?.distinct()?;
    };

    let mut projections = Vec::new();
    for tag in tags {
        let key_column = lit(&tag.name).alias("key");
        let value_column = col(Column::new_unqualified(&tag.name)).alias("value");
        let projection = plan_builder
            .clone()
            .project(vec![key_column, value_column.clone()])?;
        let filter_expr = value_column.is_not_null();
        let projection = projection.filter(filter_expr)?.distinct()?.build()?;
        projections.push(Arc::new(projection));
    }
    let df_schema = projections[0].schema().clone();

    let union = LogicalPlan::Union(Union {
        inputs: projections,
        schema: df_schema,
    });
    let union_distinct = LogicalPlanBuilder::from(union).distinct()?.build()?;

    Ok(union_distinct)
}

fn check_privilege(user: &User, privileges: Vec<Privilege<Oid>>) -> Result<()> {
    let privileges_str = privileges
        .iter()
        .map(|e| format!("{:?}", e))
        .collect::<Vec<String>>()
        .join(",");
    debug!("logical_plan's privileges: [{}]", privileges_str);

    for p in privileges.iter() {
        if !user.check_privilege(p) {
            return Err(QueryError::InsufficientPrivileges {
                privilege: format!("{}", p),
            });
        }
    }
    Ok(())
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

fn extract_database_table_name(full_name: &str, session: &SessionCtx) -> (String, String) {
    let table_ref = TableReference::from(full_name);
    let resloved_table = table_ref.resolve(session.tenant(), session.default_database());
    let table_name = resloved_table.table.to_string();
    let database_name = resloved_table.schema.to_string();

    (database_name, table_name)
}

fn object_name_to_resolved_table(
    session: &SessionCtx,
    object_name: ObjectName,
) -> Result<ResolvedTable> {
    let table = normalize_sql_object_name(object_name)?
        .resolve_object(session.tenant(), session.default_database())?;

    Ok(table)
}

// merge "catalog.db" and "db.table" to "catalog.db.table"
// if a.b and b.c => a.b.c
// if a.b and c.d => None
pub fn merge_object_name(db: Option<ObjectName>, table: Option<ObjectName>) -> Option<ObjectName> {
    let (db, table) = match (db, table) {
        (Some(db), Some(table)) => (db, table),
        (Some(db), None) => return Some(db),
        (None, Some(table)) => return Some(table),
        (None, None) => return None,
    };

    let mut db: Vec<Ident> = db.0;
    let mut table: Vec<Ident> = table.0;
    debug!("merge_object_name: db: {:#?} table: {:#?}", db, table);

    if db.is_empty() {
        return Some(ObjectName(table));
    }

    if table.len() == 1 {
        db.append(&mut table);
        return Some(ObjectName(db));
    }

    if let Some(db_name) = db.last() {
        if let Some(table_db_name) = table.get(table.len() - 2) {
            if !db_name.eq(table_db_name) {
                return None;
            } else {
                let ident = table.remove(table.len() - 1);
                db.push(ident);
                return Some(ObjectName(db));
            }
        }
    }
    None
}

// Normalize an identifier to a lowercase string unless the identifier is quoted.
pub fn normalize_ident(id: Ident) -> String {
    match id.quote_style {
        Some(_) => id.value,
        None => id.value.to_ascii_lowercase(),
    }
}

/// Normalize a SQL object name
pub fn normalize_sql_object_name(
    sql_object_name: ObjectName,
) -> Result<OwnedTableReference, DataFusionError> {
    object_name_to_table_reference(sql_object_name, true)
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::TimeUnit::Nanosecond;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::datasource::TableProvider;
    use datafusion::error::Result;
    use datafusion::execution::memory_pool::UnboundedMemoryPool;
    use datafusion::logical_expr::logical_plan::AggWithGrouping;
    use datafusion::logical_expr::{AggregateUDF, Extension, ScalarUDF, TableSource, TableType};
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::sql::planner::ContextProvider;
    use datafusion::sql::TableReference;
    use lazy_static::__Deref;
    use meta::error::MetaError;
    use models::auth::user::{User, UserDesc, UserOptions};
    use models::codec::Encoding;
    use models::schema::Tenant;
    use spi::query::session::SessionCtxFactory;
    use spi::service::protocol::ContextBuilder;

    use super::*;
    use crate::data_source::table_source::TableSourceAdapter;
    use crate::extension::logical::plan_node::table_writer::TableWriterPlanNode;
    use crate::metadata::ContextProviderExtension;
    use crate::sql::parser::ExtParser;

    #[derive(Debug)]
    struct MockContext {}

    #[async_trait::async_trait]
    impl ContextProviderExtension for MockContext {
        async fn get_user(&self, _name: &str) -> std::result::Result<UserDesc, MetaError> {
            todo!()
        }

        async fn get_tenant(&self, _name: &str) -> std::result::Result<Tenant, MetaError> {
            todo!()
        }

        fn reset_access_databases(&self) -> crate::metadata::DatabaseSet {
            Default::default()
        }

        fn get_db_precision(&self, _name: &str) -> std::result::Result<Precision, MetaError> {
            Ok(Precision::NS)
        }

        fn get_table_source(
            &self,
            name: TableReference,
        ) -> datafusion::common::Result<Arc<TableSourceAdapter>> {
            let schema = match name.table() {
                "test_tb" => Ok(Schema::new(vec![
                    Field::new("field_int", DataType::Int32, false),
                    Field::new("field_string", DataType::Utf8, false),
                ])),
                _ => {
                    unimplemented!("use test_tb for test")
                }
            };
            let table = match schema {
                Ok(tb) => Arc::new(TestTable::new(Arc::new(tb))),
                Err(e) => return Err(e),
            };

            Ok(Arc::new(TableSourceAdapter::try_new(
                name.to_owned_reference(),
                "public",
                name.table(),
                table as Arc<dyn TableProvider>,
            )?))
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

        fn options(&self) -> &datafusion::config::ConfigOptions {
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

    #[async_trait]
    impl TableProvider for TestTable {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            self.table_schema.clone()
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        async fn scan(
            &self,
            _state: &SessionState,
            _projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _agg_with_grouping: Option<&AggWithGrouping>,
            _limit: Option<usize>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            todo!()
        }
    }

    fn session() -> SessionCtx {
        let user_desc = UserDesc::new(
            0_u128,
            "test_name".to_string(),
            UserOptions::default(),
            false,
        );
        let user = User::new(user_desc, HashSet::default());
        let context = ContextBuilder::new(user).build();
        let pool = UnboundedMemoryPool::default();
        SessionCtxFactory::default()
            .create_session_ctx("", context, 0_u128, Arc::new(pool))
            .unwrap()
    }

    #[tokio::test]
    async fn test_drop() {
        let sql = "drop table if exists test_tb";
        let mut statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        let test = MockContext {};
        let planner = SqlPlanner::new(&test);
        let plan = planner
            .statement_to_plan(statements.pop_back().unwrap(), &session())
            .await
            .unwrap();
        if let Plan::DDL(DDLPlan::DropDatabaseObject(drop)) = plan.plan {
            println!("{:?}", drop);
        } else {
            panic!("expected drop plan")
        }
    }

    #[tokio::test]
    async fn test_create_table() {
        let sql = "CREATE TABLE IF NOT EXISTS default_schema.test\
            (column1 BIGINT CODEC(DELTA),\
            column2 STRING CODEC(GZIP),\
            column3 BIGINT UNSIGNED CODEC(NULL),\
            column4 BOOLEAN,\
            column5 DOUBLE CODEC(GORILLA),\
            TAGS(column6, column7))";
        let mut statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        let test = MockContext {};
        let planner = SqlPlanner::new(&test);
        let plan = planner
            .statement_to_plan(statements.pop_back().unwrap(), &session())
            .await
            .unwrap();
        if let Plan::DDL(DDLPlan::CreateTable(create)) = plan.plan {
            assert_eq!(
                create,
                CreateTable {
                    schema: vec![
                        TableColumn {
                            id: 0,
                            name: "time".to_string(),
                            column_type: ColumnType::Time(Nanosecond),
                            encoding: Encoding::Default,
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
                        },
                    ],
                    name: TableReference::parse_str("default_schema.test")
                        .resolve_object("cnosdb", "default_schema")
                        .unwrap(),
                    if_not_exists: true,
                }
            );
        } else {
            panic!("expected create table plan")
        }
    }

    #[tokio::test]
    async fn test_create_database() {
        let sql = "CREATE DATABASE test WITH TTL '10' SHARD 5 VNODE_DURATION '3d' REPLICA 10 PRECISION 'us';";
        let mut statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        let test = MockContext {};
        let planner = SqlPlanner::new(&test);
        let plan = planner
            .statement_to_plan(statements.pop_back().unwrap(), &session())
            .await
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

    #[tokio::test]
    async fn test_create_table_filed_name_same() {
        let sql = "CREATE TABLE air (visibility DOUBLE,temperature DOUBLE,pressure DOUBLE,pressure DOUBLE,TAGS(station));";
        let mut statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        let test = MockContext {};
        let planner = SqlPlanner::new(&test);
        let error = planner
            .statement_to_plan(statements.pop_back().unwrap(), &session())
            .await
            .err()
            .unwrap();
        assert!(matches!(error, QueryError::SameColumnName {column} if column.eq("pressure")));
    }

    #[tokio::test]
    async fn test_create_table_tag_name_same() {
        let sql = "CREATE TABLE air (visibility DOUBLE,temperature DOUBLE,presssure DOUBLE,TAGS(station,station));";
        let mut statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        let test = MockContext {};
        let planner = SqlPlanner::new(&test);
        let error = planner
            .statement_to_plan(statements.pop_back().unwrap(), &session())
            .await
            .err()
            .unwrap();
        assert!(matches!(error, QueryError::SameColumnName {column} if column.eq("station")));
    }

    #[tokio::test]
    async fn test_create_table_tag_field_same_name() {
        let sql = "CREATE TABLE air (visibility DOUBLE,temperature DOUBLE,pressure DOUBLE,TAGS(station,pressure));";
        let mut statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        let test = MockContext {};
        let planner = SqlPlanner::new(&test);
        let error = planner
            .statement_to_plan(statements.pop_back().unwrap(), &session())
            .await
            .err()
            .unwrap();
        assert!(matches!(error, QueryError::SameColumnName {column} if column.eq("pressure")));
    }

    #[tokio::test]
    async fn test_insert_select() {
        let sql = "insert test_tb(field_int, field_string)
                         select column1, column2
                         from
                         (values
                             (7, '7a'));";
        let mut statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        let test = MockContext {};
        let planner = SqlPlanner::new(&test);
        let plan = planner
            .statement_to_plan(statements.pop_back().unwrap(), &session())
            .await
            .unwrap();

        match plan.plan {
            Plan::Query(QueryPlan {
                df_plan: LogicalPlan::Extension(Extension { node }),
            }) => match &node.inputs()[0] {
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
