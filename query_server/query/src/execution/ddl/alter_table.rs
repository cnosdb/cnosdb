use std::sync::Arc;

use async_trait::async_trait;
use coordinator::resource_manager::ResourceManager;
use meta::error::MetaError;
use models::oid::Identifier;
use models::schema::resource_info::{ResourceInfo, ResourceOperator};
use models::schema::table_schema::TableSchema;
use models::schema::tskv_table_schema::{TableColumn, TskvTableSchema};
use snafu::ResultExt;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::{AlterTable, AlterTableAction};
use spi::{CoordinatorSnafu, MetaSnafu, QueryError, QueryResult};

// use crate::execution::ddl::query::spi::MetaSnafu;
use crate::execution::ddl::DDLDefinitionTask;

pub struct AlterTableTask {
    stmt: AlterTable,
}

impl AlterTableTask {
    pub fn new(stmt: AlterTable) -> AlterTableTask {
        Self { stmt }
    }
}
#[async_trait]
impl DDLDefinitionTask for AlterTableTask {
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> QueryResult<Output> {
        let table_name = &self.stmt.table_name;
        let tenant = table_name.tenant();
        let client = query_state_machine
            .meta
            .tenant_meta(tenant)
            .await
            .ok_or_else(|| MetaError::TenantNotFound {
                tenant: tenant.to_string(),
            })
            .context(MetaSnafu)?;

        let mut schema = client
            .get_tskv_table_schema(table_name.database(), table_name.table())
            .context(MetaSnafu)?
            .ok_or_else(|| MetaError::TableNotFound {
                table: table_name.to_string(),
            })
            .context(MetaSnafu)?
            .as_ref()
            .clone();

        let operator_info = match &self.stmt.alter_action {
            AlterTableAction::AddColumn { table_column } => {
                let table_column = table_column.to_owned();
                schema.add_column(table_column.clone());
                schema.schema_version += 1;

                Some((
                    table_column.name.clone(),
                    ResourceOperator::AddColumn(schema.clone(), table_column),
                ))
            }

            AlterTableAction::DropColumn { column_name } => {
                schema.drop_column(column_name);
                schema.schema_version += 1;

                Some((
                    column_name.clone(),
                    ResourceOperator::DropColumn(column_name.clone(), schema.clone()),
                ))
            }

            AlterTableAction::AlterColumn {
                column_name,
                new_column,
            } => {
                if !new_column.encoding_valid() {
                    return Err(QueryError::EncodingType {
                        encoding_type: new_column.encoding,
                        data_type: new_column.column_type.to_string(),
                    });
                }
                schema.alter_column(column_name, new_column.clone());
                schema.schema_version += 1;

                Some((
                    column_name.clone(),
                    ResourceOperator::AlterColumn(
                        column_name.clone(),
                        schema.clone(),
                        new_column.clone(),
                    ),
                ))
            }
            AlterTableAction::RenameColumn {
                old_column_name,
                new_column_name,
            } => {
                let alter_schema_func =
                    |schema: &mut TskvTableSchema, old_column_name: &str, new_name: &str| {
                        if let Some(old_column) = schema.get_column_by_name(old_column_name) {
                            let new_column = TableColumn::new(
                                old_column.id,
                                new_name.to_string(),
                                old_column.column_type.clone(),
                                old_column.encoding,
                            );
                            schema.alter_column(old_column_name, new_column);
                            schema.schema_version += 1;
                        } else {
                            return Err(QueryError::ColumnNotFound {
                                col: old_column_name.to_owned(),
                            });
                        }

                        Ok(())
                    };

                alter_schema_func(&mut schema, old_column_name, new_column_name)?;
                None
            }
        };

        if let Some(info) = operator_info {
            let resourceinfo = ResourceInfo::new(
                (*client.tenant().id(), table_name.database().to_string()),
                table_name.tenant().to_string()
                    + "-"
                    + table_name.database()
                    + "-"
                    + table_name.table()
                    + "-"
                    + &info.0,
                info.1,
                &None,
                query_state_machine.coord.node_id(),
            );
            ResourceManager::add_resource_task(query_state_machine.coord.clone(), resourceinfo)
                .await
                .context(CoordinatorSnafu)?;
        } else {
            client
                .update_table(&TableSchema::TsKvTableSchema(Arc::new(schema)))
                .await
                .context(MetaSnafu)?;
        }
        return Ok(Output::Nil(()));
    }
}
