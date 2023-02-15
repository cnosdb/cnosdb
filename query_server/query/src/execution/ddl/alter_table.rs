use std::sync::Arc;

use async_trait::async_trait;
use coordinator::command;
use meta::error::MetaError;
use models::schema::TableSchema;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::{AlterTable, AlterTableAction};
use spi::Result;

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
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> Result<Output> {
        let table_name = &self.stmt.table_name;
        let tenant = table_name.tenant();
        let client = query_state_machine
            .meta
            .tenant_manager()
            .tenant_meta(tenant)
            .await
            .ok_or(MetaError::TenantNotFound {
                tenant: tenant.to_string(),
            })?;

        let mut schema = client
            .get_tskv_table_schema(table_name.database(), table_name.table())?
            .ok_or(MetaError::TableNotFound {
                table: table_name.to_string(),
            })?
            .as_ref()
            .clone();

        let req = match &self.stmt.alter_action {
            AlterTableAction::AddColumn { table_column } => {
                let table_column = table_column.to_owned();
                schema.add_column(table_column.clone());

                command::AdminStatementRequest {
                    tenant: tenant.to_string(),
                    stmt: command::AdminStatementType::AddColumn {
                        db: schema.db.to_owned(),
                        table: schema.name.to_string(),
                        column: table_column,
                    },
                }
            }

            AlterTableAction::DropColumn { column_name } => {
                schema.drop_column(column_name);
                command::AdminStatementRequest {
                    tenant: tenant.to_string(),
                    stmt: command::AdminStatementType::DropColumn {
                        db: schema.db.to_owned(),
                        table: schema.name.to_string(),
                        column: column_name.clone(),
                    },
                }
            }
            AlterTableAction::AlterColumn {
                column_name,
                new_column,
            } => {
                schema.change_column(column_name, new_column.clone());
                command::AdminStatementRequest {
                    tenant: tenant.to_string(),
                    stmt: command::AdminStatementType::AlterColumn {
                        db: schema.db.to_owned(),
                        table: schema.name.to_string(),
                        column_name: column_name.to_owned(),
                        new_column: new_column.clone(),
                    },
                }
            }
        };
        schema.schema_id += 1;

        client
            .update_table(&TableSchema::TsKvTableSchema(Arc::new(schema)))
            .await?;
        query_state_machine
            .coord
            .exec_admin_stat_on_all_node(req)
            .await?;

        return Ok(Output::Nil(()));
    }
}
