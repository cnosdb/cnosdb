use std::sync::Arc;

use async_trait::async_trait;
use meta::error::MetaError;
use models::schema::{TableColumn, TableSchema, TskvTableSchema};
use protos::kv_service::admin_command_request::Command;
use protos::kv_service::{
    AddColumnRequest, AdminCommandRequest, AlterColumnRequest, DropColumnRequest,
    RenameColumnRequest,
};
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::{AlterTable, AlterTableAction, RenameColumnAction};
use spi::{QueryError, Result};

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
        let client = query_state_machine.meta.tenant_meta(tenant).await.ok_or(
            MetaError::TenantNotFound {
                tenant: tenant.to_string(),
            },
        )?;

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

                Some(AdminCommandRequest {
                    tenant: tenant.to_string(),
                    command: Some(Command::AddColumn(AddColumnRequest {
                        db: schema.db.to_owned(),
                        table: schema.name.to_string(),
                        column: table_column.encode()?,
                    })),
                })
            }

            AlterTableAction::DropColumn { column_name } => {
                schema.drop_column(column_name);
                Some(AdminCommandRequest {
                    tenant: tenant.to_string(),
                    command: Some(Command::DropColumn(DropColumnRequest {
                        db: schema.db.to_owned(),
                        table: schema.name.to_string(),
                        column: column_name.clone(),
                    })),
                })
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
                schema.change_column(column_name, new_column.clone());
                Some(AdminCommandRequest {
                    tenant: tenant.to_string(),
                    command: Some(Command::AlterColumn(AlterColumnRequest {
                        db: schema.db.to_owned(),
                        table: schema.name.to_string(),
                        name: column_name.to_owned(),
                        column: new_column.encode()?,
                    })),
                })
            }
            AlterTableAction::RenameColumn {
                old_column_name,
                new_column_name,
            } => {
                let alter_schema_func =
                    |schema: &mut TskvTableSchema, old_column_name: &str, new_name: &str| {
                        if let Some(old_column) = schema.column(old_column_name) {
                            let new_column = TableColumn::new(
                                old_column.id,
                                new_name.to_string(),
                                old_column.column_type.clone(),
                                old_column.encoding,
                            );
                            schema.change_column(old_column_name, new_column);
                        } else {
                            return Err(QueryError::ColumnNotFound {
                                col: old_column_name.to_owned(),
                            });
                        }

                        Ok(())
                    };

                match new_column_name {
                    RenameColumnAction::RenameTag(new_name) => {
                        alter_schema_func(&mut schema, old_column_name, new_name)?;

                        // construct check req
                        let mut rename_check_req = RenameColumnRequest {
                            db: schema.db.to_owned(),
                            table: schema.name.to_string(),
                            old_name: old_column_name.to_owned(),
                            new_name: new_name.to_owned(),
                            dry_run: true,
                        };
                        let check_req = AdminCommandRequest {
                            tenant: tenant.to_string(),
                            command: Some(Command::RenameColumn(rename_check_req.clone())),
                        };
                        // construct execute req
                        rename_check_req.dry_run = false;
                        let exec_req = AdminCommandRequest {
                            tenant: tenant.to_string(),
                            command: Some(Command::RenameColumn(rename_check_req)),
                        };

                        // check conflict
                        query_state_machine
                            .coord
                            .broadcast_command(check_req)
                            .await?;
                        // exec
                        query_state_machine
                            .coord
                            .broadcast_command(exec_req)
                            .await?;
                    }
                    RenameColumnAction::RenameField(new_name) => {
                        alter_schema_func(&mut schema, old_column_name, new_name)?;
                    }
                }

                // update metadata
                schema.schema_id += 1;
                client
                    .update_table(&TableSchema::TsKvTableSchema(Arc::new(schema)))
                    .await?;

                return Ok(Output::Nil(()));
            }
        };
        schema.schema_id += 1;
        // TODO: @lutengda Start a task that guarantees eventual consistency
        if let Some(req) = req {
            query_state_machine.coord.broadcast_command(req).await?;
        }

        client
            .update_table(&TableSchema::TsKvTableSchema(Arc::new(schema)))
            .await?;
        return Ok(Output::Nil(()));
    }
}
