use std::sync::Arc;

use async_trait::async_trait;
use coordinator::resource_manager::ResourceManager;
use meta::error::MetaError;
use models::oid::Identifier;
use models::schema::{ResourceInfo, ResourceOperator, TableColumn, TableSchema};
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

        let mut alter_info: Option<(
            String,
            ResourceOperator,
            (Option<String>, Option<TableColumn>, Option<String>),
        )> = None;
        match &self.stmt.alter_action {
            AlterTableAction::AddColumn { table_column } => {
                let table_column = table_column.to_owned();
                alter_info = Some((
                    table_column.name.clone(),
                    ResourceOperator::AddColumn,
                    (None, Some(table_column), None),
                ));
            }

            AlterTableAction::DropColumn { column_name } => {
                alter_info = Some((
                    column_name.clone(),
                    ResourceOperator::DropColumn,
                    (Some(column_name.clone()), None, None),
                ));
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
                alter_info = Some((
                    column_name.clone(),
                    ResourceOperator::AlterColumn,
                    (Some(column_name.clone()), Some(new_column.clone()), None),
                ));
            }
            AlterTableAction::RenameColumn {
                old_column_name,
                new_column_name,
            } => {
                if let Some(old_column) = schema.column(old_column_name) {
                    match new_column_name {
                        RenameColumnAction::RenameTag(new_name) => {
                            alter_info = Some((
                                old_column_name.clone(),
                                ResourceOperator::RenameTagName,
                                (None, Some(old_column.clone()), Some(new_name.clone())),
                            ));
                        }
                        RenameColumnAction::RenameField(new_name) => {
                            let new_column = TableColumn::new(
                                old_column.id,
                                new_name.to_string(),
                                old_column.column_type.clone(),
                                old_column.encoding,
                            );
                            schema.change_column(old_column_name, new_column);
                            schema.schema_id += 1;

                            client
                                .update_table(&TableSchema::TsKvTableSchema(Arc::new(schema)))
                                .await?;
                        }
                    }
                } else {
                    return Err(QueryError::ColumnNotFound {
                        col: old_column_name.to_owned(),
                    });
                }
            }
        };

        if let Some(info) = alter_info {
            let resourceinfo = ResourceInfo::new(
                *client.tenant().id(),
                vec![
                    table_name.tenant().to_string(),
                    table_name.database().to_string(),
                    table_name.table().to_string(),
                    info.0,
                ],
                info.1,
                &None,
                info.2,
            );
            ResourceManager::add_resource_task(query_state_machine.coord.clone(), resourceinfo)
                .await?;
        }
        return Ok(Output::Nil(()));
    }
}
