use std::collections::HashMap;
use std::sync::Arc;

use meta::model::meta_admin::AdminMeta;
use models::oid::Identifier;
use models::schema::{
    ResourceInfo, ResourceOperator, ResourceStatus, TableSchema, TskvTableSchema,
};
use models::utils::now_timestamp_nanos;
use protos::kv_service::admin_command_request::Command::{self, DropDb, DropTab, UpdateTags};
use protos::kv_service::{
    AddColumnRequest, AdminCommandRequest, AlterColumnRequest, DropColumnRequest, DropDbRequest,
    DropTableRequest, RenameColumnRequest, UpdateSetValue, UpdateTagsRequest,
};
use tracing::{debug, error, info};

use crate::errors::*;
use crate::{Coordinator, VnodeManagerCmdType};

#[derive(Clone)]
pub struct ResourceManager {}

impl ResourceManager {
    pub async fn change_and_write(
        admin_meta: Arc<AdminMeta>,
        mut resourceinfo: ResourceInfo,
        dest_status: ResourceStatus,
        comment: &str,
    ) -> CoordinatorResult<bool> {
        resourceinfo.set_status(dest_status);
        resourceinfo.set_comment(comment);
        admin_meta
            .write_resourceinfo(resourceinfo.get_names(), resourceinfo.clone())
            .await
            .map_err(|err| CoordinatorError::Meta { source: err })?;
        Ok(true)
    }

    pub async fn check_and_run(coord: Arc<dyn Coordinator>) -> CoordinatorResult<bool> {
        let (node_id, is_lock) = coord
            .meta_manager()
            .read_resourceinfos_mark()
            .await
            .map_err(|err| CoordinatorError::Meta { source: err })?;
        if !is_lock {
            // lock resourceinfos
            coord
                .meta_manager()
                .write_resourceinfos_mark(coord.node_id(), true)
                .await
                .map_err(|err| CoordinatorError::Meta { source: err })?;

            let mut is_need_loop = false;
            loop {
                let resourceinfos = coord
                    .meta_manager()
                    .read_resourceinfos(&[])
                    .await
                    .map_err(|err| CoordinatorError::Meta { source: err })?;
                let now = now_timestamp_nanos();
                for mut resourceinfo in resourceinfos {
                    if (*resourceinfo.get_status() == ResourceStatus::Schedule
                        && resourceinfo.get_time() <= now)
                        || *resourceinfo.get_status() == ResourceStatus::Failed
                    {
                        resourceinfo.increase_try_count();

                        resourceinfo.set_status(ResourceStatus::Executing);
                        if let Err(meta_err) = coord
                            .meta_manager()
                            .write_resourceinfo(resourceinfo.get_names(), resourceinfo.clone())
                            .await
                        {
                            error!("{}", meta_err.to_string());
                            is_need_loop = true;
                            continue;
                        }
                        if let Err(coord_err) =
                            ResourceManager::do_operator(coord.clone(), resourceinfo).await
                        {
                            error!("{}", coord_err.to_string());
                            is_need_loop = true;
                        }
                    }
                }
                if !is_need_loop {
                    // unlock resourceinfos
                    coord
                        .meta_manager()
                        .write_resourceinfos_mark(coord.node_id(), false)
                        .await
                        .map_err(|err| CoordinatorError::Meta { source: err })?;
                    return Ok(true);
                }
            }
        } else {
            info!("resource is executing by {}", node_id);
            Ok(false)
        }
    }

    async fn do_operator(
        coord: Arc<dyn Coordinator>,
        resourceinfo: ResourceInfo,
    ) -> CoordinatorResult<bool> {
        let operator_result = match resourceinfo.get_operator() {
            ResourceOperator::DropTenant => {
                ResourceManager::drop_tenant(coord.clone(), &resourceinfo).await
            }
            ResourceOperator::DropDatabase => {
                ResourceManager::drop_database(coord.clone(), &resourceinfo).await
            }
            ResourceOperator::DropTable => {
                ResourceManager::drop_table(coord.clone(), &resourceinfo).await
            }
            ResourceOperator::AddColumn(..)
            | ResourceOperator::DropColumn(..)
            | ResourceOperator::AlterColumn(..)
            | ResourceOperator::RenameTagName(..) => {
                ResourceManager::alter_table(coord.clone(), &resourceinfo).await
            }
            ResourceOperator::UpdateTagValue(..) => {
                ResourceManager::update_tag_value(coord.clone(), &resourceinfo).await
            }
        };

        let mut status_comment = (ResourceStatus::Successed, String::default());
        if let Err(coord_err) = &operator_result {
            status_comment.0 = ResourceStatus::Failed;
            status_comment.1 = coord_err.to_string();
        }
        ResourceManager::change_and_write(
            coord.meta_manager(),
            resourceinfo.clone(),
            status_comment.0,
            &status_comment.1,
        )
        .await?;

        operator_result
    }

    async fn drop_tenant(
        coord: Arc<dyn Coordinator>,
        resourceinfo: &ResourceInfo,
    ) -> CoordinatorResult<bool> {
        let names = resourceinfo.get_names();
        if names.len() == 1 {
            let tenant_name = names.get(0).unwrap();
            let tenant =
                coord
                    .tenant_meta(tenant_name)
                    .await
                    .ok_or(CoordinatorError::TenantNotFound {
                        name: tenant_name.clone(),
                    })?;

            // drop role in the tenant
            let all_roles = tenant
                .custom_roles()
                .await
                .map_err(|err| CoordinatorError::Meta { source: err })?;
            for role in all_roles {
                tenant
                    .drop_custom_role(role.name())
                    .await
                    .map_err(|err| CoordinatorError::Meta { source: err })?;
            }

            // drop database in the tenant
            let all_dbs = tenant
                .list_databases()
                .map_err(|err| CoordinatorError::Meta { source: err })?;
            for db_name in all_dbs {
                tenant
                    .drop_db(&db_name)
                    .await
                    .map_err(|err| CoordinatorError::Meta { source: err })?;
            }

            // drop tenant metadata
            coord
                .meta_manager()
                .drop_tenant(tenant_name)
                .await
                .map_err(|err| CoordinatorError::Meta { source: err })?;

            Ok(true)
        } else {
            Err(CoordinatorError::ResNamesIllegality {
                names: resourceinfo.get_names().join("/"),
            })
        }
    }

    async fn drop_database(
        coord: Arc<dyn Coordinator>,
        resourceinfo: &ResourceInfo,
    ) -> CoordinatorResult<bool> {
        let names = resourceinfo.get_names();
        if names.len() == 2 {
            let tenant_name = names.get(0).unwrap();
            let db_name = names.get(1).unwrap();
            let tenant =
                coord
                    .tenant_meta(tenant_name)
                    .await
                    .ok_or(CoordinatorError::TenantNotFound {
                        name: tenant_name.clone(),
                    })?;

            let req = AdminCommandRequest {
                tenant: tenant_name.clone(),
                command: Some(DropDb(DropDbRequest {
                    db: db_name.clone(),
                })),
            };

            if coord.using_raft_replication() {
                let buckets = tenant.get_db_info(db_name)?.map_or(vec![], |v| v.buckets);
                for bucket in buckets {
                    for replica in bucket.shard_group {
                        let cmd_type = VnodeManagerCmdType::DestoryRaftGroup(replica.id);
                        coord.vnode_manager(tenant_name, cmd_type).await?;
                    }
                }
            } else {
                coord.broadcast_command(req).await?;
            }
            debug!("Drop database {} of tenant {}", db_name, tenant_name);

            tenant
                .drop_db(db_name)
                .await
                .map_err(|err| CoordinatorError::Meta { source: err })?;

            Ok(true)
        } else {
            Err(CoordinatorError::ResNamesIllegality {
                names: resourceinfo.get_names().join("/"),
            })
        }
    }

    async fn drop_table(
        coord: Arc<dyn Coordinator>,
        resourceinfo: &ResourceInfo,
    ) -> CoordinatorResult<bool> {
        let names = resourceinfo.get_names();
        if names.len() == 3 {
            let tenant_name = names.get(0).unwrap();
            let db_name = names.get(1).unwrap();
            let table_name = names.get(2).unwrap();

            let tenant =
                coord
                    .tenant_meta(tenant_name)
                    .await
                    .ok_or(CoordinatorError::TenantNotFound {
                        name: tenant_name.clone(),
                    })?;

            info!("Drop table {}/{}/{}", tenant_name, db_name, table_name);
            let req = AdminCommandRequest {
                tenant: tenant_name.clone(),
                command: Some(DropTab(DropTableRequest {
                    db: db_name.clone(),
                    table: table_name.clone(),
                })),
            };
            coord.broadcast_command(req).await?;

            tenant
                .drop_table(db_name, table_name)
                .await
                .map_err(|err| CoordinatorError::Meta { source: err })?;

            Ok(true)
        } else {
            Err(CoordinatorError::ResNamesIllegality {
                names: resourceinfo.get_names().join("/"),
            })
        }
    }

    async fn alter_table(
        coord: Arc<dyn Coordinator>,
        resourceinfo: &ResourceInfo,
    ) -> CoordinatorResult<bool> {
        let names = resourceinfo.get_names();
        if names.len() == 4 {
            let tenant_name = names.get(0).unwrap();

            let tenant =
                coord
                    .tenant_meta(tenant_name)
                    .await
                    .ok_or(CoordinatorError::TenantNotFound {
                        name: tenant_name.clone(),
                    })?;

            let mut schema = TskvTableSchema::default();
            let req = match resourceinfo.get_operator() {
                ResourceOperator::AddColumn(new_column_name, table_schema) => {
                    if let Some(table_column) = table_schema.column(new_column_name) {
                        schema = table_schema.clone();
                        Some(AdminCommandRequest {
                            tenant: tenant_name.to_string(),
                            command: Some(Command::AddColumn(AddColumnRequest {
                                db: table_schema.db.to_owned(),
                                table: table_schema.name.to_string(),
                                column: table_column.encode()?,
                            })),
                        })
                    } else {
                        return Err(CoordinatorError::CommonError {
                            msg: format!("Column {} not found.", new_column_name),
                        });
                    }
                }
                ResourceOperator::DropColumn(drop_column_name, table_schema) => {
                    schema = table_schema.clone();
                    Some(AdminCommandRequest {
                        tenant: tenant_name.to_string(),
                        command: Some(Command::DropColumn(DropColumnRequest {
                            db: table_schema.db.to_owned(),
                            table: table_schema.name.to_string(),
                            column: drop_column_name.clone(),
                        })),
                    })
                }
                ResourceOperator::AlterColumn(alter_column_name, table_schema) => {
                    if let Some(table_column) = table_schema.column(alter_column_name) {
                        schema = table_schema.clone();
                        Some(AdminCommandRequest {
                            tenant: tenant_name.to_string(),
                            command: Some(Command::AlterColumn(AlterColumnRequest {
                                db: table_schema.db.to_owned(),
                                table: table_schema.name.to_string(),
                                name: alter_column_name.clone(),
                                column: table_column.encode()?,
                            })),
                        })
                    } else {
                        return Err(CoordinatorError::CommonError {
                            msg: format!("Column {} not found.", alter_column_name),
                        });
                    }
                }
                ResourceOperator::RenameTagName(old_column_name, new_column_name, table_schema) => {
                    schema = table_schema.clone();
                    Some(AdminCommandRequest {
                        tenant: tenant_name.to_string(),
                        command: Some(Command::RenameColumn(RenameColumnRequest {
                            db: table_schema.db.to_owned(),
                            table: table_schema.name.to_string(),
                            old_name: old_column_name.clone(),
                            new_name: new_column_name.clone(),
                            dry_run: false,
                        })),
                    })
                }
                _ => None,
            };

            if req.is_some() {
                coord.broadcast_command(req.unwrap()).await?;

                tenant
                    .update_table(&TableSchema::TsKvTableSchema(Arc::new(schema)))
                    .await?;
            }

            Ok(true)
        } else {
            Err(CoordinatorError::ResNamesIllegality {
                names: resourceinfo.get_names().join("/"),
            })
        }
    }

    async fn update_tag_value(
        coord: Arc<dyn Coordinator>,
        resourceinfo: &ResourceInfo,
    ) -> CoordinatorResult<bool> {
        if let ResourceOperator::UpdateTagValue(update_set_value, series_keys, replica_set) =
            resourceinfo.get_operator()
        {
            let names = resourceinfo.get_names();
            if names.len() == 4 {
                let tenant_name = names.get(0).unwrap();
                let db_name = names.get(1).unwrap();

                let new_tags_vec: Vec<UpdateSetValue> = update_set_value
                    .iter()
                    .map(|e| UpdateSetValue {
                        key: e.0.clone(),
                        value: e.1.clone(),
                    })
                    .collect();
                let update_tags_request = UpdateTagsRequest {
                    db: db_name.clone(),
                    new_tags: new_tags_vec,
                    matched_series: series_keys.clone(),
                    dry_run: false,
                };

                let req = AdminCommandRequest {
                    tenant: tenant_name.clone(),
                    command: Some(UpdateTags(update_tags_request)),
                };

                coord
                    .broadcast_command_by_vnode(req, replica_set.to_vec())
                    .await?;
            }
        }

        Ok(true)
    }

    pub async fn add_resource_task(
        coord: Arc<dyn Coordinator>,
        mut resourceinfo: ResourceInfo,
    ) -> CoordinatorResult<bool> {
        let resourceinfos = coord.meta_manager().read_resourceinfos(&[]).await?;
        let mut resourceinfos_map: HashMap<String, ResourceInfo> = HashMap::default();
        for resourceinfo in resourceinfos {
            resourceinfos_map.insert(resourceinfo.get_names().join("/"), resourceinfo);
        }

        let name = resourceinfo.get_names().join("/");
        if !resourceinfos_map.contains_key(&name)
            || *resourceinfos_map[&name].get_status() != ResourceStatus::Executing
        {
            if *resourceinfo.get_status() == ResourceStatus::Executing {
                resourceinfo.increase_try_count();
            }

            // write to meta
            coord
                .meta_manager()
                .write_resourceinfo(resourceinfo.get_names(), resourceinfo.clone())
                .await?;

            if *resourceinfo.get_status() == ResourceStatus::Executing {
                // execute right now
                ResourceManager::do_operator(coord, resourceinfo).await?;
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
