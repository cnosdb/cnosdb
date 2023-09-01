use std::collections::HashMap;
use std::sync::Arc;

use meta::model::meta_admin::AdminMeta;
use models::oid::Identifier;
use models::schema::{ResourceInfo, ResourceOperator, ResourceStatus, ResourceType};
use models::utils::now_timestamp_nanos;
use protos::kv_service::admin_command_request::Command::{DropDb, DropTab};
use protos::kv_service::{AdminCommandRequest, DropDbRequest, DropTableRequest};
use tracing::{debug, error, info};

use crate::errors::*;
use crate::Coordinator;

#[derive(Clone)]
pub struct ResourceManager {}

impl ResourceManager {
    pub async fn change_and_write(
        admin_meta: Arc<AdminMeta>,
        mut resourceinfo: ResourceInfo,
        dest_status: ResourceStatus,
        comment: &str,
    ) -> CoordinatorResult<bool> {
        resourceinfo.status = dest_status;
        resourceinfo.comment = comment.to_string();
        admin_meta
            .write_resourceinfo(resourceinfo.names(), resourceinfo.clone())
            .await
            .map_err(|err| CoordinatorError::Meta { source: err })?;
        Ok(true)
    }

    pub async fn check_and_run(coord: Arc<dyn Coordinator>) {
        let now = now_timestamp_nanos();
        match coord.meta_manager().read_resourceinfos(&[]).await {
            Ok(resourceinfos) => {
                for mut resourceinfo in resourceinfos {
                    if (resourceinfo.status == ResourceStatus::Schedule && resourceinfo.time <= now)
                        || resourceinfo.status == ResourceStatus::Failed
                    {
                        resourceinfo.status = ResourceStatus::Executing;
                        if let Err(meta_err) = coord
                            .meta_manager()
                            .write_resourceinfo(resourceinfo.names(), resourceinfo.clone())
                            .await
                        {
                            error!("{}", meta_err.to_string());
                            continue;
                        }
                        if let Err(coord_err) =
                            ResourceManager::do_operator(coord.clone(), resourceinfo).await
                        {
                            error!("{}", coord_err.to_string());
                        }
                    }
                }
            }
            Err(meta_err) => {
                error!("{}", meta_err.to_string());
            }
        }
    }

    async fn do_operator(
        coord: Arc<dyn Coordinator>,
        resourceinfo: ResourceInfo,
    ) -> CoordinatorResult<bool> {
        let operator_result = match resourceinfo.res_type {
            ResourceType::Tenant => match resourceinfo.operator {
                ResourceOperator::Drop => {
                    ResourceManager::drop_tenant(coord.clone(), &resourceinfo).await
                }
                _ => Ok(false),
            },
            ResourceType::Database => match resourceinfo.operator {
                ResourceOperator::Drop => {
                    ResourceManager::drop_database(coord.clone(), &resourceinfo).await
                }
                _ => Ok(false),
            },
            ResourceType::Table => match resourceinfo.operator {
                ResourceOperator::Drop => {
                    ResourceManager::drop_table(coord.clone(), &resourceinfo).await
                }
                _ => Ok(false),
            },
            /*ResourceType::Tagname => {
                match resourceinfo.operator {
                    ResourceOperator::Update => {
                        ResourceManager::update_tagname(coord.clone(), &resourceinfo).await;
                    },
                    _ => {}
                }
            },
            ResourceType::Tagvalue => {
                match resourceinfo.operator {
                    ResourceOperator::Update => {
                        ResourceManager::update_tagvalue(coord.clone(), &resourceinfo).await;
                    },
                    _ => {}
                }
            }*/
            _ => Ok(false),
        };

        if let Err(coord_err) = operator_result {
            ResourceManager::change_and_write(
                coord.meta_manager(),
                resourceinfo,
                ResourceStatus::Failed,
                &coord_err.to_string(),
            )
            .await
        } else {
            ResourceManager::change_and_write(
                coord.meta_manager(),
                resourceinfo,
                ResourceStatus::Successed,
                "",
            )
            .await
        }
    }

    async fn drop_tenant(
        coord: Arc<dyn Coordinator>,
        resourceinfo: &ResourceInfo,
    ) -> CoordinatorResult<bool> {
        if resourceinfo.names().len() == 1 {
            let tenant_name = resourceinfo.names().get(0).unwrap();
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
                names: resourceinfo.names().join("/"),
            })
        }
    }

    async fn drop_database(
        coord: Arc<dyn Coordinator>,
        resourceinfo: &ResourceInfo,
    ) -> CoordinatorResult<bool> {
        if resourceinfo.names().len() == 2 {
            let tenant_name = resourceinfo.names().get(0).unwrap();
            let db_name = resourceinfo.names().get(1).unwrap();

            let req = AdminCommandRequest {
                tenant: tenant_name.clone(),
                command: Some(DropDb(DropDbRequest {
                    db: db_name.clone(),
                })),
            };
            coord.broadcast_command(req).await?;
            debug!("Drop database {} of tenant {}", db_name, tenant_name);

            let tenant =
                coord
                    .tenant_meta(tenant_name)
                    .await
                    .ok_or(CoordinatorError::TenantNotFound {
                        name: tenant_name.clone(),
                    })?;
            tenant
                .drop_db(db_name)
                .await
                .map_err(|err| CoordinatorError::Meta { source: err })?;

            Ok(true)
        } else {
            Err(CoordinatorError::ResNamesIllegality {
                names: resourceinfo.names().join("/"),
            })
        }
    }

    async fn drop_table(
        coord: Arc<dyn Coordinator>,
        resourceinfo: &ResourceInfo,
    ) -> CoordinatorResult<bool> {
        if resourceinfo.names().len() == 3 {
            let tenant_name = resourceinfo.names().get(0).unwrap();
            let db_name = resourceinfo.names().get(1).unwrap();
            let table_name = resourceinfo.names().get(2).unwrap();

            info!("Drop table {}/{}/{}", tenant_name, db_name, table_name);
            let req = AdminCommandRequest {
                tenant: tenant_name.clone(),
                command: Some(DropTab(DropTableRequest {
                    db: db_name.clone(),
                    table: table_name.clone(),
                })),
            };
            coord.broadcast_command(req).await?;

            let tenant =
                coord
                    .tenant_meta(tenant_name)
                    .await
                    .ok_or(CoordinatorError::TenantNotFound {
                        name: tenant_name.clone(),
                    })?;
            tenant
                .drop_table(db_name, table_name)
                .await
                .map_err(|err| CoordinatorError::Meta { source: err })?;

            Ok(true)
        } else {
            Err(CoordinatorError::ResNamesIllegality {
                names: resourceinfo.names().join("/"),
            })
        }
    }

    /*async fn update_tagname(coord: Arc<CoordService>, resourceinfo: &ResourceInfo) {
        //TODO
    }

    async fn update_tagvalue(coord: Arc<CoordService>, resourceinfo: &ResourceInfo) {
        //TODO
    }*/

    pub async fn add_resource_task(
        coord: Arc<dyn Coordinator>,
        resourceinfo: ResourceInfo,
    ) -> CoordinatorResult<bool> {
        let resourceinfos = coord.meta_manager().read_resourceinfos(&[]).await?;
        let mut resourceinfos_map: HashMap<String, ResourceInfo> = HashMap::default();
        for resourceinfo in resourceinfos {
            resourceinfos_map.insert(resourceinfo.names().join("/"), resourceinfo);
        }

        let name = resourceinfo.names().join("/");
        if !resourceinfos_map.contains_key(&name)
            || resourceinfos_map[&name].status == ResourceStatus::Successed
            || resourceinfos_map[&name].status == ResourceStatus::Cancel
        {
            // write to cache and meta
            coord
                .meta_manager()
                .write_resourceinfo(resourceinfo.names(), resourceinfo.clone())
                .await?;

            if resourceinfo.status == ResourceStatus::Executing {
                // execute right now
                ResourceManager::do_operator(coord, resourceinfo).await?;
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
