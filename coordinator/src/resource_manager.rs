use std::collections::HashMap;
use std::sync::Arc;

use meta::model::meta_admin::AdminMeta;
use models::meta_data::ReplicationSet;
use models::schema::{ResourceInfo, ResourceOperator, ResourceStatus, TableSchema};
use models::utils::now_timestamp_nanos;
use protos::kv_service::{
    raft_write_command, DropColumnRequest, DropTableRequest, RaftWriteCommand, UpdateSetValue,
    UpdateTagsRequest,
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
            .write_resourceinfo(resourceinfo.get_name(), resourceinfo.clone())
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
                    .read_resourceinfos()
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
                            .write_resourceinfo(resourceinfo.get_name(), resourceinfo.clone())
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

                let dur = tokio::time::Duration::from_secs(1);
                tokio::time::sleep(dur).await;
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
            ResourceOperator::DropTenant(tenant_name) => {
                ResourceManager::drop_tenant(coord.clone(), tenant_name).await
            }
            ResourceOperator::DropDatabase(tenant_name, db_name) => {
                ResourceManager::drop_database(coord.clone(), tenant_name, db_name).await
            }
            ResourceOperator::DropTable(tenant_name, db_name, table_name) => {
                ResourceManager::drop_table(coord.clone(), tenant_name, db_name, table_name).await
            }
            ResourceOperator::AddColumn(table_schema, _)
            | ResourceOperator::DropColumn(_, table_schema)
            | ResourceOperator::AlterColumn(_, table_schema, _) => {
                ResourceManager::alter_table(coord.clone(), &resourceinfo, &table_schema.tenant)
                    .await
            }
            ResourceOperator::UpdateTagValue(
                tenant_name,
                db_name,
                update_set_value,
                series_keys,
                replica_set,
            ) => {
                ResourceManager::update_tag_value(
                    coord.clone(),
                    tenant_name,
                    db_name,
                    update_set_value,
                    series_keys,
                    replica_set,
                )
                .await
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
        tenant_name: &str,
    ) -> CoordinatorResult<bool> {
        let tenant = coord.meta_manager().tenant_meta(tenant_name).await.ok_or(
            CoordinatorError::TenantNotFound {
                name: tenant_name.to_string(),
            },
        )?;

        // drop database in the tenant
        let all_dbs = tenant
            .list_databases()
            .map_err(|err| CoordinatorError::Meta { source: err })?;
        for (db_name, _) in all_dbs {
            ResourceManager::drop_database(coord.clone(), tenant_name, &db_name).await?;
        }

        // drop tenant metadata
        coord
            .meta_manager()
            .drop_tenant(tenant_name)
            .await
            .map_err(|err| CoordinatorError::Meta { source: err })?;

        Ok(true)
    }

    async fn drop_database(
        coord: Arc<dyn Coordinator>,
        tenant_name: &str,
        db_name: &str,
    ) -> CoordinatorResult<bool> {
        let tenant =
            coord
                .tenant_meta(tenant_name)
                .await
                .ok_or(CoordinatorError::TenantNotFound {
                    name: tenant_name.to_string(),
                })?;

        let buckets = tenant.get_db_info(db_name)?.map_or(vec![], |v| v.buckets);
        for bucket in buckets {
            for replica in bucket.shard_group {
                let cmd_type = VnodeManagerCmdType::DestoryRaftGroup(replica.id);
                coord.vnode_manager(tenant_name, cmd_type).await?;
            }
        }

        debug!("Drop database {} of tenant {}", db_name, tenant_name);
        tenant
            .drop_db(db_name)
            .await
            .map_err(|err| CoordinatorError::Meta { source: err })?;

        Ok(true)
    }

    async fn drop_table(
        coord: Arc<dyn Coordinator>,
        tenant_name: &str,
        db_name: &str,
        table_name: &str,
    ) -> CoordinatorResult<bool> {
        info!("Drop table {}/{}/{}", tenant_name, db_name, table_name);
        let tenant =
            coord
                .tenant_meta(tenant_name)
                .await
                .ok_or(CoordinatorError::TenantNotFound {
                    name: tenant_name.to_string(),
                })?;

        let mut requests = vec![];
        let db_info = tenant
            .get_db_info(db_name)?
            .ok_or(CoordinatorError::CommonError {
                msg: format!("database not found: {}", db_name),
            })?;

        for bucket in db_info.buckets {
            for replica in bucket.shard_group {
                let request = DropTableRequest {
                    db: db_name.to_string(),
                    table: table_name.to_string(),
                };
                let command = RaftWriteCommand {
                    replica_id: replica.id,
                    tenant: tenant_name.to_string(),
                    db_name: db_name.to_string(),
                    command: Some(raft_write_command::Command::DropTable(request)),
                };

                let request = coord.write_replica_by_raft(replica.clone(), command, None);
                requests.push(request);
            }
        }

        for result in futures::future::join_all(requests).await {
            result?
        }

        tenant
            .drop_table(db_name, table_name)
            .await
            .map_err(|err| CoordinatorError::Meta { source: err })?;

        Ok(true)
    }

    async fn alter_table(
        coord: Arc<dyn Coordinator>,
        resourceinfo: &ResourceInfo,
        tenant_name: &str,
    ) -> CoordinatorResult<bool> {
        let tenant =
            coord
                .tenant_meta(tenant_name)
                .await
                .ok_or(CoordinatorError::TenantNotFound {
                    name: tenant_name.to_string(),
                })?;

        match resourceinfo.get_operator() {
            ResourceOperator::AddColumn(table_schema, _) => {
                tenant
                    .update_table(&TableSchema::TsKvTableSchema(Arc::new(
                        table_schema.clone(),
                    )))
                    .await?;
            }

            ResourceOperator::DropColumn(drop_column_name, table_schema) => {
                let mut requests = vec![];
                let db_info =
                    tenant
                        .get_db_info(&table_schema.db)?
                        .ok_or(CoordinatorError::CommonError {
                            msg: format!("database not found: {}", table_schema.db),
                        })?;

                for bucket in db_info.buckets {
                    for replica in bucket.shard_group {
                        let request = DropColumnRequest {
                            db: table_schema.db.to_string(),
                            table: table_schema.name.to_string(),
                            column: drop_column_name.to_string(),
                        };
                        let command = RaftWriteCommand {
                            replica_id: replica.id,
                            tenant: tenant_name.to_string(),
                            db_name: table_schema.db.to_string(),
                            command: Some(raft_write_command::Command::DropColumn(request)),
                        };

                        let request = coord.write_replica_by_raft(replica.clone(), command, None);
                        requests.push(request);
                    }
                }

                for result in futures::future::join_all(requests).await {
                    result?
                }

                tenant
                    .update_table(&TableSchema::TsKvTableSchema(Arc::new(
                        table_schema.clone(),
                    )))
                    .await?;
            }
            ResourceOperator::AlterColumn(_, table_schema, _) => {
                tenant
                    .update_table(&TableSchema::TsKvTableSchema(Arc::new(
                        table_schema.clone(),
                    )))
                    .await?;
            }

            _ => {}
        };

        Ok(true)
    }

    async fn update_tag_value(
        coord: Arc<dyn Coordinator>,
        tenant_name: &str,
        db_name: &str,
        update_set_value: &[(Vec<u8>, Option<Vec<u8>>)],
        series_keys: &[Vec<u8>],
        replica_set: &[ReplicationSet],
    ) -> CoordinatorResult<bool> {
        let new_tags_vec: Vec<UpdateSetValue> = update_set_value
            .iter()
            .map(|e| UpdateSetValue {
                key: e.0.clone(),
                value: e.1.clone(),
            })
            .collect();
        let update_tags_request = UpdateTagsRequest {
            db: db_name.to_string(),
            new_tags: new_tags_vec,
            matched_series: series_keys.to_vec(),
            dry_run: false,
        };

        let mut requests = vec![];
        for replica in replica_set {
            let command = RaftWriteCommand {
                replica_id: replica.id,
                tenant: tenant_name.to_string(),
                db_name: db_name.to_string(),
                command: Some(raft_write_command::Command::UpdateTags(
                    update_tags_request.clone(),
                )),
            };

            let request = coord.write_replica_by_raft(replica.clone(), command, None);
            requests.push(request);
        }

        for result in futures::future::join_all(requests).await {
            result?
        }

        Ok(true)
    }

    pub async fn add_resource_task(
        coord: Arc<dyn Coordinator>,
        mut resourceinfo: ResourceInfo,
    ) -> CoordinatorResult<bool> {
        let resourceinfos = coord.meta_manager().read_resourceinfos().await?;
        let mut resourceinfos_map: HashMap<String, ResourceInfo> = HashMap::default();
        for resourceinfo in resourceinfos {
            resourceinfos_map.insert(resourceinfo.get_name().to_string(), resourceinfo);
        }

        let name = resourceinfo.get_name();
        if !resourceinfos_map.contains_key(name)
            || *resourceinfos_map[name].get_status() == ResourceStatus::Schedule
            || *resourceinfos_map[name].get_status() == ResourceStatus::Successed
            || *resourceinfos_map[name].get_status() == ResourceStatus::Cancel
            || *resourceinfos_map[name].get_status() == ResourceStatus::Fatal
        {
            if *resourceinfo.get_status() == ResourceStatus::Executing {
                resourceinfo.increase_try_count();
            }

            // write to meta
            coord
                .meta_manager()
                .write_resourceinfo(resourceinfo.get_name(), resourceinfo.clone())
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
