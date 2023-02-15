use std::path::Path;

use coordinator::command::*;
use coordinator::errors::{CoordinatorError, CoordinatorResult};
use coordinator::file_info::{get_file_info, PathFilesMeta};
use http_protocol::status_code::OK;
use models::meta_data::{VnodeAllInfo, VnodeInfo};
use models::schema;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use trace::{error, info};

pub struct VnodeManager {
    node_id: u64,
    meta: meta::MetaRef,
    kv_inst: tskv::engine::EngineRef,
}

impl VnodeManager {
    pub fn new(meta: meta::MetaRef, kv_inst: tskv::engine::EngineRef, node_id: u64) -> Self {
        Self {
            node_id,
            meta,
            kv_inst,
        }
    }

    pub async fn move_vnode(&self, tenant: &str, vnode_id: u32) -> CoordinatorResult<()> {
        self.copy_vnode(tenant, vnode_id).await?;
        self.drop_vnode(tenant, vnode_id).await?;

        Ok(())
    }

    pub async fn copy_vnode(&self, tenant: &str, vnode_id: u32) -> CoordinatorResult<()> {
        let admin_meta = self.meta.admin_meta();
        let meta_client = self.meta.tenant_manager().tenant_meta(tenant).await.ok_or(
            CoordinatorError::TenantNotFound {
                name: tenant.to_string(),
            },
        )?;

        let new_id = admin_meta.retain_id(1).await?;
        let all_info = self.get_vnode_all_info(tenant, vnode_id).await?;
        info!(
            "Copy Vnode:{} from: {} to: {}; new id: {}",
            vnode_id, all_info.node_id, self.node_id, new_id
        );

        let owner = schema::make_owner(&all_info.tenant, &all_info.db_name);
        let path = self
            .kv_inst
            .get_storage_options()
            .ts_family_dir(&owner, new_id);
        let mut conn = admin_meta.get_node_conn(all_info.node_id).await?;
        if let Err(err) = self.download_vnode_files(&all_info, &path, &mut conn).await {
            tokio::fs::remove_dir_all(&path).await?;
            return Err(err);
        }

        let add_repl = vec![VnodeInfo {
            id: new_id,
            node_id: self.node_id,
        }];
        meta_client
            .update_replication_set(
                &all_info.db_name,
                all_info.bucket_id,
                all_info.repl_set_id,
                &[],
                &add_repl,
            )
            .await?;

        let ve = self.fetch_vnode_summary(&all_info, &mut conn).await?;
        self.kv_inst
            .apply_vnode_summary(tenant, &all_info.db_name, new_id, ve)
            .await?;

        admin_meta.put_node_conn(all_info.node_id, conn);

        Ok(())
    }

    pub async fn drop_vnode(&self, tenant: &str, vnode_id: u32) -> CoordinatorResult<()> {
        let all_info = self.get_vnode_all_info(tenant, vnode_id).await?;

        if all_info.node_id == self.node_id {
            let meta_client = self.meta.tenant_manager().tenant_meta(tenant).await.ok_or(
                CoordinatorError::TenantNotFound {
                    name: tenant.to_string(),
                },
            )?;

            self.kv_inst
                .remove_tsfamily(tenant, &all_info.db_name, vnode_id)
                .await?;

            let del_repl = vec![VnodeInfo {
                id: vnode_id,
                node_id: all_info.node_id,
            }];
            meta_client
                .update_replication_set(
                    &all_info.db_name,
                    all_info.bucket_id,
                    all_info.repl_set_id,
                    &del_repl,
                    &[],
                )
                .await?;
        } else {
            let admin_meta = self.meta.admin_meta();
            let mut conn = admin_meta.get_node_conn(all_info.node_id).await?;
            let req = AdminStatementRequest {
                tenant: all_info.tenant.to_string(),
                stmt: AdminStatementType::DeleteVnode {
                    db: all_info.db_name.to_string(),
                    vnode_id,
                },
            };

            send_command(&mut conn, &CoordinatorTcpCmd::AdminStatementCmd(req)).await?;
            let rsp_cmd = recv_command(&mut conn).await?;
            if let CoordinatorTcpCmd::StatusResponseCmd(msg) = rsp_cmd {
                admin_meta.put_node_conn(all_info.node_id, conn);
                if msg.code != SUCCESS_RESPONSE_CODE {
                    return Err(CoordinatorError::CommonError {
                        msg: format!("code: {}, msg: {}", msg.code, msg.data),
                    });
                }
            } else {
                return Err(CoordinatorError::UnExpectResponse);
            }
        }

        Ok(())
    }

    async fn fetch_vnode_summary(
        &self,
        all_info: &VnodeAllInfo,
        conn: &mut tokio::net::TcpStream,
    ) -> CoordinatorResult<tskv::VersionEdit> {
        let req = FetchVnodeSummaryRequest {
            tenant: all_info.tenant.clone(),
            database: all_info.db_name.clone(),
            vnode_id: all_info.vnode_id,
        };

        send_command(conn, &CoordinatorTcpCmd::FetchVnodeSummaryCmd(req)).await?;
        let rsp_cmd = recv_command(conn).await?;

        match rsp_cmd {
            CoordinatorTcpCmd::StatusResponseCmd(msg) => Err(CoordinatorError::CommonError {
                msg: format!("code: {}, msg: {}", msg.code, msg.data),
            }),

            CoordinatorTcpCmd::FetchVnodeSummaryResponseCmd(msg) => {
                let ve = tskv::VersionEdit::decode(&msg.version_edit)?;
                Ok(ve)
            }

            _ => Err(CoordinatorError::UnExpectResponse),
        }
    }

    async fn download_vnode_files(
        &self,
        all_info: &VnodeAllInfo,
        data_path: &Path,
        conn: &mut tokio::net::TcpStream,
    ) -> CoordinatorResult<()> {
        let files_meta = self.get_vnode_files_meta(all_info, conn).await?;
        for info in files_meta.meta.iter() {
            let relative_filename = info
                .name
                .strip_prefix(&(files_meta.path.clone() + "/"))
                .unwrap();

            let new_id = all_info.vnode_id;
            self.download_file(conn, all_info, relative_filename, data_path)
                .await?;

            let filename = data_path.join(relative_filename);
            let filename = filename.to_string_lossy().to_string();
            let tmp_info = get_file_info(&filename).await?;
            if tmp_info.md5 != info.md5 {
                return Err(CoordinatorError::CommonError {
                    msg: "file md5 ".to_string(),
                });
            }
        }

        Ok(())
    }

    async fn get_vnode_files_meta(
        &self,
        all_info: &VnodeAllInfo,
        conn: &mut tokio::net::TcpStream,
    ) -> CoordinatorResult<PathFilesMeta> {
        let req = AdminStatementRequest {
            tenant: all_info.tenant.to_string(),
            stmt: AdminStatementType::GetVnodeFilesMeta {
                db: all_info.db_name.to_string(),
                vnode_id: all_info.vnode_id,
            },
        };

        let files_meta;
        send_command(conn, &CoordinatorTcpCmd::AdminStatementCmd(req)).await?;
        let rsp_cmd = recv_command(conn).await?;
        if let CoordinatorTcpCmd::StatusResponseCmd(msg) = rsp_cmd {
            files_meta = serde_json::from_str::<PathFilesMeta>(&msg.data)
                .map_err(|e| CoordinatorError::CommonError { msg: e.to_string() })?;
            info!(
                "node id: {}, files meta: {:?}",
                all_info.vnode_id, files_meta
            );
        } else {
            return Err(CoordinatorError::UnExpectResponse);
        }

        Ok(files_meta)
    }

    async fn download_file(
        &self,
        conn: &mut tokio::net::TcpStream,
        req: &VnodeAllInfo,
        filename: &str,
        data_path: &Path,
    ) -> CoordinatorResult<()> {
        let cmd = CoordinatorTcpCmd::AdminStatementCmd(AdminStatementRequest {
            tenant: req.tenant.clone(),
            stmt: AdminStatementType::DownloadFile {
                db: req.db_name.clone(),
                vnode_id: req.vnode_id,
                filename: filename.to_string(),
            },
        });

        send_command(conn, &cmd).await?;

        let filename = data_path.join(filename);
        tokio::fs::create_dir_all(filename.parent().unwrap()).await?;
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&filename)
            .await?;

        let size = conn.read_u64().await?;
        info!("save file: {:?}, size; {}", filename, size);

        let mut read_len = 0;
        let mut buffer = Vec::with_capacity(8 * 1024);
        loop {
            let len = conn.read_buf(&mut buffer).await?;
            read_len += len;
            file.write_all(&buffer).await?;

            if len == 0 || read_len >= size as usize {
                break;
            }

            buffer.clear();
        }

        Ok(())
    }

    async fn get_vnode_all_info(
        &self,
        tenant: &str,
        vnode_id: u32,
    ) -> CoordinatorResult<VnodeAllInfo> {
        match self.meta.tenant_manager().tenant_meta(tenant).await {
            Some(meta_client) => match meta_client.get_vnode_all_info(vnode_id) {
                Some(all_info) => Ok(all_info),
                None => Err(CoordinatorError::VnodeNotFound { id: vnode_id }),
            },

            None => Err(CoordinatorError::TenantNotFound {
                name: tenant.to_string(),
            }),
        }
    }
}
