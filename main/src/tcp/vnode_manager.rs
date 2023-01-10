use std::path::Path;

use coordinator::command::*;
use coordinator::file_info::get_file_info;
use coordinator::{
    errors::{CoordinatorError, CoordinatorResult},
    file_info::PathFilesMeta,
};

use http_protocol::status_code::OK;
use models::meta_data::VnodeAllInfo;
use models::schema;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use trace::info;

pub struct VnodeManager {
    meta: meta::meta_client::MetaRef,
    kv_inst: tskv::engine::EngineRef,
}

impl VnodeManager {
    pub fn new(meta: meta::meta_client::MetaRef, kv_inst: tskv::engine::EngineRef) -> Self {
        Self { meta, kv_inst }
    }

    pub async fn move_vnode(&self, tenant: &str, vnode_id: u32) -> CoordinatorResult<()> {
        let admin_meta = self.meta.admin_meta();
        let all_info = self.get_vnode_all_info(tenant, vnode_id)?;
        let mut conn = admin_meta.get_node_conn(all_info.node_id).await?;

        let owner = schema::make_owner(&all_info.tenant, &all_info.db_name);
        let path = self
            .kv_inst
            .get_storage_options()
            .ts_family_dir(&owner, all_info.vnode_id);

        self.download_vnode_files(&all_info, &path, &mut conn)
            .await?;

        Ok(())
    }

    pub async fn copy_vnode(&self, tenant: &str, vnode_id: u32) -> CoordinatorResult<()> {
        todo!()
    }

    pub async fn drop_vnode(&self, tenant: &str, vnode_id: u32) -> CoordinatorResult<()> {
        let all_info = self.get_vnode_all_info(tenant, vnode_id)?;

        self.kv_inst
            .remove_tsfamily(tenant, &all_info.db_name, vnode_id)
            .await?;

        Ok(())
    }

    async fn download_vnode_files(
        &self,
        all_info: &VnodeAllInfo,
        data_path: &Path,
        conn: &mut tokio::net::TcpStream,
    ) -> CoordinatorResult<()> {
        let files_meta = self.get_vnode_files_meta(&all_info, conn).await?;
        for info in files_meta.meta.iter() {
            let relative_filename = info
                .name
                .strip_prefix(&(files_meta.path.clone() + "/"))
                .unwrap();

            let new_id = all_info.vnode_id;
            self.download_file(conn, &all_info, relative_filename, data_path)
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
        send_command(conn, &CoordinatorTcpCmd::AdminStatementCmd(req))
            .await
            .unwrap();
        let rsp_cmd = recv_command(conn).await.unwrap();
        if let CoordinatorTcpCmd::StatusResponseCmd(msg) = rsp_cmd {
            files_meta = serde_json::from_str::<PathFilesMeta>(&msg.data).unwrap();
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
        let _ = tokio::fs::create_dir_all(filename.parent().unwrap()).await;
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

    fn get_vnode_all_info(&self, tenant: &str, vnode_id: u32) -> CoordinatorResult<VnodeAllInfo> {
        match self.meta.tenant_manager().tenant_meta(tenant) {
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
