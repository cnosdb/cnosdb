use std::path::Path;
use std::time::Duration;

use meta::model::MetaRef;
use models::meta_data::{VnodeAllInfo, VnodeInfo, VnodeStatus};
use protos::kv_service::admin_command_request::Command::DelVnode;
use protos::kv_service::tskv_service_client::TskvServiceClient;
use protos::kv_service::{
    AdminCommandRequest, DeleteVnodeRequest, DownloadFileRequest, FetchVnodeSummaryRequest,
    GetFilesMetaResponse, GetVnodeFilesMetaRequest,
};
use tokio::io::AsyncWriteExt;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tower::timeout::Timeout;
use trace::info;
use tskv::EngineRef;

use crate::errors::{CoordinatorError, CoordinatorResult};
use crate::file_info::get_file_info;
use crate::{status_response_to_result, SUCCESS_RESPONSE_CODE};

pub struct VnodeManager {
    node_id: u64,
    meta: MetaRef,
    kv_inst: EngineRef,
}

impl VnodeManager {
    pub fn new(meta: MetaRef, kv_inst: EngineRef, node_id: u64) -> Self {
        Self {
            node_id,
            meta,
            kv_inst,
        }
    }

    pub async fn move_vnode(&self, tenant: &str, vnode_id: u32) -> CoordinatorResult<()> {
        let all_info = crate::get_vnode_all_info(self.meta.clone(), tenant, vnode_id).await?;
        let db_name = all_info.db_name;
        let src_node_id = all_info.node_id;
        self.copy_vnode(tenant, vnode_id, false).await?;
        self.drop_vnode_remote(tenant, &db_name, src_node_id, vnode_id)
            .await?;

        Ok(())
    }

    pub async fn copy_vnode(
        &self,
        tenant: &str,
        vnode_id: u32,
        add_replication: bool,
    ) -> CoordinatorResult<()> {
        let meta_client =
            self.meta
                .tenant_meta(tenant)
                .await
                .ok_or(CoordinatorError::TenantNotFound {
                    name: tenant.to_string(),
                })?;

        let mut all_info = crate::get_vnode_all_info(self.meta.clone(), tenant, vnode_id).await?;

        let (new_id, del_repl) = if add_replication {
            let id = self.meta.retain_id(1).await?;
            (id, vec![])
        } else {
            (vnode_id, vec![VnodeInfo::new(vnode_id, all_info.node_id)])
        };
        info!(
            "Begin Copy Vnode:{} from: {} to: {}; new id: {}",
            vnode_id, all_info.node_id, self.node_id, new_id
        );

        all_info.set_status(VnodeStatus::Copying);
        meta_client.update_vnode(&all_info).await?;
        let owner = models::schema::make_owner(&all_info.tenant, &all_info.db_name);
        let path = self.kv_inst.get_storage_options().move_dir(&owner, new_id);

        let channel = self.meta.get_node_conn(all_info.node_id).await?;
        let timeout_channel = Timeout::new(channel, Duration::from_secs(60 * 60));
        let mut client = TskvServiceClient::<Timeout<Channel>>::new(timeout_channel);

        if let Err(err) = self
            .download_vnode_files(&all_info, &path, &mut client)
            .await
        {
            tokio::fs::remove_dir_all(&path).await?;
            return Err(err);
        }

        let ve = self.fetch_vnode_summary(&all_info, &mut client).await?;
        self.kv_inst
            .apply_vnode_summary(tenant, &all_info.db_name, new_id, ve)
            .await?;

        let add_repl = vec![VnodeInfo::new(new_id, self.node_id)];
        meta_client
            .update_replication_set(
                &all_info.db_name,
                all_info.bucket_id,
                all_info.repl_set_id,
                &del_repl,
                &add_repl,
            )
            .await?;

        all_info.set_status(VnodeStatus::Running);
        meta_client.update_vnode(&all_info).await?;

        Ok(())
    }

    pub async fn flush_vnode(&self, tenant: &str, vnode_id: u32) -> CoordinatorResult<()> {
        let all_info = crate::get_vnode_all_info(self.meta.clone(), tenant, vnode_id).await?;

        self.kv_inst
            .flush_tsfamily(tenant, &all_info.db_name, vnode_id)
            .await?;

        Ok(())
    }

    pub async fn drop_vnode(&self, tenant: &str, vnode_id: u32) -> CoordinatorResult<()> {
        let all_info = crate::get_vnode_all_info(self.meta.clone(), tenant, vnode_id).await?;

        let meta_client =
            self.meta
                .tenant_meta(tenant)
                .await
                .ok_or(CoordinatorError::TenantNotFound {
                    name: tenant.to_string(),
                })?;

        self.kv_inst
            .remove_tsfamily(tenant, &all_info.db_name, vnode_id)
            .await?;

        let del_repl = vec![VnodeInfo {
            id: vnode_id,
            node_id: self.node_id,
            status: Default::default(),
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

        Ok(())
    }

    async fn drop_vnode_remote(
        &self,
        tenant: &str,
        db: &str,
        node_id: u64,
        vnode_id: u32,
    ) -> CoordinatorResult<()> {
        let cmd = AdminCommandRequest {
            tenant: tenant.to_string(),
            command: Some(DelVnode(DeleteVnodeRequest {
                db: db.to_string(),
                vnode_id,
            })),
        };

        let channel = self.meta.get_node_conn(node_id).await?;
        let timeout_channel = Timeout::new(channel, Duration::from_secs(60 * 60));
        let mut client = TskvServiceClient::<Timeout<Channel>>::new(timeout_channel);
        let request = tonic::Request::new(cmd);

        let response = client
            .exec_admin_command(request)
            .await
            .map_err(tskv::Error::from)?
            .into_inner();
        status_response_to_result(&response)
    }

    async fn fetch_vnode_summary(
        &self,
        all_info: &VnodeAllInfo,
        client: &mut TskvServiceClient<Timeout<Channel>>,
    ) -> CoordinatorResult<tskv::VersionEdit> {
        let request = tonic::Request::new(FetchVnodeSummaryRequest {
            tenant: all_info.tenant.clone(),
            database: all_info.db_name.clone(),
            vnode_id: all_info.vnode_id,
        });

        let resp = client
            .fetch_vnode_summary(request)
            .await
            .map_err(tskv::Error::from)?
            .into_inner();
        if resp.code != SUCCESS_RESPONSE_CODE {
            return Err(CoordinatorError::GRPCRequest {
                msg: format!(
                    "server status: {}, {:?}",
                    resp.code,
                    String::from_utf8(resp.data)
                ),
            });
        }

        let ve = tskv::VersionEdit::decode(&resp.data)?;

        Ok(ve)
    }

    async fn download_vnode_files(
        &self,
        all_info: &VnodeAllInfo,
        data_path: &Path,
        client: &mut TskvServiceClient<Timeout<Channel>>,
    ) -> CoordinatorResult<()> {
        let files_meta = self.get_vnode_files_meta(all_info, client).await?;
        for info in files_meta.infos.iter() {
            let relative_filename = info
                .name
                .strip_prefix(&(files_meta.path.clone() + "/"))
                .unwrap();

            let filename = data_path.join(relative_filename);
            VnodeManager::download_file(&info.name, &filename, client).await?;

            let filename = filename.to_string_lossy().to_string();
            let tmp_info = get_file_info(&filename).await?;
            if tmp_info.md5 != info.md5 {
                return Err(CoordinatorError::CommonError {
                    msg: "download file md5 not match ".to_string(),
                });
            }
        }

        Ok(())
    }

    async fn get_vnode_files_meta(
        &self,
        all_info: &VnodeAllInfo,
        client: &mut TskvServiceClient<Timeout<Channel>>,
    ) -> CoordinatorResult<GetFilesMetaResponse> {
        let request = tonic::Request::new(GetVnodeFilesMetaRequest {
            tenant: all_info.tenant.to_string(),
            db: all_info.db_name.to_string(),
            vnode_id: all_info.vnode_id,
        });

        let resp = client
            .get_vnode_files_meta(request)
            .await
            .map_err(tskv::Error::from)?
            .into_inner();
        info!("node id: {}, files meta: {:?}", all_info.vnode_id, resp);

        Ok(resp)
    }

    pub async fn download_file(
        download: &str,
        filename: &Path,
        client: &mut TskvServiceClient<Timeout<Channel>>,
    ) -> CoordinatorResult<()> {
        if let Some(dir) = filename.parent() {
            tokio::fs::create_dir_all(dir).await?;
        }

        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(filename)
            .await?;

        let request = tonic::Request::new(DownloadFileRequest {
            filename: download.to_string(),
        });
        let mut resp_stream = client
            .download_file(request)
            .await
            .map_err(tskv::Error::from)?
            .into_inner();
        while let Some(received) = resp_stream.next().await {
            let received = received?;
            if received.code != SUCCESS_RESPONSE_CODE {
                return Err(CoordinatorError::GRPCRequest {
                    msg: format!(
                        "server status: {}, {:?}",
                        received.code,
                        String::from_utf8(received.data)
                    ),
                });
            }

            file.write_all(&received.data).await?;
        }

        Ok(())
    }
}
