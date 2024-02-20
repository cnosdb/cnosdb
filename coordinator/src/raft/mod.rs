use std::path::{Path, PathBuf};
use std::time::Duration;

use meta::model::MetaRef;
use models::meta_data::VnodeId;
use protos::kv_service::tskv_service_client::TskvServiceClient;
use protos::kv_service::{DownloadFileRequest, RaftWriteCommand};
use protos::models_helper::parse_prost_bytes;
use protos::{tskv_service_time_out_client, DEFAULT_GRPC_SERVER_MESSAGE_LEN};
use replication::errors::{ReplicationError, ReplicationResult};
use replication::{ApplyContext, ApplyStorage};
use tokio::io::AsyncWriteExt;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tower::timeout::Timeout;
use tracing::{error, info};
use tskv::file_system::file_info;
use tskv::kv_option::StorageOptions;
use tskv::vnode_store::VnodeStorage;
use tskv::VnodeSnapshot;

use crate::errors::{CoordinatorError, CoordinatorResult};

pub mod manager;
pub mod writer;

pub struct TskvEngineStorage {
    tenant: String,
    db_name: String,
    vnode_id: VnodeId,
    meta: MetaRef,
    vnode: VnodeStorage,
    storage: tskv::EngineRef,
    grpc_enable_gzip: bool,
}

impl TskvEngineStorage {
    pub fn open(
        tenant: &str,
        db_name: &str,
        vnode_id: VnodeId,
        meta: MetaRef,
        vnode: VnodeStorage,
        storage: tskv::EngineRef,
        grpc_enable_gzip: bool,
    ) -> Self {
        Self {
            meta,
            vnode_id,
            storage,
            vnode,
            tenant: tenant.to_owned(),
            db_name: db_name.to_owned(),
            grpc_enable_gzip,
        }
    }

    pub async fn download_snapshot(
        &self,
        dir: &PathBuf,
        snapshot: &VnodeSnapshot,
    ) -> CoordinatorResult<()> {
        let channel = self.meta.get_node_conn(snapshot.node_id).await?;
        let mut client = tskv_service_time_out_client(
            channel,
            Duration::from_secs(60 * 60),
            DEFAULT_GRPC_SERVER_MESSAGE_LEN,
            self.grpc_enable_gzip,
        );

        info!("download snapshot to path: {:?}", dir);
        if let Err(err) = self
            .download_snapshot_files(dir, snapshot, &mut client)
            .await
        {
            tokio::fs::remove_dir_all(&dir).await?;
            return Err(err);
        }

        info!("success download snapshot all files");

        Ok(())
    }

    async fn download_snapshot_files(
        &self,
        dir: &Path,
        snapshot: &VnodeSnapshot,
        client: &mut TskvServiceClient<Timeout<Channel>>,
    ) -> CoordinatorResult<()> {
        let src_dir = StorageOptions::fmt_snapshot_dir(
            &models::schema::make_owner(&self.tenant, &self.db_name),
            snapshot.vnode_id,
            &snapshot.snapshot_id,
        );

        for info in snapshot.files_info.iter() {
            let filename = dir.join(&info.name);
            let src_filename = src_dir.join(&info.name).to_string_lossy().to_string();

            info!(
                "begin download file:{} -> {:?}, from {}",
                src_filename, filename, snapshot.node_id
            );

            Self::download_file(&src_filename, &filename, client).await?;
            let filename = filename.to_string_lossy().to_string();
            let tmp_info = file_info::get_file_info(&filename).await?;
            if tmp_info.md5 != info.md5 {
                return Err(CoordinatorError::CommonError {
                    msg: "download file md5 not match ".to_string(),
                });
            }
        }

        Ok(())
    }

    async fn download_file(
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
            if received.code != crate::SUCCESS_RESPONSE_CODE {
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

    async fn exec_apply(
        &self,
        ctx: &ApplyContext,
        req: &replication::Request,
    ) -> ReplicationResult<replication::Response> {
        let request = parse_prost_bytes::<RaftWriteCommand>(req)?;
        if let Some(command) = request.command {
            self.vnode.apply(ctx, command).await.map_err(|err| {
                ReplicationError::ApplyEngineErr {
                    msg: err.to_string(),
                }
            })?;
        }

        Ok(vec![])
    }
}

#[async_trait::async_trait]
impl ApplyStorage for TskvEngineStorage {
    async fn apply(
        &mut self,
        ctx: &ApplyContext,
        req: &replication::Request,
    ) -> ReplicationResult<replication::Response> {
        let apply_result = self.exec_apply(ctx, req).await;
        if let Err(err) = &apply_result {
            error!("replication apply failed: {:?}; {:?}", ctx, err);
        }

        match bincode::serialize(&apply_result) {
            Ok(data) => Ok(data),
            Err(err) => Ok(err.to_string().into()),
        }
    }

    async fn snapshot(&mut self) -> ReplicationResult<Vec<u8>> {
        let snapshot = self.vnode.create_snapshot().await.map_err(|err| {
            ReplicationError::CreateSnapshotErr {
                msg: err.to_string(),
            }
        })?;

        let data = bincode::serialize(&snapshot)?;
        Ok(data)
    }

    async fn restore(&mut self, data: &[u8]) -> ReplicationResult<()> {
        let snapshot = bincode::deserialize::<VnodeSnapshot>(data)?;
        let opt = self.storage.get_storage_options();
        let download_dir = opt.path().join(&snapshot.snapshot_id);

        self.download_snapshot(&download_dir, &snapshot)
            .await
            .map_err(|err| ReplicationError::RestoreSnapshotErr {
                msg: err.to_string(),
            })?;

        self.vnode
            .apply_snapshot(snapshot, &download_dir)
            .await
            .map_err(|err| ReplicationError::RestoreSnapshotErr {
                msg: err.to_string(),
            })?;

        Ok(())
    }

    async fn destory(&mut self) -> ReplicationResult<()> {
        info!("destory vnode id: {}", self.vnode_id);
        self.storage
            .remove_tsfamily(&self.tenant, &self.db_name, self.vnode_id)
            .await
            .map_err(|err| ReplicationError::DestoryRaftNodeErr {
                msg: err.to_string(),
            })?;

        Ok(())
    }
}
