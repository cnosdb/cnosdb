use std::path::{Path, PathBuf};
use std::time::Duration;

use meta::model::MetaRef;
use models::meta_data::VnodeId;
use protos::kv_service::tskv_service_client::TskvServiceClient;
use protos::kv_service::{DownloadFileRequest, RaftWriteCommand};
use protos::models_helper::parse_prost_bytes;
use protos::{tskv_service_time_out_client, DEFAULT_GRPC_SERVER_MESSAGE_LEN};
use replication::errors::{ReplicationError, ReplicationResult};
use replication::{ApplyContext, ApplyStorage, SnapshotMode};
use tokio::io::AsyncWriteExt;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tower::timeout::Timeout;
use tracing::{error, info};
use tskv::file_system::file_manager;
use tskv::kv_option::DATA_PATH;
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
        let src_dir = PathBuf::from(DATA_PATH)
            .join(&snapshot.version_edit.tsf_name)
            .join(snapshot.vnode_id.to_string());

        for info in snapshot.version_edit.add_files.iter() {
            let filename = dir.join(info.relative_path());
            let src_filename = src_dir
                .join(info.relative_path())
                .to_string_lossy()
                .to_string();

            info!(
                "begin download file {} -> {:?}, from {}",
                src_filename, filename, snapshot.node_id
            );

            Self::download_file(&src_filename, &filename, client).await?;
            let filename = filename.to_string_lossy().to_string();
            let length = file_manager::get_file_length(&filename).await?;
            if info.file_size != length {
                return Err(CoordinatorError::CommonError {
                    msg: format!(
                        "download file length not match {} -> {}",
                        info.file_size, length
                    ),
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
        let mut resp_stream = client.download_file(request).await?.into_inner();
        while let Some(received) = resp_stream.next().await {
            let received = received?;
            let data = crate::errors::decode_grpc_response(received)?;
            file.write_all(&data).await?;
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

    async fn snapshot(&mut self, mode: SnapshotMode) -> ReplicationResult<(Vec<u8>, Option<u64>)> {
        let snapshot = self.vnode.create_snapshot(mode).await.map_err(|err| {
            ReplicationError::CreateSnapshotErr {
                msg: err.to_string(),
            }
        })?;

        let index = snapshot.last_seq_no;
        let data = bincode::serialize(&snapshot)?;
        Ok((data, Some(index)))
    }

    async fn restore(&mut self, data: &[u8]) -> ReplicationResult<()> {
        let snapshot = bincode::deserialize::<VnodeSnapshot>(data)?;
        let opt = self.storage.get_storage_options();
        let snapshot_name = format!(
            "snap_{}_{}_{}_{}",
            snapshot.node_id, snapshot.vnode_id, snapshot.last_seq_no, snapshot.create_time
        );
        let download_dir = opt.path().join(snapshot_name);

        self.download_snapshot(&download_dir, &snapshot)
            .await
            .map_err(|err| ReplicationError::RestoreSnapshotErr {
                msg: err.to_string(),
            })?;

        self.vnode
            .apply_snapshot(snapshot, download_dir.as_path())
            .await
            .map_err(|err| ReplicationError::RestoreSnapshotErr {
                msg: err.to_string(),
            })?;

        tokio::fs::remove_dir_all(download_dir).await?;

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
