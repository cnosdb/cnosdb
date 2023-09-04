use models::meta_data::VnodeId;
use models::schema::Precision;
use protos::kv_service::WriteReplicaRequest;
use protos::models_helper::parse_prost_bytes;
use replication::apply_store::{ApplyContext, ApplyStorage};
use replication::errors::{ReplicationError, ReplicationResult};

pub mod manager;
pub mod writer;

pub struct TskvEngineStorage {
    vnode_id: VnodeId,
    storage: tskv::EngineRef,
}

impl TskvEngineStorage {
    pub fn open(vnode_id: VnodeId, storage: tskv::EngineRef) -> Self {
        Self { vnode_id, storage }
    }
}

#[async_trait::async_trait]
impl ApplyStorage for TskvEngineStorage {
    async fn apply(
        &self,
        ctx: &ApplyContext,
        req: &replication::Request,
    ) -> ReplicationResult<replication::Response> {
        let request = parse_prost_bytes::<WriteReplicaRequest>(req)?;

        self.storage
            .write_memcache(
                ctx.index,
                &request.tenant,
                request.data,
                self.vnode_id,
                Precision::from(request.precision as u8),
                None,
            )
            .await
            .map_err(|err| ReplicationError::ApplyEngineFailed {
                msg: err.to_string(),
            })?;

        Ok(vec![])
    }

    async fn snapshot(&self) -> ReplicationResult<Vec<u8>> {
        Ok(vec![])
    }

    async fn restore(&self, _snapshot: &[u8]) -> ReplicationResult<()> {
        Ok(())
    }
}
