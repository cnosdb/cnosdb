use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion::arrow::record_batch::RecordBatch;
use futures::{ready, Stream, StreamExt};
use meta::model::MetaRef;
use models::meta_data::{VnodeId, VnodeStatus};

use crate::errors::{CoordinatorError, CoordinatorResult};
use crate::{CoordinatorRecordBatchStream, SendableCoordinatorRecordBatchStream};

pub struct VnodeStatusListener {
    tenant: String,
    meta: MetaRef,
    vnode_id: VnodeId,
    input: SendableCoordinatorRecordBatchStream,
}

impl VnodeStatusListener {
    pub fn new(
        tenant: impl Into<String>,
        meta: MetaRef,
        vnode_id: VnodeId,
        input: SendableCoordinatorRecordBatchStream,
    ) -> Self {
        Self {
            tenant: tenant.into(),
            meta,
            vnode_id,
            input,
        }
    }
}

impl Stream for VnodeStatusListener {
    type Item = CoordinatorResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(self.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) => Poll::Ready(Some(Ok(batch))),
            Some(Err(err)) => {
                if err.invalid_vnode() {
                    let tenant = self.tenant.clone();
                    let vnode_id = self.vnode_id;
                    let meta = self.meta.clone();
                    trace::error!("vnode {} data broken: {:?}", vnode_id, err);

                    tokio::spawn(change_vnode_to_broken(tenant, vnode_id, meta));

                    // let _ = ready!(
                    //     Box::pin(change_vnode_to_broken(tenant, vnode_id, meta)).poll_unpin(cx)
                    // );
                    trace::debug!("updated vnode {} status broken", vnode_id);
                }

                Poll::Ready(Some(Err(err)))
            }
            None => Poll::Ready(None),
        }
    }
}

impl CoordinatorRecordBatchStream for VnodeStatusListener {}

pub async fn change_vnode_to_broken(
    tenant: String,
    vnode_id: VnodeId,
    meta: MetaRef,
) -> CoordinatorResult<()> {
    let mut all_info = crate::get_vnode_all_info(meta.clone(), &tenant, vnode_id).await?;

    let meta_client = meta
        .tenant_manager()
        .tenant_meta(&tenant)
        .await
        .ok_or(CoordinatorError::TenantNotFound { name: tenant })?;

    all_info.set_status(VnodeStatus::Broken);
    meta_client.update_vnode(&all_info).await?;

    Ok(())
}
