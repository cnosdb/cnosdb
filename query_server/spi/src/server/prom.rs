use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use coordinator::service::CoordinatorRef;
use meta::MetaRef;

use crate::service::protocol::Context;
use crate::Result;

pub type PromRemoteServerRef = Arc<dyn PromRemoteServer + Send + Sync>;

#[async_trait]
pub trait PromRemoteServer {
    async fn remote_read(&self, ctx: &Context, meta: MetaRef, req: Bytes) -> Result<Vec<u8>>;

    async fn remote_write(&self, req: Bytes, ctx: &Context, coord: CoordinatorRef) -> Result<()>;
}
