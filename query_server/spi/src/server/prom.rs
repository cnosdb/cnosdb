use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use protos::kv_service::WritePointsRequest;
use trace::SpanContext;

use crate::service::protocol::Context;
use crate::Result;

pub type PromRemoteServerRef = Arc<dyn PromRemoteServer + Send + Sync>;

#[async_trait]
pub trait PromRemoteServer {
    async fn remote_read(
        &self,
        ctx: &Context,
        req: Bytes,
        span_ctx: Option<&SpanContext>,
    ) -> Result<Vec<u8>>;
    fn remote_write(&self, ctx: &Context, req: Bytes) -> Result<WritePointsRequest>;
}
