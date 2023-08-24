use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use protocol_parser::Line;
use protos::prompb::remote::WriteRequest;
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

    fn remote_write(&self, req: Bytes) -> Result<WriteRequest>;

    fn prom_write_request_to_lines<'a>(&self, req: &'a WriteRequest) -> Result<Vec<Line<'a>>>;
}
