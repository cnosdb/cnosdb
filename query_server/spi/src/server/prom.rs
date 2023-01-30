use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use coordinator::service::CoordinatorRef;
use meta::meta_client::MetaRef;
use models::auth::user::UserInfo;

use crate::{service::protocol::Context, Result};

pub type PromRemoteServerRef = Arc<dyn PromRemoteServer + Send + Sync>;

#[async_trait]
pub trait PromRemoteServer {
    async fn remote_read(&self, ctx: &Context, meta: MetaRef, req: Bytes) -> Result<Vec<u8>>;

    async fn remote_write(
        &self,
        tenant: &str,
        db: &str,
        user_info: &UserInfo,
        req: Bytes,
        coord: CoordinatorRef,
    ) -> Result<()>;
}
