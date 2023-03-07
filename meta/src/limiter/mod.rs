use std::fmt::Debug;

use async_trait::async_trait;

use crate::error::MetaResult;

pub mod limiter_kind;
pub mod local_request_limiter;
pub mod none_limiter;
pub mod remote_request_limiter;

pub use limiter_kind::RequestLimiterKind;
pub use local_request_limiter::LocalRequestLimiter;
pub use none_limiter::NoneLimiter;

#[async_trait]
pub trait RequestLimiter: Send + Sync + Debug {
    async fn check_data_in(&self, data_len: usize) -> MetaResult<()>;
    async fn check_data_out(&self, data_len: usize) -> MetaResult<()>;
    async fn check_query(&self) -> MetaResult<()>;
    async fn check_write(&self) -> MetaResult<()>;
}
