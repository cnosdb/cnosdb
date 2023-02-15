use std::fmt::{Debug, Formatter};

use async_trait::async_trait;

use crate::error::MetaResult;
use crate::limiter::RequestLimiter;

#[derive(Default)]
pub struct NoneLimiter;

unsafe impl Send for NoneLimiter {}
unsafe impl Sync for NoneLimiter {}

impl Debug for NoneLimiter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("NoneLimiter")
    }
}

#[async_trait]
impl RequestLimiter for NoneLimiter {
    async fn check_data_in(&self, _data_len: usize) -> MetaResult<()> {
        Ok(())
    }

    async fn check_data_out(&self, _data_len: usize) -> MetaResult<()> {
        Ok(())
    }

    async fn check_query(&self) -> MetaResult<()> {
        Ok(())
    }

    async fn check_write(&self) -> MetaResult<()> {
        Ok(())
    }
}
