use std::sync::Arc;

use async_trait::async_trait;
use models::auth::user::{User, UserInfo};
use models::auth::AuthError;
use models::oid::Oid;

pub type Result<T> = std::result::Result<T, AuthError>;

pub type AccessControlRef = Arc<dyn AccessControl + Send + Sync>;

#[async_trait]
pub trait AccessControl {
    async fn access_check(&self, user_info: &UserInfo, tenant_name: Option<&str>) -> Result<User>;

    async fn tenant_id(&self, tenant_name: &str) -> Result<Oid>;
}
