use std::sync::Arc;

use meta::meta_client::MetaRef;
use models::{
    auth::{
        user::{AuthType, User, UserInfo},
        AuthError,
    },
    oid::{Identifier, Oid},
};

pub type AccessControlRef = Arc<AccessControl>;

pub type Result<T> = std::result::Result<T, AuthError>;

pub struct AccessControl {
    meta_manager: MetaRef,
}

impl AccessControl {
    pub fn new(meta_manager: MetaRef) -> Self {
        Self { meta_manager }
    }
}

impl AccessControl {
    pub fn access_check(&self, user_info: &UserInfo) -> Result<User> {
        let user_name = user_info.user.as_str();

        let user = self
            .meta_manager
            .user_manager()
            .user_with_privileges(user_name)?
            .ok_or_else(|| AuthError::UserNotFound {
                user: user_name.to_string(),
            })?;

        let user_options = user.desc().options();

        AuthType::from(user_options).access_check(user_info)?;

        Ok(user)
    }

    pub fn tenant_id(&self, tenant_name: &str) -> Result<Oid> {
        // 查询租户信息，不存在则直接报错
        // tenant(&self, tenant_name: &str) -> Result<Tenant>;
        // Tenant::id(&self) -> &Oid
        let tenant_client = self
            .meta_manager
            .tenant_manager()
            .tenant_meta(tenant_name)
            .ok_or_else(|| AuthError::TenantNotFound)?;

        Ok(*tenant_client.tenant().id())
    }
}
