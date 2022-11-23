use std::sync::Arc;

use super::auth_client::AuthClient;
use models::{
    auth::{
        user::{AuthType, User, UserInfo},
        AuthError,
    },
    oid::Oid,
};

pub type AccessControlRef<T> = Arc<AccessControl<T>>;

pub type Result<T> = std::result::Result<T, AuthError>;

pub struct AccessControl<T> {
    auth_client: Arc<T>,
}

impl<T> AccessControl<T> {
    pub fn new(auth_client: Arc<T>) -> Self {
        Self { auth_client }
    }
}

impl<T> AccessControl<T>
where
    T: AuthClient,
{
    pub fn access_check(&self, user_info: &UserInfo) -> Result<User> {
        let user_name = user_info.user.as_str();

        let user = self
            .auth_client
            .user_with_privileges(user_name)?
            .ok_or_else(|| AuthError::UserNotFound {
                user: user_name.to_string(),
            })?;

        let user_options = user.desc().options();

        AuthType::from(user_options).access_check(user_info)?;

        Ok(user)
    }

    pub fn tenant_id(&self, _tenant_name: &str) -> Result<Oid> {
        // TODO 查询租户信息，不存在则直接报错
        // tenant(&self, tenant_name: &str) -> Result<Tenant>;
        // Tenant::id(&self) -> &Oid
        Ok(0_u128)
    }
}
