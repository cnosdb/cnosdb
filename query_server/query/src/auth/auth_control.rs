use meta::model::MetaRef;
use models::auth::user::{AuthType, User, UserInfo};
use models::auth::AuthError;
use models::oid::{Identifier, Oid};
use spi::query::auth::AccessControl;
use trace::warn;

pub type Result<T> = std::result::Result<T, AuthError>;

#[derive(Clone)]
pub struct AccessControlImpl {
    inner: AccessControlNoCheck,
}

impl AccessControlImpl {
    pub fn new(inner: AccessControlNoCheck) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl AccessControl for AccessControlImpl {
    async fn access_check(&self, user_info: &UserInfo, tenant_name: Option<&str>) -> Result<User> {
        let user = self.inner.access_check(user_info, tenant_name).await?;

        let user_options = user.desc().options();
        // access check
        AuthType::from(user_options).access_check(user_info)?;

        Ok(user)
    }

    async fn tenant_id(&self, tenant_name: &str) -> Result<Oid> {
        // 查询租户信息，不存在则直接报错
        // tenant(&self, tenant_name: &str) -> Result<Tenant>;
        // Tenant::id(&self) -> &Oid
        self.inner.tenant_id(tenant_name).await
    }
}

#[derive(Clone)]
pub struct AccessControlNoCheck {
    meta_manager: MetaRef,
}

impl AccessControlNoCheck {
    pub fn new(meta_manager: MetaRef) -> Self {
        Self { meta_manager }
    }
}

#[async_trait::async_trait]
impl AccessControl for AccessControlNoCheck {
    async fn access_check(&self, user_info: &UserInfo, tenant_name: Option<&str>) -> Result<User> {
        let user_name = user_info.user.as_str();
        // only get user info with privileges
        self.meta_manager
            .user_with_privileges(user_name, tenant_name)
            .await
            .map_err(|err| {
                warn!("query user's privilege, error: {}", err);
                AuthError::Metadata {
                    err: format!("{}", err),
                }
            })
    }

    async fn tenant_id(&self, tenant_name: &str) -> Result<Oid> {
        let tenant_client = self
            .meta_manager
            .tenant_meta(tenant_name)
            .await
            .ok_or(AuthError::TenantNotFound)?;

        Ok(*tenant_client.tenant().id())
    }
}
