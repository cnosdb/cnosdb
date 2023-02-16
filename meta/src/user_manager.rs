#![allow(dead_code, unused_imports, unused_variables)]

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use models::auth::privilege::DatabasePrivilege;
use models::auth::role::{
    CustomTenantRole, SystemTenantRole, TenantRole, TenantRoleIdentifier, UserRole,
};
use models::auth::user::{User, UserDesc, UserOptions, UserOptionsBuilder};
use models::auth::AuthError;
use models::oid::{Identifier, Oid};
use trace::debug;

use crate::client::MetaHttpClient;
use crate::error::{MetaError, MetaResult};
use crate::store::command::{
    self, META_REQUEST_FAILED, META_REQUEST_USER_EXIST, META_REQUEST_USER_NOT_FOUND,
};

#[async_trait]
pub trait UserManager: Send + Sync + Debug {
    // user
    async fn create_user(
        &self,
        name: String,
        options: UserOptions,
        is_admin: bool,
    ) -> MetaResult<Oid>;
    async fn user(&self, name: &str) -> MetaResult<Option<UserDesc>>;
    // fn user_with_privileges(&self, name: &str) -> Result<Option<User>>;
    async fn users(&self) -> MetaResult<Vec<UserDesc>>;
    async fn alter_user(&self, name: &str, options: UserOptions) -> MetaResult<()>;
    async fn drop_user(&self, name: &str) -> MetaResult<bool>;
    async fn rename_user(&self, old_name: &str, new_name: String) -> MetaResult<()>;
}

#[derive(Debug)]
pub struct RemoteUserManager {
    client: MetaHttpClient,
    cluster: String,
}

impl RemoteUserManager {
    pub fn new(cluster: String, cluster_meta: String) -> Self {
        Self {
            client: MetaHttpClient::new(cluster_meta),
            cluster,
        }
    }
}

#[async_trait::async_trait]
impl UserManager for RemoteUserManager {
    async fn create_user(
        &self,
        name: String,
        options: UserOptions,
        is_admin: bool,
    ) -> MetaResult<Oid> {
        let req = command::WriteCommand::CreateUser(self.cluster.clone(), name, options, is_admin);

        match self.client.write::<command::CommonResp<Oid>>(&req).await? {
            command::CommonResp::Ok(data) => Ok(data),
            command::CommonResp::Err(status) => {
                // TODO improve response
                if status.code == META_REQUEST_USER_EXIST {
                    Err(MetaError::UserAlreadyExists { user: status.msg })
                } else {
                    Err(MetaError::CommonError { msg: status.msg })
                }
            }
        }
    }

    async fn user(&self, name: &str) -> MetaResult<Option<UserDesc>> {
        let req = command::ReadCommand::User(self.cluster.clone(), name.to_string());

        match self
            .client
            .read::<command::CommonResp<Option<UserDesc>>>(&req)
            .await?
        {
            command::CommonResp::Ok(data) => Ok(data),
            command::CommonResp::Err(status) => {
                // TODO improve response
                Err(MetaError::CommonError { msg: status.msg })
            }
        }
    }

    async fn users(&self) -> MetaResult<Vec<UserDesc>> {
        let req = command::ReadCommand::Users(self.cluster.clone());

        match self
            .client
            .read::<command::CommonResp<Vec<UserDesc>>>(&req)
            .await?
        {
            command::CommonResp::Ok(data) => Ok(data),
            command::CommonResp::Err(status) => {
                // TODO improve response
                Err(MetaError::CommonError { msg: status.msg })
            }
        }
    }

    async fn alter_user(&self, name: &str, options: UserOptions) -> MetaResult<()> {
        let req = command::WriteCommand::AlterUser(self.cluster.clone(), name.to_string(), options);

        match self
            .client
            .write::<command::CommonResp<UserDesc>>(&req)
            .await?
        {
            command::CommonResp::Ok(data) => Ok(()),
            command::CommonResp::Err(status) => {
                // TODO improve response
                if status.code == META_REQUEST_USER_NOT_FOUND {
                    Err(MetaError::UserNotFound { user: status.msg })
                } else {
                    Err(MetaError::CommonError { msg: status.msg })
                }
            }
        }
    }

    async fn drop_user(&self, name: &str) -> MetaResult<bool> {
        let req = command::WriteCommand::DropUser(self.cluster.clone(), name.to_string());

        match self.client.write::<command::CommonResp<bool>>(&req).await? {
            command::CommonResp::Ok(e) => Ok(e),
            command::CommonResp::Err(status) => {
                if status.code == META_REQUEST_USER_NOT_FOUND {
                    Err(MetaError::UserNotFound { user: status.msg })
                } else {
                    // TODO improve response
                    Err(MetaError::CommonError { msg: status.msg })
                }
            }
        }
    }

    async fn rename_user(&self, old_name: &str, new_name: String) -> MetaResult<()> {
        let req =
            command::WriteCommand::RenameUser(self.cluster.clone(), old_name.to_string(), new_name);

        match self.client.write::<command::CommonResp<()>>(&req).await? {
            command::CommonResp::Ok(_) => Ok(()),
            command::CommonResp::Err(status) => {
                // TODO improve response
                if status.code == META_REQUEST_USER_NOT_FOUND {
                    Err(MetaError::UserNotFound { user: status.msg })
                } else {
                    Err(MetaError::CommonError { msg: status.msg })
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct UserManagerMock {
    mock_user: User,
}

impl Default for UserManagerMock {
    fn default() -> Self {
        Self::new()
    }
}

impl UserManagerMock {
    pub fn new() -> Self {
        let options = unsafe {
            UserOptionsBuilder::default()
                .password("123456")
                .build()
                .unwrap_unchecked()
        };
        let mock_desc = UserDesc::new(0_u128, "name".to_string(), options, false);
        let mock_user = User::new(mock_desc, UserRole::Dba.to_privileges());
        Self { mock_user }
    }
}

#[async_trait::async_trait]
impl UserManager for UserManagerMock {
    async fn create_user(
        &self,
        name: String,
        options: UserOptions,
        is_admin: bool,
    ) -> MetaResult<Oid> {
        debug!("AuthClientMock::create_user({}, {})", name, options);
        Ok(*self.mock_user.desc().id())
    }

    async fn user(&self, name: &str) -> MetaResult<Option<UserDesc>> {
        debug!("AuthClientMock::user({})", name);

        Ok(Some(self.mock_user.desc().clone()))
    }

    async fn users(&self) -> MetaResult<Vec<UserDesc>> {
        debug!("AuthClientMock::users()");

        Ok(vec![self.mock_user.desc().clone()])
    }

    async fn alter_user(&self, user_id: &str, options: UserOptions) -> MetaResult<()> {
        debug!("AuthClientMock::alter_user({}, {})", user_id, options);

        Ok(())
    }

    async fn drop_user(&self, name: &str) -> MetaResult<bool> {
        debug!("AuthClientMock::drop_user({})", name);

        Ok(true)
    }

    async fn rename_user(&self, user_id: &str, new_name: String) -> MetaResult<()> {
        debug!("AuthClientMock::rename_user({}, {})", user_id, new_name);

        Ok(())
    }
}
