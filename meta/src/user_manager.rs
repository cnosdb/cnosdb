#![allow(dead_code, unused_imports, unused_variables)]

use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    sync::Arc,
};

use dashmap::DashMap;
use models::{
    auth::{
        privilege::DatabasePrivilege,
        role::{CustomTenantRole, SystemTenantRole, TenantRole, TenantRoleIdentifier, UserRole},
        user::{User, UserDesc, UserOptions, UserOptionsBuilder},
        AuthError,
    },
    oid::{Identifier, Oid},
};
use trace::debug;

use crate::error::{MetaError, MetaResult};
use crate::{
    client::MetaHttpClient,
    store::command::{
        self, META_REQUEST_FAILED, META_REQUEST_USER_EXIST, META_REQUEST_USER_NOT_FOUND,
    },
};

pub trait UserManager: Send + Sync + Debug {
    // user
    fn create_user(&self, name: String, options: UserOptions, is_admin: bool) -> MetaResult<Oid>;
    fn user(&self, name: &str) -> MetaResult<Option<UserDesc>>;
    // fn user_with_privileges(&self, name: &str) -> Result<Option<User>>;
    fn users(&self) -> MetaResult<Vec<UserDesc>>;
    fn alter_user(&self, name: &str, options: UserOptions) -> MetaResult<()>;
    fn drop_user(&self, name: &str) -> MetaResult<bool>;
    fn rename_user(&self, old_name: &str, new_name: String) -> MetaResult<()>;
}

#[derive(Debug)]
pub struct RemoteUserManager {
    client: MetaHttpClient,
    cluster: String,
}

impl RemoteUserManager {
    pub fn new(cluster: String, cluster_meta: String) -> Self {
        Self {
            client: MetaHttpClient::new(1, cluster_meta),
            cluster,
        }
    }
}

impl UserManager for RemoteUserManager {
    fn create_user(&self, name: String, options: UserOptions, is_admin: bool) -> MetaResult<Oid> {
        let req = command::WriteCommand::CreateUser(self.cluster.clone(), name, options, is_admin);

        match self.client.write::<command::CommonResp<Oid>>(&req)? {
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

    fn user(&self, name: &str) -> MetaResult<Option<UserDesc>> {
        let req = command::ReadCommand::User(self.cluster.clone(), name.to_string());

        match self
            .client
            .read::<command::CommonResp<Option<UserDesc>>>(&req)?
        {
            command::CommonResp::Ok(data) => Ok(data),
            command::CommonResp::Err(status) => {
                // TODO improve response
                Err(MetaError::CommonError { msg: status.msg })
            }
        }
    }

    fn users(&self) -> MetaResult<Vec<UserDesc>> {
        let req = command::ReadCommand::Users(self.cluster.clone());

        match self
            .client
            .read::<command::CommonResp<Vec<UserDesc>>>(&req)?
        {
            command::CommonResp::Ok(data) => Ok(data),
            command::CommonResp::Err(status) => {
                // TODO improve response
                Err(MetaError::CommonError { msg: status.msg })
            }
        }
    }

    fn alter_user(&self, name: &str, options: UserOptions) -> MetaResult<()> {
        let req = command::WriteCommand::AlterUser(self.cluster.clone(), name.to_string(), options);

        match self.client.write::<command::CommonResp<UserDesc>>(&req)? {
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

    fn drop_user(&self, name: &str) -> MetaResult<bool> {
        let req = command::WriteCommand::DropUser(self.cluster.clone(), name.to_string());

        match self.client.write::<command::CommonResp<bool>>(&req)? {
            command::CommonResp::Ok(e) => Ok(e),
            command::CommonResp::Err(status) => {
                // TODO improve response
                Err(MetaError::CommonError { msg: status.msg })
            }
        }
    }

    fn rename_user(&self, old_name: &str, new_name: String) -> MetaResult<()> {
        let req =
            command::WriteCommand::RenameUser(self.cluster.clone(), old_name.to_string(), new_name);

        match self.client.write::<command::CommonResp<()>>(&req)? {
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

impl UserManager for UserManagerMock {
    fn create_user(&self, name: String, options: UserOptions, is_admin: bool) -> MetaResult<Oid> {
        debug!("AuthClientMock::create_user({}, {})", name, options);
        Ok(*self.mock_user.desc().id())
    }

    fn user(&self, name: &str) -> MetaResult<Option<UserDesc>> {
        debug!("AuthClientMock::user({})", name);

        Ok(Some(self.mock_user.desc().clone()))
    }

    fn users(&self) -> MetaResult<Vec<UserDesc>> {
        debug!("AuthClientMock::users()");

        Ok(vec![self.mock_user.desc().clone()])
    }

    fn alter_user(&self, user_id: &str, options: UserOptions) -> MetaResult<()> {
        debug!("AuthClientMock::alter_user({}, {})", user_id, options);

        Ok(())
    }

    fn drop_user(&self, name: &str) -> MetaResult<bool> {
        debug!("AuthClientMock::drop_user({})", name);

        Ok(true)
    }

    fn rename_user(&self, user_id: &str, new_name: String) -> MetaResult<()> {
        debug!("AuthClientMock::rename_user({}, {})", user_id, new_name);

        Ok(())
    }
}
