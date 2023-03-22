#![allow(dead_code)]

use std::fmt::Debug;

use models::auth::user::{UserDesc, UserOptions};
use models::oid::Oid;

use crate::client::MetaHttpClient;
use crate::error::{MetaError, MetaResult};
use crate::model::UserManager;
use crate::store::command::{
    self, CommonResp, META_REQUEST_USER_EXIST, META_REQUEST_USER_NOT_FOUND,
};

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

        if let CommonResp::Err(status) = self.client.write::<CommonResp<()>>(&req).await? {
            // TODO improve response
            if status.code == META_REQUEST_USER_NOT_FOUND {
                Err(MetaError::UserNotFound { user: status.msg })
            } else {
                Err(MetaError::CommonError { msg: status.msg })
            }
        } else {
            Ok(())
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
