#![allow(dead_code)]

use std::fmt::Debug;

use models::auth::user::{UserDesc, UserOptions};
use models::oid::Oid;

use crate::client::MetaHttpClient;
use crate::error::MetaResult;
use crate::model::UserManager;
use crate::store::command;

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

        self.client.write::<Oid>(&req).await
    }

    async fn user(&self, name: &str) -> MetaResult<Option<UserDesc>> {
        let req = command::ReadCommand::User(self.cluster.clone(), name.to_string());

        self.client.read::<Option<UserDesc>>(&req).await
    }

    async fn users(&self) -> MetaResult<Vec<UserDesc>> {
        let req = command::ReadCommand::Users(self.cluster.clone());

        self.client.read::<Vec<UserDesc>>(&req).await
    }

    async fn alter_user(&self, name: &str, options: UserOptions) -> MetaResult<()> {
        let req = command::WriteCommand::AlterUser(self.cluster.clone(), name.to_string(), options);

        self.client.write::<()>(&req).await
    }

    async fn drop_user(&self, name: &str) -> MetaResult<bool> {
        let req = command::WriteCommand::DropUser(self.cluster.clone(), name.to_string());

        self.client.write::<()>(&req).await?;
        Ok(true)
    }

    async fn rename_user(&self, old_name: &str, new_name: String) -> MetaResult<()> {
        let req =
            command::WriteCommand::RenameUser(self.cluster.clone(), old_name.to_string(), new_name);

        self.client.write::<()>(&req).await
    }
}
