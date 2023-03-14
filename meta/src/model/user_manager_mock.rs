#![allow(dead_code, unused_variables)]

use std::fmt::Debug;

use models::auth::role::UserRole;
use models::auth::user::{User, UserDesc, UserOptions, UserOptionsBuilder};
use models::oid::{Identifier, Oid};
use trace::debug;

use crate::error::MetaResult;
use crate::model::UserManager;

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
