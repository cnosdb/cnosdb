use std::collections::HashMap;

use models::oid::{MemoryOidGenerator, OidGenerator};
use parking_lot::RwLock;

use super::{user::UserDesc, AuthError, Result};

#[derive(Default)]
pub struct UserManager {
    // all users
    // user name -> user desc
    // Store in meta(user name -> user desc)
    users: HashMap<String, UserDesc>,
    oid_generator: MemoryOidGenerator,

    lock: RwLock<()>,
}

impl UserManager {
    pub async fn create_user(&mut self, name: String, password: String) -> Result<()> {
        let oid = self
            .oid_generator
            .next_oid()
            .await
            .map_err(|error| AuthError::IdGenerate { error })?;

        let _lock = self.lock.write();

        if self.users.contains_key(&name) {
            return Err(AuthError::UserAlreadyExists { user: name.clone() });
        }

        let user_desc = UserDesc::new(oid, name.clone(), password);

        self.users.insert(name, user_desc);

        Ok(())
    }

    pub async fn user(&self, name: &str) -> Result<Option<&UserDesc>> {
        let _lock = self.lock.read();

        Ok(self.users.get(name))
    }

    pub async fn users(&self) -> Result<Vec<&UserDesc>> {
        let _lock = self.lock.read();

        Ok(self.users.values().collect())
    }

    pub async fn drop_user(&mut self, name: &str) -> Result<bool> {
        let _lock = self.lock.write();

        Ok(self.users.remove(name).is_some())
    }

    pub async fn rename_user(&mut self, old_name: &str, new_name: String) -> Result<()> {
        let _lock = self.lock.write();

        let new_user = self
            .users
            .remove(old_name)
            .ok_or_else(|| AuthError::UserNotFound {
                user: old_name.to_string(),
            })?
            .rename(new_name.clone());

        self.users.insert(new_name, new_user);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use models::oid::Identifier;

    use super::*;

    #[tokio::test]
    async fn test_create_user() {
        let mut manager = UserManager::default();

        let name = "test".to_string();
        let password = "123456".to_string();

        manager
            .create_user(name.clone(), password.clone())
            .await
            .expect("create user");

        let user = manager
            .user(&name)
            .await
            .expect("query user")
            .expect("user exists");
        assert_eq!(&name, user.name());
        assert!(user.check_password(&password));

        let success = manager.drop_user(&name).await.expect("drop user");
        assert!(success);

        let user = manager.user(&name).await.expect("query user");
        assert!(user.is_none());

        let success = manager.drop_user(&name).await.expect("drop user");
        assert!(!success);
    }

    #[tokio::test]
    async fn test_query_users() {
        let mut manager = UserManager::default();

        let users = vec![
            ("test1".to_string(), "123456".to_string()),
            ("test2".to_string(), "123456".to_string()),
            ("test3".to_string(), "123456".to_string()),
        ];

        for (name, password) in &users {
            manager
                .create_user(name.clone(), password.clone())
                .await
                .expect("create user");
        }

        let users_desc = manager.users().await.expect("query users");

        assert_eq!(users.len(), users_desc.len());

        for desc in users_desc {
            let exists = users.iter().any(|e| e.0 == desc.name());
            assert!(exists)
        }
    }
}
