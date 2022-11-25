use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    sync::Arc,
};

use models::{
    auth::{
        privilege::DatabasePrivilege,
        role::{CustomTenantRole, SystemTenantRole, TenantRole, TenantRoleIdentifier, UserRole},
        user::{User, UserDesc, UserOptions, UserOptionsBuilder},
        AuthError,
    },
    oid::Oid,
};
use trace::debug;

pub type Result<T> = std::result::Result<T, AuthError>;

pub trait UserManager: Send + Sync + Debug {
    // user
    fn create_user(&mut self, name: String, options: UserOptions) -> Result<&UserDesc>;
    fn user(&self, name: &str) -> Result<Option<UserDesc>>;
    fn user_with_privileges(&self, name: &str) -> Result<Option<User>>;
    fn users(&self) -> Result<Vec<UserDesc>>;
    fn alter_user(&self, user_id: &Oid, options: UserOptions) -> Result<()>;
    fn drop_user(&mut self, name: &str) -> Result<bool>;
    fn rename_user(&mut self, user_id: &Oid, new_name: String) -> Result<()>;

    // // tenant role
    // fn create_custom_role_of_tenant(
    //     &mut self,
    //     tenant_id: &Oid,
    //     role_name: String,
    //     system_role: SystemTenantRole,
    //     additiona_privileges: HashMap<String, DatabasePrivilege>,
    // ) -> Result<()>;
    // fn grant_privilege_to_custom_role_of_tenant(
    //     &mut self,
    //     database_name: String,
    //     database_privileges: Vec<(DatabasePrivilege, Oid)>,
    //     role_name: &str,
    //     tenant_id: &Oid,
    // ) -> Result<()>;
    // fn revoke_privilege_from_custom_role_of_tenant(
    //     &mut self,
    //     database_name: &str,
    //     database_privileges: Vec<(DatabasePrivilege, Oid)>,
    //     role_name: &str,
    //     tenant_id: &Oid,
    // ) -> Result<bool>;
    // fn drop_custom_role_of_tenant(&mut self, role_name: &str, tenant_id: &Oid) -> Result<bool>;
    // fn custom_roles_of_tenant(&self, tenant_id: &Oid) -> Result<Vec<CustomTenantRole<Oid>>>;
    // fn custom_role_of_tenant(
    //     &self,
    //     role_name: &str,
    //     tenant_id: &Oid,
    // ) -> Result<Option<CustomTenantRole<Oid>>>;

    // // tenant member
    // fn tenants_of_user(&mut self, user_id: &Oid) -> Result<Option<&HashSet<Oid>>>;
    // fn add_member_with_role_to_tenant(
    //     &mut self,
    //     user_id: Oid,
    //     role: TenantRoleIdentifier,
    //     tenant_id: Oid,
    // ) -> Result<()>;
    // fn remove_member_from_tenant(&mut self, user_id: Oid, tenant_id: Oid) -> Result<()>;
    // fn remove_member_from_all_tenants(&mut self, user_id: &Oid) -> Result<bool>;
    // fn reasign_member_role_in_tenant(
    //     &mut self,
    //     user_id: Oid,
    //     role: TenantRoleIdentifier,
    //     tenant_id: Oid,
    // ) -> Result<()>;
    // fn member_role_of_tenant(&self, user_id: &Oid, tenant_id: &Oid) -> Result<TenantRole<Oid>>;
    // fn members_of_tenant(&self, tenant_id: &Oid) -> Result<Option<HashSet<&Oid>>>;
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
        let mock_desc = UserDesc::new(0_u128, "name".to_string(), options);
        let mock_user = User::new(mock_desc, UserRole::Dba.to_privileges());
        Self { mock_user }
    }
}

impl UserManager for UserManagerMock {
    fn create_user(&mut self, name: String, options: UserOptions) -> Result<&UserDesc> {
        debug!("AuthClientMock::create_user({}, {})", name, options);
        Ok(self.mock_user.desc())
    }

    fn user(&self, name: &str) -> Result<Option<UserDesc>> {
        debug!("AuthClientMock::user({})", name);

        Ok(Some(self.mock_user.desc().clone()))
    }

    fn user_with_privileges(&self, name: &str) -> Result<Option<User>> {
        debug!("AuthClientMock::user_with_privileges({})", name);

        Ok(Some(self.mock_user.clone()))
    }

    fn users(&self) -> Result<Vec<UserDesc>> {
        debug!("AuthClientMock::users()");

        Ok(vec![self.mock_user.desc().clone()])
    }

    fn alter_user(&self, user_id: &Oid, options: UserOptions) -> Result<()> {
        debug!("AuthClientMock::alter_user({}, {})", user_id, options);

        Ok(())
    }

    fn drop_user(&mut self, name: &str) -> Result<bool> {
        debug!("AuthClientMock::drop_user({})", name);

        Ok(true)
    }

    fn rename_user(&mut self, user_id: &Oid, new_name: String) -> Result<()> {
        debug!("AuthClientMock::rename_user({}, {})", user_id, new_name);

        Ok(())
    }

    // fn tenants_of_user(&mut self, user_id: &Oid) -> Result<Option<&HashSet<Oid>>> {
    //     debug!("AuthClientMock::tenants_of_user({})", user_id);

    //     Ok(None)
    // }

    // fn create_custom_role_of_tenant(
    //     &mut self,
    //     tenant_id: &Oid,
    //     role_name: String,
    //     system_role: SystemTenantRole,
    //     additiona_privileges: HashMap<String, DatabasePrivilege>,
    // ) -> Result<()> {
    //     debug!(
    //         "AuthClientMock::create_custom_role_of_tenant({}, {}, {:?}, {:?})",
    //         tenant_id, role_name, system_role, additiona_privileges
    //     );

    //     Ok(())
    // }

    // fn grant_privilege_to_custom_role_of_tenant(
    //     &mut self,
    //     _database_name: String,
    //     _privilege: Vec<(DatabasePrivilege, Oid)>,
    //     _role_name: &str,
    //     _tenant_id: &Oid,
    // ) -> Result<()> {
    //     Ok(())
    // }

    // fn revoke_privilege_from_custom_role_of_tenant(
    //     &mut self,
    //     _database_name: &str,
    //     _privilege: Vec<(DatabasePrivilege, Oid)>,
    //     _role_name: &str,
    //     _tenant_id: &Oid,
    // ) -> Result<bool> {
    //     Ok(true)
    // }

    // fn drop_custom_role_of_tenant(&mut self, _role_name: &str, _tenant_id: &Oid) -> Result<bool> {
    //     Ok(true)
    // }

    // fn custom_roles_of_tenant(&self, _tenant_id: &Oid) -> Result<Vec<CustomTenantRole<Oid>>> {
    //     Ok(vec![])
    // }

    // fn custom_role_of_tenant(
    //     &self,
    //     _role_name: &str,
    //     _tenant_id: &Oid,
    // ) -> Result<Option<CustomTenantRole<Oid>>> {
    //     Ok(None)
    // }

    // fn add_member_with_role_to_tenant(
    //     &mut self,
    //     _user_id: Oid,
    //     _role: TenantRoleIdentifier,
    //     _tenant_id: Oid,
    // ) -> Result<()> {
    //     Ok(())
    // }

    // fn remove_member_from_tenant(&mut self, _user_id: Oid, _tenant_id: Oid) -> Result<()> {
    //     Ok(())
    // }

    // fn remove_member_from_all_tenants(&mut self, _user_id: &Oid) -> Result<bool> {
    //     Ok(true)
    // }

    // fn reasign_member_role_in_tenant(
    //     &mut self,
    //     _user_id: Oid,
    //     _role: TenantRoleIdentifier,
    //     _tenant_id: Oid,
    // ) -> Result<()> {
    //     Ok(())
    // }

    // fn member_role_of_tenant(&self, _user_id: &Oid, _tenant_id: &Oid) -> Result<TenantRole<Oid>> {
    //     Ok(TenantRole::System(SystemTenantRole::Owner))
    // }

    // fn members_of_tenant(&self, _tenant_id: &Oid) -> Result<Option<HashSet<&Oid>>> {
    //     Ok(None)
    // }
}
