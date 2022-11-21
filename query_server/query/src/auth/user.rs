use std::collections::HashSet;

use models::oid::{Identifier, Oid};

use super::privilege::{Privilege, PrivilegeChecker};

pub struct User {
    desc: UserDesc,
    privileges: HashSet<Privilege<Oid>>,
}

impl User {
    pub fn new(desc: UserDesc, privileges: HashSet<Privilege<Oid>>) -> Self {
        Self { desc, privileges }
    }

    pub fn desc(&self) -> &UserDesc {
        &self.desc
    }

    pub fn check_privilege(&self, privilege: &Privilege<Oid>) -> bool {
        self.privileges.iter().any(|e| e.check_privilege(privilege))
    }
}

#[derive(Debug, Clone)]
pub struct UserDesc {
    id: Oid,
    // ident
    name: String,
    password: String,
}

impl UserDesc {
    pub fn new(id: Oid, name: String, password: String) -> Self {
        Self { id, name, password }
    }

    pub fn check_password(&self, password: &str) -> bool {
        self.password == password
    }

    pub fn rename(mut self, new_name: String) -> Self {
        self.name = new_name;
        self
    }
}

impl Eq for UserDesc {}

impl PartialEq for UserDesc {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.name == other.name
    }
}

impl Identifier<Oid> for UserDesc {
    fn id(&self) -> &Oid {
        &self.id
    }

    fn name(&self) -> &str {
        &self.name
    }
}
