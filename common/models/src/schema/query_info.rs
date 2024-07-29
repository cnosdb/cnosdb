use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::auth::user::User;
use crate::meta_data::NodeId;
use crate::oid::{uuid_u64, Identifier, Oid};

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QueryId(u64);

impl QueryId {
    pub fn next_id() -> Self {
        Self(uuid_u64())
    }

    pub fn get(&self) -> u64 {
        self.0
    }
}

impl From<QueryId> for u64 {
    fn from(val: QueryId) -> Self {
        val.0
    }
}

impl From<u64> for QueryId {
    fn from(u: u64) -> Self {
        QueryId(u)
    }
}

impl TryFrom<Vec<u8>> for QueryId {
    type Error = String;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        if bytes.len() != 8 {
            return Err(format!("Incorrect content: {:?}", &bytes));
        }

        let len_bytes: [u8; 8] = unsafe { bytes[0..8].try_into().unwrap_unchecked() };

        Ok(Self(u64::from_le_bytes(len_bytes)))
    }
}

impl From<QueryId> for Vec<u8> {
    fn from(val: QueryId) -> Self {
        val.0.to_le_bytes().into()
    }
}

impl Display for QueryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryInfo {
    query_id: QueryId,
    query: String,

    tenant_id: Oid,
    tenant_name: String,
    database_name: String,
    user: User,
    pub node_id: NodeId,
}

impl QueryInfo {
    pub fn new(
        query_id: QueryId,
        query: String,
        tenant_id: Oid,
        tenant_name: String,
        database_name: String,
        user: User,
        node_id: NodeId,
    ) -> Self {
        Self {
            query_id,
            query,
            tenant_id,
            tenant_name,
            database_name,
            user,
            node_id,
        }
    }

    pub fn query_id(&self) -> QueryId {
        self.query_id
    }

    pub fn query(&self) -> &str {
        &self.query
    }

    pub fn tenant_id(&self) -> Oid {
        self.tenant_id
    }

    pub fn tenant_name(&self) -> &str {
        &self.tenant_name
    }

    pub fn database_name(&self) -> &str {
        &self.database_name
    }

    pub fn user(&self) -> &User {
        &self.user
    }

    pub fn user_id(&self) -> Oid {
        *self.user.desc().id()
    }

    pub fn user_name(&self) -> &str {
        self.user.desc().name()
    }
}
