use std::fmt::Display;
use std::hash::Hash;

use uuid::Uuid;

pub trait Id: Eq + Hash + Clone + Display {}

pub trait Identifier<T> {
    fn id(&self) -> &T;

    fn name(&self) -> &str;
}

/// object identifier(e.g. tenant/database/user)
pub type Oid = u128;

impl Id for Oid {}

#[async_trait::async_trait]
pub trait OidGenerator {
    fn next_oid(&self) -> std::result::Result<Oid, String>;
}

#[derive(Default)]
pub struct MemoryOidGenerator {
    delegate: UuidGenerator,
}

#[async_trait::async_trait]
impl OidGenerator for MemoryOidGenerator {
    fn next_oid(&self) -> std::result::Result<Oid, String> {
        Ok(self.delegate.next_id())
    }
}

#[derive(Default, Clone)]
pub struct UuidGenerator {}

impl UuidGenerator {
    pub fn next_id(&self) -> u128 {
        Uuid::new_v4().as_u128()
    }
}
