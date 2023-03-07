mod cache;
mod sharded;

pub use sharded::*;

pub type AfterRemovedFnMut<K, V> = Box<dyn FnMut(&K, &V) + Send + Sync>;
