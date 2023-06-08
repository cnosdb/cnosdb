mod cache;
mod sharded;

pub use sharded::*;

pub type AfterRemovedFnMut<K, V> = Box<dyn FnMut(&K, &mut V) + Send + Sync>;
