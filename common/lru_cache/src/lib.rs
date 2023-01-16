use std::cell::RefCell;
use std::rc::Rc;

pub mod lru_cache;


pub trait NodeVal<T> {
    fn get_value(&self) -> &T;
}
type NodeValPtr<T> = Rc<RefCell<dyn NodeVal<T>>>;

pub trait Cache<T> {
    /// Insert a mapping from key->value into the cache and assign it the specified charge
    /// against the total cache capacity.
    ///
    /// Return a handle that corresponds to the mapping. the caller must call
    /// `release(handle)` when the returned mapping is no longer needed.
    ///
    /// When the inserted entry is no longer needed, the key and value will be passed to
    /// `deleter`.
    fn insert(
        &mut self,
        key: String,
        value: T,
        charge: usize,
        deleter: Box<dyn FnMut(&String, &T)>,
    ) -> NodeValPtr<T>;

    /// If the cache has no mapping for `key`, returns `None`. Otherwise, return a handle
    /// that corresponds to the mapping. The caller must call `release(handle)` when the
    /// returned mapping is no longer needed.
    fn lookup(&self, key: &String) -> Option<NodeValPtr<T>>;

    /// Release a mapping returned by a previous `insert` or `lookup`.
    /// REQUIRES: `handle` must not have been released yet.
    /// REQUIRES: `handle` must have been returned by a method on this instance.
    fn release(&mut self, handle: NodeValPtr<T>);

    /// If the cache contains entry for the key, erase it. Note that the underlying entry
    /// will be kept around until all existing handles to it have been released.
    fn erase(&mut self, key: &String);

    /// Return a new numeric id. May be used by multiple clients who are sharing the same
    /// cache to partition the key space. Typically the client will allocate a new id at
    /// startup and prepend the id to its cache keys.
    fn new_id(&self) -> u64;

    /// Remove all cache entries that are not actively in use. Memory-constrained
    /// applications may wish to call this method to reduce memory usage.
    fn prune(&mut self);

    /// Return an estimate of the combined charges of all elements stored in the cache.
    fn total_charge(&self) -> usize;
}
