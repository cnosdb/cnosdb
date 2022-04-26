use std::{
    io::Result,
    sync::{Arc, Weak},
};

use parking_lot::*;

use super::page::{internal::PagePtr, PageId};

pub trait Scope {
    fn read(&self, id: PageId, buf: &mut [u8]) -> Result<()>;

    fn write(&self, id: PageId, buf: &[u8]) -> Result<()>;
}

pub type PageMapR<T> = evmap::ReadHandle<PageId, PagePtr<T>>;
pub type PageMapW<T> = evmap::WriteHandle<PageId, PagePtr<T>>;

pub struct ScopeShare<T: Scope> {
    // Mutex here is for proving safety. Actual sync is done by the Cache.inner mutex.
    page_map_w: Mutex<PageMapW<T>>,
    scope: Box<T>,
    page_len: usize,
}

impl<T: Scope> ScopeShare<T> {
    pub fn page_map_w(&self) -> MutexGuard<PageMapW<T>> {
        self.page_map_w.try_lock().unwrap()
    }

    pub fn scope(&self) -> &T {
        &self.scope
    }

    pub fn page_len(&self) -> usize {
        self.page_len
    }
}

struct ScopeRefInner<T: Scope> {
    page_map_r: PageMapR<T>,
    share: Arc<ScopeShare<T>>,
}

impl<T: Scope> Clone for ScopeRefInner<T> {
    fn clone(&self) -> Self {
        Self { page_map_r: self.page_map_r.clone(), share: self.share.clone() }
    }
}

pub struct ScopeRef<T: Scope>(ScopeRefInner<T>);

impl<T: Scope> ScopeRef<T> {
    pub fn new(scope: T, page_len: usize) -> Self {
        let (page_map_r, page_map_w) = evmap::new();
        Self(ScopeRefInner { page_map_r,
                             share: Arc::new(ScopeShare { page_map_w: Mutex::new(page_map_w),
                                                          scope: Box::new(scope),
                                                          page_len }) })
    }

    pub fn page_map_r(&self) -> &PageMapR<T> {
        &self.0.page_map_r
    }

    pub fn page_map_w(&self) -> MutexGuard<PageMapW<T>> {
        self.0.share.page_map_w()
    }

    pub fn share(&self) -> &Arc<ScopeShare<T>> {
        &self.0.share
    }

    pub fn get(&self) -> &T {
        &self.0.share.scope
    }

    pub fn downgrade(&self) -> WeakScopeRef<T> {
        WeakScopeRef(WeakScopeRefInner { page_map_r: Arc::new(Mutex::new(self.0
                                                                             .page_map_r
                                                                             .clone())),
                                         share: Arc::downgrade(self.share()) })
    }
}

impl<T: Scope> Clone for ScopeRef<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

struct WeakScopeRefInner<T: Scope> {
    page_map_r: Arc<Mutex<PageMapR<T>>>,
    share: Weak<ScopeShare<T>>,
}

impl<T: Scope> Clone for WeakScopeRefInner<T> {
    fn clone(&self) -> Self {
        Self { page_map_r: self.page_map_r.clone(), share: self.share.clone() }
    }
}

pub struct WeakScopeRef<T: Scope>(WeakScopeRefInner<T>);

impl<T: Scope> WeakScopeRef<T> {
    pub fn upgrade(&self) -> Option<ScopeRef<T>> {
        self.0.share.upgrade().map(|share| {
                                  ScopeRef(ScopeRefInner { page_map_r: self.0
                                                                           .page_map_r
                                                                           .lock()
                                                                           .clone(),
                                                           share })
                              })
    }
}

impl<T: Scope> Clone for WeakScopeRef<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
