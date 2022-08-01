use std::{
    fmt,
    hash::{Hash, Hasher},
    ops::{Deref, DerefMut},
    ptr::{self, NonNull},
    sync::{atomic::*, Arc},
};

use lock::*;
use parking_lot::{RwLock, RwLockWriteGuard};

use crate::direct_io::cache::{page::*, scope::*};

#[repr(transparent)]
pub struct PagePtr<T: Scope>(NonNull<Page_<T>>);

impl<T: Scope> PagePtr<T> {
    fn new(p: *mut Page_<T>) -> Self {
        Self(NonNull::new(p).unwrap())
    }

    fn dangling() -> Self {
        Self(NonNull::dangling())
    }
}

unsafe impl<T: Scope> Send for PagePtr<T> {}

impl<T: Scope> Clone for PagePtr<T> {
    fn clone(&self) -> Self {
        Self(self.0)
    }
}

impl<T: Scope> Copy for PagePtr<T> {}

impl<T: Scope> Deref for PagePtr<T> {
    type Target = NonNull<Page_<T>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Scope> DerefMut for PagePtr<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T: Scope> fmt::Debug for PagePtr<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

// Needed for evmap.

impl<T: Scope> evmap::ShallowCopy for PagePtr<T> {
    unsafe fn shallow_copy(&self) -> std::mem::ManuallyDrop<Self> {
        std::mem::ManuallyDrop::new(*self)
    }
}

impl<T: Scope> PartialEq for PagePtr<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<T: Scope> Eq for PagePtr<T> {}

impl<T: Scope> Hash for PagePtr<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Status {
    Hot,
    Cold,
    Test,
    ProtectedTest,
    End,
}

pub type IoResult = Result<(), IoError>;

#[derive(Clone, Copy)]
pub enum IoError {
    Read,
    Write,
}

pub struct Io {
    waiters: u32,
    result: RwLock<Option<IoResult>>,
}

impl Io {
    fn new() -> Self {
        Self {
            waiters: 0,
            result: Default::default(),
        }
    }

    pub fn begin(&mut self) -> IoRef {
        debug_assert_eq!(self.waiters, 0);
        self.waiters = 1;
        IoRef(self.result.try_write().unwrap())
    }

    #[must_use]
    pub fn is_running(&self) -> bool {
        self.waiters > 0
    }

    pub fn register_waiter(&mut self) {
        debug_assert!(self.waiters > 0);
        self.waiters = self.waiters.checked_add(1).unwrap();
    }

    pub fn wait_finish(&self) -> IoResult {
        self.result.read().unwrap()
    }

    fn finish(&mut self, mut io: IoRef, result: IoResult) {
        debug_assert!(self.result.try_read().is_none());
        *io.0 = Some(result);
        self.waiters = 0;
    }
}

impl Drop for Io {
    fn drop(&mut self) {
        debug_assert!(std::thread::panicking() || self.result.try_write().is_some());
    }
}

pub struct IoRef<'a>(RwLockWriteGuard<'a, Option<IoResult>>);

pub struct Page_<T: Scope> {
    /// `None` for the `End` page.
    /// This field is immutable.
    scope: Option<Arc<ScopeShare<T>>>,

    /// This field is immutable.
    id: PageId,

    /// Keeps info about IO going on on the page.
    /// Requires Cache.inner lock to access, except for the `result` field.
    io: Io,

    // The fields below can be accessed only when guarded by the CacheInner mutex.
    pub prev: PagePtr<T>,
    pub next: PagePtr<T>,
    pub status: Status,

    // `page` content can be accessed only when guarded by `lock`.
    data: AtomicPtr<RwLock<Page>>,
    lock: Lock,
    refd: AtomicBool,
}

impl<T: Scope> Page_<T> {
    pub fn new(scope: &ScopeRef<T>, id: PageId, data: Box<RwLock<Page>>) -> Box<Self> {
        Box::new(Self {
            scope: Some(scope.share().clone()),
            id,
            io: Io::new(),
            prev: PagePtr::dangling(),
            next: PagePtr::dangling(),
            status: Status::Test,
            data: AtomicPtr::new(Box::into_raw(data)),
            lock: Lock::new_exclusive(),
            refd: AtomicBool::new(false),
        })
    }

    pub fn end() -> PagePtr<T> {
        let mut page = Box::new(Self {
            scope: None,
            id: Default::default(),
            prev: PagePtr::dangling(),
            next: PagePtr::dangling(),
            status: Status::End,
            data: Default::default(),
            io: Io::new(),
            lock: Lock::new_exclusive(),
            refd: Default::default(),
        });
        page.prev = page.ptr();
        page.next = page.ptr();
        page.into_ptr()
    }

    pub fn id(&self) -> PageId {
        self.id
    }

    pub unsafe fn from_ptr(ptr: PagePtr<T>) -> Box<Self> {
        Box::from_raw(ptr.as_ptr())
    }

    pub fn into_ptr(self: Box<Self>) -> PagePtr<T> {
        PagePtr::new(Box::into_raw(self))
    }

    pub fn ptr(&self) -> PagePtr<T> {
        PagePtr::new(self as *const _ as *mut _)
    }

    pub fn io(&self) -> &Io {
        &self.io
    }

    pub fn io_mut(&mut self) -> &mut Io {
        &mut self.io
    }

    pub fn scope(&self) -> &Arc<ScopeShare<T>> {
        self.scope.as_ref().unwrap()
    }

    /// Dump pages in the following format:
    /// ```text
    /// 12H1-
    ///     ^--- '+' if resident, '-' if non-resident
    ///    ^---- '1' is refd flag is set, '0' if the flag is not set
    /// ^^------ page id
    /// ```
    #[cfg(test)]
    pub fn dump_short(&self) {
        print!(
            "{id}{status}{refd}{res} ",
            id = self.id,
            status = match self.status {
                Status::Hot => "H",
                Status::Test => "T",
                Status::ProtectedTest => "P",
                Status::Cold => "C",
                Status::End => "P",
            },
            refd = if self.refd.load(Ordering::SeqCst) {
                "1"
            } else {
                "0"
            },
            res = if self.is_resident() { "+" } else { "-" }
        );
    }

    pub fn finish_io(&mut self, io: IoRef, result: IoResult) {
        if result.is_ok() {
            self.lock.exclusive_to_shared(self.io.waiters);
        }
        self.io.finish(io, result);
    }

    pub fn is_resident(&self) -> bool {
        !self.data.load(Ordering::SeqCst).is_null()
    }

    pub fn take_data(&self) -> Option<Box<RwLock<Page>>> {
        let r = self.data.swap(ptr::null_mut(), Ordering::SeqCst);
        if r.is_null() {
            None
        } else {
            Some(unsafe { Box::from_raw(r) })
        }
    }

    pub fn data(&self) -> &RwLock<Page> {
        debug_assert_ne!(self.lock.state(), LockState::Unlocked);
        let data = self.data.load(Ordering::SeqCst);
        debug_assert!(!data.is_null());
        unsafe { &*data }
    }

    pub fn data_ref(&self) -> PageRef<T> {
        debug_assert!(self.is_resident());
        debug_assert_eq!(self.lock.state(), LockState::Shared);
        PageRef::new(self.ptr())
    }

    pub fn set_data(&self, page: Box<RwLock<Page>>) {
        let data = Box::into_raw(page);
        if cfg!(debug_assertions) {
            let v = self.data.swap(data, Ordering::SeqCst);
            debug_assert!(v.is_null());
        } else {
            self.data.store(data, Ordering::SeqCst);
        }
    }

    pub fn lock(&self) -> &Lock {
        &self.lock
    }

    #[must_use]
    pub fn lock_shared_data(&self) -> Option<PageRef<T>> {
        if self.lock.lock_shared() {
            Some(self.data_ref())
        } else {
            None
        }
    }

    pub fn mark_refd(&self) {
        self.refd.store(true, Ordering::Relaxed);
    }

    #[must_use]
    pub fn take_refd(&self) -> bool {
        self.refd.swap(false, Ordering::Relaxed)
    }

    #[cfg(test)]
    #[must_use]
    pub fn is_refd(&self) -> bool {
        self.refd.load(Ordering::Relaxed)
    }
}

impl<T: Scope> Drop for Page_<T> {
    fn drop(&mut self) {
        if self.lock.ensure_exclusive() {
            let page = self.take_data();
            assert!(
                std::thread::panicking()
                    || page.is_none()
                    || !page.unwrap().try_read().unwrap().is_dirty(),
                "page {} was dirty during drop",
                self.id
            );
        } else if !std::thread::panicking() {
            panic!("page {} was locked during drop", self.id);
        }
    }
}
