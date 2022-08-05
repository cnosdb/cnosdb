mod page;
mod scope;

use std::{
    cmp,
    io::{Error, ErrorKind, Result},
    sync::{atomic::*, Arc},
};

use page::{internal::*, lock::LockState};
pub use page::{Page, PageId, PageReadGuard, PageRef, PageWriteGuard};
use parking_lot::*;
pub use scope::Scope;
use scope::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Options {
    pub max_resident: usize,
    pub max_non_resident: usize,
    pub page_len: usize,
    pub page_align: usize,
}

pub fn new<T: Scope>(options: Options) -> CacheHandle<T> {
    CacheHandle(Arc::new(Cache::new(options)))
}

pub struct CacheHandle<T: Scope>(Arc<Cache<T>>);

impl<T: Scope> CacheHandle<T> {
    pub fn max_resident(&self) -> usize {
        self.0.options.max_resident
    }

    pub fn max_non_resident(&self) -> usize {
        self.0.options.max_non_resident
    }

    pub fn page_len(&self) -> usize {
        self.0.options.page_len
    }

    pub fn hit_count(&self) -> u64 {
        self.0.hit_count()
    }

    pub fn miss_count(&self) -> u64 {
        self.0.miss_count()
    }

    pub fn read_count(&self) -> u64 {
        self.0.read_count()
    }

    pub fn write_count(&self) -> u64 {
        self.0.write_count()
    }

    pub fn new_scope(&self, scope: T) -> ScopeHandle<T> {
        ScopeHandle {
            cache: self.clone(),
            scope: ScopeRef::new(scope, self.0.options.page_len),
        }
    }
}

impl<T: Scope> Clone for CacheHandle<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

pub struct ScopeHandle<T: Scope> {
    cache: CacheHandle<T>,
    scope: ScopeRef<T>,
}

impl<T: Scope> ScopeHandle<T> {
    pub fn downgrade(&self) -> WeakScopeHandle<T> {
        WeakScopeHandle {
            cache: self.cache.clone(),
            scope: self.scope.downgrade(),
        }
    }

    pub fn cache(&self) -> &CacheHandle<T> {
        &self.cache
    }

    pub fn get(&self) -> &T {
        self.scope.get()
    }

    pub fn page(&self, id: PageId) -> Result<PageRef<T>> {
        self.cache.0.page(&self.scope, id)
    }

    /// Writes all dirty pages clearing the dirty flag.
    /// Pages that are write-locked will be skipped.
    /// Returns the number of pages written.
    pub fn flush(&self) -> Result<usize> {
        self.cache.0.flush(&self.scope)
    }

    /// Clears the dirty flag of all pages.
    /// Pages that are write-locked will be skipped.
    pub fn discard(&self) {
        self.cache.0.discard(&self.scope)
    }
}

impl<T: Scope> Clone for ScopeHandle<T> {
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
            scope: self.scope.clone(),
        }
    }
}

pub struct WeakScopeHandle<T: Scope> {
    cache: CacheHandle<T>, // TODO Make this weak too.
    scope: WeakScopeRef<T>,
}

impl<T: Scope> WeakScopeHandle<T> {
    pub fn upgrade(&self) -> Option<ScopeHandle<T>> {
        self.scope.upgrade().map(|scope| ScopeHandle {
            cache: self.cache.clone(),
            scope,
        })
    }
}

impl<T: Scope> Clone for WeakScopeHandle<T> {
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
            scope: self.scope.clone(),
        }
    }
}

struct Stop<T>(Option<T>);

impl<T: Eq> Stop<T> {
    pub fn new() -> Self {
        Self(None)
    }

    pub fn track(&mut self, v: T) {
        if self.0.is_none() {
            self.0 = Some(v);
        }
    }

    #[must_use]
    pub fn check(&self, v: T) -> bool {
        self.0 != Some(v)
    }
}

#[derive(Clone, Copy, Debug)]
enum PageCountKind {
    Hot,
    Cold { resident: bool },
}

#[derive(Clone, Copy, Debug)]
enum PageCountUpdate {
    Inc,
    Dec,
}

struct DonorPage<T: Scope> {
    id: PageId,
    scope: Arc<ScopeShare<T>>,
}

struct PageData<T: Scope> {
    donor_page: Option<DonorPage<T>>,
    data: Box<RwLock<Page>>,
}

struct CacheInner<T: Scope> {
    options: Options,
    pages: PagePtr<T>,
    hot_page_count: usize,
    resident_cold_page_count: usize,
    non_resident_page_count: usize,
    io_page_count: usize,
    hand_hot: PagePtr<T>,
    // HAND_cold is used to search for a resident cold page for replacement.
    hand_cold: PagePtr<T>,
    hand_cold_cost: usize,
    max_hand_cold_cost: usize,
    hand_test: PagePtr<T>,
}

impl<T: Scope> CacheInner<T> {
    fn new(options: Options) -> Self {
        assert!(options.max_resident > 0);

        let pages = Page_::end();

        Self {
            options,
            pages,
            hot_page_count: 0,
            resident_cold_page_count: 0,
            non_resident_page_count: 0,
            io_page_count: 0,
            hand_hot: pages,
            hand_cold: pages,
            hand_cold_cost: 0,
            max_hand_cold_cost: 0,
            hand_test: pages,
        }
    }

    #[cfg(test)]
    fn dump_short(&self) {
        let end = self.end();
        let mut page_ptr = self.pages;
        while page_ptr != end {
            let page = unsafe { page_ptr.as_ref() };
            page.dump_short();
            page_ptr = page.next;
        }
        println!();
    }

    #[cfg(test)]
    fn dump(&self, pages: bool) {
        println!("hot_page_count: {}", self.hot_page_count);
        println!(
            "resident_cold_page_count: {}",
            self.resident_cold_page_count
        );
        println!("non_resident_page_count: {}", self.non_resident_page_count);
        println!("hand_hot: {:?}", unsafe { self.hand_hot.as_ref() }.id());
        println!("hand_cold: {:?}", unsafe { self.hand_cold.as_ref() }.id());
        println!("hand_cold_cost: {}", self.hand_cold_cost);
        println!("max_hand_cold_cost: {}", self.max_hand_cold_cost);
        println!("hand_test: {:?}", unsafe { self.hand_test.as_ref() }.id());

        if pages {
            let end = self.end();
            let mut page = self.pages;
            let mut i = 0;
            while page != end {
                println!("Page #{} ({:?})", i, page);
                self.dump_page(page);
                page = unsafe { page.as_ref() }.next;
                i += 1;
            }
        }
    }

    #[cfg(test)]
    fn dump_page(&self, page: PagePtr<T>) {
        let page = unsafe { page.as_ref() };
        println!("  id: {:?}", page.id());
        println!("  next: {:?}", page.next);
        println!("  prev: {:?}", page.prev);
        println!("  resident: {:?}", page.is_resident());
        println!("  status: {:?}", page.status);
        println!("  lock: {:?}", page.lock().state());
        println!("  refd: {}", page.is_refd());
    }

    fn run_hand_test(&mut self) {
        let stop = self.hand_test;
        loop {
            let mut page = self.hand_test;
            let page = unsafe { page.as_mut() };
            // HAND_Test moves forward and stops at the next cold page.
            self.hand_test = page.next;
            match page.status {
                Status::Cold | Status::Test => {
                    page.status = Status::Cold;
                    if !page.is_resident() {
                        let page = page.ptr();
                        self.remove_page(page, PageCountKind::Cold { resident: false });
                        break;
                    }
                }
                _ => {}
            }
            if self.hand_test == stop {
                break;
            }
        }
    }

    fn run_hand_cold(&mut self) -> PageData<T> {
        if self.resident_cold_page_count == 0 {
            self.run_hand_hot(true);
        }
        debug_assert!(self.resident_cold_page_count > 0);

        // The hand will keep moving until it encounters a cold page eligible for replacement,
        // and stops at the next resident cold page.
        loop {
            self.hand_cold_cost += 1;

            let mut page = self.hand_cold;
            let page = unsafe { page.as_mut() };
            self.hand_cold = page.next;

            match page.status {
                Status::Cold | Status::Test if page.is_resident() => {
                    if page.lock().is_locked() || page.take_refd() || !page.lock().lock_exclusive()
                    {
                        // If reference bit is set.

                        if page.status == Status::Test {
                            // If page is in its test period, we turn the cold page into a hot page,
                            // and ask HAND_hot for its actions.
                            page.status = Status::Hot;
                            self.update_page_count(
                                PageCountKind::Cold { resident: true },
                                PageCountUpdate::Dec,
                            );
                            self.update_page_count(PageCountKind::Hot, PageCountUpdate::Inc);
                            self.run_hand_hot(true);
                        } else {
                            // If reference bit is set but it is not in its test period, there are
                            // no status change as well as HAND_hot
                            // actions.
                        }

                        // In both of the cases, reference bit is reset, and we move the page to the
                        // list head.

                        self.remove_page_from_list(page);
                        self.add_page_to_list(page);
                    } else {
                        // If the reference bit of the cold page currently pointed to by HAND_cold
                        // is unset, we replace the cold page for a free
                        // space.
                        let page_data = page.take_data().unwrap();

                        let id = page.id();
                        let scope = page.scope().clone();

                        if page.status == Status::Test {
                            // The replaced cold page will remain in the list as a non-resident cold
                            // page until it runs out of its test
                            // period, if it is in its test period.

                            // We keep track of the number of non-resident cold pages.
                            // Once the number exceeds _m_, the memory size in the number of pages,
                            // we terminate the test period of the cold
                            // page pointed to by HAND_test.
                            self.update_page_count(
                                PageCountKind::Cold { resident: true },
                                PageCountUpdate::Dec,
                            );
                            self.update_page_count(
                                PageCountKind::Cold { resident: false },
                                PageCountUpdate::Inc,
                            );

                            debug_assert!(
                                self.non_resident_page_count <= self.options.max_non_resident + 1
                            );
                            if self.non_resident_page_count > self.options.max_non_resident {
                                self.run_hand_test();
                                debug_assert!(
                                    self.non_resident_page_count == self.options.max_non_resident
                                );
                            }
                        } else {
                            // If not (in the test period), we move it out of the clock.
                            self.remove_page(page.ptr(), PageCountKind::Cold { resident: true });
                        }

                        break PageData {
                            donor_page: Some(DonorPage { id, scope }),
                            data: page_data,
                        };
                    }
                }
                _ => {}
            }
        }
    }

    fn acquire_page_data(&mut self) -> Option<PageData<T>> {
        debug_assert!(self.resident_page_count() <= self.options.max_resident);
        debug_assert!(self.io_page_count <= self.resident_page_count());
        if self.resident_page_count() < self.options.max_resident {
            let page = Page::new(self.options.page_len, self.options.page_align);
            return Some(PageData {
                donor_page: None,
                data: Box::new(RwLock::new(page)),
            });
        } else if self.resident_page_count() == self.io_page_count {
            // All potential donor pages are locked in IO.
            // The operation must be retried.
            return None;
        }
        let pd = self.run_hand_cold();
        debug_assert!(self.resident_page_count() < self.options.max_resident);
        self.max_hand_cold_cost = cmp::max(self.max_hand_cold_cost, self.hand_cold_cost);
        Some(pd)
    }

    fn resident_page_count(&self) -> usize {
        self.hot_page_count + self.resident_cold_page_count
    }

    /// If `cold_page_required` is `true` this method ensures that there's at least one resident
    /// cold page available on return.
    fn run_hand_hot(&mut self, cold_page_required: bool) {
        let mut stop = Stop::new();
        while stop.check(self.hand_hot) {
            let mut page = self.hand_hot;
            let page = unsafe { page.as_mut() };
            self.hand_hot = page.next;

            match page.status {
                Status::Hot => {
                    if self.handle_hot(page) {
                        return;
                    }
                }
                Status::Cold | Status::Test => {
                    // Whenever the hand encounters a cold page, it will terminate the page’s test
                    // period.
                    page.status = Status::Cold;

                    // The hand will also remove the cold page from the clock if it is non-resident
                    // (the most probable case).
                    // TODO maybe not remove here and remove only when needed
                    // (non_resident_page_count > options.max_non_resident)
                    if !page.is_resident() {
                        let page = page.ptr();
                        self.remove_page(page, PageCountKind::Cold { resident: false });
                    } else {
                        stop.track(page.ptr())
                    }
                }
                Status::ProtectedTest | Status::End => stop.track(page.ptr()),
            }
        }
        // We did a cycle but could turn hot page into cold.
        // Cold hand relies on always being able to return a buffer when no new buffers can be
        // created. If the latter is the case we need to ensure there's at least one cold
        // resident page available.
        if cold_page_required && self.resident_cold_page_count == 0 && self.hot_page_count > 0 {
            loop {
                let mut page = self.hand_hot;
                let page = unsafe { page.as_mut() };
                self.hand_hot = page.next;
                if page.status == Status::Hot && self.handle_hot(page) {
                    break;
                }
            }
        }
        debug_assert!(!cold_page_required || self.resident_cold_page_count > 0);
    }

    #[must_use]
    fn handle_hot(&mut self, page: &mut Page_<T>) -> bool {
        debug_assert_eq!(page.status, Status::Hot);
        if page.lock().is_locked() || page.take_refd() {
            // If the ref bit is set, which indicates the page has been re-accessed, we spare
            // this page, reset its reference bit and keep it as a hot page.
            false
        } else {
            // If the reference bit of the hot page pointed to by HAND_hot is unset, we can simply
            // change its status and then move the hand forward.
            debug_assert!(page.is_resident());
            page.status = Status::Test;
            self.update_page_count(PageCountKind::Hot, PageCountUpdate::Dec);
            self.update_page_count(PageCountKind::Cold { resident: true }, PageCountUpdate::Inc);
            true
        }
    }

    fn end(&self) -> PagePtr<T> {
        unsafe { self.pages.as_ref() }.prev
    }

    #[inline]
    fn update_page_count(&mut self, kind: PageCountKind, update: PageCountUpdate) {
        match update {
            PageCountUpdate::Inc => match kind {
                PageCountKind::Hot => self.hot_page_count += 1,
                PageCountKind::Cold { resident: false } => self.non_resident_page_count += 1,
                PageCountKind::Cold { resident: true } => self.resident_cold_page_count += 1,
            },
            PageCountUpdate::Dec => match kind {
                PageCountKind::Hot => self.hot_page_count -= 1,
                PageCountKind::Cold { resident: false } => self.non_resident_page_count -= 1,
                PageCountKind::Cold { resident: true } => self.resident_cold_page_count -= 1,
            },
        }
    }

    #[inline]
    fn transit_page_count(&mut self, from: PageCountKind, to: PageCountKind) {
        self.update_page_count(from, PageCountUpdate::Dec);
        self.update_page_count(to, PageCountUpdate::Inc);
    }

    fn add_page_to_list(&mut self, page: &mut Page_<T>) {
        let mut end = self.end();
        let end = unsafe { end.as_mut() };
        debug_assert_ne!(page.ptr(), end.ptr());
        if self.pages == end.ptr() {
            self.pages = page.ptr();
            end.next = page.ptr();
        } else {
            unsafe { end.prev.as_mut() }.next = page.ptr();
        }
        page.prev = end.prev;
        end.prev = page.ptr();
        page.next = end.ptr();
    }

    fn add_page(&mut self, mut page: Box<Page_<T>>, count_kind: PageCountKind) -> PagePtr<T> {
        self.add_page_to_list(&mut page);
        self.update_page_count(count_kind, PageCountUpdate::Inc);

        let id = page.id();
        let ptr = page.ptr();

        {
            let mut page_map = page.scope().page_map_w();
            page_map.update(id, page.ptr());
            page_map.refresh();
        }

        std::mem::forget(page);

        ptr
    }

    fn remove_page_from_list(&mut self, page: &mut Page_<T>) {
        debug_assert_ne!(page.ptr(), self.end());
        let next = page.next;
        if self.pages == page.ptr() {
            self.pages = next;
        }
        if self.hand_hot == page.ptr() {
            self.hand_hot = next;
        }
        if self.hand_cold == page.ptr() {
            self.hand_cold = next;
        }
        if self.hand_test == page.ptr() {
            self.hand_test = next;
        }
        unsafe { page.prev.as_mut() }.next = next;
        unsafe { page.next.as_mut() }.prev = page.prev;
    }

    fn remove_page(&mut self, page: PagePtr<T>, count_kind: PageCountKind) {
        let mut page = unsafe { Page_::from_ptr(page) };

        self.remove_page_from_list(&mut page);
        self.update_page_count(count_kind, PageCountUpdate::Dec);

        let mut page_map = page.scope().page_map_w();
        page_map.empty(page.id());
        page_map.refresh();
    }

    fn replace_page_data(&mut self, mut page: PagePtr<T>, data: Box<RwLock<Page>>) -> PagePtr<T> {
        let page = unsafe { page.as_mut() };

        debug_assert_eq!(page.lock().state(), LockState::Exclusive);

        // If the cold page is in the list (must be in its test period), the faulted page turns
        // into a hot page and is placed at the head of the list.
        debug_assert_eq!(page.status, Status::ProtectedTest);
        self.remove_page_from_list(page);
        self.update_page_count(
            PageCountKind::Cold { resident: false },
            PageCountUpdate::Dec,
        );

        page.status = Status::Hot;
        page.set_data(data);

        // We run HAND_hot to turn a hot page with a large recency into a cold page.
        self.run_hand_hot(false);

        self.add_page_to_list(page);
        self.update_page_count(PageCountKind::Hot, PageCountUpdate::Inc);

        page.ptr()
    }

    fn add_page_data(
        &mut self,
        scope: &ScopeRef<T>,
        id: PageId,
        data: Box<RwLock<Page>>,
    ) -> PagePtr<T> {
        // If the faulted cold page is not in the list, its reuse distance is highly likely to
        // be larger than the recency of hot pages. So the page is still categorized a
        // cold page and is placed at the list head. The page also initiates its test period.

        // TODO reuse pages.
        let page = Page_::new(scope, id, data);

        self.add_page(page, PageCountKind::Cold { resident: true })
    }
}

unsafe impl<T: Scope> Send for CacheInner<T> {}

impl<T: Scope> Drop for CacheInner<T> {
    fn drop(&mut self) {
        let end = self.end();
        let mut page = self.pages;
        while page != end {
            page = unsafe { Page_::from_ptr(page) }.next;
        }
        unsafe {
            Page_::from_ptr(end);
        }
    }
}

struct Cache<T: Scope> {
    inner: Mutex<CacheInner<T>>,
    options: Options,
    hit_count: AtomicU64,
    miss_count: AtomicU64,
    read_count: AtomicU64,
    write_count: AtomicU64,
}

impl<T: Scope> Cache<T> {
    fn new(options: Options) -> Self {
        Self {
            inner: Mutex::new(CacheInner::new(options.clone())),
            options,
            hit_count: 0.into(),
            miss_count: 0.into(),
            read_count: 0.into(),
            write_count: 0.into(),
        }
    }

    pub fn hit_count(&self) -> u64 {
        self.hit_count.load(Ordering::Relaxed)
    }

    pub fn miss_count(&self) -> u64 {
        self.miss_count.load(Ordering::Relaxed)
    }

    pub fn read_count(&self) -> u64 {
        self.read_count.load(Ordering::Relaxed)
    }

    pub fn write_count(&self) -> u64 {
        self.write_count.load(Ordering::Relaxed)
    }

    pub fn page(self: &Arc<Self>, scope: &ScopeRef<T>, id: PageId) -> Result<PageRef<T>> {
        if let Some(page) = scope.page_map_r().get(&id) {
            let mut page = *page.iter().next().unwrap();
            let page = unsafe { page.as_mut() };
            if let Some(data) = page.lock_shared_data() {
                page.mark_refd();
                self.hit_count.fetch_add(1, Ordering::Relaxed);
                return Ok(data);
            }
        }

        self.miss_count.fetch_add(1, Ordering::Relaxed);

        // When there is a page fault, the faulted page must be a cold page.

        let (donor_page, mut page, page_io) = loop {
            let mut inner = self.inner.lock();

            let page = {
                scope
                    .page_map_r()
                    .get(&id)
                    .map(|page| *page.iter().next().unwrap())
            };
            if let Some(mut page) = page {
                let page = unsafe { page.as_mut() };
                if page.io().is_running() {
                    // Other task is doing IO on the page.
                    // Register itself as waiter.
                    page.io_mut().register_waiter();
                    break (None, page.ptr(), None);
                } else if let Some(page) = page.lock_shared_data() {
                    // Other task has already done IO on the page.
                    // Still not counting this as a hit.
                    return Ok(page);
                }

                debug_assert!(!page.is_resident());

                // Protect the page from being removed or altered otherwise.
                debug_assert!(
                    matches!(page.status, Status::Test | Status::ProtectedTest),
                    "{:?}",
                    page.status
                );
                page.status = Status::ProtectedTest;
            }

            // We first run HAND_cold for a free space.

            //let (data_page_id, data)
            let pd = if let Some(v) = inner.acquire_page_data() {
                v
            } else {
                continue;
            };
            let page = if let Some(page) = page {
                inner.replace_page_data(page, pd.data)
            } else {
                inner.add_page_data(scope, id, pd.data)
            };

            scope.page_map_w().refresh();

            // Must be done inside the current lock scope so other threads won't lock it before
            // we do.
            let page_io = unsafe { &mut *page.as_ptr() }.io_mut().begin();

            inner.hand_cold_cost = 0;
            inner.io_page_count += 1;

            break (pd.donor_page, page, Some(page_io));
        };

        // Here it's guaranteed the page doesn't get dropped because PageIo.waiters is the deferred
        // shared lock count of the page.

        {
            if let Some(page_io) = page_io {
                let (write_result, read_result) = {
                    let data = unsafe { page.as_ref() }.data();
                    let mut data = data.try_write().unwrap();
                    let write_result = if let Some(donor_page) = donor_page {
                        self.write(&donor_page.scope, donor_page.id, &mut data)
                    } else {
                        Ok(false)
                    };
                    let read_result = if write_result.is_ok() {
                        Some(self.read(scope.share(), id, &mut data))
                    } else {
                        None
                    };
                    (write_result, read_result)
                };

                let (page_io_result, result) = match (write_result, read_result) {
                    (Ok(_), Some(r)) => {
                        let pr = if r.is_ok() {
                            Ok(())
                        } else {
                            Err(IoError::Read)
                        };
                        (pr, r)
                    }
                    (Err(e), None) => (Err(IoError::Write), Err(e)),
                    _ => unreachable!(),
                };

                {
                    let mut inner = self.inner.lock();
                    unsafe { page.as_mut() }.finish_io(page_io, page_io_result);
                    inner.io_page_count -= 1;

                    if result.is_err() {
                        // TODO Recycle the page data in case of error.
                        let page = unsafe { page.as_mut() };
                        page.take_data().unwrap();
                        let from = match page.status {
                            Status::Hot => {
                                page.status = Status::Cold;
                                PageCountKind::Hot
                            }
                            Status::Test => PageCountKind::Cold { resident: true },
                            Status::Cold | Status::ProtectedTest | Status::End => unreachable!(),
                        };
                        inner.transit_page_count(from, PageCountKind::Cold { resident: false });

                        // The page is left locked exclusively.

                        return result.map(|_| unreachable!());
                    }
                }
            } else {
                match unsafe { page.as_ref() }.io().wait_finish() {
                    Ok(_) => {}
                    Err(IoError::Read) => {
                        return Err(Error::new(
                            ErrorKind::Other,
                            format!("error reading page {}", id),
                        ));
                    }
                    Err(IoError::Write) => {
                        return Err(Error::new(
                            ErrorKind::Other,
                            format!("error writing page {}", donor_page.as_ref().unwrap().id),
                        ));
                    }
                }
            }
        }

        Ok(unsafe { page.as_ref() }.data_ref())
    }

    pub fn flush(&self, scope: &ScopeRef<T>) -> Result<usize> {
        let mut count = 0;
        for (id, page) in self.find_dirty_pages(scope) {
            if let Some(mut page) = page.try_write() {
                if self.write(scope.share(), id, &mut page)? {
                    count += 1;
                }
            }
        }
        Ok(count)
    }

    pub fn discard(&self, scope: &ScopeRef<T>) {
        for (_, page) in self.find_dirty_pages(scope) {
            if let Some(mut page) = page.try_write() {
                page.set_dirty(false);
            }
        }
    }

    fn find_dirty_pages(&self, scope: &ScopeRef<T>) -> Vec<(PageId, PageRef<T>)> {
        scope
            .page_map_r()
            .read()
            .unwrap()
            .iter()
            .filter_map(|(_, page)| {
                let mut page = *page.iter().next().unwrap();
                let page = unsafe { page.as_mut() };
                page.lock_shared_data()
                    // If a page is write-locked this situation is interpreted as if the page was
                    // clean here but got dirty later, before this method returned.
                    // This interpretation allows skipping write-locked pages.
                    .filter(|data| data.try_read().map(|data| data.is_dirty()).unwrap_or(false))
                    .map(|data| (page.id(), data))
            })
            .collect::<Vec<_>>()
    }

    fn read(&self, scope: &ScopeShare<T>, id: PageId, page: &mut Page) -> Result<()> {
        self.read_count.fetch_add(1, Ordering::Relaxed);
        scope.scope().read(id, page)?;
        page.set_dirty(false);
        Ok(())
    }

    fn write(&self, scope: &ScopeShare<T>, id: PageId, page: &mut Page) -> Result<bool> {
        Ok(if page.is_dirty() {
            self.read_count.fetch_add(1, Ordering::Relaxed);
            scope.scope().write(id, page)?;
            page.set_dirty(false);
            true
        } else {
            false
        })
    }

    #[cfg(test)]
    fn dump(&self, pages: bool) {
        println!("hit_count: {}", self.hit_count.load(Ordering::Relaxed));
        println!("miss_count: {}", self.miss_count.load(Ordering::Relaxed));
        println!("read_count: {}", self.read_count.load(Ordering::Relaxed));
        println!("write_count: {}", self.write_count.load(Ordering::Relaxed));
        self.inner.lock().dump(pages);
    }

    #[cfg(test)]
    fn dump_short(&self) {
        self.inner.lock().dump_short();
    }
}
