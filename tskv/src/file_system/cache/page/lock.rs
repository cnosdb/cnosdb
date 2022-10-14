use std::sync::atomic::*;

type StateInt = usize;
const UNLOCKED: StateInt = 0;
const MIN_SHARED: StateInt = 1;
const MAX_SHARED: StateInt = MIN_EXCLUSIVE - 1;
const MIN_EXCLUSIVE: StateInt = MAX_EXCLUSIVE / 2 + 1;
const MAX_EXCLUSIVE: StateInt = StateInt::MAX;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LockState {
    Unlocked,
    Shared,
    Exclusive,
}

impl From<StateInt> for LockState {
    fn from(v: StateInt) -> Self {
        match v {
            UNLOCKED => LockState::Unlocked,
            MIN_SHARED..=MAX_SHARED => LockState::Shared,
            _ => LockState::Exclusive,
        }
    }
}

pub struct Lock(AtomicUsize);

impl Lock {
    pub fn new_exclusive() -> Self {
        Self::new(MIN_EXCLUSIVE)
    }

    fn new(state: StateInt) -> Self {
        Self(state.into())
    }

    #[must_use]
    pub fn is_locked(&self) -> bool {
        self.state() != LockState::Unlocked
    }

    #[must_use]
    pub fn lock_shared(&self) -> bool {
        let state = self.inc();
        if state <= MAX_SHARED {
            assert!(state < MAX_SHARED);
            true
        } else {
            assert!(state < MAX_EXCLUSIVE);
            if state >= MIN_EXCLUSIVE / 2 + MAX_EXCLUSIVE / 2 {
                self.cas(state, MIN_EXCLUSIVE);
            }
            false
        }
    }

    pub fn unlock_shared(&self) {
        let v = self.dec();
        debug_assert!(v > UNLOCKED);
        debug_assert!(v <= MAX_SHARED);
    }

    #[must_use]
    pub fn state(&self) -> LockState {
        self.load().into()
    }

    #[must_use]
    pub fn lock_exclusive(&self) -> bool {
        self.lock_exclusive0() == UNLOCKED
    }

    #[must_use]
    pub fn ensure_exclusive(&self) -> bool {
        let state = LockState::from(self.lock_exclusive0());
        matches!(state, LockState::Unlocked | LockState::Exclusive)
    }

    pub fn exclusive_to_shared(&self, count: u32) {
        let count = count as StateInt;
        debug_assert!(count > 0);
        debug_assert!(count <= MAX_SHARED);
        if cfg!(debug_assertions) {
            debug_assert!(LockState::from(self.swap(count)) == LockState::Exclusive);
        } else {
            self.store(count);
        }
    }

    #[must_use]
    fn load(&self) -> StateInt {
        self.0.load(Ordering::SeqCst)
    }

    fn store(&self, v: StateInt) {
        self.0.store(v, Ordering::SeqCst);
    }

    fn cas(&self, old: StateInt, new: StateInt) -> StateInt {
        match self
            .0
            .compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst)
        {
            Ok(old) => old,
            Err(old) => old,
        }
    }

    fn swap(&self, v: StateInt) -> StateInt {
        self.0.swap(v, Ordering::SeqCst)
    }

    fn inc(&self) -> StateInt {
        self.0.fetch_add(1, Ordering::SeqCst)
    }

    fn dec(&self) -> StateInt {
        self.0.fetch_sub(1, Ordering::SeqCst)
    }

    #[must_use]
    fn lock_exclusive0(&self) -> StateInt {
        self.cas(UNLOCKED, MIN_EXCLUSIVE)
    }
}
