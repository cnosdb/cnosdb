use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default, Debug)]
pub struct GlobalContext {
    /// Database file id
    file_id: AtomicU64,
}

impl GlobalContext {
    pub fn new() -> Self {
        Self {
            file_id: AtomicU64::new(0),
        }
    }
}

impl GlobalContext {
    /// Get the current file id.
    pub fn file_id(&self) -> u64 {
        self.file_id.load(Ordering::Acquire)
    }

    /// Get the current file id and add 1.
    pub fn file_id_next(&self) -> u64 {
        self.file_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Set current file id.
    pub fn set_file_id(&self, v: u64) {
        self.file_id.store(v, Ordering::Release);
    }

    pub fn mark_file_id_used(&self, v: u64) {
        let mut old = self.file_id.load(Ordering::Acquire);
        while old <= v {
            match self
                .file_id
                .compare_exchange(old, v + 1, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => break,
                Err(x) => old = x,
            }
        }
    }
}
