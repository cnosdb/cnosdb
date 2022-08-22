use std::sync::{
    atomic::{AtomicU32, AtomicU64, Ordering},
    Arc,
};

#[derive(Default, Debug)]
pub struct GlobalContext {
    file_id: AtomicU64,
    mem_seq: AtomicU64,
    last_seq: AtomicU64,
    max_tsf_id: AtomicU32,
}

impl GlobalContext {
    pub fn new() -> Self {
        Self {
            file_id: AtomicU64::new(0),
            mem_seq: AtomicU64::new(0),
            last_seq: AtomicU64::new(0),
            max_tsf_id: AtomicU32::new(0),
        }
    }
}

impl GlobalContext {
    pub fn file_id(&self) -> u64 {
        self.file_id.load(Ordering::Acquire)
    }

    pub fn file_id_next(&self) -> u64 {
        self.file_id.fetch_add(1, Ordering::SeqCst)
    }

    pub fn mem_seq_next(&self) -> u64 {
        self.mem_seq.fetch_add(1, Ordering::SeqCst)
    }

    pub fn last_seq(&self) -> u64 {
        self.last_seq.load(Ordering::Acquire)
    }

    pub fn fetch_add_log_seq(&self, n: u64) -> u64 {
        self.file_id.fetch_add(n, Ordering::SeqCst)
    }

    pub fn set_last_seq(&self, v: u64) {
        self.last_seq.store(v, Ordering::Release);
    }
    pub fn set_file_id(&self, v: u64) {
        self.file_id.store(v, Ordering::Release);
    }

    pub fn set_max_tsf_idy(&self, v: u32) {
        self.max_tsf_id.store(v, Ordering::Release);
    }

    pub fn max_tsf_id(&self) -> u32 {
        self.max_tsf_id.load(Ordering::Acquire)
    }

    pub fn next_tsf_id(&self) {
        self.max_tsf_id.fetch_add(1, Ordering::SeqCst);
    }

    pub fn mark_log_number_used(&self, v: u64) {
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
