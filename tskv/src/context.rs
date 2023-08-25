use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::Receiver;

use crate::TseriesFamilyId;

#[derive(Default, Debug)]
pub struct GlobalContext {
    /// Database file id
    file_id: AtomicU64,
    /// Sequence-No of the latest write request.
    last_seq: AtomicU64,
}

impl GlobalContext {
    pub fn new() -> Self {
        Self {
            file_id: AtomicU64::new(0),
            last_seq: AtomicU64::new(0),
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

    /// Get the current sequence number of the latest write request.
    pub fn last_seq(&self) -> u64 {
        self.last_seq.load(Ordering::Acquire)
    }

    /// Set current sequence number of the latest write request.
    pub fn set_last_seq(&self, v: u64) {
        self.last_seq.store(v, Ordering::Release);
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

#[derive(Debug)]
pub struct GlobalSequenceContext {
    min_seq: AtomicU64,
    inner: Arc<RwLock<GlobalSequenceContextInner>>,
}

impl GlobalSequenceContext {
    pub fn new(tsf_seq_map: HashMap<TseriesFamilyId, u64>) -> Self {
        let mut inner = GlobalSequenceContextInner {
            min_seq: 0_u64,
            tsf_seq_map,
        };
        inner.reset_min_seq();
        Self {
            min_seq: AtomicU64::new(inner.min_seq),
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    /// Write lock the inner, apply arguments, then update atomic min_seq.
    pub fn next_stage(
        &self,
        del_ts_family: HashSet<TseriesFamilyId>,
        ts_family_min_seq: HashMap<TseriesFamilyId, u64>,
    ) {
        let mut inner = self.inner.write();

        for (tsf_id, min_seq) in ts_family_min_seq {
            inner.tsf_seq_map.insert(tsf_id, min_seq);
        }
        if !del_ts_family.is_empty() {
            for tsf_id in del_ts_family {
                inner.tsf_seq_map.remove(&tsf_id);
            }
        }
        inner.reset_min_seq();
        self.min_seq.store(inner.min_seq, Ordering::Release);
    }

    /// Get the minimum sequence number of persisted write request
    /// of all `TseriesFamily`s in all `Database`s
    pub fn min_seq(&self) -> u64 {
        self.min_seq.load(Ordering::Acquire)
    }

    /// Get the maximum sequence number of persisted write request
    /// of all `TseriesFamily`s in all `Database`s
    pub fn max_seq(&self) -> u64 {
        self.inner.read().max_seq()
    }

    pub fn cloned(&self) -> HashMap<TseriesFamilyId, u64> {
        self.inner.read().tsf_seq_map.clone()
    }

    pub fn vnode_min_seq(&self, vnode_id: TseriesFamilyId) -> u64 {
        self.inner
            .read()
            .tsf_seq_map
            .get(&vnode_id)
            .copied()
            .unwrap_or(0)
    }
}

#[cfg(test)]
impl GlobalSequenceContext {
    pub fn empty() -> Arc<Self> {
        Arc::new(Self {
            min_seq: AtomicU64::new(0),
            inner: Arc::new(RwLock::new(GlobalSequenceContextInner {
                min_seq: 0,
                tsf_seq_map: HashMap::new(),
            })),
        })
    }

    pub fn set_min_seq(&self, min_seq: u64) {
        let mut inner = self.inner.write();
        inner.min_seq = min_seq;
        self.min_seq.store(inner.min_seq, Ordering::Release);
    }
}

#[derive(Debug)]
struct GlobalSequenceContextInner {
    /// Minimum sequence number of presisted write request
    /// among all `TseriesFamily`s in all `Database`s.ß
    min_seq: u64,
    /// Maps `TseriesFamily`-ID to it's last sequence number flushed to disk.
    tsf_seq_map: HashMap<TseriesFamilyId, u64>,
}

impl GlobalSequenceContextInner {
    /// Reset min_seq using current tsf_seq_map.
    fn reset_min_seq(&mut self) {
        if self.tsf_seq_map.is_empty() {
            self.min_seq = 0;
        } else {
            let mut min_seq = u64::MAX;
            for (_, v) in self.tsf_seq_map.iter() {
                min_seq = min_seq.min(*v);
            }
            self.min_seq = min_seq;
        }
    }

    fn max_seq(&self) -> u64 {
        let mut max_seq = 0_u64;
        for seq in self.tsf_seq_map.values() {
            max_seq = max_seq.max(*seq);
        }
        max_seq
    }
}

pub struct GlobalSequenceTask {
    pub del_ts_family: HashSet<TseriesFamilyId>,
    pub ts_family_min_seq: HashMap<TseriesFamilyId, u64>,
}

/// Start a async job to maintain GlobalSequenceCOntext.
pub(crate) fn run_global_context_job(
    runtime: Arc<Runtime>,
    mut receiver: Receiver<GlobalSequenceTask>,
    global_sequence_context: Arc<GlobalSequenceContext>,
) {
    runtime.spawn(async move {
        while let Some(t) = receiver.recv().await {
            global_sequence_context.next_stage(t.del_ts_family, t.ts_family_min_seq);
        }
    });
}

#[cfg(test)]
mod test_context {
    use std::collections::{HashMap, HashSet};

    use super::GlobalSequenceContext;

    #[test]
    fn test_global_sequence_context_new() {
        let ctx = GlobalSequenceContext::new(HashMap::from([(1, 2), (2, 3), (3, 4)]));
        assert_eq!(ctx.min_seq(), 2);

        let ctx = GlobalSequenceContext::new(HashMap::new());
        assert_eq!(ctx.min_seq(), 0);
    }

    #[test]
    fn test_global_sequence_context_next_stage() {
        let ctx = GlobalSequenceContext::empty();
        assert_eq!(ctx.min_seq(), 0);

        // Add tsfamily, no delete tsfamily
        ctx.next_stage(HashSet::new(), HashMap::from([(1, 2), (2, 10), (3, 5)]));
        assert_eq!(ctx.min_seq(), 2);

        // Delete tsfamily, no add tsfamily
        ctx.next_stage(HashSet::from([1, 2]), HashMap::new());
        assert_eq!(ctx.min_seq(), 5);

        // Delete tsfamily and add tsfamily
        ctx.next_stage(HashSet::from([3]), HashMap::from([(4, 6), (5, 8), (6, 10)]));
        assert_eq!(ctx.min_seq(), 6);
    }
}
