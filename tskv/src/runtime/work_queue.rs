use core_affinity::CoreId;
use runtime::Runtime;
use std::rc::Rc;
use std::thread_local;
use tokio::sync::oneshot;

pub struct WorkerQueue {
    runtime: Runtime,
}

impl WorkerQueue {
    pub fn new(core_ids: &[CoreId]) -> Self {
        let runtime = Runtime::new(core_ids);

        Self { runtime }
    }

    pub async fn append(&self, id: Id, bytes: Bytes) {
        let (tx, rx) = oneshot::channel();

        let id = shard_id(id);
        self.runtime.spawn(shard_id(id), async move {
            thread_local! (static SHARD:WorkerQueue = WorkerQueue::new() );

            SHARD.with(|shard| {
                shard.append(id, bytes);
            });

            tx.send(()).unwrap();
        });

        rx.await.unwrap();
    }

    pub async fn get(&self, id: Id, size: usize) {
        let (tx, rx) = oneshot::channel();

        let id = shard_id(id);
        self.runtime.spawn(shard_id(id), async move {
            thread_local! (static SHARD:WorkerQueue = WorkerQueue::new() );

            let result = SHARD.with(|shard| {
                shard.get(id, size);
            });

            tx.send(result).unwrap();
        });

        let _ = rx.await.unwrap();
    }
}

#[inline]
fn shard_id(id: Id) -> usize {
    1
}

