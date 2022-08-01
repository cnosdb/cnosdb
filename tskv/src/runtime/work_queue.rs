use futures::Future;
use runtime::Runtime;
use trace::error;

use crate::{error::Result, runtime::runtime};

pub struct WorkerQueue {
    pub work_queue: Runtime,
    pub core_num: usize,
}

impl WorkerQueue {
    pub fn new(core_num: usize) -> Self {
        let core_ids = match core_affinity::get_core_ids() {
            None => {
                error!("failed to get core_ids, return none");
                vec![]
            }
            Some(v) => v,
        };
        let queue = Runtime::new(&core_ids[0..core_num]);
        Self {
            work_queue: queue,
            core_num,
        }
    }

    pub fn add_task<F>(&self, index: usize, task: F) -> Result<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.work_queue.add_task(index, task)
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::oneshot;

    use super::WorkerQueue;
    #[test]
    fn workqueue_test() {
        let q = WorkerQueue::new(2);
        let num = 3;
        let (tx, rx) = oneshot::channel();
        let _ = q.work_queue.add_task(num % q.core_num, async move {
            let mut sum: u64 = 0;
            let mut i = 1000000;
            while i > 0 {
                sum += i;
                i -= 1;
            }
            tx.send(sum).unwrap();
        });

        println!("work queue calu {}", rx.blocking_recv().unwrap());
    }
}
