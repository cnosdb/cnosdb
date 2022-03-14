use core_affinity::CoreId;
use crossbeam::channel::{unbounded, Sender};
use std::future::Future;
use std::thread;

use crate::task::ArcTask;

pub struct Runtime {
    queues: Vec<Sender<ArcTask>>,
}

impl Runtime {
    pub fn new(core_ids: &[CoreId]) -> Self {
        let mut queues = Vec::with_capacity(core_ids.len());
        for core_id in core_ids {
            let (tx, rx) = unbounded::<ArcTask>();
            queues.push(tx);
            let core_id = core_id.to_owned();
            thread::spawn(move || {
                core_affinity::set_for_current(core_id);
                loop {
                    while let Ok(task) = rx.recv() {
                        unsafe { task.poll() }
                    }
                }
            });
        }

        Self { queues }
    }

    pub fn spawn<F>(&self, index: usize, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.queues[index]
            .send(ArcTask::new(task, self.queues[index].clone()))
            .unwrap();
    }
}
