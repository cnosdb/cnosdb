use std::{
    cell::UnsafeCell,
    future::Future,
    mem::{forget, ManuallyDrop},
    pin::Pin,
    sync::{
        atomic::{
            AtomicU8,
            Ordering::{self, Relaxed},
        },
        Arc,
    },
    task::{Context, RawWaker, RawWakerVTable, Waker},
};

use crossbeam::channel::Sender;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

const WAITING: u8 = 0;
const POLLING: u8 = 1;
const REPOLL: u8 = 2;
const COMPLETE: u8 = 3;

/// default ordering
const ORDERING: Ordering = Relaxed;

struct Task {
    task: UnsafeCell<BoxFuture<'static, ()>>,
    queue: Sender<ArcTask>,
    status: AtomicU8,
}

#[derive(Clone)]
pub struct ArcTask(Arc<Task>);

unsafe impl Send for ArcTask {}

impl ArcTask {
    #[inline]
    pub fn new<F>(future: F, queue: Sender<ArcTask>) -> Self
        where F: Future<Output = ()> + Send + 'static
    {
        let future = Arc::new(Task { task: UnsafeCell::new(Box::pin(future)),
                                     queue,
                                     status: AtomicU8::new(WAITING) });
        let future: *const Task = Arc::into_raw(future) as *const Task;
        unsafe { task(future) }
    }

    #[inline]
    pub unsafe fn poll(self) {
        self.0.status.store(POLLING, ORDERING);
        let waker = ManuallyDrop::new(waker(&*self.0));
        let mut cx = Context::from_waker(&waker);
        loop {
            if Pin::new(&mut *self.0.task.get()).poll(&mut cx).is_ready() {
                break self.0.status.store(COMPLETE, ORDERING);
            }
            match self.0.status.compare_exchange(POLLING, WAITING, ORDERING, ORDERING) {
                Ok(_) => break,
                Err(_) => self.0.status.store(POLLING, ORDERING),
            }
        }
    }
}

#[inline]
unsafe fn waker(task: *const Task) -> Waker {
    Waker::from_raw(RawWaker::new(task as *const (),
                                  &RawWakerVTable::new(clone_raw,
                                                       wake_raw,
                                                       wake_ref_raw,
                                                       drop_raw)))
}

#[inline]
unsafe fn clone_raw(this: *const ()) -> RawWaker {
    let task = clone_task(this as *const Task);
    RawWaker::new(Arc::into_raw(task.0) as *const (),
                  &RawWakerVTable::new(clone_raw, wake_raw, wake_ref_raw, drop_raw))
}

#[inline]
unsafe fn drop_raw(this: *const ()) {
    drop(task(this as *const Task))
}

#[inline]
unsafe fn wake_raw(this: *const ()) {
    let task = task(this as *const Task);
    let mut status = task.0.status.load(ORDERING);
    loop {
        match status {
            WAITING => match task.0.status.compare_exchange(WAITING, POLLING, ORDERING, ORDERING) {
                Ok(_) => {
                    task.0.queue.send(clone_task(&*task.0)).unwrap();
                    break;
                },
                Err(cur) => status = cur,
            },
            POLLING => match task.0.status.compare_exchange(POLLING, REPOLL, ORDERING, ORDERING) {
                Ok(_) => break,
                Err(cur) => status = cur,
            },
            _ => break,
        }
    }
}

#[inline]
unsafe fn wake_ref_raw(this: *const ()) {
    let task = ManuallyDrop::new(task(this as *const Task));
    let mut status = task.0.status.load(ORDERING);
    loop {
        match status {
            WAITING => match task.0.status.compare_exchange(WAITING, POLLING, ORDERING, ORDERING) {
                Ok(_) => {
                    task.0.queue.send(clone_task(&*task.0)).unwrap();
                    break;
                },
                Err(cur) => status = cur,
            },
            POLLING => match task.0.status.compare_exchange(POLLING, REPOLL, ORDERING, ORDERING) {
                Ok(_) => break,
                Err(cur) => status = cur,
            },
            _ => break,
        }
    }
}

#[inline]
unsafe fn task(future: *const Task) -> ArcTask {
    ArcTask(Arc::from_raw(future))
}

#[inline]
unsafe fn clone_task(future: *const Task) -> ArcTask {
    let task = task(future);
    forget(task.clone());
    task
}
