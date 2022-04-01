use crate::direct_io::File;
use crate::error::Error;
use crate::error::Result;
use core::slice;
use crossbeam::queue::ArrayQueue;
use futures::channel::oneshot::Sender as OnceSender;
use parking_lot::Condvar;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

const QUEUE_SIZE: usize = 1 << 10;
struct IoTask {
    priority: u32,
    ptr: *mut u8,
    size: usize,
    offset: u64,
    fd: Arc<File>,
    cb: OnceSender<Result<usize>>,
}

unsafe impl Send for IoTask {}
unsafe impl Sync for IoTask {}

impl IoTask {
    fn run(self) {
        unsafe {
            let buf = slice::from_raw_parts_mut(self.ptr, self.size);
            let ret = self
                .fd
                .read_at(self.offset, buf)
                .map_err(|e| Error::IO { source: e });
            let _ = self.cb.send(ret);
        }
    }
}

pub struct AsyncContext {
    read_queue: ArrayQueue<IoTask>,
    write_queue: ArrayQueue<IoTask>,
    high_op_queue: ArrayQueue<IoTask>,
    worker_count: AtomicUsize,
    total_count: usize,
    thread_state: Mutex<Vec<bool>>,
    thread_conv: Condvar,
    closed: AtomicBool,
}

impl AsyncContext {
    fn new(thread_num: usize) -> Self {
        Self {
            read_queue: ArrayQueue::new(QUEUE_SIZE),
            write_queue: ArrayQueue::new(QUEUE_SIZE),
            high_op_queue: ArrayQueue::new(QUEUE_SIZE),
            worker_count: AtomicUsize::new(thread_num),
            total_count: thread_num,
            thread_state: Mutex::new(vec![false; thread_num]),
            thread_conv: Condvar::default(),
            closed: AtomicBool::new(false),
        }
    }

    fn wait(&self, id: usize) {
        let mut state = self.thread_state.lock();
        if !self.high_op_queue.is_empty()
            || !self.read_queue.is_empty()
            || !self.write_queue.is_empty()
        {
            return;
        }
        state[id] = true;
        self.worker_count.fetch_sub(1, Ordering::SeqCst);
        while state[id] {
            self.thread_conv.wait(&mut state);
        }
        self.worker_count.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_up_one(&self){
        if self.worker_count.load(Ordering::SeqCst) >= self.total_count{
            return;
        }

        let mut state = self.thread_state.lock();
        for iter in state.iter_mut(){
            if *iter == false{
                *iter = true;
                break;
            }
        }
        self.thread_conv.notify_all();
    }

    fn closed(&self){
        if !self.closed.swap(true, Ordering::SeqCst){
            while let Some(t) = self.read_queue.pop(){
                let _ = t.cb.send(Err(Error::Cancel{info: "stop async file system".to_string()}));
            }
            while let Some(t) = self.write_queue.pop(){
                 let _ = t.cb.send(Err(Error::Cancel{info: "stop async file system".to_string()}));

            }
            while let Some(t) = self.high_op_queue.pop(){
                let _ = t.cb.send(Err(Error::Cancel{info: "stop async file system".to_string()}));
            }
        }
    }
    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}
