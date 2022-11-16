use datafusion::arrow::record_batch::RecordBatch;
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::errors::CoordinatorResult;

pub struct ReaderIterator {
    pub sender: Sender<CoordinatorResult<RecordBatch>>,
    pub receiver: Receiver<CoordinatorResult<RecordBatch>>,
}

impl ReaderIterator {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(1024);

        Self { sender, receiver }
    }

    pub async fn next(&mut self) -> Option<CoordinatorResult<RecordBatch>> {
        self.receiver.recv().await
    }
}
