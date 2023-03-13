use datafusion::arrow::record_batch::RecordBatch;
use models::record_batch_encode;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::errors::{CoordinatorError, CoordinatorResult};

#[derive(Debug)]
pub struct ReaderIterator {
    receiver: Receiver<CoordinatorResult<RecordBatch>>,
}

impl ReaderIterator {
    pub fn new() -> (Self, Sender<CoordinatorResult<RecordBatch>>) {
        let (sender, receiver) = mpsc::channel(1024);

        (Self { receiver }, sender)
    }

    pub async fn next(&mut self) -> Option<CoordinatorResult<RecordBatch>> {
        self.receiver.recv().await
    }

    pub async fn next_and_encdoe(&mut self) -> Option<CoordinatorResult<Vec<u8>>> {
        if let Some(record) = self.receiver.recv().await {
            match record {
                Ok(record) => match record_batch_encode(&record) {
                    Ok(data) => return Some(Ok(data)),
                    Err(err) => return Some(Err(CoordinatorError::ArrowError { source: err })),
                },
                Err(err) => return Some(Err(err)),
            }
        }

        None
    }
}
