use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow_array::{ArrayRef, RecordBatch};
use futures::{ready, Stream, StreamExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use trace::warn;

use super::page::{PageReaderRef, PrimitiveArrayReader};
use super::{
    BatchReader, Projection, SchemableTskvRecordBatchStream, SendableSchemableTskvRecordBatchStream,
};
use crate::tsm2::page::{Chunk, PageWriteSpec};
use crate::tsm2::reader::TSM2Reader;
use crate::Result;

pub struct ChunkReader {
    reader: Arc<TSM2Reader>,
    chunk: Arc<Chunk>,
    page_readers: Vec<PageReaderRef>,
    schema: SchemaRef,
}
impl ChunkReader {
    pub fn try_new(
        reader: Arc<TSM2Reader>,
        chunk: Arc<Chunk>,
        projection: &Projection,
        _batch_size: usize,
    ) -> Result<Self> {
        let columns = projection
            .0
            .iter()
            .filter_map(|e| chunk.pages().iter().find(|e2| &e2.meta().column.name == e));

        let page_readers = columns
            .clone()
            .map(|e| build_reader(reader.clone(), e))
            .collect::<Result<Vec<_>>>()?;

        let fields = columns
            .map(|e| &e.meta().column)
            .map(Field::from)
            .collect::<Vec<_>>();

        let schema = Arc::new(Schema::new(fields));

        Ok(Self {
            reader,
            chunk,
            page_readers,
            schema,
        })
    }
}

impl BatchReader for ChunkReader {
    fn process(&self) -> Result<SendableSchemableTskvRecordBatchStream> {
        let streams = self
            .page_readers
            .iter()
            .map(|r| r.process())
            .collect::<Result<Vec<_>>>()?;

        let (join_handles, buffers): (Vec<_>, Vec<_>) = streams
            .into_iter()
            .map(|mut s| {
                let (sender, receiver) = mpsc::channel::<Result<ArrayRef>>(1);

                let task = async move {
                    while let Some(item) = s.next().await {
                        let exit = item.is_err();
                        // If send fails, stream being torn down,
                        // there is no place to send the error.
                        if sender.send(item).await.is_err() {
                            warn!("Stopping execution: output is gone, PageReader cancelling");
                            return;
                        }
                        if exit {
                            return;
                        }
                    }
                };
                let join_handle = tokio::spawn(task);
                (join_handle, receiver)
            })
            .unzip();

        Ok(Box::pin(ChunkRecordBatchStream {
            schema: self.schema.clone(),
            column_arrays: Vec::with_capacity(self.schema.fields().len()),
            buffers,
            join_handles,
        }))
    }
}

struct ChunkRecordBatchStream {
    schema: SchemaRef,

    /// Stream entries
    column_arrays: Vec<ArrayRef>,
    buffers: Vec<mpsc::Receiver<Result<ArrayRef>>>,
    join_handles: Vec<JoinHandle<()>>,
}

impl SchemableTskvRecordBatchStream for ChunkRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for ChunkRecordBatchStream {
    type Item = Result<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let schema = self.schema.clone();
        let column_nums = self.buffers.len();

        loop {
            let exists_column_nums = self.column_arrays.len();

            match ready!(self.buffers[exists_column_nums].poll_recv(cx)) {
                Some(Ok(array)) => {
                    let arrays = &mut self.column_arrays;
                    arrays.push(array);

                    if arrays.len() == column_nums {
                        // 可以构造 RecordBatch
                        let arrays = std::mem::take(arrays);
                        return Poll::Ready(Some(
                            RecordBatch::try_new(schema, arrays).map_err(Into::into),
                        ));
                    }
                    continue;
                }
                Some(Err(e)) => {
                    return Poll::Ready(Some(Err(e)));
                }
                None => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

fn build_reader(reader: Arc<TSM2Reader>, page_meta: &PageWriteSpec) -> Result<PageReaderRef> {
    // TODO 根据指定列及其元数据和文件读取器，构造 PageReader
    let data_type = page_meta.meta().column.column_type.to_physical_data_type();
    Ok(Arc::new(PrimitiveArrayReader::new(
        data_type, reader, page_meta,
    )))
}
