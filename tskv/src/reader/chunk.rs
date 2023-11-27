use std::sync::Arc;

use super::{BatchReader, BatchReaderRef, Projection, SendableSchemableTskvRecordBatchStream};
use crate::reader::column_group::ColumnGroupReader;
use crate::tsm2::page::Chunk;
use crate::tsm2::reader::TSM2Reader;
use crate::Result;

pub struct ChunkReader {
    batch_readers: Vec<BatchReaderRef>,
}
impl ChunkReader {
    pub fn try_new(
        reader: Arc<TSM2Reader>,
        chunk: Arc<Chunk>,
        projection: &Projection,
        _batch_size: usize,
    ) -> Result<Self> {
        // todo(yukkit): filter column group
        let batch_readers = chunk
            .column_group()
            .values()
            .map(|e| {
                let column_group_reader =
                    ColumnGroupReader::try_new(reader.clone(), e.clone(), projection, _batch_size)?;
                Ok(Arc::new(column_group_reader) as BatchReaderRef)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self { batch_readers })
    }

    pub fn new_with_unchecked(batch_readers: Vec<BatchReaderRef>) -> Self {
        Self { batch_readers }
    }
}

impl BatchReader for ChunkReader {
    fn process(&self) -> Result<SendableSchemableTskvRecordBatchStream> {
        self.batch_readers.process()
    }
}
