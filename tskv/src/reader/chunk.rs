use std::sync::Arc;

use super::{BatchReader, BatchReaderRef, Projection, SendableSchemableTskvRecordBatchStream};
use crate::reader::column_group::ColumnGroupReader;
use crate::tsm2::page::Chunk;
use crate::tsm2::reader::TSM2Reader;
use crate::Result;

pub struct ChunkReader {
    chunk: Arc<Chunk>,
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

        Ok(Self {
            chunk,
            batch_readers,
        })
    }

    pub fn new_with_unchecked(chunk: Arc<Chunk>, batch_readers: Vec<BatchReaderRef>) -> Self {
        Self {
            chunk,
            batch_readers,
        }
    }
}

impl BatchReader for ChunkReader {
    fn process(&self) -> Result<SendableSchemableTskvRecordBatchStream> {
        self.batch_readers.process()
    }

    fn fmt_as(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let column_group_nums = self.chunk.column_group().len();
        let time_range = self.chunk.time_range();

        write!(
            f,
            "ChunkReader: column_group_nums={column_group_nums}, time_range={time_range}"
        )
    }

    fn children(&self) -> Vec<BatchReaderRef> {
        self.batch_readers.clone()
    }
}
