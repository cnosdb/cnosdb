use std::cmp::Ordering;
use std::marker::PhantomData;

use arrow::compute::interleave;
use arrow::datatypes::SchemaRef;
use arrow_array::{Array, ArrayRef, RecordBatch};
use datafusion::common::DataFusionError;
use models::datafusion::cursor::{FieldArray, FieldValues};

use crate::Result;

#[derive(Debug, Copy, Clone, Default)]
struct BatchCursor {
    /// The index into BatchMergeBuilder::batches
    batch_idx: usize,
    /// The row index within the given batch
    row_idx: usize,
}

/// Provides an API to incrementally build a [`RecordBatch`] from partitioned [`RecordBatch`]
#[derive(Debug)]
pub struct BatchMergeBuilder<T: FieldArray> {
    /// The schema of the RecordBatches yielded by this stream
    schema: SchemaRef,

    /// Maintain a list of [`RecordBatch`] and their corresponding stream
    batches: Vec<(Option<usize>, RecordBatch)>,

    /// The current [`BatchCursor`] for each stream
    cursors: Vec<BatchCursor>,

    /// The accumulated stream indexes from which to pull rows
    /// Consists of a tuple of `(batch_idx, row_idx)`
    indices: Vec<(usize, usize)>,

    // batch_index, column_index, row_index, same_rows
    last: Option<(usize, usize, usize)>,

    last_same_rows: Vec<(usize, usize)>,

    phantom: PhantomData<T>,
}

impl<T: FieldArray> BatchMergeBuilder<T> {
    /// Create a new [`BatchMergeBuilder`] with the provided `stream_count` and `batch_size`
    pub fn new(schema: SchemaRef, stream_count: usize, batch_size: usize) -> Self {
        Self {
            schema,
            batches: Vec::with_capacity(stream_count),
            cursors: vec![BatchCursor::default(); stream_count],
            indices: Vec::with_capacity(batch_size),
            last: None,
            last_same_rows: vec![],
            phantom: Default::default(),
        }
    }

    /// Append a new batch in `stream_idx`
    pub fn push_batch(&mut self, stream_idx: usize, batch: RecordBatch) {
        let batch_idx = self.batches.len();
        self.batches.push((Some(stream_idx), batch));
        self.cursors[stream_idx] = BatchCursor {
            batch_idx,
            row_idx: 0,
        }
    }

    /// Append the next row from `stream_idx`
    pub fn push_row(&mut self, stream_idx: usize, column_idx: usize) -> Result<()> {
        let cursor = &mut self.cursors[stream_idx];
        let row_idx = cursor.row_idx;
        cursor.row_idx += 1;
        match self.last.as_mut() {
            None => {
                self.last = Some((cursor.batch_idx, column_idx, row_idx));
                self.last_same_rows.push((cursor.batch_idx, row_idx))
            }

            Some((last_batch_idx, last_column_idx, last_row_idx)) => {
                let last_batch: &RecordBatch = &self.batches[*last_batch_idx].1;
                let last_array = last_batch.column(*last_column_idx);
                let last_array: &T = last_array
                    .as_any()
                    .downcast_ref::<T>()
                    .expect("field values");
                let last_values = last_array.values();
                let last_value = last_values.value(*last_row_idx);

                let now_batch_idx = cursor.batch_idx;
                let now_row_idx = row_idx;
                let now_column_idx = column_idx;
                let now_batch: &RecordBatch = &self.batches[cursor.batch_idx].1;
                let now_array = now_batch.column(now_column_idx);
                let now_array: &T = now_array
                    .as_any()
                    .downcast_ref::<T>()
                    .expect("field values");
                let now_values = now_array.values();
                let now_value = now_values.value(row_idx);

                match T::Values::compare(last_value, now_value) {
                    Ordering::Less => {
                        *last_batch_idx = now_batch_idx;
                        *last_column_idx = now_column_idx;
                        *last_row_idx = now_row_idx;
                        self.take_last_and_merge()?;
                        self.last_same_rows.push((now_batch_idx, now_row_idx));
                    }
                    Ordering::Equal => {
                        *last_batch_idx = now_batch_idx;
                        *last_column_idx = now_column_idx;
                        *last_row_idx = now_row_idx;
                        self.last_same_rows.push((now_batch_idx, now_row_idx));
                    }
                    Ordering::Greater => {
                        self.take_last_and_merge()?;
                    }
                }
            }
        }
        Ok(())
    }

    pub fn take_last_and_merge(&mut self) -> Result<(), DataFusionError> {
        if self.last_same_rows.len() > 1 {
            let batches = self
                .last_same_rows
                .iter()
                .map(|(batch_idx, _)| &self.batches[*batch_idx].1)
                .collect::<Vec<&RecordBatch>>();
            let rows_idx = self
                .last_same_rows
                .iter()
                .map(|(_, row_idx)| *row_idx)
                .collect::<Vec<_>>();
            let new_record_batch = merge_rows(self.schema.clone(), &batches, &rows_idx)?;
            self.batches.push((None, new_record_batch));
            self.indices.push((self.batches.len() - 1, 0));
            self.last_same_rows.clear();
        } else {
            self.indices.append(&mut self.last_same_rows);
        }

        Ok(())
    }

    /// Returns the number of in-progress rows in this [`BatchMergeBuilder`]
    pub fn len(&self) -> usize {
        self.indices.len()
    }

    /// Returns `true` if this [`BatchMergeBuilder`] contains no in-progress rows
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    /// Returns the schema of this [`BatchMergeBuilder`]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Drains the in_progress row indexes, and builds a new RecordBatch from them
    ///
    /// Will then drop any batches for which all rows have been yielded to the output
    ///
    /// Returns `None` if no pending rows
    pub fn build_record_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.is_empty() {
            return Ok(None);
        }

        let columns = (0..self.schema.fields.len())
            .map(|column_idx| {
                let arrays: Vec<_> = self
                    .batches
                    .iter()
                    .map(|(_, batch)| batch.column(column_idx).as_ref())
                    .collect();
                Ok(interleave(&arrays, &self.indices)?)
            })
            .collect::<std::result::Result<Vec<_>, DataFusionError>>()?;
        self.indices.clear();

        Ok(Some(RecordBatch::try_new(self.schema.clone(), columns)?))
    }
}

/// (1, 1, null, 2)
/// (1, 1, null, 3)  =>   (1, 2, 3, 3)
/// (1, 2, 3, null)
///
pub fn merge_rows(
    schema: SchemaRef,
    batches: &[&RecordBatch],
    rows_idx: &[usize],
) -> Result<RecordBatch, DataFusionError> {
    let indices = rows_idx.iter().cloned().enumerate().collect::<Vec<_>>();

    let columns = (0..schema.fields.len())
        .map(|column_idx| {
            let arrays: Vec<_> = batches
                .iter()
                .map(|b| b.column(column_idx).as_ref())
                .collect();
            let array = interleave(&arrays, &indices)?;
            Ok(merge_column(&array))
        })
        .collect::<std::result::Result<Vec<_>, DataFusionError>>()?;

    Ok(RecordBatch::try_new(schema, columns)?)
}

///
/// ┌──────┐
/// │  1   │
/// ├──────┤
/// │ null │
/// ├──────┤
/// │  2   │    ┌──────┐
/// ├──────┼───►│  3   │
/// │ null │    └──────┘
/// ├──────┤
/// │  3   │
/// ├──────┤
/// │ null │
/// └──────┘
pub fn merge_column(array: &ArrayRef) -> ArrayRef {
    if array.len() <= 1 {
        return array.clone();
    }
    let mut i = array.len() - 1;
    loop {
        if !array.is_null(i) {
            return array.slice(i, 1);
        }
        if i == 0 {
            return array.slice(0, 1);
        }
        i -= 1;
    }
}
