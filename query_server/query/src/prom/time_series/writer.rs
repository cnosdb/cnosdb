use std::collections::HashMap;

use datafusion::arrow::array::{ArrayRef, PrimitiveArray, StringArray};
use datafusion::arrow::datatypes::{
    DataType, Float16Type, Float32Type, Float64Type, SchemaRef, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
use datafusion::arrow::record_batch::RecordBatch;
use models::schema::TIME_FIELD_NAME;
use protos::prompb::prometheus::{Label, Sample, TimeSeries};
use spi::{CommonSnafu, QueryError, QueryResult};
use trace::debug;

use crate::prom::METRIC_SAMPLE_COLUMN_NAME;

#[derive(Debug)]
pub struct Writer<'a> {
    /// The object to write to
    tag_name_indices: Vec<usize>,
    // The column name of the tag_name_indices index
    tag_names: Vec<String>,
    sample_value_idx: usize,
    sample_time_idx: usize,
    schema: SchemaRef,

    labels_to_series: &'a mut HashMap<String, TimeSeries>,
}

impl Writer<'_> {
    /// Convert a record to a metric
    fn apply(&mut self, batch: &[ArrayRef], row_index: usize) -> QueryResult<()> {
        let mut labels = Vec::with_capacity(self.tag_name_indices.len());
        for (tag_idx, tag_name) in self.tag_name_indices.iter().zip(&self.tag_names) {
            let col = &batch[*tag_idx];
            let tag_value = match col.data_type() {
                DataType::Utf8 => col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Invalid data, this maybe DataFusion's bug.")
                    .value(row_index)
                    .to_owned(),
                _ => {
                    return Err(CommonSnafu {
                        msg: "Tag noly support string type".to_string(),
                    }
                    .build());
                }
            };
            labels.push(Label {
                name: tag_name.to_owned(),
                value: tag_value,
            });
        }

        let col = &batch[self.sample_value_idx];
        let sample_value = unsafe {
            match col.data_type() {
                DataType::Float64 => col
                    .as_any()
                    .downcast_ref::<PrimitiveArray<Float64Type>>()
                    .unwrap_unchecked()
                    .value(row_index),
                DataType::Float32 => f64::from(
                    col.as_any()
                        .downcast_ref::<PrimitiveArray<Float32Type>>()
                        .unwrap_unchecked()
                        .value(row_index),
                ),
                DataType::Float16 => f64::from(
                    col.as_any()
                        .downcast_ref::<PrimitiveArray<Float16Type>>()
                        .unwrap_unchecked()
                        .value(row_index),
                ),
                _ => {
                    return Err(CommonSnafu {
                        msg: "Prom sample value noly support float type".to_string(),
                    }
                    .build());
                }
            }
        };

        let col = &batch[self.sample_time_idx];
        let sample_timestamp_ms = unsafe {
            match col.data_type() {
                DataType::Timestamp(time_unit, _) => match time_unit {
                    TimeUnit::Second => {
                        col.as_any()
                            .downcast_ref::<PrimitiveArray<TimestampSecondType>>()
                            .unwrap_unchecked()
                            .value(row_index)
                            * 1_000
                    }
                    TimeUnit::Millisecond => col
                        .as_any()
                        .downcast_ref::<PrimitiveArray<TimestampMillisecondType>>()
                        .unwrap_unchecked()
                        .value(row_index),
                    TimeUnit::Microsecond => {
                        col.as_any()
                            .downcast_ref::<PrimitiveArray<TimestampMicrosecondType>>()
                            .unwrap_unchecked()
                            .value(row_index)
                            / 1_000
                    }
                    TimeUnit::Nanosecond => {
                        col.as_any()
                            .downcast_ref::<PrimitiveArray<TimestampNanosecondType>>()
                            .unwrap_unchecked()
                            .value(row_index)
                            / 1_000_000
                    }
                },
                _ => {
                    return Err(CommonSnafu {
                        msg: "Prom sample value noly support TimestampMillisecondType".to_string(),
                    }
                    .build());
                }
            }
        };

        // construct sample
        let sample = Sample {
            value: sample_value,
            timestamp: sample_timestamp_ms,
        };

        // save Sample
        let labels_str = concat_labels(&labels);
        debug!(
            "Metric labels str: {}, row_index: {}",
            labels_str, row_index
        );
        self.labels_to_series
            .entry(labels_str)
            .or_insert_with(|| TimeSeries {
                labels,
                ..Default::default()
            })
            .samples
            .push(sample);

        Ok(())
    }

    /// Write a vector of record batches to time series vec
    pub fn write(&mut self, batch: &RecordBatch) -> QueryResult<()> {
        debug_assert_eq!(self.schema.fields(), batch.schema().fields());

        let columns = batch.columns();

        for row_index in 0..batch.num_rows() {
            self.apply(columns, row_index)?;
        }

        Ok(())
    }
}

/// A CSV writer builder
#[derive(Debug)]
pub struct WriterBuilder {
    tag_name_indices: Vec<usize>,
    // The column name of the tag_name_indices index
    tag_names: Vec<String>,
    sample_value_idx: usize,
    sample_time_idx: usize,
    schema: SchemaRef,
}

impl WriterBuilder {
    pub fn try_new(
        tag_name_indices: Vec<usize>,
        sample_value_idx: usize,
        sample_time_idx: usize,
        schema: SchemaRef,
    ) -> QueryResult<Self> {
        // prom remote read only support data type: string(label)/float(sample value)/timestamp(time)
        let _ = schema
            .fields()
            .iter()
            .map(|array| match array.data_type() {
                DataType::Utf8 | DataType::Float64 | DataType::Timestamp(_, _) => Ok(()),
                type_ => {
                    return Err(QueryError::DataType {
                        data_type: type_.to_string(),
                        column: array.name().to_string(),
                        prompt: "only support data type: string(label)/float(sample value)/timestamp(time)".to_string(),
                    });
                }
            })
            .collect::<QueryResult<Vec<_>>>()?;
        // valid indices
        let tags = schema.project(&tag_name_indices)?;
        let _ = schema
            .fields
            .get(sample_value_idx)
            .ok_or_else(|| QueryError::ColumnNotFound {
                col: METRIC_SAMPLE_COLUMN_NAME.to_string(),
            })?;
        let _ = schema
            .fields
            .get(sample_time_idx)
            .ok_or_else(|| QueryError::ColumnNotFound {
                col: TIME_FIELD_NAME.to_string(),
            })?;

        let tag_names = tags.fields().iter().map(|e| e.name()).cloned().collect();

        Ok(Self {
            tag_name_indices,
            tag_names,
            sample_value_idx,
            sample_time_idx,
            schema,
        })
    }

    /// Create a new `Writer`
    pub fn build(self, labels_to_series: &mut HashMap<String, TimeSeries>) -> Writer<'_> {
        Writer {
            tag_name_indices: self.tag_name_indices,
            tag_names: self.tag_names,
            sample_value_idx: self.sample_value_idx,
            sample_time_idx: self.sample_time_idx,
            schema: self.schema,
            labels_to_series,
        }
    }
}

fn concat_labels(labels: &[Label]) -> String {
    labels
        .iter()
        .flat_map(|e| [e.name[..].as_ref(), e.value[..].as_ref()])
        .collect::<Vec<_>>()
        // 0x01 cannot occur in valid UTF-8 sequences, so use it
        // as a separator here.
        .join("\x01")
}
