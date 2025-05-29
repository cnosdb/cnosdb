use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::Accumulator;
use datafusion::scalar::ScalarValue;
use models::arrow::DataType;
use parking_lot::lock_api::MutexGuard;
use parking_lot::{Mutex, RawMutex};
use spi::QueryError;

use super::common::{DataQualityFunction, DataSeriesQuality};

#[derive(Debug)]
pub enum DataQualityAccumulator {
    Completeness { series: Arc<Mutex<Vec<(f64, f64)>>> },
    Consistency { series: Arc<Mutex<Vec<(f64, f64)>>> },
    Timeliness { series: Arc<Mutex<Vec<(f64, f64)>>> },
    Validity { series: Arc<Mutex<Vec<(f64, f64)>>> },
}

impl DataQualityAccumulator {
    pub fn new(func: DataQualityFunction) -> Self {
        match func {
            DataQualityFunction::Completeness => DataQualityAccumulator::Completeness {
                series: Arc::new(Mutex::new(vec![])),
            },
            DataQualityFunction::Consistency => DataQualityAccumulator::Consistency {
                series: Arc::new(Mutex::new(vec![])),
            },
            DataQualityFunction::Timeliness => DataQualityAccumulator::Timeliness {
                series: Arc::new(Mutex::new(vec![])),
            },
            DataQualityFunction::Validity => DataQualityAccumulator::Validity {
                series: Arc::new(Mutex::new(vec![])),
            },
        }
    }

    fn series_guard(&self) -> MutexGuard<'_, RawMutex, Vec<(f64, f64)>> {
        match self {
            DataQualityAccumulator::Completeness { series, .. } => series.lock(),
            DataQualityAccumulator::Consistency { series, .. } => series.lock(),
            DataQualityAccumulator::Timeliness { series, .. } => series.lock(),
            DataQualityAccumulator::Validity { series, .. } => series.lock(),
        }
    }
}

impl Accumulator for DataQualityAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        let times = values[0].as_ref();
        let values = values[1].as_ref();

        let mut series_guard = self.series_guard();

        *series_guard = (0..times.len())
            .map(|index| {
                Ok((
                    ScalarValue::try_from_array(times, index)
                        .map(|v: ScalarValue| scalar_to_f64(&v))?,
                    ScalarValue::try_from_array(values, index)
                        .map(|v: ScalarValue| scalar_to_f64(&v))?,
                ))
            })
            .collect::<datafusion::error::Result<Vec<_>>>()?;

        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        let mut series_guard = self.series_guard();
        series_guard
            .sort_unstable_by(|&a, &b| a.0.partial_cmp(&b.0).expect("NaN should not appear here"));

        let (times, values): (Vec<_>, Vec<_>) =
            std::mem::take(&mut *series_guard).into_iter().unzip();

        let mut series_quality = DataSeriesQuality::new(times, values)?;

        match self {
            DataQualityAccumulator::Completeness { .. } => {
                series_quality.time_detect();
                Ok(ScalarValue::Float64(Some(series_quality.completeness())))
            }
            DataQualityAccumulator::Consistency { .. } => {
                series_quality.time_detect();
                Ok(ScalarValue::Float64(Some(series_quality.consistency())))
            }
            DataQualityAccumulator::Timeliness { .. } => {
                series_quality.time_detect();
                Ok(ScalarValue::Float64(Some(series_quality.timeliness())))
            }
            DataQualityAccumulator::Validity { .. } => {
                series_quality.value_detect();
                Ok(ScalarValue::Float64(Some(series_quality.validity())))
            }
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of::<DataQualityAccumulator>()
            + self.series_guard().capacity() * std::mem::size_of::<(f64, f64)>()
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let series_guard = self.series_guard();
        let mut left = Vec::with_capacity(series_guard.len());
        let mut right = Vec::with_capacity(series_guard.len());
        for x in series_guard.iter() {
            left.push(ScalarValue::Float64(Some(x.0)));
            right.push(ScalarValue::Float64(Some(x.1)));
        }
        drop(series_guard);

        Ok(vec![
            ScalarValue::List(ScalarValue::new_list_nullable(&left, &DataType::Float64)),
            ScalarValue::List(ScalarValue::new_list_nullable(&right, &DataType::Float64)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        if states[0].is_empty() {
            return Ok(());
        }

        let (time_array, value_array) = (&states[0], &states[1]);
        let time_lists = (0..time_array.len())
            .map(|index| ScalarValue::try_from_array(time_array, index))
            .collect::<DFResult<Vec<_>>>()?;
        let value_lists = (0..time_array.len())
            .map(|index| ScalarValue::try_from_array(value_array, index))
            .collect::<DFResult<Vec<_>>>()?;

        let mut series_guard = self.series_guard();

        for (time_list, value_list) in time_lists.into_iter().zip(value_lists.into_iter()) {
            match (time_list, value_list) {
                (ScalarValue::List(time_list), ScalarValue::List(value_list)) => {
                    let len = time_list.len().min(value_list.len());
                    let mut series = Vec::with_capacity(len);
                    for i in 0..len {
                        let tv = ScalarValue::try_from_array(time_list.as_ref(), i)?;
                        let t = scalar_to_f64(&tv);
                        let vv = ScalarValue::try_from_array(value_list.as_ref(), i)?;
                        let v = scalar_to_f64(&vv);
                        series.push((t, v));
                    }
                    series_guard.append(&mut series);
                }
                (other1, other2) => {
                    return Err(DataFusionError::External(Box::new(QueryError::Internal {
                        reason: format!(
                            "data quality accumulator state type should be ScalarValue::List, but found: time_list: {:?}, value_list: {:?}",
                            other1,
                            other2
                        ),
                    })))
                }
            }
        }

        Ok(())
    }
}

fn scalar_to_f64(v: &ScalarValue) -> f64 {
    match v {
        ScalarValue::Int32(Some(val)) => *val as f64,
        ScalarValue::Int64(Some(val)) => *val as f64,
        ScalarValue::Float32(Some(val)) => *val as f64,
        ScalarValue::Float64(Some(val)) => *val,
        ScalarValue::TimestampMicrosecond(Some(val), None) => *val as f64,
        ScalarValue::TimestampNanosecond(Some(val), None) => 1000.0 * *val as f64,
        ScalarValue::TimestampMillisecond(Some(val), None) => 1000000.0 * *val as f64,
        ScalarValue::TimestampSecond(Some(val), None) => 1000000000.0 * *val as f64,
        _ => f64::NAN,
    }
}
