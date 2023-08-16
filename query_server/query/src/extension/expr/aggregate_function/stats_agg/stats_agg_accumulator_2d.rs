use std::fmt::Debug;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, UInt64Array};
use datafusion::arrow::compute::{and, cast, filter, is_not_null};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::arrow::error::ArrowError;
use datafusion::common::downcast_value;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::Accumulator;
use datafusion::scalar::ScalarValue;
use spi::DFResult;

use super::{m3, m4};

pub const RETRACT_FLOATING_ERROR_THRESHOLD: f64 = 0.99;

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct StatsAggAccumulator2D {
    n: u64,   // count
    sx: f64,  // sum(x)
    sx2: f64, // sum((x-sx/n)^2) (sum of squares)
    sx3: f64, // sum((x-sx/n)^3)
    sx4: f64, // sum((x-sx/n)^4)
    sy: f64,  // sum(y)
    sy2: f64, // sum((y-sy/n)^2) (sum of squares)
    sy3: f64, // sum((y-sy/n)^3)
    sy4: f64, // sum((y-sy/n)^4)
    sxy: f64, // sum((x-sx/n)*(y-sy/n)) (sum of products)
}

impl StatsAggAccumulator2D {
    fn n_f64(&self) -> f64 {
        self.n as f64
    }

    pub(crate) fn get_datatype() -> DataType {
        DataType::Struct(Fields::from(vec![
            Field::new("n", DataType::UInt64, false),
            Field::new("sx", DataType::Float64, false),
            Field::new("sx2", DataType::Float64, false),
            Field::new("sx3", DataType::Float64, false),
            Field::new("sx4", DataType::Float64, false),
            Field::new("sy", DataType::Float64, false),
            Field::new("sy2", DataType::Float64, false),
            Field::new("sy3", DataType::Float64, false),
            Field::new("sy4", DataType::Float64, false),
            Field::new("sxy", DataType::Float64, false),
        ]))
    }

    fn has_infinite(&self) -> bool {
        self.sx.is_infinite()
            || self.sx2.is_infinite()
            || self.sx3.is_infinite()
            || self.sx4.is_infinite()
            || self.sy.is_infinite()
            || self.sy2.is_infinite()
            || self.sy3.is_infinite()
            || self.sy4.is_infinite()
            || self.sxy.is_infinite()
    }

    fn check_overflow(&self, old: &Self, x: f64, y: f64) -> bool {
        //Only report overflow if we have finite inputs that lead to infinite results.
        ((self.sx.is_infinite()
            || self.sx2.is_infinite()
            || self.sx3.is_infinite()
            || self.sx4.is_infinite())
            && old.sx.is_finite()
            && x.is_finite())
            || ((self.sy.is_infinite()
                || self.sy2.is_infinite()
                || self.sy3.is_infinite()
                || self.sy4.is_infinite())
                && old.sy.is_finite()
                && y.is_finite())
            || (self.sxy.is_infinite()
                && old.sx.is_finite()
                && x.is_finite()
                && old.sy.is_finite()
                && y.is_finite())
    }
}

impl TryInto<ScalarValue> for StatsAggAccumulator2D {
    type Error = DataFusionError;

    fn try_into(self) -> Result<ScalarValue, Self::Error> {
        let datatype = Self::get_datatype();
        if self.n == 0 {
            return Self::get_datatype().try_into();
        }
        let fields = match datatype {
            DataType::Struct(f) => f,
            _ => unreachable!(),
        };

        Ok(ScalarValue::Struct(
            Some(vec![
                ScalarValue::from(self.n),
                ScalarValue::from(self.sx),
                ScalarValue::from(self.sx2),
                ScalarValue::from(self.sx3),
                ScalarValue::from(self.sx4),
                ScalarValue::from(self.sy),
                ScalarValue::from(self.sy2),
                ScalarValue::from(self.sy3),
                ScalarValue::from(self.sy4),
                ScalarValue::from(self.sxy),
            ]),
            fields,
        ))
    }
}

impl TryFrom<ScalarValue> for StatsAggAccumulator2D {
    type Error = DataFusionError;

    fn try_from(value: ScalarValue) -> Result<Self, Self::Error> {
        match value {
            ScalarValue::Struct(None, ..) => Ok(Self::default()),
            ScalarValue::Struct(Some(values), ..) => {
                if values.len() != 10 {
                    return Err(DataFusionError::Execution(
                        "TryFrom<ScalarValue> for StatsAggAccumulator2D fail".into(),
                    ));
                }
                Ok(Self {
                    n: TryFrom::try_from(values[0].clone())?,
                    sx: TryFrom::try_from(values[1].clone())?,
                    sx2: TryFrom::try_from(values[2].clone())?,
                    sx3: TryFrom::try_from(values[3].clone())?,
                    sx4: TryFrom::try_from(values[4].clone())?,
                    sy: TryFrom::try_from(values[5].clone())?,
                    sy2: TryFrom::try_from(values[6].clone())?,
                    sy3: TryFrom::try_from(values[7].clone())?,
                    sy4: TryFrom::try_from(values[8].clone())?,
                    sxy: TryFrom::try_from(values[9].clone())?,
                })
            }
            _ => Err(DataFusionError::Execution(
                "TryFrom<ScalarValue> for StatsAggAccumulator2D fail".into(),
            )),
        }
    }
}

fn filter_not_null(arrays: &[ArrayRef]) -> DFResult<Vec<ArrayRef>> {
    if !arrays.is_empty() && arrays.iter().any(|array| array.null_count() != 0) {
        let mut mask = is_not_null(&arrays[0])?;
        for array in arrays.iter().skip(1) {
            mask = and(&mask, &is_not_null(&array)?)?;
        }
        let res = arrays
            .iter()
            .map(|array| filter(array, &mask))
            .collect::<Result<Vec<ArrayRef>, ArrowError>>()?;
        Ok(res)
    } else {
        Ok(arrays.to_vec())
    }
}

impl Accumulator for StatsAggAccumulator2D {
    // stats_agg(y, x)
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::common::Result<()> {
        let values = filter_not_null(&values[0..2])?;
        let x_array = &cast(&values[1], &DataType::Float64)?;
        let y_array = &cast(&values[0], &DataType::Float64)?;

        let x_array = downcast_value!(x_array, Float64Array).iter().flatten();
        let y_array = downcast_value!(y_array, Float64Array).iter().flatten();

        for (x, y) in x_array.zip(y_array) {
            let old = *self;
            self.n += 1;
            self.sx += x;
            self.sy += y;

            if old.n > 0 {
                let scale = (self.n_f64() * old.n_f64()).recip();

                let tmp_x = x * self.n_f64() - self.sx;
                self.sx2 += tmp_x * tmp_x * scale;
                self.sx3 = m3::accum(old.n_f64(), old.sx, old.sx2, old.sx3, x);
                self.sx4 = m4::accum(old.n_f64(), old.sx, old.sx2, old.sx3, old.sx4, x);

                let tmp_y = y * self.n_f64() - self.sy;
                self.sy2 += tmp_y * tmp_y * scale;
                self.sy3 = m3::accum(old.n_f64(), old.sy, old.sy2, old.sy3, y);
                self.sy4 = m4::accum(old.n_f64(), old.sy, old.sy2, old.sy3, old.sy4, y);

                self.sxy += tmp_x * tmp_y * scale;

                if self.has_infinite() {
                    if self.check_overflow(&old, x, y) {
                        return Err(DataFusionError::Execution("stats_agg overflow".into()));
                    }
                    // sxx, syy, and sxy should be set to NaN if any of their inputs are
                    // infinite, so if they ended up as infinite and there wasn't an overflow,
                    // we need to set them to NaN instead as this implies that there was an
                    // infinite input (because they necessarily involve multiplications of
                    // infinites, which are NaNs)
                    if self.sx2.is_infinite() {
                        self.sx2 = f64::NAN;
                    }
                    if self.sx3.is_infinite() {
                        self.sx3 = f64::NAN;
                    }

                    if self.sx4.is_infinite() {
                        self.sx4 = f64::NAN;
                    }
                    if self.sy2.is_infinite() {
                        self.sy2 = f64::NAN;
                    }
                    if self.sy3.is_infinite() {
                        self.sy3 = f64::NAN;
                    }
                    if self.sy4.is_infinite() {
                        self.sy4 = f64::NAN;
                    }
                }
            } else {
                // first input, leave sxx/syy/sxy alone unless we have infinite inputs
                if !x.is_finite() {
                    self.sx2 = f64::NAN;
                    self.sx3 = f64::NAN;
                    self.sx4 = f64::NAN;
                    self.sxy = f64::NAN;
                }

                if !y.is_finite() {
                    self.sy2 = f64::NAN;
                    self.sy3 = f64::NAN;
                    self.sy4 = f64::NAN;
                    self.sxy = f64::NAN;
                }
            }
        }
        Ok(())
    }

    fn evaluate(&self) -> datafusion::common::Result<ScalarValue> {
        if self.n == 0 {
            Self::get_datatype().try_into()
        } else {
            (*self).try_into()
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&self) -> datafusion::common::Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.n),
            ScalarValue::from(self.sx),
            ScalarValue::from(self.sx2),
            ScalarValue::from(self.sx3),
            ScalarValue::from(self.sx4),
            ScalarValue::from(self.sy),
            ScalarValue::from(self.sy2),
            ScalarValue::from(self.sy3),
            ScalarValue::from(self.sy4),
            ScalarValue::from(self.sxy),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::common::Result<()> {
        let ns = downcast_value!(states[0], UInt64Array);
        let sxs = downcast_value!(states[1], Float64Array);
        let sx2s = downcast_value!(states[2], Float64Array);
        let sx3s = downcast_value!(states[3], Float64Array);
        let sx4s = downcast_value!(states[4], Float64Array);

        let sys = downcast_value!(states[5], Float64Array);
        let sy2s = downcast_value!(states[6], Float64Array);
        let sy3s = downcast_value!(states[7], Float64Array);
        let sy4s = downcast_value!(states[8], Float64Array);
        let sxys = downcast_value!(states[9], Float64Array);

        for i in 0..ns.len() {
            if ns.value(i) == 0 {
                continue;
            }
            let other = Self {
                n: ns.value(i),
                sx: sxs.value(i),
                sx2: sx2s.value(i),
                sx3: sx3s.value(i),
                sx4: sx4s.value(i),
                sy: sys.value(i),
                sy2: sy2s.value(i),
                sy3: sy3s.value(i),
                sy4: sy4s.value(i),
                sxy: sxys.value(i),
            };
            if self.n == 0 {
                *self = other;
                continue;
            }
            let tmpx = self.sx / self.n_f64() - other.sx / other.n_f64();
            let tmpy = self.sy / self.n_f64() - other.sy / other.n_f64();
            let n = self.n + other.n;

            let new = Self {
                n,
                sx: self.sx + other.sx,
                sx2: self.sx2 + other.sx2 + self.n_f64() * other.n_f64() * tmpx * tmpx / n as f64,
                sx3: m3::combine(
                    self.n_f64(),
                    other.n_f64(),
                    self.sx,
                    other.sx,
                    self.sx2,
                    other.sx2,
                    self.sx3,
                    other.sx3,
                ),
                sx4: m4::combine(
                    self.n_f64(),
                    other.n_f64(),
                    self.sx,
                    other.sx,
                    self.sx2,
                    other.sx2,
                    self.sx3,
                    other.sx3,
                    self.sx4,
                    other.sx4,
                ),
                sy: self.sy + other.sy,
                sy2: self.sy2 + other.sy2 + self.n_f64() * other.n_f64() * tmpy * tmpy / n as f64,
                sy3: m3::combine(
                    self.n_f64(),
                    other.n_f64(),
                    self.sy,
                    other.sy,
                    self.sy2,
                    other.sy2,
                    self.sy3,
                    other.sy3,
                ),
                sy4: m4::combine(
                    self.n_f64(),
                    other.n_f64(),
                    self.sy,
                    other.sy,
                    self.sy2,
                    other.sy2,
                    self.sy3,
                    other.sy3,
                    self.sy4,
                    other.sy4,
                ),
                sxy: self.sxy + other.sxy + self.n_f64() * other.n_f64() * tmpx * tmpy / n as f64,
            };

            if new.has_infinite() && !self.has_infinite() && !other.has_infinite() {
                return Err(DataFusionError::Execution(
                    "stats_agg merge batch overflow".into(),
                ));
            }
        }

        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> datafusion::common::Result<()> {
        let values = filter_not_null(&values[0..2])?;
        let x_array = &cast(&values[1], &DataType::Float64)?;
        let y_array = &cast(&values[0], &DataType::Float64)?;

        let x_array = downcast_value!(x_array, Float64Array).iter().flatten();
        let y_array = downcast_value!(y_array, Float64Array).iter().flatten();

        for (x, y) in x_array.zip(y_array) {
            // if we are trying to remove a nan/infinite input, it's time to recalculate.
            if !x.is_finite() || !y.is_finite() {
                return Err(DataFusionError::Execution(
                    "stats_agg retract infinite number".into(),
                ));
            }
            // if we are removing a value that is very large compared to the sum of the values that we're removing it from,
            // we should probably recalculate to avoid accumulating error. We might want a different test for this, if there
            // is a  way to calculate the error directly, that might be best...
            if x / self.sx > RETRACT_FLOATING_ERROR_THRESHOLD
                || y / self.sy > RETRACT_FLOATING_ERROR_THRESHOLD
            {
                return Err(DataFusionError::Execution(
                    "stats_agg retract number over threshold".into(),
                ));
            }

            // if we're removing the last point we should just return a completely empty value to eliminate any errors, it can only be completely empty at that point.
            if self.n == 1 {
                *self = StatsAggAccumulator2D::default();
                continue;
            }

            let mut new = Self {
                n: self.n - 1,
                sx: self.sx - x,
                sy: self.sy - y,
                sx2: Default::default(), // initialize these for now.
                sx3: Default::default(),
                sx4: Default::default(),
                sy2: Default::default(),
                sy3: Default::default(),
                sy4: Default::default(),
                sxy: Default::default(),
            };

            let scale = (self.n_f64() * new.n_f64()).recip();

            let tmpx = x * self.n_f64() - self.sx;
            new.sx2 = self.sx2 - tmpx * tmpx * scale;
            new.sx3 = m3::remove(new.n_f64(), new.sx, new.sx2, self.sx3, x);
            new.sx4 = m4::remove(new.n_f64(), new.sx, new.sx2, new.sx3, self.sx4, x);

            let tmpy = y * self.n_f64() - self.sy;
            new.sy2 = self.sy2 - tmpy * tmpy * scale;
            new.sy3 = m3::remove(new.n_f64(), new.sy, new.sy2, self.sy3, y);
            new.sy4 = m4::remove(new.n_f64(), new.sy, new.sy2, new.sy3, self.sy4, y);

            new.sxy = self.sxy - tmpx * tmpy * scale;
            *self = new;
        }

        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use datafusion::arrow::array::PrimitiveArray;
    use datafusion::arrow::datatypes::Int32Type;
    use datafusion::common::ScalarValue;
    use datafusion::physical_plan::Accumulator;

    use crate::extension::expr::aggregate_function::stats_agg::stats_agg_accumulator_2d::StatsAggAccumulator2D;

    #[test]
    fn test_serialize_stats_agg() {
        let stats_agg = StatsAggAccumulator2D::default();
        let value: ScalarValue = stats_agg.try_into().unwrap();
        let new_stats_agg = StatsAggAccumulator2D::try_from(value).unwrap();
        assert_eq!(stats_agg, new_stats_agg);
    }

    #[test]
    fn test_stats_agg_update() {
        let mut stats_agg = StatsAggAccumulator2D::default();
        let x = vec![1, 1, 1, 1, 1, 2, 2, 2, 2, 2];
        let y = vec![1, 2, 3, 4, 5, 1, 2, 3, 4, 5];
        let x = x.into_iter().collect::<PrimitiveArray<Int32Type>>();
        let y = y.into_iter().collect::<PrimitiveArray<Int32Type>>();
        stats_agg.update_batch(&[Arc::new(y), Arc::new(x)]).unwrap();
        assert_eq!(
            stats_agg,
            StatsAggAccumulator2D {
                n: 10,
                sx: 15.0,
                sx2: 2.5,
                sx3: -2.7755575615628914e-16,
                sx4: 0.6249999999999999,
                sy: 30.0,
                sy2: 20.0,
                sy3: -1.7763568394002505e-15,
                sy4: 68.0,
                sxy: 0.0,
            }
        );
        let value: ScalarValue = stats_agg.try_into().unwrap();
        let new_stats_agg = StatsAggAccumulator2D::try_from(value).unwrap();
        assert_eq!(stats_agg, new_stats_agg);
    }
}
