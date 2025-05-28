use std::sync::Arc;

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature};
use models::arrow::{DataType, Field};
use serde::Deserialize;

use crate::extension::expr::ts_gen_func::utils::{full_signatures, get_arg};

#[derive(Debug)]
pub struct ValueFillFunc {
    signature: Signature,
}

impl ValueFillFunc {
    pub fn new() -> Self {
        Self {
            signature: full_signatures(),
        }
    }
}

impl ScalarUDFImpl for ValueFillFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "value_fill"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::List(Arc::new(Field::new_list_field(
            arg_types[1].clone(),
            true,
        ))))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        Err(DataFusionError::Plan(format!(
            "{} can only be used in the SELECT clause as the top-level expression",
            self.name()
        )))
    }
}

pub fn compute(
    timestamps: &mut Vec<i64>,
    fields: &mut [Vec<f64>],
    arg_str: Option<&str>,
) -> DFResult<(Vec<i64>, Vec<f64>)> {
    let arg = get_arg(arg_str)?;
    let method = get_fill_method_from_arg(arg)?;
    let field = std::mem::take(&mut fields[0]);
    let mut fill = ValueFill::new(field, method)?;
    fill.fill()?;
    Ok((std::mem::take(timestamps), fill.into_filled_values()))
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct Arg {
    method: Option<String>,
}

enum FillMethod {
    Mean,
    Previous,
    Linear,
    AR,
    MA,
}

impl FillMethod {
    pub fn try_from_str(method: &str) -> Option<Self> {
        match method.to_ascii_lowercase().as_str() {
            "mean" => Some(Self::Mean),
            "previous" => Some(Self::Previous),
            "linear" => Some(Self::Linear),
            "ar" => Some(Self::AR),
            "ma" => Some(Self::MA),
            _ => None,
        }
    }
}

fn get_fill_method_from_arg(arg: Arg) -> DFResult<FillMethod> {
    Ok(match arg.method.as_deref() {
        Some(s) => FillMethod::try_from_str(s).ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!("Invalid fill method: {}", s))
        })?,
        None => FillMethod::Linear,
    })
}

struct ValueFill {
    original: Vec<f64>,
    repaired: Vec<f64>,
    not_nan: usize,
    method: FillMethod,
}

impl ValueFill {
    pub fn new(mut values: Vec<f64>, method: FillMethod) -> DFResult<Self> {
        let n = values.len();
        let mut not_nan = 0;

        for v in values.iter_mut() {
            if !v.is_finite() {
                *v = f64::NAN;
            } else {
                not_nan += 1;
            }
        }

        if not_nan == 0 {
            return Err(datafusion::error::DataFusionError::Execution(
                "All values are Invalid".to_string(),
            ));
        }

        Ok(Self {
            original: values,
            repaired: Vec::with_capacity(n),
            not_nan,
            method,
        })
    }

    pub fn into_filled_values(self) -> Vec<f64> {
        self.repaired
    }

    pub fn fill(&mut self) -> DFResult<()> {
        match self.method {
            FillMethod::Mean => self.fill_mean(),
            FillMethod::Previous => self.fill_previous(),
            FillMethod::Linear => self.fill_linear(),
            FillMethod::AR => self.fill_ar()?,
            FillMethod::MA => self.fill_ma(),
        }
        Ok(())
    }

    fn fill_mean(&mut self) {
        let mean =
            self.original.iter().filter(|v| v.is_finite()).sum::<f64>() / self.not_nan as f64;
        for v in self.original.iter() {
            if v.is_finite() {
                self.repaired.push(*v);
            } else {
                self.repaired.push(mean);
            }
        }
    }

    fn fill_previous(&mut self) {
        let mut last = f64::NAN;
        for v in self.original.iter() {
            if v.is_finite() {
                last = *v;
            }
            self.repaired.push(last);
        }
    }

    fn fill_linear(&mut self) {
        let mut prev_not_nan = usize::MAX;
        let repaired = self.repaired.spare_capacity_mut();
        for (i, &v) in self.original.iter().enumerate() {
            if !v.is_nan() {
                let mut k = 0.0;
                if prev_not_nan != usize::MAX {
                    k = v - self.original[prev_not_nan];
                    k /= (i - prev_not_nan) as f64;
                }
                let mut t = prev_not_nan.wrapping_add(1);
                while t < i {
                    repaired[t].write(v + k * (t as i64 - i as i64) as f64);
                    t += 1;
                }
                repaired[i].write(v);
                prev_not_nan = i;
            }
        }
        if prev_not_nan < self.original.len() - 1 {
            let mut t = prev_not_nan;
            while t < self.original.len() - 1 {
                repaired[t].write(self.original[prev_not_nan]);
                t += 1;
            }
        }
        unsafe { self.repaired.set_len(self.original.len()) };
    }

    fn fill_ar(&mut self) -> DFResult<()> {
        let mean =
            self.original.iter().filter(|v| v.is_finite()).sum::<f64>() / self.not_nan as f64;
        let mut theta = 0.1;
        let mut acf = 0.0;
        let mut factor = 0.0;
        let mut window_iter = self.original.windows(2);
        while let Some(&[mut left, mut right]) = window_iter.next() {
            if left.is_nan() {
                left = 0.0;
            }
            if right.is_nan() {
                right = 0.0;
            }
            acf += left * right;
            factor += left * left;
        }
        if factor == 0.0 || theta >= 1.0 {
            return Err(datafusion::error::DataFusionError::Execution(
                "Cannot fit AR(1) model. Please try another method.".to_string(),
            ));
        }
        theta = acf / factor;
        let mut mean_epsilon = 0.0;
        let mut cnt_epsilon = 0.0;
        let mut window_iter = self.original.windows(2);
        while let Some(&[left, right]) = window_iter.next() {
            if left.is_nan() || right.is_nan() {
                continue;
            }
            cnt_epsilon += 1.0;
            let epsilon = right - theta * left;
            mean_epsilon += epsilon;
        }
        if cnt_epsilon == 0.0 {
            return Err(datafusion::error::DataFusionError::Execution(
                "Cannot fit AR(1) model. Please try another method.".to_string(),
            ));
        }
        mean_epsilon /= cnt_epsilon;
        for (i, &v) in self.original.iter().enumerate() {
            if v.is_nan() {
                self.repaired.push(if i != 0 {
                    theta * self.repaired[i - 1] + mean_epsilon
                } else {
                    mean
                });
            } else {
                self.repaired.push(v);
            }
        }

        Ok(())
    }

    fn fill_ma(&mut self) {
        let window_size = 5;
        let mut window_sum = 0.0;
        let mut window_cnt = 0;
        let mut l = 0;
        let mut r = window_size - 1;

        for i in l..r.min(self.original.len()) {
            if !self.original[i].is_nan() {
                window_sum += self.original[i];
                window_cnt += 1;
            }
        }
        for (i, &v) in self.original.iter().enumerate() {
            if !v.is_nan() {
                self.repaired.push(v);
            } else {
                self.repaired.push(window_sum / window_cnt as f64);
            }
            if i <= (window_size - 1) / 2 || i >= self.original.len() - (window_size - 1) / 2 - 1 {
                continue;
            }
            if !self.original[r].is_nan() {
                window_sum += self.original[r];
                window_cnt += 1;
            }
            l += 1;
            r += 1;
        }
    }
}
