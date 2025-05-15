use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::rc::Rc;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature};
use models::arrow::DataType;
use serde::Deserialize;

use crate::extension::expr::ts_gen_func::utils::{full_signatures, get_arg};

pub struct ValueRepairFunc {
    signature: Signature,
}

impl ValueRepairFunc {
    pub fn new() -> Self {
        Self {
            signature: full_signatures(),
        }
    }
}

impl ScalarUDFImpl for ValueRepairFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "value_repair"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::List(Arc::new(arg_types[1].clone())))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
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
    let method = get_method_from_arg(&arg)?;
    let repaired = value_repair(timestamps, &mut fields[0], method)?;
    Ok((std::mem::take(timestamps), repaired))
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct Arg {
    method: Option<String>,
    min_speed: Option<f64>,
    max_speed: Option<f64>,
    center: Option<f64>,
    sigma: Option<f64>,
}

enum RepairMethod {
    Screen {
        smin: Option<f64>,
        smax: Option<f64>,
    },
    LsGreedy {
        center: Option<f64>,
        sigma: Option<f64>,
    },
}

fn get_method_from_arg(arg: &Arg) -> DFResult<RepairMethod> {
    Ok(match &arg.method {
        Some(method) => match method.to_ascii_lowercase().as_str() {
            "screen" => RepairMethod::Screen {
                smin: arg.min_speed,
                smax: arg.max_speed,
            },
            "lsgreedy" => RepairMethod::LsGreedy {
                center: arg.center,
                sigma: arg.sigma,
            },
            _ => Err(datafusion::error::DataFusionError::Execution(format!(
                "Invalid method: {method}"
            )))?,
        },
        None => RepairMethod::Screen {
            smin: arg.min_speed,
            smax: arg.max_speed,
        },
    })
}

fn value_repair(
    timestamps: &[i64],
    values: &mut [f64],
    method: RepairMethod,
) -> DFResult<Vec<f64>> {
    process_nan(timestamps, values)?;
    let values_repaired = match method {
        RepairMethod::Screen { smin, smax } => screen(timestamps, values, smin, smax),
        RepairMethod::LsGreedy { center, sigma } => lsgreedy(timestamps, values, center, sigma),
    };

    Ok(values_repaired)
}

fn interval_median(timestamps: &[i64]) -> i64 {
    let mut interval: Vec<i64> = Vec::new();
    let n: usize = timestamps.len();
    for i in 1..n {
        interval.push(timestamps[i] - timestamps[i - 1]);
    }
    interval.sort();
    if n % 2 == 0 {
        (interval[n / 2 - 1] + interval[n / 2]) / 2
    } else {
        interval[n / 2]
    }
}

fn f64_median(values: &[f64]) -> f64 {
    let mut values_clone = values.to_vec();
    values_clone.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n: usize = values_clone.len();
    if n % 2 == 0 {
        (values_clone[n / 2 - 1] + values_clone[n / 2]) / 2.0
    } else {
        values_clone[n / 2]
    }
}

fn mad(values: &[f64]) -> f64 {
    let mid: f64 = f64_median(values);
    let mut mad: Vec<f64> = Vec::new();
    for &v in values.iter() {
        mad.push((v - mid).abs());
    }
    1.4826 * f64_median(&mad)
}

fn speed(timestamps: &[i64], values: &[f64]) -> Vec<f64> {
    let n: usize = values.len();
    let mut speed: Vec<f64> = Vec::new();
    for i in 0..n - 1 {
        speed.push((values[i + 1] - values[i]) / (timestamps[i + 1] - timestamps[i]) as f64);
    }
    speed
}

fn process_nan(timestamps: &[i64], values: &mut [f64]) -> DFResult<()> {
    let n = values.len();
    let mut index1 = 0;
    let mut index2;
    while index1 < n && !values[index1].is_finite() {
        index1 += 1;
    }
    index2 = index1 + 1;
    while index2 < n && !values[index2].is_finite() {
        index2 += 1;
    }
    if index2 > n {
        return Err(datafusion::error::DataFusionError::Execution(
            "At least two non-NaN values are needed".to_string(),
        ));
    }
    for i in 0..index2 {
        values[i] = values[index1]
            + (values[index2] - values[index1])
                * ((timestamps[i] - timestamps[index1]) as f64
                    / (timestamps[index2] - timestamps[index1]) as f64);
    }
    for i in index2 + 1..n {
        if values[i].is_finite() {
            index1 = index2;
            index2 = i;
            for j in index1 + 1..index2 {
                values[j] = values[index1]
                    + (values[index2] - values[index1])
                        * ((timestamps[j] - timestamps[index1]) as f64
                            / (timestamps[index2] - timestamps[index1]) as f64);
            }
        }
    }
    for i in index2 + 1..n {
        values[i] = values[index1]
            + (values[index2] - values[index1])
                * ((timestamps[i] - timestamps[index1]) as f64
                    / (timestamps[index2] - timestamps[index1]) as f64);
    }
    Ok(())
}

fn screen(timestamps: &[i64], values: &[f64], smin: Option<f64>, smax: Option<f64>) -> Vec<f64> {
    let w: i64 = 5 * interval_median(timestamps);
    let speed: Vec<f64> = speed(timestamps, values);
    let sigma: f64 = mad(&speed);
    let mid: f64 = f64_median(&speed);
    let smin = smin.unwrap_or(mid - 3.0 * sigma);
    let smax = smax.unwrap_or(mid + 3.0 * sigma);
    let mut values_repaired = vec![0.0; values.len()];

    let mut ans: Vec<(i64, f64)> = Vec::new();
    ans.push((timestamps[0], values[0]));

    let mut start_index: usize = 0;
    let n: usize = timestamps.len();
    for i in 1..n {
        ans.push((timestamps[i], values[i]));
        while ans[start_index].0 + w < ans[i].0 {
            local(&mut ans, start_index, w, smin, smax);
            start_index += 1;
        }
    }

    while start_index < n {
        local(&mut ans, start_index, w, smin, smax);
        start_index += 1;
    }

    for k in 0..ans.len() {
        values_repaired[k] = ans[k].1;
    }

    values_repaired
}

fn local(ans: &mut [(i64, f64)], start_index: usize, w: i64, smin: f64, smax: f64) {
    let mid: f64 = get_median(ans, start_index, w, smin, smax);
    if start_index == 0 {
        ans[start_index] = (ans[start_index].0, mid);
    } else {
        let temp: f64 = get_repaired_value(ans, start_index, mid, smin, smax);
        ans[start_index] = (ans[start_index].0, temp);
    }
}

fn get_median(ans: &[(i64, f64)], start_index: usize, w: i64, smin: f64, smax: f64) -> f64 {
    let mut m: usize = 0;
    while start_index + m + 1 < ans.len() && ans[start_index + m + 1].0 <= ans[start_index].0 + w {
        m += 1;
    }
    let mut x: Vec<f64> = vec![0.0; 2 * m + 1];
    x[0] = ans[start_index].1;
    for i in 1..=m {
        x[i] = ans[start_index + i].1 + smin * (ans[start_index].0 - ans[start_index + i].0) as f64;
        x[i + m] =
            ans[start_index + i].1 + smax * (ans[start_index].0 - ans[start_index + i].0) as f64;
    }
    x.sort_by(|a, b| a.partial_cmp(b).unwrap());
    x[m]
}

fn get_repaired_value(
    ans: &[(i64, f64)],
    start_index: usize,
    mid: f64,
    smin: f64,
    smax: f64,
) -> f64 {
    let xmin: f64 =
        ans[start_index - 1].1 + smin * (ans[start_index].0 - ans[start_index - 1].0) as f64;
    let xmax: f64 =
        ans[start_index - 1].1 + smax * (ans[start_index].0 - ans[start_index - 1].0) as f64;
    let mut temp: f64 = mid;
    temp = xmax.min(temp);
    temp = xmin.max(temp);
    temp
}

fn lsgreedy(
    timestamps: &[i64],
    values: &[f64],
    center: Option<f64>,
    sigma: Option<f64>,
) -> Vec<f64> {
    let speed: Vec<f64> = speed(timestamps, values);
    let speedchange: Vec<f64> = variation(&speed);

    let center: f64 = center.unwrap_or(0.0);
    let sigma: f64 = sigma.unwrap_or(mad(&speedchange));
    let epsilon: f64 = 1e-12;

    let mut values_repaired = values.to_vec();
    //n需要商榷
    let n: usize = values.len();
    let mut table = Vec::new();
    let mut heap = BinaryHeap::new();
    table.push(Rc::new(RepairNode {
        index: 0,
        u: 0.0,
        center: 0.0,
    }));

    for i in 1..n - 1 {
        let node = Rc::new(RepairNode::new(i, timestamps, &values_repaired, center));
        table.push(node.clone());
        if (node.get_u() - node.center).abs() > 3.0 * sigma {
            heap.push(Reverse(node));
        }
    }

    while let Some(Reverse(top)) = heap.peek() {
        if (top.get_u() - center).abs() < epsilon.max(3.0 * sigma) {
            break;
        }
        top.modify(timestamps, &mut values_repaired, center, sigma, epsilon);
        for (i, v) in table
            .iter_mut()
            .enumerate()
            .take((n - 2).min(top.get_index() + 1) + 1)
            .skip(1.max(top.get_index() - 1))
        {
            heap.retain(|Reverse(node)| !Rc::ptr_eq(node, v));
            let temp = Rc::new(RepairNode::new(i, timestamps, &values_repaired, center));
            *v = temp.clone();
            if (temp.get_u() - center).abs() > 3.0 * sigma {
                heap.push(Reverse(temp));
            }
        }
    }
    values_repaired
}

fn variation(values: &[f64]) -> Vec<f64> {
    let n: usize = values.len();
    let mut variance: Vec<f64> = Vec::new();
    for i in 0..n - 1 {
        variance.push(values[i + 1] - values[i]);
    }
    variance
}

struct RepairNode {
    index: usize,
    u: f64,
    center: f64,
}

impl RepairNode {
    fn new(index: usize, timestamps: &[i64], values_repaired: &[f64], center: f64) -> RepairNode {
        let v1: f64 = (values_repaired[index + 1] - values_repaired[index])
            / (timestamps[index + 1] - timestamps[index]) as f64;
        let v2: f64 = (values_repaired[index] - values_repaired[index - 1])
            / (timestamps[index] - timestamps[index - 1]) as f64;
        RepairNode {
            index,
            u: v1 - v2,
            center,
        }
    }

    fn modify(
        &self,
        timestamps: &[i64],
        values_repaired: &mut [f64],
        center: f64,
        sigma: f64,
        epsilon: f64,
    ) {
        let mut temp: f64;
        if sigma < epsilon {
            temp = (self.u - center).abs();
        } else {
            temp = sigma.max(((self.u - center) / 3.0).abs());
        }
        temp *= (timestamps[self.index + 1] - timestamps[self.index]) as f64
            * (timestamps[self.index] - timestamps[self.index - 1]) as f64
            / (timestamps[self.index + 1] - timestamps[self.index - 1]) as f64;
        if self.u > center {
            values_repaired[self.index] += temp;
        } else {
            values_repaired[self.index] -= temp;
        }
    }

    fn get_index(&self) -> usize {
        self.index
    }

    fn get_u(&self) -> f64 {
        self.u
    }
}

impl PartialOrd for RepairNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RepairNode {
    fn cmp(&self, other: &Self) -> Ordering {
        let u1 = (self.u - self.center).abs();
        let u2 = (other.u - self.center).abs();

        if u1 > u2 {
            Ordering::Less // Return Less because we want higher values to be at the top of the heap
        } else if u1 == u2 {
            Ordering::Equal
        } else {
            Ordering::Greater // Return Greater because we want lower values to be at the top of the heap
        }
    }
}

impl PartialEq for RepairNode {
    fn eq(&self, other: &Self) -> bool {
        // 比较两个 RepairNode 是否相等
        self.index == other.index && (self.u - other.u).abs() < f64::EPSILON
    }
}

impl Eq for RepairNode {}

impl Clone for RepairNode {
    fn clone(&self) -> Self {
        RepairNode {
            index: self.index,
            u: self.u,
            center: self.center,
        }
    }
}
