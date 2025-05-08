use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use models::arrow::DataType;
use serde::Deserialize;
use spi::DFResult;

use crate::extension::expr::scalar_function::unimplemented_scalar_impl;
use crate::extension::expr::ts_gen_func::utils::{full_signatures, get_arg};

pub struct TimestampRepairFunc {
    signature: Signature,
}

impl TimestampRepairFunc {
    pub fn new() -> Self {
        Self {
            signature: full_signatures(),
        }
    }
}

impl ScalarUDFImpl for TimestampRepairFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "timestamp_repair"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::List(Arc::new(arg_types[1].clone())))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        unimplemented_scalar_impl(self.name())
    }
}

pub fn compute(
    timestamps: &mut [i64],
    fields: &mut [Vec<f64>],
    arg_str: Option<&str>,
) -> DFResult<(Vec<i64>, Vec<f64>)> {
    let arg = get_arg(arg_str)?;
    let (interval_mode, start_mode) = get_interval_mode_and_start_mode_from_arg(&arg)?;
    let (timestamps_repaired, values_repaired) =
        timestamps_repair(timestamps, &mut fields[0], interval_mode, start_mode);
    Ok((timestamps_repaired, values_repaired))
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct Arg {
    method: Option<String>,
    interval: Option<i64>,
    start_mode: Option<String>,
}

enum IntervalMode {
    Mode,
    Cluster,
    Median,
    Interval(i64),
}

impl IntervalMode {
    pub fn try_from_str(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "mode" => Some(IntervalMode::Mode),
            "cluster" => Some(IntervalMode::Cluster),
            "median" => Some(IntervalMode::Median),
            _ => None,
        }
    }
}

enum StartMode {
    Linear,
    Mode,
}

impl StartMode {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "linear" => Some(StartMode::Linear),
            "mode" => Some(StartMode::Mode),
            _ => None,
        }
    }
}

fn get_interval_mode_and_start_mode_from_arg(arg: &Arg) -> DFResult<(IntervalMode, StartMode)> {
    let start_mode = if let Some(s) = &arg.start_mode {
        StartMode::from_str(s).ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!("Invalid start_mode: {}", s))
        })?
    } else {
        StartMode::Mode
    };

    let interval_mode = if let Some(i) = arg.interval {
        if i < 0 {
            return Err(datafusion::error::DataFusionError::Execution(
                "interval must be positive".to_string(),
            ));
        }
        IntervalMode::Interval(i)
    } else if let Some(m) = &arg.method {
        IntervalMode::try_from_str(m).ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!("Invalid method: {}", m))
        })?
    } else {
        IntervalMode::Median
    };

    Ok((interval_mode, start_mode))
}

#[derive(Clone, PartialEq)]
pub enum Op {
    Nothing,
    Insert,
    Remove,
}

fn timestamps_repair(
    timestamps: &[i64],
    values: &mut [f64],
    interval_mode: IntervalMode,
    start_mode: StartMode,
) -> (Vec<i64>, Vec<f64>) {
    for v in values.iter_mut() {
        if !v.is_finite() {
            *v = f64::NAN;
        }
    }

    let interval: i64 = match interval_mode {
        IntervalMode::Cluster => get_interval_cluster(timestamps),
        IntervalMode::Mode => get_interval_mode(timestamps),
        IntervalMode::Median => get_interval_median(timestamps),
        IntervalMode::Interval(interval) => interval * 1_000_000,
    };
    let start: i64 = match start_mode {
        StartMode::Linear => getstart_linear(timestamps, interval),
        StartMode::Mode => getstart_mode(timestamps, interval),
    };

    dp_repair(start, interval, timestamps, values)
}

fn dp_repair(
    start: i64,
    interval: i64,
    timestamps_i64: &[i64],
    values_f64: &[f64],
) -> (Vec<i64>, Vec<f64>) {
    if timestamps_i64.len() <= 2 {
        return (timestamps_i64.to_vec(), values_f64.to_vec());
    }
    let n: usize = ((timestamps_i64[timestamps_i64.len() - 1] - start) as f64 / interval as f64
        + 1.0)
        .ceil() as usize;
    let m = timestamps_i64.len();
    let mut timestamps_repaired = vec![0; n];
    let mut values_repaired = vec![0.0; n];
    let addcost = 100_000_000_000;
    let mut f: Vec<Vec<i64>> = vec![vec![0; m + 1]; n + 1];
    let mut steps: Vec<Vec<Op>> = vec![vec![Op::Nothing; m + 1]; n + 1];
    for i in 0..n + 1 {
        f[i][0] = addcost * i as i64;
        steps[i][0] = Op::Insert;
    }
    for i in 0..m + 1 {
        f[0][i] = addcost * i as i64;
        steps[0][i] = Op::Remove;
    }
    for i in 1..n + 1 {
        for j in 1..m + 1 {
            if timestamps_i64[j - 1] == start + interval * (i - 1) as i64 {
                f[i][j] = f[i - 1][j - 1];
                steps[i][j] = Op::Nothing;
            } else {
                if f[i - 1][j] < f[i][j - 1] {
                    f[i][j] = f[i - 1][j] + addcost;
                    steps[i][j] = Op::Insert;
                } else {
                    f[i][j] = f[i][j - 1] + addcost;
                    steps[i][j] = Op::Remove;
                }
                let modify_result = f[i - 1][j - 1]
                    + (timestamps_i64[j - 1] - start - (i - 1) as i64 * interval).abs();
                if modify_result < f[i][j] {
                    f[i][j] = modify_result;
                    steps[i][j] = Op::Nothing;
                }
            }
        }
    }
    let mut i: usize = n;
    let mut j: usize = m;
    while i >= 1 && j >= 1 {
        let ps: i64 = start + interval * (i - 1) as i64;
        if steps[i][j] == Op::Nothing {
            timestamps_repaired[i - 1] = ps;
            values_repaired[i - 1] = values_f64[j - 1];
            i -= 1;
            j -= 1;
        } else if steps[i][j] == Op::Insert {
            timestamps_repaired[i - 1] = ps;
            values_repaired[i - 1] = f64::NAN;
            i -= 1;
        } else {
            j -= 1;
        }
    }

    (timestamps_repaired, values_repaired)
}

fn getstart_linear(timestamps_f64: &[i64], delta: i64) -> i64 {
    let mut sum: i128 = 0;
    for (i, &v) in timestamps_f64.iter().enumerate() {
        sum += v as i128;
        sum -= i as i128 * delta as i128;
    }
    (sum / timestamps_f64.len() as i128) as i64
}

fn getstart_mode(timestamps_i64: &[i64], delta: i64) -> i64 {
    let mut count = HashMap::new();
    let mut timestamps_modn = Vec::with_capacity(timestamps_i64.len());
    for &v in timestamps_i64 {
        let temp = v % delta;
        timestamps_modn.push(temp);
        *count.entry(temp).or_insert(0) += 1;
    }
    let mut max_key = 0;
    let mut max: u32 = 0;
    for (&key, &times) in &count {
        if max < times {
            max = times;
            max_key = key;
        }
    }
    let mut result: i64 = 0;
    for i in 0..timestamps_modn.len() {
        if timestamps_modn[i] == max_key {
            result = timestamps_i64[i];
        }
    }
    let first_item: i64 = timestamps_i64[0];
    while result > first_item {
        result -= delta;
    }
    result
}

fn k_means_clustering(data: &[i64], k: usize) -> i64 {
    let mut means = Vec::with_capacity(k);
    let mut results = vec![0; data.len()];
    let mut cnts = vec![0; k];
    let mut cluster_samples: HashMap<usize, Vec<i64>> = HashMap::new();
    // 使用等间距值初始化means
    let max_val = data.iter().max().cloned().unwrap_or(0);
    let min_val = data.iter().min().cloned().unwrap_or(0);
    for i in 0..k {
        means.push(min_val + (i + 1) as i64 * (max_val - min_val) / (k + 1) as i64);
    }
    let mut changed = true;
    while changed {
        changed = false;
        // 将每个数据点分配到最近的平均值
        for i in 0..data.len() {
            let mut min_dis = i64::MAX;
            let mut min_dis_id = 0;
            for (j, &v) in means.iter().enumerate().take(k) {
                let distance = (data[i] - v).abs();
                if distance < min_dis {
                    min_dis = distance;
                    min_dis_id = j;
                }
            }
            if min_dis_id != results[i] {
                changed = true;
                results[i] = min_dis_id;
            }
        }
        // 清除之前的聚类样本
        cluster_samples.clear();
        // 更新聚类样本
        for (i, &result) in results.iter().enumerate() {
            cluster_samples.entry(result).or_default().push(data[i]);
        }
        // 重新计算平均值
        for i in 0..k {
            let mut sum = 0;
            cnts[i] = 0;
            for &sample in cluster_samples.get(&i).unwrap_or(&vec![]) {
                sum += sample;
                cnts[i] += 1;
            }
            if cnts[i] != 0 {
                means[i] = sum / cnts[i] as i64;
            }
        }
    }
    // 找到数量最大的聚类
    let max_cluster_id = cnts
        .iter()
        .enumerate()
        .max_by_key(|&(_, count)| count)
        .map(|(id, _)| id)
        .unwrap_or(0);

    // 返回数量最大的聚类的数的平均值
    if let Some(samples) = cluster_samples.get(&max_cluster_id) {
        samples.iter().sum::<i64>() / samples.len() as i64
    } else {
        0
    }
}

fn get_interval_cluster(timestamps_i64: &[i64]) -> i64 {
    let size: usize = timestamps_i64.len();
    let mut intervals: Vec<i64> = Vec::with_capacity(size - 1);
    for i in 0..size - 1 {
        intervals.push(timestamps_i64[i + 1] - timestamps_i64[i]);
    }
    k_means_clustering(&intervals, 3)
}

fn get_interval_mode(timestamps_i64: &[i64]) -> i64 {
    let mut count = HashMap::new();
    for i in 0..timestamps_i64.len() - 1 {
        *count
            .entry(timestamps_i64[i + 1] - timestamps_i64[i])
            .or_insert(0) += 1;
    }
    let mut max_key = 0;
    let mut max: u32 = 0;
    for (&key, &times) in &count {
        if max < times {
            max = times;
            max_key = key;
        }
    }
    max_key
}

fn get_interval_median(timestamps_i64: &[i64]) -> i64 {
    let size: usize = timestamps_i64.len();
    let mut intervals: Vec<i64> = Vec::with_capacity(size - 1);
    for i in 0..size - 1 {
        intervals.push(timestamps_i64[i + 1] - timestamps_i64[i]);
    }
    intervals.sort_unstable();
    if size % 2 == 0 {
        (intervals[size / 2 - 1] + intervals[size / 2]) / 2
    } else {
        intervals[size / 2]
    }
}
