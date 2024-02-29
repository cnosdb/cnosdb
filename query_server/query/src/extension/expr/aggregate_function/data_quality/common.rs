use datafusion::error::DataFusionError;

use crate::extension::expr::aggregate_function::{
    COMPLETENESS_UDF_NAME, CONSISTENCY_UDF_NAME, TIMELINESS_UDF_NAME, VALIDITY_UDF_NAME,
};

#[derive(Debug, Clone, Copy)]
pub(super) enum DataQualityFunction {
    Completeness,
    Consistency,
    Timeliness,
    Validity,
}

impl DataQualityFunction {
    pub fn name(&self) -> &'static str {
        match self {
            DataQualityFunction::Completeness => COMPLETENESS_UDF_NAME,
            DataQualityFunction::Consistency => CONSISTENCY_UDF_NAME,
            DataQualityFunction::Timeliness => TIMELINESS_UDF_NAME,
            DataQualityFunction::Validity => VALIDITY_UDF_NAME,
        }
    }
}

pub(super) struct DataSeriesQuality {
    times: Vec<f64>,
    values: Vec<f64>,
    cnt: u32,
    misscnt: u32,
    specialcnt: u32,
    latecnt: u32,
    redundancycnt: u32,
    valuecnt: u32,
    variationcnt: u32,
    speedcnt: u32,
    speedchangecnt: u32,
}

impl DataSeriesQuality {
    pub fn new(times: Vec<f64>, mut values: Vec<f64>) -> datafusion::error::Result<Self> {
        let cnt = times.len() as u32;
        let mut specialcnt = 0;
        for v in values.iter_mut() {
            if !v.is_finite() {
                *v = f64::NAN;
                specialcnt += 1;
            }
        }
        let mut res = Self {
            times,
            values,
            cnt,
            misscnt: 0,
            specialcnt,
            latecnt: 0,
            redundancycnt: 0,
            valuecnt: 0,
            variationcnt: 0,
            speedcnt: 0,
            speedchangecnt: 0,
        };
        res.nan_process()?;

        Ok(res)
    }

    pub fn nan_process(&mut self) -> datafusion::error::Result<()> {
        let n = self.values.len();
        let mut index1: usize = 0;
        while index1 < n && self.values[index1].is_nan() {
            index1 += 1;
        }
        let mut index2 = index1 + 1;
        while index2 < n && self.values[index2].is_nan() {
            index2 += 1;
        }
        if index2 >= n {
            return Err(DataFusionError::Execution(
                "At least two non-NaN values are needed".to_string(),
            ));
        }
        for i in 0..index2 {
            self.values[i] = self.values[index1]
                + (self.values[index2] - self.values[index1])
                    * ((self.times[i] - self.times[index1])
                        / (self.times[index2] - self.times[index1]));
        }
        for i in index2 + 1..n {
            if !self.values[i].is_nan() {
                index1 = index2;
                index2 = i;
                for j in index1 + 1..index2 {
                    self.values[j] = self.values[index1]
                        + (self.values[index2] - self.values[index1])
                            * ((self.times[j] - self.times[index1])
                                / (self.times[index2] - self.times[index1]));
                }
            }
        }
        for i in index2 + 1..n {
            self.values[i] = self.values[index1]
                + (self.values[index2] - self.values[index1])
                    * ((self.times[i] - self.times[index1])
                        / (self.times[index2] - self.times[index1]));
        }
        Ok(())
    }

    pub fn time_detect(&mut self) {
        const WINDOW_SIZE: usize = 10;
        let mut interval: Vec<f64> = vec![0.0; self.times.len() - 1];
        for (k, v) in interval.iter_mut().enumerate() {
            *v = self.times[k + 1] - self.times[k];
        }
        let base: f64 = median(&interval);

        let mut window: Vec<f64> = vec![];

        let mut i: usize = 0;
        while i < std::cmp::min(WINDOW_SIZE, self.times.len()) {
            window.push(self.times[i]);
            i += 1;
        }

        while window.len() > 1 {
            let times: f64 = (window[1] - window[0]) / base;
            if times <= 0.5 {
                window.remove(1);
                self.redundancycnt += 1;
            } else if (2.0..=9.0).contains(&times) {
                let mut temp: u32 = 0;
                let mut j: usize = 2;
                while j < window.len() {
                    let times2: f64 = (window[j] - window[j - 1]) / base;
                    if times2 >= 2.0 {
                        break;
                    }
                    if times2 <= 0.5 {
                        temp += 1;
                        window.remove(j);
                        j -= 1;
                        if temp == (times - 1.0).round() as u32 {
                            break;
                        }
                    }
                    j += 1;
                }
                self.latecnt += temp;
                self.misscnt += (times - 1.0).round() as u32 - temp;
            }
            window.remove(0);
            while window.len() < WINDOW_SIZE && i < self.cnt as usize {
                window.push(self.times[i]);
                i += 1;
            }
        }
    }

    pub fn findoutliers(val: &[f64], k: f64) -> u32 {
        let mid: f64 = median(val);
        let sigma: f64 = mad(val);
        let mut num: u32 = 0;
        for v in val {
            if (*v - mid).abs() > k * sigma {
                num += 1;
            }
        }
        num
    }

    pub fn speed(value: &[f64], time: &[f64]) -> Vec<f64> {
        let n = value.len();
        let mut speed: Vec<f64> = vec![];
        for i in 0..n - 1 {
            speed.push((value[i + 1] - value[i]) / (time[i + 1] - time[i]));
        }
        speed
    }

    pub fn value_detect(&mut self) {
        let k: f64 = 3.0;
        self.valuecnt = Self::findoutliers(&self.values, k);
        let mut variation: Vec<f64> = vec![];
        for i in 0..self.values.len() - 1 {
            variation.push(self.values[i + 1] - self.values[i]);
        }
        self.variationcnt = Self::findoutliers(&variation, k);
        let speed: Vec<f64> = Self::speed(&self.values, &self.times);
        self.speedcnt = Self::findoutliers(&speed, k);
        let mut speedchange: Vec<f64> = vec![];
        for i in 0..speed.len() - 1 {
            speedchange.push(speed[i + 1] - speed[i]);
        }
        self.speedchangecnt = Self::findoutliers(&speedchange, k)
    }

    pub fn completeness(&self) -> f64 {
        1.0 - ((self.misscnt + self.specialcnt) as f64 / (self.cnt + self.misscnt) as f64)
    }

    pub fn consistency(&self) -> f64 {
        1.0 - self.redundancycnt as f64 / self.cnt as f64
    }

    pub fn timeliness(&self) -> f64 {
        1.0 - self.latecnt as f64 / self.cnt as f64
    }

    pub fn validity(&self) -> f64 {
        1.0 - 0.25
            * (self.valuecnt + self.variationcnt + self.speedcnt + self.speedchangecnt) as f64
            / self.cnt as f64
    }
}

fn median(nums: &[f64]) -> f64 {
    let mut nums = nums.to_owned();
    if nums.len() == 1 {
        return nums[0];
    }
    nums.sort_unstable_by(|a, b| a.partial_cmp(b).expect("NaN should not appear here"));
    let len = nums.len();
    let mid = len / 2;
    if len % 2 == 0 {
        (nums[mid - 1] + nums[mid]) / 2.0
    } else {
        nums[mid]
    }
}

fn mad(val: &[f64]) -> f64 {
    let mid = median(val);
    let mut d: Vec<f64> = vec![];
    for v in val {
        d.push((*v - mid).abs());
    }
    1.4826 * median(&d)
}
