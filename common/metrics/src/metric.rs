use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::label::Labels;
use crate::metric_type::MetricType;
use crate::reporter::Reporter;
use crate::MetricRecorder;

#[derive(Debug, Clone)]
pub struct Metric<T: MetricRecorder> {
    pub name: &'static str,
    pub description: &'static str,
    pub labels: Labels,
    pub metric_type: MetricType,
    pub shard: Arc<MetricShared<T>>,
}

#[derive(Debug)]
pub struct MetricShared<T: MetricRecorder> {
    options: T::Options,
    values: Mutex<BTreeMap<Labels, T>>,
}

impl<T: MetricRecorder> MetricShared<T> {
    pub fn new(options: T::Options) -> Self {
        Self {
            options,
            values: Default::default(),
        }
    }

    pub fn report(&self, register_labels: &Labels, reporter: &mut dyn Reporter) {
        self.values
            .lock()
            .iter()
            .for_each(|(recorder_label, recorder)| {
                let mut metric_labels = register_labels.clone();
                metric_labels.extend(recorder_label.clone());
                reporter.report(&metric_labels, recorder.value())
            });
    }
}

unsafe impl<T: Clone + Default + MetricRecorder> Send for Metric<T> {}
unsafe impl<T: Clone + Default + MetricRecorder> Sync for Metric<T> {}

impl<T: MetricRecorder> Metric<T> {
    pub fn new(name: &'static str, description: &'static str, options: T::Options) -> Metric<T> {
        Self {
            name,
            description,
            labels: Labels::default(),
            metric_type: T::metric_type(),
            shard: Arc::new(MetricShared::new(options)),
        }
    }

    pub fn new_with_labels(
        name: &'static str,
        description: &'static str,
        labels: impl Into<Labels>,
        options: T::Options,
    ) -> Metric<T> {
        Self {
            name,
            description,
            labels: labels.into(),
            metric_type: T::metric_type(),
            shard: Arc::new(MetricShared::new(options)),
        }
    }

    pub fn recorder(&self, labels: impl Into<Labels>) -> T {
        let mut guard = self.shard.values.lock();
        match guard.entry(labels.into()) {
            Entry::Occupied(o) => o.get().clone(),
            Entry::Vacant(v) => {
                let res = T::create(&self.shard.options);
                v.insert(res.clone());
                res
            }
        }
    }

    pub fn register_recorder(&self, labels: impl Into<Labels>, recorder: T) {
        let mut guard = self.shard.values.lock();
        guard.insert(labels.into(), recorder);
    }
}
