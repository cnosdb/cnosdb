use std::borrow::Cow;
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
    pub name: Cow<'static, str>,
    pub description: Cow<'static, str>,
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
    /// Create a new metric with the given name, description, and options.
    pub fn new(
        name: impl Into<Cow<'static, str>>,
        description: impl Into<Cow<'static, str>>,
        options: T::Options,
    ) -> Metric<T> {
        Self {
            name: name.into(),
            description: description.into(),
            labels: Labels::default(),
            metric_type: T::metric_type(),
            shard: Arc::new(MetricShared::new(options)),
        }
    }

    /// Create a new metric with the given name, description, labels, and options.
    pub fn new_with_labels(
        name: impl Into<Cow<'static, str>>,
        description: impl Into<Cow<'static, str>>,
        labels: impl Into<Labels>,
        options: T::Options,
    ) -> Metric<T> {
        Self {
            name: name.into(),
            description: description.into(),
            labels: labels.into(),
            metric_type: T::metric_type(),
            shard: Arc::new(MetricShared::new(options)),
        }
    }

    /// Get the recorder of the metric with the given labels.
    /// If the recorder of the given labels does not exist, register a new one and return it.
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

    /// Register a recorder with the given labels.
    /// If the recorder of the given labels already exists, it will be replaced.
    pub fn register_recorder(&self, labels: impl Into<Labels>, recorder: T) {
        let mut guard = self.shard.values.lock();
        guard.insert(labels.into(), recorder);
    }

    /// Remove the recorder with the given labels.
    pub fn remove(&self, labels: impl Into<Labels>) {
        let mut guard = self.shard.values.lock();
        guard.remove(&labels.into());
    }
}
