use std::borrow::Cow;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::label::Labels;
use crate::metric::Metric;
use crate::reporter::Reporter;
use crate::{CreateMetricRecorder, Measure, MetricRecorder};

/// A registry for metrics.
#[derive(Debug)]
pub struct MetricsRegister {
    /// The external labels for metrics.
    /// Metrics and sub-registers in this register will inherit these labels.
    labels: Labels,

    /// Just report one time
    one_time: Mutex<Vec<Box<dyn Measure>>>,

    /// The metrics registered in this register.
    measures: Mutex<BTreeMap<Cow<'static, str>, Box<dyn Measure>>>,

    /// The sub-registers of this register.
    sub_register: Mutex<BTreeMap<Labels, Arc<MetricsRegister>>>,
}

unsafe impl Send for MetricsRegister {}
unsafe impl Sync for MetricsRegister {}

impl Default for MetricsRegister {
    fn default() -> Self {
        Self::new(Labels::default())
    }
}

impl MetricsRegister {
    /// Create a new register with external labels.
    pub fn new(labels: impl Into<Labels>) -> Self {
        Self {
            labels: labels.into(),
            measures: Default::default(),
            sub_register: Default::default(),
            one_time: Default::default(),
        }
    }

    pub fn labels(&self) -> Labels {
        self.labels.clone()
    }

    /// Create a sub-register with the given labels and the external labels.
    pub fn sub_register(&self, labels: impl Into<Labels>) -> Arc<MetricsRegister> {
        let mut register_labels = self.labels.clone();
        register_labels.extend(labels.into());
        let mut guard = self.sub_register.lock();
        match guard.get(&register_labels) {
            Some(r) => r.clone(),
            None => {
                let register = Arc::new(MetricsRegister::new(register_labels.clone()));
                guard.insert(register_labels, register.clone());
                register
            }
        }
    }

    /// Get all owned sub-registers of this register.
    pub fn sub_registers(&self) -> Vec<Arc<MetricsRegister>> {
        self.sub_register
            .lock()
            .iter()
            .map(|(_, v)| v.clone())
            .collect()
    }

    /// Register a metric with the default implementation of CreateMetricRecorder and return Metric<I>,
    /// type `<I>` could be `U64Counter`, `U64Gauge`, `DurationGauge`, `DurationCounter`.
    ///
    /// The external labels of the metric is inherited from the register.
    pub fn metric<I>(
        &self,
        name: impl Into<Cow<'static, str>>,
        description: impl Into<Cow<'static, str>>,
    ) -> Metric<I>
    where
        I: MetricRecorder + Clone + 'static + Default + CreateMetricRecorder<Options = ()>,
    {
        let mut measures = self.measures.lock();
        let name = name.into();
        match measures.entry(name.clone()) {
            Entry::Occupied(o) => o
                .get()
                .as_any()
                .downcast_ref::<Metric<I>>()
                .expect("measure type error")
                .clone(),
            Entry::Vacant(v) => {
                let res = Metric::new_with_labels(name, description, self.labels.clone(), ());
                v.insert(Box::new(res.clone()));
                res
            }
        }
    }

    pub fn append_onetime(&self, mut metrics: Vec<Box<dyn Measure>>) {
        let mut one_time = self.one_time.lock();
        one_time.append(&mut metrics);
    }

    /// Register a metric with the manual implementation of CreateMetricRecorder and return Metric<I>,
    /// type `<I>` could be `U64Counter`, `U64Gauge`, `DurationGauge`, `DurationCounter`, `U64Histogram`, `DurationHistogram`.
    ///
    /// The external labels of the metric is inherited from the register.
    pub fn register_metric<I: MetricRecorder + Clone + 'static>(
        &self,
        name: impl Into<Cow<'static, str>>,
        description: impl Into<Cow<'static, str>>,
        options: I::Options,
    ) -> Metric<I> {
        let mut measures = self.measures.lock();
        let name = name.into();
        match measures.entry(name.clone()) {
            Entry::Occupied(o) => o
                .get()
                .as_any()
                .downcast_ref::<Metric<I>>()
                .expect("measure type error")
                .clone(),
            Entry::Vacant(v) => {
                let res = Metric::new_with_labels(
                    name.clone(),
                    description,
                    self.labels.clone(),
                    options,
                );
                v.insert(Box::new(res.clone()));
                res
            }
        }
    }

    /// Report all metrics in this register and its sub-registers.
    pub fn report(&self, reporter: &mut dyn Reporter) {
        let measures = self.measures.lock();
        for measure in measures.values() {
            measure.report(reporter)
        }
        drop(measures);

        let mut one_time = self.one_time.lock();
        for measure in one_time.iter() {
            measure.report(reporter)
        }
        one_time.clear();
        drop(one_time);

        self.sub_register
            .lock()
            .iter()
            .for_each(|(_, r)| r.report(reporter))
    }
}
