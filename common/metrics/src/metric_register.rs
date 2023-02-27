use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::label::Labels;
use crate::metric::Metric;
use crate::reporter::Reporter;
use crate::{CreateMetricRecorder, Measure, MetricRecorder};

#[derive(Debug)]
pub struct MetricsRegister {
    labels: Labels,
    measures: Mutex<BTreeMap<&'static str, Box<dyn Measure>>>,
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
    pub fn new(labels: impl Into<Labels>) -> Self {
        Self {
            labels: labels.into(),
            measures: Default::default(),
            sub_register: Default::default(),
        }
    }

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

    pub fn sub_registers(&self) -> Vec<Arc<MetricsRegister>> {
        self.sub_register
            .lock()
            .iter()
            .map(|(_, v)| v.clone())
            .collect()
    }

    pub fn metric<I>(&self, name: &'static str, description: &'static str) -> Metric<I>
    where
        I: MetricRecorder + Clone + 'static + Default + CreateMetricRecorder<Options = ()>,
    {
        let mut measures = self.measures.lock();
        match measures.entry(name) {
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

    pub fn register_metric<I: MetricRecorder + Clone + 'static>(
        &self,
        name: &'static str,
        description: &'static str,
        options: I::Options,
    ) -> Metric<I> {
        let mut measures = self.measures.lock();
        match measures.entry(name) {
            Entry::Occupied(o) => o
                .get()
                .as_any()
                .downcast_ref::<Metric<I>>()
                .expect("measure type error")
                .clone(),
            Entry::Vacant(v) => {
                let res = Metric::new_with_labels(name, description, self.labels.clone(), options);
                v.insert(Box::new(res.clone()));
                res
            }
        }
    }

    pub fn report(&self, reporter: &mut dyn Reporter) {
        let measures = self.measures.lock();
        for measure in measures.values() {
            measure.report(reporter)
        }
        drop(measures);
        self.sub_register
            .lock()
            .iter()
            .for_each(|(_, r)| r.report(reporter))
    }
}
