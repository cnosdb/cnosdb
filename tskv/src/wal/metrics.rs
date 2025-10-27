use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::Arc;

use metrics::gauge::U64Gauge;
use metrics::label::Labels;
use metrics::metric::Metric;
use metrics::metric_register::MetricsRegister;

/// Factory for `WalMetrics`.
pub struct WalMetricsFactory {
    wal_file_size_old: Arc<Metric<U64Gauge>>,
    wal_file_size_current: Arc<Metric<U64Gauge>>,
}

impl WalMetricsFactory {
    /// Create a new `WalMetricsFactory`.
    ///
    /// Register metrics:
    /// - `wal_file_size_old`: U64Gauge
    /// - `wal_file_size_current`: U64Gauge
    pub fn new(metrics_register: &MetricsRegister) -> Self {
        let wal_file_size_old =
            metrics_register.metric::<U64Gauge>("wal_file_size_old", "bytes of old wal files");
        let wal_file_size_current = metrics_register.metric::<U64Gauge>(
            "wal_file_size_current",
            "bytes of the current writing wal file",
        );
        Self {
            wal_file_size_old: Arc::new(wal_file_size_old),
            wal_file_size_current: Arc::new(wal_file_size_current),
        }
    }

    /// Build metrics recorder `WalMetrics` with `owner`, `vnode_id` and `group_id`.
    pub fn build(&self, owner: Arc<String>, vnode_id: u32, group_id: u32) -> Arc<WalMetrics> {
        let mut labels: BTreeMap<&'static str, Cow<'_, str>> = BTreeMap::new();
        labels.insert("owner", Cow::Owned(owner.to_string()));
        labels.insert("vnode_id", Cow::Owned(vnode_id.to_string()));
        labels.insert("group_id", Cow::Owned(group_id.to_string()));
        let labels = Labels(labels);
        let wal_file_size_old = self.wal_file_size_old.recorder(labels.clone());
        let wal_file_size_current = self.wal_file_size_current.recorder(labels.clone());
        Arc::new(WalMetrics {
            labels,
            metrics_wal_file_size_old: self.wal_file_size_old.clone(),
            metrics_wal_file_size_current: self.wal_file_size_current.clone(),
            wal_file_size_old,
            wal_file_size_current,
        })
    }
}

pub struct WalMetrics {
    /// Store the labels(`owner`, `vnode_id`, `group_id`) to deregister metrics while dropping the vnode.
    labels: Labels,
    metrics_wal_file_size_old: Arc<Metric<U64Gauge>>,
    metrics_wal_file_size_current: Arc<Metric<U64Gauge>>,

    /// Recorder for `wal_file_size_old`, bytes of old wal files.
    pub wal_file_size_old: U64Gauge,
    /// Recorder for `wal_file_size_current`, bytes of the current writing wal file.
    pub wal_file_size_current: U64Gauge,
}

impl WalMetrics {
    /// Remove metrics recorders from the registry. Stop the subsequent metrics recording.
    pub fn remove(&self) {
        self.metrics_wal_file_size_old.remove(self.labels.clone());
        self.metrics_wal_file_size_current
            .remove(self.labels.clone());
    }
}
