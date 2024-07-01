use metrics::gauge::U64Gauge;
use metrics::metric_register::MetricsRegister;
use models::schema::database_schema::split_owner;

#[derive(Debug)]
pub struct TsfMetrics {
    vnode_disk_storage: U64Gauge,
    vnode_cache_size: U64Gauge,
}

impl TsfMetrics {
    pub fn new(register: &MetricsRegister, owner: &str, vnode_id: u64) -> Self {
        let (tenant, db) = split_owner(owner);
        let metric = register.metric::<U64Gauge>("vnode_disk_storage", "disk storage of vnode");
        let disk_storage_gauge = metric.recorder([
            ("tenant", tenant),
            ("database", db),
            ("vnode_id", vnode_id.to_string().as_str()),
        ]);

        let metric = register.metric::<U64Gauge>("vnode_cache_size", "cache size of vnode");
        let cache_gauge = metric.recorder([
            ("tenant", tenant),
            ("database", db),
            ("vnode_id", vnode_id.to_string().as_str()),
        ]);

        Self {
            vnode_disk_storage: disk_storage_gauge,
            vnode_cache_size: cache_gauge,
        }
    }

    pub fn record_disk_storage(&self, size: u64) {
        self.vnode_disk_storage.set(size)
    }

    pub fn record_cache_size(&self, size: u64) {
        self.vnode_cache_size.set(size)
    }

    pub fn drop(register: &MetricsRegister, owner: &str, vnode_id: u64) {
        let (tenant, db) = split_owner(owner);
        let metric = register.metric::<U64Gauge>("vnode_disk_storage", "disk storage of vnode");
        metric.remove([
            ("tenant", tenant),
            ("database", db),
            ("vnode_id", vnode_id.to_string().as_str()),
        ]);

        let metric = register.metric::<U64Gauge>("vnode_cache_size", "cache size of vnode");
        metric.remove([
            ("tenant", tenant),
            ("database", db),
            ("vnode_id", vnode_id.to_string().as_str()),
        ]);
    }
}
