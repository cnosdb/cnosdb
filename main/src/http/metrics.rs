use std::sync::Arc;

use metrics::count::U64Counter;
use metrics::label::Labels;
use metrics::metric::Metric;
use metrics::metric_register::MetricsRegister;

pub struct HttpMetrics {
    queries: Metric<U64Counter>,
    writes: Metric<U64Counter>,
    write_data_in: Metric<U64Counter>,
}

impl HttpMetrics {
    pub fn new(register: &Arc<MetricsRegister>) -> Self {
        let queries = register.metric(
            "user_queries",
            "the number of query requests received by the user",
        );
        let writes = register.metric(
            "user_writes",
            "the number of write requests received by the user",
        );

        let write_data_in = register.metric(
            "write_data_in",
            "Traffic statistics written by tenants through the http write",
        );

        Self {
            queries,
            writes,
            write_data_in,
        }
    }

    fn tenant_user_db_host_labels<'a>(
        tenant: &'a str,
        user: &'a str,
        db: &'a str,
        host: &'a str,
    ) -> impl Into<Labels> + 'a {
        [
            ("tenant", tenant),
            ("user", user),
            ("database", db),
            ("host", host),
        ]
    }

    pub fn queries_inc(&self, tenant: &str, user: &str, db: &str, host: &str) {
        self.queries
            .recorder(Self::tenant_user_db_host_labels(tenant, user, db, host))
            .inc_one()
    }

    pub fn writes_inc(&self, tenant: &str, user: &str, db: &str, host: &str) {
        self.writes
            .recorder(Self::tenant_user_db_host_labels(tenant, user, db, host))
            .inc_one()
    }

    pub fn write_data_in_inc(&self, tenant: &str, user: &str, db: &str, host: &str, data_in: u64) {
        self.write_data_in
            .recorder(Self::tenant_user_db_host_labels(tenant, user, db, host))
            .inc(data_in)
    }
}
