use std::sync::Arc;

use metrics::count::U64Counter;
use metrics::duration::{DurationHistogram, DurationHistogramOptions};
use metrics::label::Labels;
use metrics::metric::Metric;
use metrics::metric_register::MetricsRegister;

pub struct HttpMetrics {
    http_data_in: Metric<U64Counter>,
    http_data_out: Metric<U64Counter>,

    http_queries: Metric<U64Counter>,
    http_writes: Metric<U64Counter>,

    http_query_duration: Metric<DurationHistogram>,
    http_write_duration: Metric<DurationHistogram>,

    http_response_time: Metric<DurationHistogram>,
    http_flow: Metric<U64Counter>,

    write_parse_lp_duration: Metric<DurationHistogram>,
}

unsafe impl Send for HttpMetrics {}
unsafe impl Sync for HttpMetrics {}

macro_rules! generate_gets {
    ($field: ident, $metrics_type: ty) => {
        impl HttpMetrics {
            pub fn $field(
                &self,
                tenant: &str,
                user: &str,
                db: Option<&str>,
                host: &str,
                api: crate::http::api_type::HttpApiType,
            ) -> $metrics_type {
                self.$field.recorder(Self::tenant_user_db_host_labels(
                    Some(tenant),
                    Some(user),
                    db,
                    Some(host),
                    Some(&api.to_string()),
                ))
            }
        }
    };
}

macro_rules! generate_gets_other {
    ($field: ident, $metrics_type: ty) => {
        impl HttpMetrics {
            pub fn $field(
                &self,
                host: &str,
                api: crate::http::api_type::HttpApiType,
            ) -> $metrics_type {
                self.$field
                    .recorder([("host", host), ("api", &api.to_string())])
            }
        }
    };
}

generate_gets!(http_queries, U64Counter);
generate_gets!(http_writes, U64Counter);
generate_gets!(http_data_in, U64Counter);
generate_gets!(http_data_out, U64Counter);
generate_gets!(http_query_duration, DurationHistogram);
generate_gets!(http_write_duration, DurationHistogram);
generate_gets!(write_parse_lp_duration, DurationHistogram);
generate_gets_other!(http_flow, U64Counter);
generate_gets_other!(http_response_time, DurationHistogram);

impl HttpMetrics {
    pub fn new(register: &Arc<MetricsRegister>) -> Self {
        let http_queries = register.metric(
            "http_queries",
            "the number of query requests received by the user",
        );
        let http_writes = register.metric(
            "http_writes",
            "the number of write requests received by the user",
        );

        let http_data_in = register.metric("http_data_in", "Count the body of http request");

        let http_data_out = register.metric("http_data_out", "Count the body of http response");

        let http_query_duration: Metric<DurationHistogram> = register.register_metric(
            "http_query_duration",
            "Duration of the http sql handle",
            DurationHistogramOptions::default(),
        );

        let http_write_duration: Metric<DurationHistogram> = register.register_metric(
            "http_write_duration",
            "Duration of the http write handle",
            DurationHistogramOptions::default(),
        );

        let write_parse_lp_duration: Metric<DurationHistogram> = register.register_metric(
            "write_parse_lp_duration",
            "Duration of the parse line protocol handle",
            DurationHistogramOptions::default(),
        );

        let http_flow = register.metric(
            "http_flow",
            "Count the sum of request body len and response body len",
        );

        let http_response_time = register.register_metric(
            "http_response_time",
            "Duration of the http request",
            DurationHistogramOptions::default(),
        );

        Self {
            http_data_in,
            http_data_out,
            http_queries,
            http_writes,
            http_query_duration,
            http_write_duration,
            write_parse_lp_duration,
            http_response_time,
            http_flow,
        }
    }

    fn tenant_user_db_host_labels<'a>(
        tenant: Option<&'a str>,
        user: Option<&'a str>,
        db: Option<&'a str>,
        host: Option<&'a str>,
        api_type: Option<&'a str>,
    ) -> impl Into<Labels> + 'a {
        [
            ("tenant", tenant),
            ("user", user),
            ("database", db),
            ("host", host),
            ("api", api_type),
        ]
    }
}
