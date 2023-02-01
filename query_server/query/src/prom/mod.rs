pub mod remote_server;
pub mod time_series;

pub const METRIC_NAME_LABEL: &str = "__name__";

pub const METRIC_SAMPLE_COLUMN_NAME: &str = "value";

pub const DEFAULT_PROM_TABLE_NAME: &str = "prom_metric_not_specified";
