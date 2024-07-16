use std::fmt::{Display, Formatter};

#[derive(Copy, Clone)]
#[allow(clippy::enum_variant_names)]
pub enum HttpApiType {
    ApiV1Write,
    ApiV1OpenTsDBWrite,
    ApiV1OpenTsDBPut,
    ApiV1PromWrite,

    ApiV1Sql,
    ApiV1PromRead,
    ApiV1ESLogWrite,

    ApiV1Ping,
    DebugBacktrace,
    Write,
    ApiV1metaleader,
    ApiV1Meta,
    ApiV1Raft,
    DebugPprof,
    DebugJeprof,
    Metrics,
    ApiV1DumpSqlDdl,
    V1Traces,
}

impl Display for HttpApiType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            HttpApiType::ApiV1Write => {
                write!(f, "api/v1/write")
            }
            HttpApiType::ApiV1OpenTsDBWrite => {
                write!(f, "api/v1/opentsdb/write")
            }
            HttpApiType::ApiV1OpenTsDBPut => {
                write!(f, "api/v1/opentsdb/put")
            }
            HttpApiType::ApiV1PromWrite => {
                write!(f, "api/v1/prom/write")
            }
            HttpApiType::ApiV1Sql => {
                write!(f, "api/v1/sql")
            }
            HttpApiType::ApiV1PromRead => {
                write!(f, "api/v1/prom/read")
            }
            HttpApiType::ApiV1ESLogWrite => {
                write!(f, "api/v1/es/write")
            }
            HttpApiType::ApiV1Ping => {
                write!(f, "api/v1/ping")
            }
            HttpApiType::DebugBacktrace => {
                write!(f, "debug/backtrace")
            }
            HttpApiType::Write => {
                write!(f, "write")
            }
            HttpApiType::ApiV1metaleader => {
                write!(f, "api/v1/meta_leader")
            }
            HttpApiType::ApiV1Meta => {
                write!(f, "api/v1/meta")
            }
            HttpApiType::ApiV1Raft => {
                write!(f, "api/v1/raft")
            }
            HttpApiType::DebugPprof => {
                write!(f, "debug/pprof")
            }
            HttpApiType::DebugJeprof => {
                write!(f, "debug/jeprof")
            }
            HttpApiType::Metrics => {
                write!(f, "metrics")
            }
            HttpApiType::ApiV1DumpSqlDdl => {
                write!(f, "api/v1/dump/sql/ddl")
            }
            HttpApiType::V1Traces => {
                write!(f, "v1/traces")
            }
        }
    }
}

// if api metrics need record TAG database return true
pub fn metrics_record_db(api: &HttpApiType) -> bool {
    match api {
        HttpApiType::ApiV1Write
        | HttpApiType::ApiV1OpenTsDBPut
        | HttpApiType::ApiV1OpenTsDBWrite
        | HttpApiType::ApiV1PromWrite
        | HttpApiType::ApiV1ESLogWrite
        | HttpApiType::ApiV1PromRead
        | HttpApiType::V1Traces => true,
        HttpApiType::ApiV1Sql
        | HttpApiType::ApiV1Ping
        | HttpApiType::DebugBacktrace
        | HttpApiType::Write
        | HttpApiType::ApiV1metaleader
        | HttpApiType::ApiV1Meta
        | HttpApiType::ApiV1Raft
        | HttpApiType::DebugPprof
        | HttpApiType::DebugJeprof
        | HttpApiType::Metrics
        | HttpApiType::ApiV1DumpSqlDdl => false,
    }
}
