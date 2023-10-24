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
        | HttpApiType::ApiV1PromRead => true,
        HttpApiType::ApiV1Sql => false,
    }
}
