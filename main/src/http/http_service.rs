#![allow(clippy::too_many_arguments)]

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::fmt;
use std::fmt::Display;
use std::mem::size_of_val;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use config::tskv::TLSConfig;
use coordinator::service::CoordinatorRef;
use datafusion::arrow::array::{Array, StringArray};
use futures::TryStreamExt;
use http_protocol::encoding::Encoding;
use http_protocol::header::{
    ACCEPT, APPLICATION_JSON, AUTHORIZATION, DB, PRIVATE_KEY, TABLE, TENANT,
};
use http_protocol::parameter::{
    DebugParam, DumpParam, FindTracesParam, GetOperationParam, LogParam, SqlParam, WriteParam,
};
use http_protocol::response::ErrorResponse;
use http_protocol::status_code::OK;
use meta::error::{MetaError, MetaResult};
use meta::limiter::RequestLimiter;
use meta::model::MetaRef;
use metrics::count::U64Counter;
use metrics::metric_register::MetricsRegister;
use metrics::prom_reporter::PromReporter;
use models::auth::privilege::{DatabasePrivilege, Privilege, TenantObjectPrivilege};
use models::error_code::UnknownCodeWithMessage;
use models::oid::{Identifier, Oid};
use models::schema::{DEFAULT_CATALOG, DEFAULT_DATABASE};
use models::utils::now_timestamp_nanos;
use protocol_parser::json_protocol::parser::{
    parse_json_to_eslog, parse_json_to_lokilog, parse_json_to_ndjsonlog, parse_protobuf_to_lokilog,
    parse_protobuf_to_otlptrace, parse_to_line, Command, JsonProtocol,
};
use protocol_parser::json_protocol::JsonType;
use protocol_parser::line_protocol::line_protocol_to_lines;
use protocol_parser::open_tsdb::open_tsdb_to_lines;
use protocol_parser::{DataPoint, Line};
use query::prom::remote_server::PromRemoteSqlServer;
use reqwest::header::{HeaderName, HeaderValue, ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_TYPE};
use snafu::{IntoError, ResultExt};
use spi::query::config::StreamTriggerInterval;
use spi::server::dbms::DBMSRef;
use spi::server::prom::PromRemoteServerRef;
use spi::service::protocol::{Context, ContextBuilder, Query};
use spi::QueryError;
use tokio::sync::oneshot;
use trace::http::http_ctx::{HeaderDecodeSnafu, DEFAULT_TRACE_HEADER_NAME};
use trace::span_ctx_ext::SpanContextExt;
use trace::span_ext::SpanExt;
use trace::{debug, error, info, Span, SpanContext};
use utils::backtrace;
use utils::precision::Precision;
use warp::hyper::body::Bytes;
use warp::hyper::Body;
use warp::reject::{MethodNotAllowed, MissingHeader, PayloadTooLarge};
use warp::reply::Response;
use warp::{header, reject, Filter, Rejection, Reply};

use super::header::Header;
use super::{ContextSnafu, CoordinatorSnafu, DecodeRequestSnafu, Error as HttpError, MetaSnafu};
use crate::http::api_type::{metrics_record_db, HttpApiType};
use crate::http::encoding::{get_accept_encoding_from_header, get_content_encoding_from_header};
use crate::http::metrics::HttpMetrics;
use crate::http::response::{HttpResponse, ResponseBuilder};
use crate::http::result_format::{get_result_format_from_header, ResultFormat};
use crate::http::QuerySnafu;
use crate::opentelemetry::jaeger_model::{Operation, Process, Trace};
use crate::opentelemetry::otlp_to_jaeger::{
    FilterType, OtlpToJaeger, LIBRARY_NAME_COL_NAME, LIBRARY_VERSION_COL_NAME,
    OPERATION_NAME_COL_NAME, PARENT_SPAN_ID_COL_NAME, SERVICE_NAME_COL_NAME, SPAN_ID_COL_NAME,
    SPAN_KIND_COL_NAME, STATUS_CODE_COL_NAME, TRACE_ID_COL_NAME, TRACE_STATE_COL_NAME,
};
use crate::server::ServiceHandle;
use crate::spi::service::Service;
use crate::{server, VERSION};

pub enum ServerMode {
    Store,
    Query,
    Bundle,
}

impl Display for ServerMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServerMode::Store => {
                write!(f, "store mode")
            }
            ServerMode::Query => {
                write!(f, "query mode")
            }
            ServerMode::Bundle => {
                write!(f, "bundle mode")
            }
        }
    }
}

pub struct HttpService {
    tls_config: Option<TLSConfig>,
    addr: SocketAddr,
    dbms: DBMSRef,
    coord: CoordinatorRef,
    prs: PromRemoteServerRef,
    handle: Option<ServiceHandle<()>>,
    query_body_limit: u64,
    write_body_limit: u64,
    mode: ServerMode,
    metrics_register: Arc<MetricsRegister>,
    http_metrics: Arc<HttpMetrics>,
    auto_generate_span: bool,
}

impl HttpService {
    pub fn new(
        dbms: DBMSRef,
        coord: CoordinatorRef,
        addr: SocketAddr,
        tls_config: Option<TLSConfig>,
        query_body_limit: u64,
        write_body_limit: u64,
        mode: ServerMode,
        metrics_register: Arc<MetricsRegister>,
        auto_generate_span: bool,
    ) -> Self {
        let http_metrics = Arc::new(HttpMetrics::new(&metrics_register));

        let prs = Arc::new(PromRemoteSqlServer::new(dbms.clone(), coord.clone()));

        Self {
            tls_config,
            addr,
            dbms,
            coord,
            prs,
            handle: None,
            query_body_limit,
            write_body_limit,
            mode,
            metrics_register,
            http_metrics,
            auto_generate_span,
        }
    }

    /// user_id
    /// database
    /// =》
    /// Authorization
    /// Accept
    fn handle_header(&self) -> impl Filter<Extract = (Header,), Error = warp::Rejection> + Clone {
        header::optional::<String>(ACCEPT.as_str())
            .and(header::optional::<String>(ACCEPT_ENCODING.as_str()))
            .and(header::optional::<String>(CONTENT_ENCODING.as_str()))
            .and(header::<String>(AUTHORIZATION.as_str()))
            .and(header::optional::<String>(PRIVATE_KEY))
            .and(header::optional::<String>(TENANT))
            .and(header::optional::<String>(DB))
            .and(header::optional::<String>(TABLE))
            .and_then(
                |accept,
                 accept_encoding,
                 content_encoding,
                 authorization,
                 private_key,
                 tenant,
                 db,
                 table| async move {
                    let res: Result<Header, warp::Rejection> = Ok(Header::with_private_key(
                        accept,
                        accept_encoding,
                        content_encoding,
                        authorization,
                        private_key,
                        tenant,
                        db,
                        table,
                    ));
                    res
                },
            )
    }

    fn handle_span_header(
        &self,
    ) -> impl Filter<Extract = (Option<SpanContext>,), Error = warp::Rejection> + Clone {
        let auto_generate_span = self.auto_generate_span;
        header::optional::<String>(DEFAULT_TRACE_HEADER_NAME).and_then(
            move |trace: Option<String>| {
                let result = match trace {
                    Some(s) => SpanContext::from_str(&s)
                        .map(Some)
                        .context(HeaderDecodeSnafu {
                            header: DEFAULT_TRACE_HEADER_NAME,
                        })
                        .context(ContextSnafu)
                        .map_err(|e| {
                            error!("Failed to decode trace header, err: {:?}", e);
                            reject::custom(e)
                        }),
                    None if auto_generate_span => Ok(Some(SpanContext::random())),
                    None => Ok(None),
                };
                async move { result }
            },
        )
    }

    fn with_dbms(&self) -> impl Filter<Extract = (DBMSRef,), Error = Infallible> + Clone {
        let dbms = self.dbms.clone();
        warp::any().map(move || dbms.clone())
    }

    fn with_hostaddr(&self) -> impl Filter<Extract = (String,), Error = Infallible> + Clone {
        let hostaddr = self.addr.clone().to_string();
        warp::any().map(move || hostaddr.clone())
    }

    fn with_coord(&self) -> impl Filter<Extract = (CoordinatorRef,), Error = Infallible> + Clone {
        let coord = self.coord.clone();
        warp::any().map(move || coord.clone())
    }
    fn with_prom_remote_server(
        &self,
    ) -> impl Filter<Extract = (PromRemoteServerRef,), Error = Infallible> + Clone {
        let prs = self.prs.clone();
        warp::any().map(move || prs.clone())
    }

    fn with_metrics_register(
        &self,
    ) -> impl Filter<Extract = (Arc<MetricsRegister>,), Error = Infallible> + Clone {
        let register = self.metrics_register.clone();
        warp::any().map(move || register.clone())
    }

    fn with_http_metrics(
        &self,
    ) -> impl Filter<Extract = (Arc<HttpMetrics>,), Error = Infallible> + Clone {
        let metric = self.http_metrics.clone();
        warp::any().map(move || metric.clone())
    }

    fn with_meta(&self) -> impl Filter<Extract = (MetaRef,), Error = Infallible> + Clone {
        let meta = self.coord.meta_manager();
        warp::any().map(move || meta.clone())
    }

    fn routes_query(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        self.ping()
            .or(self.query())
            .or(self.mock_influxdb_write())
            .or(self.metrics())
            .or(self.print_meta())
            .or(self.meta_leader_addr())
            .or(self.debug_pprof())
            .or(self.debug_jeprof())
            .or(self.prom_remote_read())
            .or(self.backtrace())
            .or(self.print_raft())
            .or(self.dump_ddl_sql())
            .or(self.prom_remote_write())
            .or(self.write_open_tsdb())
            .or(self.put_open_tsdb())
            .or(self.write_line_protocol())
            .or(self.get_es_version())
            .or(self.get_es_empty())
            .or(self.get_es_license())
            .or(self.get_es_ingest())
            .or(self.get_es_node())
            .or(self.get_es_policy())
            .or(self.get_es_template())
            .or(self.write_es_log())
            .or(self.write_otlp_trace())
            .or(self.search_traces())
            .or(self.get_trace())
            .or(self.get_services())
            .or(self.get_operations())
            .or(self.get_operations_by_service())
    }

    fn routes_store(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        self.ping()
            .or(self.metrics())
            .or(self.print_meta())
            .or(self.meta_leader_addr())
            .or(self.debug_pprof())
            .or(self.debug_jeprof())
            .or(self.backtrace())
            .or(self.print_raft())
            .or(self.dump_ddl_sql())
    }

    fn ping(&self) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("api" / "v1" / "ping")
            .and(warp::get().or(warp::head()))
            .and(self.with_http_metrics())
            .and(self.with_hostaddr())
            .map(|_, metrics: Arc<HttpMetrics>, addr: String| {
                let start = Instant::now();
                let mut resp = HashMap::new();
                resp.insert("version", VERSION.as_str());
                resp.insert("status", "healthy");
                let keys_values_size: usize = resp
                    .iter()
                    .map(|(key, value)| size_of_val(*key) + size_of_val(*value))
                    .sum();
                http_response_time_and_flow_metrics(
                    &metrics,
                    &addr,
                    keys_values_size,
                    start,
                    HttpApiType::ApiV1Ping,
                );
                warp::reply::json(&resp)
            })
    }

    fn backtrace(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("debug" / "backtrace")
            .and(warp::get().or(warp::head()))
            .and(self.with_http_metrics())
            .and(self.with_hostaddr())
            .map(|_, metrics: Arc<HttpMetrics>, addr: String| {
                let start = Instant::now();
                let res = backtrace::backtrace();
                let mut resp = HashMap::new();
                resp.insert("taskdump_tree:", res);
                let keys_values_size: usize = resp
                    .iter()
                    .map(|(key, value)| size_of_val(*key) + size_of_val(value))
                    .sum();
                http_response_time_and_flow_metrics(
                    &metrics,
                    &addr,
                    keys_values_size,
                    start,
                    HttpApiType::DebugBacktrace,
                );
                warp::reply::json(&resp)
            })
    }

    fn query(&self) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        // let dbms = self.dbms.clone();
        warp::path!("api" / "v1" / "sql")
            .and(warp::post())
            .and(warp::body::content_length_limit(self.query_body_limit))
            .and(warp::body::bytes())
            .and(self.handle_header())
            .and(warp::query::<SqlParam>())
            .and(self.with_dbms())
            .and(self.with_meta())
            .and(self.with_coord())
            .and(self.with_http_metrics())
            .and(self.with_hostaddr())
            .and(self.handle_span_header())
            // construct_query
            .and_then(
                |mut req: Bytes,
                 header: Header,
                 param: SqlParam,
                 dbms: DBMSRef,
                 meta: MetaRef,
                 coord: CoordinatorRef,
                 metrics: Arc<HttpMetrics>,
                 addr: String,
                 parent_span_ctx: Option<SpanContext>| async move {
                    let start = Instant::now();
                    debug!(
                        "Receive http sql request, header: {:?}, param: {:?}",
                        header, param
                    );

                    let span = Span::from_context("rest sql request", parent_span_ctx.as_ref());
                    let req_len = req.len();
                    let content_encoding = get_content_encoding_from_header(&header)?;
                    if let Some(encoding) = content_encoding {
                        req = encoding.decode(req).map_err(|e| {
                            error!("Failed to decode request, err: {:?}", e);
                            reject::custom(HttpError::DecodeRequest { source: e })
                        })?;
                    }
                    let query = {
                        let mut span = Span::enter_with_parent("authenticate", &span);

                        // Parse req、header and param to construct query request
                        let query = construct_query(req, &header, param, dbms.clone(), coord)
                            .await
                            .map_err(|e| {
                                error!("Failed to construct query, err: {:?}", e);
                                reject::custom(e)
                            })?;
                        record_context_in_span(&mut span, query.context());
                        query
                    };

                    let result_fmt = get_result_format_from_header(&header)?;
                    let result_encoding = get_accept_encoding_from_header(&header)?;
                    http_limiter_check_query(&meta, query.context().tenant(), req_len)
                        .await
                        .map_err(|e| {
                            error!("Failed to check query limiter, err: {:?}", e);
                            reject::custom(e)
                        })?;

                    let tenant = query.context().tenant();
                    let user = query.context().user().desc().name();
                    let addr_str = addr.as_str();

                    let result = {
                        let span = Span::enter_with_parent("sql handle", &span);
                        let limiter = meta
                            .limiter(query.context().tenant())
                            .await
                            .context(MetaSnafu)?;
                        let http_data_out = metrics.http_data_out(
                            tenant,
                            user,
                            None,
                            addr_str,
                            HttpApiType::ApiV1Sql,
                        );
                        sql_handle(
                            &query,
                            &dbms,
                            result_fmt,
                            result_encoding,
                            span.context().as_ref(),
                            limiter,
                            http_data_out,
                        )
                        .await
                        .map_err(|e| {
                            span.error(e.to_string());
                            error!("Failed to handle http sql request, err: {:?}", e);
                            reject::custom(e)
                        })
                    };

                    // some sql maybe query other database, so don't record database
                    http_record_query_metrics(
                        &metrics,
                        query.context(),
                        &addr,
                        req_len,
                        start,
                        HttpApiType::ApiV1Sql,
                    );
                    let result_size = size_of_val(&result);
                    let value_size = match &result {
                        Ok(value) => size_of_val(value),
                        Err(error) => size_of_val(error),
                    };

                    let total_size = result_size + value_size + req_len;
                    http_response_time_and_flow_metrics(
                        &metrics,
                        &addr,
                        total_size,
                        start,
                        HttpApiType::ApiV1Sql,
                    );
                    result
                },
            )
    }

    fn write_line_protocol(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("api" / "v1" / "write")
            .and(warp::post())
            .and(warp::body::content_length_limit(self.write_body_limit))
            .and(warp::body::bytes())
            .and(self.handle_header())
            .and(warp::query::<WriteParam>())
            .and(self.with_dbms())
            .and(self.with_coord())
            .and(self.with_http_metrics())
            .and(self.with_hostaddr())
            .and(self.handle_span_header())
            .and_then(
                |mut req: Bytes,
                 header: Header,
                 param: WriteParam,
                 dbms: DBMSRef,
                 coord: CoordinatorRef,
                 metrics: Arc<HttpMetrics>,
                 addr: String,
                 parent_span_ctx: Option<SpanContext>| async move {
                    let start = Instant::now();
                    let span =
                        Span::from_context("rest line protocol write", parent_span_ctx.as_ref());
                    let span_context = span.context();

                    let req_len = req.len();
                    let content_encoding = get_content_encoding_from_header(&header)?;
                    if let Some(encoding) = content_encoding {
                        req = encoding.decode(req).map_err(|e| {
                            error!("Failed to decode request, err: {:?}", e);
                            reject::custom(HttpError::DecodeRequest { source: e })
                        })?;
                    }

                    let ctx = {
                        let mut span = Span::enter_with_parent("construct write context", &span);
                        let ctx = construct_write_context_and_check_privilege(
                            header,
                            param,
                            dbms,
                            coord.clone(),
                        )
                        .await
                        .map_err(|e| {
                            error!("Failed to construct write context, err: {:?}", e);
                            reject::custom(e)
                        })?;
                        record_context_in_span(&mut span, &ctx);
                        ctx
                    };

                    http_limiter_check_write(&coord.meta_manager(), ctx.tenant(), req_len).await?;

                    let precision = Precision::new(ctx.precision()).unwrap_or(Precision::NS);

                    let write_points_lines = {
                        let mut span = Span::enter_with_parent("try parse req to lines", &span);
                        span.add_property(|| ("bytes", req.len().to_string()));
                        try_parse_req_to_lines(&req).map_err(|e| {
                            error!("Failed to parse request to lines, err: {:?}", e);
                            reject::custom(e)
                        })?
                    };

                    let resp = coord_write_points_with_span_recorder(
                        &coord,
                        ctx.tenant(),
                        ctx.database(),
                        precision,
                        write_points_lines,
                        span_context.as_ref(),
                    )
                    .await;

                    http_record_write_metrics(
                        &metrics,
                        &ctx,
                        &addr,
                        req_len,
                        start,
                        HttpApiType::ApiV1Write,
                    );
                    let result_size = size_of_val(&resp);
                    let value_size = match &resp {
                        Ok(value) => size_of_val(value),
                        Err(error) => size_of_val(error),
                    };

                    let total_size = result_size + value_size + req_len;
                    http_response_time_and_flow_metrics(
                        &metrics,
                        &addr,
                        total_size,
                        start,
                        HttpApiType::ApiV1Write,
                    );
                    resp.map(|_| ResponseBuilder::ok()).map_err(|e| {
                        error!("Failed to handle http write request, err: {:?}", e);
                        reject::custom(e)
                    })
                },
            )
    }

    fn mock_influxdb_write(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("write")
            .and(warp::post())
            .and(warp::body::bytes())
            .and(warp::query::query())
            .and(self.with_dbms())
            .and(self.with_coord())
            .and(self.with_http_metrics())
            .and(self.with_hostaddr())
            .and_then(
                |req: Bytes,
                 mut query: HashMap<String, String>,
                 dbms: DBMSRef,
                 coord: CoordinatorRef,
                 metrics: Arc<HttpMetrics>,
                 addr: String| async move {
                    let start = Instant::now();
                    let db = query
                        .remove("db")
                        .unwrap_or_else(|| DEFAULT_DATABASE.to_string());
                    let header = Header::with(
                        Some(APPLICATION_JSON.to_string()),
                        None,
                        None,
                        "Basic cm9vdDo=".to_string(),
                    );
                    let param = WriteParam {
                        db: Some(db),
                        precision: None,
                        tenant: None,
                    };
                    let precision = Precision::NS;

                    let ctx = construct_write_context_and_check_privilege(
                        header,
                        param,
                        dbms,
                        coord.clone(),
                    )
                    .await
                    .map_err(|e| {
                        error!("Failed to construct write context, err: {:?}", e);
                        reject::custom(e)
                    })?;

                    let lines = try_parse_req_to_lines(&req).map_err(|e| {
                        error!("Failed to parse request to lines, err: {:?}", e);
                        reject::custom(e)
                    })?;

                    let resp = coord_write_points_with_span_recorder(
                        &coord,
                        ctx.tenant(),
                        ctx.database(),
                        precision,
                        lines,
                        None,
                    )
                    .await;

                    let result_size = size_of_val(&resp);
                    let value_size = match &resp {
                        Ok(value) => size_of_val(value),
                        Err(error) => size_of_val(error),
                    };

                    let total_size = result_size + value_size + req.len();
                    http_response_time_and_flow_metrics(
                        &metrics,
                        &addr,
                        total_size,
                        start,
                        HttpApiType::Write,
                    );
                    resp.map(|_| ResponseBuilder::ok()).map_err(|e| {
                        error!("Failed to handle http write request, err: {:?}", e);
                        reject::custom(e)
                    })
                },
            )
    }

    fn write_open_tsdb(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("api" / "v1" / "opentsdb" / "write")
            .and(warp::post())
            .and(warp::body::content_length_limit(self.write_body_limit))
            .and(warp::body::bytes())
            .and(self.handle_header())
            .and(warp::query::<WriteParam>())
            .and(self.with_dbms())
            .and(self.with_coord())
            .and(self.with_http_metrics())
            .and(self.with_hostaddr())
            .and(self.handle_span_header())
            .and_then(
                |mut req: Bytes,
                 header: Header,
                 param: WriteParam,
                 dbms: DBMSRef,
                 coord: CoordinatorRef,
                 metrics: Arc<HttpMetrics>,
                 addr: String,
                 parent_span_ctx: Option<SpanContext>| async move {
                    let start = Instant::now();
                    let span = Span::from_context("rest open tsdb write", parent_span_ctx.as_ref());
                    let span_context = span.context();

                    let req_len = req.len();
                    let content_encoding = get_content_encoding_from_header(&header)?;
                    if let Some(encoding) = content_encoding {
                        req = encoding.decode(req).map_err(|e| {
                            error!("Failed to decode request, err: {:?}", e);
                            reject::custom(HttpError::DecodeRequest { source: e })
                        })?;
                    }

                    let ctx = {
                        let mut span = Span::enter_with_parent("construct write context", &span);
                        let ctx = construct_write_context_and_check_privilege(
                            header,
                            param,
                            dbms,
                            coord.clone(),
                        )
                        .await
                        .map_err(|e| {
                            error!("Failed to construct write context, err: {:?}", e);
                            reject::custom(e)
                        })?;
                        record_context_in_span(&mut span, &ctx);
                        ctx
                    };

                    let precision = Precision::new(ctx.precision()).unwrap_or(Precision::NS);

                    http_limiter_check_write(&coord.meta_manager(), ctx.tenant(), req_len)
                        .await
                        .map_err(|e| {
                            error!("Failed to check write limiter, err: {:?}", e);
                            reject::custom(e)
                        })?;

                    let write_points_req = {
                        let mut span =
                            Span::enter_with_parent("construct write tsdb points request", &span);
                        span.add_property(|| ("bytes", req.len().to_string()));
                        construct_write_tsdb_points_request(&req).map_err(|e| {
                            error!(
                                "Failed to construct write tsdb points request, err: {:?}",
                                e
                            );
                            reject::custom(e)
                        })?
                    };
                    let resp = coord_write_points_with_span_recorder(
                        &coord,
                        ctx.tenant(),
                        ctx.database(),
                        precision,
                        write_points_req,
                        span_context.as_ref(),
                    )
                    .await;

                    http_record_write_metrics(
                        &metrics,
                        &ctx,
                        &addr,
                        req_len,
                        start,
                        HttpApiType::ApiV1OpenTsDBWrite,
                    );

                    let result_size = size_of_val(&resp);
                    let value_size = match &resp {
                        Ok(value) => size_of_val(value),
                        Err(error) => size_of_val(error),
                    };

                    let total_size = result_size + value_size + req_len;
                    http_response_time_and_flow_metrics(
                        &metrics,
                        &addr,
                        total_size,
                        start,
                        HttpApiType::ApiV1OpenTsDBWrite,
                    );
                    resp.map(|_| ResponseBuilder::ok()).map_err(|e| {
                        error!("Failed to handle http write request, err: {:?}", e);
                        reject::custom(e)
                    })
                },
            )
    }

    fn put_open_tsdb(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("api" / "v1" / "opentsdb" / "put")
            .and(warp::post())
            .and(warp::body::content_length_limit(self.write_body_limit))
            .and(warp::body::bytes())
            .and(self.handle_header())
            .and(warp::query::<WriteParam>())
            .and(self.with_dbms())
            .and(self.with_coord())
            .and(self.with_http_metrics())
            .and(self.with_hostaddr())
            .and(self.handle_span_header())
            .and_then(
                |mut req: Bytes,
                 header: Header,
                 param: WriteParam,
                 dbms: DBMSRef,
                 coord: CoordinatorRef,
                 metrics: Arc<HttpMetrics>,
                 addr: String,
                 parent_span_ctx: Option<SpanContext>| async move {
                    let start = Instant::now();
                    let span = Span::from_context("rest open tsdb put", parent_span_ctx.as_ref());
                    let span_context = span.context();

                    let req_len = req.len();
                    let content_encoding = get_content_encoding_from_header(&header)?;
                    if let Some(encoding) = content_encoding {
                        req = encoding
                            .decode(req)
                            .context(DecodeRequestSnafu)
                            .map_err(|e| {
                                error!("Failed to decode request, err: {:?}", e);
                                let r = snafu::Report::from_error(e);
                                reject::custom(HttpError::InvalidHeader {
                                    reason: r.to_string(),
                                })
                            })?;
                    }

                    let ctx = {
                        let mut span = Span::enter_with_parent("construct write context", &span);
                        let ctx = construct_write_context_and_check_privilege(
                            header,
                            param,
                            dbms,
                            coord.clone(),
                        )
                        .await
                        .map_err(|e| {
                            error!("Failed to construct write context, err: {:?}", e);
                            reject::custom(e)
                        })?;
                        record_context_in_span(&mut span, &ctx);
                        ctx
                    };

                    let precision = Precision::new(ctx.precision()).unwrap_or(Precision::NS);

                    http_limiter_check_write(&coord.meta_manager(), ctx.tenant(), req_len)
                        .await
                        .map_err(|e| {
                            error!("Failed to check write limiter, err: {:?}", e);
                            reject::custom(e)
                        })?;

                    let write_points_req = {
                        let mut span = Span::enter_with_parent(
                            "construct write tsdb points json request",
                            &span,
                        );
                        span.add_property(|| ("bytes", req.len().to_string()));
                        construct_write_tsdb_points_json_request(&req).map_err(|e| {
                            error!(
                                "Failed to construct write tsdb points json request, err: {:?}",
                                e
                            );
                            reject::custom(e)
                        })?
                    };
                    let resp = coord_write_points_with_span_recorder(
                        &coord,
                        ctx.tenant(),
                        ctx.database(),
                        precision,
                        write_points_req,
                        span_context.as_ref(),
                    )
                    .await;

                    http_record_write_metrics(
                        &metrics,
                        &ctx,
                        &addr,
                        req_len,
                        start,
                        HttpApiType::ApiV1OpenTsDBPut,
                    );
                    let result_size = size_of_val(&resp);
                    let value_size = match &resp {
                        Ok(value) => size_of_val(value),
                        Err(error) => size_of_val(error),
                    };

                    let total_size = result_size + value_size + req_len;
                    http_response_time_and_flow_metrics(
                        &metrics,
                        &addr,
                        total_size,
                        start,
                        HttpApiType::ApiV1OpenTsDBPut,
                    );
                    resp.map(|_| ResponseBuilder::ok()).map_err(|e| {
                        error!("Failed to handle http write request, err: {:?}", e);
                        reject::custom(e)
                    })
                },
            )
    }

    fn meta_leader_addr(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("api" / "v1" / "meta_leader")
            .and(self.handle_header())
            .and(self.with_coord())
            .and(self.with_http_metrics())
            .and(self.with_hostaddr())
            .and_then(
                |_header: Header,
                 coord: CoordinatorRef,
                 metrics: Arc<HttpMetrics>,
                 addr: String| async move {
                    let start = Instant::now();
                    let resp = match coord.meta_manager().meta_leader().await {
                        Ok(data) => Ok(data),
                        Err(err) => {
                            error!("Failed to get meta leader addr, err: {:?}", err);
                            Err(reject::custom(MetaSnafu.into_error(err)))
                        }
                    };
                    let result_size = size_of_val(&resp);
                    let value_size = match &resp {
                        Ok(value) => size_of_val(value),
                        Err(error) => size_of_val(error),
                    };

                    let total_size = result_size + value_size;
                    http_response_time_and_flow_metrics(
                        &metrics,
                        &addr,
                        total_size,
                        start,
                        HttpApiType::ApiV1metaleader,
                    );
                    resp
                },
            )
    }

    fn print_meta(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("api" / "v1" / "meta")
            .and(self.handle_header())
            .and(self.with_coord())
            .and(self.with_http_metrics())
            .and(self.with_hostaddr())
            .and_then(
                |_header: Header,
                 coord: CoordinatorRef,
                 metrics: Arc<HttpMetrics>,
                 addr: String| async move {
                    let start = Instant::now();
                    let tenant = DEFAULT_CATALOG.to_string();

                    let meta_client = match coord.tenant_meta(&tenant).await {
                        Some(client) => client,
                        None => {
                            let e = HttpError::Meta {
                                source: MetaError::TenantNotFound { tenant },
                            };
                            error!("Failed to get meta client, err: {:?}", e);
                            return Err(reject::custom(e));
                        }
                    };
                    let data = meta_client.print_data();
                    http_response_time_and_flow_metrics(
                        &metrics,
                        &addr,
                        data.len(),
                        start,
                        HttpApiType::ApiV1Meta,
                    );
                    Ok(data)
                },
            )
    }

    fn print_raft(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("api" / "v1" / "raft")
            .and(warp::query::<DebugParam>())
            .and(self.with_coord())
            .and(self.with_http_metrics())
            .and(self.with_hostaddr())
            .and_then(
                |param: DebugParam,
                 coord: CoordinatorRef,
                 metrics: Arc<HttpMetrics>,
                 addr: String| async move {
                    let start = Instant::now();
                    let raft_manager = coord.raft_manager();
                    let data = raft_manager.metrics(param.id.unwrap_or(0)).await;

                    let res: Result<String, warp::Rejection> = Ok(data);
                    let result_size = size_of_val(&res);
                    let value_size = match &res {
                        Ok(value) => size_of_val(value),
                        Err(error) => size_of_val(error),
                    };

                    let total_size = result_size + value_size;
                    http_response_time_and_flow_metrics(
                        &metrics,
                        &addr,
                        total_size,
                        start,
                        HttpApiType::ApiV1Raft,
                    );
                    res
                },
            )
    }

    fn debug_pprof(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("debug" / "pprof")
            .and(self.with_http_metrics())
            .and(self.with_hostaddr())
            .and_then(|metrics: Arc<HttpMetrics>, addr: String| async move {
                let start = Instant::now();
                #[cfg(unix)]
                {
                    let res = utils::pprof_tools::gernate_pprof().await;
                    info!("debug pprof: {:?}", res);
                    let resp = match res {
                        Ok(v) => Ok(v),
                        Err(e) => Err(reject::custom(HttpError::PProf { reason: e })),
                    };
                    let result_size = size_of_val(&resp);
                    let value_size = match &resp {
                        Ok(value) => size_of_val(value),
                        Err(error) => size_of_val(error),
                    };

                    let total_size = result_size + value_size;
                    http_response_time_and_flow_metrics(
                        &metrics,
                        &addr,
                        total_size,
                        start,
                        HttpApiType::DebugPprof,
                    );
                    resp
                }
                #[cfg(not(unix))]
                {
                    Err::<String, _>(reject::not_found())
                }
            })
    }

    fn debug_jeprof(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("debug" / "jeprof")
            .and(self.with_http_metrics())
            .and(self.with_hostaddr())
            .and_then(|metrics: Arc<HttpMetrics>, addr: String| async move {
                let start = Instant::now();
                #[cfg(unix)]
                {
                    let res = utils::pprof_tools::gernate_jeprof().await;
                    info!("debug jeprof: {:?}", res);
                    let resp = match res {
                        Ok(v) => Ok(v),
                        Err(e) => Err(reject::custom(HttpError::PProf { reason: e })),
                    };
                    let result_size = size_of_val(&resp);
                    let value_size = match &resp {
                        Ok(value) => size_of_val(value),
                        Err(error) => size_of_val(error),
                    };

                    let total_size = result_size + value_size;
                    http_response_time_and_flow_metrics(
                        &metrics,
                        &addr,
                        total_size,
                        start,
                        HttpApiType::DebugJeprof,
                    );
                    resp
                }
                #[cfg(not(unix))]
                {
                    Err::<String, _>(reject::not_found())
                }
            })
    }

    fn metrics(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("metrics")
            .and(self.with_metrics_register())
            .and(self.with_http_metrics())
            .and(self.with_hostaddr())
            .map(
                |register: Arc<MetricsRegister>, metrics: Arc<HttpMetrics>, addr: String| {
                    let start = Instant::now();
                    let mut buffer: Vec<u8> = Vec::new();
                    let mut prom_reporter = PromReporter::new(&mut buffer);
                    register.report(&mut prom_reporter);
                    http_response_time_and_flow_metrics(
                        &metrics,
                        &addr,
                        size_of_val(&buffer),
                        start,
                        HttpApiType::Metrics,
                    );
                    Response::new(Body::from(buffer))
                },
            )
    }

    fn prom_remote_read(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("api" / "v1" / "prom" / "read")
            .and(warp::post())
            .and(warp::body::content_length_limit(self.query_body_limit))
            .and(warp::body::bytes())
            .and(self.handle_header())
            .and(warp::query::<SqlParam>())
            .and(self.with_dbms())
            .and(self.with_meta())
            .and(self.with_coord())
            .and(self.with_http_metrics())
            .and(self.with_prom_remote_server())
            .and(self.with_hostaddr())
            .and(self.handle_span_header())
            .and_then(
                |req: Bytes,
                 header: Header,
                 param: SqlParam,
                 dbms: DBMSRef,
                 meta: MetaRef,
                 coord: CoordinatorRef,
                 metrics: Arc<HttpMetrics>,
                 prs: PromRemoteServerRef,
                 addr: String,
                 parent_span_ctx: Option<SpanContext>| async move {
                    let start = Instant::now();
                    debug!(
                        "Receive rest prom remote read request, header: {:?}, param: {:?}",
                        header, param
                    );
                    let span =
                        Span::from_context("rest prom remote read", parent_span_ctx.as_ref());

                    // Parse req、header and param to construct query request
                    let context = {
                        let mut span = Span::enter_with_parent("construct context", &span);
                        span.add_property(|| ("bytes", req.len().to_string()));
                        let ctx = construct_read_context(&header, param, dbms, coord, false)
                            .await
                            .map_err(|e| {
                                error!("Failed to construct read context, err: {:?}", e);
                                reject::custom(e)
                            })?;
                        record_context_in_span(&mut span, &ctx);
                        ctx
                    };
                    let req_len = req.len();

                    http_limiter_check_query(&meta, context.tenant(), req_len)
                        .await
                        .map_err(|e| {
                            error!("Failed to check query limiter, err: {:?}", e);
                            reject::custom(e)
                        })?;

                    let tenant_name = context.tenant();
                    let username = context.user().desc().name();
                    let database_name = context.database();

                    let http_query_data_out = metrics.http_data_out(
                        tenant_name,
                        username,
                        Some(database_name),
                        addr.as_str(),
                        HttpApiType::ApiV1PromRead,
                    );

                    let result = {
                        let span = Span::enter_with_parent("remote read", &span);
                        prs.remote_read(&context, req, span.context().as_ref())
                            .await
                            .map_err(|e| {
                                span.error(e.to_string());
                                error!("Failed to handle prom remote read request, err: {:?}", e);
                                reject::custom(QuerySnafu.into_error(e))
                            })
                            .map(|b| {
                                http_query_data_out.inc(b.len() as u64);
                                b
                            })
                    };

                    http_record_query_metrics(
                        &metrics,
                        &context,
                        &addr,
                        req_len,
                        start,
                        HttpApiType::ApiV1PromRead,
                    );
                    let result_size = size_of_val(&result);
                    let value_size = match &result {
                        Ok(value) => size_of_val(value),
                        Err(error) => size_of_val(error),
                    };

                    let total_size = result_size + value_size + req_len;
                    http_response_time_and_flow_metrics(
                        &metrics,
                        &addr,
                        total_size,
                        start,
                        HttpApiType::ApiV1PromRead,
                    );
                    result
                },
            )
    }

    fn prom_remote_write(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("api" / "v1" / "prom" / "write")
            .and(warp::post())
            .and(warp::body::content_length_limit(self.query_body_limit))
            .and(warp::body::bytes())
            .and(self.handle_header())
            .and(warp::query::<WriteParam>())
            .and(self.with_coord())
            .and(self.with_dbms())
            .and(self.with_prom_remote_server())
            .and(self.with_http_metrics())
            .and(self.with_hostaddr())
            .and(self.handle_span_header())
            .and_then(
                |req: Bytes,
                 header: Header,
                 param: WriteParam,
                 coord: CoordinatorRef,
                 dbms: DBMSRef,
                 prs: PromRemoteServerRef,
                 metrics: Arc<HttpMetrics>,
                 addr: String,
                 parent_span_ctx: Option<SpanContext>| async move {
                    let start = Instant::now();
                    debug!(
                        "Receive rest prom remote write request, header: {:?}, param: {:?}",
                        header, param
                    );
                    let span =
                        Span::from_context("rest prom remote write", parent_span_ctx.as_ref());
                    let span_context = span.context();

                    // Parse req、header and param to construct query request
                    let ctx = {
                        let mut span = Span::enter_with_parent("construct context", &span);
                        span.add_property(|| ("bytes", req.len().to_string()));
                        let ctx = construct_write_context_and_check_privilege(
                            header,
                            param,
                            dbms,
                            coord.clone(),
                        )
                        .await
                        .map_err(|e| {
                            error!("Failed to construct write context, err: {:?}", e);
                            reject::custom(e)
                        })?;
                        record_context_in_span(&mut span, &ctx);
                        ctx
                    };

                    let req_len = req.len();
                    http_limiter_check_write(&coord.meta_manager(), ctx.tenant(), req_len)
                        .await
                        .map_err(|e| {
                            error!("Failed to check write limiter, err: {:?}", e);
                            reject::custom(e)
                        })?;

                    let span = Span::enter_with_parent("remote write", &span);
                    let prom_write_request = prs.remote_write(req).map_err(|e| {
                        span.error(e.to_string());
                        error!("Failed to handle prom remote write request, err: {:?}", e);
                        reject::custom(QuerySnafu.into_error(e))
                    })?;
                    let write_request = prs
                        .prom_write_request_to_lines(&prom_write_request)
                        .map_err(|e| {
                            span.error(e.to_string());
                            error!("Failed to handle prom remote write request, err: {:?}", e);
                            reject::custom(QuerySnafu.into_error(e))
                        })?;

                    let resp = coord_write_points_with_span_recorder(
                        &coord,
                        ctx.tenant(),
                        ctx.database(),
                        Precision::NS,
                        write_request,
                        span_context.as_ref(),
                    )
                    .await;
                    http_record_write_metrics(
                        &metrics,
                        &ctx,
                        &addr,
                        req_len,
                        start,
                        HttpApiType::ApiV1PromWrite,
                    );

                    let result_size = size_of_val(&resp);
                    let value_size = match &resp {
                        Ok(value) => size_of_val(value),
                        Err(error) => size_of_val(error),
                    };

                    let total_size = result_size + value_size + req_len;
                    http_response_time_and_flow_metrics(
                        &metrics,
                        &addr,
                        total_size,
                        start,
                        HttpApiType::ApiV1PromWrite,
                    );
                    resp.map(|_| ResponseBuilder::ok()).map_err(|e| {
                        error!("Failed to handle http write request, err: {:?}", e);
                        reject::custom(e)
                    })
                },
            )
    }

    fn dump_ddl_sql(&self) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
        async fn dump_sql_ddl_impl(meta: MetaRef, tenant: Option<String>) -> MetaResult<String> {
            let cluster = meta.cluster();
            let leader = meta.meta_leader().await?;
            let url = match tenant {
                Some(t) => {
                    format!("http://{}/{}/{cluster}/{t}", leader, "dump/sql/ddl")
                }
                None => {
                    format!("http://{}/{}/{cluster}", leader, "dump/sql/ddl")
                }
            };

            let resp = reqwest::get(url.clone())
                .await
                .map_err(|e| MetaError::MetaClientErr { msg: e.to_string() })?;
            let status = resp.status();

            let data = resp
                .text()
                .await
                .map_err(|e| MetaError::MetaClientErr { msg: e.to_string() })?;

            if !status.is_success() {
                return Err(MetaError::MetaClientErr {
                    msg: format!("httpcode: {}, response:{}", status, data),
                });
            }
            Ok(data)
        }
        warp::path!("api" / "v1" / "dump" / "sql" / "ddl")
            .and(self.with_meta())
            .and(warp::query::<DumpParam>())
            .and(self.with_http_metrics())
            .and(self.with_hostaddr())
            .and_then(
                |meta, param: DumpParam, metrics: Arc<HttpMetrics>, addr: String| async move {
                    let start = Instant::now();
                    let resp = dump_sql_ddl_impl(meta, param.tenant)
                        .await
                        .map(|r| r.into_bytes())
                        .map_err(|e| {
                            error!("Failed to dump ddl sql, err: {:?}", e);
                            reject::custom(e)
                        });
                    let result_size = size_of_val(&resp);
                    let value_size = match &resp {
                        Ok(value) => size_of_val(value),
                        Err(error) => size_of_val(error),
                    };

                    let total_size = result_size + value_size;
                    http_response_time_and_flow_metrics(
                        &metrics,
                        &addr,
                        total_size,
                        start,
                        HttpApiType::ApiV1DumpSqlDdl,
                    );
                    resp
                },
            )
    }

    fn get_es_version(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("api" / "v1" / "es").and(warp::get()).map(|| {
            #[derive(serde::Serialize)]
            struct Version {
                number: &'static str,
            }

            let mut resp = HashMap::new();
            resp.insert("version", Version { number: "8.4.0" });
            let mut builder = ResponseBuilder::new(OK);
            builder = builder.insert_header((
                HeaderName::from_static("x-elastic-product"),
                HeaderValue::from_static("Elasticsearch"),
            ));

            builder.json(&resp)
        })
    }

    fn get_es_empty(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("api" / "v1" / "es").and(warp::head()).map(|| {
            let mut builder = ResponseBuilder::new(OK);
            builder = builder.insert_header((
                HeaderName::from_static("x-elastic-product"),
                HeaderValue::from_static("Elasticsearch"),
            ));
            builder.build(Vec::new())
        })
    }

    fn get_es_license(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("api" / "v1" / "es" / "_license")
            .and(warp::get().or(warp::head()))
            .map(|_| {
                #[derive(serde::Serialize)]
                struct License {
                    uid: &'static str,
                    license_type: &'static str,
                    status: &'static str,
                    expiry_date_in_millis: u64,
                }

                let mut resp = HashMap::new();
                resp.insert(
                    "license",
                    License {
                        uid: "cbff45e7-c553-41f7-ae4f-9205eabd80xx",
                        license_type: "oss",
                        status: "active",
                        expiry_date_in_millis: 4000000000000,
                    },
                );
                warp::reply::json(&resp)
            })
    }

    fn get_es_ingest(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("api" / "v1" / "es" / "_ingest" / ..).map(ResponseBuilder::ok)
    }

    fn get_es_node(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("api" / "v1" / "es" / "_nodes" / ..).map(ResponseBuilder::ok)
    }

    fn get_es_policy(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("api" / "v1" / "es" / "_ilm" / "policy" / ..).map(ResponseBuilder::ok)
    }

    fn get_es_template(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("api" / "v1" / "es" / "_index_template" / ..).map(ResponseBuilder::ok)
    }

    fn write_es_log(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("api" / "v1" / "es" / "_bulk")
            .and(warp::post())
            .and(warp::body::content_length_limit(self.write_body_limit))
            .and(warp::body::bytes())
            .and(warp::header::optional::<String>("content-type"))
            .and(self.handle_header())
            .and(warp::query::<LogParam>())
            .and(self.with_dbms())
            .and(self.with_coord())
            .and(self.with_http_metrics())
            .and(self.with_hostaddr())
            .and(self.handle_span_header())
            .and_then(
                |mut req: Bytes,
                 content_type: Option<String>,
                 header: Header,
                 param: LogParam,
                 dbms: DBMSRef,
                 coord: CoordinatorRef,
                 metrics: Arc<HttpMetrics>,
                 addr: String,
                 parent_span_ctx: Option<SpanContext>| async move {
                    let start = Instant::now();
                    let span = Span::from_context("rest log write", parent_span_ctx.as_ref());
                    let span_context = span.context();

                    let req_len = req.len();
                    let content_encoding = get_content_encoding_from_header(&header)?;
                    if let Some(encoding) = content_encoding {
                        req = encoding.decode(req).map_err(|e| {
                            error!("Failed to decode request, err: {:?}", e);
                            reject::custom(HttpError::DecodeRequest { source: e })
                        })?;
                    }

                    let write_param = WriteParam {
                        precision: None,
                        tenant: param.tenant,
                        db: param.db,
                    };

                    if param.table.is_none() {
                        let e = HttpError::ParseLog {
                            source: protocol_parser::JsonLogError::Common {
                                content: "table param is None".to_string(),
                            },
                        };
                        error!("Failed to parse request to log, err: {:?}", e);
                        return Err(reject::custom(e));
                    }

                    let ctx = {
                        let mut span =
                            Span::from_context("construct write context", span_context.as_ref());
                        let ctx = construct_write_context_and_check_privilege(
                            header,
                            write_param,
                            dbms,
                            coord.clone(),
                        )
                        .await
                        .map_err(|e| {
                            error!("Failed to construct write context, err: {:?}", e);
                            reject::custom(e)
                        })?;
                        record_context_in_span(&mut span, &ctx);
                        ctx
                    };

                    let log_type = param.log_type.unwrap_or_else(|| "bulk".to_string());
                    let log_type = JsonType::try_parse(log_type).map_err(|e| {
                        error!("Failed to parse log_type, err: {:?}", e);
                        HttpError::ParseLog { source: e }
                    })?;
                    http_limiter_check_write(&coord.meta_manager(), ctx.tenant(), req_len).await?;

                    let logs = {
                        let mut span =
                            Span::from_context("try parse req to log", span_context.as_ref());
                        span.add_property(|| ("bytes", req.len().to_string()));
                        try_parse_log_req(req, log_type, content_type).map_err(|e| {
                            error!("Failed to parse request to log, err: {:?}", e);
                            reject::custom(e)
                        })?
                    };

                    let resp = coord_write_log(
                        &coord,
                        ctx.tenant(),
                        ctx.database(),
                        &param.table.unwrap(),
                        logs,
                        param.time_column,
                        param.tag_columns,
                        span_context.as_ref(),
                    )
                    .await;

                    http_record_write_metrics(
                        &metrics,
                        &ctx,
                        &addr,
                        req_len,
                        start,
                        HttpApiType::ApiV1ESLogWrite,
                    );
                    let result_size = size_of_val(&resp);
                    let value_size = match &resp {
                        Ok(value) => size_of_val(value),
                        Err(error) => size_of_val(error),
                    };

                    let total_size = result_size + value_size + req_len;
                    http_response_time_and_flow_metrics(
                        &metrics,
                        &addr,
                        total_size,
                        start,
                        HttpApiType::ApiV1ESLogWrite,
                    );
                    resp.map_err(|e| {
                        error!("Failed to handle http write request, err: {:?}", e);
                        reject::custom(e)
                    })
                },
            )
    }

    fn write_otlp_trace(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("api" / "v1" / "traces")
            .and(warp::post())
            .and(warp::body::content_length_limit(self.write_body_limit))
            .and(warp::body::bytes())
            .and(warp::header::optional::<String>("content-type"))
            .and(self.handle_header())
            .and(self.with_dbms())
            .and(self.with_coord())
            .and(self.with_http_metrics())
            .and(self.with_hostaddr())
            .and(self.handle_span_header())
            .and_then(
                |mut req: Bytes,
                 content_type: Option<String>,
                 header: Header,
                 dbms: DBMSRef,
                 coord: CoordinatorRef,
                 metrics: Arc<HttpMetrics>,
                 addr: String,
                 parent_span_ctx: Option<SpanContext>| async move {
                    let start = Instant::now();
                    let span = Span::from_context("rest log write", parent_span_ctx.as_ref());
                    let span_context = span.context();

                    let tenant = header
                        .get_tenant()
                        .clone()
                        .unwrap_or(DEFAULT_CATALOG.to_string());
                    let db = header
                        .get_db()
                        .clone()
                        .unwrap_or(DEFAULT_DATABASE.to_string());
                    let table = header.get_table().clone().ok_or(HttpError::ParseLog {
                        source: protocol_parser::JsonLogError::Common {
                            content: "table param is None".to_string(),
                        },
                    })?;

                    let span =
                        Span::from_context("rest otlp trace write", parent_span_ctx.as_ref());

                    let content_encoding = get_content_encoding_from_header(&header)?;
                    if let Some(encoding) = content_encoding {
                        req = encoding.decode(req).map_err(|e| {
                            error!("Failed to decode request, err: {:?}", e);
                            reject::custom(HttpError::DecodeRequest { source: e })
                        })?;
                    }

                    let write_param = WriteParam {
                        precision: None,
                        tenant: header.get_tenant(),
                        db: header.get_db(),
                    };
                    let ctx = {
                        let mut span = Span::enter_with_parent("construct write context", &span);
                        let ctx = construct_write_context_and_check_privilege(
                            header,
                            write_param,
                            dbms,
                            coord.clone(),
                        )
                        .await
                        .map_err(|e| {
                            error!("Failed to construct write context, err: {:?}", e);
                            reject::custom(e)
                        })?;
                        record_context_in_span(&mut span, &ctx);
                        ctx
                    };

                    let req_len = req.len();
                    http_limiter_check_write(&coord.meta_manager(), ctx.tenant(), req_len).await?;

                    let logs = try_parse_log_req(req.clone(), JsonType::OtlpTrace, content_type)
                        .map_err(|e| {
                            error!("Failed to parse request to log, err: {:?}", e);
                            reject::custom(e)
                        })?;

                    let time_column =
                        "ResourceSpans/ScopeSpans/Span/start_time_unix_nano".to_string();
                    let tag_columns = [
                        LIBRARY_NAME_COL_NAME,
                        LIBRARY_VERSION_COL_NAME,
                        STATUS_CODE_COL_NAME,
                        SPAN_ID_COL_NAME,
                        OPERATION_NAME_COL_NAME,
                        PARENT_SPAN_ID_COL_NAME,
                        SPAN_ID_COL_NAME,
                        TRACE_ID_COL_NAME,
                        TRACE_STATE_COL_NAME,
                        SERVICE_NAME_COL_NAME,
                    ]
                    .join(",");

                    let resp = coord_write_log(
                        &coord,
                        &tenant,
                        &db,
                        &table,
                        logs,
                        Some(time_column),
                        Some(tag_columns),
                        span_context.as_ref(),
                    )
                    .await;

                    http_record_write_metrics(
                        &metrics,
                        &ctx,
                        &addr,
                        req_len,
                        start,
                        HttpApiType::ApiV1Traces,
                    );
                    let result_size = size_of_val(&resp);
                    let value_size = match &resp {
                        Ok(value) => size_of_val(value),
                        Err(error) => size_of_val(error),
                    };

                    let total_size = result_size + value_size + req_len;
                    http_response_time_and_flow_metrics(
                        &metrics,
                        &addr,
                        total_size,
                        start,
                        HttpApiType::ApiV1Traces,
                    );

                    resp.map(|_| ResponseBuilder::ok()).map_err(|e| {
                        error!(
                            "Failed to handle http write otlp traces request, err: {:?}",
                            e
                        );
                        reject::custom(e)
                    })
                },
            )
    }

    fn search_traces(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("api" / "traces")
            .and(warp::get())
            .and(self.handle_header())
            .and(warp::query::<FindTracesParam>())
            .and(self.with_dbms())
            .and(self.with_coord())
            .and(self.with_http_metrics())
            .and(self.with_hostaddr())
            .and_then(
                |header: Header,
                 param: FindTracesParam,
                 dbms: DBMSRef,
                 coord: CoordinatorRef,
                 metrics: Arc<HttpMetrics>,
                 addr: String| async move {
                    let start = Instant::now();
                    // authenticate
                    let sql_param = SqlParam {
                        tenant: header.get_tenant().clone(),
                        db: header.get_db().clone(),
                        chunked: None,
                        target_partitions: None,
                        stream_trigger_interval: None,
                    };
                    let _ = construct_read_context(&header, sql_param, dbms, coord.clone(), false)
                        .await
                        .map_err(|e| {
                            error!("Failed to construct query, err: {:?}", e);
                            reject::custom(e)
                        })?;

                    let traces = if let Some(trace_ids) = param.trace_ids {
                        let mut traces = Vec::new();
                        for trace_id in trace_ids.split(',').collect::<Vec<&str>>() {
                            traces.push(
                                Self::get_trace_inner(coord.clone(), &header, trace_id.to_string())
                                    .await
                                    .map_err(|e| {
                                        error!("Failed to get trace, err: {:?}", e);
                                        reject::custom(e)
                                    })?,
                            );
                        }
                        traces
                    } else {
                        // get param
                        let tenant = header
                            .get_tenant()
                            .clone()
                            .unwrap_or(DEFAULT_CATALOG.to_string());
                        let db = header
                            .get_db()
                            .clone()
                            .unwrap_or(DEFAULT_DATABASE.to_string());
                        let table = header.get_table().clone().ok_or(HttpError::ParseLog {
                            source: protocol_parser::JsonLogError::Common {
                                content: "table param is None".to_string(),
                            },
                        })?;

                        // contruct stream by query param
                        let iterators = OtlpToJaeger::get_tskv_iterator(
                            coord,
                            FilterType::FindTraces(Some(param)),
                            tenant,
                            db,
                            table,
                        )
                        .await
                        .map_err(|e| HttpError::Query { source: e })?;

                        // read recordbatch from stream
                        let mut record_batch_vec = Vec::new();
                        for mut iter in iterators {
                            while let Some(record_batch) = iter
                                .try_next()
                                .await
                                .map_err(|e| HttpError::Coordinator { source: e })?
                            {
                                record_batch_vec.push(record_batch);
                            }
                        }

                        // contruct Trace from recordbatch
                        let mut trace_map: HashMap<String, Trace> = HashMap::default();
                        for batch in record_batch_vec {
                            let column_trace_id = batch
                                .column_by_name(TRACE_ID_COL_NAME)
                                .ok_or(HttpError::FetchResult {
                                    reason: format!("column {} is not exist", TRACE_ID_COL_NAME),
                                })?
                                .as_any()
                                .downcast_ref::<StringArray>()
                                .ok_or(HttpError::FetchResult {
                                    reason: format!(
                                        "column {} is not StringArray",
                                        TRACE_ID_COL_NAME
                                    ),
                                })?;
                            if let Some(i) = (0..column_trace_id.len()).next() {
                                let trace_id = column_trace_id.value(i).to_string();
                                let mut span = OtlpToJaeger::recordbatch_to_span(batch, i)
                                    .map_err(|e| HttpError::FetchResult {
                                        reason: e.message().to_string(),
                                    })?;
                                if let Some(trace) = trace_map.get_mut(&trace_id) {
                                    let process_id = format!("p{}", trace.spans.len());
                                    span.process_id = Some(process_id.clone());
                                    if let Some(process) = &span.process {
                                        trace.processes.insert(process_id, process.clone());
                                    }
                                    trace.spans.push(span);
                                } else {
                                    let process_id = "p1";
                                    span.process_id = Some(process_id.to_string());
                                    let process =
                                        span.process.clone().unwrap_or(Process::default());
                                    trace_map.entry(trace_id.clone()).or_insert_with(|| {
                                        let mut trace = Trace {
                                            trace_id: trace_id.clone(),
                                            spans: vec![span],
                                            ..Default::default()
                                        };
                                        trace.processes.insert(process_id.to_string(), process);
                                        trace
                                    });
                                }
                            }
                        }

                        trace_map.into_values().collect()
                    };

                    #[derive(serde::Serialize)]
                    struct Traces {
                        data: Vec<Trace>,
                        total: u64,
                        limit: u64,
                        offset: u64,
                        errors: Option<String>,
                    }

                    let total = traces.len() as u64;
                    let resp = Traces {
                        data: traces,
                        total,
                        limit: 0,
                        offset: 0,
                        errors: None,
                    };

                    http_response_time_and_flow_metrics(
                        &metrics,
                        &addr,
                        size_of_val(&resp),
                        start,
                        HttpApiType::ApiTraces,
                    );

                    // return
                    let builder = ResponseBuilder::new(OK).insert_header((
                        CONTENT_TYPE,
                        HeaderValue::from_static("application/json"),
                    ));
                    Ok::<_, Rejection>(builder.json(&resp))
                },
            )
    }

    async fn get_trace_inner(
        coord: CoordinatorRef,
        header: &Header,
        trace_id: String,
    ) -> Result<Trace, HttpError> {
        // get param
        let tenant = header
            .get_tenant()
            .clone()
            .unwrap_or(DEFAULT_CATALOG.to_string());
        let db = header
            .get_db()
            .clone()
            .unwrap_or(DEFAULT_DATABASE.to_string());
        let table = header.get_table().clone().ok_or(HttpError::ParseLog {
            source: protocol_parser::JsonLogError::Common {
                content: "table param is None".to_string(),
            },
        })?;

        // contruct stream by trace_id
        let iterators = OtlpToJaeger::get_tskv_iterator(
            coord,
            FilterType::GetTraceID(trace_id.clone()),
            tenant,
            db,
            table,
        )
        .await
        .map_err(|e| HttpError::Query { source: e })?;

        // read recordbatch from stream
        let mut record_batch_vec = Vec::new();
        for mut iter in iterators {
            while let Some(record_batch) = iter
                .try_next()
                .await
                .map_err(|e| HttpError::Coordinator { source: e })?
            {
                record_batch_vec.push(record_batch);
            }
        }

        // contruct Trace from recordbatch
        let mut trace = Trace::default();
        for batch in record_batch_vec {
            let column_trace_id = batch
                .column_by_name(TRACE_ID_COL_NAME)
                .ok_or(HttpError::FetchResult {
                    reason: format!("column {} is not exist", TRACE_ID_COL_NAME),
                })?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or(HttpError::FetchResult {
                    reason: format!("column {} is not StringArray", TRACE_ID_COL_NAME),
                })?;
            if let Some(i) = (0..column_trace_id.len()).next() {
                trace
                    .spans
                    .push(OtlpToJaeger::recordbatch_to_span(batch, i).map_err(|e| {
                        HttpError::FetchResult {
                            reason: e.message().to_string(),
                        }
                    })?);
            }
        }
        if !trace.spans.is_empty() {
            trace.trace_id = trace_id;
        }

        Ok(trace)
    }

    fn get_trace(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("api" / "traces" / String)
            .and(warp::get())
            .and(self.handle_header())
            .and(self.with_dbms())
            .and(self.with_coord())
            .and(self.with_http_metrics())
            .and(self.with_hostaddr())
            .and_then(
                |trace_id: String,
                 header: Header,
                 dbms: DBMSRef,
                 coord: CoordinatorRef,
                 metrics: Arc<HttpMetrics>,
                 addr: String| async move {
                    let start = Instant::now();
                    // authenticate
                    let sql_param = SqlParam {
                        tenant: header.get_tenant().clone(),
                        db: header.get_db().clone(),
                        chunked: None,
                        target_partitions: None,
                        stream_trigger_interval: None,
                    };
                    let _ = construct_read_context(&header, sql_param, dbms, coord.clone(), false)
                        .await
                        .map_err(|e| {
                            error!("Failed to construct query, err: {:?}", e);
                            reject::custom(e)
                        })?;
                    let mut trace = Self::get_trace_inner(coord, &header, trace_id).await?;
                    for (i, span) in trace.spans.iter_mut().enumerate() {
                        if let Some(process) = &span.process {
                            let process_id = format!("p{}", i);
                            span.process_id = Some(process_id.clone());
                            trace.processes.insert(process_id, process.clone());
                        }
                    }

                    #[derive(serde::Serialize)]
                    struct Traces {
                        data: Vec<Trace>,
                        total: u64,
                        limit: u64,
                        offset: u64,
                        errors: Option<String>,
                    }
                    let resp = Traces {
                        data: vec![trace],
                        total: 0,
                        limit: 0,
                        offset: 0,
                        errors: None,
                    };

                    http_response_time_and_flow_metrics(
                        &metrics,
                        &addr,
                        size_of_val(&resp),
                        start,
                        HttpApiType::ApiTracesID,
                    );

                    // return
                    let builder = ResponseBuilder::new(OK).insert_header((
                        CONTENT_TYPE,
                        HeaderValue::from_static("application/json"),
                    ));
                    Ok::<_, Rejection>(builder.json(&resp))
                },
            )
    }

    fn get_services(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("api" / "services")
            .and(warp::get())
            .and(self.handle_header())
            .and(self.with_dbms())
            .and(self.with_coord())
            .and(self.with_http_metrics())
            .and(self.with_hostaddr())
            .and_then(
                |header: Header,
                 dbms: DBMSRef,
                 coord: CoordinatorRef,
                 metrics: Arc<HttpMetrics>,
                 addr: String| async move {
                    let start = Instant::now();
                    // authenticate
                    let sql_param = SqlParam {
                        tenant: header.get_tenant().clone(),
                        db: header.get_db().clone(),
                        chunked: None,
                        target_partitions: None,
                        stream_trigger_interval: None,
                    };
                    let _ = construct_read_context(&header, sql_param, dbms, coord.clone(), false)
                        .await
                        .map_err(|e| {
                            error!("Failed to construct query, err: {:?}", e);
                            reject::custom(e)
                        })?;

                    // get param
                    let tenant = header
                        .get_tenant()
                        .clone()
                        .unwrap_or(DEFAULT_CATALOG.to_string());
                    let db = header
                        .get_db()
                        .clone()
                        .unwrap_or(DEFAULT_DATABASE.to_string());
                    let table = header.get_table().clone().ok_or(HttpError::ParseLog {
                        source: protocol_parser::JsonLogError::Common {
                            content: "table param is None".to_string(),
                        },
                    })?;

                    // contruct stream
                    let iterators = OtlpToJaeger::get_tskv_iterator(
                        coord,
                        FilterType::GetServices,
                        tenant,
                        db,
                        table,
                    )
                    .await
                    .map_err(|e| HttpError::Query { source: e })?;

                    // read recordbatch from stream
                    let mut record_batch_vec = Vec::new();
                    for mut iter in iterators {
                        while let Some(record_batch) = iter
                            .try_next()
                            .await
                            .map_err(|e| HttpError::Coordinator { source: e })?
                        {
                            record_batch_vec.push(record_batch);
                        }
                    }

                    // contruct services from recordbatch
                    let mut services = HashSet::new();
                    for batch in record_batch_vec {
                        let column = batch
                            .column_by_name(SERVICE_NAME_COL_NAME)
                            .ok_or(HttpError::FetchResult {
                                reason: format!("column {} is not exist", SERVICE_NAME_COL_NAME),
                            })?
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or(HttpError::FetchResult {
                                reason: format!("column {} is not StringArray", TRACE_ID_COL_NAME),
                            })?;
                        for i in 0..column.len() {
                            services.insert(column.value(i).to_string());
                        }
                    }

                    #[derive(serde::Serialize)]
                    struct Services {
                        data: Vec<String>,
                        total: u64,
                        limit: u64,
                        offset: u64,
                        errors: Option<String>,
                    }

                    let total = services.len() as u64;
                    let resp = Services {
                        data: services.into_iter().collect(),
                        total,
                        limit: 0,
                        offset: 0,
                        errors: None,
                    };

                    http_response_time_and_flow_metrics(
                        &metrics,
                        &addr,
                        size_of_val(&resp),
                        start,
                        HttpApiType::ApiServices,
                    );

                    // return
                    let builder = ResponseBuilder::new(OK).insert_header((
                        CONTENT_TYPE,
                        HeaderValue::from_static("application/json"),
                    ));
                    Ok::<_, Rejection>(builder.json(&resp))
                },
            )
    }

    fn get_operations(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("api" / "operations")
            .and(warp::get())
            .and(self.handle_header())
            .and(warp::query::<GetOperationParam>())
            .and(self.with_dbms())
            .and(self.with_coord())
            .and(self.with_http_metrics())
            .and(self.with_hostaddr())
            .and_then(
                |header: Header,
                 param: GetOperationParam,
                 dbms: DBMSRef,
                 coord: CoordinatorRef,
                 metrics: Arc<HttpMetrics>,
                 addr: String| async move {
                    let start = Instant::now();
                    // authenticate
                    let sql_param = SqlParam {
                        tenant: header.get_tenant().clone(),
                        db: header.get_db().clone(),
                        chunked: None,
                        target_partitions: None,
                        stream_trigger_interval: None,
                    };
                    let _ = construct_read_context(&header, sql_param, dbms, coord.clone(), false)
                        .await
                        .map_err(|e| {
                            error!("Failed to construct query, err: {:?}", e);
                            reject::custom(e)
                        })?;

                    // get param
                    let tenant = header
                        .get_tenant()
                        .clone()
                        .unwrap_or(DEFAULT_CATALOG.to_string());
                    let db = header
                        .get_db()
                        .clone()
                        .unwrap_or(DEFAULT_DATABASE.to_string());
                    let table = header.get_table().clone().ok_or(HttpError::ParseLog {
                        source: protocol_parser::JsonLogError::Common {
                            content: "table param is None".to_string(),
                        },
                    })?;
                    let service = param.service.ok_or(HttpError::FetchResult {
                        reason: "service is empty".to_string(),
                    })?;
                    let span_kind = param.span_kind.unwrap_or_default();

                    // contruct stream by service and span_kind
                    let iterators = OtlpToJaeger::get_tskv_iterator(
                        coord,
                        FilterType::GetOperation(service, span_kind),
                        tenant,
                        db,
                        table,
                    )
                    .await
                    .map_err(|e| HttpError::Query { source: e })?;

                    // read recordbatch from stream
                    let mut record_batch_vec = Vec::new();
                    for mut iter in iterators {
                        while let Some(record_batch) = iter
                            .try_next()
                            .await
                            .map_err(|e| HttpError::Coordinator { source: e })?
                        {
                            record_batch_vec.push(record_batch);
                        }
                    }

                    // contruct Operations from recordbatch
                    let mut operations = HashSet::new();
                    for batch in record_batch_vec {
                        let name_col = batch
                            .column_by_name(OPERATION_NAME_COL_NAME)
                            .ok_or(HttpError::FetchResult {
                                reason: format!("column {} is not exist", OPERATION_NAME_COL_NAME),
                            })?
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or(HttpError::FetchResult {
                                reason: format!("column {} is not StringArray", TRACE_ID_COL_NAME),
                            })?;
                        let span_kind_col = batch
                            .column_by_name(SPAN_KIND_COL_NAME)
                            .ok_or(HttpError::FetchResult {
                                reason: format!("column {} is not exist", SPAN_KIND_COL_NAME),
                            })?
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or(HttpError::FetchResult {
                                reason: format!("column {} is not StringArray", TRACE_ID_COL_NAME),
                            })?;
                        for i in 0..name_col.len() {
                            operations.insert(Operation {
                                name: name_col.value(i).to_string(),
                                span_kind: OtlpToJaeger::to_jaeger_span_kind(
                                    span_kind_col.value(i),
                                )
                                .to_string(),
                            });
                        }
                    }
                    let operations = operations.into_iter().collect::<Vec<Operation>>();
                    #[derive(serde::Serialize)]
                    struct Operations {
                        data: Vec<Operation>,
                        total: u64,
                        limit: u64,
                        offset: u64,
                        errors: Option<String>,
                    }
                    let total = operations.len() as u64;
                    let resp = Operations {
                        data: operations.into_iter().collect(),
                        total,
                        limit: 0,
                        offset: 0,
                        errors: None,
                    };

                    http_response_time_and_flow_metrics(
                        &metrics,
                        &addr,
                        size_of_val(&resp),
                        start,
                        HttpApiType::ApiOperations,
                    );

                    // return
                    let builder = ResponseBuilder::new(OK).insert_header((
                        CONTENT_TYPE,
                        HeaderValue::from_static("application/json"),
                    ));
                    Ok::<_, Rejection>(builder.json(&resp))
                },
            )
    }

    fn get_operations_by_service(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("api" / "services" / String / "operations")
            .and(warp::get())
            .and(self.handle_header())
            .and(self.with_dbms())
            .and(self.with_coord())
            .and(self.with_http_metrics())
            .and(self.with_hostaddr())
            .and_then(
                |service: String,
                 header: Header,
                 dbms: DBMSRef,
                 coord: CoordinatorRef,
                 metrics: Arc<HttpMetrics>,
                 addr: String| async move {
                    let start = Instant::now();
                    // authenticate
                    let sql_param = SqlParam {
                        tenant: header.get_tenant().clone(),
                        db: header.get_db().clone(),
                        chunked: None,
                        target_partitions: None,
                        stream_trigger_interval: None,
                    };
                    let _ = construct_read_context(&header, sql_param, dbms, coord.clone(), false)
                        .await
                        .map_err(|e| {
                            error!("Failed to construct query, err: {:?}", e);
                            reject::custom(e)
                        })?;

                    // get param
                    let tenant = header
                        .get_tenant()
                        .clone()
                        .unwrap_or(DEFAULT_CATALOG.to_string());
                    let db = header
                        .get_db()
                        .clone()
                        .unwrap_or(DEFAULT_DATABASE.to_string());
                    let table = header.get_table().clone().ok_or(HttpError::ParseLog {
                        source: protocol_parser::JsonLogError::Common {
                            content: "table param is None".to_string(),
                        },
                    })?;

                    // contruct stream by service and span_kind
                    let iterators = OtlpToJaeger::get_tskv_iterator(
                        coord,
                        FilterType::GetOperation(service, "".to_string()),
                        tenant,
                        db,
                        table,
                    )
                    .await
                    .map_err(|e| HttpError::Query { source: e })?;

                    // read recordbatch from stream
                    let mut record_batch_vec = Vec::new();
                    for mut iter in iterators {
                        while let Some(record_batch) = iter
                            .try_next()
                            .await
                            .map_err(|e| HttpError::Coordinator { source: e })?
                        {
                            record_batch_vec.push(record_batch);
                        }
                    }

                    // contruct Operations from recordbatch
                    let mut operations = HashSet::new();
                    for batch in record_batch_vec {
                        let name_col = batch
                            .column_by_name(OPERATION_NAME_COL_NAME)
                            .ok_or(HttpError::FetchResult {
                                reason: format!("column {} is not exist", OPERATION_NAME_COL_NAME),
                            })?
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or(HttpError::FetchResult {
                                reason: format!("column {} is not StringArray", TRACE_ID_COL_NAME),
                            })?;
                        for i in 0..name_col.len() {
                            operations.insert(name_col.value(i).to_string());
                        }
                    }
                    let data = operations.into_iter().collect::<Vec<String>>();
                    #[derive(serde::Serialize)]
                    struct Operations {
                        data: Vec<String>,
                        total: u64,
                        limit: u64,
                        offset: u64,
                        errors: Option<String>,
                    }
                    let total = data.len() as u64;
                    let resp = Operations {
                        data,
                        total,
                        limit: 0,
                        offset: 0,
                        errors: None,
                    };

                    http_response_time_and_flow_metrics(
                        &metrics,
                        &addr,
                        size_of_val(&resp),
                        start,
                        HttpApiType::ApiServicesOperations,
                    );

                    // return
                    let builder = ResponseBuilder::new(OK).insert_header((
                        CONTENT_TYPE,
                        HeaderValue::from_static("application/json"),
                    ));
                    Ok::<_, Rejection>(builder.json(&resp))
                },
            )
    }
}

#[async_trait::async_trait]
impl Service for HttpService {
    fn start(&mut self) -> Result<(), server::Error> {
        let (shutdown, rx) = oneshot::channel();
        let signal = async {
            rx.await.ok();
            info!("http server graceful shutdown!");
        };
        let join_handle = if let Some(TLSConfig {
            certificate,
            private_key,
        }) = &self.tls_config
        {
            match self.mode {
                ServerMode::Store => {
                    let routes = self.routes_store().recover(handle_rejection);
                    let (addr, server) = warp::serve(routes)
                        .tls()
                        .cert_path(certificate)
                        .key_path(private_key)
                        .bind_with_graceful_shutdown(self.addr, signal);
                    info!("http server start addr: {}, {}", addr, self.mode);
                    tokio::spawn(server)
                }
                ServerMode::Query => {
                    let routes = self.routes_query().recover(handle_rejection);
                    let (addr, server) = warp::serve(routes)
                        .tls()
                        .cert_path(certificate)
                        .key_path(private_key)
                        .bind_with_graceful_shutdown(self.addr, signal);
                    info!("http server start addr: {}, {}", addr, self.mode);
                    tokio::spawn(server)
                }
                ServerMode::Bundle => {
                    let routes = self.routes_query().recover(handle_rejection);
                    let (addr, server) = warp::serve(routes)
                        .tls()
                        .cert_path(certificate)
                        .key_path(private_key)
                        .bind_with_graceful_shutdown(self.addr, signal);
                    info!("http server start addr: {}, {}", addr, self.mode);
                    tokio::spawn(server)
                }
            }
        } else {
            match self.mode {
                ServerMode::Store => {
                    let routes = self.routes_store().recover(handle_rejection);
                    let (addr, server) =
                        warp::serve(routes).bind_with_graceful_shutdown(self.addr, signal);
                    info!("http server start addr: {}, {}", addr, self.mode);
                    tokio::spawn(server)
                }
                ServerMode::Query => {
                    let routes = self.routes_query().recover(handle_rejection);
                    let (addr, server) =
                        warp::serve(routes).bind_with_graceful_shutdown(self.addr, signal);
                    info!("http server start addr: {}, {}", addr, self.mode);
                    tokio::spawn(server)
                }
                ServerMode::Bundle => {
                    let routes = self.routes_query().recover(handle_rejection);
                    let (addr, server) =
                        warp::serve(routes).bind_with_graceful_shutdown(self.addr, signal);
                    info!("http server start addr: {}, {}", addr, self.mode);
                    tokio::spawn(server)
                }
            }
        };
        self.handle = Some(ServiceHandle::new(
            "http service".to_string(),
            join_handle,
            shutdown,
        ));
        Ok(())
    }

    async fn stop(&mut self, force: bool) {
        if let Some(stop) = self.handle.take() {
            stop.shutdown(force).await
        };
    }
}

async fn construct_query(
    req: Bytes,
    header: &Header,
    param: SqlParam,
    dbms: DBMSRef,
    coord: CoordinatorRef,
) -> Result<Query, HttpError> {
    let context = construct_read_context(header, param, dbms, coord, true).await?;

    Ok(Query::new(
        context,
        String::from_utf8_lossy(req.as_ref()).to_string(),
    ))
}

async fn construct_read_context(
    header: &Header,
    param: SqlParam,
    dbms: DBMSRef,
    coord: CoordinatorRef,
    is_sql: bool,
) -> Result<Context, HttpError> {
    let user_info = header.try_get_basic_auth()?;

    let tenant = param.tenant;
    let user = dbms
        .authenticate(&user_info, tenant.as_deref().unwrap_or(DEFAULT_CATALOG))
        .await
        .context(QuerySnafu)?;

    if !is_sql
        && coord.get_config().query.auth_enabled
        && user
            .desc()
            .options()
            .must_change_password()
            .is_some_and(|x| x)
    {
        return Err(HttpError::Query {
            source: QueryError::InsufficientPrivileges {
                privilege: "change password".to_string(),
            },
        });
    }

    let context = ContextBuilder::new(user)
        .with_tenant(tenant)
        .with_database(param.db)
        .with_target_partitions(param.target_partitions)
        .with_chunked(param.chunked)
        .with_stream_trigger_interval(
            param
                .stream_trigger_interval
                .map(|ref e| {
                    e.parse::<StreamTriggerInterval>()
                        .map_err(|reason| HttpError::InvalidHeader { reason })
                })
                .transpose()?,
        )
        .build();

    Ok(context)
}

async fn construct_write_context(
    header: &Header,
    param: WriteParam,
    dbms: DBMSRef,
) -> Result<Context, HttpError> {
    let user_info = header.try_get_basic_auth()?;
    let tenant = param.tenant;
    let db = param.db;
    let precision = param.precision;

    let user = dbms
        .authenticate(&user_info, tenant.as_deref().unwrap_or(DEFAULT_CATALOG))
        .await
        .context(QuerySnafu)?;

    let context = ContextBuilder::new(user)
        .with_tenant(tenant)
        .with_database(db)
        .with_precision(precision)
        .build();

    Ok(context)
}

fn _construct_write_db_privilege(tenant_id: Oid, database: &str) -> Privilege<Oid> {
    Privilege::TenantObject(
        TenantObjectPrivilege::Database(DatabasePrivilege::Write, Some(database.to_string())),
        Some(tenant_id),
    )
}

// construct context and check privilege
async fn construct_write_context_and_check_privilege(
    header: Header,
    param: WriteParam,
    dbms: DBMSRef,
    coord: CoordinatorRef,
) -> Result<Context, HttpError> {
    let context = construct_write_context(&header, param, dbms).await?;

    let tenant_id = *coord
        .tenant_meta(context.tenant())
        .await
        .ok_or_else(|| MetaError::TenantNotFound {
            tenant: context.tenant().to_string(),
        })
        .context(MetaSnafu)?
        .tenant()
        .id();

    if coord.get_config().query.auth_enabled
        && context
            .user()
            .desc()
            .options()
            .must_change_password()
            .is_some_and(|x| x)
    {
        return Err(HttpError::Query {
            source: QueryError::InsufficientPrivileges {
                privilege: "change password".to_string(),
            },
        });
    }
    let privilege = Privilege::TenantObject(
        TenantObjectPrivilege::Database(
            DatabasePrivilege::Write,
            Some(context.database().to_string()),
        ),
        Some(tenant_id),
    );
    if !context.user().check_privilege(&privilege) {
        return Err(HttpError::Query {
            source: QueryError::InsufficientPrivileges {
                privilege: format!("{privilege}"),
            },
        });
    }
    Ok(context)
}

fn try_parse_req_to_lines(req: &Bytes) -> Result<Vec<Line>, HttpError> {
    let lines = simdutf8::basic::from_utf8(req.as_ref())
        .map_err(|e| HttpError::InvalidUTF8 { source: e })?;
    let line_protocol_lines = line_protocol_to_lines(lines, now_timestamp_nanos())
        .map_err(|e| HttpError::ParseLineProtocol { source: e })?;

    Ok(line_protocol_lines)
}

fn construct_write_tsdb_points_request(req: &Bytes) -> Result<Vec<Line>, HttpError> {
    let lines = simdutf8::basic::from_utf8(req.as_ref())
        .map_err(|e| HttpError::InvalidUTF8 { source: e })?;

    let tsdb_protocol_lines = open_tsdb_to_lines(lines, now_timestamp_nanos())
        .map_err(|e| HttpError::ParseOpentsdbProtocol { source: e })?;

    Ok(tsdb_protocol_lines)
}

fn construct_write_tsdb_points_json_request(req: &Bytes) -> Result<Vec<Line>, HttpError> {
    let lines = simdutf8::basic::from_utf8(req.as_ref())
        .map_err(|e| HttpError::InvalidUTF8 { source: e })?;
    let tsdb_datapoints = match serde_json::from_str::<DataPoint>(lines) {
        Ok(datapoint) => vec![datapoint],
        Err(_) => match serde_json::from_str::<Vec<DataPoint>>(lines) {
            Ok(datapoints) => datapoints,
            Err(e) => {
                error!("{}", e);
                return Err(HttpError::ParseOpentsdbJsonProtocol { source: e });
            }
        },
    }
    .into_iter()
    .map(Line::from)
    .collect::<Vec<Line>>();

    Ok(tsdb_datapoints)
}

fn try_parse_log_req(
    req: Bytes,
    log_type: JsonType,
    content_type: Option<String>,
) -> Result<Vec<JsonProtocol>, HttpError> {
    if content_type.is_some_and(|x| x.eq("application/x-protobuf")) {
        if log_type.eq(&JsonType::Loki) {
            let logs =
                parse_protobuf_to_lokilog(req).map_err(|e| HttpError::ParseLog { source: e })?;
            return Ok(logs);
        } else if log_type.eq(&JsonType::OtlpTrace) {
            let logs =
                parse_protobuf_to_otlptrace(req).map_err(|e| HttpError::ParseLog { source: e })?;
            return Ok(logs);
        }
    }

    let lines = simdutf8::basic::from_utf8(req.as_ref())
        .map_err(|e| HttpError::InvalidUTF8 { source: e })?;

    let json_chunk: Vec<&str> = lines.trim().split('\n').collect();
    match log_type {
        JsonType::Bulk => {
            let logs =
                parse_json_to_eslog(json_chunk).map_err(|e| HttpError::ParseLog { source: e })?;
            Ok(logs)
        }
        JsonType::Ndjson => {
            let logs = parse_json_to_ndjsonlog(json_chunk)
                .map_err(|e| HttpError::ParseLog { source: e })?;
            Ok(logs)
        }
        JsonType::Loki => {
            let logs =
                parse_json_to_lokilog(json_chunk).map_err(|e| HttpError::ParseLog { source: e })?;
            Ok(logs)
        }
        _ => Err(HttpError::ParseLog {
            source: protocol_parser::JsonLogError::InvaildSyntax,
        }),
    }
}

async fn coord_write_log(
    coord: &CoordinatorRef,
    tenant: &str,
    db: &str,
    table: &str,
    logs: Vec<JsonProtocol>,
    time_column: Option<String>,
    tag_columns: Option<String>,
    span_context: Option<&SpanContext>,
) -> Result<Response, HttpError> {
    let span = Span::from_context("write points", span_context);

    let mut table_exist = {
        if let Some(meta) = coord.meta_manager().tenant_meta(tenant).await {
            meta.get_table_schema(db, table).unwrap().is_some()
        } else {
            return Err(HttpError::ParseLog {
                source: protocol_parser::JsonLogError::InvaildSyntax,
            });
        }
    };
    let mut lines = Vec::new();

    let time_column = time_column.unwrap_or_else(|| "time".to_string());
    let tag_columns = tag_columns.unwrap_or_default();

    let mut res: String = String::new();
    for (i, log) in logs.iter().enumerate() {
        let line = parse_to_line(log, table, &time_column, &tag_columns)
            .map_err(|e| HttpError::ParseLog { source: e })?;

        if let JsonProtocol::ESLog(log) = log {
            if let Command::Create(_) = log.command {
                if table_exist {
                    res = format!("The {}th command fails because the table '{}' already exists and cannot be created repeatedly\n", i + 1, table).to_string();
                    break;
                }
            }
            table_exist = true;
        }
        lines.push(line);
    }

    coord
        .write_lines(tenant, db, Precision::NS, lines, span.context().as_ref())
        .await
        .map_err(|e| {
            span.error(e.to_string());
            e
        })
        .context(CoordinatorSnafu)?;

    if res.is_empty() {
        Ok(ResponseBuilder::ok())
    } else {
        Ok(ResponseBuilder::new(warp::http::StatusCode::OK).build(res.into_bytes()))
    }
}

async fn coord_write_points_with_span_recorder(
    coord: &CoordinatorRef,
    tenant: &str,
    db: &str,
    precision: Precision,
    write_points_lines: Vec<Line<'_>>,
    span_context: Option<&SpanContext>,
) -> Result<usize, HttpError> {
    let span = Span::from_context("write points", span_context);
    coord
        .write_lines(
            tenant,
            db,
            precision,
            write_points_lines,
            span.context().as_ref(),
        )
        .await
        .map_err(|e| {
            span.error(e.to_string());
            CoordinatorSnafu.into_error(e)
        })
}

async fn sql_handle(
    query: &Query,
    dbms: &DBMSRef,
    fmt: ResultFormat,
    encoding: Option<Encoding>,
    span_ctx: Option<&SpanContext>,
    limiter: Arc<dyn RequestLimiter>,
    http_query_data_out: U64Counter,
) -> Result<Response, HttpError> {
    // debug!("prepare to execute: {:?}", query.content());
    let handle = {
        let span = Span::from_context("execute", span_ctx);
        dbms.execute(query, span.context().as_ref())
            .await
            .map_err(|err| {
                span.error(err.to_string());
                err
            })
            .context(QuerySnafu)?
    };

    let out = handle.result();

    let resp = HttpResponse::new(
        out,
        fmt.clone(),
        encoding,
        http_query_data_out.clone(),
        limiter.clone(),
    );

    let span = Span::from_context("build response", span_ctx);
    if !query.context().chunked() {
        let result = resp.wrap_batches_to_response().await;
        if let Err(err) = &result {
            if tskv::TskvError::vnode_broken_code(err.error_code().code()) {
                info!("tsm file broken {:?}, try read....", err);
                let handle = {
                    let span = Span::enter_with_parent("retry execute", &span);
                    dbms.execute(query, span.context().as_ref())
                        .await
                        .map_err(|err| {
                            span.error(err.to_string());
                            err
                        })
                        .context(QuerySnafu)?
                };
                let out = handle.result();
                let resp = HttpResponse::new(
                    out,
                    fmt,
                    encoding,
                    http_query_data_out.clone(),
                    limiter.clone(),
                );
                return resp.wrap_batches_to_response().await;
            }
        }

        result
    } else {
        resp.wrap_stream_to_response()
    }
}

async fn http_limiter_check_query(
    meta: &MetaRef,
    tenant: &str,
    req_len: usize,
) -> Result<(), HttpError> {
    let limiter = meta.limiter(tenant).await.context(MetaSnafu)?;
    limiter.check_http_queries().await.context(MetaSnafu)?;
    limiter
        .check_http_data_in(req_len)
        .await
        .context(MetaSnafu)?;
    Ok(())
}

async fn http_limiter_check_write(
    meta: &MetaRef,
    tenant: &str,
    req_len: usize,
) -> Result<(), HttpError> {
    let limiter = meta.limiter(tenant).await.context(MetaSnafu)?;
    limiter.check_http_writes().await.context(MetaSnafu)?;
    limiter
        .check_http_data_in(req_len)
        .await
        .context(MetaSnafu)?;
    Ok(())
}

fn http_record_query_metrics(
    metrics: &HttpMetrics,
    ctx: &Context,
    addr: &str,
    req_len: usize,
    start: Instant,
    api_type: HttpApiType,
) {
    let (tenant, user, db) = (ctx.tenant(), ctx.user().desc().name(), ctx.database());
    let db = if metrics_record_db(&api_type) {
        Some(db)
    } else {
        None
    };
    metrics
        .http_queries(tenant, user, db, addr, api_type)
        .inc_one();
    metrics
        .http_data_in(tenant, user, db, addr, api_type)
        .inc(req_len as u64);
    metrics
        .http_query_duration(tenant, user, db, addr, api_type)
        .record(start.elapsed());
}

fn http_record_write_metrics(
    metrics: &HttpMetrics,
    ctx: &Context,
    addr: &str,
    req_len: usize,
    start: Instant,
    api_type: HttpApiType,
) {
    let (tenant, user, db) = (ctx.tenant(), ctx.user().desc().name(), ctx.database());
    let db = if metrics_record_db(&api_type) {
        Some(db)
    } else {
        None
    };

    metrics
        .http_writes(tenant, user, db, addr, api_type)
        .inc_one();
    metrics
        .http_data_in(tenant, user, db, addr, api_type)
        .inc(req_len as u64);
    metrics
        .http_write_duration(tenant, user, db, addr, api_type)
        .record(start.elapsed());
}

fn http_response_time_and_flow_metrics(
    metrics: &HttpMetrics,
    addr: &str,
    flow: usize,
    start: Instant,
    api_type: HttpApiType,
) {
    metrics
        .http_response_time(addr, api_type)
        .record(start.elapsed());
    metrics.http_flow(addr, api_type).inc(flow as u64);
}

/*************** top ****************/
// Custom rejection handler that maps rejections into responses.
async fn handle_rejection(err: Rejection) -> Result<impl Reply, std::convert::Infallible> {
    if err.is_not_found() {
        Ok(ResponseBuilder::not_found())
    } else if err.find::<MethodNotAllowed>().is_some() {
        Ok(ResponseBuilder::method_not_allowed())
    } else if err.find::<PayloadTooLarge>().is_some() {
        Ok(ResponseBuilder::payload_too_large())
    } else if let Some(e) = err.find::<MissingHeader>() {
        let error_resp = ErrorResponse::new(&UnknownCodeWithMessage(e.to_string()));
        Ok(ResponseBuilder::bad_request(&error_resp))
    } else if let Some(e) = err.find::<HttpError>() {
        let resp: Response = e.into();
        Ok(resp)
    } else {
        trace::warn!("unhandled rejection: {:?}", err);
        Ok(ResponseBuilder::internal_server_error())
    }
}

fn record_context_in_span(span: &mut Span, context: &Context) {
    span.add_properties(|| {
        [
            ("user", context.user().desc().name().to_owned()),
            ("tenant", context.tenant().to_owned()),
            ("database", context.database().to_owned()),
            ("chunked", context.chunked().to_string()),
        ]
    });
}

/**************** bottom *****************/
#[cfg(test)]
mod test {
    use tokio::time;

    #[tokio::test]
    async fn test1() {
        // use futures_util::future::TryFutureExt;
        use tokio::sync::oneshot;
        use warp::Filter;

        let routes = warp::any().map(|| "Hello, World!");

        let (tx, rx) = oneshot::channel();

        let (_addr, server) =
            warp::serve(routes).bind_with_graceful_shutdown(([127, 0, 0, 1], 30001), async {
                rx.await.ok();
            });

        // Spawn the server into a runtime
        tokio::task::spawn(server);
        dbg!("Server started");
        time::sleep(time::Duration::from_secs(1)).await;
        // Later, start the shutdown...
        dbg!("Server stop");
        let _ = tx.send(());
    }
}
