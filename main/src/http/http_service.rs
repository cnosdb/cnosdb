#![allow(clippy::too_many_arguments)]

use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt;
use std::fmt::Display;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use chrono::Local;
use config::TLSConfig;
use coordinator::service::CoordinatorRef;
use http_protocol::header::{ACCEPT, AUTHORIZATION, PRIVATE_KEY};
use http_protocol::parameter::{DebugParam, SqlParam, WriteParam};
use http_protocol::response::ErrorResponse;
use meta::error::MetaError;
use meta::limiter::RequestLimiter;
use meta::model::MetaRef;
use metrics::count::U64Counter;
use metrics::gather_metrics;
use metrics::metric_register::MetricsRegister;
use metrics::prom_reporter::PromReporter;
use models::auth::privilege::{DatabasePrivilege, Privilege, TenantObjectPrivilege};
use models::error_code::UnknownCodeWithMessage;
use models::oid::{Identifier, Oid};
use models::schema::{Precision, DEFAULT_CATALOG};
use protocol_parser::line_protocol::line_protocol_to_lines;
use protocol_parser::open_tsdb::open_tsdb_to_lines;
use protocol_parser::{DataPoint, Line};
use query::prom::remote_server::PromRemoteSqlServer;
use snafu::ResultExt;
use spi::query::config::StreamTriggerInterval;
use spi::server::dbms::DBMSRef;
use spi::server::prom::PromRemoteServerRef;
use spi::service::protocol::{Context, ContextBuilder, Query};
use spi::QueryError;
use tokio::sync::oneshot;
use trace::{debug, error, info, SpanContext, SpanExt, SpanRecorder};
use trace_http::ctx::{SpanContextExtractor, DEFAULT_TRACE_HEADER_NAME};
use utils::backtrace;
use warp::hyper::body::Bytes;
use warp::hyper::Body;
use warp::reject::{MethodNotAllowed, MissingHeader, PayloadTooLarge};
use warp::reply::Response;
use warp::{header, reject, Filter, Rejection, Reply};

use super::header::Header;
use super::Error as HttpError;
use crate::http::metrics::HttpMetrics;
use crate::http::response::{HttpResponse, ResponseBuilder};
use crate::http::result_format::{get_result_format_from_header, ResultFormat};
use crate::http::{meta_err_to_reject, QuerySnafu};
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
    span_context_extractor: Arc<SpanContextExtractor>,
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
        span_context_extractor: Arc<SpanContextExtractor>,
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
            span_context_extractor,
        }
    }

    /// user_id
    /// database
    /// =》
    /// Authorization
    /// Accept
    fn handle_header(&self) -> impl Filter<Extract = (Header,), Error = warp::Rejection> + Clone {
        header::optional::<String>(ACCEPT.as_str())
            .and(header::<String>(AUTHORIZATION.as_str()))
            .and(header::optional::<String>(PRIVATE_KEY))
            .and_then(|accept, authorization, private_key| async move {
                let res: Result<Header, warp::Rejection> =
                    Ok(Header::with_private_key(accept, authorization, private_key));
                res
            })
    }

    fn handle_span_header(
        &self,
    ) -> impl Filter<Extract = (Option<SpanContext>,), Error = warp::Rejection> + Clone {
        let span_context_extractor = self.span_context_extractor.clone();

        header::optional::<String>(DEFAULT_TRACE_HEADER_NAME).and_then(
            move |trace: Option<String>| {
                let result = span_context_extractor
                    .extract_from_value(DEFAULT_TRACE_HEADER_NAME, trace)
                    .map_err(HttpError::from)
                    .map_err(reject::custom);

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

    fn routes_bundle(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        self.ping()
            .or(self.query())
            .or(self.write_line_protocol())
            .or(self.metrics())
            .or(self.print_meta())
            .or(self.debug_pprof())
            .or(self.debug_jeprof())
            .or(self.prom_remote_read())
            .or(self.prom_remote_write())
            .or(self.backtrace())
            .or(self.write_open_tsdb())
            .or(self.put_open_tsdb())
            .or(self.print_raft())
    }

    fn routes_query(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        self.ping()
            .or(self.query())
            .or(self.metrics())
            .or(self.print_meta())
            .or(self.debug_pprof())
            .or(self.debug_jeprof())
            .or(self.prom_remote_read())
            .or(self.backtrace())
            .or(self.print_raft())
    }

    fn routes_store(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        self.ping()
            .or(self.write_line_protocol())
            .or(self.metrics())
            .or(self.print_meta())
            .or(self.debug_pprof())
            .or(self.debug_jeprof())
            .or(self.prom_remote_write())
            .or(self.write_open_tsdb())
            .or(self.put_open_tsdb())
            .or(self.backtrace())
            .or(self.print_raft())
    }

    fn ping(&self) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("api" / "v1" / "ping")
            .and(warp::get().or(warp::head()))
            .map(|_| {
                let mut resp = HashMap::new();
                resp.insert("version", VERSION.as_str());
                resp.insert("status", "healthy");
                warp::reply::json(&resp)
            })
    }
    fn backtrace(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("debug" / "backtrace")
            .and(warp::get().or(warp::head()))
            .map(|_| {
                let res = backtrace::backtrace();
                let mut resp = HashMap::new();
                resp.insert("taskdump_tree:", res);
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
            .and(self.with_http_metrics())
            .and(self.with_hostaddr())
            .and(self.handle_span_header())
            // construct_query
            .and_then(
                |req: Bytes,
                 header: Header,
                 param: SqlParam,
                 dbms: DBMSRef,
                 meta: MetaRef,
                 metrics: Arc<HttpMetrics>,
                 addr: String,
                 parent_span_ctx: Option<SpanContext>| async move {
                    let start = Instant::now();
                    debug!(
                        "Receive http sql request, header: {:?}, param: {:?}",
                        header, param
                    );
                    let span_recorder =
                        SpanRecorder::new(parent_span_ctx.child_span("rest sql request"));
                    let span_context = span_recorder.span_ctx();
                    let req_len = req.len();
                    let query = {
                        let mut span_recorder =
                            SpanRecorder::new(span_context.child_span("authenticate"));

                        // Parse req、header and param to construct query request
                        let query = construct_query(req, &header, param, dbms.clone())
                            .await
                            .map_err(reject::custom)?;

                        span_recorder.record(query)
                    };

                    let result_fmt = get_result_format_from_header(&header)?;
                    http_limiter_check_query(&meta, query.context().tenant(), req_len)
                        .await
                        .map_err(reject::custom)?;

                    let tenant = query.context().tenant();
                    let db = query.context().database();
                    let user = query.context().user_info().desc().name();
                    let addr_str = addr.as_str();

                    let result = {
                        let mut span_recorder =
                            SpanRecorder::new(span_context.child_span("sql handle"));
                        let limiter = meta
                            .limiter(query.context().tenant())
                            .await
                            .map_err(meta_err_to_reject)?;
                        let http_data_out = metrics.http_data_out(tenant, user, db, addr_str);
                        sql_handle(
                            &query,
                            &dbms,
                            result_fmt,
                            span_recorder.span_ctx(),
                            limiter,
                            http_data_out,
                        )
                        .await
                        .map_err(|e| {
                            span_recorder.error(e.to_string());
                            trace::error!("Failed to handle http sql request, err: {}", e);
                            reject::custom(e)
                        })
                    };

                    http_record_query_metrics(&metrics, query.context(), &addr, req_len, start);
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
                |req: Bytes,
                 header: Header,
                 param: WriteParam,
                 dbms: DBMSRef,
                 coord: CoordinatorRef,
                 metrics: Arc<HttpMetrics>,
                 addr: String,
                 parent_span_ctx: Option<SpanContext>| async move {
                    let start = Instant::now();
                    let span_recorder =
                        SpanRecorder::new(parent_span_ctx.child_span("rest line protocol write"));
                    let span_context = span_recorder.span_ctx();

                    let ctx = {
                        let mut span_recorder =
                            SpanRecorder::new(span_context.child_span("construct write context"));
                        let ctx = construct_write_context_and_check_privilege(
                            header,
                            param,
                            dbms,
                            coord.clone(),
                        )
                        .await
                        .map_err(reject::custom)?;
                        span_recorder.record(ctx)
                    };

                    let req_len = req.len();
                    http_limiter_check_write(&coord.meta_manager(), ctx.tenant(), req_len).await?;

                    let precision = Precision::new(ctx.precision()).unwrap_or(Precision::NS);

                    let req_len = req.len();
                    let write_points_lines = {
                        let mut span_recorder =
                            SpanRecorder::new(span_context.child_span("try parse req to lines"));
                        span_recorder.set_metadata("bytes", req.len());
                        try_parse_req_to_lines(&req).map_err(reject::custom)?
                    };

                    let resp = coord_write_points_with_span_recorder(
                        &coord,
                        ctx.tenant(),
                        ctx.database(),
                        precision,
                        write_points_lines,
                        span_context,
                    )
                    .await;

                    http_record_write_metrics(&metrics, &ctx, &addr, req_len, start);
                    resp.map(|_| ResponseBuilder::ok()).map_err(reject::custom)
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
                |req: Bytes,
                 header: Header,
                 param: WriteParam,
                 dbms: DBMSRef,
                 coord: CoordinatorRef,
                 metrics: Arc<HttpMetrics>,
                 addr: String,
                 parent_span_ctx: Option<SpanContext>| async move {
                    let start = Instant::now();
                    let span_recorder =
                        SpanRecorder::new(parent_span_ctx.child_span("rest open tsdb write"));
                    let span_context = span_recorder.span_ctx();

                    let ctx = {
                        let mut span_recorder =
                            SpanRecorder::new(span_context.child_span("construct write context"));
                        let ctx = construct_write_context_and_check_privilege(
                            header,
                            param,
                            dbms,
                            coord.clone(),
                        )
                        .await
                        .map_err(reject::custom)?;
                        span_recorder.record(ctx)
                    };

                    let precision = Precision::new(ctx.precision()).unwrap_or(Precision::NS);

                    let req_len = req.len();
                    http_limiter_check_write(&coord.meta_manager(), ctx.tenant(), req_len)
                        .await
                        .map_err(reject::custom)?;

                    let write_points_req = {
                        let mut span_recorder = SpanRecorder::new(
                            span_context.child_span("construct write tsdb points request"),
                        );
                        span_recorder.set_metadata("bytes", req.len());
                        construct_write_tsdb_points_request(&req).map_err(reject::custom)?
                    };
                    let resp = coord_write_points_with_span_recorder(
                        &coord,
                        ctx.tenant(),
                        ctx.database(),
                        precision,
                        write_points_req,
                        span_context,
                    )
                    .await;

                    http_record_write_metrics(&metrics, &ctx, &addr, req_len, start);
                    resp.map(|_| ResponseBuilder::ok()).map_err(reject::custom)
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
                |req: Bytes,
                 header: Header,
                 param: WriteParam,
                 dbms: DBMSRef,
                 coord: CoordinatorRef,
                 metrics: Arc<HttpMetrics>,
                 addr: String,
                 parent_span_ctx: Option<SpanContext>| async move {
                    let start = Instant::now();
                    let span_recorder =
                        SpanRecorder::new(parent_span_ctx.child_span("rest open tsdb put"));
                    let span_context = span_recorder.span_ctx();

                    let ctx = {
                        let mut span_recorder =
                            SpanRecorder::new(span_context.child_span("construct write context"));
                        let ctx = construct_write_context_and_check_privilege(
                            header,
                            param,
                            dbms,
                            coord.clone(),
                        )
                        .await
                        .map_err(reject::custom)?;
                        span_recorder.record(ctx)
                    };

                    let precision = Precision::new(ctx.precision()).unwrap_or(Precision::NS);

                    let req_len = req.len();
                    http_limiter_check_write(&coord.meta_manager(), ctx.tenant(), req_len)
                        .await
                        .map_err(reject::custom)?;

                    let write_points_req = {
                        let mut span_recorder = SpanRecorder::new(
                            span_context.child_span("construct write tsdb points json request"),
                        );
                        span_recorder.set_metadata("bytes", req.len());
                        construct_write_tsdb_points_json_request(&req).map_err(reject::custom)?
                    };
                    let resp = coord_write_points_with_span_recorder(
                        &coord,
                        ctx.tenant(),
                        ctx.database(),
                        precision,
                        write_points_req,
                        span_context,
                    )
                    .await;

                    http_record_write_metrics(&metrics, &ctx, &addr, req_len, start);
                    resp.map(|_| ResponseBuilder::ok()).map_err(reject::custom)
                },
            )
    }

    fn print_meta(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("api" / "v1" / "meta")
            .and(self.handle_header())
            .and(self.with_coord())
            .and_then(|_header: Header, coord: CoordinatorRef| async move {
                let tenant = DEFAULT_CATALOG.to_string();

                let meta_client = match coord.tenant_meta(&tenant).await {
                    Some(client) => client,
                    None => {
                        return Err(reject::custom(HttpError::Meta {
                            source: meta::error::MetaError::TenantNotFound { tenant },
                        }));
                    }
                };
                let data = meta_client.print_data();

                Ok(data)
            })
    }

    fn print_raft(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("api" / "v1" / "raft")
            .and(warp::query::<DebugParam>())
            .and(self.with_coord())
            .and_then(|param: DebugParam, coord: CoordinatorRef| async move {
                let raft_manager = coord.raft_manager();
                let data = raft_manager.metrics(param.id.unwrap_or(0)).await;

                let res: Result<String, warp::Rejection> = Ok(data);
                res
            })
    }

    fn debug_pprof(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("debug" / "pprof").and_then(|| async move {
            #[cfg(unix)]
            {
                let res = utils::pprof_tools::gernate_pprof().await;
                info!("debug pprof: {:?}", res);
                match res {
                    Ok(v) => Ok(v),
                    Err(e) => Err(reject::custom(HttpError::PProfError { reason: e })),
                }
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
        warp::path!("debug" / "jeprof").and_then(|| async move {
            #[cfg(unix)]
            {
                let res = utils::pprof_tools::gernate_jeprof().await;
                info!("debug jeprof: {:?}", res);
                match res {
                    Ok(v) => Ok(v),
                    Err(e) => Err(reject::custom(HttpError::PProfError { reason: e })),
                }
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
            .map(|register: Arc<MetricsRegister>| {
                let mut buffer = gather_metrics();
                let mut prom_reporter = PromReporter::new(&mut buffer);
                register.report(&mut prom_reporter);
                Response::new(Body::from(buffer))
            })
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
                 metrics: Arc<HttpMetrics>,
                 prs: PromRemoteServerRef,
                 addr: String,
                 parent_span_ctx: Option<SpanContext>| async move {
                    let start = Instant::now();
                    debug!(
                        "Receive rest prom remote read request, header: {:?}, param: {:?}",
                        header, param
                    );
                    let span_recorder =
                        SpanRecorder::new(parent_span_ctx.child_span("rest prom remote read"));
                    let span_context = span_recorder.span_ctx();

                    // Parse req、header and param to construct query request
                    let context = {
                        let mut span_recorder =
                            SpanRecorder::new(span_context.child_span("construct context"));
                        span_recorder.set_metadata("bytes", req.len());
                        let ctx = construct_read_context(&header, param, dbms)
                            .await
                            .map_err(reject::custom)?;
                        span_recorder.record(ctx)
                    };
                    let req_len = req.len();

                    http_limiter_check_query(&meta, context.tenant(), req_len)
                        .await
                        .map_err(reject::custom)?;

                    let tenant_name = context.tenant();
                    let username = context.user_info().desc().name();
                    let database_name = context.database();

                    let http_query_data_out =
                        metrics.http_data_out(tenant_name, username, database_name, addr.as_str());

                    let result = {
                        let mut span_recorder =
                            SpanRecorder::new(span_context.child_span("remote read"));
                        prs.remote_read(&context, req, span_recorder.span_ctx())
                            .await
                            .map_err(|e| {
                                span_recorder.error(e.to_string());
                                trace::error!(
                                    "Failed to handle prom remote read request, err: {}",
                                    e
                                );
                                reject::custom(HttpError::from(e))
                            })
                            .map(|b| {
                                http_query_data_out.inc(b.len() as u64);
                                b
                            })
                    };

                    http_record_query_metrics(&metrics, &context, &addr, req_len, start);
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
                    let span_recorder =
                        SpanRecorder::new(parent_span_ctx.child_span("rest prom remote write"));
                    let span_context = span_recorder.span_ctx();

                    // Parse req、header and param to construct query request
                    let ctx = {
                        let mut span_recorder =
                            SpanRecorder::new(span_context.child_span("construct context"));
                        span_recorder.set_metadata("bytes", req.len());
                        let ctx = construct_write_context_and_check_privilege(
                            header,
                            param,
                            dbms,
                            coord.clone(),
                        )
                        .await
                        .map_err(reject::custom)?;
                        span_recorder.record(ctx)
                    };

                    let req_len = req.len();
                    http_limiter_check_write(&coord.meta_manager(), ctx.tenant(), req_len)
                        .await
                        .map_err(reject::custom)?;

                    let mut span_recorder =
                        SpanRecorder::new(span_context.child_span("remote write"));
                    let prom_write_request = prs.remote_write(req).map_err(|e| {
                        span_recorder.error(e.to_string());
                        error!("Failed to handle prom remote write request, err: {}", e);
                        reject::custom(HttpError::from(e))
                    })?;
                    let write_request = prs
                        .prom_write_request_to_lines(&prom_write_request)
                        .map_err(|e| {
                            span_recorder.error(e.to_string());
                            error!("Failed to handle prom remote write request, err: {}", e);
                            reject::custom(HttpError::from(e))
                        })?;

                    let resp = coord_write_points_with_span_recorder(
                        &coord,
                        ctx.tenant(),
                        ctx.database(),
                        Precision::NS,
                        write_request,
                        span_context,
                    )
                    .await;
                    http_record_write_metrics(&metrics, &ctx, &addr, req_len, start);

                    resp.map(|_| ResponseBuilder::ok()).map_err(reject::custom)
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
                    let routes = self.routes_bundle().recover(handle_rejection);
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
                    let routes = self.routes_bundle().recover(handle_rejection);
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
) -> Result<Query, HttpError> {
    let context = construct_read_context(header, param, dbms).await?;

    Ok(Query::new(
        context,
        String::from_utf8_lossy(req.as_ref()).to_string(),
    ))
}

async fn construct_read_context(
    header: &Header,
    param: SqlParam,
    dbms: DBMSRef,
) -> Result<Context, HttpError> {
    let user_info = header.try_get_basic_auth()?;

    let tenant = param.tenant;
    let user = dbms
        .authenticate(&user_info, tenant.as_deref())
        .await
        .context(QuerySnafu)?;

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

    let user = dbms.authenticate(&user_info, tenant.as_deref()).await?;

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
        })?
        .tenant()
        .id();

    let privilege = Privilege::TenantObject(
        TenantObjectPrivilege::Database(
            DatabasePrivilege::Write,
            Some(context.database().to_string()),
        ),
        Some(tenant_id),
    );
    if !context.user_info().check_privilege(&privilege) {
        return Err(HttpError::Query {
            source: QueryError::InsufficientPrivileges {
                privilege: format!("{privilege}"),
            },
        });
    }
    Ok(context)
}

fn try_parse_req_to_lines(req: &Bytes) -> Result<Vec<Line>, HttpError> {
    let lines = unsafe { std::str::from_utf8_unchecked(req.as_ref()) };
    let line_protocol_lines = line_protocol_to_lines(lines, Local::now().timestamp_nanos())
        .map_err(|e| HttpError::ParseLineProtocol { source: e })?;

    Ok(line_protocol_lines)
}

fn construct_write_tsdb_points_request(req: &Bytes) -> Result<Vec<Line>, HttpError> {
    let lines = unsafe { std::str::from_utf8_unchecked(req.as_ref()) };

    let tsdb_protocol_lines = open_tsdb_to_lines(lines, Local::now().timestamp_nanos())
        .map_err(|e| HttpError::ParseOpentsdbProtocol { source: e })?;

    Ok(tsdb_protocol_lines)
}

fn construct_write_tsdb_points_json_request(req: &Bytes) -> Result<Vec<Line>, HttpError> {
    let lines = unsafe { std::str::from_utf8_unchecked(req.as_ref()) };
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

async fn coord_write_points_with_span_recorder(
    coord: &CoordinatorRef,
    tenant: &str,
    db: &str,
    precision: Precision,
    write_points_lines: Vec<Line<'_>>,
    span_context: Option<&SpanContext>,
) -> Result<usize, HttpError> {
    let mut span_recorder = SpanRecorder::new(span_context.child_span("write points"));
    coord
        .write_lines(
            tenant,
            db,
            precision,
            write_points_lines,
            span_recorder.span_ctx(),
        )
        .await
        .map_err(|e| {
            span_recorder.error(e.to_string());
            e.into()
        })
}

async fn sql_handle(
    query: &Query,
    dbms: &DBMSRef,
    fmt: ResultFormat,
    span_ctx: Option<&SpanContext>,
    limiter: Arc<dyn RequestLimiter>,
    http_query_data_out: U64Counter,
) -> Result<Response, HttpError> {
    debug!("prepare to execute: {:?}", query.content());
    let handle = {
        let mut execute_span_recorder = SpanRecorder::new(span_ctx.child_span("execute"));
        dbms.execute(query, execute_span_recorder.span_ctx())
            .await
            .map_err(|err| {
                execute_span_recorder.error(err.to_string());
                err
            })?
    };

    let out = handle.result();

    let resp = HttpResponse::new(
        out,
        fmt.clone(),
        http_query_data_out.clone(),
        limiter.clone(),
    );

    let _response_span_recorder = SpanRecorder::new(span_ctx.child_span("build response"));
    if !query.context().chunked() {
        let result = resp.wrap_batches_to_response().await;
        if let Err(err) = &result {
            if tskv::Error::vnode_broken_code(err.error_code().code()) {
                info!("tsm file broken {:?}, try read....", err);
                let handle = {
                    let mut execute_span_recorder =
                        SpanRecorder::new(span_ctx.child_span("retry execute"));
                    dbms.execute(query, execute_span_recorder.span_ctx())
                        .await
                        .map_err(|err| {
                            execute_span_recorder.error(err.to_string());
                            err
                        })?
                };
                let out = handle.result();
                let resp =
                    HttpResponse::new(out, fmt, http_query_data_out.clone(), limiter.clone());
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
    let limiter = meta.limiter(tenant).await?;
    limiter.check_http_queries().await?;
    limiter.check_http_data_in(req_len).await?;
    Ok(())
}

async fn http_limiter_check_write(
    meta: &MetaRef,
    tenant: &str,
    req_len: usize,
) -> Result<(), HttpError> {
    let limiter = meta.limiter(tenant).await?;
    limiter.check_http_writes().await?;
    limiter.check_http_data_in(req_len).await?;
    Ok(())
}

fn http_record_query_metrics(
    metrics: &HttpMetrics,
    ctx: &Context,
    addr: &str,
    req_len: usize,
    start: Instant,
) {
    let (tenant, user, db) = (ctx.tenant(), ctx.user_info().desc().name(), ctx.database());
    metrics.http_queries(tenant, user, db, addr).inc_one();
    metrics
        .http_data_in(tenant, user, db, addr)
        .inc(req_len as u64);
    metrics
        .http_query_duration(tenant, user, db, addr)
        .record(start.elapsed());
}

fn http_record_write_metrics(
    metrics: &HttpMetrics,
    ctx: &Context,
    addr: &str,
    req_len: usize,
    start: Instant,
) {
    let (tenant, user, db) = (ctx.tenant(), ctx.user_info().desc().name(), ctx.database());

    metrics.http_writes(tenant, user, db, addr).inc_one();
    metrics
        .http_data_in(tenant, user, db, addr)
        .inc(req_len as u64);
    metrics
        .http_write_duration(tenant, user, db, addr)
        .record(start.elapsed());
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
