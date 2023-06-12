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
use http_protocol::parameter::{SqlParam, WriteParam};
use http_protocol::response::ErrorResponse;
use meta::error::MetaError;
use metrics::metric_register::MetricsRegister;
use metrics::prom_reporter::PromReporter;
use metrics::{gather_metrics, sample_point_write_duration, sample_query_read_duration};
use models::auth::privilege::{DatabasePrivilege, Privilege, TenantObjectPrivilege};
use models::consistency_level::ConsistencyLevel;
use models::error_code::UnknownCodeWithMessage;
use models::oid::{Identifier, Oid};
use models::schema::{Precision, DEFAULT_CATALOG};
use protocol_parser::line_protocol::line_protocol_to_lines;
use protocol_parser::lines_convert::parse_lines_to_points;
use protocol_parser::open_tsdb::open_tsdb_to_lines;
use protocol_parser::{DataPoint, Line};
use protos::kv_service::WritePointsRequest;
use query::prom::remote_server::PromRemoteSqlServer;
use snafu::ResultExt;
use spi::query::config::StreamTriggerInterval;
use spi::server::dbms::DBMSRef;
use spi::server::prom::PromRemoteServerRef;
use spi::service::protocol::{Context, ContextBuilder, Query};
use spi::QueryError;
use tokio::sync::oneshot;
use trace::{debug, error, info, Span, SpanContext, SpanExt, SpanRecorder, TraceExporter};
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
use crate::http::QuerySnafu;
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
    tracer_collector: Option<Arc<dyn TraceExporter>>,
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
        tracer_collector: Option<Arc<dyn TraceExporter>>,
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
            tracer_collector,
            metrics_register,
            http_metrics,
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

    fn with_new_span_recorder(
        &self,
        span_name: &str,
    ) -> impl Filter<Extract = (SpanRecorder,), Error = Infallible> + Clone {
        let tracer_collector = self.tracer_collector.clone();
        let span_name = span_name.to_string();

        let span_recorder = move || match tracer_collector.clone() {
            Some(trace_collector) => {
                SpanRecorder::new(Some(Span::root(span_name.clone(), trace_collector)))
            }
            None => SpanRecorder::new(None),
        };

        warp::any().map(span_recorder)
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
            .and(self.with_http_metrics())
            .and(self.with_hostaddr())
            .and(self.with_new_span_recorder("rest sql request"))
            // construct_query
            .and_then(
                |req: Bytes,
                 header: Header,
                 param: SqlParam,
                 dbms: DBMSRef,
                 metrics: Arc<HttpMetrics>,
                 addr: String,
                 span_recorder: SpanRecorder| async move {
                    debug!(
                        "Receive http sql request, header: {:?}, param: {:?}",
                        header, param
                    );

                    let span_context = span_recorder.span_ctx();

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

                    let result = {
                        let span_recorder =
                            SpanRecorder::new(span_context.child_span("sql handle"));
                        sql_handle(&query, &dbms, result_fmt, span_recorder.span_ctx())
                            .await
                            .map_err(|e| {
                                trace::error!("Failed to handle http sql request, err: {}", e);
                                reject::custom(e)
                            })
                    };

                    let tenant = query.context().tenant();
                    let db = query.context().database();
                    let user = query.context().user_info().desc().name();

                    metrics.queries_inc(tenant, user, db, addr.as_str());

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
            .and(self.with_new_span_recorder("rest line protocol write"))
            .and_then(
                |req: Bytes,
                 header: Header,
                 param: WriteParam,
                 dbms: DBMSRef,
                 coord: CoordinatorRef,
                 metrics: Arc<HttpMetrics>,
                 addr: String,
                 span_recorder: SpanRecorder| async move {
                    let start = Instant::now();
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

                    let req_len = req.len() as u64;
                    let write_points_req = {
                        let mut span_recorder = SpanRecorder::new(
                            span_context.child_span("construct write lines points request"),
                        );
                        span_recorder.set_metadata("bytes", req.len());
                        construct_write_lines_points_request(req, ctx.database())
                            .map_err(reject::custom)?
                    };

                    let resp = coord_write_points_with_span_recorder(
                        &coord,
                        ctx.tenant().to_string(),
                        ConsistencyLevel::Any,
                        precision,
                        write_points_req,
                        span_context,
                    )
                    .await;

                    let (tenant, db, user, addr) = (
                        ctx.tenant(),
                        ctx.database(),
                        ctx.user_info().desc().name(),
                        addr.as_str(),
                    );

                    metrics.writes_inc(tenant, user, db, addr);
                    metrics.write_data_in_inc(tenant, user, db, addr, req_len);

                    sample_point_write_duration(
                        tenant,
                        db,
                        resp.is_ok(),
                        start.elapsed().as_millis() as f64,
                    );
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
            .and(self.with_new_span_recorder("rest open tsdb write"))
            .and_then(
                |req: Bytes,
                 header: Header,
                 param: WriteParam,
                 dbms: DBMSRef,
                 coord: CoordinatorRef,
                 metrics: Arc<HttpMetrics>,
                 addr: String,
                 span_recorder: SpanRecorder| async move {
                    let start = Instant::now();
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

                    let req_len = req.len() as u64;
                    let write_points_req = {
                        let mut span_recorder = SpanRecorder::new(
                            span_context.child_span("construct write tsdb points request"),
                        );
                        span_recorder.set_metadata("bytes", req.len());
                        construct_write_tsdb_points_request(req, &ctx).map_err(reject::custom)?
                    };
                    let resp = coord_write_points_with_span_recorder(
                        &coord,
                        ctx.tenant().to_string(),
                        ConsistencyLevel::Any,
                        precision,
                        write_points_req,
                        span_context,
                    )
                    .await;

                    let (tenant, db, user, addr) = (
                        ctx.tenant(),
                        ctx.database(),
                        ctx.user_info().desc().name(),
                        addr.as_str(),
                    );

                    metrics.writes_inc(tenant, user, db, addr);
                    metrics.write_data_in_inc(tenant, user, db, addr, req_len);

                    sample_point_write_duration(
                        tenant,
                        db,
                        resp.is_ok(),
                        start.elapsed().as_millis() as f64,
                    );
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
            .and(self.with_new_span_recorder("rest open tsdb put"))
            .and_then(
                |req: Bytes,
                 header: Header,
                 param: WriteParam,
                 dbms: DBMSRef,
                 coord: CoordinatorRef,
                 metrics: Arc<HttpMetrics>,
                 addr: String,
                 span_recorder: SpanRecorder| async move {
                    let start = Instant::now();
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

                    let req_len = req.len() as u64;
                    let write_points_req = {
                        let mut span_recorder = SpanRecorder::new(
                            span_context.child_span("construct write tsdb points json request"),
                        );
                        span_recorder.set_metadata("bytes", req.len());
                        construct_write_tsdb_points_json_request(req, &ctx)
                            .map_err(reject::custom)?
                    };
                    let resp = coord_write_points_with_span_recorder(
                        &coord,
                        ctx.tenant().to_string(),
                        ConsistencyLevel::Any,
                        precision,
                        write_points_req,
                        span_context,
                    )
                    .await;

                    let (tenant, db, user, addr) = (
                        ctx.tenant(),
                        ctx.database(),
                        ctx.user_info().desc().name(),
                        addr.as_str(),
                    );

                    metrics.writes_inc(tenant, user, db, addr);
                    metrics.write_data_in_inc(tenant, user, db, addr, req_len);

                    sample_point_write_duration(
                        tenant,
                        db,
                        resp.is_ok(),
                        start.elapsed().as_millis() as f64,
                    );
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
            .and(self.with_http_metrics())
            .and(self.with_prom_remote_server())
            .and(self.with_hostaddr())
            .and(self.with_new_span_recorder("rest prom remote read"))
            .and_then(
                |req: Bytes,
                 header: Header,
                 param: SqlParam,
                 dbms: DBMSRef,
                 metrics: Arc<HttpMetrics>,
                 prs: PromRemoteServerRef,
                 addr: String,
                 span_recorder: SpanRecorder| async move {
                    let start = Instant::now();
                    debug!(
                        "Receive rest prom remote read request, header: {:?}, param: {:?}",
                        header, param
                    );
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

                    let result = {
                        let mut span_recorder =
                            SpanRecorder::new(span_context.child_span("remote read"));
                        prs.remote_read(&context, req, span_recorder.span_ctx())
                            .await
                            .map(|_| ResponseBuilder::ok())
                            .map_err(|e| {
                                span_recorder.error(e.to_string());
                                trace::error!(
                                    "Failed to handle prom remote read request, err: {}",
                                    e
                                );
                                reject::custom(HttpError::from(e))
                            })
                    };

                    let tenant_name = context.tenant();
                    let username = context.user_info().desc().name();
                    let database_name = context.database();

                    metrics.queries_inc(tenant_name, username, database_name, addr.as_str());

                    sample_query_read_duration(
                        context.tenant(),
                        context.database(),
                        result.is_ok(),
                        start.elapsed().as_millis() as f64,
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
            .and(self.with_new_span_recorder("rest prom remote write"))
            .and_then(
                |req: Bytes,
                 header: Header,
                 param: WriteParam,
                 coord: CoordinatorRef,
                 dbms: DBMSRef,
                 prs: PromRemoteServerRef,
                 metrics: Arc<HttpMetrics>,
                 addr: String,
                 span_recorder: SpanRecorder| async move {
                    let start = Instant::now();
                    debug!(
                        "Receive rest prom remote write request, header: {:?}, param: {:?}",
                        header, param
                    );
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

                    let req_len = req.len() as u64;
                    let write_points_req = {
                        let mut span_recorder =
                            SpanRecorder::new(span_context.child_span("remote write"));
                        prs.remote_write(&ctx, req).map_err(|e| {
                            span_recorder.error(e.to_string());
                            trace::error!("Failed to handle prom remote write request, err: {}", e);
                            reject::custom(HttpError::from(e))
                        })?
                    };
                    let resp = coord_write_points_with_span_recorder(
                        &coord,
                        ctx.tenant().to_string(),
                        ConsistencyLevel::Any,
                        Precision::NS,
                        write_points_req,
                        span_context,
                    )
                    .await;

                    let (tenant, user, db, addr) = (
                        ctx.tenant(),
                        ctx.database(),
                        ctx.user_info().desc().name(),
                        addr.as_str(),
                    );

                    metrics.writes_inc(tenant, user, db, addr);
                    metrics.write_data_in_inc(tenant, user, db, addr, req_len);

                    sample_point_write_duration(
                        ctx.tenant(),
                        ctx.database(),
                        resp.is_ok(),
                        start.elapsed().as_millis() as f64,
                    );
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

fn construct_write_lines_points_request(
    req: Bytes,
    db: &str,
) -> Result<WritePointsRequest, HttpError> {
    let lines = String::from_utf8_lossy(req.as_ref());
    let line_protocol_lines = line_protocol_to_lines(&lines, Local::now().timestamp_nanos())
        .map_err(|e| HttpError::ParseLineProtocol { source: e })?;

    let points = parse_lines_to_points(db, &line_protocol_lines);

    let req = WritePointsRequest {
        version: 1,
        meta: None,
        points,
    };
    Ok(req)
}

fn construct_write_tsdb_points_request(
    req: Bytes,
    ctx: &Context,
) -> Result<WritePointsRequest, HttpError> {
    let lines = String::from_utf8_lossy(req.as_ref());
    let tsdb_protocol_lines = open_tsdb_to_lines(&lines, Local::now().timestamp_nanos())
        .map_err(|e| HttpError::ParseOpentsdbProtocol { source: e })?;

    let points = parse_lines_to_points(ctx.database(), &tsdb_protocol_lines);

    let req = WritePointsRequest {
        version: 1,
        meta: None,
        points,
    };
    Ok(req)
}

fn construct_write_tsdb_points_json_request(
    req: Bytes,
    ctx: &Context,
) -> Result<WritePointsRequest, HttpError> {
    let lines = String::from_utf8_lossy(req.as_ref());
    let tsdb_datapoints = match serde_json::from_str::<DataPoint>(&lines) {
        Ok(datapoint) => vec![datapoint],
        Err(_) => match serde_json::from_str::<Vec<DataPoint>>(&lines) {
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

    let points = parse_lines_to_points(ctx.database(), &tsdb_datapoints);

    let req = WritePointsRequest {
        version: 1,
        meta: None,
        points,
    };
    Ok(req)
}

async fn coord_write_points_with_span_recorder(
    coord: &CoordinatorRef,
    tenant: String,
    consistency_level: ConsistencyLevel,
    precision: Precision,
    write_points_req: WritePointsRequest,
    span_context: Option<&SpanContext>,
) -> Result<(), HttpError> {
    let mut span_recorder = SpanRecorder::new(span_context.child_span("write points"));
    coord
        .write_points(
            tenant,
            consistency_level,
            precision,
            write_points_req,
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
    let resp = HttpResponse::new(out, fmt.clone());

    let _response_span_recorder = SpanRecorder::new(span_ctx.child_span("build response"));
    if !query.context().chunked() {
        let result = resp.wrap_batches_to_response().await;
        if let Err(err) = &result {
            if err.to_string().contains("read tsm block file error") {
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
                let resp = HttpResponse::new(out, fmt);
                return resp.wrap_batches_to_response().await;
            }
        }

        result
    } else {
        resp.wrap_stream_to_response()
    }
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
