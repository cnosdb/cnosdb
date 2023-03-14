#![allow(clippy::too_many_arguments)]

use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt;
use std::fmt::Display;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Instant;

use chrono::Local;
use config::{Config, TLSConfig};
use coordinator::service::CoordinatorRef;
use http_protocol::header::{ACCEPT, AUTHORIZATION};
use http_protocol::parameter::{SqlParam, WriteParam};
use http_protocol::response::ErrorResponse;
use line_protocol::{line_protocol_to_lines, parse_lines_to_points};
use meta::error::MetaError;
use metrics::metric_register::{MetricsRegister, MetricsRegisterRef};
use metrics::prom_reporter::PromReporter;
use metrics::{gather_metrics, sample_point_write_duration, sample_query_read_duration};
use models::auth::privilege::{DatabasePrivilege, Privilege, TenantObjectPrivilege};
use models::consistency_level::ConsistencyLevel;
use models::error_code::UnknownCodeWithMessage;
use models::oid::{Identifier, Oid};
use models::schema::DEFAULT_CATALOG;
use once_cell::sync::Lazy;
use protos::kv_service::WritePointsRequest;
use snafu::ResultExt;
use spi::server::dbms::DBMSRef;
use spi::server::prom::PromRemoteServerRef;
use spi::service::protocol::{Context, ContextBuilder, Query};
use spi::QueryError;
use tokio::sync::oneshot;
use trace::{debug, info};
use utils::backtrace;
use warp::hyper::body::Bytes;
use warp::hyper::Body;
use warp::reject::{MethodNotAllowed, MissingHeader, PayloadTooLarge};
use warp::reply::Response;
use warp::{header, reject, Filter, Rejection, Reply};

use super::header::Header;
use super::Error as HttpError;
use crate::http::metrics::HttpMetrics;
use crate::http::response::ResponseBuilder;
use crate::http::result_format::{fetch_record_batches, ResultFormat};
use crate::http::QuerySnafu;
use crate::server::ServiceHandle;
use crate::spi::service::Service;
use crate::{server, VERSION};

#[derive(Copy, Clone)]
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
    http_service_ctx: Arc<HttpServiceContext>,
    handle: Option<ServiceHandle<()>>,
}

impl HttpService {
    pub fn ctx(&self) -> &Arc<HttpServiceContext> {
        &self.http_service_ctx
    }
    pub fn mode(&self) -> ServerMode {
        self.ctx().mode()
    }
    pub fn addr(&self) -> SocketAddr {
        self.ctx().addr()
    }
}

pub struct HttpServiceContext {
    config: Arc<Config>,
    dbms: DBMSRef,
    prs: PromRemoteServerRef,
    mode: ServerMode,
    coord: CoordinatorRef,
    metrics_register: MetricsRegisterRef,
    http_metrics: Arc<HttpMetrics>,
}

impl HttpServiceContext {
    pub fn new(
        config: Arc<Config>,
        dbms: DBMSRef,
        prs: PromRemoteServerRef,
        mode: ServerMode,
        coord: CoordinatorRef,
        metrics_register: MetricsRegisterRef,
        http_metrics: Arc<HttpMetrics>,
    ) -> Self {
        Self {
            config,
            dbms,
            prs,
            mode,
            coord,
            metrics_register,
            http_metrics,
        }
    }
    pub fn addr(&self) -> SocketAddr {
        self.config
            .cluster
            .http_listen_addr
            .parse()
            .expect("Http address error")
    }
    pub fn config(&self) -> &Arc<Config> {
        &self.config
    }
    pub fn dbms(&self) -> &DBMSRef {
        &self.dbms
    }
    pub fn prom_remote_server(&self) -> &PromRemoteServerRef {
        &self.prs
    }
    pub fn mode(&self) -> ServerMode {
        self.mode
    }
    pub fn coord(&self) -> &CoordinatorRef {
        &self.coord
    }
    pub fn metrics_register(&self) -> &Arc<MetricsRegister> {
        &self.metrics_register
    }
    pub fn metrics(&self) -> &Arc<HttpMetrics> {
        &self.http_metrics
    }
    pub fn query_sql_limit(&self) -> u64 {
        self.config.query.query_sql_limit
    }
    pub fn write_sql_limit(&self) -> u64 {
        self.config.query.write_sql_limit
    }
}

impl HttpService {
    pub fn new(http_service_ctx: Arc<HttpServiceContext>) -> Self {
        Self {
            http_service_ctx,
            handle: None,
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
            .and_then(|accept, authorization| async move {
                let res: Result<Header, warp::Rejection> = Ok(Header::with(accept, authorization));
                res
            })
    }

    fn with_http_service_ctx(
        &self,
    ) -> impl Filter<Extract = (Arc<HttpServiceContext>,), Error = Infallible> + Clone {
        let ctx = self.ctx().clone();
        warp::any().map(move || ctx.clone())
    }

    fn query_body(&self) -> impl Filter<Extract = (Bytes,), Error = Rejection> + Clone {
        let query_body_limit = self.ctx().config().query_body_limit();
        warp::post()
            .and(warp::body::content_length_limit(query_body_limit))
            .and(warp::body::bytes())
    }

    fn write_body(&self) -> impl Filter<Extract = (Bytes,), Error = Rejection> + Clone {
        let write_body_limit = self.ctx().config().query_body_limit();
        warp::post()
            .and(warp::body::content_length_limit(write_body_limit))
            .and(warp::body::bytes())
    }

    fn with_ctx(
        &self,
    ) -> impl Filter<Extract = (Arc<HttpServiceContext>,), Error = Infallible> + Clone {
        let ctx = self.ctx().clone();
        warp::any().map(move || ctx.clone())
    }

    fn with_coord(&self) -> impl Filter<Extract = (CoordinatorRef,), Error = Infallible> + Clone {
        let coord = self.ctx().coord().clone();
        warp::any().map(move || coord.clone())
    }

    fn tls_config(&self) -> Option<&TLSConfig> {
        self.ctx().config().tls_config()
    }

    fn with_metrics_register(
        &self,
    ) -> impl Filter<Extract = (Arc<MetricsRegister>,), Error = Infallible> + Clone {
        let register = self.ctx().metrics_register().clone();
        warp::any().map(move || register.clone())
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
            .or(self.backtrace())
    }

    fn ping(&self) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        pub static RESP: Lazy<HashMap<&'static str, &'static str>> = Lazy::new(|| {
            let mut res = HashMap::new();
            res.insert("version", VERSION.as_str());
            res.insert("status", "healthy");
            res
        });
        warp::path!("api" / "v1" / "ping")
            .and(warp::get().or(warp::head()))
            .map(|_| warp::reply::json(RESP.deref()))
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
            .and(self.query_body())
            .and(self.handle_header())
            .and(self.with_ctx())
            .and(warp::query::<SqlParam>())
            .and_then(
                |req: Bytes,
                 header: Header,
                 http_service_ctx: Arc<HttpServiceContext>,
                 param: SqlParam| async move {
                    let start = Instant::now();
                    debug!(
                        "Receive http sql request, header: {:?}, param: {:?}",
                        header, param
                    );
                    // Parse req、header and param to construct query request
                    let query = construct_query(req, &header, param, http_service_ctx.dbms())
                        .await
                        .map_err(|e| {
                            sample_query_read_duration("", "", false, 0.0);
                            reject::custom(e)
                        })?;
                    let result = sql_handle(&query, header, http_service_ctx.dbms())
                        .await
                        .map_err(|e| {
                            trace::error!("Failed to handle http sql request, err: {}", e);
                            reject::custom(e)
                        });
                    let tenant = query.context().tenant();
                    let db = query.context().database();
                    let user = query.context().user_info().desc().name();

                    http_service_ctx.metrics().queries_inc(tenant, user, db);

                    sample_query_read_duration(
                        tenant,
                        db,
                        result.is_ok(),
                        start.elapsed().as_millis() as f64,
                    );
                    result
                },
            )
    }

    fn write_line_protocol(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("api" / "v1" / "write")
            .and(self.write_body())
            .and(self.handle_header())
            .and(warp::query::<WriteParam>())
            .and(self.with_ctx())
            .and_then(
                |req: Bytes,
                 header: Header,
                 param: WriteParam,
                 http_service_context: Arc<HttpServiceContext>| async move {
                    let coord = http_service_context.coord();
                    let start = Instant::now();
                    let ctx = construct_write_context(
                        header,
                        param,
                        http_service_context.dbms(),
                        http_service_context.coord(),
                    )
                    .await
                    .map_err(reject::custom)?;

                    let req = construct_write_points_request(req, &ctx).map_err(reject::custom)?;

                    let resp: Result<(), HttpError> = coord
                        .write_points(ctx.tenant().to_string(), ConsistencyLevel::Any, req)
                        .await
                        .map_err(|e| e.into());

                    let (tenant, db, user) =
                        (ctx.tenant(), ctx.database(), ctx.user_info().desc().name());

                    http_service_context.metrics().writes_inc(tenant, user, db);

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

    fn print_meta(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("api" / "v1" / "meta")
            .and(self.handle_header())
            .and(self.with_coord())
            .and_then(|_, coord: CoordinatorRef| async move {
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
            let res = utils::pprof_tools::gernate_pprof().await;
            info!("debug pprof: {:?}", res);
            match res {
                Ok(v) => Ok(v),
                Err(e) => Err(reject::custom(HttpError::PProfError { reason: e })),
            }
        })
    }

    fn debug_jeprof(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("debug" / "jeprof").and_then(|| async move {
            let res = utils::pprof_tools::gernate_jeprof().await;
            info!("debug jeprof: {:?}", res);
            match res {
                Ok(v) => Ok(v),
                Err(e) => Err(reject::custom(HttpError::PProfError { reason: e })),
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
            .and(self.query_body())
            .and(self.handle_header())
            .and(warp::query::<SqlParam>())
            .and(self.with_ctx())
            .and_then(
                |req: Bytes,
                 header: Header,
                 param: SqlParam,
                 http_service_ctx: Arc<HttpServiceContext>| async move {
                    let start = Instant::now();
                    debug!(
                        "Receive rest prom remote read request, header: {:?}, param: {:?}",
                        header, param
                    );

                    // Parse req、header and param to construct query request
                    let user_info = header.try_get_basic_auth().map_err(reject::custom)?;
                    let tenant = param.tenant;
                    let user = http_service_ctx
                        .dbms()
                        .authenticate(&user_info, tenant.as_deref())
                        .await
                        .map_err(|e| reject::custom(HttpError::from(e)))?;
                    let context = ContextBuilder::new(user)
                        .with_tenant(tenant)
                        .with_database(param.db)
                        .with_target_partitions(param.target_partitions)
                        .build();

                    let meta_manager = http_service_ctx.coord().meta_manager();
                    let result = http_service_ctx
                        .prom_remote_server()
                        .remote_read(&context, meta_manager, req)
                        .await
                        .map(|_| ResponseBuilder::ok())
                        .map_err(|e| {
                            trace::error!("Failed to handle prom remote read request, err: {}", e);
                            reject::custom(HttpError::from(e))
                        });

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
            .and(self.write_body())
            .and(self.handle_header())
            .and(warp::query::<WriteParam>())
            .and(self.with_http_service_ctx())
            .and_then(
                |req: Bytes,
                 header: Header,
                 param: WriteParam,
                 http_service_ctx: Arc<HttpServiceContext>| async move {
                    let start = Instant::now();
                    debug!(
                        "Receive rest prom remote write request, header: {:?}, param: {:?}",
                        header, param
                    );
                    let (dbms, coord) = (http_service_ctx.dbms(), http_service_ctx.coord());

                    let ctx = construct_write_context(header, param, dbms, coord)
                        .await
                        .map_err(reject::custom)?;

                    let result = http_service_ctx
                        .prom_remote_server()
                        .remote_write(req, &ctx, coord.clone())
                        .await
                        .map(|_| ResponseBuilder::ok())
                        .map_err(|e| {
                            trace::error!("Failed to handle prom remote write request, err: {}", e);
                            reject::custom(HttpError::from(e))
                        });

                    let (tenant, user, db) =
                        (ctx.tenant(), ctx.database(), ctx.user_info().desc().name());

                    http_service_ctx.metrics().writes_inc(tenant, user, db);

                    sample_point_write_duration(
                        ctx.tenant(),
                        ctx.database(),
                        result.is_ok(),
                        start.elapsed().as_millis() as f64,
                    );
                    result
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
        }) = &self.tls_config()
        {
            match self.mode() {
                ServerMode::Store => {
                    let routes = self.routes_store().recover(handle_rejection);
                    let (addr, server) = warp::serve(routes)
                        .tls()
                        .cert_path(certificate)
                        .key_path(private_key)
                        .bind_with_graceful_shutdown(self.ctx().addr(), signal);
                    info!("http server start addr: {}, {}", addr, self.mode());
                    tokio::spawn(server)
                }
                ServerMode::Query => {
                    let routes = self.routes_query().recover(handle_rejection);
                    let (addr, server) = warp::serve(routes)
                        .tls()
                        .cert_path(certificate)
                        .key_path(private_key)
                        .bind_with_graceful_shutdown(self.ctx().addr(), signal);
                    info!("http server start addr: {}, {}", addr, self.mode());
                    tokio::spawn(server)
                }
                ServerMode::Bundle => {
                    let routes = self.routes_query().recover(handle_rejection);
                    let (addr, server) = warp::serve(routes)
                        .tls()
                        .cert_path(certificate)
                        .key_path(private_key)
                        .bind_with_graceful_shutdown(self.ctx().addr(), signal);
                    info!("http server start addr: {}, {}", addr, self.ctx().mode);
                    tokio::spawn(server)
                }
            }
        } else {
            match self.ctx().mode() {
                ServerMode::Store => {
                    let routes = self.routes_store().recover(handle_rejection);
                    let (addr, server) =
                        warp::serve(routes).bind_with_graceful_shutdown(self.ctx().addr(), signal);
                    info!("http server start addr: {}, {}", addr, self.mode());
                    tokio::spawn(server)
                }
                ServerMode::Query => {
                    let routes = self.routes_query().recover(handle_rejection);
                    let (addr, server) =
                        warp::serve(routes).bind_with_graceful_shutdown(self.ctx().addr(), signal);
                    info!("http server start addr: {}, {}", addr, self.mode());
                    tokio::spawn(server)
                }
                ServerMode::Bundle => {
                    let routes = self.routes_bundle().recover(handle_rejection);
                    let (addr, server) =
                        warp::serve(routes).bind_with_graceful_shutdown(self.addr(), signal);
                    info!("http server start addr: {}, {}", addr, self.mode());
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
    dbms: &DBMSRef,
) -> Result<Query, HttpError> {
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
        .build();

    Ok(Query::new(
        context,
        String::from_utf8_lossy(req.as_ref()).to_string(),
    ))
}

fn _construct_write_db_privilege(tenant_id: Oid, database: &str) -> Privilege<Oid> {
    Privilege::TenantObject(
        TenantObjectPrivilege::Database(DatabasePrivilege::Write, Some(database.to_string())),
        Some(tenant_id),
    )
}

// construct context and check privilege
async fn construct_write_context(
    header: Header,
    param: WriteParam,
    dbms: &DBMSRef,
    coord: &CoordinatorRef,
) -> Result<Context, HttpError> {
    let user_info = header.try_get_basic_auth()?;
    let tenant = param.tenant;

    let user = dbms.authenticate(&user_info, tenant.as_deref()).await?;

    let context = ContextBuilder::new(user)
        .with_tenant(tenant)
        .with_database(Some(param.db))
        .build();

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

fn construct_write_points_request(
    req: Bytes,
    ctx: &Context,
) -> Result<WritePointsRequest, HttpError> {
    let lines = String::from_utf8_lossy(req.as_ref());
    let line_protocol_lines = line_protocol_to_lines(&lines, Local::now().timestamp_nanos())
        .map_err(|e| HttpError::ParseLineProtocol { source: e })?;

    let points = parse_lines_to_points(ctx.database(), &line_protocol_lines);

    let req = WritePointsRequest {
        version: 1,
        meta: None,
        points,
    };
    Ok(req)
}

async fn sql_handle(query: &Query, header: Header, dbms: &DBMSRef) -> Result<Response, HttpError> {
    debug!("prepare to execute: {:?}", query.content());

    let fmt = ResultFormat::try_from(header.get_accept())?;

    let result = dbms.execute(query).await.context(QuerySnafu)?;

    let batches = fetch_record_batches(result)
        .await
        .map_err(|e| HttpError::FetchResult {
            reason: format!("{e}"),
        })?;

    fmt.wrap_batches_to_response(&batches)
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
