use std::{collections::HashMap, convert::Infallible, net::SocketAddr};

use super::header::Header;
use super::header::ACCEPT;
use super::header::AUTHORIZATION;
use super::parameter::SqlParam;
use super::parameter::WriteParam;
use super::response::ErrorResponse;
use super::Error as HttpError;
use super::QuerySnafu;
use crate::http::response::ResponseBuilder;
use crate::http::result_format::fetch_record_batches;
use crate::http::result_format::ResultFormat;
use crate::http::Error;
use crate::http::ParseLineProtocolSnafu;
use crate::http::TskvSnafu;
use crate::server;
use crate::server::{Service, ServiceHandle};
use chrono::Local;
use config::TLSConfig;
use datafusion::parquet::data_type::AsBytes;
use flatbuffers::FlatBufferBuilder;
use line_protocol::{line_protocol_to_lines, Line};
use metrics::{
    gather_metrics_as_prometheus_string, incr_point_write_failed, incr_point_write_success,
    incr_query_read_failed, incr_query_read_success, sample_point_write_latency,
    sample_query_read_latency,
};
use models::error_code::ErrorCode;
use protos::kv_service::WritePointsRpcRequest;
use protos::models as fb_models;
use protos::models::{FieldBuilder, Point, PointArgs, Points, PointsArgs, TagBuilder};
use snafu::ResultExt;
use spi::server::dbms::DBMSRef;
use spi::service::protocol::ContextBuilder;
use spi::service::protocol::Query;
use std::time::Instant;
use tokio::sync::oneshot;
use trace::debug;
use trace::info;
use tskv::engine::EngineRef;
use warp::hyper::body::Bytes;
use warp::reject::MethodNotAllowed;
use warp::reject::MissingHeader;
use warp::reject::PayloadTooLarge;
use warp::reply::Response;
use warp::Rejection;
use warp::Reply;
use warp::{header, reject, Filter};

const QUERY_LEN: u64 = 1024 * 16;
const WRITE_LEN: u64 = 100 * 1024 * 1024;

pub struct HttpService {
    tls_config: Option<TLSConfig>,
    addr: SocketAddr,
    dbms: DBMSRef,
    kv_inst: EngineRef,
    handle: Option<ServiceHandle<()>>,
}

impl HttpService {
    pub fn new(
        dbms: DBMSRef,
        kv_inst: EngineRef,
        addr: SocketAddr,
        tls_config: Option<TLSConfig>,
    ) -> Self {
        Self {
            tls_config,
            addr,
            dbms,
            kv_inst,
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
    fn with_dbms(&self) -> impl Filter<Extract = (DBMSRef,), Error = Infallible> + Clone {
        let dbms = self.dbms.clone();
        warp::any().map(move || dbms.clone())
    }
    fn with_kv_inst(&self) -> impl Filter<Extract = (EngineRef,), Error = Infallible> + Clone {
        let kv_inst = self.kv_inst.clone();
        warp::any().map(move || kv_inst.clone())
    }

    fn routes(&self) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        self.ping()
            .or(self.query())
            .or(self.write_line_protocol())
            .or(self.metrics())
    }

    fn ping(&self) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("api" / "v1" / "ping")
            .and(warp::get().or(warp::head()))
            .map(|_| {
                let mut resp = HashMap::new();
                resp.insert("version", "0.1.0");
                resp.insert("status", "healthy");
                warp::reply::json(&resp)
            })
    }

    fn query(&self) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        // let dbms = self.dbms.clone();
        warp::path!("api" / "v1" / "sql")
            .and(warp::post())
            .and(warp::body::content_length_limit(QUERY_LEN))
            .and(warp::body::bytes())
            .and(self.handle_header())
            .and(warp::query::<SqlParam>())
            .and(self.with_dbms())
            .and_then(
                |req: Bytes, header: Header, param: SqlParam, dbms: DBMSRef| async move {
                    debug!(
                        "Receive http sql request, header: {:?}, param: {:?}",
                        header, param
                    );

                    // Parse req、header and param to construct query request
                    let query_req = construct_query(req, &header, param);

                    match query_req {
                        Ok(ref q) => {
                            let start = Instant::now();

                            let result = sql_handle(q, header, dbms).await;

                            let result = result.map_err(|e| {
                                trace::error!("Failed to handle http sql request, err: {}", e);
                                e
                            });

                            sample_query_read_latency(
                                q.context().catalog(),
                                q.context().database(),
                                start.elapsed().as_millis() as f64,
                            );

                            match result {
                                Ok(resp) => {
                                    incr_query_read_success();
                                    Ok(resp)
                                }
                                Err(e) => {
                                    incr_query_read_failed();
                                    Err(reject::custom(e))
                                }
                            }
                        }
                        Err(e) => {
                            incr_query_read_failed();
                            Err(reject::custom(e))
                        }
                    }
                },
            )
    }

    fn write_line_protocol(
        &self,
    ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("api" / "v1" / "write")
            .and(warp::post())
            .and(warp::body::content_length_limit(WRITE_LEN))
            .and(warp::body::bytes())
            .and(self.handle_header())
            .and(warp::query::<WriteParam>())
            .and(self.with_kv_inst())
            .and_then(
                |req: Bytes, header: Header, param: WriteParam, kv_inst: EngineRef| async move {
                    let start = Instant::now();
                    let lines = String::from_utf8_lossy(req.as_ref());
                    let line_protocol_lines =
                        line_protocol_to_lines(&lines, Local::now().timestamp_nanos())
                            .context(ParseLineProtocolSnafu)?;
                    let points = parse_lines_to_points(&param.db, &line_protocol_lines)?;
                    let req = WritePointsRpcRequest { version: 1, points };
                    let resp = kv_inst.write(req).await.context(TskvSnafu);

                    let user_info = match header.try_get_basic_auth() {
                        Ok(u) => u,
                        Err(e) => return Err(reject::custom(e)),
                    };

                    sample_point_write_latency(
                        &user_info.user,
                        &param.db,
                        start.elapsed().as_millis() as f64,
                    );
                    match resp {
                        Ok(_) => {
                            incr_point_write_success();
                            Ok(ResponseBuilder::ok())
                        }
                        Err(e) => {
                            incr_point_write_failed();
                            Err(reject::custom(e))
                        }
                    }
                },
            )
    }

    fn metrics(&self) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("api" / "v1" / "metrics")
            .map(|| warp::reply::json(&gather_metrics_as_prometheus_string()))
    }
}

#[async_trait::async_trait]
impl Service for HttpService {
    fn start(&mut self) -> Result<(), server::Error> {
        let routes = self.routes().recover(handle_rejection);
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
            let (addr, server) = warp::serve(routes)
                .tls()
                .cert_path(certificate)
                .key_path(private_key)
                .bind_with_graceful_shutdown(self.addr, signal);
            info!("http server start addr: {}", addr);
            tokio::spawn(server)
        } else {
            let (addr, server) = warp::serve(routes).bind_with_graceful_shutdown(self.addr, signal);
            info!("http server start addr: {}", addr);
            tokio::spawn(server)
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

fn parse_lines_to_points(db: &str, lines: &[Line]) -> Result<Vec<u8>, Error> {
    let mut fbb = FlatBufferBuilder::new();
    let mut point_offsets = Vec::with_capacity(lines.len());
    for line in lines.iter() {
        let mut tags = Vec::with_capacity(line.tags.len());
        for (k, v) in line.tags.iter() {
            let fbk = fbb.create_vector(k.as_bytes());
            let fbv = fbb.create_vector(v.as_bytes());
            let mut tag_builder = TagBuilder::new(&mut fbb);
            tag_builder.add_key(fbk);
            tag_builder.add_value(fbv);
            tags.push(tag_builder.finish());
        }
        let mut fields = Vec::with_capacity(line.fields.len());
        for (k, v) in line.fields.iter() {
            let fbk = fbb.create_vector(k.as_bytes());
            let (fbv_type, fbv) = match v {
                line_protocol::FieldValue::U64(field_val) => (
                    fb_models::FieldType::Unsigned,
                    fbb.create_vector(&field_val.to_be_bytes()),
                ),
                line_protocol::FieldValue::I64(field_val) => (
                    fb_models::FieldType::Integer,
                    fbb.create_vector(&field_val.to_be_bytes()),
                ),
                line_protocol::FieldValue::Str(field_val) => {
                    (fb_models::FieldType::String, fbb.create_vector(field_val))
                }
                line_protocol::FieldValue::F64(field_val) => (
                    fb_models::FieldType::Float,
                    fbb.create_vector(&field_val.to_be_bytes()),
                ),
                line_protocol::FieldValue::Bool(field_val) => (
                    fb_models::FieldType::Boolean,
                    if *field_val {
                        fbb.create_vector(&[1_u8][..])
                    } else {
                        fbb.create_vector(&[0_u8][..])
                    },
                ),
            };
            let mut field_builder = FieldBuilder::new(&mut fbb);
            field_builder.add_name(fbk);
            field_builder.add_type_(fbv_type);
            field_builder.add_value(fbv);
            fields.push(field_builder.finish());
        }
        let point_args = PointArgs {
            db: Some(fbb.create_vector(db.as_bytes())),
            table: Some(fbb.create_vector(line.measurement.as_bytes())),
            tags: Some(fbb.create_vector(&tags)),
            fields: Some(fbb.create_vector(&fields)),
            timestamp: line.timestamp,
        };
        point_offsets.push(Point::create(&mut fbb, &point_args));
    }

    let fbb_db = fbb.create_vector(db.as_bytes());
    let points_raw = fbb.create_vector(&point_offsets);
    let points = Points::create(
        &mut fbb,
        &PointsArgs {
            database: Some(fbb_db),
            points: Some(points_raw),
        },
    );
    fbb.finish(points, None);
    Ok(fbb.finished_data().to_vec())
}

fn construct_query(req: Bytes, header: &Header, param: SqlParam) -> Result<Query, HttpError> {
    let user_info = header.try_get_basic_auth()?;

    let context = ContextBuilder::new(user_info)
        .with_database(param.db)
        .with_target_partitions(param.target_partitions)
        .build();

    Ok(Query::new(
        context,
        String::from_utf8_lossy(req.as_ref()).to_string(),
    ))
}

async fn sql_handle(query: &Query, header: Header, dbms: DBMSRef) -> Result<Response, HttpError> {
    debug!("prepare to execute: {:?}", query.content());

    let fmt = ResultFormat::try_from(header.get_accept())?;

    let mut result = dbms.execute(query).await.context(QuerySnafu)?;

    let batches = fetch_record_batches(&mut result)
        .await
        .map_err(|e| HttpError::FetchResult {
            reason: format!("{}", e),
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
        let error_resp = ErrorResponse::new(ErrorCode::Unknown, e.to_string());
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
        use warp::Filter;
        // use futures_util::future::TryFutureExt;
        use tokio::sync::oneshot;

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
