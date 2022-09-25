use std::{collections::HashMap, convert::Infallible, net::SocketAddr};

use super::QuerySnafu;
use crate::http::Error;
use crate::http::ParseLineProtocolSnafu;
use crate::http::TskvSnafu;
use crate::server;
use crate::server::{Service, ServiceHandle};
use chrono::Local;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::parquet::data_type::AsBytes;
use flatbuffers::FlatBufferBuilder;
use futures::StreamExt;
use line_protocol::{line_protocol_to_lines, Line};
use protos::kv_service::WritePointsRpcRequest;
use protos::models as fb_models;
use protos::models::{FieldBuilder, Point, PointArgs, Points, PointsArgs, TagBuilder};
use serde::Serialize;
use snafu::ResultExt;
use spi::query::execution::Output;
use spi::server::dbms::DBMSRef;
use spi::service::protocol::{Context, Query, QueryHandle};
use std::ops::DerefMut;
use tokio::sync::oneshot;
use trace::{error, info};
use tskv::engine::EngineRef;
use warp::http::StatusCode;
use warp::hyper::body::Bytes;
use warp::{header, reject, Filter};

const PING: &str = "ping";
const QUERY: &str = "query";
const WRITE: &str = "write";

const QUERY_LEN: u64 = 1024 * 16;
const WRITE_LEN: u64 = 100 * 1024 * 1024;

pub struct HttpService {
    //todo: tls config
    addr: SocketAddr,
    dbms: DBMSRef,
    kv_inst: EngineRef,
    handle: Option<ServiceHandle<()>>,
}

impl HttpService {
    pub fn new(dbms: DBMSRef, kv_inst: EngineRef, addr: SocketAddr) -> Self {
        Self {
            addr,
            dbms,
            kv_inst,
            handle: None,
        }
    }
    fn handle_header(&self) -> impl Filter<Extract = (Context,), Error = warp::Rejection> + Clone {
        header::optional::<String>("user_id")
            .and(header::optional::<String>("database"))
            .and_then(|catalog, schema| async move {
                let res: Result<Context, warp::Rejection> = Ok(Context::with(catalog, schema));
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
        self.ping().or(self.query()).or(self.write_line_protocol())
        // self.ping()
    }

    fn ping(&self) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path(PING).map(|| {
            let mut resp = HashMap::new();
            resp.insert("version", "0.1.0");
            resp.insert("status", "healthy");
            warp::reply::json(&resp)
        })
    }

    fn query(&self) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        // let dbms = self.dbms.clone();
        warp::path(QUERY)
            .and(warp::post())
            .and(warp::body::content_length_limit(QUERY_LEN))
            .and(warp::body::bytes())
            .and(self.handle_header())
            .and(self.with_dbms())
            .and_then(|req: Bytes, context: Context, dbms: DBMSRef| async move {
                let query = Query::new(context, String::from_utf8_lossy(req.as_ref()).to_string());
                let mut result = dbms.execute(&query).await.context(QuerySnafu);
                match result {
                    // Ok(ref mut res) => Ok(warp::reply::json(&wrap_result(res).await)),
                    Ok(ref mut res) => Ok(wrap_result(res).await.result),
                    Err(e) => Err(reject::custom(QueryFailed {
                        reason: format!("Query failed: {}", e),
                    })),
                }
            })
    }

    fn write_line_protocol(
        &self,
    ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path(WRITE)
            .and(warp::post())
            .and(warp::body::content_length_limit(WRITE_LEN))
            .and(warp::body::bytes())
            .and(self.handle_header())
            .and(self.with_kv_inst())
            .and_then(
                |req: Bytes, content: Context, kv_inst: EngineRef| async move {
                    let lines = String::from_utf8_lossy(req.as_ref());
                    let line_protocol_lines =
                        line_protocol_to_lines(&lines, Local::now().timestamp_millis())
                            .context(ParseLineProtocolSnafu)?;
                    let points = parse_lines_to_points(&content.schema, &line_protocol_lines)?;
                    let req = WritePointsRpcRequest { version: 1, points };
                    let resp = kv_inst.write(req).await.context(TskvSnafu);
                    match resp {
                        Ok(_) => Ok(warp::reply::json(&TempResponse {
                            result: "success".to_string(),
                        })),
                        Err(_) => Err(reject::custom(WriteFailed {
                            reason: "Write failed".to_string(),
                        })),
                    }
                },
            )
    }
}

#[async_trait::async_trait]
impl Service for HttpService {
    fn start(&mut self) -> Result<(), server::Error> {
        let routes = self.routes().recover(handle_rejection);
        let (shutdown, rx) = oneshot::channel();
        let (addr, server) = warp::serve(routes).bind_with_graceful_shutdown(self.addr, async {
            rx.await.ok();
            info!("http server graceful shutdown!");
        });
        info!("http server start addr: {}", addr);
        let join_handle = tokio::spawn(server);
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
        let mut tags = Vec::new();
        for (k, v) in line.tags.iter() {
            let fbk = fbb.create_vector(k.as_bytes());
            let fbv = fbb.create_vector(v.as_bytes());
            let mut tag_builder = TagBuilder::new(&mut fbb);
            tag_builder.add_key(fbk);
            tag_builder.add_value(fbv);
            tags.push(tag_builder.finish());
        }
        let mut fields = Vec::new();
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

/*************** top ****************/
//todo: redefine the req/resp struct
#[derive(Debug, Serialize)]
struct ErrorResponse {
    code: u16,
    message: String,
}

#[derive(Debug, Serialize)]
struct EmptyResponse {}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
struct TempResponse {
    result: String,
}

#[derive(Debug, Serialize)]
struct QueryFailed {
    reason: String,
}
impl reject::Reject for QueryFailed {}

#[derive(Debug, Serialize)]
struct WriteFailed {
    reason: String,
}
impl reject::Reject for WriteFailed {}

async fn wrap_result(res: &mut QueryHandle) -> TempResponse {
    let mut actual = vec![];

    for ele in res.result().iter_mut() {
        match ele {
            Output::StreamData(item) => {
                while let Some(next) = item.next().await {
                    let batch = next.unwrap();
                    actual.push(batch);
                }
            }
            Output::Nil(_) => {}
        }
    }

    let result = pretty_format_batches(actual.deref_mut())
        .unwrap()
        .to_string();

    TempResponse { result }
}

async fn handle_rejection(rejection: warp::Rejection) -> Result<impl warp::Reply, Infallible> {
    error!("handle error: {:?}", rejection);

    let code = StatusCode::INTERNAL_SERVER_ERROR;
    let message = format!("TODO Wrap Error: {:?}", rejection);

    let json = warp::reply::json(&ErrorResponse {
        code: code.as_u16(),
        message,
    });

    Ok(warp::reply::with_status(json, code))
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
