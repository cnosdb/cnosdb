use std::sync::Arc;

use chrono::Local;
use datafusion::arrow::util::pretty::pretty_format_batches;
use flatbuffers::FlatBufferBuilder;
use futures::StreamExt;
use hyper::{Body, Request, Response};
use line_protocol::{line_protocol_to_lines, Line};
use protos::kv_service::WritePointsRpcRequest;
use protos::models::{
    self as fb_models, FieldBuilder, Point, PointArgs, Points, PointsArgs, TagBuilder,
};
use regex::Regex;
use snafu::ResultExt;
use spi::server::dbms::DatabaseManagerSystem;
use spi::service::protocol::{Context, Query};
use std::ops::DerefMut;
use tokio::sync::oneshot;
use trace::debug;

use super::QuerySnafu;
use crate::http::{parse_query, AsyncChanSendSnafu, Error, HyperSnafu, ParseLineProtocolSnafu};
use async_channel as channel;
use spi::query::execution::Output;

pub(crate) async fn route(
    req: Request<Body>,
    db: Arc<dyn DatabaseManagerSystem + Send + Sync>,
    sender: channel::Sender<tskv::Task>,
) -> Result<Response<Body>, Error> {
    match req.uri().path() {
        "/ping" => ping(req).await,
        "/write/line_protocol" => write_line_protocol(req, sender).await,
        "/query" => query(req, db).await,
        _ => message_404(req),
    }
}

pub(crate) async fn ping(req: Request<Body>) -> Result<Response<Body>, Error> {
    let mut verbose: Option<&str> = None;
    if let Some(query) = req.uri().query() {
        let query_params = parse_query(query);
        verbose = query_params.get("verbose").copied();
    }

    let resp = match verbose {
        Some(v) if !v.is_empty() && v != "0" || v != "false" => http::Response::builder()
            .body(Body::from(format!("{{ \"version\" = {} }}", "0.0.1")))
            .unwrap(),
        _ => http::Response::builder()
            .status(204)
            .body(Body::empty())
            .unwrap(),
    };

    Ok(resp)
}

pub(crate) async fn write_line_protocol(
    req: Request<Body>,
    sender: channel::Sender<tskv::Task>,
) -> Result<Response<Body>, Error> {
    let db: String;
    if let Some(query) = req.uri().query() {
        let query_params = parse_query(query);
        db = query_params.get("db").unwrap_or(&"").to_string();
    } else {
        return Err(Error::Syntax {
            reason: "Need some request parameters.".to_string(),
        });
    }

    if db.is_empty() {
        return Err(Error::Syntax {
            reason: "Request has no parameter 'db'.".to_string(),
        });
    }

    let mut body = req.into_body();
    let mut buffer: Vec<u8> = Vec::with_capacity(512);
    let mut len = 0_usize;
    while let Some(chunk) = body.next().await {
        let chunk = chunk.context(HyperSnafu)?;
        len += chunk.len();
        if len > 5 * 1024 * 1024 {
            return Err(Error::BodyOversize { size: 102400 });
        }
        buffer.extend_from_slice(chunk.as_ref());
    }
    let lines = String::from_utf8(buffer).map_err(|_| Error::NotUtf8)?;
    let line_protocol_lines = line_protocol_to_lines(&lines, Local::now().timestamp_millis())
        .context(ParseLineProtocolSnafu)?;
    debug!("Write request: {:?}", line_protocol_lines);
    let points = parse_lines_to_points(&db, &line_protocol_lines)?;

    let req = WritePointsRpcRequest { version: 1, points };

    // Send Request to handler
    let (tx, rx) = oneshot::channel();
    sender
        .send(tskv::Task::WritePoints { req, tx })
        .await
        .context(AsyncChanSendSnafu)?;

    // Receive Response from handler
    let _ = match rx.await {
        Ok(Ok(resp)) => resp,
        Ok(Err(err)) => return Err(Error::Tskv { source: err }),
        Err(err) => return Err(Error::ChannelReceive { source: err }),
    };

    let resp = http::Response::builder()
        .status(204)
        .body(Body::empty())
        .unwrap();
    Ok(resp)
}

pub(crate) async fn query(
    req: Request<Body>,
    database: Arc<dyn DatabaseManagerSystem + Send + Sync>,
) -> Result<Response<Body>, Error> {
    let db: String;
    if let Some(query) = req.uri().query() {
        let query_params = parse_query(query);
        db = query_params.get("db").unwrap_or(&"").to_string();
    } else {
        return Err(Error::Syntax {
            reason: "Need some request parameters.".to_string(),
        });
    }

    if db.is_empty() {
        return Err(Error::Syntax {
            reason: "Request has no parameter 'db'.".to_string(),
        });
    }

    let mut body = req.into_body();
    let mut buffer: Vec<u8> = Vec::with_capacity(512);
    let mut len = 0_usize;
    while let Some(chunk) = body.next().await {
        let chunk = chunk.context(HyperSnafu)?;
        len += chunk.len();
        if len > 5 * 1024 * 1024 {
            return Err(Error::BodyOversize { size: 102400 });
        }
        buffer.extend_from_slice(chunk.as_ref());
    }
    let sql = String::from_utf8(buffer).map_err(|_| Error::NotUtf8)?;
    debug!("Query request: {}", &sql);

    let mut actual = vec![];
    let query = Query::new(Context::default(), sql);
    let mut result = database.execute(&query).await.context(QuerySnafu)?;
    let mut resp_msg = "execute ok".to_string();
    for stmt_result in result.result().iter_mut() {
        match stmt_result {
            Output::StreamData(res) => {
                while let Some(batch) = res.next().await {
                    actual.push(batch.unwrap());
                }
            }
            Output::Nil(_) => resp_msg = "sql execute Ok".to_string(),
        }
    }
    if !actual.is_empty() {
        resp_msg = format!("{}", pretty_format_batches(actual.deref_mut()).unwrap());
    }
    let resp = http::Response::builder()
        .body(Body::from(resp_msg))
        .unwrap();
    Ok(resp)
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

pub(crate) fn message_404(req: Request<Body>) -> Result<Response<Body>, Error> {
    Ok(Response::builder()
        .status(404)
        .body(Body::from(format!("URI not found: {}", req.uri().path())))
        .unwrap())
}

#[cfg(test)]
mod test {
    use std::{fs::File, io::Read};

    use line_protocol::Parser;

    use crate::http::handler::parse_lines_to_points;

    #[test]
    #[ignore]
    fn test_generated_data_to_points() {
        let mut lp_file = File::open("/tmp/cnosdb-data").unwrap();
        let mut lp_lines = String::new();
        lp_file.read_to_string(&mut lp_lines).unwrap();

        let lines: Vec<&str> = lp_lines.split('\n').collect();
        println!("Received line-protocol lines: {}", lines.len());

        let parser = Parser::new(0);
        for line in lines {
            println!("Parsing: {}", line);
            let parsed_lines = parser.parse(line).unwrap();
            let _ = parse_lines_to_points("test", &parsed_lines);
            // println!("{:?}", points);
        }
    }
}
