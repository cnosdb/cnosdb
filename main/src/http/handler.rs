use std::sync::Arc;

use chrono::Local;
use flatbuffers::FlatBufferBuilder;
use futures::StreamExt;
use hyper::{Body, Request, Response};
use line_protocol::{line_protocol_to_lines, Line};
use protos::kv_service::WritePointsRpcRequest;
use protos::models::{self as fb_models, FieldBuilder, Point, PointArgs, TagBuilder};
use query::db::Db;
use snafu::ResultExt;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use trace::debug;

use crate::http::{parse_query, ChannelSendSnafu, Error, HyperSnafu, ParseLineProtocolSnafu};

pub(crate) async fn route(
    req: Request<Body>,
    db: Arc<Db>,
    sender: UnboundedSender<tskv::Task>,
) -> Result<Response<Body>, Error> {
    match req.uri().path() {
        "/write/line_protocol" => write_line_protocol(req, sender).await,
        "/query/sql" => query_sql(req, db).await,
        _ => message_404(req),
    }
}

pub(crate) async fn write_line_protocol(
    req: Request<Body>,
    sender: UnboundedSender<tskv::Task>,
) -> Result<Response<Body>, Error> {
    let database: String;
    if let Some(query) = req.uri().query() {
        let query_params = parse_query(query);
        database = query_params
            .get("database")
            .unwrap_or_else(|| &"")
            .to_string();
    } else {
        return Err(Error::Syntax {
            reason: format!("Need some request parameters."),
        });
    }

    if database.is_empty() {
        return Err(Error::Syntax {
            reason: format!("Request has no parameter 'database'."),
        });
    }

    let mut body = req.into_body();
    let mut buffer: Vec<u8> = Vec::with_capacity(512);
    let mut len = 0_usize;
    while let Some(chunk) = body.next().await {
        let chunk = chunk.context(HyperSnafu)?;
        len += chunk.len();
        if len > 102400 {
            return Err(Error::BodyOversize { size: 102400 });
        }
        buffer.extend_from_slice(chunk.as_ref());
    }
    let lines = String::from_utf8(buffer).map_err(|_| Error::NotUtf8)?;
    let line_protocol_lines = line_protocol_to_lines(&lines, Local::now().timestamp_millis())
        .context(ParseLineProtocolSnafu)?;
    debug!("Write request: {:?}", line_protocol_lines);
    let points = parse_lines_to_points(&line_protocol_lines);

    let req = WritePointsRpcRequest {
        version: 1,
        database,
        points,
    };

    // Send Request to handler
    let (tx, rx) = oneshot::channel();
    let _ = sender
        .send(tskv::Task::WritePoints { req, tx })
        .context(ChannelSendSnafu)?;

    // Receive Response from handler
    let _ = match rx.await {
        Ok(Ok(resp)) => resp,
        Ok(Err(err)) => return Err(Error::Tskv { source: err }),
        Err(err) => return Err(Error::ChannelReceive { source: err }),
    };

    let resp = http::Response::builder()
        .body(Body::from("Write succeed."))
        .unwrap();
    Ok(resp)
}

pub(crate) async fn query_sql(req: Request<Body>, db: Arc<Db>) -> Result<Response<Body>, Error> {
    let database: String;
    if let Some(query) = req.uri().query() {
        let query_params = parse_query(query);
        database = query_params
            .get("database")
            .unwrap_or_else(|| &"")
            .to_string();
    } else {
        return Err(Error::Syntax {
            reason: format!("Need some request parameters."),
        });
    }

    if database.is_empty() {
        return Err(Error::Syntax {
            reason: format!("Request has no parameter 'database'."),
        });
    }

    let mut body = req.into_body();
    let mut buffer: Vec<u8> = Vec::with_capacity(512);
    let mut len = 0_usize;
    while let Some(chunk) = body.next().await {
        let chunk = chunk.context(HyperSnafu)?;
        len += chunk.len();
        if len > 102400 {
            return Err(Error::BodyOversize { size: 102400 });
        }
        buffer.extend_from_slice(chunk.as_ref());
    }
    let sql = String::from_utf8(buffer).map_err(|_| Error::NotUtf8)?;

    let ret = db.run_query(&sql).await;
    dbg!(&ret);

    let resp = http::Response::builder()
        .body(Body::from("Write succeed."))
        .unwrap();
    Ok(resp)
}

fn parse_lines_to_points(lines: &[Line]) -> Vec<u8> {
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
            let fbv = fbb.create_vector(v.as_bytes());
            let mut field_builder = FieldBuilder::new(&mut fbb);
            field_builder.add_name(fbk);
            field_builder.add_type_(fb_models::FieldType::Float);
            field_builder.add_value(fbv);
            fields.push(field_builder.finish());
        }
        let point_args = PointArgs {
            tags: Some(fbb.create_vector(&tags)),
            fields: Some(fbb.create_vector(&fields)),
            timestamp: line.timestamp,
        };
        point_offsets.push(Point::create(&mut fbb, &point_args));
    }
    let points = fbb.create_vector(&point_offsets);
    fbb.finish(points, None);
    fbb.finished_data().to_vec()
}

fn message_404(req: Request<Body>) -> Result<Response<Body>, Error> {
    Ok(Response::builder()
        .status(404)
        .body(Body::from(format!("URI not found: {}", req.uri().path())))
        .unwrap())
}
