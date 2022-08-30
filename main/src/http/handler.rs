use std::sync::Arc;

use chrono::Local;
use datafusion::arrow::util::pretty::pretty_format_batches;
use flatbuffers::FlatBufferBuilder;
use futures::StreamExt;
use hyper::{Body, Request, Response};
use lazy_static::lazy_static;
use line_protocol::{line_protocol_to_lines, Line};
use protos::kv_service::WritePointsRpcRequest;
use protos::models::{
    self as fb_models, FieldBuilder, Point, PointArgs, Points, PointsArgs, TagBuilder,
};
use query::db::Db;
use regex::Regex;
use snafu::ResultExt;
use tokio::sync::oneshot;
use trace::debug;

use crate::http::{parse_query, AsyncChanSendSnafu, Error, HyperSnafu, ParseLineProtocolSnafu};
use async_channel as channel;

lazy_static! {
    static ref NUMBER_PATTERN: Regex = Regex::new(r"^[+-]?\d+([IiUu]?|(.\d*))$").unwrap();
    static ref STRING_PATTERN: Regex = Regex::new("^\".*\"$").unwrap();
    static ref TRUE_PATTERN: Regex = Regex::new("^(t|(true))$").unwrap();
    static ref FALSE_PATTERN: Regex = Regex::new("^(f|(false))$").unwrap();
}

pub(crate) async fn route(
    req: Request<Body>,
    db: Arc<Db>,
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

pub(crate) async fn query(req: Request<Body>, database: Arc<Db>) -> Result<Response<Body>, Error> {
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

    let record_batches = if let Some(rbs) = database.run_query(&sql).await {
        rbs
    } else {
        Vec::new()
    };

    let resp_msg = format!("{}", pretty_format_batches(&record_batches).unwrap());

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
            let (fbv_type, fbv) = if NUMBER_PATTERN.is_match(v) {
                if v.ends_with("i") || v.ends_with("I") {
                    (
                        fb_models::FieldType::Integer,
                        fbb.create_vector(
                            &v[..v.len() - 1]
                                .parse::<i64>()
                                .map_err(|e| Error::Syntax {
                                    reason: format!(
                                        "Value '{}' is not valid i64: {}",
                                        v,
                                        e.to_string()
                                    ),
                                })?
                                .to_be_bytes(),
                        ),
                    )
                } else if v.ends_with("u") || v.ends_with("U") {
                    (
                        fb_models::FieldType::Unsigned,
                        fbb.create_vector(
                            &v[..v.len() - 1]
                                .parse::<u64>()
                                .map_err(|e| Error::Syntax {
                                    reason: format!(
                                        "Value '{}' is not valid u64: {}",
                                        v,
                                        e.to_string()
                                    ),
                                })?
                                .to_be_bytes(),
                        ),
                    )
                } else {
                    (
                        fb_models::FieldType::Float,
                        fbb.create_vector(
                            &v[..]
                                .parse::<f64>()
                                .map_err(|e| Error::Syntax {
                                    reason: format!(
                                        "Value '{}' is not valid f64: {}",
                                        v,
                                        e.to_string()
                                    ),
                                })?
                                .to_be_bytes(),
                        ),
                    )
                }
            } else if STRING_PATTERN.is_match(v) {
                (
                    fb_models::FieldType::String,
                    fbb.create_vector(v.as_bytes()),
                )
            } else {
                let vl = v.to_lowercase();
                if TRUE_PATTERN.is_match(&vl) {
                    (
                        fb_models::FieldType::Boolean,
                        fbb.create_vector(&[1_u8][..]),
                    )
                } else if FALSE_PATTERN.is_match(&vl) {
                    (
                        fb_models::FieldType::Boolean,
                        fbb.create_vector(&[0_u8][..]),
                    )
                } else {
                    (
                        fb_models::FieldType::Unknown,
                        fbb.create_vector(v.as_bytes()),
                    )
                }
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
    fn test_parse() {
        let number_strings = ["0.1", "1.99999", "99999999i", "999I", "1U", "1u"];
        let non_number_strings = ["\"0.1\"", "1.99.99.9", ".50", "999B", "1.0I", "1.1U"];
        for s in number_strings {
            println!("{} is number? {}", s, super::NUMBER_PATTERN.is_match(s));
        }
        for s in non_number_strings {
            println!("{} is number? {}", s, super::NUMBER_PATTERN.is_match(s));
        }

        let bolean_strings = ["t", "f", "true", "false"];
        let non_bolean_strings = ["t1", "f_", "@true", "false@"];
        for s in bolean_strings {
            println!("{} is true? {}", s, super::TRUE_PATTERN.is_match(s));
            println!("{} is false? {}", s, super::FALSE_PATTERN.is_match(s));
        }
        for s in non_bolean_strings {
            println!("{} is true? {}", s, super::TRUE_PATTERN.is_match(s));
            println!("{} is false? {}", s, super::FALSE_PATTERN.is_match(s));
        }
    }

    #[test]
    #[ignore]
    fn test_generated_data_to_points() {
        let mut lp_file = File::open("/tmp/cnosdb-data").unwrap();
        let mut lp_lines = String::new();
        lp_file.read_to_string(&mut lp_lines).unwrap();

        let lines: Vec<&str> = lp_lines.split("\n").collect();
        println!("Received line-protocol lines: {}", lines.len());

        let parser = Parser::new(0);
        for line in lines {
            println!("Parsing: {}", line);
            let parsed_lines = parser.parse(&line).unwrap();
            let _ = parse_lines_to_points("test", &parsed_lines);
            // println!("{:?}", points);
        }
    }
}
