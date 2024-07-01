use std::borrow::Cow;
use std::collections::BTreeMap;

use bytes::Bytes;
use models::snappy::SnappyCodec;
use models::utils::now_timestamp_nanos;
use prost::Message;
use protos::{logproto, FieldValue};
use serde::{Deserialize, Serialize};
use serde_json;
use snafu::Snafu;

use crate::Line;

#[derive(Serialize, Deserialize, Debug)]
pub enum Command {
    #[serde(rename = "create")]
    Create(CommandInfo),
    #[serde(rename = "index")]
    Index(CommandInfo),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CommandInfo {
    pub _id: Option<String>,
    pub _index: Option<String>,
}

pub enum JsonProtocol {
    ESLog(ESLog),
    Ndjson(NdJsonLog),
    Loki(LoKiLog),
}

impl JsonProtocol {
    pub fn get_fields(&self) -> &BTreeMap<String, serde_json::Value> {
        match self {
            JsonProtocol::ESLog(log) => &log.fields,
            JsonProtocol::Ndjson(log) => &log.fields,
            JsonProtocol::Loki(log) => &log.fields,
        }
    }
}

pub struct ESLog {
    pub command: Command,
    pub fields: BTreeMap<String, serde_json::Value>,
}

pub struct NdJsonLog {
    pub fields: BTreeMap<String, serde_json::Value>,
}

pub struct LoKiLog {
    pub fields: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Snafu, PartialEq, Eq)]
pub enum Error {
    #[snafu(display("invalid log type: {name}"))]
    InvalidType { name: String },

    #[snafu(display("invalid log syntax"))]
    InvaildSyntax,

    #[snafu(display("{}", content))]
    Common { content: String },

    #[snafu(display("invalid time format"))]
    ParseTime,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub fn flatten_json(name: String, input: serde_json::Value) -> BTreeMap<String, serde_json::Value> {
    let mut output = BTreeMap::new();
    match input {
        serde_json::Value::Object(map) => {
            for (k, v) in map {
                let res = flatten_json(k, v);
                for (k2, v2) in res {
                    output.insert(format!("{}.{}", name, k2), v2);
                }
            }
        }
        serde_json::Value::Array(arr) => {
            for (idx, value) in arr.iter().enumerate() {
                let res = flatten_json(idx.to_string(), value.clone());

                for (k, v) in res {
                    output.insert(format!("{}.{}", name, k), v);
                }
            }
        }
        _ => {
            output.insert(name, input);
        }
    }
    output
}

pub fn parse_json_to_eslog(json_chunk: Vec<&str>) -> Result<Vec<JsonProtocol>> {
    /*
    because the es log is a pair of command and fields like:
    { "index" : { "_index" : "test", "_id" : "1" } }
    { "field1" : "value1" }
    { "create" : { "_index" : "test", "_id" : "3" } }
    { "field1" : "value3" }
    */
    let n = json_chunk.len();
    let mut logs = vec![];
    if n % 2 != 0 {
        return Err(Error::InvaildSyntax);
    }

    let mut i = 0;
    while i < n {
        let command: Command =
            serde_json::from_str(json_chunk[i]).map_err(|_| Error::InvaildSyntax)?;
        let fields = flatten_json(
            String::new(),
            serde_json::from_str(json_chunk[i + 1]).map_err(|_| Error::InvaildSyntax)?,
        );

        logs.push(ESLog { command, fields });

        i += 2;
    }

    let res = logs.into_iter().map(JsonProtocol::ESLog).collect();
    Ok(res)
}

pub fn parse_json_to_ndjsonlog(json_chunk: Vec<&str>) -> Result<Vec<JsonProtocol>> {
    let mut logs = Vec::new();
    for line in json_chunk {
        let fields = flatten_json(
            String::new(),
            serde_json::from_str(line).map_err(|_| Error::InvaildSyntax)?,
        );

        logs.push(NdJsonLog { fields });
    }

    let res = logs.into_iter().map(JsonProtocol::Ndjson).collect();
    Ok(res)
}

pub fn parse_json_to_lokilog(json_chunk: Vec<&str>) -> Result<Vec<JsonProtocol>> {
    let mut logs = Vec::new();
    for line in json_chunk {
        let fields = flatten_json(
            String::new(),
            serde_json::from_str(line).map_err(|_| Error::InvaildSyntax)?,
        );

        logs.push(LoKiLog { fields });
    }

    let res = logs.into_iter().map(JsonProtocol::Loki).collect();
    Ok(res)
}

pub fn parse_protobuf_to_lokilog(req: Bytes) -> Result<Vec<JsonProtocol>> {
    let mut buf: Vec<u8> = Vec::new();
    let decoder = SnappyCodec {};
    decoder
        .decompress(&req, &mut buf, None)
        .map_err(|_| Error::InvalidType {
            name: "loki".to_string(),
        })?;
    let buf = Bytes::from(buf);

    let push_request = logproto::PushRequest::decode(buf).map_err(|_| Error::InvaildSyntax)?;

    let mut logs = Vec::new();
    let mut fields = BTreeMap::new();
    for stream in push_request.streams {
        fields.clear();
        fields.insert(
            "lables".to_string(),
            serde_json::Value::String(stream.labels),
        );
        for entry in stream.entries {
            let ts = entry.timestamp.unwrap_or_default();
            let timestamp: i64 = ts.seconds * 1_000_000_000 + ts.nanos as i64;

            fields.insert(
                "time".to_string(),
                serde_json::Value::Number(timestamp.into()),
            );
            fields.insert("msg".to_string(), serde_json::Value::String(entry.line));
            logs.push(LoKiLog {
                fields: fields.clone(),
            });
        }
    }
    let res = logs.into_iter().map(JsonProtocol::Loki).collect();
    Ok(res)
}

pub fn parse_to_line<'a>(
    value: &'a JsonProtocol,
    table: &'a str,
    time_column: &'a str,
    tag_columns: &'a str,
) -> Result<Line<'a>> {
    let tag_columns: Vec<&str> = tag_columns.split(',').map(|s| s.trim()).collect();
    let mut timestamp = now_timestamp_nanos();
    let mut tags = vec![];
    let mut fields = vec![];
    for (key, value) in value.get_fields() {
        if tag_columns.contains(&key.as_str()) {
            let value = value.to_string().trim_matches('"').to_owned();
            tags.push((Cow::Borrowed(key.as_str()), Cow::Owned(value)));
        } else if time_column.eq(key) {
            match value {
                serde_json::Value::String(timestamp_str) => {
                    match chrono::DateTime::parse_from_rfc3339(timestamp_str)
                        .map_err(|_| Error::ParseTime)?
                        .timestamp_nanos_opt()
                    {
                        Some(time) => {
                            timestamp = time;
                        }
                        None => {
                            return Err(Error::Common {
                                content: "Out of the time frame that can be saved".to_string(),
                            })
                        }
                    }
                }
                serde_json::Value::Number(timestamp_num) => match timestamp_num.as_i64() {
                    Some(time) => {
                        timestamp = time;
                    }
                    None => {
                        return Err(Error::Common {
                            content: "Out of the time frame that can be saved".to_string(),
                        })
                    }
                },
                _ => return Err(Error::ParseTime),
            }
        } else {
            let field = match value {
                serde_json::Value::Bool(field) => FieldValue::Bool(*field),
                serde_json::Value::Number(field) => FieldValue::F64(field.as_f64().unwrap()),
                serde_json::Value::String(field) => FieldValue::Str(field.as_bytes().to_owned()),
                _ => {
                    return Err(Error::Common {
                        content: format!("unsupported field type: {}", value),
                    });
                }
            };
            fields.push((Cow::Borrowed(key.as_str()), field));
        }
    }

    Ok(Line {
        hash_id: 0,
        table: Cow::Borrowed(table),
        tags,
        fields,
        timestamp,
    })
}
