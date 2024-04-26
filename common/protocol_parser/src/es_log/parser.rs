use std::borrow::Cow;
use std::collections::BTreeMap;

use models::utils::now_timestamp_nanos;
use protos::FieldValue;
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

pub struct ESLog {
    pub command: Command,
    pub fields: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Snafu, PartialEq, Eq)]
pub enum Error {
    #[snafu(display("invalid es log syntax"))]
    InvaildSyntax,

    #[snafu(display("{}", content))]
    Common { content: String },

    #[snafu(display("invalid time format"))]
    ParseTime,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub fn flatten_json(input: serde_json::Value) -> BTreeMap<String, serde_json::Value> {
    let mut output = BTreeMap::new();
    match input {
        serde_json::Value::Object(map) => {
            for (k, v) in map {
                match v {
                    serde_json::Value::Object(sub_map) => {
                        let res = flatten_json(serde_json::Value::Object(sub_map));
                        for (k2, v2) in res {
                            output.insert(format!("{}.{}", k, k2), v2);
                        }
                    }
                    serde_json::Value::Array(arr) => {
                        for (i, item) in arr.iter().enumerate() {
                            let res = flatten_json(item.clone());
                            for (k2, v2) in res {
                                output.insert(format!("{}.{}.{}", k, i, k2), v2);
                            }
                        }
                    }
                    _ => {
                        output.insert(k, v);
                    }
                }
            }
        }
        _ => {
            output.insert("root".to_string(), input);
        }
    }
    output
}

pub fn es_parse_to_line<'a>(
    value: &'a ESLog,
    table: &'a str,
    time_column: &'a str,
    tag_columns: &'a str,
) -> Result<Line<'a>> {
    let tag_columns: Vec<&str> = tag_columns.split(',').map(|s| s.trim()).collect();
    let mut timestamp = now_timestamp_nanos();
    let mut tags = vec![];
    let mut fields = vec![];
    for (key, value) in &value.fields {
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
