use std::borrow::Cow;

use models::utils::now_timestamp_nanos;
use protos::FieldValue;
use serde::{Deserialize, Serialize};
use serde_json;
use snafu::Snafu;

use crate::Line;

const ES_LOG_MSG_FIELD: &str = "_msg";

#[derive(Serialize, Deserialize, Debug)]
pub struct Fields {
    #[serde(rename = "_msg")]
    msg: Option<String>,
    #[serde(rename = "_time")]
    pub time: Option<String>,
    #[serde(flatten)]
    ohters: serde_json::Map<String, serde_json::Value>,
}

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
    pub fields: Fields,
}

pub struct Parser {
    table: String,
    // TODO Some statistics here
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

impl Parser {
    pub fn new(table: String) -> Self {
        Self { table }
    }

    pub fn parse<'a>(
        &self,
        req: &'a str,
        time_alias: Option<String>,
        msg_alias: Option<String>,
    ) -> Result<Vec<Line<'a>>> {
        let mut ret: Vec<Line> = Vec::new();

        let json_chunk: Vec<&str> = req.trim().split('\n').collect();
        let lines_len = json_chunk.len();

        //the es log is a pair of command
        if lines_len % 2 != 0 {
            return Err(Error::InvaildSyntax);
        }

        let time_key = time_alias.unwrap_or_default();
        let msg_key = msg_alias.unwrap_or_default();

        let mut pos = 0;
        loop {
            let mut line =
                self.next_line(json_chunk[pos], json_chunk[pos + 1], &time_key, &msg_key)?;
            line.sort_dedup_and_hash();
            ret.push(line);
            pos += 2;
            if pos == lines_len {
                break;
            }
        }
        Ok(ret)
    }

    fn next_line<'a>(
        &self,
        command_buf: &'a str,
        field_buf: &'a str,
        time_alias: &str,
        msg_alias: &str,
    ) -> Result<Line<'a>> {
        let command: Command = serde_json::from_str(command_buf).map_err(|e| Error::Common {
            content: e.to_string(),
        })?;
        let req_fields: Fields = serde_json::from_str(field_buf).map_err(|e| Error::Common {
            content: e.to_string(),
        })?;

        let es_log = ESLog {
            command,
            fields: req_fields,
        };

        let timestamp = parse_time(&es_log, time_alias)?;

        let fields = {
            let msg = parse_msg(&es_log, msg_alias)?;

            let fields = vec![(Cow::Borrowed(ES_LOG_MSG_FIELD), FieldValue::Str(msg))];

            fields
        };

        let tags = {
            let mut tags = vec![];
            for (key, value) in es_log.fields.ohters {
                if key.eq(time_alias) || key.eq(msg_alias) {
                    continue;
                }
                let value = value.to_string().trim_end_matches('"').to_owned();

                tags.push((Cow::Owned(key), Cow::Owned(value)));
            }
            tags
        };

        Ok(Line {
            hash_id: 0,
            table: Cow::Owned(self.table.clone()),
            timestamp,
            tags,
            fields,
        })
    }
}

pub fn es_parse_to_line<'a>(
    value: &'a ESLog,
    table: &'a str,
    time_alias: &'a str,
    msg_alias: &'a str,
) -> Result<Line<'a>> {
    let timestamp = parse_time(value, time_alias)?;
    let fields = {
        let msg = parse_msg(value, msg_alias)?;
        let fields = vec![(Cow::Borrowed(ES_LOG_MSG_FIELD), FieldValue::Str(msg))];

        fields
    };

    let tags = {
        let mut tags = vec![];
        for (key, value) in value.fields.ohters.iter() {
            if key.eq(time_alias) || key.eq(msg_alias) {
                continue;
            }
            let value = value.to_string().trim_matches('"').to_owned();

            tags.push((Cow::Borrowed(key.as_str()), Cow::Owned(value)));
        }
        tags
    };

    Ok(Line {
        hash_id: 0,
        table: Cow::Borrowed(table),
        tags,
        fields,
        timestamp,
    })
}

fn parse_time(value: &ESLog, time_alias: &str) -> Result<i64> {
    if !time_alias.is_empty() {
        match value.fields.ohters.get(time_alias) {
            Some(serde_json::Value::String(timestamp)) => {
                let parse_time_to_int = chrono::DateTime::parse_from_rfc3339(timestamp)
                    .map_err(|_| Error::ParseTime)?
                    .timestamp_nanos_opt()
                    .unwrap_or_default();
                return Ok(parse_time_to_int);
            }
            _ => return Err(Error::ParseTime),
        }
    }

    if value.fields.time.is_some() {
        let timestamp = value.fields.time.clone().unwrap();
        let parse_time_to_int = chrono::DateTime::parse_from_rfc3339(&timestamp)
            .map_err(|_| Error::ParseTime)?
            .timestamp_nanos_opt()
            .unwrap_or_default();

        return Ok(parse_time_to_int);
    }

    Ok(now_timestamp_nanos())
}

fn parse_msg(value: &ESLog, msg_alias: &str) -> Result<Vec<u8>> {
    if !msg_alias.is_empty() {
        if let Some(serde_json::Value::String(msg)) = value.fields.ohters.get(msg_alias) {
            return Ok(msg.clone().into_bytes());
        }
    }

    if value.fields.msg.is_some() {
        if let Some(msg) = value.fields.msg.clone() {
            return Ok(msg.into_bytes());
        }
    }

    Err(Error::Common {
        content: "msg field is unset".to_string(),
    })
}
