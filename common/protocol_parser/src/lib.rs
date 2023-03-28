extern crate core;

use std::cmp::Ordering;
use std::collections::BTreeMap;

use protos::FieldValue;
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use utils::BkdrHasher;

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub mod line_protocol;
pub mod lines_convert;
pub mod open_tsdb;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Error: pos: {}, in: '{}'", pos, content))]
    Parse { pos: usize, content: String },

    #[snafu(display("line missing field : {} or line invalid: {}", field, buf))]
    MissingField { field: String, buf: String },

    #[snafu(display("{}", content))]
    Common { content: String },
}

#[derive(Debug, PartialEq)]
pub struct Line<'a> {
    hash_id: u64,
    pub table: &'a str,
    pub tags: Vec<(&'a str, &'a str)>,
    pub fields: Vec<(&'a str, FieldValue)>,
    pub timestamp: i64,
}

impl<'a> From<DataPoint<'a>> for Line<'a> {
    fn from(value: DataPoint<'a>) -> Self {
        let tags = value.tags.into_iter().collect();
        let fields = vec![("value", FieldValue::F64(value.value))];
        Line {
            hash_id: 0,
            table: value.metric,
            tags,
            fields,
            timestamp: value.timestamp,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct DataPoint<'a> {
    pub metric: &'a str,
    pub timestamp: i64,
    pub value: f64,
    pub tags: BTreeMap<&'a str, &'a str>,
}

impl<'a> Line<'_> {
    pub fn hash_id(&mut self) -> u64 {
        if self.hash_id == 0 {
            self.tags
                .sort_by(|a, b| -> Ordering { a.0.partial_cmp(b.0).unwrap() });

            let mut hasher = BkdrHasher::new();
            hasher.hash_with(self.table.as_bytes());
            for (k, v) in &self.tags {
                hasher.hash_with(k.as_bytes());
                hasher.hash_with(v.as_bytes());
            }

            self.hash_id = hasher.number()
        }

        self.hash_id
    }

    pub fn new(
        measurement: &'a str,
        tags: Vec<(&'a str, &'a str)>,
        fields: Vec<(&'a str, FieldValue)>,
        timestamp: i64,
    ) -> Line<'a> {
        let mut res = Line {
            hash_id: 0,
            table: measurement,
            tags,
            fields,
            timestamp,
        };
        res.hash_id = res.hash_id();
        res
    }

    pub fn sort_and_dedup(&mut self) {
        self.tags.sort_by(|a, b| a.0.cmp(b.0));
        self.fields.sort_by(|a, b| a.0.cmp(b.0));
        self.tags.dedup_by(|a, b| a.0 == b.0);
        self.fields.dedup_by(|a, b| a.0 == b.0);
    }
}

fn check_pos_valid(buf: &str, pos: usize) -> Result<()> {
    if pos < buf.len() {
        return Ok(());
    }
    Err(Error::Parse {
        pos,
        content: String::from(buf),
    })
}

fn next_measurement(buf: &str) -> Option<(&str, usize)> {
    let mut escaped = false;
    let mut exists_measurement = false;
    let (mut tok_begin, mut tok_end) = (0, buf.len());
    let mut i = 0;
    for (_, c) in buf.chars().enumerate() {
        // Measurement begin character
        if c == '\\' {
            escaped = true;
            if !exists_measurement {
                exists_measurement = true;
                tok_begin = i;
            }
            i += c.len_utf8();
            continue;
        }
        if exists_measurement {
            // Measurement end character
            if c == ',' {
                tok_end = i;
                break;
            }
        } else {
            // Measurement begin character
            if c.is_alphanumeric() {
                exists_measurement = true;
                tok_begin = i;
            }
        }
        if escaped {
            escaped = false;
        }

        i += c.len_utf8();
    }
    if exists_measurement {
        Some((&buf[tok_begin..tok_end], tok_end + 1))
    } else {
        None
    }
}

fn next_metric(buf: &str) -> Option<(&str, usize)> {
    let mut escaped = false;
    let mut exists_metric = false;
    let (mut tok_begin, mut tok_end) = (0, buf.len());
    let mut i = 0;
    for (_, c) in buf.chars().enumerate() {
        // Measurement begin character
        if c == '\\' {
            escaped = true;
            if !exists_metric {
                exists_metric = true;
                tok_begin = i;
            }
            i += c.len_utf8();
            continue;
        }
        if exists_metric {
            // Measurement end character
            if c == ' ' {
                tok_end = i;
                break;
            }
        } else {
            // Measurement begin character
            if c.is_alphanumeric() {
                exists_metric = true;
                tok_begin = i;
            }
        }
        if escaped {
            escaped = false;
        }

        i += c.len_utf8();
    }
    if exists_metric {
        Some((&buf[tok_begin..tok_end], tok_end + 1))
    } else {
        None
    }
}

fn is_tagset_character(c: char) -> bool {
    c.is_alphanumeric() || c == '\\'
}

fn next_tag_set(buf: &str) -> Option<(Vec<(&str, &str)>, usize)> {
    let mut escaped = false;
    let mut exists_tag_set = false;
    let mut tok_offsets = [0_usize; 3];
    let mut tok_end = 0_usize;

    let mut tag_set: Vec<(&str, &str)> = Vec::new();
    let mut i = 0;
    for (_, c) in buf.chars().enumerate() {
        // TagSet begin character
        if !escaped && c == '\\' {
            escaped = true;
            if !exists_tag_set {
                exists_tag_set = true;
                tok_offsets[0] = i;
            }
            i += c.len_utf8();
            continue;
        }
        if exists_tag_set {
            if !escaped && c == '=' {
                tok_offsets[1] = i;
                if buf.len() <= i + 1 {
                    return None;
                }
                tok_offsets[2] = i + 1;
            }
            if !escaped && c == ',' {
                tag_set.push((
                    &buf[tok_offsets[0]..tok_offsets[1]],
                    &buf[tok_offsets[2]..i],
                ));
                if buf.len() <= i + 1 {
                    return None;
                }
                tok_offsets[0] = i + 1;
            }

            // TagSet end character
            if !escaped && c == ' ' {
                tok_end = i;
                break;
            }
        } else {
            // TagSet begin character
            if is_tagset_character(c) {
                exists_tag_set = true;
                tok_offsets[0] = i;
            }
        }
        if escaped {
            escaped = false;
        }

        i += c.len_utf8();
    }
    if exists_tag_set {
        if tok_end == 0 {
            tok_end = buf.len();
            if buf.ends_with('\n') {
                tok_end -= 1;
            }
        }
        tag_set.push((
            &buf[tok_offsets[0]..tok_offsets[1]],
            &buf[tok_offsets[2]..tok_end],
        ));
        Some((tag_set, tok_end + 1))
    } else {
        None
    }
}

type FieldSet<'a> = Vec<(&'a str, FieldValue)>;

fn next_field_set(buf: &str) -> Result<Option<(FieldSet, usize)>> {
    let mut escaped = false;
    let mut quoted = false;
    let mut exists_field_set = false;
    let mut tok_offsets = [0_usize; 3];
    let mut tok_end = 0_usize;

    let mut field_set: FieldSet = Vec::new();
    let mut i = 0;
    for (_, c) in buf.chars().enumerate() {
        // TagSet begin character
        if c == '\\' {
            escaped = true;
            if !exists_field_set {
                exists_field_set = true;
                tok_offsets[0] = i;
            }
            i += c.len_utf8();
            continue;
        }
        if exists_field_set {
            if !escaped && c == '"' {
                quoted = !quoted;
                i += c.len_utf8();
                continue;
            }

            if !quoted && c == '=' {
                tok_offsets[1] = i;
                if buf.len() <= i + 1 {
                    return Ok(None);
                }
                tok_offsets[2] = i + 1;
            }
            if !quoted && c == ',' {
                field_set.push((
                    &buf[tok_offsets[0]..tok_offsets[1]],
                    parse_field_value(&buf[tok_offsets[2]..i])?,
                ));
                if buf.len() <= i + 1 {
                    return Ok(None);
                }
                tok_offsets[0] = i + 1;
            }

            // FieldSet end character
            if !quoted && c == ' ' || c == '\n' {
                tok_end = i;
                break;
            }
        } else {
            // FieldSet begin character
            if c.is_alphanumeric() {
                exists_field_set = true;
                tok_offsets[0] = i;
            }
        }
        if escaped {
            escaped = false;
        }

        i += c.len_utf8();
    }
    if exists_field_set {
        if tok_end == 0 {
            tok_end = buf.len()
        }
        field_set.push((
            &buf[tok_offsets[0]..tok_offsets[1]],
            parse_field_value(&buf[tok_offsets[2]..tok_end])?,
        ));
        Ok(Some((field_set, tok_end + 1)))
    } else {
        Ok(None)
    }
}

fn parse_field_value(buf: &str) -> Result<FieldValue> {
    if let Some((_, c)) = buf.chars().enumerate().next() {
        let ret = match c {
            ch if ch.is_numeric() => parse_numeric_field(buf, true),
            '+' => parse_numeric_field(&buf[1..], true),
            '-' => parse_numeric_field(&buf[1..], false),
            't' | 'T' => parse_boolean_field(buf, true),
            'f' | 'F' => parse_boolean_field(buf, false),
            '"' => parse_string_field(buf),
            _ => Err(Error::Parse {
                pos: 0,
                content: buf.to_string(),
            }),
        };
        return ret;
    }
    Ok(FieldValue::F64(1.0))
}

fn parse_numeric_field(buf: &str, positive: bool) -> Result<FieldValue> {
    if buf.is_empty() {
        return Err(Error::Parse {
            pos: 0,
            content: buf.to_string(),
        });
    }
    let field_val = match &buf[buf.len() - 1..] {
        "i" | "I" => {
            let v = buf[..buf.len() - 1]
                .parse::<i64>()
                .map_err(|_e| Error::Parse {
                    pos: 0,
                    content: buf.to_string(),
                })?;
            FieldValue::I64(if positive { v } else { -v })
        }
        "u" | "U" => {
            if !positive {
                return Err(Error::Parse {
                    pos: 0,
                    content: buf.to_string(),
                });
            }
            let v = buf[..buf.len() - 1]
                .parse::<u64>()
                .map_err(|_e| Error::Parse {
                    pos: 0,
                    content: buf.to_string(),
                })?;
            FieldValue::U64(v)
        }
        _ => {
            let v = buf.parse::<f64>().map_err(|_e| Error::Parse {
                pos: 0,
                content: buf.to_string(),
            })?;
            FieldValue::F64(if positive { v } else { -v })
        }
    };

    Ok(field_val)
}

fn parse_boolean_field(buf: &str, boolean: bool) -> Result<FieldValue> {
    const TRUE: &str = "true";
    const FALSE: &str = "false";

    if buf.len() == 1 {
        return Ok(FieldValue::Bool(boolean));
    }
    let check_iter = if boolean {
        if buf.len() < TRUE.len() {
            return Err(Error::Parse {
                pos: 0,
                content: buf.to_string(),
            });
        }
        TRUE.chars()
    } else {
        if buf.len() < FALSE.len() {
            return Err(Error::Parse {
                pos: 0,
                content: buf.to_string(),
            });
        }
        FALSE.chars()
    };
    for (c, check_c) in buf.chars().zip(check_iter) {
        match c.to_lowercase().next() {
            Some(ch) => {
                if ch != check_c {
                    return Err(Error::Parse {
                        pos: 0,
                        content: buf.to_string(),
                    });
                }
            }
            None => {
                return Err(Error::Parse {
                    pos: 0,
                    content: buf.to_string(),
                })
            }
        }
    }

    Ok(FieldValue::Bool(boolean))
}

fn parse_string_field(buf: &str) -> Result<FieldValue> {
    match &buf[buf.len() - 1..] {
        "\"" => return Ok(FieldValue::Str(buf[1..buf.len() - 1].as_bytes().to_vec())),
        _ => Err(Error::Parse {
            pos: 0,
            content: buf.to_string(),
        }),
    }
}

fn next_value(buf: &str) -> Option<(&str, usize)> {
    let mut exists_timestamp = false;
    let (mut tok_begin, mut tok_end) = (0, buf.len());
    for (i, c) in buf.chars().enumerate() {
        if exists_timestamp {
            // Timestamp end character
            if !c.is_numeric() {
                tok_end = i;
                break;
            }
        } else {
            // Timestamp begin character
            if c == '-' || c.is_numeric() {
                tok_begin = i;
                exists_timestamp = true;
            }
        }
    }
    if exists_timestamp {
        Some((&buf[tok_begin..tok_end], tok_end + 1))
    } else {
        None
    }
}
