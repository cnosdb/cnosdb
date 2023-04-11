use protos::FieldValue;

use crate::Error::Common;
use crate::{check_pos_valid, next_metric, next_tag_set, next_value, Error, Line, Result};

const OPEN_TSDB_DEFAULT_FIELD: &str = "value";

pub struct Parser {
    default_time: i64,
    // TODO Some statistics here
}

impl Parser {
    pub fn new(default_time: i64) -> Self {
        Self { default_time }
    }

    pub fn parse<'a>(&self, lines: &'a str) -> Result<Vec<Line<'a>>> {
        let mut ret: Vec<Line> = Vec::new();
        let mut pos = 0_usize;
        while let Some((mut line, offset)) = self.next_line(lines, pos)? {
            line.sort_and_dedup();
            ret.push(line);
            pos += offset;
        }
        Ok(ret)
    }

    fn next_line<'a>(&self, buf: &'a str, position: usize) -> Result<Option<(Line<'a>, usize)>> {
        if position > buf.len() {
            return Ok(None);
        }

        let start_pos = position;
        let mut pos = position;
        let metric = if let Some(m) = next_metric(&buf[pos..]) {
            pos += m.1;
            m.0
        } else {
            return Ok(None);
        };
        check_pos_valid(buf, pos)?;

        let timestamp = if pos < buf.len() {
            if let Some(t) = next_value(&buf[pos..]) {
                let timestamp = t.0.parse::<i64>().map_err(|e| Error::Parse {
                    pos,
                    content: format!("{}: '{}'", e, buf),
                })?;
                pos += t.1;
                timestamp
            } else {
                self.default_time
            }
        } else {
            self.default_time
        };

        let value = if pos < buf.len() {
            if let Some(t) = next_value(&buf[pos..]) {
                let value = t.0.parse::<f64>().map_err(|e| Error::Parse {
                    pos,
                    content: format!("{}: '{}'", e, buf),
                })?;
                pos += t.1;
                value
            } else {
                return Err(Error::MissingField {
                    field: OPEN_TSDB_DEFAULT_FIELD.to_string(),
                    buf: String::from(buf),
                });
            }
        } else {
            return Err(Error::Parse {
                pos,
                content: String::from(buf),
            });
        };

        let fields = vec![(OPEN_TSDB_DEFAULT_FIELD, FieldValue::F64(value))];

        check_pos_valid(buf, pos)?;

        let mut tags = vec![];
        while let Some(t) = next_tag_set(&buf[pos..]) {
            pos += t.1;
            tags.extend(t.0);
            if check_pos_valid(buf, pos).is_err() {
                break;
            }
        }

        Ok(Some((
            Line {
                hash_id: 0,
                table: metric,
                timestamp,
                fields,
                tags,
            },
            pos - start_pos,
        )))
    }

    pub fn parse_tcp<'a>(&self, buf: &'a mut [u8]) -> Result<(Vec<Line<'a>>, usize)> {
        let mut ret: Vec<Line> = Vec::new();
        let mut pos = 0;
        let lines = std::str::from_utf8(buf).map_err(|e| Error::Common {
            content: format!("Invalid Point: {}", e),
        })?;
        let lines = lines.split("\r\n").collect::<Vec<&str>>();
        for line_raw in lines {
            if let Some(mut line) = self.next_tcp_line(line_raw)? {
                line.sort_and_dedup();
                ret.push(line);
                pos += line_raw.len() + 2;
            }
        }
        Ok((ret, pos))
    }

    fn next_tcp_line<'a>(&self, buf: &'a str) -> Result<Option<Line<'a>>> {
        if buf.is_empty() {
            return Ok(None);
        }
        let tokens = buf.split_whitespace().collect::<Vec<&str>>();
        let cmd = if tokens.is_empty() { "" } else { tokens[0] };
        // OpenTSDB command is case sensitive, verified in real OpenTSDB.
        if cmd != "put" {
            return Err(Common {
                content: "unknown command".to_string(),
            });
        }

        if tokens.len() < 4 {
            return Err(Common {
                content: format!(
                    "put: illegal argument: not enough arguments (need least 4, got {})",
                    tokens.len()
                ),
            });
        }

        let metric = tokens[1];

        let ts = tokens[2].parse::<i64>().map_err(|_| Common {
            content: format!("put: invalid timestamp: {}", tokens[2]),
        })?;

        let value = match tokens[3].parse::<f64>() {
            Ok(v) => v,
            Err(_) => {
                return Err(Common {
                    content: format!("put: invalid value: {}", tokens[3]),
                })
            }
        };

        let mut tags = Vec::with_capacity(tokens.len() - 4);
        for token in tokens.iter().skip(4) {
            let tag = token.split('=').collect::<Vec<&str>>();
            if tag.len() != 2 || tag[0].is_empty() || tag[1].is_empty() {
                return Err(Common {
                    content: format!("put: invalid tag: {}", tokens[3]),
                });
            }
            tags.push((tag[0], tag[1]));
        }

        Ok(Some(Line {
            hash_id: 0,
            table: metric,
            timestamp: ts,
            fields: vec![(OPEN_TSDB_DEFAULT_FIELD, FieldValue::F64(value))],
            tags,
        }))
    }
}
