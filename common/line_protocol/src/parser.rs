use std::cmp::Ordering;

use utils::BkdrHasher;

use crate::{Error, Result};

pub struct Parser {
    default_time: i64,
    // TODO Some statistics here
}

impl Parser {
    pub fn new(default_time: i64) -> Parser {
        Self { default_time }
    }

    pub fn parse<'a>(&self, lines: &'a str) -> Result<Vec<Line<'a>>> {
        let mut ret: Vec<Line> = Vec::new();
        let mut pos = 0_usize;
        while let Some((line, offset)) = self.next_line(lines, pos)? {
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
        let measurement = if let Some(m) = next_measurement(&buf[pos..]) {
            pos += m.1;
            m.0
        } else {
            return Ok(None);
        };
        check_pos_valid(buf, pos)?;

        let tags = if let Some(t) = next_tag_set(&buf[pos..]) {
            pos += t.1;
            t.0
        } else {
            return Err(Error::Parse {
                pos,
                content: String::from(buf),
            });
        };
        check_pos_valid(buf, pos)?;

        let fields = if let Some(f) = next_field_set(&buf[pos..])? {
            pos += f.1;
            f.0
        } else {
            return Err(Error::Parse {
                pos,
                content: String::from(buf),
            });
        };

        let timestamp = if pos < buf.len() {
            if let Some(t) = next_timestamp(&buf[pos..]) {
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

        Ok(Some((
            Line {
                hash_id: 0,
                measurement,
                tags,
                fields,
                timestamp,
            },
            pos - start_pos,
        )))
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum FieldValue {
    U64(u64),
    I64(i64),
    Str(Vec<u8>),
    F64(f64),
    Bool(bool),
}

#[derive(Debug, PartialEq)]
pub struct Line<'a> {
    hash_id: u64,
    pub measurement: &'a str,
    pub tags: Vec<(&'a str, &'a str)>,
    pub fields: Vec<(&'a str, FieldValue)>,
    pub timestamp: i64,
}

impl<'a> Line<'_> {
    pub fn hash_id(&mut self) -> u64 {
        if self.hash_id == 0 {
            self.tags
                .sort_by(|a, b| -> Ordering { a.0.partial_cmp(b.0).unwrap() });

            let mut hasher = BkdrHasher::new();
            hasher.hash_with(self.measurement.as_bytes());
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
            measurement,
            tags,
            fields,
            timestamp,
        };
        res.hash_id = res.hash_id();
        res
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
            tok_end = buf.len()
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

fn next_timestamp(buf: &str) -> Option<(&str, usize)> {
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

#[cfg(test)]
mod test {
    use std::fs::File;
    use std::io::Read;

    use crate::parser::{
        next_field_set, next_measurement, next_tag_set, next_timestamp, FieldValue, Line, Parser,
    };

    #[test]
    fn test_parse_functions() {
        //! measurement: ma
        //! | ta  | tb | fa       | fb | ts |
        //! | --  | -- | ------   | -- | -- |
        //! | 2\\ | 1  | "112\"3" | 2  | 1  |
        //!
        //! measurement: mb
        //! | tb | tc  | fa  | fc  | ts |
        //! | -- | --- | --- | --- | -- |
        //! | 2  | abc | 1.3 | 0.9 |    |

        let lines = "ma,ta=2\\\\,tb=1 fa=\"112\\\"3\",fb=2 1  \n mb,tb=2,tc=abc fa=1.3,fc=0.9";
        println!(
            "Length of the line protocol string in test case: {}\n======\n{}\n======",
            lines.len(),
            lines
        );

        let mut pos = 0;
        let measurement = next_measurement(&lines[pos..]).unwrap();
        assert_eq!(measurement, ("ma", 3));
        pos += measurement.1;

        if pos < lines.len() {
            let tagset = next_tag_set(&lines[pos..]).unwrap();
            assert_eq!(tagset, (vec![("ta", "2\\\\"), ("tb", "1")], 12));
            pos += tagset.1;
        }

        if pos < lines.len() {
            let fieldset = next_field_set(&lines[pos..]).unwrap().unwrap();
            assert_eq!(
                fieldset,
                (
                    vec![
                        ("fa", FieldValue::Str(b"112\\\"3".to_vec())),
                        ("fb", FieldValue::F64(2.0))
                    ],
                    17
                )
            );
            pos += fieldset.1;
        }

        if pos < lines.len() {
            let timestamp = next_timestamp(&lines[pos..]);
            assert_eq!(timestamp, Some(("1", 2)));
            pos += timestamp.unwrap().1;
        }

        println!("==========");

        if pos < lines.len() {
            let measurement = next_measurement(&lines[pos..]).unwrap();
            assert_eq!(measurement, ("mb", 6));
            pos += measurement.1;
        }

        if pos < lines.len() {
            let tagset = next_tag_set(&lines[pos..]).unwrap();
            assert_eq!(tagset, (vec![("tb", "2"), ("tc", "abc")], 12));
            pos += tagset.1;
        }

        if pos < lines.len() {
            let fieldset = next_field_set(&lines[pos..]).unwrap().unwrap();
            assert_eq!(
                fieldset,
                (
                    vec![("fa", FieldValue::F64(1.3)), ("fc", FieldValue::F64(0.9))],
                    14
                )
            );
            pos += fieldset.1;
        }

        if pos < lines.len() {
            let timestamp = next_timestamp(&lines[pos..]);
            assert_eq!(timestamp, None);
        }
    }

    #[test]
    fn test_line_parser() {
        //! measurement: ma
        //! | ta  | tb | fa     | fb | fc             | ts |
        //! | --  | -- | ------ | -- | -------------- | -- |
        //! | 2\\ | 1  | 112\"" | 2  | "hello, world" | 1  |
        //!
        //! measurement: mb
        //! | tb | tc  | fa  | fc  | ts |
        //! | -- | --- | --- | --- | -- |
        //! | 2  | abc | 1.3 | 0.9 |    |

        let lines = "ma,ta=2\\\\,tb=1 fa=\"112\\\"3\",fb=2,fc=\"hello, world\" 1  \n mb,tb=2,tc=abc fa=1.3,fc=0.9";
        println!(
            "Length of the line protocol string in test case: {}\n======\n{}\n======",
            lines.len(),
            lines
        );

        let parser = Parser::new(-1);
        let data = parser.parse(lines).unwrap();
        assert_eq!(data.len(), 2);

        let data_1 = data.get(0).unwrap();
        assert_eq!(
            *data_1,
            Line {
                hash_id: 0,
                measurement: "ma",
                tags: vec![("ta", "2\\\\"), ("tb", "1")],
                fields: vec![
                    ("fa", FieldValue::Str(b"112\\\"3".to_vec())),
                    ("fb", FieldValue::F64(2.0)),
                    ("fc", FieldValue::Str(b"hello, world".to_vec())),
                ],
                timestamp: 1
            }
        );

        let data_2 = data.get(1).unwrap();
        assert_eq!(
            *data_2,
            Line {
                hash_id: 0,
                measurement: "mb",
                tags: vec![("tb", "2"), ("tc", "abc")],
                fields: vec![("fa", FieldValue::F64(1.3)), ("fc", FieldValue::F64(0.9))],
                timestamp: -1
            }
        );
    }

    #[test]
    #[ignore]
    fn test_generated_data() {
        let mut lp_file = File::open("/tmp/cnosdb-data").unwrap();
        let mut lp_lines = String::new();
        lp_file.read_to_string(&mut lp_lines).unwrap();

        let parser = Parser::new(0);
        let lines = parser.parse(&lp_lines).unwrap();

        for l in lines {
            println!("{:?}", l);
        }
    }

    #[test]
    fn test_unicode() {
        let parser = Parser::new(-1);
        let lp = parser.parse("m,t1=中,t2=发,t3=majh f=\"白\"").unwrap();
        assert_eq!(lp.len(), 1);
        assert_eq!(lp[0].measurement, "m");
        assert_eq!(lp[0].tags.len(), 3);
        assert_eq!(lp[0].tags[0], ("t1", "中"));
        assert_eq!(lp[0].tags[1], ("t2", "发"));
        assert_eq!(lp[0].tags[2], ("t3", "majh"));
        assert_eq!(lp[0].fields.len(), 1);
        assert_eq!(
            lp[0].fields[0],
            ("f", FieldValue::Str("白".to_string().into_bytes().to_vec()))
        );
    }
}
