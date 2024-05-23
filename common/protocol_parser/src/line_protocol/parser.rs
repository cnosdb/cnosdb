use std::borrow::Cow;

use itertools::Itertools;
use protos::FieldValue;
use snafu::Snafu;

use crate::Line;

pub struct Parser {
    default_time: i64,
    // TODO Some statistics here
}

#[derive(Debug, Snafu, PartialEq, Eq)]
pub enum Error {
    #[snafu(display("invalid line protocol syntax"))]
    InvaildSyntax,

    #[snafu(display("unexpect token '{}' at {}", token, pos))]
    UnexpectedToken { pos: usize, token: char },

    #[snafu(display("invalid field value: '{}'", content))]
    FieldValue { content: String },

    #[snafu(display("fail to parse timestamp at {}", pos))]
    Timestamp { pos: usize },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Parser {
    pub fn new(default_time: i64) -> Self {
        Self { default_time }
    }

    pub fn parse<'a>(&self, lines: &'a str) -> Result<Vec<Line<'a>>> {
        let lines = lines.as_bytes();
        let mut res = vec![];
        let mut spos = 0;
        while let Some((line, newpos)) = self.parse_line(lines, spos)? {
            res.push(line);
            if newpos >= lines.len() {
                break;
            }
            spos = newpos;
        }
        Ok(res)
    }

    fn parse_line<'a>(&self, lines: &'a [u8], spos: usize) -> Result<Option<(Line<'a>, usize)>> {
        let len = lines.len();
        let mut spos = spos;
        while spos < len && lines[spos].is_ascii_whitespace() {
            spos += 1;
        }
        if spos == len {
            return Ok(None);
        }

        let (epos, need_unescape) = search_comma_space_in_measurement(lines, spos)?;
        let measurement = get_unescaped_measurement(&lines[spos..epos], need_unescape)?;
        spos = epos + 1;

        let mut tags = vec![];
        while spos < len && lines[spos - 1] != b' ' {
            let (epos, need_unescape) = search_equal(lines, spos)?;
            let key = get_unescaped_common(&lines[spos..epos], need_unescape)?;
            spos = epos + 1;
            if spos >= len {
                return Err(Error::InvaildSyntax);
            }

            let (epos, need_unescape) = search_comma_space_in_common(lines, spos)?;
            let value = get_unescaped_common(&lines[spos..epos], need_unescape)?;
            spos = epos + 1;

            tags.push((key, value));
        }

        while spos < len && lines[spos] == b' ' {
            spos += 1;
        }

        if spos >= len {
            return Err(Error::InvaildSyntax);
        }

        let mut fields = vec![];
        loop {
            let (epos, need_unescape) = search_equal(lines, spos)?;
            let key = get_unescaped_common(&lines[spos..epos], need_unescape)?;
            spos = epos + 1;
            if spos >= len {
                return Err(Error::InvaildSyntax);
            }

            let (epos, need_unescape) = search_filed_value_end(lines, spos);
            let value = parse_field_value(&lines[spos..epos], need_unescape)?;
            spos = epos + 1;
            fields.push((key, value));

            if spos >= len
                || lines[spos - 1] == b' '
                || lines[spos - 1] == b'\n'
                || lines[spos - 1] == b'\r'
            {
                break;
            }
        }

        if spos >= len {
            let mut line = Line {
                hash_id: 0,
                table: measurement,
                tags,
                fields,
                timestamp: self.default_time,
            };
            line.sort_dedup_and_hash();
            return Ok(Some((line, spos)));
        }

        if lines[spos - 1] == b' ' {
            while spos < len && lines[spos] == b' ' {
                spos += 1;
            }
        }
        let timestamp = if lines[spos - 1] == b' ' {
            let res = atoi_simd::parse_until_invalid(&lines[spos..])
                .map_err(|_| Error::Timestamp { pos: spos })?;
            spos += res.1;
            res.0
        } else {
            self.default_time
        };

        let mut line = Line {
            hash_id: 0,
            table: measurement,
            tags,
            fields,
            timestamp,
        };
        line.sort_dedup_and_hash();
        Ok(Some((line, spos)))
    }
}

fn get_unescaped_measurement(s: &[u8], need_unescape: bool) -> Result<Cow<str>> {
    if !need_unescape {
        return Ok(Cow::Borrowed(u8_slice_to_str_unchecked(s)));
    }
    let mut ownd_bytes = Vec::with_capacity(s.len());
    for (&v1, &v2) in s.iter().tuple_windows() {
        if v2 == b',' || v2 == b' ' {
            if v1 != b'\\' {
                return Err(Error::InvaildSyntax);
            }
            continue;
        }
        ownd_bytes.push(v1);
    }
    ownd_bytes.push(s[s.len() - 1]);
    Ok(Cow::Owned(unsafe {
        String::from_utf8_unchecked(ownd_bytes)
    }))
}

fn get_unescaped_common(s: &[u8], need_unescape: bool) -> Result<Cow<str>> {
    if !need_unescape {
        return Ok(Cow::Borrowed(u8_slice_to_str_unchecked(s)));
    }
    let mut ownd_bytes = Vec::with_capacity(s.len());
    for (&v1, &v2) in s.iter().tuple_windows() {
        if v2 == b',' || v2 == b' ' || v2 == b'=' {
            if v1 != b'\\' {
                return Err(Error::InvaildSyntax);
            }
            continue;
        }
        ownd_bytes.push(v1);
    }
    ownd_bytes.push(s[s.len() - 1]);
    Ok(Cow::Owned(unsafe {
        String::from_utf8_unchecked(ownd_bytes)
    }))
}

fn search_comma_space_in_measurement(lines: &[u8], spos: usize) -> Result<(usize, bool)> {
    if lines[spos] == b',' {
        return Err(Error::UnexpectedToken {
            pos: spos,
            token: lines[spos] as char,
        });
    }
    let mut need_unescape = false;
    for ((_, &v1), (i, &v2)) in lines[spos..].iter().enumerate().tuple_windows() {
        if (v2 == b' ' || v2 == b',') && v1 != b'\\' {
            return Ok((spos + i, need_unescape));
        }
        need_unescape |= v2 == b',' || v2 == b' ';
    }
    Err(Error::InvaildSyntax)
}

fn search_comma_space_in_common(lines: &[u8], spos: usize) -> Result<(usize, bool)> {
    if lines[spos] == b' ' || lines[spos] == b',' || lines[spos] == b'=' {
        return Err(Error::UnexpectedToken {
            pos: spos,
            token: lines[spos] as char,
        });
    }
    let mut need_unescape = false;
    for ((_, &v1), (i, &v2)) in lines[spos..].iter().enumerate().tuple_windows() {
        if (v2 == b' ' || v2 == b',') && v1 != b'\\' {
            return Ok((spos + i, need_unescape));
        }
        need_unescape |= v2 == b',' || v2 == b' ' || v2 == b'=';
    }
    Err(Error::InvaildSyntax)
}

fn search_filed_value_end(lines: &[u8], spos: usize) -> (usize, bool) {
    let mut inside_quotes = lines[spos] == b'"';
    let mut need_unescape = false;
    let mut v1_unescaped = false;
    for ((_, &v1), (i, &v2)) in lines[spos..].iter().enumerate().tuple_windows() {
        if v2 == b'"' && (v1 != b'\\' || v1_unescaped) {
            inside_quotes = !inside_quotes;
            v1_unescaped = false;
            continue;
        }
        if inside_quotes {
            v1_unescaped = v1 == b'\\' && (v2 == b'"' || v2 == b'\\');
            need_unescape |= v1_unescaped;
            continue;
        }
        if v2 == b' ' || v2 == b',' || v2 == b'\n' || v2 == b'\r' {
            return (spos + i, need_unescape);
        }
    }
    (lines.len(), need_unescape)
}

fn search_equal(lines: &[u8], spos: usize) -> Result<(usize, bool)> {
    if lines[spos] == b'=' || lines[spos] == b' ' || lines[spos] == b',' {
        return Err(Error::UnexpectedToken {
            pos: spos,
            token: lines[spos] as char,
        });
    }
    let mut need_unescape = false;
    for ((_, &v1), (i, &v2)) in lines[spos..].iter().enumerate().tuple_windows() {
        if v2 == b'=' && v1 != b'\\' {
            return Ok((spos + i, need_unescape));
        }
        need_unescape |= v2 == b',' || v2 == b' ' || v2 == b'=';
    }
    Err(Error::InvaildSyntax)
}

fn parse_field_value(buf: &[u8], need_unescape: bool) -> Result<FieldValue> {
    match buf[0] {
        b't' | b'T' => parse_boolean_field(buf, true),
        b'f' | b'F' => parse_boolean_field(buf, false),
        b'"' => parse_string_field(buf, need_unescape),
        b'+' | b'-' | b'0'..=b'9' => parse_numeric_field(buf),
        _ => Err(Error::FieldValue {
            content: u8_slice_to_str_unchecked(buf).to_owned(),
        }),
    }
}

fn parse_numeric_field(buf: &[u8]) -> Result<FieldValue> {
    let field_val = match &buf[buf.len() - 1] {
        b'i' => {
            let v = atoi_simd::parse(&buf[..buf.len() - 1]).map_err(|_| Error::FieldValue {
                content: u8_slice_to_str_unchecked(buf).to_owned(),
            })?;
            FieldValue::I64(v)
        }
        b'u' => {
            let v = atoi_simd::parse_pos(&buf[..buf.len() - 1]).map_err(|_| Error::FieldValue {
                content: u8_slice_to_str_unchecked(buf).to_owned(),
            })?;
            FieldValue::U64(v)
        }
        _ => {
            let v = fast_float::parse(buf).map_err(|_| Error::FieldValue {
                content: u8_slice_to_str_unchecked(buf).to_owned(),
            })?;
            FieldValue::F64(v)
        }
    };

    Ok(field_val)
}

fn parse_boolean_field(buf: &[u8], boolean: bool) -> Result<FieldValue> {
    if buf.len() == 1 {
        return Ok(FieldValue::Bool(boolean));
    }
    match buf {
        b"true" | b"True" | b"TRUE" | b"false" | b"False" | b"FALSE" => {
            Ok(FieldValue::Bool(boolean))
        }
        _ => Err(Error::FieldValue {
            content: u8_slice_to_str_unchecked(buf).to_owned(),
        }),
    }
}

fn parse_string_field(buf: &[u8], need_unescape: bool) -> Result<FieldValue> {
    if buf.len() < 2 {
        return Err(Error::FieldValue {
            content: u8_slice_to_str_unchecked(buf).to_owned(),
        });
    }
    match (buf[0], buf[buf.len() - 1]) {
        (b'"', b'"') => Ok(FieldValue::Str(get_unescaped_str(
            &buf[1..buf.len() - 1],
            need_unescape,
        )?)),
        _ => Err(Error::FieldValue {
            content: u8_slice_to_str_unchecked(buf).to_owned(),
        }),
    }
}

fn get_unescaped_str(buf: &[u8], need_unescape: bool) -> Result<Vec<u8>> {
    if !need_unescape {
        return Ok(buf.to_owned());
    }
    let mut ownd_bytes = Vec::with_capacity(buf.len());
    let mut in_unescape = false;
    for (&v1, &v2) in buf.iter().tuple_windows() {
        if v1 == b'\\' && (v2 == b'"' || v2 == b'\\') {
            if in_unescape && v2 == b'"' {
                return Err(Error::FieldValue {
                    content: u8_slice_to_str_unchecked(buf).to_owned(),
                });
            }
            if !in_unescape {
                in_unescape = true;
                continue;
            }
        }
        ownd_bytes.push(v1);
        in_unescape = false;
    }
    ownd_bytes.push(buf[buf.len() - 1]);
    Ok(ownd_bytes)
}

fn u8_slice_to_str_unchecked(slice: &[u8]) -> &str {
    unsafe { std::str::from_utf8_unchecked(slice) }
}

#[cfg(test)]
mod test {
    use std::borrow::Cow;
    use std::fs::File;
    use std::io::Read;

    use protos::FieldValue;

    use crate::line_protocol::parser::Parser;
    use crate::line_protocol::Line;
    use crate::LineProtocolError;

    // Some of the tests are from https://github.com/influxdata/line-protocol/blob/v2/lineprotocol/decoder_test.go
    #[test]
    fn test_line_parser() {
        //! measurement: ma
        //! | ta  | tb | fa     | fb | fc             | ts |
        //! | --  | -- | ------ | -- | -------------- | -- |
        //! | 2,  | 1  | 112"3  | 2  | "hello, world" | 1  |
        //!
        //! measurement: mb
        //! | tb | tc  | fa  | fc  | fs |
        //! | -- | --- | --- | --- | -- |
        //! | 2  | abc | 1.3 | 0.9 |    |

        let lines = "ma,ta=2\\,,tb=1 fa=\"112\\\"3\",fb=2,fc=\"hello, world\" 1  \n mb,tb=2,tc=abc fa=1.3,fc=0.9,fs=\"\"";
        println!(
            "Length of the line protocol string in test case: {}\n======\n{}\n======",
            lines.len(),
            lines
        );

        let parser = Parser::new(-1);
        let data = parser.parse(lines).unwrap();
        assert_eq!(data.len(), 2);

        let data_1 = data.first().unwrap();
        assert_eq!(
            *data_1,
            Line {
                hash_id: 447297711207326024,
                table: Cow::Borrowed("ma"),
                tags: vec![
                    (Cow::Borrowed("ta"), Cow::Borrowed("2,")),
                    (Cow::Borrowed("tb"), Cow::Borrowed("1"))
                ],
                fields: vec![
                    (Cow::Borrowed("fa"), FieldValue::Str(b"112\"3".to_vec())),
                    (Cow::Borrowed("fb"), FieldValue::F64(2.0)),
                    (
                        Cow::Borrowed("fc"),
                        FieldValue::Str(b"hello, world".to_vec())
                    ),
                ],
                timestamp: 1
            }
        );

        let data_2 = data.get(1).unwrap();
        let mut line = Line {
            hash_id: 0,
            table: Cow::Borrowed("mb"),
            tags: vec![
                (Cow::Borrowed("tb"), Cow::Borrowed("2")),
                (Cow::Borrowed("tc"), Cow::Borrowed("abc")),
            ],
            fields: vec![
                (Cow::Borrowed("fa"), FieldValue::F64(1.3)),
                (Cow::Borrowed("fc"), FieldValue::F64(0.9)),
                (Cow::Borrowed("fs"), FieldValue::Str(vec![])),
            ],
            timestamp: -1,
        };
        line.init_hash_id();
        assert_eq!(*data_2, line);

        let lines = r#"
        somename,tag1=val1,tag2=val2  floatfield=1,strfield="hello",intfield=-1i,uintfield=1u,boolfield=true  1602841605822791506
        "#;

        let data = parser.parse(lines).unwrap();
        let mut line = Line {
            hash_id: 0,
            table: Cow::Borrowed("somename"),
            tags: vec![
                (Cow::Borrowed("tag1"), Cow::Borrowed("val1")),
                (Cow::Borrowed("tag2"), Cow::Borrowed("val2")),
            ],
            fields: vec![
                (Cow::Borrowed("boolfield"), FieldValue::Bool(true)),
                (Cow::Borrowed("floatfield"), FieldValue::F64(1.0)),
                (Cow::Borrowed("intfield"), FieldValue::I64(-1)),
                (
                    Cow::Borrowed("strfield"),
                    FieldValue::Str(b"hello".to_vec()),
                ),
                (Cow::Borrowed("uintfield"), FieldValue::U64(1)),
            ],
            timestamp: 1602841605822791506,
        };
        line.init_hash_id();
        assert_eq!(data[0], line);

        let lines = r#"
    m1,tag1=val1  x="first"  1602841605822791506
        m2,foo=bar  x="second"  1602841605822792000
      
        "#;

        let data = parser.parse(lines).unwrap();
        let mut lines = vec![
            Line {
                hash_id: 0,
                table: Cow::Borrowed("m1"),
                tags: vec![(Cow::Borrowed("tag1"), Cow::Borrowed("val1"))],
                fields: vec![(Cow::Borrowed("x"), FieldValue::Str(b"first".to_vec()))],
                timestamp: 1602841605822791506,
            },
            Line {
                hash_id: 0,
                table: Cow::Borrowed("m2"),
                tags: vec![(Cow::Borrowed("foo"), Cow::Borrowed("bar"))],
                fields: vec![(Cow::Borrowed("x"), FieldValue::Str(b"second".to_vec()))],
                timestamp: 1602841605822792000,
            },
        ];
        lines[0].init_hash_id();
        lines[1].init_hash_id();
        assert_eq!(data, lines);

        let lines = r#"
comma\,1,equals\==e\,x,two=val2 field\=x="fir\"
,st\\"  1602841605822791506
        
            "#;

        let data = parser.parse(lines).unwrap();
        let mut line = Line {
            hash_id: 0,
            table: Cow::Borrowed("comma,1"),
            tags: vec![
                (Cow::Borrowed("equals="), Cow::Borrowed("e,x")),
                (Cow::Borrowed("two"), Cow::Borrowed("val2")),
            ],
            fields: vec![(
                Cow::Borrowed("field=x"),
                FieldValue::Str(b"fir\"\n,st\\".to_vec()),
            )],
            timestamp: 1602841605822791506,
        };
        line.init_hash_id();
        assert_eq!(data[0], line);

        let lines = r#"a f=1"#;

        let data = parser.parse(lines).unwrap();
        let mut line = Line {
            hash_id: 0,
            table: Cow::Borrowed("a"),
            tags: vec![],
            fields: vec![(Cow::Borrowed("f"), FieldValue::F64(1.0))],
            timestamp: -1,
        };
        line.init_hash_id();
        assert_eq!(data[0], line);
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
    fn test_unicode0() {
        let parser = Parser::new(-1);
        let lp = parser.parse("m,t1=中,t2=发,t3=majh f=\"白\n\"").unwrap();
        assert_eq!(lp.len(), 1);
        assert_eq!(lp[0].table, "m");
        assert_eq!(lp[0].tags.len(), 3);
        assert_eq!(lp[0].tags[0], (Cow::Borrowed("t1"), Cow::Borrowed("中")));
        assert_eq!(lp[0].tags[1], (Cow::Borrowed("t2"), Cow::Borrowed("发")));
        assert_eq!(lp[0].tags[2], (Cow::Borrowed("t3"), Cow::Borrowed("majh")));
        assert_eq!(lp[0].fields.len(), 1);
        assert_eq!(
            lp[0].fields[0],
            (
                Cow::Borrowed("f"),
                FieldValue::Str("白\n".to_string().into_bytes().to_vec())
            )
        );
    }

    #[test]
    fn test_unicode2() {
        let parser = Parser::new(-1);
        let lp = parser.parse("m,t1=中,t2=发,t3=majh f=\"白\n1\"").unwrap();
        assert_eq!(lp.len(), 1);
        assert_eq!(lp[0].table, "m");
        assert_eq!(lp[0].tags.len(), 3);
        assert_eq!(lp[0].tags[0], (Cow::Borrowed("t1"), Cow::Borrowed("中")));
        assert_eq!(lp[0].tags[1], (Cow::Borrowed("t2"), Cow::Borrowed("发")));
        assert_eq!(lp[0].tags[2], (Cow::Borrowed("t3"), Cow::Borrowed("majh")));
        assert_eq!(lp[0].fields.len(), 1);
        assert_eq!(
            lp[0].fields[0],
            (
                Cow::Borrowed("f"),
                FieldValue::Str("白\n1".to_string().into_bytes().to_vec())
            )
        );
    }

    #[test]
    fn test_parse_err() {
        let parser = Parser::new(-1);
        let line = "log,host=domain.1,service=service-1,a,b,c d=1";
        let res = parser.parse(line);
        assert_eq!(res, Err(LineProtocolError::InvaildSyntax));
    }
}
