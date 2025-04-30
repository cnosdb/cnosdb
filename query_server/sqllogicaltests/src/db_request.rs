use std::path::Path;
use std::str::FromStr;

use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::{alpha1, alphanumeric1, none_of, space0};
use nom::combinator::{map_parser, map_res, recognize};
use nom::error::Error as NomError;
use nom::multi::{many0_count, many1};
use nom::sequence::{delimited, pair};
use nom::Parser;

use crate::error::SqlError;
use crate::instance::{CnosdbClient, CnosdbColumnType};
use crate::utils::normalize;

type Result<T, E = SqlError> = std::result::Result<T, E>;

pub struct CnosdbRequest {
    protocol: Protocol,
    body: RequestBody,
}

impl CnosdbRequest {
    pub fn new(protocol: Protocol, body: RequestBody) -> Self {
        Self { protocol, body }
    }
}

pub enum Protocol {
    Default,
    Http,
}

#[derive(Clone, Debug)]
pub enum RequestBody {
    Sql(String),
    LineProtocol(String),
    OpenTSDBProtocol(String),
    OpenTSDBJson(String),
    Nothing,
}

/// Parse an instruction with a literal value.
///
/// ### Examples
/// - `"-- #HTTP_PORT = 8086"` returns `"8086"`
/// - `"-- #HTTP_PORT = 8086 -- comment_0"` returns `"8086"`
/// - `"SELECT * FROM tbl_a"` returns error
pub fn instruction_parse_str<'a>(
    instruction_name: &'a str,
) -> impl Parser<&'a str, Output = &'a str, Error = nom::error::Error<&'a str>> {
    delimited(
        (
            tag("--"),
            space0,
            tag("#"),
            tag_no_case(instruction_name),
            space0,
            tag("="),
            space0,
        ),
        recognize(many1(none_of(" \t\n\r"))),
        space0,
    )
}

/// Parse an instruction with a identity value (`[a-zA-Z_][a-zA-Z0-9_]*`).
pub fn instruction_parse_identity<'a>(
    instruction_name: &'a str,
) -> impl Parser<&'a str, Output = &'a str, Error = NomError<&'a str>> {
    map_parser(
        instruction_parse_str(instruction_name),
        recognize(pair(
            alt((alpha1, tag("_"))),
            many0_count(alt((alphanumeric1, tag("_")))),
        )),
    )
}

/// Parse an instruction with a value and convert it to a type.
pub fn instruction_parse_to<'a, T: FromStr>(
    instruction_name: &'a str,
) -> impl Parser<&'a str, Output = T, Error = NomError<&'a str>> {
    map_res(instruction_parse_str(instruction_name), |s: &str| {
        s.parse::<T>()
    })
}

/// Match an instruction line.
///
/// ### Examples
/// - `"-- #LP_BEGIN"` returns true
/// - `"SELECT * FROM tbl_a"` returns false
pub fn instruction_match(instruction_name: &str, input: &str) -> bool {
    let mut parser = (
        tag("--"),
        space0::<&str, NomError<&str>>,
        tag("#"),
        tag_no_case(instruction_name),
    );
    parser.parse(input).is_ok()
}

impl CnosdbRequest {
    pub fn parse_db_request(
        lines: &str,
        options: &mut CnosdbClient,
    ) -> Result<CnosdbRequest, SqlError> {
        let lines = lines
            .lines()
            .filter(|l| !l.trim().is_empty())
            .collect::<Vec<_>>();
        let mut sql = vec![];
        let mut protocol = Protocol::Default;
        for (i, line) in lines.iter().enumerate() {
            let line = *line;
            if instruction_match("LP_BEGIN", line) {
                let block = parse_block(&lines[i + 1..], "LP_END")?;
                return Ok(CnosdbRequest::new(
                    protocol,
                    RequestBody::LineProtocol(block),
                ));
            } else if instruction_match("OPENTSDB_BEGIN", line) {
                let block = parse_block(&lines[i + 1..], "OPENTSDB_END")?;
                return Ok(CnosdbRequest::new(
                    protocol,
                    RequestBody::OpenTSDBProtocol(block),
                ));
            } else if instruction_match("OPENTSDB_JSON_BEGIN", line) {
                let block = parse_block(&lines[i + 1..], "OPENTSDB_JSON_END")?;
                return Ok(CnosdbRequest::new(
                    protocol,
                    RequestBody::OpenTSDBJson(block),
                ));
            } else if instruction_match("HTTP", line) {
                protocol = Protocol::Http;
            } else if instruction_match("", line) {
                options.apply_instruction(line)
            } else {
                sql.push(line)
            }
        }
        if sql.is_empty() {
            Ok(CnosdbRequest::new(protocol, RequestBody::Nothing))
        } else {
            Ok(CnosdbRequest::new(
                protocol,
                RequestBody::Sql(sql.join("\n")),
            ))
        }
    }

    pub async fn execute(
        &self,
        client: &CnosdbClient,
        path: &Path,
    ) -> Result<(Vec<CnosdbColumnType>, Vec<Vec<String>>)> {
        match &self.body {
            RequestBody::Sql(sql) => match self.protocol {
                Protocol::Default => self.execute_record_batch(client, path, sql).await,
                Protocol::Http => self.execute_http(client, path, sql).await,
            },
            RequestBody::LineProtocol(lp) => {
                println!("[{}] Execute LineProtocol: \"{}\"", path.display(), lp);
                client.http_api_v1_write(lp).await?;
                Ok((vec![], vec![]))
            }
            RequestBody::OpenTSDBProtocol(content) => {
                println!(
                    "[{}] Execute OpenTSDBProtocol: \"{}\"",
                    path.display(),
                    content
                );
                client.http_api_v1_opentsdb_write(content).await?;
                Ok((vec![], vec![]))
            }
            RequestBody::OpenTSDBJson(content) => {
                println!(
                    "[{}] Execute OpenTSDBJsonProtocol: \"{}\"",
                    path.display(),
                    content
                );
                client.http_api_v1_opentsdb_put(content).await?;
                Ok((vec![], vec![]))
            }
            RequestBody::Nothing => {
                println!("[{}] Execute Nothing", path.display());
                Ok((vec![], vec![]))
            }
        }
    }

    async fn execute_record_batch(
        &self,
        client: &CnosdbClient,
        path: &Path,
        sql: &str,
    ) -> Result<(Vec<CnosdbColumnType>, Vec<Vec<String>>)> {
        println!("[{}] Execute Sql (flight): \"{}\"", path.display(), sql);
        let (schema, batches) = client.flight_query(sql).await?;
        let types = normalize::convert_schema_to_types(schema.fields());
        let rows = normalize::convert_batches(batches)?;
        Ok((types, rows))
    }

    async fn execute_http(
        &self,
        client: &CnosdbClient,
        path: &Path,
        sql: &str,
    ) -> Result<(Vec<CnosdbColumnType>, Vec<Vec<String>>)> {
        println!("[{}] Execute Sql (http): \"{}\"", path.display(), sql);
        let rows = client.http_api_v1_sql(sql).await?;
        let cols_num = rows.first().map(|c| c.len()).unwrap_or(0);
        Ok((vec![CnosdbColumnType::Text; cols_num], rows))
    }
}

fn parse_block(lines: &[&str], end: &str) -> Result<String, SqlError> {
    let block_end = lines
        .iter()
        .enumerate()
        .find(|(_, l)| instruction_match(end, l));

    match block_end {
        None => Err(SqlError::Other {
            reason: format!("Expected block ending '{end}' not found"),
        }),
        Some((i, _)) => {
            if i != lines.len() - 1 {
                Err(SqlError::Other {
                    reason: format!(
                        "Expected block ending '{end}' found too early ({i} < {})",
                        lines.len() - 1
                    ),
                })
            } else {
                Ok(lines[0..i].join("\n"))
            }
        }
    }
}

#[cfg(test)]
mod test {
    use nom::Parser as _;

    use super::{
        instruction_match, instruction_parse_identity, instruction_parse_str, instruction_parse_to,
        parse_block,
    };

    #[test]
    fn test_parsers() {
        {
            let mut parser = instruction_parse_str("HTTP_PORT");
            assert_eq!(parser.parse("-- #HTTP_PORT = 8086").unwrap().1, "8086");
            assert_eq!(parser.parse("--#HTTP_PORT = 8086").unwrap().1, "8086");
            assert_eq!(
                parser.parse("-- #HTTP_PORT = 8086 -- comment").unwrap().1,
                "8086"
            );
            assert!(parser.parse("-- # HTTP_PORT = 8086").is_err());
            assert!(parser.parse("-- #HTTP_PORT").is_err());
            assert!(parser.parse("-- #HTTP_PORT = ").is_err());
            assert!(parser.parse("SELECT * FROM tbl_a").is_err());
            assert!(parser.parse("-- #").is_err());
            assert!(parser.parse("").is_err());
        }
        {
            let mut parser = instruction_parse_identity("DATABASE");
            assert_eq!(parser.parse("-- #DATABASE = dba").unwrap().1, "dba");
            assert_eq!(parser.parse("--#DATABASE = dba").unwrap().1, "dba");
            assert_eq!(
                parser.parse("-- #DATABASE = dba -- comment").unwrap().1,
                "dba"
            );
            assert!(parser.parse("-- # DATABASE = dba").is_err());
            assert!(parser.parse("-- #DATABASE = 1a").is_err());
            assert!(parser.parse("-- #DATABASE = ").is_err());
            assert!(parser.parse("SELECT * FROM tbl_a").is_err());
            assert!(parser.parse("-- #").is_err());
            assert!(parser.parse("").is_err());
        }
        {
            let mut parser = instruction_parse_to::<u16>("HTTP_PORT");
            assert_eq!(parser.parse("-- #HTTP_PORT = 8086").unwrap().1, 8086);
            assert_eq!(parser.parse("--#HTTP_PORT = 8086").unwrap().1, 8086);
            assert_eq!(
                parser.parse("-- #HTTP_PORT = 8086 -- comment").unwrap().1,
                8086
            );
            assert!(parser.parse("-- # HTTP_PORT = 8086").is_err());
            assert!(parser.parse("-- #HTTP_PORT").is_err());
            assert!(parser.parse("-- #HTTP_PORT = ").is_err());
            assert!(parser.parse("SELECT * FROM tbl_a").is_err());
            assert!(parser.parse("-- #").is_err());
            assert!(parser.parse("").is_err());
        }
        {
            assert!(instruction_match("LP_BEGIN", "-- #LP_BEGIN"));
            assert!(instruction_match("LP_BEGIN", "--#LP_BEGIN"));
            assert!(!instruction_match("LP_BEGIN", "-- # LP_BEGIN"));
        }
        {
            assert_eq!(
                parse_block(
                    &["ma,ta=a1 fa=1 1", "ma,ta=a2 fa=2 2", "-- #LP_END",],
                    "LP_END"
                )
                .unwrap(),
                "ma,ta=a1 fa=1 1\nma,ta=a2 fa=2 2".to_string()
            );
            assert!(parse_block(
                &["ma,ta=a1 fa=1 1", "-- #LP_END", "ma,ta=a2 fa=2 2",],
                "LP_END"
            )
            .is_err(),);
        }
    }
}
