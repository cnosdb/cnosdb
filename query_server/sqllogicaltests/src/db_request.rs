use std::path::Path;
use std::str::FromStr;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::{alpha1, alphanumeric1, none_of, space0};
use nom::combinator::{map_parser, map_res, recognize};
use nom::multi::{many0_count, many1};
use nom::sequence::{delimited, pair, tuple};
use nom::IResult;

use crate::error::SqlError;
use crate::instance::{
    run_lp_write, run_open_tsdb_json_write, run_open_tsdb_write, run_query, SqlClientOptions,
};

type Result<T, E = SqlError> = std::result::Result<T, E>;

#[derive(Clone, Debug)]
pub enum DBRequest {
    Sql(String),
    LineProtocol(String),
    OpenTSDBProtocol(String),
    OpenTSDBJson(String),
    Nothing,
}

pub fn instruction_parse_str<'a>(
    instruction_name: &'a str,
) -> impl FnMut(&'a str) -> IResult<&'a str, &'a str> {
    delimited(
        tuple((
            tag("--"),
            space0,
            tag("#"),
            tag_no_case(instruction_name),
            space0,
            tag("="),
            space0,
        )),
        recognize(many1(none_of(" \t\n\r"))),
        space0,
    )
}

pub fn instruction_parse_identity<'a>(
    instruction_name: &'a str,
) -> impl FnMut(&'a str) -> IResult<&'a str, &'a str> {
    map_parser(
        instruction_parse_str(instruction_name),
        recognize(pair(
            alt((alpha1, tag("_"))),
            many0_count(alt((alphanumeric1, tag("_")))),
        )),
    )
}

pub fn instruction_parse_to<'a, T: FromStr>(
    instruction_name: &'a str,
) -> impl FnMut(&'a str) -> IResult<&'a str, T> {
    map_res(instruction_parse_str(instruction_name), |s: &str| {
        s.parse::<T>()
    })
}

pub fn instruction_match(instruction_name: &str, input: &str) -> bool {
    tuple((
        tag("--"),
        space0::<&str, nom::error::Error<&str>>,
        tag("#"),
        tag_no_case(instruction_name),
    ))(input)
    .is_ok()
}

impl DBRequest {
    pub fn parse_db_request(
        lines: &str,
        options: &mut SqlClientOptions,
    ) -> Result<DBRequest, SqlError> {
        let lines = lines
            .lines()
            .filter(|l| !l.trim().is_empty())
            .collect::<Vec<_>>();
        let mut sql = vec![];
        for (i, line) in lines.iter().enumerate() {
            let line = *line;
            if instruction_match("LP_BEGIN", line) {
                return Ok(DBRequest::LineProtocol(parse_block(
                    &lines[i + 1..],
                    "LP_END",
                )?));
            } else if instruction_match("OPENTSDB_BEGIN", line) {
                return Ok(DBRequest::OpenTSDBProtocol(parse_block(
                    &lines[i + 1..],
                    "OPENTSDB_END",
                )?));
            } else if instruction_match("OPENTSDB_JSON_BEGIN", line) {
                return Ok(DBRequest::OpenTSDBJson(parse_block(
                    &lines[i + 1..],
                    "OPENTSDB_JSON_END",
                )?));
            } else if instruction_match("", line) {
                options.parse_and_change(line)
            } else {
                sql.push(line)
            }
        }
        if sql.is_empty() {
            Ok(DBRequest::Nothing)
        } else {
            Ok(DBRequest::Sql(sql.join("\n")))
        }
    }

    pub async fn execute(
        &self,
        options: &SqlClientOptions,
        path: &Path,
    ) -> Result<(Schema, Vec<RecordBatch>)> {
        match self {
            DBRequest::Sql(sql) => {
                println!("[{}] Execute Sql: \"{}\"", path.display(), sql);
                Ok(run_query(options, sql).await?)
            }
            DBRequest::LineProtocol(lp) => {
                println!("[{}] Execute LineProtocol: \"{}\"", path.display(), lp);
                run_lp_write(options, lp).await?;
                Ok((Schema::empty(), vec![]))
            }
            DBRequest::OpenTSDBProtocol(open_tsdb) => {
                println!(
                    "[{}] Execute OpenTSDBProtocol: \"{}\"",
                    path.display(),
                    open_tsdb
                );
                run_open_tsdb_write(options, open_tsdb).await?;
                Ok((Schema::empty(), vec![]))
            }
            DBRequest::OpenTSDBJson(open_tsdb_json) => {
                println!(
                    "[{}] Execute OpenTSDBJsonProtocol: \"{}\"",
                    path.display(),
                    open_tsdb_json
                );
                run_open_tsdb_json_write(options, open_tsdb_json).await?;
                Ok((Schema::empty(), vec![]))
            }
            DBRequest::Nothing => {
                println!("[{}] Execute Nothing", path.display());
                Ok((Schema::empty(), vec![]))
            }
        }
    }
}

fn parse_block(lines: &[&str], end: &str) -> Result<String, SqlError> {
    let iter = lines
        .iter()
        .enumerate()
        .find(|(_, l)| instruction_match(end, l));

    match iter {
        None => Err(SqlError::Other {
            reason: "parse error".to_string(),
        }),
        Some((i, _)) => {
            if i != lines.len() - 1 {
                Err(SqlError::Other {
                    reason: "parse error".to_string(),
                })
            } else {
                Ok(lines[0..i].join("\n"))
            }
        }
    }
}
