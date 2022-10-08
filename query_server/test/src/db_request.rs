use std::string::ToString;

use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::{alphanumeric1, space0};
use nom::combinator::map_opt;
use nom::sequence::{delimited, tuple};
use nom::IResult;

#[derive(Debug, Clone)]
pub struct Instruction {
    db_name: String,
    sort: bool,
    pretty: bool,
    user_name: String,
}

impl Default for Instruction {
    fn default() -> Self {
        Self {
            db_name: "public".to_string(),
            sort: false,
            pretty: true,
            user_name: "cnosdb".to_string(),
        }
    }
}
fn instruction_parse_str<'a>(
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
        alphanumeric1,
        space0,
    )
}

fn instruction_parse_bool<'a>(
    instruction_name: &'a str,
) -> impl FnMut(&'a str) -> IResult<&'a str, bool> {
    map_opt(instruction_parse_str(instruction_name), |s: &str| {
        match s.to_lowercase().as_str() {
            "t" | "true" => Some(true),
            "f" | "false" => Some(false),
            _ => None,
        }
    })
}

impl Instruction {
    /// get database where sql is running
    pub fn db_name(&self) -> &str {
        &self.db_name
    }

    /// the result whether need sort
    /// TODO
    #[allow(dead_code)]
    pub fn sort(&self) -> bool {
        self.sort
    }

    /// the result whether need pretty
    pub fn pretty(&self) -> bool {
        self.pretty
    }
    pub fn user_name(&self) -> &str {
        &self.user_name
    }

    /// parse line to modify instruction
    pub fn parse_and_change(&mut self, line: &str) {
        if let Ok((_, dbname)) = instruction_parse_str("DATABASE")(line) {
            self.db_name = dbname.to_string();
        }

        if let Ok((_, user_name)) = instruction_parse_str("USER_NAME")(line) {
            self.user_name = user_name.to_string();
        }

        if let Ok((_, pretty)) = instruction_parse_bool("PRETTY")(line) {
            self.pretty = pretty;
        }

        if let Ok((_, sort)) = instruction_parse_bool("SORT")(line) {
            self.sort = sort;
        }
    }
}

#[test]
fn test_parse_instruction() {
    let line = r##"--#DATABASE    =   abc"##;
    let mut instruction = Instruction::default();
    instruction.parse_and_change(line);
    assert_eq!(instruction.db_name, "abc");
    let line = r##"--#SORT = true"##;
    instruction.parse_and_change(line);
    assert!(instruction.sort);
    let line = r##"--#SORT = false"##;
    instruction.parse_and_change(line);
    assert!(!instruction.sort);
}

/// one Query
#[derive(Debug, Clone)]
pub struct Query {
    instruction: Instruction,
    query: String,
}

#[derive(Clone, Debug)]
pub struct LineProtocol {
    instruction: Instruction,
    lines: String,
}

pub struct LineProtocolBuild {
    lines: String,
}

impl LineProtocolBuild {
    pub fn finish(self, instruction: Instruction) -> LineProtocol {
        LineProtocol {
            instruction,
            lines: self.lines,
        }
    }
    pub fn new() -> LineProtocolBuild {
        LineProtocolBuild {
            lines: String::new(),
        }
    }
    pub fn push(&mut self, line: &str) {
        self.lines.push_str(line);
        self.lines.push('\n');
    }

    pub fn finished(&self) -> bool {
        self.lines.is_empty()
    }
}

impl LineProtocol {
    pub fn as_str(&self) -> &str {
        self.lines.as_str()
    }
    pub fn instruction(&self) -> &Instruction {
        &self.instruction
    }
}

#[derive(Clone, Debug)]
pub enum DBRequest {
    Query(Query),
    LineProtocol(LineProtocol),
}

impl DBRequest {
    pub fn parse_requests(lines: &str) -> Vec<DBRequest> {
        let mut requests = Vec::<DBRequest>::new();
        let mut instruction = Instruction::default();

        let mut query_build = QueryBuild::new();
        let mut parsing_line_protocol = false;

        let mut line_protocol_build = LineProtocolBuild::new();

        for line in lines.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            if line.starts_with("--#LP_BEGIN") {
                parsing_line_protocol = true;
                continue;
            }
            if line.starts_with("--#LP_END") {
                parsing_line_protocol = false;
                requests.push(DBRequest::LineProtocol(
                    line_protocol_build.finish(instruction.clone()),
                ));
                line_protocol_build = LineProtocolBuild::new();
                continue;
            }

            if line.starts_with("--") {
                instruction.parse_and_change(line);
                continue;
            }

            if parsing_line_protocol {
                line_protocol_build.push(line);
            } else {
                query_build.push_str(line);
                if line.ends_with(';') {
                    let query = query_build.finish(instruction.clone());
                    requests.push(DBRequest::Query(query));
                    query_build = QueryBuild::new();
                }
            }
        }
        if !query_build.finished() {
            let query = query_build.finish(instruction.clone());
            requests.push(DBRequest::Query(query));
        }

        if !line_protocol_build.finished() {
            let line_protocol = line_protocol_build.finish(instruction.clone());
            requests.push(DBRequest::LineProtocol(line_protocol));
        }
        requests
    }
}

impl Query {
    pub fn as_str(&self) -> &str {
        &self.query
    }

    pub fn instruction(&self) -> &Instruction {
        &self.instruction
    }
}

pub struct QueryBuild {
    buffer: String,
}

impl QueryBuild {
    pub fn new() -> QueryBuild {
        QueryBuild {
            buffer: String::new(),
        }
    }

    pub fn push_str(&mut self, line: &str) -> &mut Self {
        let line = line.trim();
        if line.is_empty() {
            return self;
        }

        if !self.buffer.is_empty() {
            self.buffer.push(' ');
        }

        self.buffer.push_str(line);

        self
    }

    pub fn finish(&mut self, instruction: Instruction) -> Query {
        if !self.buffer.ends_with(';') {
            self.buffer.push(';');
        }

        let res = Query {
            instruction,
            query: self.buffer.clone(),
        };

        self.buffer.clear();
        res
    }

    pub fn finished(&self) -> bool {
        self.buffer.is_empty()
    }
}

#[test]
fn test_query_build() {
    let mut build = QueryBuild::new();
    let query = build
        .push_str("Select * ")
        .push_str("From table\n")
        .finish(Instruction::default());
    println!("{}", query.as_str());
}

#[test]
fn test_queries_parse() {
    let content = r##"
    -- #DATABASE=hello
    -- #SORT = true
    -- #PRETTY = false
    SElect * from table;


    SELECT name, age
    -- #PREETY = false
    From people;

    "##;
    let db_requests = DBRequest::parse_requests(content);
    println!("{:#?}", &db_requests);
}
