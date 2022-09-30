use std::string::ToString;

use crate::error::Result;
use lazy_regex::{lazy_regex, Lazy, Regex};

#[derive(Debug, Clone)]
pub struct Instruction {
    db_name: String,
    sort: bool,
    pretty: bool,
}

impl Default for Instruction {
    fn default() -> Self {
        Self {
            db_name: "".to_string(),
            sort: false,
            pretty: true,
        }
    }
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

    /// parse line to modify instruction
    pub fn parse(line: &str, old: &mut Self) {
        static DATABASE: Lazy<Regex> =
            lazy_regex!(r##"--[\s]*#DATABASE[\s]*=[\s]*([a-zA-z][a-zA-Z0-9]*)"##);

        static PRETTY: Lazy<Regex> = lazy_regex!(r##"--[\s]*#PRETTY[\s]*=[\s]*((true|false))"##);

        static SORT: Lazy<Regex> = lazy_regex!(r##"--[\s]*#SORT[\s]*=[\s]*((true|false))"##);

        // assert!(DATABASE.is_match(line));
        if let Some(captures) = DATABASE.captures(line) {
            if let Some(matches) = captures.get(1) {
                old.db_name = matches.as_str().to_string();
            }
            return;
        }

        if let Some(captures) = PRETTY.captures(line) {
            if let Some(matches) = captures.get(1) {
                if matches.as_str() == "true" {
                    old.pretty = true;
                } else {
                    old.pretty = false
                }

                return;
            }
        }

        if let Some(captures) = SORT.captures(line) {
            if let Some(matches) = captures.get(1) {
                if matches.as_str() == "true" {
                    old.sort = true;
                } else {
                    old.sort = false
                }
            }
        }
    }
}

#[test]
fn test_parse_instruction() {
    let line = r##"-- #DATABASE    =   abc"##;
    let mut instruction = Instruction::default();
    Instruction::parse(line, &mut instruction);
    assert_eq!(instruction.db_name, "abc");
    let line = r##"-- #SORT = true"##;
    Instruction::parse(line, &mut instruction);
    assert!(instruction.sort);
    let line = r##"-- #SORT = false"##;
    Instruction::parse(line, &mut instruction);
    assert!(!instruction.sort);
}

/// one Query
#[derive(Debug, Clone)]
pub struct Query {
    instruction: Instruction,
    query: String,
}

impl Query {
    pub fn as_str(&self) -> &str {
        &self.query
    }

    pub fn instruction(&self) -> &Instruction {
        &self.instruction
    }

    pub fn parse_queries(lines: &str) -> Result<Vec<Query>> {
        let mut queries = Vec::new();
        let mut query_build = QueryBuild::new();
        for line in lines.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            if line.starts_with("--") {
                if query_build.finished() {
                    query_build.parse_instruction(line);
                }
                continue;
            }
            query_build.push_str(line);

            if line.ends_with(';') {
                queries.push(query_build.finish());
            }
        }
        if !query_build.finished() {
            queries.push(query_build.finish());
        }
        Ok(queries)
    }
}

pub struct QueryBuild {
    buffer: String,
    instruction: Instruction,
}

impl QueryBuild {
    pub fn new() -> QueryBuild {
        QueryBuild {
            instruction: Instruction::default(),
            buffer: String::new(),
        }
    }

    pub fn parse_instruction(&mut self, line: &str) {
        Instruction::parse(line, &mut self.instruction);
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

    pub fn finish(&mut self) -> Query {
        if !self.buffer.ends_with(';') {
            self.buffer.push(';');
        }

        let res = Query {
            instruction: self.instruction.clone(),
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
        .finish();
    println!("{}", query.as_str());
}

#[test]
fn test_queries_parse() {
    let sqls = r##"
    -- #DATABASE=hello
    -- #SORT = true
    -- #PRETTY = false
    SElect * from table;


    SELECT name, age
    -- #PREETY = false
    From people;

    "##;
    let queries = Query::parse_queries(sqls);
    println!("{:#?}", &queries);
}
