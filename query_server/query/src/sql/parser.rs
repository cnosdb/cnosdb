use std::collections::VecDeque;

use datafusion::logical_plan::FileType;
use datafusion::sql::parser::CreateExternalTable;
use snafu::ResultExt;
use spi::query::ast::{DropObject, ExtStatement, ObjectType};
use spi::query::parser::Parser as CnosdbParser;
use spi::query::ParserSnafu;
use sqlparser::{
    dialect::{keywords::Keyword, Dialect, GenericDialect},
    parser::{Parser, ParserError},
    tokenizer::{Token, Tokenizer},
};
use trace::debug;

pub type Result<T, E = ParserError> = std::result::Result<T, E>;

// Use `Parser::expected` instead, if possible
macro_rules! parser_err {
    ($MSG:expr) => {
        Err(ParserError::ParserError($MSG.to_string()))
    };
}

#[derive(Default)]
pub struct DefaultParser {}

impl CnosdbParser for DefaultParser {
    fn parse(&self, sql: &str) -> spi::query::Result<VecDeque<ExtStatement>> {
        ExtParser::parse_sql(sql).context(ParserSnafu)
    }
}

/// SQL Parser
pub struct ExtParser<'a> {
    parser: Parser<'a>,
}

impl<'a> ExtParser<'a> {
    /// Parse the specified tokens
    pub fn new(sql: &str) -> Result<Self> {
        let dialect = &GenericDialect {};
        ExtParser::new_with_dialect(sql, dialect)
    }
    /// Parse the specified tokens with dialect
    fn new_with_dialect(sql: &str, dialect: &'a dyn Dialect) -> Result<Self> {
        let mut tokenizer = Tokenizer::new(dialect, sql);
        let tokens = tokenizer.tokenize()?;

        Ok(ExtParser {
            parser: Parser::new(tokens, dialect),
        })
    }

    /// Parse a SQL statement and produce a set of statements
    pub fn parse_sql(sql: &str) -> Result<VecDeque<ExtStatement>> {
        let dialect = &GenericDialect {};
        ExtParser::parse_sql_with_dialect(sql, dialect)
    }
    /// Parse a SQL statement and produce a set of statements
    pub fn parse_sql_with_dialect(
        sql: &str,
        dialect: &dyn Dialect,
    ) -> Result<VecDeque<ExtStatement>> {
        let mut parser = ExtParser::new_with_dialect(sql, dialect)?;
        let mut stmts = VecDeque::new();
        let mut expecting_statement_delimiter = false;
        loop {
            // ignore empty statements (between successive statement delimiters)
            while parser.parser.consume_token(&Token::SemiColon) {
                expecting_statement_delimiter = false;
            }

            if parser.parser.peek_token() == Token::EOF {
                break;
            }
            if expecting_statement_delimiter {
                return parser.expected("end of statement", parser.parser.peek_token());
            }

            let statement = parser.parse_statement()?;
            stmts.push_back(statement);
            expecting_statement_delimiter = true;
        }

        debug!("Parser sql:{}, stmts:{:#?}", sql, stmts);

        Ok(stmts)
    }

    /// Parse a new expression
    fn parse_statement(&mut self) -> Result<ExtStatement> {
        match self.parser.peek_token() {
            Token::Word(w) => match w.keyword {
                Keyword::DROP => {
                    self.parser.next_token();
                    self.parse_drop()
                }
                Keyword::DESCRIBE | Keyword::DESC => {
                    self.parser.next_token();
                    self.parse_describe()
                }

                Keyword::SHOW => {
                    self.parser.next_token();
                    self.parse_show()
                }
                Keyword::ALTER => {
                    self.parser.next_token();
                    self.parse_alter()
                }
                Keyword::CREATE => {
                    self.parser.next_token();
                    self.parse_create()
                }
                _ => Ok(ExtStatement::SqlStatement(Box::new(
                    self.parser.parse_statement()?,
                ))),
            },
            _ => Ok(ExtStatement::SqlStatement(Box::new(
                self.parser.parse_statement()?,
            ))),
        }
    }
    // Report unexpected token
    fn expected<T>(&self, expected: &str, found: Token) -> Result<T> {
        parser_err!(format!("Expected {}, found: {}", expected, found))
    }

    /// Parse a SQL SHOW statement
    fn parse_show(&mut self) -> Result<ExtStatement> {
        if self.consume_token(&Token::make_keyword("TABLES")) {
            Ok(ExtStatement::ShowTables)
        } else if self.consume_token(&Token::make_keyword("DATABASES")) {
            Ok(ExtStatement::ShowDatabases)
        } else {
            self.expected("tables/databases", self.parser.peek_token())
        }
    }

    /// Parse a SQL DESCRIBE statement
    fn parse_describe(&mut self) -> Result<ExtStatement> {
        todo!()
    }

    /// Parse a SQL ALTER statement
    fn parse_alter(&mut self) -> Result<ExtStatement> {
        todo!()
    }

    /// This is a copy of the equivalent implementation in Datafusion.
    fn parse_csv_has_header(&mut self) -> bool {
        self.consume_token(&Token::make_keyword("WITH"))
            & self.consume_token(&Token::make_keyword("HEADER"))
            & self.consume_token(&Token::make_keyword("ROW"))
    }

    /// This is a copy of the equivalent implementation in Datafusion.
    fn parse_has_delimiter(&mut self) -> bool {
        self.consume_token(&Token::make_keyword("DELIMITER"))
    }

    /// This is a copy of the equivalent implementation in Datafusion.
    fn parse_delimiter(&mut self) -> Result<char, ParserError> {
        let token = self.parser.parse_literal_string()?;
        match token.len() {
            1 => Ok(token.chars().next().unwrap()),
            _ => Err(ParserError::TokenizerError(
                "Delimiter must be a single char".to_string(),
            )),
        }
    }

    /// This is a copy of the equivalent implementation in Datafusion.
    fn parse_has_partition(&mut self) -> bool {
        self.consume_token(&Token::make_keyword("PARTITIONED"))
            & self.consume_token(&Token::make_keyword("BY"))
    }

    /// Parses the set of valid formats
    /// This is a copy of the equivalent implementation in Datafusion.
    fn parse_file_format(&mut self) -> Result<FileType, ParserError> {
        match self.parser.next_token() {
            Token::Word(w) => parse_file_type(&w.value),
            unexpected => self.expected("one of PARQUET, NDJSON, or CSV", unexpected),
        }
    }

    /// This is a copy of the equivalent implementation in Datafusion.
    fn parse_partitions(&mut self) -> Result<Vec<String>, ParserError> {
        let mut partitions: Vec<String> = vec![];
        if !self.parser.consume_token(&Token::LParen) || self.parser.consume_token(&Token::RParen) {
            return Ok(partitions);
        }

        loop {
            if let Token::Word(_) = self.parser.peek_token() {
                let identifier = self.parser.parse_identifier()?;
                partitions.push(identifier.to_string());
            } else {
                return self.expected("partition name", self.parser.peek_token());
            }
            let comma = self.parser.consume_token(&Token::Comma);
            if self.parser.consume_token(&Token::RParen) {
                // allow a trailing comma, even though it's not in standard
                break;
            } else if !comma {
                return self.expected(
                    "',' or ')' after partition definition",
                    self.parser.peek_token(),
                );
            }
        }
        Ok(partitions)
    }

    fn parse_create_external_table(&mut self) -> Result<ExtStatement> {
        self.parser.expect_keyword(Keyword::TABLE)?;
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let table_name = self.parser.parse_object_name()?;
        let (columns, _) = self.parser.parse_columns()?;
        self.parser
            .expect_keywords(&[Keyword::STORED, Keyword::AS])?;

        // THIS is the main difference: we parse a different file format.
        let file_type = self.parse_file_format()?;

        let has_header = self.parse_csv_has_header();

        let has_delimiter = self.parse_has_delimiter();
        let delimiter = match has_delimiter {
            true => self.parse_delimiter()?,
            false => ',',
        };

        let table_partition_cols = if self.parse_has_partition() {
            self.parse_partitions()?
        } else {
            vec![]
        };

        self.parser.expect_keyword(Keyword::LOCATION)?;
        let location = self.parser.parse_literal_string()?;

        let create = CreateExternalTable {
            name: table_name.to_string(),
            columns,
            file_type,
            has_header,
            delimiter,
            location,
            table_partition_cols,
            if_not_exists,
        };
        Ok(ExtStatement::CreateExternalTable(create))
    }

    /// Parse a SQL CREATE statement
    fn parse_create(&mut self) -> Result<ExtStatement> {
        // Currently only supports the creation of external tables
        self.parser.expect_keyword(Keyword::EXTERNAL)?;
        self.parse_create_external_table()
    }

    /// Parse a SQL DROP statement
    fn parse_drop(&mut self) -> Result<ExtStatement> {
        let obj_type = if self.parser.parse_keyword(Keyword::TABLE) {
            ObjectType::Table
        } else if self.parser.parse_keyword(Keyword::DATABASE) {
            ObjectType::Database
        } else {
            return self.expected("TABLE,DATABASE after DROP", self.parser.peek_token());
        };
        let if_exist = self.parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let object_name = self.parser.parse_object_name()?.to_string();

        Ok(ExtStatement::Drop(DropObject {
            object_name,
            if_exist,
            obj_type,
        }))
    }

    fn consume_token(&mut self, expected: &Token) -> bool {
        let token = self.parser.peek_token().to_string().to_uppercase();
        let token = Token::make_keyword(&token);
        if token == *expected {
            self.parser.next_token();
            true
        } else {
            false
        }
    }
}

/// This is a copy of the equivalent implementation in Datafusion.
fn parse_file_type(s: &str) -> Result<FileType, ParserError> {
    match s.to_uppercase().as_str() {
        "PARQUET" => Ok(FileType::Parquet),
        "NDJSON" => Ok(FileType::NdJson),
        "CSV" => Ok(FileType::CSV),
        "AVRO" => Ok(FileType::Avro),
        other => Err(ParserError::ParserError(format!(
            "expect one of PARQUET, AVRO, NDJSON, or CSV, found: {}",
            other
        ))),
    }
}

#[cfg(test)]
mod tests {
    use spi::query::ast::{DropObject, ExtStatement};

    use super::*;

    #[test]
    fn test_drop() {
        let sql = "drop table test_tb";
        let statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            ExtStatement::Drop(DropObject {
                object_name,
                if_exist,
                obj_type,
            }) => {
                assert_eq!(object_name.to_string(), "test_tb".to_string());
                assert_eq!(if_exist.to_string(), "false".to_string());
                assert_eq!(obj_type.to_string(), "TABLE".to_string());
            }
            _ => panic!("failed"),
        }

        let sql = "drop table if exists test_tb";
        let statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            ExtStatement::Drop(DropObject {
                object_name,
                if_exist,
                obj_type,
            }) => {
                assert_eq!(object_name.to_string(), "test_tb".to_string());
                assert_eq!(if_exist.to_string(), "true".to_string());
                assert_eq!(obj_type.to_string(), "TABLE".to_string());
            }
            _ => panic!("failed"),
        }

        let sql = "drop database if exists test_db";
        let statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            ExtStatement::Drop(DropObject {
                object_name,
                if_exist,
                obj_type,
            }) => {
                assert_eq!(object_name.to_string(), "test_db".to_string());
                assert_eq!(if_exist.to_string(), "true".to_string());
                assert_eq!(obj_type.to_string(), "DATABASE".to_string());
            }
            _ => panic!("failed"),
        }

        let sql = "drop database test_db";
        let statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            ExtStatement::Drop(DropObject {
                object_name,
                if_exist,
                obj_type,
            }) => {
                assert_eq!(object_name.to_string(), "test_db".to_string());
                assert_eq!(if_exist.to_string(), "false".to_string());
                assert_eq!(obj_type.to_string(), "DATABASE".to_string());
            }
            _ => panic!("failed"),
        }
    }
}
