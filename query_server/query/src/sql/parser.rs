use std::collections::VecDeque;
use std::fmt::Display;
use std::ops::Not;
use std::str::FromStr;

use datafusion::common::parsers::CompressionTypeVariant;
use datafusion::sql::parser::CreateExternalTable;
use datafusion::sql::sqlparser::ast::{
    DataType, Expr, Ident, ObjectName, Offset, OrderByExpr, SqlOption, TableFactor,
};
use datafusion::sql::sqlparser::dialect::keywords::Keyword;
use datafusion::sql::sqlparser::dialect::{Dialect, GenericDialect};
use datafusion::sql::sqlparser::parser::{IsOptional, Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::{Token, TokenWithLocation, Tokenizer};
use models::codec::Encoding;
use models::meta_data::{NodeId, ReplicationSetId, VnodeId};
use snafu::ResultExt;
use spi::query::ast::{
    self, parse_string_value, Action, AlterDatabase, AlterTable, AlterTableAction, AlterTenant,
    AlterTenantOperation, AlterUser, AlterUserOperation, ChecksumGroup, ColumnOption, CompactVnode,
    CopyIntoLocation, CopyIntoTable, CopyTarget, CopyVnode, CreateDatabase, CreateRole,
    CreateStream, CreateTable, CreateTenant, CreateUser, DatabaseOptions, DescribeDatabase,
    DescribeTable, DropDatabaseObject, DropGlobalObject, DropTenantObject, DropVnode, Explain,
    ExtStatement, GrantRevoke, MoveVnode, OutputMode, Privilege, ShowSeries, ShowTagBody,
    ShowTagValues, Trigger, UriLocation, With,
};
use spi::query::logical_planner::{DatabaseObjectType, GlobalObjectType, TenantObjectType};
use spi::query::parser::Parser as CnosdbParser;
use spi::ParserSnafu;
use trace::debug;

// support tag token
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(non_camel_case_types, clippy::upper_case_acronyms)]
enum CnosKeyWord {
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    TAGS,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    CODEC,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    TAG,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    FIELD,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    DATABASES,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    TENANT,

    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    TTL,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    SHARD,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    VNODE_DURATION,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    REPLICA,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    PRECISION,

    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    QUERIES,

    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    INHERIT,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    READ,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    WRITE,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    REMOVE,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    SERIES,

    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    FILES,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    PATTERN,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    FILE_FORMAT,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    COPY_OPTIONS,

    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    VNODE,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    NODE,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    MOVE,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    COMPACT,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    CHECKSUM,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    STREAM,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    STREAMS,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    TRIGGER,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    WATERMARK,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    OUTPUT_MODE,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    ONCE,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    COMPLETE,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    APPEND,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    UNSET,
}

impl FromStr for CnosKeyWord {
    type Err = ParserError;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "TAGS" => Ok(CnosKeyWord::TAGS),
            "CODEC" => Ok(CnosKeyWord::CODEC),
            "TAG" => Ok(CnosKeyWord::TAG),
            "FIELD" => Ok(CnosKeyWord::FIELD),
            "TTL" => Ok(CnosKeyWord::TTL),
            "SHARD" => Ok(CnosKeyWord::SHARD),
            "VNODE_DURATION" => Ok(CnosKeyWord::VNODE_DURATION),
            "REPLICA" => Ok(CnosKeyWord::REPLICA),
            "PRECISION" => Ok(CnosKeyWord::PRECISION),
            "DATABASES" => Ok(CnosKeyWord::DATABASES),
            "QUERIES" => Ok(CnosKeyWord::QUERIES),
            "TENANT" => Ok(CnosKeyWord::TENANT),
            "INHERIT" => Ok(CnosKeyWord::INHERIT),
            "READ" => Ok(CnosKeyWord::READ),
            "WRITE" => Ok(CnosKeyWord::WRITE),
            "REMOVE" => Ok(CnosKeyWord::REMOVE),
            "SERIES" => Ok(CnosKeyWord::SERIES),
            "FILES" => Ok(CnosKeyWord::FILES),
            "PATTERN" => Ok(CnosKeyWord::PATTERN),
            "FILE_FORMAT" => Ok(CnosKeyWord::FILE_FORMAT),
            "COPY_OPTIONS" => Ok(CnosKeyWord::COPY_OPTIONS),
            "VNODE" => Ok(CnosKeyWord::VNODE),
            "NODE" => Ok(CnosKeyWord::NODE),
            "MOVE" => Ok(CnosKeyWord::MOVE),
            "COMPACT" => Ok(CnosKeyWord::COMPACT),
            "CHECKSUM" => Ok(CnosKeyWord::CHECKSUM),
            "STREAM" => Ok(CnosKeyWord::STREAM),
            "STREAMS" => Ok(CnosKeyWord::STREAMS),
            "TRIGGER" => Ok(CnosKeyWord::TRIGGER),
            "WATERMARK" => Ok(CnosKeyWord::WATERMARK),
            "OUTPUT_MODE" => Ok(CnosKeyWord::OUTPUT_MODE),
            "ONCE" => Ok(CnosKeyWord::ONCE),
            "COMPLETE" => Ok(CnosKeyWord::COMPLETE),
            "APPEND" => Ok(CnosKeyWord::APPEND),
            "UNSET" => Ok(CnosKeyWord::UNSET),
            _ => Err(ParserError::ParserError(format!(
                "fail parse {} to CnosKeyWord",
                s
            ))),
        }
    }
}

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
    fn parse(&self, sql: &str) -> spi::Result<VecDeque<ExtStatement>> {
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
            parser: Parser::new(dialect).with_tokens(tokens),
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

        debug!("Parser sql: {}, stmts: {:#?}", sql, stmts);

        Ok(stmts)
    }

    /// Parse a new expression
    fn parse_statement(&mut self) -> Result<ExtStatement> {
        match self.parser.peek_token().token {
            Token::Word(w) => match w.keyword {
                Keyword::COPY => {
                    self.parser.next_token();
                    self.parse_copy()
                }
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
                Keyword::GRANT => {
                    self.parser.next_token();
                    self.parse_grant()
                }
                Keyword::REVOKE => {
                    self.parser.next_token();
                    self.parse_revoke()
                }
                Keyword::EXPLAIN => {
                    self.parser.next_token();
                    self.parse_explain()
                }
                _ => {
                    if let Ok(word) = CnosKeyWord::from_str(&w.to_string()) {
                        return match word {
                            CnosKeyWord::MOVE => {
                                self.parser.next_token();
                                self.parse_move()
                            }
                            CnosKeyWord::COMPACT => {
                                self.parser.next_token();
                                self.parse_compact()
                            }
                            CnosKeyWord::CHECKSUM => {
                                self.parser.next_token();
                                self.parse_checksum()
                            }
                            _ => Ok(ExtStatement::SqlStatement(Box::new(
                                self.parser.parse_statement()?,
                            ))),
                        };
                    }
                    Ok(ExtStatement::SqlStatement(Box::new(
                        self.parser.parse_statement()?,
                    )))
                }
            },
            _ => Ok(ExtStatement::SqlStatement(Box::new(
                self.parser.parse_statement()?,
            ))),
        }
    }
    // Report unexpected token
    fn expected<T>(&self, expected: &str, found: impl Display) -> Result<T> {
        parser_err!(format!("Expected {}, found: {}", expected, found))
    }

    // fn expected_cnos_keyword<T>(&self, expected: &str, found: CnosKeyWord) -> Result<T> {
    //     parser_err!(format!("Expected {}, found: {:?}", expected, found))
    // }

    /// Parse a SQL SHOW statement
    fn parse_show(&mut self) -> Result<ExtStatement> {
        if self.parser.parse_keyword(Keyword::TABLES) {
            self.parse_show_tables()
        } else if self.parse_cnos_keyword(CnosKeyWord::DATABASES) {
            self.parse_show_databases()
        } else if self.parse_cnos_keyword(CnosKeyWord::SERIES) {
            self.parse_show_series()
        } else if self.parse_cnos_keyword(CnosKeyWord::TAG) {
            if self.parser.parse_keyword(Keyword::VALUES) {
                self.parse_show_tag_values()
            } else {
                self.expected("VALUES", self.parser.peek_token())
            }
        } else if self.parse_cnos_keyword(CnosKeyWord::QUERIES) {
            self.parse_show_queries()
        } else if self.parse_cnos_keyword(CnosKeyWord::STREAMS) {
            let verbose = self
                .parser
                .parse_keyword(Keyword::VERBOSE)
                .then_some(true)
                .unwrap_or_default();
            Ok(ExtStatement::ShowStreams(ast::ShowStreams { verbose }))
        } else {
            self.expected(
                "TABLES or DATABASES or SERIES or TAG or QUERIES or STREAMS",
                self.parser.peek_token(),
            )
        }
    }

    fn parse_on_database(&mut self) -> Result<Option<Ident>> {
        if self.parser.parse_keyword(Keyword::ON) {
            Ok(Some(self.parser.parse_identifier()?))
        } else {
            Ok(None)
        }
    }

    fn parse_where(&mut self) -> Result<Option<Expr>> {
        if self.parser.parse_keyword(Keyword::WHERE) {
            Ok(Some(self.parser.parse_expr()?))
        } else {
            Ok(None)
        }
    }

    fn parse_limit_offset(&mut self) -> Result<(Option<Expr>, Option<Offset>)> {
        let mut limit = None;
        let mut offset = None;
        for _x in 0..2 {
            if limit.is_none() && self.parser.parse_keyword(Keyword::LIMIT) {
                limit = self.parser.parse_limit()?
            }

            if offset.is_none() && self.parser.parse_keyword(Keyword::OFFSET) {
                offset = Some(self.parser.parse_offset()?)
            }
        }
        Ok((limit, offset))
    }

    fn parse_order_by(&mut self) -> Result<Vec<OrderByExpr>> {
        Ok(
            if self.parser.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
                self.parser
                    .parse_comma_separated(Parser::parse_order_by_expr)?
            } else {
                vec![]
            },
        )
    }

    fn parse_with(&mut self) -> Result<With> {
        self.parser
            .expect_keywords(&[Keyword::WITH, Keyword::KEY])?;

        match self.parser.next_token().token {
            Token::Eq => Ok(With::Equal(self.parser.parse_identifier()?)),
            Token::Neq => Ok(With::UnEqual(self.parser.parse_identifier()?)),
            Token::Word(word) => match &word.keyword {
                Keyword::IN => {
                    self.parser.expect_token(&Token::LParen)?;
                    let idents = self
                        .parser
                        .parse_comma_separated(Parser::parse_identifier)?;
                    self.parser.expect_token(&Token::RParen)?;
                    Ok(With::In(idents))
                }
                Keyword::NOT => {
                    self.parser.expect_keyword(Keyword::IN)?;
                    self.parser.expect_token(&Token::LParen)?;
                    let idents = self
                        .parser
                        .parse_comma_separated(Parser::parse_identifier)?;
                    self.parser.expect_token(&Token::RParen)?;
                    Ok(With::NotIn(idents))
                }
                _ => self.expected("=, !=, <>, IN, NOT IN", Token::Word(word)),
            },
            token => self.expected("=, !=, <>, IN, NOT IN", token),
        }
    }

    fn parse_show_series(&mut self) -> Result<ExtStatement> {
        let database_name = self.parse_on_database()?;
        self.parser.expect_keyword(Keyword::FROM)?;
        let table = self.parser.parse_identifier()?;
        let selection = self.parse_where()?;
        let order_by = self.parse_order_by()?;
        let (limit, offset) = self.parse_limit_offset()?;

        Ok(ExtStatement::ShowSeries(Box::new(ShowSeries {
            body: ShowTagBody {
                database_name,
                table,
                selection,
                order_by,
                limit,
                offset,
            },
        })))
    }

    fn parse_show_tag_values(&mut self) -> Result<ExtStatement> {
        let database_name = self.parse_on_database()?;
        self.parser.expect_keyword(Keyword::FROM)?;
        let table = self.parser.parse_identifier()?;
        let with = self.parse_with()?;
        let selection = self.parse_where()?;
        let order_by = self.parse_order_by()?;
        let (limit, offset) = self.parse_limit_offset()?;
        Ok(ExtStatement::ShowTagValues(Box::new(ShowTagValues {
            body: ShowTagBody {
                database_name,
                table,
                selection,
                order_by,
                limit,
                offset,
            },
            with,
        })))
    }

    fn parse_explain(&mut self) -> Result<ExtStatement> {
        let analyze = self.parser.parse_keyword(Keyword::ANALYZE);
        let verbose = self.parser.parse_keyword(Keyword::VERBOSE);
        let mut format = None;
        if self.parser.parse_keyword(Keyword::FORMAT) {
            format = Some(self.parser.parse_analyze_format()?);
        }
        if self.parser.parse_keyword(Keyword::EXPLAIN) {
            return Err(ParserError::ParserError(
                "EXPLAIN can only appear once".to_string(),
            ));
        }

        let stmt = self.parse_statement()?;

        Ok(ExtStatement::Explain(Explain {
            analyze,
            verbose,
            format,
            ext_statement: Box::new(stmt),
        }))
    }

    fn parse_show_queries(&mut self) -> Result<ExtStatement> {
        Ok(ExtStatement::ShowQueries)
    }

    fn parse_show_databases(&mut self) -> Result<ExtStatement> {
        Ok(ExtStatement::ShowDatabases())
    }

    fn parse_show_tables(&mut self) -> Result<ExtStatement> {
        Ok(ExtStatement::ShowTables(self.parse_on_database()?))
    }

    /// Parse a SQL DESCRIBE DATABASE statement
    fn parse_describe_database(&mut self) -> Result<ExtStatement> {
        debug!("Parse Describe DATABASE statement");
        let database_name = self.parser.parse_identifier()?;

        let describe = DescribeDatabase { database_name };

        Ok(ExtStatement::DescribeDatabase(describe))
    }

    /// Parse a SQL DESCRIBE TABLE statement
    fn parse_describe_table(&mut self) -> Result<ExtStatement> {
        let table_name = self.parser.parse_object_name()?;

        let describe = DescribeTable { table_name };

        Ok(ExtStatement::DescribeTable(describe))
    }

    fn parse_describe(&mut self) -> Result<ExtStatement> {
        if self.parser.parse_keyword(Keyword::TABLE) {
            self.parse_describe_table()
        } else if self.parser.parse_keyword(Keyword::DATABASE) {
            self.parse_describe_database()
        } else {
            self.expected("table/database", self.parser.peek_token())
        }
    }

    /// Parse a SQL ALTER statement
    fn parse_alter(&mut self) -> Result<ExtStatement> {
        if self.parser.parse_keyword(Keyword::TABLE) {
            self.parse_alter_table()
        } else if self.parser.parse_keyword(Keyword::DATABASE) {
            self.parse_alter_database()
        } else if self.parse_cnos_keyword(CnosKeyWord::TENANT) {
            self.parse_alter_tenant()
        } else if self.parser.parse_keyword(Keyword::USER) {
            self.parse_alter_user()
        } else {
            self.expected("TABLE/DATABASE/TENANT/USER", self.parser.peek_token())
        }
    }

    fn parse_alter_table(&mut self) -> Result<ExtStatement> {
        let table_name = self.parser.parse_object_name()?;

        if self.parser.parse_keyword(Keyword::ADD) {
            self.parse_alter_table_add_column(table_name)
        } else if self.parser.parse_keyword(Keyword::ALTER) {
            self.parse_alter_table_alter_column(table_name)
        } else if self.parser.parse_keyword(Keyword::DROP) {
            self.parse_alter_table_drop_column(table_name)
        } else {
            self.expected("ADD or ALTER or DROP", self.parser.peek_token())
        }
    }

    fn parse_alter_table_add_column(&mut self, table_name: ObjectName) -> Result<ExtStatement> {
        if self.parse_cnos_keyword(CnosKeyWord::FIELD) {
            let field_name = self.parser.parse_identifier()?;
            let data_type = self.parse_column_type()?;
            let encoding = if self.peek_cnos_keyword().eq(&Ok(CnosKeyWord::CODEC)) {
                Some(self.parse_codec_type()?)
            } else {
                None
            };
            let column = ColumnOption::new_field(field_name, data_type, encoding);
            Ok(ExtStatement::AlterTable(AlterTable {
                table_name,
                alter_action: AlterTableAction::AddColumn { column },
            }))
        } else if self.parse_cnos_keyword(CnosKeyWord::TAG) {
            let tag_name = self.parser.parse_identifier()?;
            let column = ColumnOption::new_tag(tag_name);
            Ok(ExtStatement::AlterTable(AlterTable {
                table_name,
                alter_action: AlterTableAction::AddColumn { column },
            }))
        } else {
            self.expected("FIELD or TAG", self.parser.peek_token())
        }
    }

    fn parse_alter_table_drop_column(&mut self, table_name: ObjectName) -> Result<ExtStatement> {
        let column_name = self.parser.parse_identifier()?;
        Ok(ExtStatement::AlterTable(AlterTable {
            table_name,
            alter_action: AlterTableAction::DropColumn { column_name },
        }))
    }

    fn parse_alter_table_alter_column(&mut self, table_name: ObjectName) -> Result<ExtStatement> {
        let column_name = self.parser.parse_identifier()?;
        self.parser.expect_keyword(Keyword::SET)?;
        let encoding = self.parse_codec_type()?;
        Ok(ExtStatement::AlterTable(AlterTable {
            table_name,
            alter_action: AlterTableAction::AlterColumnEncoding {
                column_name,
                encoding,
            },
        }))
    }

    fn parse_alter_database(&mut self) -> Result<ExtStatement> {
        let database_name = self.parser.parse_identifier()?;
        self.parser.expect_keyword(Keyword::SET)?;
        let mut options = DatabaseOptions::default();
        if !self.parse_database_option(&mut options)? {
            return parser_err!(format!(
                "expected database option, but found {}",
                self.parser.peek_token()
            ));
        }
        Ok(ExtStatement::AlterDatabase(AlterDatabase {
            name: database_name,
            options,
        }))
    }

    fn parse_alter_tenant(&mut self) -> Result<ExtStatement> {
        let name = self.parser.parse_identifier()?;

        let operation = if self.parser.parse_keyword(Keyword::ADD) {
            self.parser.expect_keyword(Keyword::USER)?;
            let user_name = self.parser.parse_identifier()?;
            self.parser.expect_keyword(Keyword::AS)?;
            let role_name = self.parser.parse_identifier()?;
            AlterTenantOperation::AddUser(user_name, role_name)
        } else if self.parse_cnos_keyword(CnosKeyWord::REMOVE) {
            self.parser.expect_keyword(Keyword::USER)?;
            let user_name = self.parser.parse_identifier()?;
            AlterTenantOperation::RemoveUser(user_name)
        } else if self.parser.parse_keyword(Keyword::SET) {
            if self.parser.parse_keyword(Keyword::USER) {
                let user_name = self.parser.parse_identifier()?;
                self.parser.expect_keyword(Keyword::AS)?;
                let role_name = self.parser.parse_identifier()?;
                AlterTenantOperation::SetUser(user_name, role_name)
            } else {
                let sql_option = self.parser.parse_sql_option()?;
                AlterTenantOperation::Set(sql_option)
            }
        } else if self.parse_cnos_keyword(CnosKeyWord::UNSET) {
            let ident = self.parser.parse_identifier()?;
            AlterTenantOperation::UnSet(ident)
        } else {
            self.expected("ADD, REMOVE, SET, UNSET", self.parser.peek_token())?
        };

        Ok(ExtStatement::AlterTenant(AlterTenant { name, operation }))
    }

    fn parse_alter_user(&mut self) -> Result<ExtStatement> {
        let name = self.parser.parse_identifier()?;

        let operation = if self.parser.parse_keyword(Keyword::RENAME) {
            self.parser.expect_keyword(Keyword::TO)?;
            let new_name = self.parser.parse_identifier()?;
            AlterUserOperation::RenameTo(new_name)
        } else if self.parser.parse_keyword(Keyword::SET) {
            let sql_option = self.parser.parse_sql_option()?;
            AlterUserOperation::Set(sql_option)
        } else {
            self.expected("RENAME,SET", self.parser.peek_token())?
        };

        Ok(ExtStatement::AlterUser(AlterUser { name, operation }))
    }

    /// Parses the set of
    fn parse_file_compression_type(&mut self) -> Result<CompressionTypeVariant, ParserError> {
        let token = self.parser.next_token();
        match &token.token {
            Token::Word(w) => CompressionTypeVariant::from_str(&w.value),
            _ => self.expected("one of GZIP, BZIP2, XZ", token),
        }
    }

    /// Parse the ordering clause of a `CREATE EXTERNAL TABLE` SQL statement
    pub fn parse_order_by_exprs(&mut self) -> Result<Vec<OrderByExpr>, ParserError> {
        let mut values = vec![];
        self.parser.expect_token(&Token::LParen)?;
        loop {
            values.push(self.parser.parse_order_by_expr()?);
            if !self.parser.consume_token(&Token::Comma) {
                self.parser.expect_token(&Token::RParen)?;
                return Ok(values);
            }
        }
    }

    fn parse_has_file_compression_type(&mut self) -> bool {
        self.parser
            .parse_keywords(&[Keyword::COMPRESSION, Keyword::TYPE])
    }

    /// This is a copy of the equivalent implementation in Datafusion.
    fn parse_csv_has_header(&mut self) -> bool {
        self.parser
            .parse_keywords(&[Keyword::WITH, Keyword::HEADER, Keyword::ROW])
    }

    /// This is a copy of the equivalent implementation in Datafusion.
    fn parse_has_delimiter(&mut self) -> bool {
        self.parser.parse_keyword(Keyword::DELIMITER)
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

    /// Parses the set of valid formats
    /// This is a copy of the equivalent implementation in Datafusion.
    fn parse_file_format(&mut self) -> Result<String, ParserError> {
        let TokenWithLocation { token, location: _ } = self.parser.next_token();
        match token {
            Token::Word(w) => parse_file_type(&w.value),
            unexpected => self.expected("one of PARQUET, NDJSON, or CSV", unexpected),
        }
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

        let file_compression_type = if self.parse_has_file_compression_type() {
            self.parse_file_compression_type()?
        } else {
            CompressionTypeVariant::UNCOMPRESSED
        };

        let order_exprs = if self.parser.parse_keywords(&[Keyword::WITH, Keyword::ORDER]) {
            self.parse_order_by_exprs()?
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
            table_partition_cols: Default::default(),
            if_not_exists,
            file_compression_type,
            options: Default::default(),
            order_exprs,
        };
        Ok(ExtStatement::CreateExternalTable(create))
    }

    fn parse_create_table(&mut self) -> Result<ExtStatement> {
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let table_name = self.parser.parse_object_name()?;
        let columns = self.parse_cnos_columns()?;

        let create = CreateTable {
            name: table_name,
            if_not_exists,
            columns,
        };
        Ok(ExtStatement::CreateTable(create))
    }

    fn parse_database_options(&mut self) -> Result<DatabaseOptions> {
        if self.parser.parse_keyword(Keyword::WITH) {
            let mut options = DatabaseOptions::default();
            loop {
                if !self.parse_database_option(&mut options)? {
                    return Ok(options);
                }
            }
        }
        Ok(DatabaseOptions::default())
    }

    fn parse_database_option(&mut self, options: &mut DatabaseOptions) -> Result<bool> {
        if self.parse_cnos_keyword(CnosKeyWord::TTL) {
            options.ttl = Some(self.parse_string_value()?);
        } else if self.parse_cnos_keyword(CnosKeyWord::SHARD) {
            options.shard_num = Some(self.parse_number::<u64>()?);
        } else if self.parse_cnos_keyword(CnosKeyWord::VNODE_DURATION) {
            options.vnode_duration = Some(self.parse_string_value()?);
        } else if self.parse_cnos_keyword(CnosKeyWord::REPLICA) {
            options.replica = Some(self.parse_number::<u64>()?);
        } else if self.parse_cnos_keyword(CnosKeyWord::PRECISION) {
            options.precision = Some(self.parse_string_value()?);
        } else {
            return Ok(false);
        }
        Ok(true)
    }

    fn parse_number<T: FromStr>(&mut self) -> Result<T> {
        let num = self.parser.parse_number_value()?.to_string();
        match num.parse::<T>() {
            Ok(v) => Ok(v),
            Err(_) => {
                parser_err!(format!(
                    "this option should be a unsigned number, but get {}",
                    num
                ))
            }
        }
    }

    fn parse_string_value(&mut self) -> Result<String> {
        let value = self.parser.parse_value()?;
        parse_string_value(value)
    }

    fn parse_create_database(&mut self) -> Result<ExtStatement> {
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let database_name = self.parser.parse_identifier()?;
        let options = self.parse_database_options()?;
        Ok(ExtStatement::CreateDatabase(CreateDatabase {
            name: database_name,
            if_not_exists,
            options,
        }))
    }

    fn parse_grant_permission(&mut self) -> Result<Action, ParserError> {
        if self.parse_cnos_keyword(CnosKeyWord::READ) {
            Ok(Action::Read)
        } else if self.parse_cnos_keyword(CnosKeyWord::WRITE) {
            Ok(Action::Write)
        } else if self.parser.parse_keyword(Keyword::ALL) {
            Ok(Action::All)
        } else {
            self.expected(
                "a privilege keyword [Read, Write, All]",
                self.parser.peek_token(),
            )?
        }
    }

    fn parse_privilege(&mut self) -> Result<Privilege, ParserError> {
        let action = self.parse_grant_permission()?;
        self.parser.expect_keyword(Keyword::ON)?;
        self.parser.expect_keyword(Keyword::DATABASE)?;
        let database = self.parser.parse_identifier()?;
        Ok(Privilege { action, database })
    }

    fn parse_grant(&mut self) -> Result<ExtStatement> {
        // grant read on database "db1" to [role] rrr;
        // grant write on database "db2" to rrr;
        // grant all on database "db3" to rrr;
        let privileges = self.parse_comma_separated(ExtParser::parse_privilege)?;

        self.parser.expect_keyword(Keyword::TO)?;
        let _ = self.parser.parse_keyword(Keyword::ROLE);

        let role_name = self.parser.parse_identifier()?;

        Ok(ExtStatement::GrantRevoke(GrantRevoke {
            is_grant: true,
            privileges,
            role_name,
        }))
    }

    fn parse_revoke(&mut self) -> Result<ExtStatement> {
        // revoke read on database "db1" from [role] rrr;
        // revoke write on database "db2" from rrr;
        // revoke all on database "db3" from rrr;
        let privileges = self.parse_comma_separated(ExtParser::parse_privilege)?;

        self.parser.expect_keyword(Keyword::FROM)?;
        let _ = self.parser.parse_keyword(Keyword::ROLE);

        let role_name = self.parser.parse_identifier()?;

        Ok(ExtStatement::GrantRevoke(GrantRevoke {
            is_grant: false,
            privileges,
            role_name,
        }))
    }

    fn parse_create_user(&mut self) -> Result<ExtStatement> {
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);

        let name = self.parser.parse_identifier()?;

        let with_options = if self.parser.parse_keyword(Keyword::WITH) {
            self.parser
                .parse_comma_separated(Parser::parse_sql_option)?
        } else {
            vec![]
        };

        Ok(ExtStatement::CreateUser(CreateUser {
            if_not_exists,
            name,
            with_options,
        }))
    }

    fn parse_create_role(&mut self) -> Result<ExtStatement> {
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);

        let name = self.parser.parse_identifier()?;

        let inherit = if self.parse_cnos_keyword(CnosKeyWord::INHERIT) {
            self.parser.parse_identifier().ok()
        } else {
            None
        };

        Ok(ExtStatement::CreateRole(CreateRole {
            if_not_exists,
            name,
            inherit,
        }))
    }

    fn parse_create_tenant(&mut self) -> Result<ExtStatement> {
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);

        let name = self.parser.parse_identifier()?;

        let with_options = if self.parser.parse_keyword(Keyword::WITH) {
            self.parser
                .parse_comma_separated(Parser::parse_sql_option)?
        } else {
            vec![]
        };

        Ok(ExtStatement::CreateTenant(CreateTenant {
            if_not_exists,
            name,
            with_options,
        }))
    }

    fn parse_create_stream(&mut self) -> Result<ExtStatement> {
        if self.parser.parse_keyword(Keyword::TABLE) {
            self.parse_create_stream_table()
        } else {
            self.parse_create_stream_query()
        }
    }

    /// e.g.
    /// CREATE STREAM TABLE TskvTable (
    ///   time TIMESTAMP
    ///   name STRING,
    ///   driver STRING,
    ///   load_capacity DOUBLE,
    /// ) WITH (
    ///   'db' = 'public',
    ///   'table' = 'test_stream',
    /// ) engine = tskv;
    fn parse_create_stream_table(&mut self) -> Result<ExtStatement> {
        Ok(ExtStatement::CreateStreamTable(
            self.parser.parse_create_table(false, false, None, false)?,
        ))
    }

    /// 废弃
    fn parse_create_stream_query(&mut self) -> Result<ExtStatement> {
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);

        let name = self.parser.parse_identifier()?;

        let mut trigger = None;
        let mut watermark = None;
        let mut output_mode = None;

        loop {
            if self.parse_cnos_keyword(CnosKeyWord::TRIGGER) {
                self.parser.expect_token(&Token::Eq)?;

                trigger = Some(if self.parse_cnos_keyword(CnosKeyWord::ONCE) {
                    Trigger::Once
                } else {
                    let interval = self.parse_string_value()?;
                    Trigger::Interval(interval)
                });
            } else if self.parse_cnos_keyword(CnosKeyWord::WATERMARK) {
                self.parser.expect_token(&Token::Eq)?;

                watermark = Some(self.parse_string_value()?);
            } else if self.parse_cnos_keyword(CnosKeyWord::OUTPUT_MODE) {
                self.parser.expect_token(&Token::Eq)?;

                output_mode = Some(if self.parse_cnos_keyword(CnosKeyWord::COMPLETE) {
                    OutputMode::Complete
                } else if self.parse_cnos_keyword(CnosKeyWord::APPEND) {
                    OutputMode::Append
                } else if self.parser.parse_keyword(Keyword::UPDATE) {
                    OutputMode::Update
                } else {
                    return self.expected(
                        "one of COMPLETE, APPEND, or UPDATE",
                        self.parser.peek_token(),
                    );
                });
            } else {
                break;
            }
        }

        self.parser.expect_keyword(Keyword::AS)?;
        self.parser.expect_keyword(Keyword::INSERT)?;
        let statement = Box::new(self.parser.parse_insert()?);

        Ok(ExtStatement::CreateStream(CreateStream {
            if_not_exists,
            name,
            trigger,
            watermark,
            output_mode,
            statement,
        }))
    }

    /// Parse a SQL CREATE statement
    fn parse_create(&mut self) -> Result<ExtStatement> {
        // Currently only supports the creation of external tables
        if self.parser.parse_keyword(Keyword::EXTERNAL) {
            self.parse_create_external_table()
        } else if self.parser.parse_keyword(Keyword::TABLE) {
            self.parse_create_table()
        } else if self.parser.parse_keyword(Keyword::DATABASE) {
            self.parse_create_database()
        } else if self.parse_cnos_keyword(CnosKeyWord::TENANT) {
            self.parse_create_tenant()
        } else if self.parser.parse_keyword(Keyword::USER) {
            self.parse_create_user()
        } else if self.parser.parse_keyword(Keyword::ROLE) {
            self.parse_create_role()
        } else if self.parse_cnos_keyword(CnosKeyWord::STREAM) {
            self.parse_create_stream()
        } else {
            self.expected("an object type after CREATE", self.parser.peek_token())
        }
    }

    /// Parse a copy statement
    fn parse_copy(&mut self) -> Result<ExtStatement> {
        if self.parse_cnos_keyword(CnosKeyWord::VNODE) {
            let vnode_id = self.parse_number::<VnodeId>()?;
            self.parser.expect_keyword(Keyword::TO)?;
            if self.parse_cnos_keyword(CnosKeyWord::NODE).not() {
                return parser_err!("expected NODE, after TO");
            }
            let node_id = self.parse_number::<NodeId>()?;
            Ok(ExtStatement::CopyVnode(CopyVnode { vnode_id, node_id }))
        } else if self.parser.parse_keyword(Keyword::INTO) {
            self.parse_copy_into()
        } else {
            parser_err!("expected VNODE, after COPY")
        }
    }

    fn parse_copy_into_table(&mut self, table_name: ObjectName) -> Result<CopyTarget> {
        // COPY INTO <table> [(columns,...)] FROM <location>
        let columns = self
            .parser
            .parse_parenthesized_column_list(IsOptional::Optional, false)?;

        self.parser.expect_keyword(Keyword::FROM)?;

        // external location's path
        let path = self.parser.parse_literal_string()?;

        let mut connection_options = vec![];
        // let mut files = vec![];
        // let mut pattern = None;
        loop {
            if self.parser.parse_keyword(Keyword::CONNECTION) {
                // external location's connection options
                //   CONNECTION = (
                //         ENDPOINT_URL = 'https://<endpoint>',
                //         CREDENTIAL = '<credential>'
                //   )
                connection_options = self.parse_options()?;
            // } else if self.parse_cnos_keyword(CnosKeyWord::FILES) {
            //     //   FILES = (
            //     //         '<file_name>' [ , '<file_name>' ] [ , ... ]
            //     //   )
            //     files = self.parse_files_of_copy_into()?;
            // } else if self.parse_cnos_keyword(CnosKeyWord::PATTERN) {
            //     //   PATTERN = '<regex_pattern>'
            //     self.parser.expect_token(&Token::Eq)?;
            //     pattern = Some(self.parser.parse_literal_string()?);
            } else {
                break;
            };
        }

        Ok(CopyTarget::IntoTable(CopyIntoTable {
            location: UriLocation {
                path,
                connection_options,
                // files,
                // pattern,
            },
            table_name,
            columns,
        }))
    }

    fn parse_copy_into_location(&mut self, path: String) -> Result<CopyTarget> {
        self.parser.expect_keyword(Keyword::FROM)?;

        let from = if self.parser.consume_token(&Token::LParen) {
            let subquery = Box::new(self.parser.parse_query()?);
            self.parser.expect_token(&Token::RParen)?;

            TableFactor::Derived {
                lateral: false,
                subquery,
                alias: None,
            }
        } else {
            let name = self.parser.parse_object_name()?;
            TableFactor::Table {
                name,
                alias: None,
                args: None,
                with_hints: vec![],
            }
        };

        // external location's connection options
        let connection_options = if self.parser.parse_keyword(Keyword::CONNECTION) {
            self.parse_options()?
        } else {
            Default::default()
        };

        Ok(CopyTarget::IntoLocation(CopyIntoLocation {
            from,
            location: UriLocation {
                path,
                connection_options,
            },
        }))
    }

    /// Parse a copy into statement
    fn parse_copy_into(&mut self) -> Result<ExtStatement> {
        let copy_target = match self.parser.peek_token().token {
            Token::SingleQuotedString(path) => {
                // COPY INTO <location> FROM <table>
                let _ = self.parser.next_token();
                self.parse_copy_into_location(path)?
            }
            _ => {
                // COPY INTO <table> FROM <location>
                let table_name = self.parser.parse_object_name()?;
                self.parse_copy_into_table(table_name)?
            }
        };

        let mut copy_options = vec![];
        let mut file_format_options = vec![];
        loop {
            if self.parse_cnos_keyword(CnosKeyWord::FILE_FORMAT) {
                // parse file format & format options
                // FILE_FORMAT = ({
                //     TYPE = { CSV | JSON | AVRO | ORC | PARQUET | XML }
                //     | DELIMITER = '<character>'
                //     | WITH_HEADER = { true | false }
                //     | ...
                // } [, ...])
                file_format_options = self.parse_options()?;
            } else if self.parse_cnos_keyword(CnosKeyWord::COPY_OPTIONS) {
                // COPY_OPTIONS = ({
                //     ON_ERROR = { continue | abort }
                //     | , ...
                // } [, ...])
                copy_options = self.parse_options()?;
            } else {
                break;
            }
        }

        Ok(ExtStatement::Copy(ast::Copy {
            copy_target,
            file_format_options,
            copy_options,
        }))
    }

    fn _parse_files_of_copy_into(&mut self) -> Result<Vec<String>> {
        self.parser.expect_token(&Token::Eq)?;
        self.parser.expect_token(&Token::LParen)?;
        let files = self
            .parser
            .parse_comma_separated(Parser::parse_literal_string)?;
        self.parser.expect_token(&Token::RParen)?;

        Ok(files)
    }

    /// = (
    ///     key1 = value1 [, keyN = valueN] [, ......]
    /// )
    fn parse_options(&mut self) -> Result<Vec<SqlOption>> {
        self.parser.expect_token(&Token::Eq)?;
        self.parser.expect_token(&Token::LParen)?;

        let options = self
            .parser
            .parse_comma_separated(Parser::parse_sql_option)?;

        self.parser.expect_token(&Token::RParen)?;

        Ok(options)
    }

    /// Parse a SQL DROP statement
    fn parse_drop(&mut self) -> Result<ExtStatement> {
        let ast = if self.parser.parse_keyword(Keyword::TABLE) {
            let if_exist = self.parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
            let object_name = self.parser.parse_object_name()?;
            ExtStatement::DropDatabaseObject(DropDatabaseObject {
                object_name,
                if_exist,
                obj_type: DatabaseObjectType::Table,
            })
        } else if self.parser.parse_keyword(Keyword::DATABASE) {
            let if_exist = self.parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
            let object_name = self.parser.parse_identifier()?;
            ExtStatement::DropTenantObject(DropTenantObject {
                object_name,
                if_exist,
                obj_type: TenantObjectType::Database,
            })
        } else if self.parse_cnos_keyword(CnosKeyWord::TENANT) {
            let if_exist = self.parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
            let object_name = self.parser.parse_identifier()?;
            ExtStatement::DropGlobalObject(DropGlobalObject {
                object_name,
                if_exist,
                obj_type: GlobalObjectType::Tenant,
            })
        } else if self.parser.parse_keyword(Keyword::USER) {
            let if_exist = self.parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
            let object_name = self.parser.parse_identifier()?;
            ExtStatement::DropGlobalObject(DropGlobalObject {
                object_name,
                if_exist,
                obj_type: GlobalObjectType::User,
            })
        } else if self.parser.parse_keyword(Keyword::ROLE) {
            let if_exist = self.parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
            let object_name = self.parser.parse_identifier()?;
            ExtStatement::DropTenantObject(DropTenantObject {
                object_name,
                if_exist,
                obj_type: TenantObjectType::Role,
            })
        } else if self.parse_cnos_keyword(CnosKeyWord::VNODE) {
            let vnode_id = self.parse_number::<VnodeId>()?;
            ExtStatement::DropVnode(DropVnode { vnode_id })
        } else if self.parse_cnos_keyword(CnosKeyWord::STREAM) {
            let if_exist = self.parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
            let name = self.parser.parse_identifier()?;
            ExtStatement::DropStream(ast::DropStream { if_exist, name })
        } else {
            return self.expected(
                "TABLE,DATABASE,TENANT,USER,ROLE,VNODE,STREAM after DROP",
                self.parser.peek_token(),
            );
        };

        Ok(ast)
    }

    fn parse_move(&mut self) -> Result<ExtStatement> {
        if self.parse_cnos_keyword(CnosKeyWord::VNODE) {
            let vnode_id = self.parse_number::<VnodeId>()?;
            self.parser.expect_keyword(Keyword::TO)?;
            if self.parse_cnos_keyword(CnosKeyWord::NODE).not() {
                return parser_err!("expected NODE, after TO");
            }
            let node_id = self.parse_number::<NodeId>()?;
            Ok(ExtStatement::MoveVnode(MoveVnode { vnode_id, node_id }))
        } else {
            parser_err!("expected VNODE, after MOVE")
        }
    }

    fn parse_compact(&mut self) -> Result<ExtStatement> {
        if self.parse_cnos_keyword(CnosKeyWord::VNODE) {
            let mut vnode_ids = Vec::new();
            loop {
                vnode_ids.push(self.parse_number::<VnodeId>()?);
                if self.parser.expect_token(&Token::SemiColon).is_ok() {
                    break;
                }
            }
            Ok(ExtStatement::CompactVnode(CompactVnode { vnode_ids }))
        } else {
            parser_err!("Expected VNODE, after COMPACT")
        }
    }

    fn parse_checksum(&mut self) -> Result<ExtStatement> {
        if self.parser.parse_keyword(Keyword::GROUP) {
            let replication_set_id = self.parse_number::<ReplicationSetId>()?;
            Ok(ExtStatement::ChecksumGroup(ChecksumGroup {
                replication_set_id,
            }))
        } else {
            parser_err!("Expected GROUP. after CHECKSUM")
        }
    }

    fn consume_token(&mut self, expected: &Token) -> bool {
        if self.parser.peek_token().token == *expected {
            self.parser.next_token();
            true
        } else {
            false
        }
    }

    /// Parse a comma-separated list of 1+ items accepted by `F`
    pub fn parse_comma_separated<T, F>(&mut self, mut f: F) -> Result<Vec<T>, ParserError>
    where
        F: FnMut(&mut ExtParser<'a>) -> Result<T, ParserError>,
    {
        let mut values = vec![];
        loop {
            values.push(f(self)?);
            if !self.consume_token(&Token::Comma) {
                break;
            }
        }
        Ok(values)
    }

    fn peek_cnos_keyword(&self) -> Result<CnosKeyWord> {
        self.parser.peek_token().to_string().as_str().parse()
    }

    fn parse_cnos_keyword(&mut self, key_word: CnosKeyWord) -> bool {
        if self.peek_cnos_keyword().eq(&Ok(key_word)) {
            self.parser.next_token();
            true
        } else {
            false
        }
    }

    fn parse_cnos_columns(&mut self) -> Result<Vec<ColumnOption>> {
        // -- Parse as is without adding any semantics
        let mut all_columns: Vec<ColumnOption> = vec![];
        let mut field_columns: Vec<ColumnOption> = vec![];

        if !self.consume_token(&Token::LParen) || self.consume_token(&Token::RParen) {
            return parser_err!("Expected field columns when create table");
        }
        loop {
            let name = self.parser.parse_identifier()?;
            let column_type = self.parse_column_type()?;

            let encoding = if self.parser.peek_token().eq(&Token::Comma) {
                None
            } else {
                Some(self.parse_codec_type()?)
            };

            field_columns.push(ColumnOption {
                name,
                is_tag: false,
                data_type: column_type,
                encoding,
            });
            let comma = self.consume_token(&Token::Comma);
            if self.consume_token(&Token::RParen) {
                // allow a trailing comma, even though it's not in standard
                // return parser_err!(format!("table should have TAGS"));
                break;
            } else if !comma {
                return self.expected(
                    "',' or ')' after column definition",
                    self.parser.peek_token(),
                );
            }
            if self.parse_cnos_keyword(CnosKeyWord::TAGS) {
                self.parse_tag_columns(&mut all_columns)?;
                self.parser.expect_token(&Token::RParen)?;
                break;
            }
        }
        // tag1, tag2, ..., field1, field2, ...
        all_columns.append(&mut field_columns);

        Ok(all_columns)
    }

    fn parse_tag_columns(&mut self, columns: &mut Vec<ColumnOption>) -> Result<()> {
        if !self.consume_token(&Token::LParen) || self.consume_token(&Token::RParen) {
            return Ok(());
        }
        loop {
            let name = self.parser.parse_identifier()?;
            columns.push(ColumnOption {
                name,
                is_tag: true,
                data_type: DataType::String,
                encoding: None,
            });
            let is_comma = self.consume_token(&Token::Comma);
            if self.consume_token(&Token::RParen) {
                break;
            }
            if !is_comma {
                return parser_err!(format!(", is expected after column"));
            }
        }
        Ok(())
    }

    fn parse_column_type(&mut self) -> Result<DataType> {
        let TokenWithLocation { token, location: _ } = self.parser.next_token();
        match token {
            Token::Word(w) => match w.keyword {
                Keyword::TIMESTAMP => parser_err!(format!("already have timestamp column")),
                Keyword::BIGINT => {
                    if self.parser.parse_keyword(Keyword::UNSIGNED) {
                        Ok(DataType::UnsignedBigInt(None))
                    } else {
                        Ok(DataType::BigInt(None))
                    }
                }
                Keyword::DOUBLE => Ok(DataType::Double),
                Keyword::STRING => Ok(DataType::String),
                Keyword::BOOLEAN => Ok(DataType::Boolean),
                _ => parser_err!(format!("{} is not a supported type", w)),
            },
            unexpected => parser_err!(format!("{} is not a type", unexpected)),
        }
    }

    fn parse_codec_encoding(&mut self) -> Result<Encoding, String> {
        self.parser
            .peek_token()
            .to_string()
            .parse()
            .map(|encoding| {
                self.parser.next_token();
                encoding
            })
    }

    fn parse_codec_type(&mut self) -> Result<Encoding> {
        if !self.parse_cnos_keyword(CnosKeyWord::CODEC) {
            return self.expected("CODEC or ','", self.parser.peek_token());
        }

        self.parser.expect_token(&Token::LParen)?;
        if self.parser.peek_token().eq(&Token::RParen) {
            return parser_err!(format!("expect codec encoding type in ()"));
        }
        let encoding = match self.parse_codec_encoding() {
            Ok(encoding) => encoding,
            Err(str) => return parser_err!(format!("{} is not valid encoding", str)),
        };
        self.parser.expect_token(&Token::RParen)?;
        Ok(encoding)
    }
}

/// This is a copy of the equivalent implementation in Datafusion.
fn parse_file_type(s: &str) -> Result<String, ParserError> {
    Ok(s.to_uppercase())
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;

    use datafusion::sql::sqlparser::ast::{
        ColumnDef, Ident, ObjectName, SetExpr, Statement, TableFactor, TimezoneInfo, Value,
    };
    use spi::query::ast::{AlterTable, DropDatabaseObject, ExtStatement, ShowStreams, UriLocation};
    use spi::query::logical_planner::{DatabaseObjectType, TenantObjectType};

    use super::*;

    fn parse_sql(sql: &str) -> ExtStatement {
        ExtParser::parse_sql(sql)
            .unwrap()
            .into_iter()
            .last()
            .unwrap()
    }

    fn make_column_def(name: impl Into<String>, data_type: DataType) -> ColumnDef {
        ColumnDef {
            name: Ident {
                value: name.into(),
                quote_style: None,
            },
            data_type,
            collation: None,
            options: vec![],
        }
    }

    #[test]
    fn test_drop() {
        let sql = "drop table test_tb";
        let statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            ExtStatement::DropDatabaseObject(DropDatabaseObject {
                object_name,
                if_exist,
                obj_type,
            }) => {
                assert_eq!(object_name.to_string(), "test_tb".to_string());
                assert_eq!(if_exist.to_string(), "false".to_string());
                assert_eq!(obj_type, &DatabaseObjectType::Table);
            }
            _ => panic!("failed"),
        }

        let sql = "drop table if exists test_tb";
        let statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            ExtStatement::DropDatabaseObject(DropDatabaseObject {
                object_name,
                if_exist,
                obj_type,
            }) => {
                assert_eq!(object_name.to_string(), "test_tb".to_string());
                assert_eq!(if_exist.to_string(), "true".to_string());
                assert_eq!(obj_type, &DatabaseObjectType::Table);
            }
            _ => panic!("failed"),
        }

        let sql = "drop database if exists test_db";
        let statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            ExtStatement::DropTenantObject(DropTenantObject {
                object_name,
                if_exist,
                obj_type,
            }) => {
                assert_eq!(object_name.to_string(), "test_db".to_string());
                assert_eq!(if_exist.to_string(), "true".to_string());
                assert_eq!(obj_type, &TenantObjectType::Database);
            }
            _ => panic!("failed"),
        }

        let sql = "drop database test_db";
        let statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            ExtStatement::DropTenantObject(DropTenantObject {
                object_name,
                if_exist,
                obj_type,
            }) => {
                assert_eq!(object_name.to_string(), "test_db".to_string());
                assert_eq!(if_exist.to_string(), "false".to_string());
                assert_eq!(obj_type, &TenantObjectType::Database);
            }
            _ => panic!("failed"),
        }
    }

    #[test]
    fn test_create_table_statement() {
        let sql = "CREATE TABLE IF NOT EXISTS test\
            (column1 BIGINT CODEC(DELTA),\
            column2 STRING CODEC(GZIP),\
            column3 BIGINT UNSIGNED CODEC(NULL),\
            column4 BOOLEAN,\
            column5 DOUBLE CODEC(GORILLA),\
            TAGS(column6, column7))";
        let statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            ExtStatement::CreateTable(CreateTable {
                name,
                if_not_exists,
                columns,
            }) => {
                assert_eq!(name.to_string(), "test".to_string());
                assert_eq!(if_not_exists.to_string(), "true".to_string());
                assert_eq!(columns.len(), 7);
                assert_eq!(
                    *columns,
                    vec![
                        ColumnOption {
                            name: Ident::from("column6"),
                            is_tag: true,
                            data_type: DataType::String,
                            encoding: None
                        },
                        ColumnOption {
                            name: Ident::from("column7"),
                            is_tag: true,
                            data_type: DataType::String,
                            encoding: None
                        },
                        ColumnOption {
                            name: Ident::from("column1"),
                            is_tag: false,
                            data_type: DataType::BigInt(None),
                            encoding: Some(Encoding::Delta)
                        },
                        ColumnOption {
                            name: Ident::from("column2"),
                            is_tag: false,
                            data_type: DataType::String,
                            encoding: Some(Encoding::Gzip)
                        },
                        ColumnOption {
                            name: Ident::from("column3"),
                            is_tag: false,
                            data_type: DataType::UnsignedBigInt(None),
                            encoding: Some(Encoding::Null)
                        },
                        ColumnOption {
                            name: Ident::from("column4"),
                            is_tag: false,
                            data_type: DataType::Boolean,
                            encoding: None
                        },
                        ColumnOption {
                            name: Ident::from("column5"),
                            is_tag: false,
                            data_type: DataType::Double,
                            encoding: Some(Encoding::Gorilla)
                        }
                    ]
                );
            }
            _ => panic!("failed"),
        }
    }

    #[test]
    fn test_insert_values() {
        let sql = "insert public.test(TIME, ta, tb, fa, fb)
                         values
                             (7, '7a', '7b', 7, 7);";

        let statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match statements[0] {
            ExtStatement::SqlStatement(ref stmt) => match stmt.deref() {
                Statement::Insert {
                    table_name: ref sql_object_name,
                    columns: ref sql_column_names,
                    source: _,
                    ..
                } => {
                    let expect_table_name = &ObjectName(vec![
                        Ident {
                            value: "public".to_string(),
                            quote_style: None,
                        },
                        Ident {
                            value: "test".to_string(),
                            quote_style: None,
                        },
                    ]);

                    let expect_column_names = &vec![
                        Ident {
                            value: "TIME".to_string(),
                            quote_style: None,
                        },
                        Ident {
                            value: "ta".to_string(),
                            quote_style: None,
                        },
                        Ident {
                            value: "tb".to_string(),
                            quote_style: None,
                        },
                        Ident {
                            value: "fa".to_string(),
                            quote_style: None,
                        },
                        Ident {
                            value: "fb".to_string(),
                            quote_style: None,
                        },
                    ];

                    assert_eq!(sql_object_name, expect_table_name);
                    assert_eq!(sql_column_names, expect_column_names);
                }
                _ => panic!("failed"),
            },
            _ => panic!("failed"),
        }
    }

    #[test]
    fn test_insert_select() {
        let sql = "insert public.test_insert_subquery(TIME, ta, tb, fa, fb)
                        select column1, column2, column3, column4, column5
                        from
                        (values
                            (7, '7a', '7b', 7, 7));";

        let statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match statements[0] {
            ExtStatement::SqlStatement(ref stmt) => match stmt.deref() {
                Statement::Insert {
                    table_name: ref sql_object_name,
                    columns: ref sql_column_names,
                    source,
                    ..
                } => {
                    let expect_table_name = &ObjectName(vec![
                        Ident {
                            value: "public".to_string(),
                            quote_style: None,
                        },
                        Ident {
                            value: "test_insert_subquery".to_string(),
                            quote_style: None,
                        },
                    ]);

                    let expect_column_names = &vec![
                        Ident {
                            value: "TIME".to_string(),
                            quote_style: None,
                        },
                        Ident {
                            value: "ta".to_string(),
                            quote_style: None,
                        },
                        Ident {
                            value: "tb".to_string(),
                            quote_style: None,
                        },
                        Ident {
                            value: "fa".to_string(),
                            quote_style: None,
                        },
                        Ident {
                            value: "fb".to_string(),
                            quote_style: None,
                        },
                    ];

                    assert_eq!(sql_object_name, expect_table_name);
                    assert_eq!(sql_column_names, expect_column_names);

                    match source.deref().body.deref() {
                        SetExpr::Select(_) => {}
                        _ => panic!("failed"),
                    }
                }
                _ => panic!("failed"),
            },
            _ => panic!("failed"),
        }
    }

    #[test]
    fn test_create_database() {
        let sql = "CREATE DATABASE test WITH TTl '10d' SHARD 5 VNOdE_DURATiON '3d' REPLICA 10 pRECISIOn 'us';";
        let statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match statements[0] {
            ExtStatement::CreateDatabase(ref stmt) => {
                let ans = format!("{:?}", stmt);
                println!("{ans}");
                let expectd = "CreateDatabase { name: Ident { value: \"test\", quote_style: None }, if_not_exists: false, options: DatabaseOptions { ttl: Some(\"10d\"), shard_num: Some(5), vnode_duration: Some(\"3d\"), replica: Some(10), precision: Some(\"us\") } }";
                assert_eq!(ans, expectd);
            }
            _ => panic!("impossible"),
        }
    }
    #[test]
    #[should_panic]
    fn test_create_table_without_fields() {
        let sql = "CREATE TABLE test0(TAGS(column6, column7));";
        ExtParser::parse_sql(sql).unwrap();
    }

    #[test]
    fn test_alter_table() {
        let sql = r#"
            ALTER TABLE m ADD TAG t;
            ALTER TABLE m ADD FIELD f BIGINT CODEC(DEFAULT);
            ALTER TABLE m DROP t;
            ALTER TABLE m DROP f;
            ALTER TABLE m ALTER f SET CODEC(DEFAULT);
            ALTER TABLE m ALTER TIME SET CODEC(NULL);
        "#;
        let statement = ExtParser::parse_sql(sql).unwrap();
        let statement: Vec<AlterTable> = statement
            .into_iter()
            .map(|s| match s {
                ExtStatement::AlterTable(s) => s,
                _ => panic!("Expect AlterTable"),
            })
            .collect();
        assert_eq!(
            statement,
            vec![
                AlterTable {
                    table_name: ObjectName(vec![Ident::from("m")]),
                    alter_action: AlterTableAction::AddColumn {
                        column: ColumnOption {
                            name: Ident::from("t"),
                            is_tag: true,
                            data_type: DataType::String,
                            encoding: None
                        }
                    }
                },
                AlterTable {
                    table_name: ObjectName(vec![Ident::from("m")]),
                    alter_action: AlterTableAction::AddColumn {
                        column: ColumnOption {
                            name: Ident::from("f"),
                            is_tag: false,
                            data_type: DataType::BigInt(None),
                            encoding: Some(Encoding::Default)
                        }
                    }
                },
                AlterTable {
                    table_name: ObjectName(vec![Ident::from("m")]),
                    alter_action: AlterTableAction::DropColumn {
                        column_name: Ident::from("t")
                    }
                },
                AlterTable {
                    table_name: ObjectName(vec![Ident::from("m")]),
                    alter_action: AlterTableAction::DropColumn {
                        column_name: Ident::from("f")
                    }
                },
                AlterTable {
                    table_name: ObjectName(vec![Ident::from("m")]),
                    alter_action: AlterTableAction::AlterColumnEncoding {
                        column_name: Ident::from("f"),
                        encoding: Encoding::Default
                    }
                },
                AlterTable {
                    table_name: ObjectName(vec![Ident::from("m")]),
                    alter_action: AlterTableAction::AlterColumnEncoding {
                        column_name: Ident::from("TIME"),
                        encoding: Encoding::Null
                    }
                }
            ]
        );
    }

    #[test]
    fn test_parse_copy_into() {
        let sql = r#"
        copy into mytable from 's3://bucket/path' file_format = (type = 'csv');
        "#;
        let statement = ExtParser::parse_sql(sql)
            .unwrap()
            .into_iter()
            .last()
            .unwrap();

        let copy_target = ast::CopyTarget::IntoTable(CopyIntoTable {
            location: UriLocation {
                path: "s3://bucket/path".to_string(),
                connection_options: vec![],
            },
            table_name: ObjectName(vec!["mytable".into()]),
            columns: vec![],
        });

        let expected = ExtStatement::Copy(ast::Copy {
            copy_target,
            file_format_options: vec![SqlOption {
                name: "type".into(),
                value: Value::SingleQuotedString("csv".to_string()),
            }],
            copy_options: vec![],
        });

        assert_eq!(expected, statement)
    }

    #[test]
    fn test_vnode_sql() {
        let sql1 = "move vnode 1 to node 2;";
        let statement = ExtParser::parse_sql(sql1).unwrap();
        assert_eq!(
            statement[0],
            ExtStatement::MoveVnode(MoveVnode {
                vnode_id: 1,
                node_id: 2,
            })
        );
        let sql2 = "copy vnode 3 to node 4;";
        let statement = ExtParser::parse_sql(sql2).unwrap();
        assert_eq!(
            statement[0],
            ExtStatement::CopyVnode(CopyVnode {
                vnode_id: 3,
                node_id: 4
            })
        );
        let sql3 = "drop vnode 5;";
        let statement = ExtParser::parse_sql(sql3).unwrap();
        assert_eq!(
            statement[0],
            ExtStatement::DropVnode(DropVnode { vnode_id: 5 })
        );
        let sql4 = "compact vnode 6 7 8 9;";
        let statement = ExtParser::parse_sql(sql4).unwrap();
        assert_eq!(
            statement[0],
            ExtStatement::CompactVnode(CompactVnode {
                vnode_ids: vec![6, 7, 8, 9],
            })
        );
        let sql5 = "checksum group 10";
        let statement = ExtParser::parse_sql(sql5).unwrap();
        assert_eq!(
            statement[0],
            ExtStatement::ChecksumGroup(ChecksumGroup {
                replication_set_id: 10
            })
        );
    }

    #[test]
    fn test_parse_copy_into_table_no_error() {
        let sql = r#"
            copy into mytable from 's3://bucket/path' file_format = (type = 'csv');
            copy into mytable from 's3://bucket/path' file_format = (type = 'csv') COPY_OPTIONS = (ON_ERROR = 'abort');
            copy into mytable from 's3://bucket/path' COPY_OPTIONS = (ON_ERROR = 'abort') file_format = (type = 'csv');
            copy into mytable from 's3://bucket/path' CONNECTION = (xx='a', sss='ss') file_format = (type = 'csv');
            copy into mytable from 's3://bucket/path' CONNECTION = (xx='a', sss='ss') file_format = (type = 'csv') copy_options = (on_error = 'abort');
        "#;

        let _ = ExtParser::parse_sql(sql).unwrap();
    }

    #[test]
    fn test_parse_copy_into_location() {
        let sql = r#"
            copy into 's3://bucket/path' from mytable CONNECTION = (xx='a', sss='ss') file_format = (type = 'csv');
        "#;
        let statement = ExtParser::parse_sql(sql)
            .unwrap()
            .into_iter()
            .last()
            .unwrap();

        let copy_target = ast::CopyTarget::IntoLocation(CopyIntoLocation {
            from: TableFactor::Table {
                name: ObjectName(vec!["mytable".into()]),
                alias: None,
                args: None,
                with_hints: vec![],
            },
            location: UriLocation {
                path: "s3://bucket/path".to_string(),
                connection_options: vec![
                    SqlOption {
                        name: "xx".into(),
                        value: Value::SingleQuotedString("a".to_string()),
                    },
                    SqlOption {
                        name: "sss".into(),
                        value: Value::SingleQuotedString("ss".to_string()),
                    },
                ],
            },
        });

        let expected = ExtStatement::Copy(ast::Copy {
            copy_target,
            file_format_options: vec![SqlOption {
                name: "type".into(),
                value: Value::SingleQuotedString("csv".to_string()),
            }],
            copy_options: vec![],
        });

        assert_eq!(expected, statement)
    }

    #[test]
    fn test_parse_copy_into_location_no_error() {
        let sql = r#"
            copy into 's3://bucket/path' from mytable file_format = (type = 'csv');
            copy into 's3://bucket/path' from mytable file_format = (type = 'csv') COPY_OPTIONS = (ON_ERROR = 'abort');
            copy into 's3://bucket/path' from mytable COPY_OPTIONS = (ON_ERROR = 'abort') file_format = (type = 'csv');
            copy into 's3://bucket/path' from mytable CONNECTION = (xx='a', sss='ss') file_format = (type = 'csv');
            copy into 's3://bucket/path' from mytable CONNECTION = (xx='a', sss='ss') file_format = (type = 'csv') copy_options = (on_error = 'abort');
        "#;

        let _ = ExtParser::parse_sql(sql).unwrap();
    }

    #[test]
    fn test_create_stream() {
        let statement = parse_sql("create stream if not exists test_s trigger = once watermark = '10s' output_mode = update as insert into t_tbl select 1;");

        match statement {
            ExtStatement::CreateStream(s) => {
                let CreateStream {
                    if_not_exists,
                    name,
                    trigger,
                    watermark,
                    output_mode,
                    statement,
                } = s;

                assert!(if_not_exists);
                assert_eq!(name, Ident::new("test_s"));
                assert_eq!(trigger, Some(Trigger::Once));
                assert_eq!(watermark, Some("10s".into()));
                assert_eq!(output_mode, Some(OutputMode::Update));
                assert!(matches!(statement.deref(), Statement::Insert { .. }));
            }
            _ => panic!("expect CreateStream"),
        }
    }

    #[test]
    fn test_drop_stream() {
        let result = parse_sql("drop stream if exists test_s;");

        let expected = ExtStatement::DropStream(ast::DropStream {
            if_exist: true,
            name: Ident::new("test_s"),
        });

        assert_eq!(expected, result);
    }

    #[test]
    fn test_show_streams() {
        let result = parse_sql("show streams verbose;");

        let expected = ExtStatement::ShowStreams(ShowStreams { verbose: true });

        assert_eq!(expected, result);
    }

    #[test]
    fn test_create_stream_table() {
        let statement = parse_sql(
            "CREATE STREAM TABLE TskvTable (
            time TIMESTAMP,
            name STRING,
            driver STRING,
            load_capacity DOUBLE
          ) WITH (
            db = 'public',
            table = 'readings_kv',
            event_time_column = 'time'
          ) engine = tskv;",
        );

        match statement {
            ExtStatement::CreateStreamTable(s) => {
                if let Statement::CreateTable {
                    if_not_exists,
                    name,
                    columns,
                    with_options,
                    engine,
                    ..
                } = s
                {
                    let columns_expected = vec![
                        make_column_def("time", DataType::Timestamp(None, TimezoneInfo::None)),
                        make_column_def("name", DataType::String),
                        make_column_def("driver", DataType::String),
                        make_column_def("load_capacity", DataType::Double),
                    ];
                    let with_options_expected = vec![
                        SqlOption {
                            name: "db".into(),
                            value: Value::SingleQuotedString("public".into()),
                        },
                        SqlOption {
                            name: "table".into(),
                            value: Value::SingleQuotedString("readings_kv".into()),
                        },
                        SqlOption {
                            name: "event_time_column".into(),
                            value: Value::SingleQuotedString("time".into()),
                        },
                    ];

                    assert!(!if_not_exists);
                    assert_eq!("TskvTable", &name.to_string());
                    assert_eq!(columns_expected, columns);
                    assert_eq!(with_options_expected, with_options);
                    assert_eq!(Some("tskv".into()), engine);

                    return;
                }

                panic!("expect CreateStreamTable")
            }
            _ => panic!("expect CreateStream"),
        }
    }
}
