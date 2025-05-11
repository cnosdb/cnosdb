use std::collections::{HashMap, VecDeque};
use std::fmt::Display;
use std::ops::Not;
use std::str::FromStr;

use datafusion::common::parsers::CompressionTypeVariant;
use datafusion::sql::parser::CreateExternalTable;
use datafusion::sql::sqlparser::ast::{
    DataType, Expr, Ident, ObjectName, Offset, OrderByExpr, SqlOption, TableFactor, Value,
};
use datafusion::sql::sqlparser::dialect::keywords::Keyword;
use datafusion::sql::sqlparser::dialect::Dialect;
use datafusion::sql::sqlparser::parser::{IsOptional, Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::{Token, TokenWithLocation, Tokenizer};
use models::codec::Encoding;
use models::meta_data::{NodeId, ReplicationSetId, VnodeId};
use serde_json::Value as JsonValue;
use snafu::ResultExt;
use spi::query::ast::{
    self, parse_string_value, Action, AlterDatabase, AlterTable, AlterTableAction, AlterTenant,
    AlterTenantOperation, AlterUser, AlterUserOperation, ChecksumGroup, ColumnOption,
    CompactDatabase, CompactVnode, CopyIntoLocation, CopyIntoTable, CopyTarget, CopyVnode,
    CreateDatabase, CreateRole, CreateStream, CreateTable, CreateTenant, CreateUser,
    DatabaseConfig, DatabaseOptions, DescribeDatabase, DescribeTable, DropDatabaseObject,
    DropGlobalObject, DropTenantObject, DropVnode, Explain, ExtStatement, GrantRevoke, MoveVnode,
    OutputMode, Privilege, RecoverDatabase, RecoverTenant, ShowSeries, ShowTagBody, ShowTagValues,
    Trigger, UriLocation, With,
};
use spi::query::logical_planner::{DatabaseObjectType, GlobalObjectType, TenantObjectType};
use spi::query::parser::Parser as CnosdbParser;
use spi::ParserSnafu;
use trace::debug;

use super::dialect::CnosDBDialect;

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
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    AFTER,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    RECOVER,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    REPLICA_ID,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    NODE_ID,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    PROMOTE,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    DESTORY,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    REPLICAS,

    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    MAX_MEMCACHE_SIZE,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    MEMCACHE_PARTITIONS,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    WAL_MAX_FILE_SIZE,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    WAL_SYNC,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    STRICT_WRITE,
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    MAX_CACHE_READERS,
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
            "AFTER" => Ok(CnosKeyWord::AFTER),
            "RECOVER" => Ok(CnosKeyWord::RECOVER),
            "REPLICA_ID" => Ok(CnosKeyWord::REPLICA_ID),
            "NODE_ID" => Ok(CnosKeyWord::NODE_ID),
            "PROMOTE" => Ok(CnosKeyWord::PROMOTE),
            "DESTORY" => Ok(CnosKeyWord::DESTORY),
            "REPLICAS" => Ok(CnosKeyWord::REPLICAS),
            "MAX_MEMCACHE_SIZE" => Ok(CnosKeyWord::MAX_MEMCACHE_SIZE),
            "MEMCACHE_PARTITIONS" => Ok(CnosKeyWord::MEMCACHE_PARTITIONS),
            "WAL_MAX_FILE_SIZE" => Ok(CnosKeyWord::WAL_MAX_FILE_SIZE),
            "WAL_SYNC" => Ok(CnosKeyWord::WAL_SYNC),
            "STRICT_WRITE" => Ok(CnosKeyWord::STRICT_WRITE),
            "MAX_CACHE_READERS" => Ok(CnosKeyWord::MAX_CACHE_READERS),
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
    fn parse(&self, sql: &str) -> spi::QueryResult<VecDeque<ExtStatement>> {
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
        let dialect = &CnosDBDialect {};
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
        let dialect = &CnosDBDialect {};
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

        // debug!("Parser sql: {}, stmts: {:#?}", sql, stmts);

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
                Keyword::UPDATE => {
                    self.parser.next_token();
                    let update_ast = self.parser.parse_update()?;
                    Ok(ExtStatement::SqlStatement(Box::new(update_ast)))
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
                            CnosKeyWord::RECOVER => {
                                self.parser.next_token();
                                self.parse_recover()
                            }
                            CnosKeyWord::REPLICA => {
                                self.parser.next_token();
                                self.parse_replica()
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
        } else if self.parse_cnos_keyword(CnosKeyWord::REPLICAS) {
            self.parse_show_replicas()
        } else {
            parser_err!(format!("nonsupport: {}", self.parser.peek_token()))
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

    fn parse_show_replicas(&mut self) -> Result<ExtStatement> {
        Ok(ExtStatement::ShowReplicas)
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
        } else if self.parser.parse_keyword(Keyword::RENAME) {
            let alter_tbl = self.parse_alter_table_rename(table_name)?;
            Ok(ExtStatement::AlterTable(alter_tbl))
        } else {
            self.expected("ADD or ALTER or DROP or RENAME", self.parser.peek_token())
        }
    }

    fn parse_alter_table_add_column(&mut self, table_name: ObjectName) -> Result<ExtStatement> {
        if self.parse_cnos_keyword(CnosKeyWord::FIELD) {
            let column = self.parse_cnos_field()?;
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

    fn parse_alter_table_rename(&mut self, table_name: ObjectName) -> Result<AlterTable> {
        if self.parser.parse_keyword(Keyword::COLUMN) {
            let old_column_name = self.parser.parse_identifier()?;
            self.parser.expect_keyword(Keyword::TO)?;
            let new_column_name = self.parser.parse_identifier()?;
            Ok(AlterTable {
                table_name,
                alter_action: AlterTableAction::RenameColumn {
                    old_column_name,
                    new_column_name,
                },
            })
        } else {
            self.expected("COLUMN", self.parser.peek_token())?
        }
    }

    fn parse_alter_table_alter_column(&mut self, table_name: ObjectName) -> Result<ExtStatement> {
        let column_name = self.parser.parse_identifier()?;
        // parse: SET CODEC(encoding_type)
        self.parser.expect_keyword(Keyword::SET)?;

        self.expect_cnos_keyword(CnosKeyWord::CODEC)?;
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
        let mut config = DatabaseConfig::default();
        if !self.parse_database_option_or_config(&mut options, &mut config)? {
            return parser_err!(format!(
                "expected database option, but found {}",
                self.parser.peek_token()
            ));
        }
        if config.has_some() {
            return parser_err!("database config is unmodifiable, only can modify database option: TTL, SHARD, VNODE_DURATION, REPLICA".to_string());
        }
        Ok(ExtStatement::AlterDatabase(
            AlterDatabase {
                name: database_name,
                options,
            }
            .into(),
        ))
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
                let sql_option = self.alter_parse_limiter()?;
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

    fn alter_parse_limiter(&mut self) -> Result<SqlOption, ParserError> {
        let mut limiter_options = serde_json::Map::new(); // 用于存储 _limiter 内部配置
        let mut drop_after = None;
        let mut comment = None;
        let mut has_limiter_option = false;
        let mut has_comment_option = false;
        let mut has_drop_after_option = false;

        while self.parser.peek_token().token != Token::EOF {
            let name = self.parser.parse_identifier()?;
            // self.parser.expect_token(&Token::Eq)?;
            // 跳过所有空白字符，包括空格、制表符和换行符
            while matches!(
                self.parser.peek_token().token,
                Token::Whitespace(_) | Token::Eq
            ) {
                self.parser.next_token(); // 跳过空白字符
            }

            match name.value.to_lowercase().as_str() {
                "comment" => {
                    comment = Some(self.parser.parse_literal_string()?);
                    has_comment_option = true;
                }
                "drop_after" => {
                    drop_after = Some(self.parser.parse_literal_string()?);
                    has_drop_after_option = true;
                }
                "object_config" => {
                    limiter_options.insert(name.value.to_lowercase(), self.parse_object_config()?);
                    has_limiter_option = true;
                }
                "coord_data_in" | "coord_data_out" | "coord_queries" | "coord_writes"
                | "http_data_in" | "http_data_out" | "http_queries" | "http_writes" => {
                    limiter_options.insert(name.value.to_lowercase(), self.parse_request_config()?);
                    has_limiter_option = true; // 记录有有效的选项
                }
                _ => {
                    return Err(ParserError::ParserError(format!(
                        "Unknown tenant option: {}",
                        name.value
                    )));
                }
            }
            // 处理逗号
            if !self.parser.consume_token(&Token::Comma) {
                break;
            }
        }
        // 检查是否有多个选项同时为 true
        let has_options_count = [
            has_comment_option,
            has_drop_after_option,
            has_limiter_option,
        ]
        .iter()
        .filter(|&&x| x)
        .count();

        if has_options_count > 1 {
            return Err(ParserError::ParserError(
                "Cannot set multiple options (comment, drop_after, _limiter) at the same time"
                    .to_string(),
            ));
        }

        let mut request_config = serde_json::Map::new();
        if let Some(coord_data_in) = limiter_options.remove("coord_data_in") {
            request_config.insert("coord_data_in".to_string(), coord_data_in);
        }
        if let Some(coord_data_out) = limiter_options.remove("coord_data_out") {
            request_config.insert("coord_data_out".to_string(), coord_data_out);
        }
        if let Some(coord_queries) = limiter_options.remove("coord_queries") {
            request_config.insert("coord_queries".to_string(), coord_queries);
        }
        if let Some(coord_writes) = limiter_options.remove("coord_writes") {
            request_config.insert("coord_writes".to_string(), coord_writes);
        }
        if let Some(http_data_in) = limiter_options.remove("http_data_in") {
            request_config.insert("http_data_in".to_string(), http_data_in);
        }
        if let Some(http_data_out) = limiter_options.remove("http_data_out") {
            request_config.insert("http_data_out".to_string(), http_data_out);
        }
        if let Some(http_queries) = limiter_options.remove("http_queries") {
            request_config.insert("http_queries".to_string(), http_queries);
        }
        if let Some(http_writes) = limiter_options.remove("http_writes") {
            request_config.insert("http_writes".to_string(), http_writes);
        }
        let mut limiter_json = serde_json::Map::new();
        if let Some(object_config) = limiter_options.remove("object_config") {
            limiter_json.insert("object_config".to_string(), object_config);
        }
        if !request_config.is_empty() {
            limiter_json.insert(
                "request_config".to_string(),
                JsonValue::Object(request_config),
            );
        }

        // 将 limiter_json 转换为 SqlOption
        if has_comment_option {
            return Ok(SqlOption {
                name: Ident::new("comment"),
                value: Value::SingleQuotedString(comment.unwrap().to_string()),
            });
        }
        if has_drop_after_option {
            return Ok(SqlOption {
                name: Ident::new("drop_after"),
                value: Value::SingleQuotedString(drop_after.unwrap().to_string()),
            });
        }
        if has_limiter_option {
            return Ok(SqlOption {
                name: Ident::new("_limiter"),
                value: Value::SingleQuotedString(JsonValue::Object(limiter_json).to_string()),
            });
        }
        Err(ParserError::ParserError(
            "No valid options found".to_string(),
        ))
    }

    fn parse_alter_user(&mut self) -> Result<ExtStatement> {
        let name = self.parser.parse_identifier()?;

        let operation = if self.parser.parse_keyword(Keyword::RENAME) {
            self.parser.expect_keyword(Keyword::TO)?;
            let new_name = self.parser.parse_identifier()?;
            AlterUserOperation::RenameTo(new_name)
        } else if self.parser.parse_keyword(Keyword::SET) {
            let sql_option = ExtParser::parse_sql_option(&mut self.parser)?;
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

    fn parse_create_external_table(&mut self, unbounded: bool) -> Result<ExtStatement> {
        self.parser.expect_keyword(Keyword::TABLE)?;
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let table_name = self.parser.parse_object_name()?;
        check_name_not_contain_illegal_character(&table_name)?;
        let (columns, _) = self.parser.parse_columns()?;

        #[derive(Default)]
        struct Builder {
            file_type: Option<String>,
            location: Option<String>,
            has_header: Option<bool>,
            delimiter: Option<char>,
            file_compression_type: Option<CompressionTypeVariant>,
            table_partition_cols: Option<Vec<String>>,
            order_exprs: Vec<Vec<OrderByExpr>>,
            options: Option<HashMap<String, String>>,
        }
        let mut builder = Builder::default();

        fn ensure_not_set<T>(field: &Option<T>, name: &str) -> Result<(), ParserError> {
            if field.is_some() {
                return Err(ParserError::ParserError(format!(
                    "{name} specified more than once",
                )));
            }
            Ok(())
        }

        loop {
            if let Some(keyword) = self.parser.parse_one_of_keywords(&[
                Keyword::STORED,
                Keyword::LOCATION,
                Keyword::WITH,
                Keyword::DELIMITER,
                Keyword::COMPRESSION,
                Keyword::PARTITIONED,
                Keyword::OPTIONS,
            ]) {
                match keyword {
                    Keyword::STORED => {
                        self.parser.expect_keyword(Keyword::AS)?;
                        ensure_not_set(&builder.file_type, "STORED AS")?;
                        builder.file_type = Some(self.parse_file_format()?);
                    }
                    Keyword::LOCATION => {
                        ensure_not_set(&builder.location, "LOCATION")?;
                        builder.location = Some(self.parser.parse_literal_string()?);
                    }
                    Keyword::WITH => {
                        if self.parser.parse_keyword(Keyword::ORDER) {
                            builder.order_exprs.push(self.parse_order_by_exprs()?);
                        } else {
                            self.parser.expect_keyword(Keyword::HEADER)?;
                            self.parser.expect_keyword(Keyword::ROW)?;
                            ensure_not_set(&builder.has_header, "WITH HEADER ROW")?;
                            builder.has_header = Some(true);
                        }
                    }
                    Keyword::DELIMITER => {
                        ensure_not_set(&builder.delimiter, "DELIMITER")?;
                        builder.delimiter = Some(self.parse_delimiter()?);
                    }
                    Keyword::COMPRESSION => {
                        self.parser.expect_keyword(Keyword::TYPE)?;
                        ensure_not_set(&builder.file_compression_type, "COMPRESSION TYPE")?;
                        builder.file_compression_type = Some(self.parse_file_compression_type()?);
                    }
                    Keyword::PARTITIONED => {
                        self.parser.expect_keyword(Keyword::BY)?;
                        ensure_not_set(&builder.table_partition_cols, "PARTITIONED BY")?;
                        builder.table_partition_cols = Some(self.parse_partitions()?);
                    }
                    Keyword::OPTIONS => {
                        ensure_not_set(&builder.options, "OPTIONS")?;
                        builder.options = Some(self.parse_string_options()?);
                    }
                    _ => {
                        unreachable!()
                    }
                }
            } else {
                let token = self.parser.next_token();
                if token == Token::EOF || token == Token::SemiColon {
                    break;
                } else {
                    return Err(ParserError::ParserError(format!(
                        "Unexpected token {token}"
                    )));
                }
            }
        }

        // Validations: location and file_type are required
        if builder.file_type.is_none() {
            return Err(ParserError::ParserError(
                "Missing STORED AS clause in CREATE EXTERNAL TABLE statement".into(),
            ));
        }
        if builder.location.is_none() {
            return Err(ParserError::ParserError(
                "Missing LOCATION clause in CREATE EXTERNAL TABLE statement".into(),
            ));
        }

        let create = CreateExternalTable {
            name: table_name.to_string(),
            columns,
            file_type: builder.file_type.unwrap(),
            has_header: builder.has_header.unwrap_or(false),
            delimiter: builder.delimiter.unwrap_or(','),
            location: builder.location.unwrap(),
            table_partition_cols: builder.table_partition_cols.unwrap_or(vec![]),
            order_exprs: builder.order_exprs,
            if_not_exists,
            file_compression_type: builder
                .file_compression_type
                .unwrap_or(CompressionTypeVariant::UNCOMPRESSED),
            unbounded,
            options: builder.options.unwrap_or(HashMap::new()),
        };
        Ok(ExtStatement::CreateExternalTable(create))
    }

    fn parse_create_table(&mut self) -> Result<ExtStatement> {
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let table_name = self.parser.parse_object_name()?;
        check_name_not_contain_illegal_character(&table_name)?;
        let columns = self.parse_cnos_columns()?;
        let create = CreateTable {
            name: table_name,
            if_not_exists,
            columns,
        };
        Ok(ExtStatement::CreateTable(create))
    }

    fn parse_database_options_and_config(&mut self) -> Result<(DatabaseOptions, DatabaseConfig)> {
        if self.parser.parse_keyword(Keyword::WITH) {
            let mut options = DatabaseOptions::default();
            let mut config = DatabaseConfig::default();
            loop {
                if !self.parse_database_option_or_config(&mut options, &mut config)? {
                    return Ok((options, config));
                }
            }
        }
        Ok((DatabaseOptions::default(), DatabaseConfig::default()))
    }

    fn parse_database_option_or_config(
        &mut self,
        options: &mut DatabaseOptions,
        config: &mut DatabaseConfig,
    ) -> Result<bool> {
        if self.parse_cnos_keyword(CnosKeyWord::TTL) {
            let _ = self.parser.expect_token(&Token::Eq);
            options.ttl = Some(self.parse_string_value()?);
        } else if self.parse_cnos_keyword(CnosKeyWord::SHARD) {
            let _ = self.parser.expect_token(&Token::Eq);
            let shard_num = self.parse_number::<u64>()?;
            if shard_num == 0 {
                return parser_err!("shard number should be greater than 0");
            }
            options.shard_num = Some(shard_num);
        } else if self.parse_cnos_keyword(CnosKeyWord::VNODE_DURATION) {
            let _ = self.parser.expect_token(&Token::Eq);
            options.vnode_duration = Some(self.parse_string_value()?);
        } else if self.parse_cnos_keyword(CnosKeyWord::REPLICA) {
            let _ = self.parser.expect_token(&Token::Eq);
            let replica = self.parse_number::<u64>()?;
            if replica == 0 {
                return parser_err!("replica number should be greater than 0");
            }
            options.replica = Some(replica);
        } else if self.parse_cnos_keyword(CnosKeyWord::PRECISION) {
            let _ = self.parser.expect_token(&Token::Eq);
            config.precision = Some(self.parse_string_value()?);
        } else if self.parse_cnos_keyword(CnosKeyWord::MAX_MEMCACHE_SIZE) {
            let _ = self.parser.expect_token(&Token::Eq);
            config.max_memcache_size = Some(self.parse_string_value()?);
        } else if self.parse_cnos_keyword(CnosKeyWord::MEMCACHE_PARTITIONS) {
            let _ = self.parser.expect_token(&Token::Eq);
            config.memcache_partitions = Some(self.parse_number::<u64>()?);
        } else if self.parse_cnos_keyword(CnosKeyWord::WAL_MAX_FILE_SIZE) {
            let _ = self.parser.expect_token(&Token::Eq);
            config.wal_max_file_size = Some(self.parse_string_value()?);
        } else if self.parse_cnos_keyword(CnosKeyWord::WAL_SYNC) {
            let _ = self.parser.expect_token(&Token::Eq);
            config.wal_sync = Some(self.parse_string_value()?);
        } else if self.parse_cnos_keyword(CnosKeyWord::STRICT_WRITE) {
            let _ = self.parser.expect_token(&Token::Eq);
            config.strict_write = Some(self.parse_string_value()?);
        } else if self.parse_cnos_keyword(CnosKeyWord::MAX_CACHE_READERS) {
            let _ = self.parser.expect_token(&Token::Eq);
            config.max_cache_readers = Some(self.parse_number::<u64>()?);
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

    fn parse_partitions(&mut self) -> Result<Vec<String>, ParserError> {
        let mut partitions: Vec<String> = vec![];
        if !self.parser.consume_token(&Token::LParen) || self.parser.consume_token(&Token::RParen) {
            return Ok(partitions);
        }

        loop {
            if let Token::Word(_) = self.parser.peek_token().token {
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

    /// Parses (key value) style options where the values are literal strings.
    fn parse_string_options(&mut self) -> Result<HashMap<String, String>, ParserError> {
        let mut options = HashMap::new();
        self.parser.expect_token(&Token::LParen)?;

        loop {
            let key = self.parser.parse_literal_string()?;
            let value = self.parser.parse_literal_string()?;
            options.insert(key, value);
            let comma = self.parser.consume_token(&Token::Comma);
            if self.parser.consume_token(&Token::RParen) {
                // allow a trailing comma, even though it's not in standard
                break;
            } else if !comma {
                return self.expected(
                    "',' or ')' after option definition",
                    self.parser.peek_token(),
                );
            }
        }
        Ok(options)
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
        let name_vec = ObjectName(vec![database_name.clone()]);
        check_name_not_contain_illegal_character(&name_vec)?;
        let (options, config) = self.parse_database_options_and_config()?;
        Ok(ExtStatement::CreateDatabase(
            CreateDatabase {
                name: database_name,
                if_not_exists,
                options,
                config,
            }
            .into(),
        ))
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
        let name_vec = ObjectName(vec![name.clone()]);
        check_name_not_contain_illegal_character(&name_vec)?;

        let with_options = if self.parser.parse_keyword(Keyword::WITH) {
            self.parser
                .parse_comma_separated(ExtParser::parse_sql_option)?
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
        let name_vec = ObjectName(vec![name.clone()]);
        check_name_not_contain_illegal_character(&name_vec)?;

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
    // --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    fn parse_create_tenant(&mut self) -> Result<ExtStatement> {
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);

        let name = self.parser.parse_identifier()?;
        let name_vec = ObjectName(vec![name.clone()]);
        check_name_not_contain_illegal_character(&name_vec)?;

        let with_options = if self.parser.parse_keyword(Keyword::WITH) {
            self.parse_tenant_with_options()?
        } else {
            vec![]
        };

        Ok(ExtStatement::CreateTenant(CreateTenant {
            if_not_exists,
            name,
            with_options,
        }))
    }

    fn parse_tenant_with_options(&mut self) -> Result<Vec<SqlOption>, ParserError> {
        let mut with_options = vec![];
        let mut limiter_options = serde_json::Map::new(); // 用于存储 _limiter 内部配置
        let mut comment = None;
        let mut drop_after = None;
        let mut has_limiter_option = false; // 标志，用于检查是否有有效的选项

        while self.parser.peek_token().token != Token::EOF {
            let name = self.parser.parse_identifier()?;
            // self.parser.expect_token(&Token::Eq)?;
            // 跳过所有空白字符，包括空格、制表符和换行符
            while matches!(
                self.parser.peek_token().token,
                Token::Whitespace(_) | Token::Eq
            ) {
                self.parser.next_token(); // 跳过空白字符
            }

            match name.value.to_lowercase().as_str() {
                "comment" => {
                    comment = Some(self.parser.parse_literal_string()?);
                }
                "drop_after" => {
                    drop_after = Some(self.parser.parse_literal_string()?);
                }
                "object_config" => {
                    limiter_options.insert(name.value.to_lowercase(), self.parse_object_config()?);
                    has_limiter_option = true; // 记录有有效的选项
                }
                "coord_data_in" | "coord_data_out" | "coord_queries" | "coord_writes"
                | "http_data_in" | "http_data_out" | "http_queries" | "http_writes" => {
                    limiter_options.insert(name.value.to_lowercase(), self.parse_request_config()?);
                    has_limiter_option = true; // 记录有有效的选项
                }
                _ => {
                    return Err(ParserError::ParserError(format!(
                        "Unknown tenant option: {}",
                        name.value
                    )));
                }
            }
            // 处理逗号
            if !self.parser.consume_token(&Token::Comma) {
                break;
            }
        }
        // 构建 _limiter 的 JSON 字符串，仅包含存在的字段
        let mut request_config = serde_json::Map::new();
        if let Some(coord_data_in) = limiter_options.remove("coord_data_in") {
            request_config.insert("coord_data_in".to_string(), coord_data_in);
        }
        if let Some(coord_data_out) = limiter_options.remove("coord_data_out") {
            request_config.insert("coord_data_out".to_string(), coord_data_out);
        }
        if let Some(coord_queries) = limiter_options.remove("coord_queries") {
            request_config.insert("coord_queries".to_string(), coord_queries);
        }
        if let Some(coord_writes) = limiter_options.remove("coord_writes") {
            request_config.insert("coord_writes".to_string(), coord_writes);
        }
        if let Some(http_data_in) = limiter_options.remove("http_data_in") {
            request_config.insert("http_data_in".to_string(), http_data_in);
        }
        if let Some(http_data_out) = limiter_options.remove("http_data_out") {
            request_config.insert("http_data_out".to_string(), http_data_out);
        }
        if let Some(http_queries) = limiter_options.remove("http_queries") {
            request_config.insert("http_queries".to_string(), http_queries);
        }
        if let Some(http_writes) = limiter_options.remove("http_writes") {
            request_config.insert("http_writes".to_string(), http_writes);
        }

        let mut limiter_json = serde_json::Map::new();
        if let Some(object_config) = limiter_options.remove("object_config") {
            limiter_json.insert("object_config".to_string(), object_config);
        }
        if !request_config.is_empty() {
            limiter_json.insert(
                "request_config".to_string(),
                JsonValue::Object(request_config),
            );
        }
        if let Some(drop_after) = drop_after {
            with_options.push(SqlOption {
                name: Ident::new("drop_after"),
                value: Value::SingleQuotedString(drop_after),
            });
        }
        // 将 comment 和 drop_after 添加到 with_options 中
        if let Some(comment) = comment {
            with_options.push(SqlOption {
                name: Ident::new("comment"),
                value: Value::SingleQuotedString(comment),
            });
        }
        // 仅在有有效的选项时才添加 _limiter
        if has_limiter_option && !limiter_json.is_empty() {
            with_options.push(SqlOption {
                name: Ident::new("_limiter"),
                value: Value::SingleQuotedString(JsonValue::Object(limiter_json).to_string()),
            });
        }

        Ok(with_options)
    }

    fn parse_object_config(&mut self) -> Result<JsonValue, ParserError> {
        let mut map = serde_json::Map::new();
        let valid_keys = [
            "max_users_number",
            "max_databases",
            "max_shard_number",
            "max_replicate_number",
            "max_retention_time",
        ];

        // 假设只有一组键值对，没有逗号分隔
        while let Token::Word(_) = self.parser.peek_token().token {
            let key = self.parser.parse_identifier()?;

            // 检查键是否在有效键列表中
            if !valid_keys.contains(&key.value.to_lowercase().as_str()) {
                return Err(ParserError::ParserError(key.value)); // 返回解析错误
            }

            self.parser.expect_token(&Token::Eq)?;

            // 处理负数
            let mut is_negative = false;
            if self.parser.peek_token().token == Token::Minus {
                self.parser.next_token();
                is_negative = true;
            }

            let val = self.parser.parse_value()?;
            // 检查值是否为非负数
            if let Value::Number(num, _) = &val {
                let parsed_num = num
                    .parse::<f64>()
                    .map_err(|_| ParserError::ParserError(key.value.clone()))?;
                if is_negative && parsed_num > 0.0 {
                    return Err(ParserError::ParserError(format!(
                        "{} cannot be negative",
                        key.value
                    )));
                }
            }
            map.insert(
                key.value.to_lowercase(),
                ExtParser::sql_value_to_json_value(val)?,
            );

            // 这里不再检查逗号，直接继续下一个键值对，直到没有更多键值对
        }

        Ok(JsonValue::Object(map))
    }

    fn parse_request_config(&mut self) -> Result<JsonValue, ParserError> {
        let mut remote_map = serde_json::Map::new();
        let mut local_map = serde_json::Map::new();

        let valid_keys = [
            "remote_max",
            "remote_initial",
            "remote_refill",
            "remote_interval",
            "local_max",
            "local_initial",
        ];

        while let Token::Word(_) = self.parser.peek_token().token {
            let key = self.parser.parse_identifier()?;
            let key_str = key.value.to_lowercase();

            if !valid_keys.contains(&key_str.as_str()) {
                return Err(ParserError::ParserError(key.value));
            }

            self.parser.expect_token(&Token::Eq)?;
            // 处理负数
            let mut is_negative = false;
            if self.parser.peek_token().token == Token::Minus {
                self.parser.next_token();
                is_negative = true;
            }
            let val = self.parser.parse_value()?;
            // 检查值是否为非负数
            if let Value::Number(num, _) = &val {
                let parsed_num = num
                    .parse::<f64>()
                    .map_err(|_| ParserError::ParserError(key.value.clone()))?;
                if is_negative && parsed_num > 0.0 {
                    return Err(ParserError::ParserError(format!(
                        "{} cannot be negative",
                        key.value
                    )));
                }
            }
            let json_val = ExtParser::sql_value_to_json_value(val)?;

            // 将键值对添加到相应的桶中
            if key_str.starts_with("remote") {
                let field_key = key_str.strip_prefix("remote_").unwrap();
                remote_map.insert(field_key.to_string(), json_val);
            } else if key_str.starts_with("local") {
                let field_key = key_str.strip_prefix("local_").unwrap();
                local_map.insert(field_key.to_string(), json_val);
            }
        }

        // 使用 collect() 将数组转换为 serde_json::Map
        let result_map: serde_json::Map<String, serde_json::Value> = [
            ("remote_bucket".to_string(), JsonValue::Object(remote_map)),
            ("local_bucket".to_string(), JsonValue::Object(local_map)),
        ]
        .iter()
        .cloned()
        .collect();

        Ok(JsonValue::Object(result_map))
    }

    fn sql_value_to_json_value(value: Value) -> Result<JsonValue, ParserError> {
        match value {
            Value::Number(n, _) => Ok(JsonValue::Number(
                n.parse::<serde_json::Number>()
                    .map_err(|e| ParserError::TokenizerError(e.to_string()))?,
            )),
            Value::SingleQuotedString(s) => Ok(JsonValue::String(s)),
            Value::Boolean(b) => Ok(JsonValue::Bool(b)),
            Value::Null => Ok(JsonValue::Null),
            _ => Err(ParserError::TokenizerError(
                "Unsupported value type".to_string(),
            )),
        }
    }
    // --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
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
            self.parse_create_external_table(false)
        } else if self.parser.parse_keyword(Keyword::UNBOUNDED) {
            self.parser.expect_keyword(Keyword::EXTERNAL)?;
            self.parse_create_external_table(true)
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
            .parse_comma_separated(ExtParser::parse_sql_option)?;

        self.parser.expect_token(&Token::RParen)?;

        Ok(options)
    }

    fn parse_recover(&mut self) -> Result<ExtStatement> {
        let ast = if self.parser.parse_keyword(Keyword::DATABASE) {
            let if_exist = self.parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
            let object_name = self.parser.parse_identifier()?;
            ExtStatement::RecoverDatabase(RecoverDatabase {
                object_name,
                if_exist,
            })
        } else if self.parse_cnos_keyword(CnosKeyWord::TENANT) {
            let if_exist = self.parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
            let object_name = self.parser.parse_identifier()?;
            ExtStatement::RecoverTenant(RecoverTenant {
                object_name,
                if_exist,
            })
        } else {
            return self.expected("DATABASE,TENANT after RECOVER", self.parser.peek_token());
        };

        Ok(ast)
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
            let mut after = None;
            if self.parse_cnos_keyword(CnosKeyWord::AFTER) {
                after = Some(self.parse_string_value()?);
            }
            ExtStatement::DropTenantObject(DropTenantObject {
                object_name,
                if_exist,
                obj_type: TenantObjectType::Database,
                after,
            })
        } else if self.parse_cnos_keyword(CnosKeyWord::TENANT) {
            let if_exist = self.parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
            let object_name = self.parser.parse_identifier()?;
            let mut after = None;
            if self.parse_cnos_keyword(CnosKeyWord::AFTER) {
                after = Some(self.parse_string_value()?);
            }
            ExtStatement::DropGlobalObject(DropGlobalObject {
                object_name,
                if_exist,
                obj_type: GlobalObjectType::Tenant,
                after,
            })
        } else if self.parser.parse_keyword(Keyword::USER) {
            let if_exist = self.parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
            let object_name = self.parser.parse_identifier()?;
            ExtStatement::DropGlobalObject(DropGlobalObject {
                object_name,
                if_exist,
                obj_type: GlobalObjectType::User,
                after: None,
            })
        } else if self.parser.parse_keyword(Keyword::ROLE) {
            let if_exist = self.parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
            let object_name = self.parser.parse_identifier()?;
            ExtStatement::DropTenantObject(DropTenantObject {
                object_name,
                if_exist,
                obj_type: TenantObjectType::Role,
                after: None,
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
        } else if self.parser.parse_keyword(Keyword::DATABASE) {
            let database_name = self.parser.parse_identifier()?;
            Ok(ExtStatement::CompactDatabase(CompactDatabase {
                database_name,
            }))
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

    fn parse_replica(&mut self) -> Result<ExtStatement> {
        if self.parser.parse_keyword(Keyword::ADD) {
            if !self.parse_cnos_keyword(CnosKeyWord::REPLICA_ID) {
                return parser_err!("expected REPLICA_ID, after ADD");
            }
            let replica_id = self.parse_number::<ReplicationSetId>()?;
            if !self.parse_cnos_keyword(CnosKeyWord::NODE_ID) {
                return parser_err!("expected NODE_ID, after REPLICA_ID");
            }
            let node_id = self.parse_number::<NodeId>()?;
            Ok(ExtStatement::ReplicaAdd(ast::ReplicaAdd {
                replica_id,
                node_id,
            }))
        } else if self.parse_cnos_keyword(CnosKeyWord::REMOVE) {
            if !self.parse_cnos_keyword(CnosKeyWord::REPLICA_ID) {
                return parser_err!("expected REPLICA_ID, after ADD");
            }
            let replica_id = self.parse_number::<ReplicationSetId>()?;
            if !self.parse_cnos_keyword(CnosKeyWord::NODE_ID) {
                return parser_err!("expected NODE_ID, after REPLICA_ID");
            }
            let node_id = self.parse_number::<NodeId>()?;
            Ok(ExtStatement::ReplicaRemove(ast::ReplicaRemove {
                replica_id,
                node_id,
            }))
        } else if self.parse_cnos_keyword(CnosKeyWord::PROMOTE) {
            if !self.parse_cnos_keyword(CnosKeyWord::REPLICA_ID) {
                return parser_err!("expected REPLICA_ID, after ADD");
            }
            let replica_id = self.parse_number::<ReplicationSetId>()?;
            if !self.parse_cnos_keyword(CnosKeyWord::NODE_ID) {
                return parser_err!("expected NODE_ID, after REPLICA_ID");
            }
            let node_id = self.parse_number::<NodeId>()?;
            Ok(ExtStatement::ReplicaPromote(ast::ReplicaPromote {
                replica_id,
                node_id,
            }))
        } else if self.parse_cnos_keyword(CnosKeyWord::DESTORY) {
            if !self.parse_cnos_keyword(CnosKeyWord::REPLICA_ID) {
                return parser_err!("expected REPLICA_ID, after ADD");
            }
            let replica_id = self.parse_number::<ReplicationSetId>()?;
            Ok(ExtStatement::ReplicaDestroy(ast::ReplicaDestroy {
                replica_id,
            }))
        } else {
            parser_err!("expected VNODE, after MOVE")
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

    fn expect_cnos_keyword(&mut self, key_word: CnosKeyWord) -> Result<()> {
        if self.parse_cnos_keyword(key_word) {
            Ok(())
        } else {
            self.parser.expected(
                format!("{:?}", &key_word).as_str(),
                self.parser.peek_token(),
            )
        }
    }

    // parse: ident data_type [CODEC(encoding_type)]
    fn parse_cnos_field(&mut self) -> Result<ColumnOption, ParserError> {
        let name = self.parser.parse_identifier()?;
        let column_type = self.parser.parse_data_type()?;
        let encoding = if self.parse_cnos_keyword(CnosKeyWord::CODEC) {
            Some(self.parse_codec_type()?)
        } else {
            None
        };
        Ok(ColumnOption::new_field(name, column_type, encoding))
    }

    fn parse_cnos_columns(&mut self) -> Result<Vec<ColumnOption>> {
        // -- Parse as is without adding any semantics
        let mut all_columns: Vec<ColumnOption> = vec![];
        let mut field_columns: Vec<ColumnOption> = vec![];

        if !self.consume_token(&Token::LParen) || self.consume_token(&Token::RParen) {
            return parser_err!("Expected field columns when create table");
        }
        loop {
            let column = self.parse_cnos_field()?;
            field_columns.push(column);
            if self.consume_token(&Token::Comma) {
                // parse TAGS(...,...)
                if self.parse_cnos_keyword(CnosKeyWord::TAGS) {
                    let idents: Vec<Ident> = self
                        .parser
                        .parse_parenthesized_column_list(IsOptional::Mandatory, true)?;
                    let column_options = idents.into_iter().map(|i| ColumnOption {
                        name: i,
                        is_tag: true,
                        data_type: DataType::String,
                        encoding: None,
                    });
                    all_columns.extend(column_options);
                    self.parser.expect_token(&Token::RParen)?;
                    break;
                }
            } else if self.parser.consume_token(&Token::RParen) {
                break;
            } else {
                return parser_err!("Expected token ',', 'TAGS' or ')'");
            }
        }
        // tag1, tag2, ..., field1, field2, ...
        all_columns.append(&mut field_columns);

        Ok(all_columns)
    }

    fn parse_codec_encoding(&mut self) -> Result<Encoding, String> {
        self.parser
            .peek_token()
            .to_string()
            .parse()
            .inspect(|_encoding| {
                self.parser.next_token();
            })
    }

    fn parse_codec_type(&mut self) -> Result<Encoding> {
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

    fn parse_sql_option(parser: &mut Parser<'_>) -> Result<SqlOption, ParserError> {
        let name = parser.parse_identifier()?;
        let _ = parser.expect_token(&Token::Eq);
        let value = parser.parse_value()?;
        Ok(SqlOption { name, value })
    }
}

fn check_name_not_contain_illegal_character(object_name: &ObjectName) -> Result<(), ParserError> {
    let names: Vec<String> = object_name
        .0
        .iter()
        .map(|ident| ident.value.clone())
        .collect();

    let name_str = names.join(".");

    // 检查组合后的字符串是否包含 '/'
    if name_str.contains('/') {
        return Err(ParserError::ParserError(format!(
            "not supported keyword contains '/': {}",
            name_str
        )));
    }
    if name_str.trim().is_empty() {
        return Err(ParserError::ParserError(
            "Name cannot be empty or contain only spaces".to_string(),
        ));
    }

    Ok(())
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
    use spi::query::ast::{AlterTable, ExtStatement, ShowStreams, UriLocation};
    use spi::query::logical_planner::TenantObjectType;

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
        let sql = "drop database if exists test_db";
        let statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            ExtStatement::DropTenantObject(DropTenantObject {
                object_name,
                if_exist,
                obj_type,
                after: None,
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
                after: None,
            }) => {
                assert_eq!(object_name.to_string(), "test_db".to_string());
                assert_eq!(if_exist.to_string(), "false".to_string());
                assert_eq!(obj_type, &TenantObjectType::Database);
            }
            _ => panic!("failed"),
        }

        let sql = "drop database test_db after '1h'";
        let statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            ExtStatement::DropTenantObject(DropTenantObject {
                object_name,
                if_exist,
                obj_type,
                after,
            }) => {
                assert_eq!(object_name.to_string(), "test_db".to_string());
                assert_eq!(if_exist.to_string(), "false".to_string());
                assert_eq!(obj_type, &TenantObjectType::Database);
                assert_eq!(*after, Some("1h".to_string()));
            }
            _ => panic!("failed"),
        }

        let sql = "drop database if exists test_db after '1h'";
        let statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            ExtStatement::DropTenantObject(DropTenantObject {
                object_name,
                if_exist,
                obj_type,
                after,
            }) => {
                assert_eq!(object_name.to_string(), "test_db".to_string());
                assert_eq!(if_exist.to_string(), "true".to_string());
                assert_eq!(obj_type, &TenantObjectType::Database);
                assert_eq!(*after, Some("1h".to_string()));
            }
            _ => panic!("failed"),
        }

        let sql = "drop tenant if exists test_tt";
        let statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            ExtStatement::DropGlobalObject(DropGlobalObject {
                object_name,
                if_exist,
                obj_type,
                after: None,
            }) => {
                assert_eq!(object_name.to_string(), "test_tt".to_string());
                assert_eq!(if_exist.to_string(), "true".to_string());
                assert_eq!(obj_type, &GlobalObjectType::Tenant);
            }
            _ => panic!("failed"),
        }

        let sql = "drop tenant test_tt";
        let statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            ExtStatement::DropGlobalObject(DropGlobalObject {
                object_name,
                if_exist,
                obj_type,
                after: None,
            }) => {
                assert_eq!(object_name.to_string(), "test_tt".to_string());
                assert_eq!(if_exist.to_string(), "false".to_string());
                assert_eq!(obj_type, &GlobalObjectType::Tenant);
            }
            _ => panic!("failed"),
        }

        let sql = "drop tenant test_tt after '1h'";
        let statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            ExtStatement::DropGlobalObject(DropGlobalObject {
                object_name,
                if_exist,
                obj_type,
                after,
            }) => {
                assert_eq!(object_name.to_string(), "test_tt".to_string());
                assert_eq!(if_exist.to_string(), "false".to_string());
                assert_eq!(obj_type, &GlobalObjectType::Tenant);
                assert_eq!(*after, Some("1h".to_string()));
            }
            _ => panic!("failed"),
        }

        let sql = "drop tenant if exists test_tt after '1h'";
        let statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            ExtStatement::DropGlobalObject(DropGlobalObject {
                object_name,
                if_exist,
                obj_type,
                after,
            }) => {
                assert_eq!(object_name.to_string(), "test_tt".to_string());
                assert_eq!(if_exist.to_string(), "true".to_string());
                assert_eq!(obj_type, &GlobalObjectType::Tenant);
                assert_eq!(*after, Some("1h".to_string()));
            }
            _ => panic!("failed"),
        }
    }

    #[test]
    fn test_recover() {
        let sql = "recover database test_db";
        let statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            ExtStatement::RecoverDatabase(RecoverDatabase {
                object_name,
                if_exist,
            }) => {
                assert_eq!(object_name.to_string(), "test_db".to_string());
                assert_eq!(if_exist.to_string(), "false".to_string());
            }
            _ => panic!("failed"),
        }

        let sql = "recover database if exists test_db";
        let statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            ExtStatement::RecoverDatabase(RecoverDatabase {
                object_name,
                if_exist,
            }) => {
                assert_eq!(object_name.to_string(), "test_db".to_string());
                assert_eq!(if_exist.to_string(), "true".to_string());
            }
            _ => panic!("failed"),
        }

        let sql = "recover tenant test_tt";
        let statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            ExtStatement::RecoverTenant(RecoverTenant {
                object_name,
                if_exist,
            }) => {
                assert_eq!(object_name.to_string(), "test_tt".to_string());
                assert_eq!(if_exist.to_string(), "false".to_string());
            }
            _ => panic!("failed"),
        }

        let sql = "recover tenant if exists test_tt";
        let statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            ExtStatement::RecoverTenant(RecoverTenant {
                object_name,
                if_exist,
            }) => {
                assert_eq!(object_name.to_string(), "test_tt".to_string());
                assert_eq!(if_exist.to_string(), "true".to_string());
            }
            _ => panic!("failed"),
        }
    }

    #[test]
    fn test_create_table_without_tags() {
        let sql = "CREATE TABLE test(column1 BIGINT);";
        let statement1 = ExtParser::parse_sql(sql).unwrap().pop_front().unwrap();
        assert_eq!(
            statement1,
            ExtStatement::CreateTable(CreateTable {
                name: ObjectName(vec!["test".into()]),
                if_not_exists: false,
                columns: vec![ColumnOption {
                    name: "column1".into(),
                    is_tag: false,
                    data_type: DataType::BigInt(None),
                    encoding: None
                }]
            })
        );

        let sql = "CREATE TABLE test(column1 BIGINT,);";
        ExtParser::parse_sql(sql).err().unwrap();
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
                let expected = CreateDatabase {
                    name: Ident {
                        value: "test".to_string(),
                        quote_style: None,
                    },
                    if_not_exists: false,
                    options: DatabaseOptions {
                        ttl: Some("10d".to_string()),
                        shard_num: Some(5),
                        vnode_duration: Some("3d".to_string()),
                        replica: Some(10),
                    },
                    config: DatabaseConfig {
                        precision: Some("us".to_string()),
                        max_memcache_size: None,
                        memcache_partitions: None,
                        wal_max_file_size: None,
                        wal_sync: None,
                        strict_write: None,
                        max_cache_readers: None,
                    },
                };
                assert_eq!(stmt.as_ref(), &expected);
            }
            _ => panic!("impossible"),
        }
    }

    #[test]
    fn test_create_database0() {
        let sql = "create database test with ttl 'inf' shard 6 vnode_duration '730.5d' replica 1 precision 'us' max_memcache_size '128MiB' memcache_partitions 10 wal_max_file_size '300M' wal_sync 'true' strict_write 'true' max_cache_readers 100;";
        let statements = ExtParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        match statements[0] {
            ExtStatement::CreateDatabase(ref stmt) => {
                let expected = CreateDatabase {
                    name: Ident {
                        value: "test".to_string(),
                        quote_style: None,
                    },
                    if_not_exists: false,
                    options: DatabaseOptions {
                        ttl: Some("inf".to_string()),
                        shard_num: Some(6),
                        vnode_duration: Some("730.5d".to_string()),
                        replica: Some(1),
                    },
                    config: DatabaseConfig {
                        precision: Some("us".to_string()),
                        max_memcache_size: Some("128MiB".to_string()),
                        memcache_partitions: Some(10),
                        wal_max_file_size: Some("300M".to_string()),
                        wal_sync: Some("true".to_string()),
                        strict_write: Some("true".to_string()),
                        max_cache_readers: Some(100),
                    },
                };
                assert_eq!(stmt.as_ref(), &expected);
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
    fn test_replica_sql() {
        let sql1 = "replica add replica_id 111 node_id 2001;";
        let statement = ExtParser::parse_sql(sql1).unwrap();
        assert_eq!(
            statement[0],
            ExtStatement::ReplicaAdd(ast::ReplicaAdd {
                replica_id: 111,
                node_id: 2001,
            })
        );

        let sql1 = "replica remove replica_id 111 node_id 2001;";
        let statement = ExtParser::parse_sql(sql1).unwrap();
        assert_eq!(
            statement[0],
            ExtStatement::ReplicaRemove(ast::ReplicaRemove {
                replica_id: 111,
                node_id: 2001,
            })
        );

        let sql1 = "replica promote replica_id 111 node_id 2001;";
        let statement = ExtParser::parse_sql(sql1).unwrap();
        assert_eq!(
            statement[0],
            ExtStatement::ReplicaPromote(ast::ReplicaPromote {
                replica_id: 111,
                node_id: 2001,
            })
        );

        let sql1 = "replica destory replica_id 111;";
        let statement = ExtParser::parse_sql(sql1).unwrap();
        assert_eq!(
            statement[0],
            ExtStatement::ReplicaDestroy(ast::ReplicaDestroy { replica_id: 111 })
        );

        let sql1 = "show replicas;";
        let statement = ExtParser::parse_sql(sql1).unwrap();
        assert_eq!(statement[0], ExtStatement::ShowReplicas);
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

    #[test]
    fn test_alter_table_rename_column() {
        let statement = parse_sql("ALTER TABLE TskvTable RENAME COLUMN tag1 to tag2;");

        match statement {
            ExtStatement::AlterTable(AlterTable {
                table_name,
                alter_action:
                    AlterTableAction::RenameColumn {
                        old_column_name,
                        new_column_name,
                    },
            }) => {
                assert_eq!("TskvTable", &table_name.to_string());
                assert_eq!(Ident::from("tag1"), old_column_name);
                assert_eq!(Ident::from("tag2"), new_column_name);
            }
            _ => panic!("expect RenameColumn"),
        }
    }

    #[test]
    fn test_update() {
        let statement = parse_sql("UPDATE TskvTable SET tag1 = '1' WHERE tag2 = '2';");

        match statement {
            ExtStatement::SqlStatement(ast) => match ast.as_ref() {
                Statement::Update {
                    table,
                    assignments,
                    from,
                    returning,
                    ..
                } => {
                    assert_eq!("TskvTable", &table.to_string());
                    assert_eq!(1, assignments.len());
                    assert_eq!(Ident::from("tag1"), assignments[0].id[0]);
                    assert_eq!(
                        Expr::Value(Value::SingleQuotedString("1".into())),
                        assignments[0].value
                    );
                    assert!(from.is_none());
                    assert!(returning.is_none());
                }
                _ => {
                    panic!("expect Update")
                }
            },
            _ => panic!("expect RenameColumn"),
        }
    }

    #[test]
    fn test_create_tenant() {
        let sql = "create tenant test_tenant with drop_after='10d', comment='test tenant',
        object_config        max_users_number = 1
                            max_databases = 3
                            max_shard_number = 2
                            max_replicate_number = 2 
                            max_retention_time = 30,
        coord_data_in              remote_max = 10000
                            remote_initial = 0
                            remote_refill = 10000
                            remote_interval = 100
                            local_max = 10000
                            local_initial = 0,
        coord_data_out             remote_max = 100
                            remote_initial = 0
                            remote_refill = 100
                            remote_interval = 100
                            local_max = 100
                            local_initial = 0;";
        let statements = ExtParser::parse_sql(sql).unwrap();
        println!("{:?}", statements);
        assert_eq!(statements.len(), 1);
        match statements[0] {
            ExtStatement::CreateTenant(ref stmt) => {
                let expected = CreateTenant {
                    name: Ident {
                        value: "test_tenant".to_string(),
                        quote_style: None,
                    },
                    if_not_exists: false,
                    with_options: vec![
                        SqlOption {
                            name: "drop_after".into(),
                            value: Value::SingleQuotedString("10d".to_string()),
                        },
                        SqlOption {
                            name: "comment".into(),
                            value: Value::SingleQuotedString("test tenant".to_string()),
                        },
                        SqlOption {
                            name: "_limiter".into(),
                            value: Value::SingleQuotedString(
                                serde_json::json!({
                                        "object_config": {
                                            "max_users_number": 1,
                                            "max_databases": 3,
                                            "max_shard_number": 2,
                                            "max_replicate_number": 2,
                                            "max_retention_time": 30
                                        },
                                        "request_config": {
                                            "coord_data_in": {
                                    "remote_bucket": {
                                        "max": 10000,
                                        "initial": 0,
                                        "refill": 10000,
                                        "interval": 100
                                    },
                                    "local_bucket": {
                                        "max": 10000,
                                        "initial": 0
                                    }
                                },
                                "coord_data_out": {
                                    "remote_bucket": {
                                        "max": 100,
                                        "initial": 0,
                                        "refill": 100,
                                        "interval": 100
                                    },
                                    "local_bucket": {
                                        "max": 100,
                                        "initial": 0
                                    }
                                }
                                        }
                                    })
                                .to_string(),
                            ),
                        },
                    ],
                };
                if stmt != &expected {
                    println!("Actual: {:?}", stmt);
                }
                assert_eq!(stmt, &expected);
            }
            _ => panic!("impossible"),
        }
    }
}
