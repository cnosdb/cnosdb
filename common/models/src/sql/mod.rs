use std::str::FromStr;

use datafusion::datasource::file_format::file_type::{FileCompressionType, FileType};

use crate::arrow::arrow_data_type_to_sql_data_type;
use crate::auth::role::CustomTenantRole;
use crate::auth::user::UserDesc;
use crate::codec::Encoding;
use crate::datafusion::SqlParserValue;
use crate::errors::DumpSnafu;
use crate::oid::{Identifier, Oid};
use crate::schema::database_schema::DatabaseSchema;
use crate::schema::external_table_schema::ExternalTableSchema;
use crate::schema::stream_table_schema::StreamTable;
use crate::schema::table_schema::TableSchema;
use crate::schema::tenant::Tenant;
use crate::schema::tskv_table_schema::{ColumnType, TskvTableSchema};
use crate::schema::utils::CnosByteNumber;
use crate::ModelError;

type Result<T, E = ModelError> = std::result::Result<T, E>;

pub fn sql_option_to_sql_str(opts: Vec<Option<(&str, SqlParserValue)>>) -> String {
    opts.into_iter()
        .flatten()
        .map(|(k, v)| format!("{k}={}", v))
        .collect::<Vec<_>>()
        .join(", ")
}

pub trait ToDDLSql {
    fn to_ddl_sql(&self, if_not_exists: bool) -> Result<String>;
}

// CREATE TENANT
impl ToDDLSql for Tenant {
    fn to_ddl_sql(&self, if_not_exists: bool) -> Result<String> {
        let mut res = String::new();
        res.push_str("create tenant ");
        if if_not_exists {
            res.push_str("if not exists ");
        }
        res.push_str(format!("\"{}\" ", self.name()).as_str());

        let option = self.options();
        let mut sql_opts = Vec::new();
        let comment = option
            .comment
            .as_ref()
            .map(|a| ("comment", SqlParserValue::SingleQuotedString(a.to_string())));
        let drop_after = option.get_drop_after().as_ref().map(|time| {
            (
                "drop_after",
                SqlParserValue::SingleQuotedString(time.to_string()),
            )
        });
        let limit = option
            .limiter_config
            .as_ref()
            .map(|a| {
                // Safety
                serde_json::to_string(&a)
                    .map_err(|e| {
                        DumpSnafu {
                            msg: format!("dump tenant limiter faile: {}", e),
                        }
                        .build()
                    })
                    .map(|config| ("_limiter", SqlParserValue::SingleQuotedString(config)))
            })
            .transpose()?;
        sql_opts.push(comment);
        sql_opts.push(drop_after);
        sql_opts.push(limit);
        let str = sql_option_to_sql_str(sql_opts);
        if !str.is_empty() {
            res.push_str("with ");
            res.push_str(str.as_str())
        }
        res = res.trim().to_string() + ";";
        Ok(res)
    }
}

// CREATE USER
impl ToDDLSql for UserDesc {
    fn to_ddl_sql(&self, if_not_exists: bool) -> Result<String> {
        let mut res = String::new();
        res.push_str("create user ");
        if if_not_exists {
            res.push_str("if not exists ")
        }
        res.push_str(format!("\"{}\" ", self.name()).as_str());

        let option = self.options();
        let hash_password = option.hash_password().map(|p| {
            (
                "hash_password",
                SqlParserValue::SingleQuotedString(p.to_string()),
            )
        });
        let comment = option
            .comment()
            .map(|c| ("comment", SqlParserValue::SingleQuotedString(c.to_string())));
        let must_change_password = option
            .must_change_password()
            .map(|v| ("must_change_password", SqlParserValue::Boolean(v)));
        let rsa_public_key = option.rsa_public_key().map(|v| {
            (
                "rsa_public_key",
                SqlParserValue::SingleQuotedString(v.to_string()),
            )
        });
        let granted_admin = option
            .granted_admin()
            .map(|v| ("granted_admin", SqlParserValue::Boolean(v)));

        let sql_opts = vec![
            hash_password,
            comment,
            must_change_password,
            rsa_public_key,
            granted_admin,
        ];
        let opt_sql = sql_option_to_sql_str(sql_opts);
        if !opt_sql.is_empty() {
            res.push_str("with ");
            res.push_str(opt_sql.as_str());
        }
        res.push(';');
        Ok(res)
    }
}

// CREATE DATABASE
impl ToDDLSql for DatabaseSchema {
    fn to_ddl_sql(&self, if_not_exists: bool) -> Result<String> {
        let mut res = String::new();
        res.push_str("create database ");
        if if_not_exists {
            res.push_str("if not exists ");
        }
        res.push_str(format!("\"{}\" ", self.database_name()).as_str());
        res.push_str("with ");

        res.push_str(
            format!(
                "precision {} ",
                SqlParserValue::SingleQuotedString(self.config.precision().to_string())
            )
            .as_str(),
        );
        res.push_str(
            format!(
                "max_memcache_size '{}' ",
                CnosByteNumber::format_bytes(self.config.max_memcache_size())
            )
            .as_str(),
        );
        res.push_str(
            format!("memcache_partitions {} ", self.config.memcache_partitions()).as_str(),
        );
        res.push_str(
            format!(
                "wal_max_file_size '{}' ",
                CnosByteNumber::format_bytes(self.config.wal_max_file_size())
            )
            .as_str(),
        );
        res.push_str(format!("wal_sync '{}' ", self.config.wal_sync()).as_str());
        res.push_str(format!("strict_write '{}' ", self.config.strict_write()).as_str());
        res.push_str(format!("max_cache_readers {} ", self.config.max_cache_readers()).as_str());
        res.push_str(format!("ttl '{}' ", self.options.ttl()).as_str());
        res.push_str(format!("shard {} ", self.options.shard_num()).as_str());
        res.push_str(format!("replica {} ", self.options.replica()).as_str());
        res.push_str(format!("vnode_duration '{}' ", self.options.vnode_duration()).as_str());

        if res.trim().ends_with("with") {
            res = res.trim().trim_end_matches("with").trim().to_string();
        }
        res = res.trim().to_string();

        res.push(';');
        Ok(res)
    }
}

// CREATE ROLE
impl ToDDLSql for CustomTenantRole<Oid> {
    fn to_ddl_sql(&self, if_not_exists: bool) -> Result<String> {
        let sql = if if_not_exists {
            match self.inherit_role() {
                Some(role) => format!(
                    "create role if not exists \"{}\" inherit {};",
                    self.name(),
                    role
                ),
                None => format!("create role if not exists \"{}\";", self.name()),
            }
        } else {
            match self.inherit_role() {
                Some(role) => format!("create role \"{}\" inherit {};", self.name(), role),
                None => format!("create role \"{}\";", self.name()),
            }
        };
        Ok(sql)
    }
}

pub fn role_to_sql(role: &CustomTenantRole<Oid>) -> Result<Vec<String>> {
    let mut res = vec![];
    let role_sql = role.to_ddl_sql(false)?;
    res.push(role_sql);
    res.append(&mut privilege_to_sql(role));
    Ok(res)
}

// GRANT privilege
pub fn privilege_to_sql(role: &CustomTenantRole<Oid>) -> Vec<String> {
    let privileges = role.additional_privileges();
    privileges
        .iter()
        .map(|(d, p)| {
            format!(
                "grant {} on database \"{}\" to \"{}\";",
                p.as_str(),
                d,
                role.name()
            )
        })
        .collect()
}

// Add member
pub fn add_member_to_sql(tenant_name: &str, user: &str, role: &str) -> String {
    format!(
        "alter tenant \"{}\" add user \"{}\" as \"{}\";",
        tenant_name, user, role
    )
}

// CREATE TABLES
pub fn create_table_sqls(tables: &[TableSchema], if_not_exists: bool) -> Result<Vec<String>> {
    // first create ts table
    let mut res = vec![];
    let mut ts_table = vec![];
    let mut ex_table = vec![];
    let mut stream_table = vec![];
    for table in tables {
        match table {
            TableSchema::TsKvTableSchema(t) => ts_table.push(t),
            TableSchema::ExternalTableSchema(t) => ex_table.push(t),
            TableSchema::StreamTableSchema(s) => stream_table.push(s),
        }
    }

    for ts in ts_table.into_iter() {
        res.push(ts.to_ddl_sql(if_not_exists)?)
    }

    for ex in ex_table.into_iter() {
        res.push(ex.to_ddl_sql(if_not_exists)?);
    }

    for stream in stream_table.into_iter() {
        res.push(stream.to_ddl_sql(if_not_exists)?)
    }

    Ok(res)
}

// CREATE TABLE
impl ToDDLSql for TableSchema {
    fn to_ddl_sql(&self, if_not_exists: bool) -> Result<String> {
        match self {
            TableSchema::TsKvTableSchema(t) => t.to_ddl_sql(if_not_exists),
            TableSchema::ExternalTableSchema(t) => t.to_ddl_sql(if_not_exists),
            TableSchema::StreamTableSchema(t) => t.to_ddl_sql(if_not_exists),
        }
    }
}

// CREATE TS TABLE
impl ToDDLSql for TskvTableSchema {
    fn to_ddl_sql(&self, if_not_exists: bool) -> Result<String> {
        let mut res = String::new();
        res.push_str("create table ");
        if if_not_exists {
            res.push_str("if not exists ");
        }
        res.push_str(format!("\"{}\".\"{}\" ", &self.db, &self.name).as_str());
        res.push('(');
        self.columns()
            .iter()
            .filter(|c| c.column_type.is_field())
            .for_each(|c| {
                if let ColumnType::Field(v_t) = c.column_type {
                    res.push_str(format!("\"{}\" {}", c.name, v_t.to_sql_type_str()).as_str())
                }

                if c.encoding != Encoding::Default {
                    res.push_str(format!(" CODEC({})", c.encoding.as_str()).as_str());
                }
                res.push_str(", ");
            });

        let tags = self
            .columns()
            .iter()
            .filter(|c| c.column_type.is_tag())
            .map(|c| format!("\"{}\"", c.name))
            .collect::<Vec<_>>()
            .join(", ");
        res.push_str(format!("tags ({})", tags).as_str());
        res.push_str(");");
        Ok(res)
    }
}

// CREATE EXTERNAL TABLE
impl ToDDLSql for ExternalTableSchema {
    fn to_ddl_sql(&self, if_not_exists: bool) -> Result<String> {
        let mut res = String::new();
        res.push_str("create external table ");
        if if_not_exists {
            res.push_str("if not exists ")
        }
        res.push_str(format!("\"{}\".\"{}\" ", self.db, self.name).as_str());
        let file_type = FileType::from_str(&self.file_type)
            .map_err(|e| DumpSnafu { msg: e.to_string() }.build())?;
        if file_type.ne(&FileType::PARQUET) {
            let columns = self
                .schema
                .fields
                .iter()
                .map(|f| {
                    let data_type = arrow_data_type_to_sql_data_type(f.data_type())?;
                    let res = format!("\"{}\" {}", f.name(), data_type,);
                    Ok(res)
                })
                .collect::<Result<Vec<_>, crate::ModelError>>()?
                .join(", ");
            res.push('(');
            res.push_str(columns.as_str());
            res.push_str(") ");
        }

        res.push_str(format!("stored as {} ", self.file_type).as_str());
        if file_type.eq(&FileType::CSV) {
            if self.has_header {
                res.push_str("with header row ");
            }
            res.push_str(
                format!(
                    "delimiter {} ",
                    SqlParserValue::SingleQuotedString((self.delimiter as char).to_string())
                )
                .as_str(),
            )
        }

        if matches!(file_type, FileType::CSV | FileType::JSON) {
            let compression_type = FileCompressionType::from_str(&self.file_compression_type)
                .map_err(|e| DumpSnafu { msg: e.to_string() }.build())?;

            if compression_type.eq(&FileCompressionType::GZIP) {
                res.push_str("compression type gzip")
            } else if compression_type.eq(&FileCompressionType::XZ) {
                res.push_str("compression type xz")
            } else if compression_type.eq(&FileCompressionType::BZIP2) {
                res.push_str("compression type bzip2")
            } else if compression_type.eq(&FileCompressionType::ZSTD) {
                res.push_str("compression type zstd")
            }
        }

        let metadata = self.schema.metadata();

        if !metadata.is_empty() {
            res.push_str("options ");
            let options = self
                .schema
                .metadata()
                .iter()
                .map(|(k, v)| {
                    format!(
                        "{} {}",
                        SqlParserValue::SingleQuotedString(k.to_owned()),
                        SqlParserValue::SingleQuotedString(v.to_owned())
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            res.push('(');
            res.push_str(options.as_str());
            res.push(')');
        }

        res.push_str(
            format!(
                "location {};",
                SqlParserValue::SingleQuotedString(self.location.to_string())
            )
            .as_str(),
        );

        Ok(res)
    }
}

// CREATE STREAM TABLE
impl ToDDLSql for StreamTable {
    fn to_ddl_sql(&self, if_not_exists: bool) -> Result<String> {
        let mut res = String::new();
        res.push_str("create stream table ");
        if if_not_exists {
            res.push_str("if not exists ")
        }
        res.push_str(format!("\"{}\".\"{}\" ", self.db(), self.name()).as_str());

        let columns_def = self.schema().fields.clone();
        if !columns_def.is_empty() {
            let columns = self
                .schema()
                .fields
                .iter()
                .map(|f| {
                    let datatype = arrow_data_type_to_sql_data_type(f.data_type())?;
                    let res = format!("\"{}\" {}", f.name(), datatype,);
                    Ok(res)
                })
                .collect::<Result<Vec<_>, crate::ModelError>>()?
                .join(", ");
            res.push('(');
            res.push_str(columns.as_str());
            res.push_str(") ");
        }

        res.push_str("with (");
        let options = self.extra_options();
        let db = options
            .get("db")
            .ok_or_else(|| {
                DumpSnafu {
                    msg: format!("couldn't found option 'db' of stream table {}", self.name()),
                }
                .build()
            })?
            .as_str();

        let table = options
            .get("table")
            .ok_or_else(|| {
                DumpSnafu {
                    msg: format!(
                        "couldn't found option 'table' of stream table {}",
                        self.name()
                    ),
                }
                .build()
            })?
            .as_str();
        let event_time_column = self.watermark().column.as_str();
        let watermark_delay = self.watermark().delay;

        let mut options = vec![
            format!("db={}", SqlParserValue::SingleQuotedString(db.to_string())),
            format!(
                "table={}",
                SqlParserValue::SingleQuotedString(table.to_string())
            ),
            format!(
                "event_time_column={}",
                SqlParserValue::SingleQuotedString(event_time_column.to_string())
            ),
        ];

        if watermark_delay.as_millis() > 0 {
            let watermark_delay_str = format!("{}ms", watermark_delay.as_millis());
            options.push(format!(
                "watermark_delay={})",
                SqlParserValue::SingleQuotedString(watermark_delay_str)
            ));
        }

        res.push_str(options.join(", ").as_str());
        res.push_str(") ");

        res.push_str("engine = ");
        res.push_str(self.stream_type());
        res.push(';');
        Ok(res)
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
    use config::common::TenantLimiterConfig;

    use crate::auth::user::{UserDesc, UserOptionsBuilder};
    use crate::schema::database_schema::{
        DatabaseConfig, DatabaseOptions, DatabaseSchema, Precision,
    };
    use crate::schema::external_table_schema::ExternalTableSchema;
    use crate::schema::stream_table_schema::{StreamTable, Watermark};
    use crate::schema::tenant::{Tenant, TenantOptionsBuilder};
    use crate::schema::tskv_table_schema::{ColumnType, TableColumn, TskvTableSchema};
    use crate::schema::utils::CnosDuration;
    use crate::sql::ToDDLSql;
    use crate::ValueType;

    #[test]
    fn test_create_tenant() {
        let config_str = r#"
[object_config]
# add user limit
max_users_number = 1
# create database limit
max_databases = 3
max_shard_number = 2
max_replicate_number = 2
max_retention_time = 30


[request_config.coord_data_in]
local_bucket = {max = 100, initial = 0}
remote_bucket = {max = 100, initial = 0, refill = 100, interval = 100}


[request_config.coord_data_out]
local_bucket = {max = 100, initial = 0}
remote_bucket = {max = 100, initial = 0, refill = 100, interval = 100}

[request_config.coord_data_writes]
local_bucket = {max = 100, initial = 0}
remote_bucket = {max = 100, initial = 0, refill = 100, interval = 100}

[request_config.coord_data_queries]
local_bucket = {max = 100, initial = 0}
remote_bucket = {max = 100, initial = 0, refill = 100, interval = 100}

[request_config.http_data_in]
local_bucket = {max = 100, initial = 0}
remote_bucket = {max = 100, initial = 0, refill = 100, interval = 100}

[request_config.http_data_out]
local_bucket = {max = 100, initial = 0}
remote_bucket = {max = 100, initial = 0, refill = 100, interval = 100}

[request_config.http_queries]
local_bucket = {max = 100, initial = 0}
remote_bucket = {max = 100, initial = 0, refill = 100, interval = 100}

[request_config.http_writes]
local_bucket = {max = 100, initial = 0}
remote_bucket = {max = 100, initial = 0, refill = 100, interval = 100}
"#;

        let config: TenantLimiterConfig = toml::from_str(config_str).unwrap();

        let opts = TenantOptionsBuilder::default()
            .comment("test")
            .limiter_config(config)
            .build()
            .unwrap();

        let tenant = Tenant::new(0, "hello".to_string(), opts);
        assert_eq!(
            tenant.to_ddl_sql(true).unwrap(),
            r#"create tenant if not exists "hello" with comment='test', _limiter='{"object_config":{"max_users_number":1,"max_databases":3,"max_shard_number":2,"max_replicate_number":2,"max_retention_time":30},"request_config":{"coord_data_in":{"remote_bucket":{"max":100,"initial":0,"refill":100,"interval":100},"local_bucket":{"max":100,"initial":0}},"coord_data_out":{"remote_bucket":{"max":100,"initial":0,"refill":100,"interval":100},"local_bucket":{"max":100,"initial":0}},"coord_queries":null,"coord_writes":null,"http_data_in":{"remote_bucket":{"max":100,"initial":0,"refill":100,"interval":100},"local_bucket":{"max":100,"initial":0}},"http_data_out":{"remote_bucket":{"max":100,"initial":0,"refill":100,"interval":100},"local_bucket":{"max":100,"initial":0}},"http_queries":{"remote_bucket":{"max":100,"initial":0,"refill":100,"interval":100},"local_bucket":{"max":100,"initial":0}},"http_writes":{"remote_bucket":{"max":100,"initial":0,"refill":100,"interval":100},"local_bucket":{"max":100,"initial":0}}}}';"#
        )
    }

    #[test]
    fn test_create_user() {
        let user_option = UserOptionsBuilder::default()
            .hash_password("123")
            .granted_admin(true)
            .must_change_password(true)
            .comment("test")
            .rsa_public_key("aaa")
            .build()
            .unwrap();
        let desc = UserDesc::new(0_u128, "test".to_string(), user_option, true);

        let sql = desc.to_ddl_sql(false).unwrap();
        assert_eq!(
            sql,
            r#"create user "test" with hash_password='123', comment='test', must_change_password=true, rsa_public_key='aaa', granted_admin=true;"#
        )
    }

    #[test]
    fn test_create_database() {
        let db_option = DatabaseOptions::new(
            CnosDuration::new_with_day(1),
            3,
            CnosDuration::new("50h").unwrap(),
            1,
        );
        let db_config = DatabaseConfig::new(
            Precision::US,
            512 * 1024 * 1024 * 1024,
            12,
            1024,
            false,
            true,
            32,
        );
        let db = DatabaseSchema::new("test", "test", db_option, Arc::new(db_config));
        assert_eq!(
            format!("{}", db.to_ddl_sql(true).unwrap()),
            r#"create database if not exists "test" with precision 'US' max_memcache_size '512 GiB' memcache_partitions 12 wal_max_file_size '1 KiB' wal_sync 'false' strict_write 'true' max_cache_readers 32 ttl '1day' shard 3 replica 1 vnode_duration '2days 2h';"#
        );
    }

    #[test]
    fn test_create_ts_table() {
        let schema = TskvTableSchema::new(
            "test_tenant".to_string(),
            "test".to_string(),
            "test_table".to_string(),
            vec![
                TableColumn::new_time_column(1, TimeUnit::Nanosecond),
                TableColumn::new_tag_column(2, "tag_col_1".to_string()),
                TableColumn::new_tag_column(3, "tag_col_2".to_string()),
                TableColumn::new(
                    4,
                    "f_col_1".to_string(),
                    ColumnType::Field(ValueType::Float),
                    Default::default(),
                ),
            ],
        );
        assert_eq!(
            schema.to_ddl_sql(false).unwrap(),
            r#"create table "test"."test_table" ("f_col_1" DOUBLE, tags ("tag_col_1", "tag_col_2"));"#
        );
    }

    #[test]
    fn test_create_ex_table() {
        let schema = ExternalTableSchema {
            tenant: "".to_string(),
            db: "test".to_string(),
            name: "nation".to_string(),
            file_compression_type: "".to_string(),
            file_type: "csv".to_string(),
            location: "query_server/sqllogicaltests/resource/tpch-csv/nation.csv".to_string(),
            target_partitions: 0,
            table_partition_cols: vec![],
            has_header: true,
            delimiter: b',',
            schema: Schema {
                fields: Fields::from(vec![
                    Field::new("n_nationkey", DataType::UInt64, true),
                    Field::new("n_name", DataType::Utf8, true),
                    Field::new("n_regionkey", DataType::Int64, true),
                    Field::new("n_comment", DataType::Utf8, true),
                ]),
                metadata: Default::default(),
            },
        };
        assert_eq!(
            schema.to_ddl_sql(false).unwrap(),
            r#"create external table "test"."nation" ("n_nationkey" BIGINT UNSIGNED, "n_name" STRING, "n_regionkey" BIGINT, "n_comment" STRING) stored as csv with header row delimiter ',' location 'query_server/sqllogicaltests/resource/tpch-csv/nation.csv';"#
        )
    }

    #[test]
    fn create_stream_table() {
        let stream_table = StreamTable::new(
            "",
            "test",
            "test_stream",
            SchemaRef::new(Schema::new(Fields::from(vec![
                Field::new("visibility", DataType::Float64, true),
                Field::new("temperature", DataType::Float64, true),
                Field::new("pressure", DataType::Float64, true),
                Field::new("station", DataType::Utf8, true),
            ]))),
            "tskv",
            Watermark {
                column: "time".to_string(),
                delay: std::time::Duration::from_millis(0),
            },
            {
                let mut res = HashMap::new();
                res.insert("db".to_string(), "test".to_string());
                res.insert("table".to_string(), "air".to_string());
                res
            },
        );
        assert_eq!(
            stream_table.to_ddl_sql(false).unwrap(),
            r#"create stream table "test"."test_stream" ("visibility" DOUBLE, "temperature" DOUBLE, "pressure" DOUBLE, "station" STRING) with (db='test', table='air', event_time_column='time') engine = tskv;"#
        );
    }
}
