use std::str::FromStr;

use datafusion::datasource::file_format::file_type::{FileCompressionType, FileType};

use crate::arrow::arrow_data_type_to_sql_data_type;
use crate::auth::role::CustomTenantRole;
use crate::auth::user::UserDesc;
use crate::oid::{Identifier, Oid};
use crate::schema::{
    ColumnType, DatabaseSchema, DurationUnit, ExternalTableSchema, StreamTable, TableSchema,
    Tenant, TskvTableSchema,
};
use crate::{Error, SqlParserValue};

type Result<T, E = Error> = std::result::Result<T, E>;

pub fn sql_option_to_sql_str(opts: Vec<Option<(&str, SqlParserValue)>>) -> String {
    opts.into_iter()
        .flatten()
        .map(|(k, v)| format!("{k}={}", v))
        .collect::<Vec<_>>()
        .join(", ")
}

// CREATE TENANT
pub fn tenant_to_sql(tenant: &Tenant) -> Result<String> {
    let mut res = String::new();
    res.push_str("create tenant if not exists ");
    res.push_str(format!("\"{}\" ", tenant.name()).as_str());

    let option = tenant.options();
    let mut sql_opts = Vec::new();
    let comment = option
        .comment
        .as_ref()
        .map(|a| ("comment", SqlParserValue::SingleQuotedString(a.to_string())));
    let limit = option
        .limiter_config
        .as_ref()
        .map(|a| {
            // Safety
            serde_json::to_string(&a)
                .map_err(|e| Error::DumpError {
                    msg: format!("dump tenant limiter faile: {}", e),
                })
                .map(|config| ("_limiter", SqlParserValue::SingleQuotedString(config)))
        })
        .transpose()?;
    sql_opts.push(comment);
    sql_opts.push(limit);
    let str = sql_option_to_sql_str(sql_opts);
    if !str.is_empty() {
        res.push_str("with ");
        res.push_str(str.as_str())
    }
    res = res.trim().to_string() + ";";
    Ok(res)
}

// CREATE USER
pub fn user_to_sql(user: &UserDesc, if_not_exists: bool) -> String {
    let mut res = String::new();
    res.push_str("create user ");
    if if_not_exists {
        res.push_str("if not exists ")
    }
    res.push_str(format!("\"{}\" ", user.name()).as_str());

    let option = user.options();
    let hash_password = option.password().map(|p| {
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
    res
}

// CREATE DATABASE
pub fn database_to_sql(database: &DatabaseSchema) -> String {
    let mut res = String::new();
    res.push_str("create database if not exists ");
    res.push_str(format!("\"{}\" ", database.database_name()).as_str());
    res.push_str("with ");
    if let Some(p) = database.config.precision() {
        res.push_str(
            format!(
                "precision {} ",
                SqlParserValue::SingleQuotedString(p.to_string())
            )
            .as_str(),
        );
    };

    if let Some(ttl) = database.config.ttl() {
        let unit = match ttl.unit {
            DurationUnit::Minutes => Some("M"),
            DurationUnit::Hour => Some("H"),
            DurationUnit::Day => Some("D"),
            DurationUnit::Inf => None,
        };
        if let Some(u) = unit {
            res.push_str(format!("ttl '{}{}' ", ttl.time_num, u).as_str())
        }
    }

    if let Some(shard) = database.config.shard_num() {
        res.push_str(format!("shard {} ", shard).as_str())
    }

    if let Some(rep) = database.config.replica() {
        res.push_str(format!("replica {} ", rep).as_str())
    }

    if let Some(d) = database.config.vnode_duration() {
        let unit = match d.unit {
            DurationUnit::Minutes => Some("M"),
            DurationUnit::Hour => Some("H"),
            DurationUnit::Day => Some("D"),
            DurationUnit::Inf => None,
        };
        if let Some(u) = unit {
            res.push_str(format!("vnode_duration '{}{}' ", d.time_num, u).as_str())
        }
    }

    if res.trim().ends_with("with") {
        res = res.trim().trim_end_matches("with").trim().to_string();
    }
    res = res.trim().to_string();

    res.push(';');
    res
}

// CREATE ROLE
pub fn role_to_sql(role: &CustomTenantRole<Oid>) -> Vec<String> {
    let mut res = vec![];
    let role_sql = format!(
        "create role \"{}\" inherit {};",
        role.name(),
        role.inherit_role()
    );
    res.push(role_sql);
    res.append(&mut privilege_to_sql(role));
    res
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

// CREATE TABLE
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
        res.push(create_ts_table_sql(ts, if_not_exists))
    }

    for ex in ex_table.into_iter() {
        res.push(create_external_table_sql(ex, if_not_exists)?);
    }

    for stream in stream_table.into_iter() {
        res.push(create_stream_table_sql(stream, if_not_exists)?)
    }

    Ok(res)
}

// CREATE TS TABLE
pub fn create_ts_table_sql(tskv_table: &TskvTableSchema, if_not_exists: bool) -> String {
    let mut res = String::new();
    res.push_str("create table ");
    if if_not_exists {
        res.push_str("if not exists ");
    }
    res.push_str(format!("\"{}\".\"{}\" ", &tskv_table.db, &tskv_table.name).as_str());
    res.push('(');
    tskv_table
        .columns()
        .iter()
        .filter(|c| c.column_type.is_field())
        .for_each(|c| {
            if let ColumnType::Field(v_t) = c.column_type {
                res.push_str(format!("\"{}\" {}, ", c.name, v_t.to_sql_type_str()).as_str())
            }
        });

    let tags = tskv_table
        .columns()
        .iter()
        .filter(|c| c.column_type.is_tag())
        .map(|c| format!("\"{}\"", c.name))
        .collect::<Vec<_>>()
        .join(", ");
    res.push_str(format!("tags ({})", tags).as_str());
    res.push_str(");");
    res
}

// CREATE EXTERNAL TABLE
pub fn create_external_table_sql(
    ex_table: &ExternalTableSchema,
    if_not_exists: bool,
) -> Result<String> {
    let mut res = String::new();
    res.push_str("create external table ");
    if if_not_exists {
        res.push_str("if not exists ")
    }
    res.push_str(format!("\"{}\".\"{}\" ", ex_table.db, ex_table.name).as_str());
    let file_type = FileType::from_str(&ex_table.file_type)
        .map_err(|e| Error::DumpError { msg: e.to_string() })?;
    if file_type.ne(&FileType::PARQUET) {
        let columns = ex_table
            .schema
            .fields
            .iter()
            .map(|f| {
                format!(
                    "\"{}\" {}",
                    f.name(),
                    arrow_data_type_to_sql_data_type(f.data_type())
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        res.push('(');
        res.push_str(columns.as_str());
        res.push_str(") ");
    }

    res.push_str(format!("stored as {} ", ex_table.file_type).as_str());
    if file_type.eq(&FileType::CSV) {
        if ex_table.has_header {
            res.push_str("with header row ");
        }
        res.push_str(
            format!(
                "delimiter {} ",
                SqlParserValue::SingleQuotedString((ex_table.delimiter as char).to_string())
            )
            .as_str(),
        )
    }

    if matches!(file_type, FileType::CSV | FileType::JSON) {
        let compression_type = FileCompressionType::from_str(&ex_table.file_compression_type)
            .map_err(|e| Error::DumpError { msg: e.to_string() })?;

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

    let metadata = ex_table.schema.metadata();

    if !metadata.is_empty() {
        res.push_str("options ");
        let options = ex_table
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
            SqlParserValue::SingleQuotedString(ex_table.location.to_string())
        )
        .as_str(),
    );

    Ok(res)
}

// CREATE STREAM TABLE
pub fn create_stream_table_sql(stream_table: &StreamTable, if_not_exists: bool) -> Result<String> {
    let mut res = String::new();
    res.push_str("create stream table ");
    if if_not_exists {
        res.push_str("if not exists ")
    }
    res.push_str(format!("\"{}\".\"{}\" ", stream_table.db(), stream_table.name()).as_str());

    let columns_def = stream_table.schema().fields.clone();
    if !columns_def.is_empty() {
        let columns = stream_table
            .schema()
            .fields
            .iter()
            .map(|f| {
                format!(
                    "\"{}\" {}",
                    f.name(),
                    arrow_data_type_to_sql_data_type(f.data_type())
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        res.push('(');
        res.push_str(columns.as_str());
        res.push_str(") ");
    }

    res.push_str("with (");
    let options = stream_table.extra_options();
    let db = options
        .get("db")
        .ok_or_else(|| Error::DumpError {
            msg: format!(
                "couldn't found option 'db' of stream table {}",
                stream_table.name()
            ),
        })?
        .as_str();

    let table = options
        .get("table")
        .ok_or_else(|| Error::DumpError {
            msg: format!(
                "couldn't found option 'table' of stream table {}",
                stream_table.name()
            ),
        })?
        .as_str();
    let event_time_column = stream_table.watermark().column.as_str();
    let watermark_delay = stream_table.watermark().delay;

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
    res.push_str(stream_table.stream_type());
    res.push(';');
    Ok(res)
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
    use config::TenantLimiterConfig;

    use crate::auth::user::{UserDesc, UserOptionsBuilder};
    use crate::schema::{
        ColumnType, DatabaseOptions, DatabaseSchema, Duration, ExternalTableSchema, Precision,
        StreamTable, TableColumn, Tenant, TenantOptionsBuilder, TskvTableSchema, Watermark,
    };
    use crate::sql::{
        create_external_table_sql, create_stream_table_sql, create_ts_table_sql, database_to_sql,
        tenant_to_sql, user_to_sql,
    };
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
            tenant_to_sql(&tenant).unwrap(),
            r#"create tenant if not exists "hello" with comment='test', _limiter='{"object_config":{"max_users_number":1,"max_databases":3,"max_shard_number":2,"max_replicate_number":2,"max_retention_time":30},"request_config":{"coord_data_in":{"remote_bucket":{"max":100,"initial":0,"refill":100,"interval":100},"local_bucket":{"max":100,"initial":0}},"coord_data_out":{"remote_bucket":{"max":100,"initial":0,"refill":100,"interval":100},"local_bucket":{"max":100,"initial":0}},"coord_queries":null,"coord_writes":null,"http_data_in":{"remote_bucket":{"max":100,"initial":0,"refill":100,"interval":100},"local_bucket":{"max":100,"initial":0}},"http_data_out":{"remote_bucket":{"max":100,"initial":0,"refill":100,"interval":100},"local_bucket":{"max":100,"initial":0}},"http_queries":{"remote_bucket":{"max":100,"initial":0,"refill":100,"interval":100},"local_bucket":{"max":100,"initial":0}},"http_writes":{"remote_bucket":{"max":100,"initial":0,"refill":100,"interval":100},"local_bucket":{"max":100,"initial":0}}}}';"#
        )
    }

    #[test]
    fn test_create_user() {
        let user_option = UserOptionsBuilder::default()
            .password("123")
            .granted_admin(true)
            .must_change_password(true)
            .comment("test")
            .rsa_public_key("aaa")
            .build()
            .unwrap();
        let desc = UserDesc::new(0_u128, "test".to_string(), user_option, true);

        let sql = user_to_sql(&desc, false);
        assert_eq!(
            sql,
            r#"create user "test" with hash_password='123', comment='test', must_change_password=true, rsa_public_key='aaa', granted_admin=true;"#
        )
    }

    #[test]
    fn test_create_database() {
        let db_option = DatabaseOptions::new(
            Some(Duration::new_with_day(1)),
            Some(3),
            Some(Duration::new("50H").unwrap()),
            Some(1),
            Some(Precision::MS),
        );
        let db = DatabaseSchema::new_with_options("test", "test", db_option);
        assert_eq!(
            format!("{}", database_to_sql(&db)),
            r#"create database if not exists "test" with precision 'MS' ttl '1D' shard 3 replica 1 vnode_duration '50H';"#
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
            create_ts_table_sql(&schema, false),
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
            location: "query_server/sqllogicaltests/data/tpch-csv/nation.csv".to_string(),
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
            create_external_table_sql(&schema, false).unwrap(),
            r#"create external table "test"."nation" ("n_nationkey" BIGINT UNSIGNED, "n_name" STRING, "n_regionkey" BIGINT, "n_comment" STRING) stored as csv with header row delimiter ',' location 'query_server/sqllogicaltests/data/tpch-csv/nation.csv';"#
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
            create_stream_table_sql(&stream_table, false).unwrap(),
            r#"create stream table "test"."test_stream" ("visibility" DOUBLE, "temperature" DOUBLE, "pressure" DOUBLE, "station" STRING) with (db='test', table='air', event_time_column='time') engine = tskv;"#
        );
    }
}
