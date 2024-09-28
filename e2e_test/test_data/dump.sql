-- Dump Global Object
create tenant if not exists "tenant_b";
create tenant if not exists "test_cols_tenant1";
create tenant if not exists "test_coord_data_in";
create tenant if not exists "test_dbs_tenant1";
create tenant if not exists "test_dps_tenant";
create tenant if not exists "test_dump_info" with drop_after='1m';
create tenant if not exists "test_ers_tenant1";
create tenant if not exists "test_ms_tenant1";
create tenant if not exists "test_rs_tenant1";
create tenant if not exists "test_tbls_tenant1";
create tenant if not exists "test_ts_tenant1";
create tenant if not exists "test_us_tenant1";
create user "test_au_u1" with must_change_password=false, granted_admin=false;
create user "test_au_u2" with must_change_password=false, granted_admin=true;
create user "test_cdi_u1" with must_change_password=false, granted_admin=false;
create user "test_cols_u1" with must_change_password=false, granted_admin=false;
create user "test_cols_u2" with must_change_password=false, granted_admin=false;
create user "test_dbs_u1" with must_change_password=false, granted_admin=false;
create user "test_dbs_u2" with must_change_password=false, granted_admin=false;
create user "test_dps_u0" with must_change_password=false, granted_admin=false;
create user "test_dps_u1" with must_change_password=false, granted_admin=false;
create user "test_dps_u2" with must_change_password=false, granted_admin=false;
create user "test_dps_u3" with must_change_password=false, granted_admin=false;
create user "test_ers_u1" with must_change_password=false, granted_admin=false;
create user "test_ers_u2" with must_change_password=false, granted_admin=false;
create user "test_ers_u3" with must_change_password=false, granted_admin=false;
create user "test_ms_u1" with must_change_password=false, granted_admin=false;
create user "test_ms_u2" with must_change_password=false, granted_admin=false;
create user "test_rs_u1" with must_change_password=false, granted_admin=false;
create user "test_rs_u2" with must_change_password=false, granted_admin=false;
create user "test_tbls_u1" with must_change_password=false, granted_admin=false;
create user "test_tbls_u2" with must_change_password=false, granted_admin=false;
create user "test_ts_u1" with must_change_password=false, granted_admin=false;
create user "test_ts_u2" with must_change_password=false, granted_admin=false;
create user "test_us_u1" with comment='test comment', must_change_password=false, granted_admin=false;
create user "test_us_u2" with must_change_password=false, granted_admin=false;
create user "user_b" with must_change_password=false, granted_admin=false;
create user "writer" with must_change_password=false, granted_admin=false;
-- Dump Tenant cnosdb Object
\change_tenant cnosdb
create database if not exists "alter_database" with precision 'US' max_memcache_size '512 MiB' memcache_partitions 16 wal_max_file_size '128 MiB' wal_sync 'false' strict_write 'false' max_cache_readers 32 ttl '30days' shard 6 replica 1 vnode_duration '3months 8days 16h 19m 12s';
create database if not exists "alter_table" with precision 'NS' max_memcache_size '512 MiB' memcache_partitions 16 wal_max_file_size '128 MiB' wal_sync 'false' strict_write 'false' max_cache_readers 32 ttl '273years 9months 12days 18h 57m 36s' shard 1 replica 1 vnode_duration '1year';
create database if not exists "avg_daily_driving_duration" with precision 'NS' max_memcache_size '512 MiB' memcache_partitions 16 wal_max_file_size '128 MiB' wal_sync 'false' strict_write 'false' max_cache_readers 32 ttl '9years 11months 27days 21h 50m 24s' shard 1 replica 1 vnode_duration '1year';
create database if not exists "bottom_func" with precision 'NS' max_memcache_size '512 MiB' memcache_partitions 16 wal_max_file_size '128 MiB' wal_sync 'false' strict_write 'false' max_cache_readers 32 ttl '273years 9months 12days 18h 57m 36s' shard 1 replica 1 vnode_duration '1year';
create database if not exists "ci_table_db" with precision 'NS' max_memcache_size '512 MiB' memcache_partitions 16 wal_max_file_size '128 MiB' wal_sync 'false' strict_write 'false' max_cache_readers 32 ttl '9years 11months 27days 21h 50m 24s' shard 1 replica 1 vnode_duration '1year';
create database if not exists "cli_precision" with precision 'MS' max_memcache_size '512 MiB' memcache_partitions 16 wal_max_file_size '128 MiB' wal_sync 'false' strict_write 'false' max_cache_readers 32 ttl '273years 9months 12days 18h 57m 36s' shard 1 replica 1 vnode_duration '1year';
create database if not exists "cluster_schema" with precision 'NS' max_memcache_size '2 MiB' memcache_partitions 16 wal_max_file_size '128 MiB' wal_sync 'false' strict_write 'false' max_cache_readers 32 ttl 'INF' shard 1 replica 1 vnode_duration '1year';
create database if not exists "create_external_table" with precision 'NS' max_memcache_size '512 MiB' memcache_partitions 16 wal_max_file_size '128 MiB' wal_sync 'false' strict_write 'false' max_cache_readers 32 ttl '273years 9months 12days 18h 57m 36s' shard 1 replica 1 vnode_duration '1year';
create database if not exists "createstreamtable" with precision 'NS' max_memcache_size '512 MiB' memcache_partitions 16 wal_max_file_size '128 MiB' wal_sync 'false' strict_write 'false' max_cache_readers 32 ttl 'INF' shard 1 replica 1 vnode_duration '1year';
create database if not exists "db_precision" with precision 'MS' max_memcache_size '512 MiB' memcache_partitions 16 wal_max_file_size '128 MiB' wal_sync 'false' strict_write 'false' max_cache_readers 32 ttl '273years 9months 12days 18h 57m 36s' shard 1 replica 1 vnode_duration '1year';
create database if not exists "describe_database" with precision 'NS' max_memcache_size '512 MiB' memcache_partitions 16 wal_max_file_size '128 MiB' wal_sync 'false' strict_write 'false' max_cache_readers 32 ttl '10days' shard 1 replica 1 vnode_duration '1year';
create database if not exists "empty_table" with precision 'NS' max_memcache_size '512 MiB' memcache_partitions 16 wal_max_file_size '128 MiB' wal_sync 'false' strict_write 'false' max_cache_readers 32 ttl 'INF' shard 1 replica 1 vnode_duration '1year';
create database if not exists "explain_stream_query" with precision 'NS' max_memcache_size '512 MiB' memcache_partitions 16 wal_max_file_size '128 MiB' wal_sync 'false' strict_write 'false' max_cache_readers 32 ttl 'INF' shard 1 replica 1 vnode_duration '1year';
create database if not exists "filter_push_down" with precision 'NS' max_memcache_size '512 MiB' memcache_partitions 16 wal_max_file_size '128 MiB' wal_sync 'false' strict_write 'false' max_cache_readers 32 ttl '273years 9months 12days 18h 57m 36s' shard 1 replica 1 vnode_duration '1year';
create database if not exists "http_stream_select" with precision 'NS' max_memcache_size '512 MiB' memcache_partitions 16 wal_max_file_size '128 MiB' wal_sync 'false' strict_write 'false' max_cache_readers 32 ttl '273years 9months 12days 18h 57m 36s' shard 1 replica 1 vnode_duration '1year';
create database if not exists "limit" with precision 'NS' max_memcache_size '512 MiB' memcache_partitions 16 wal_max_file_size '128 MiB' wal_sync 'false' strict_write 'false' max_cache_readers 32 ttl '273years 9months 12days 18h 57m 36s' shard 1 replica 1 vnode_duration '1year';
create database if not exists "only_tag_col" with precision 'NS' max_memcache_size '512 MiB' memcache_partitions 16 wal_max_file_size '128 MiB' wal_sync 'false' strict_write 'false' max_cache_readers 32 ttl '273years 9months 12days 18h 57m 36s' shard 1 replica 1 vnode_duration '1year';
create database if not exists "only_time_col" with precision 'NS' max_memcache_size '512 MiB' memcache_partitions 16 wal_max_file_size '128 MiB' wal_sync 'false' strict_write 'false' max_cache_readers 32 ttl '273years 9months 12days 18h 57m 36s' shard 1 replica 1 vnode_duration '1year';
create database if not exists "show_series" with precision 'NS' max_memcache_size '512 MiB' memcache_partitions 16 wal_max_file_size '128 MiB' wal_sync 'false' strict_write 'false' max_cache_readers 32 ttl '273years 9months 12days 18h 57m 36s' shard 1 replica 1 vnode_duration '1year';
create database if not exists "show_tag_values" with precision 'NS' max_memcache_size '512 MiB' memcache_partitions 16 wal_max_file_size '128 MiB' wal_sync 'false' strict_write 'false' max_cache_readers 32 ttl '273years 9months 12days 18h 57m 36s' shard 1 replica 1 vnode_duration '1year';
create database if not exists "sqlancer1" with precision 'NS' max_memcache_size '512 MiB' memcache_partitions 16 wal_max_file_size '128 MiB' wal_sync 'false' strict_write 'false' max_cache_readers 32 ttl '273years 9months 12days 18h 57m 36s' shard 1 replica 1 vnode_duration '1year';
create database if not exists "sqlancer2" with precision 'NS' max_memcache_size '512 MiB' memcache_partitions 16 wal_max_file_size '128 MiB' wal_sync 'false' strict_write 'false' max_cache_readers 32 ttl '273years 9months 12days 18h 57m 36s' shard 1 replica 1 vnode_duration '1year';
create database if not exists "sqlancer3" with precision 'NS' max_memcache_size '512 MiB' memcache_partitions 16 wal_max_file_size '128 MiB' wal_sync 'false' strict_write 'false' max_cache_readers 32 ttl '273years 9months 12days 18h 57m 36s' shard 1 replica 1 vnode_duration '1year';
create database if not exists "tc_between" with precision 'NS' max_memcache_size '512 MiB' memcache_partitions 16 wal_max_file_size '128 MiB' wal_sync 'false' strict_write 'false' max_cache_readers 32 ttl '273years 9months 12days 18h 57m 36s' shard 1 replica 1 vnode_duration '1year';
create database if not exists "test_lp_writer" with precision 'NS' max_memcache_size '512 MiB' memcache_partitions 16 wal_max_file_size '128 MiB' wal_sync 'false' strict_write 'false' max_cache_readers 32 ttl '27years 4months 16days 11h 45m 36s' shard 1 replica 1 vnode_duration '1year';
create database if not exists "topk_func" with precision 'NS' max_memcache_size '512 MiB' memcache_partitions 16 wal_max_file_size '128 MiB' wal_sync 'false' strict_write 'false' max_cache_readers 32 ttl '273years 9months 12days 18h 57m 36s' shard 1 replica 1 vnode_duration '1year';
create database if not exists "usage_schema" with precision 'NS' max_memcache_size '2 MiB' memcache_partitions 16 wal_max_file_size '128 MiB' wal_sync 'false' strict_write 'false' max_cache_readers 32 ttl 'INF' shard 1 replica 1 vnode_duration '1year';
create role "lp_writer" inherit member;
grant Read on database "test_lp_writer" to "lp_writer";
create role "role_a" inherit member;
alter tenant "cnosdb" add user "test_au_u1" as "member";
alter tenant "cnosdb" add user "test_au_u2" as "member";
alter tenant "cnosdb" add user "test_cdi_u1" as "owner";
alter tenant "cnosdb" add user "test_ts_u1" as "owner";
alter tenant "cnosdb" add user "test_ts_u2" as "member";
alter tenant "cnosdb" add user "test_us_u1" as "owner";
alter tenant "cnosdb" add user "test_us_u2" as "member";
alter tenant "cnosdb" add user "writer" as "lp_writer";
create table "alter_table"."test" ("f1" BIGINT, "f0" BIGINT, "d0" DOUBLE, "s0" STRING, "b0" BOOLEAN, tags ("t0", "t1"));
create table "avg_daily_driving_duration"."readings_kv" ("latitude" DOUBLE, "longitude" DOUBLE, "elevation" DOUBLE, "velocity" DOUBLE, "heading" DOUBLE, "grade" DOUBLE, "fuel_consumption" DOUBLE, "load_capacity" DOUBLE, "fuel_capacity" DOUBLE, "nominal_fuel_consumption" DOUBLE, tags ("name", "fleet", "driver", "model", "device_version"));
create external table "avg_daily_driving_duration"."readings" stored as PARQUET location 'query_server/sqllogicaltests/resource/parquet/part-0.parquet';
create table "bottom_func"."m2" ("f0" BIGINT, "f1" DOUBLE, tags ("t0", "t1", "t2"));
create table "ci_table_db"."inner_csv" ("bigint_c" BIGINT, "string_c" STRING, "ubigint_c" BIGINT UNSIGNED, "boolean_c" BOOLEAN, "double_c" DOUBLE, tags ("tag1", "tag2"));
create table "ci_table_db"."inner_csv_v2" ("string_c" STRING, "bigint_c" BIGINT, "boolean_c" BOOLEAN, "ubigint_c" BIGINT UNSIGNED, "double_c" DOUBLE, tags ("tag1", "tag2"));
create table "ci_table_db"."inner_parquet" ("latitude" DOUBLE, "longitude" DOUBLE, "elevation" DOUBLE, "velocity" DOUBLE, "heading" DOUBLE, "grade" DOUBLE, "fuel_consumption" DOUBLE, "load_capacity" DOUBLE, "fuel_capacity" DOUBLE, "nominal_fuel_consumption" DOUBLE, tags ("name", "fleet", "driver", "model", "device_version"));
create table "cli_precision"."nice" ("value" DOUBLE, tags ("dc", "host"));
create table "cli_precision"."test1" ("fa" DOUBLE, "fb" DOUBLE, tags ("ta", "tb"));
create table "cli_precision"."test2" ("value" DOUBLE, tags ("ta", "tb"));
create external table "create_external_table"."cpu" ("cpu_hz" DECIMAL(10,6), "temp" DOUBLE, "version_num" BIGINT, "is_old" BOOLEAN, "weight" DECIMAL(12,7)) stored as CSV with header row delimiter ',' location 'query_server/query/tests/data/csv/decimal_data.csv';
create table "createstreamtable"."test0" ("column1" BIGINT, "column2" STRING, "column3" BIGINT UNSIGNED, "column4" BOOLEAN, "column5" DOUBLE, tags ("column6", "column7"));
create table "db_precision"."ms_t" ("value" BIGINT, tags ("str"));
create table "db_precision"."test" ("col" BIGINT, tags ());
create table "describe_database"."test0" ("column1" BIGINT, "column2" STRING, "column3" BIGINT UNSIGNED, "column4" BOOLEAN, "column5" DOUBLE, tags ("column6", "column7"));
create table "empty_table"."empty" ("f" DOUBLE, tags ("t"));
create table "explain_stream_query"."test0" ("column1" BIGINT, "column2" STRING, "column3" BIGINT UNSIGNED, "column4" BOOLEAN, "column5" DOUBLE, tags ("column6", "column7"));
create stream table "explain_stream_query"."tskvtable" ("time" TIMESTAMP, "column1" STRING, "column6" STRING) with (db='explain_stream_query', table='test0', event_time_column='time') engine = tskv;
create stream table "explain_stream_query"."tskvtablewithoutschema" with (db='explain_stream_query', table='test0', event_time_column='time') engine = tskv;
create table "filter_push_down"."m0" ("f0" BIGINT, "f1" STRING, "f2" BIGINT UNSIGNED, "f3" BOOLEAN, "f4" DOUBLE, tags ("t0", "t1"));
create table "http_stream_select"."m0" ("f0" BOOLEAN, "f1" DOUBLE, tags ("t0"));
create table "limit"."limit_test" ("f0" BIGINT, "f1" DOUBLE, tags ("t0", "t1"));
create table "only_tag_col"."m2" ("f0" BIGINT, "f1" DOUBLE, tags ("t0", "t1", "t2"));
create table "only_time_col"."m2" ("f0" BIGINT, "f1" DOUBLE, tags ("t0", "t1", "t2"));
create table "public"."air" ("visibility" DOUBLE, "temperature" DOUBLE, "presssure" DOUBLE, tags ("station", "region"));
create table "public"."test" ("column1" BIGINT, "column2" STRING, "column3" BIGINT UNSIGNED, "column4" BOOLEAN, "column5" DOUBLE, tags ("column6", "column7"));
create table "public"."test_double_conv" ("fa" DOUBLE, "fb" DOUBLE, tags ("ta", "tb"));
create table "public"."test_insert_subquery" ("fa" DOUBLE, "fb" DOUBLE, tags ("ta", "tb"));
create table "public"."test_timestamp_conv" ("fa" DOUBLE, "fb" DOUBLE, tags ("ta", "tb"));
create external table "public"."ci_location_tbl" stored as PARQUET location 'query_server/sqllogicaltests/resource/parquet/part-0.parquet';
create external table "public"."ci_location_tbl2_ext" stored as PARQUET location 'file:///tmp/data/parquet_out2/';
create external table "public"."ci_location_tbl_ext" stored as PARQUET location 'file:///tmp/data/parquet_out1/';
create external table "public"."ci_location_tbl_ext_csv" ("time" TIMESTAMP, "name" STRING) stored as CSV with header row delimiter ',' location 'file:///tmp/data/csv_out/';
create external table "public"."ci_location_tbl_ext_json" ("name" STRING, "time" STRING) stored as JSON location 'file:///tmp/data/json_out/';
create external table "public"."local_to_table_json" ("name" STRING, "time" STRING) stored as JSON location 'file:///tmp/data/json_out/';
create table "show_series"."test" ("f0" DOUBLE, "f1" STRING, tags ("t0", "t1", "t2"));
create table "show_tag_values"."test" ("f0" DOUBLE, "f1" STRING, tags ("t0", "t1", "t2"));
create table "sqlancer1"."m0" ("f0" DOUBLE, "f1" BOOLEAN, tags ("ta"));
create table "sqlancer2"."m0" ("f0" BOOLEAN, "f1" DOUBLE, tags ("t0"));
create table "sqlancer2"."m1" ("f0" STRING, "f1" BOOLEAN, tags ("t0"));
create table "sqlancer2"."m2" ("f0" BOOLEAN, "f1" STRING, tags ("t0"));
create table "sqlancer2"."m3" ("f0" BIGINT, "f1" BOOLEAN, tags ("t0", "t1"));
create table "sqlancer2"."m4" ("f0" BIGINT, tags ("t0", "t1", "t2"));
create table "sqlancer3"."m0" ("f0" DOUBLE, tags ("t0"));
create table "sqlancer3"."m1" ("f0" STRING, "f1" BOOLEAN, "f2" BIGINT, tags ("t0"));
create table "sqlancer3"."m2" ("f0" BIGINT, "f1" DOUBLE, tags ("t0", "t1", "t2"));
create table "sqlancer3"."m3" ("f0" BIGINT, tags ("t0", "t1"));
create table "sqlancer3"."m4" ("f0" BOOLEAN, tags ("t0", "t1"));
create table "tc_between"."m2" ("f0" BIGINT UNSIGNED, "f1" BIGINT, tags ("t0", "t1"));
create table "topk_func"."m2" ("f0" BIGINT, "f1" DOUBLE, tags ("t0", "t1", "t2"));
create table if not exists "usage_schema"."coord_data_in" ("value" BIGINT UNSIGNED, tags ("database", "node_id", "tenant"));
create table if not exists "usage_schema"."coord_data_out" ("value" BIGINT UNSIGNED, tags ("database", "node_id", "tenant"));
create table if not exists "usage_schema"."coord_queries" ("value" BIGINT UNSIGNED, tags ("database", "node_id", "tenant"));
create table if not exists "usage_schema"."coord_writes" ("value" BIGINT UNSIGNED, tags ("database", "node_id", "tenant"));
create table if not exists "usage_schema"."http_data_in" ("value" BIGINT UNSIGNED, tags ("api", "host", "node_id", "tenant", "user", "database"));
create table if not exists "usage_schema"."http_data_out" ("value" BIGINT UNSIGNED, tags ("api", "host", "node_id", "tenant", "user"));
create table if not exists "usage_schema"."http_queries" ("value" BIGINT UNSIGNED, tags ("api", "host", "node_id", "tenant", "user"));
create table if not exists "usage_schema"."http_query_duration" ("value" DOUBLE, tags ("api", "host", "le", "node_id", "tenant", "user"));
create table if not exists "usage_schema"."http_write_duration" ("value" DOUBLE, tags ("api", "database", "host", "le", "node_id", "tenant", "user"));
create table if not exists "usage_schema"."http_writes" ("value" BIGINT UNSIGNED, tags ("api", "database", "host", "node_id", "tenant", "user"));
create table if not exists "usage_schema"."sql_data_in" ("value" BIGINT UNSIGNED, tags ("database", "node_id", "tenant"));
create table if not exists "usage_schema"."vnode_cache_size" ("value" BIGINT UNSIGNED, tags ("database", "node_id", "tenant", "vnode_id"));
create table if not exists "usage_schema"."vnode_disk_storage" ("value" BIGINT UNSIGNED, tags ("database", "node_id", "tenant", "vnode_id"));
-- Dump Tenant tenant_b Object
\change_tenant tenant_b
create database if not exists "db_b" with precision 'NS' max_memcache_size '512 MiB' memcache_partitions 16 wal_max_file_size '128 MiB' wal_sync 'false' strict_write 'false' max_cache_readers 32 ttl 'INF' shard 1 replica 1 vnode_duration '1year';
alter tenant "tenant_b" add user "user_b" as "owner";
create table "db_b"."air_b" ("visibility" DOUBLE, "temperature" DOUBLE, "pressure" DOUBLE, tags ("station"));
-- Dump Tenant test_cols_tenant1 Object
\change_tenant test_cols_tenant1
create database if not exists "public2" with precision 'NS' max_memcache_size '512 MiB' memcache_partitions 16 wal_max_file_size '128 MiB' wal_sync 'false' strict_write 'false' max_cache_readers 32 ttl 'INF' shard 1 replica 1 vnode_duration '1year';
alter tenant "test_cols_tenant1" add user "root" as "member";
alter tenant "test_cols_tenant1" add user "test_cols_u1" as "owner";
alter tenant "test_cols_tenant1" add user "test_cols_u2" as "member";
create table "public2"."test_info_schema_tbl2" ("column1" BIGINT, "column2" STRING, "column3" BIGINT UNSIGNED, "column4" BOOLEAN, "column5" DOUBLE, tags ("column6", "column7"));
-- Dump Tenant test_coord_data_in Object
\change_tenant test_coord_data_in
alter tenant "test_coord_data_in" add user "test_cdi_u1" as "owner";
-- Dump Tenant test_dbs_tenant1 Object
\change_tenant test_dbs_tenant1
create database if not exists "test_dbs_db1" with precision 'NS' max_memcache_size '512 MiB' memcache_partitions 16 wal_max_file_size '128 MiB' wal_sync 'false' strict_write 'false' max_cache_readers 32 ttl 'INF' shard 1 replica 1 vnode_duration '1year';
create database if not exists "test_dbs_db2" with precision 'NS' max_memcache_size '512 MiB' memcache_partitions 16 wal_max_file_size '128 MiB' wal_sync 'false' strict_write 'false' max_cache_readers 32 ttl 'INF' shard 1 replica 1 vnode_duration '1year';
alter tenant "test_dbs_tenant1" add user "test_dbs_u1" as "owner";
alter tenant "test_dbs_tenant1" add user "test_dbs_u2" as "member";
-- Dump Tenant test_dps_tenant Object
\change_tenant test_dps_tenant
create database if not exists "test_dps_db" with precision 'NS' max_memcache_size '512 MiB' memcache_partitions 16 wal_max_file_size '128 MiB' wal_sync 'false' strict_write 'false' max_cache_readers 32 ttl '273years 9months 12days 18h 57m 36s' shard 1 replica 1 vnode_duration '1year';
create role "test_dps_role1" inherit member;
grant Read on database "test_dps_db" to "test_dps_role1";
create role "test_dps_role2" inherit member;
grant Write on database "test_dps_db" to "test_dps_role2";
create role "test_dps_role3" inherit member;
grant All on database "test_dps_db" to "test_dps_role3";
alter tenant "test_dps_tenant" add user "test_dps_u0" as "owner";
alter tenant "test_dps_tenant" add user "test_dps_u1" as "test_dps_role1";
alter tenant "test_dps_tenant" add user "test_dps_u2" as "test_dps_role2";
alter tenant "test_dps_tenant" add user "test_dps_u3" as "test_dps_role3";
create table "test_dps_db"."test_dps_table1" ("a" BIGINT, tags ("b"));
-- Dump Tenant test_dump_info Object
\change_tenant test_dump_info
create database if not exists "test_db" with precision 'NS' max_memcache_size '512 MiB' memcache_partitions 16 wal_max_file_size '128 MiB' wal_sync 'false' strict_write 'false' max_cache_readers 32 ttl 'INF' shard 1 replica 1 vnode_duration '1year';
create table "test_db"."test" ("visibility" DOUBLE CODEC(GORILLA), "temperature" DOUBLE, "pressure" DOUBLE, tags ("station"));
-- Dump Tenant test_ers_tenant1 Object
\change_tenant test_ers_tenant1
create role "test_ers_role1" inherit member;
alter tenant "test_ers_tenant1" add user "test_ers_u1" as "owner";
alter tenant "test_ers_tenant1" add user "test_ers_u2" as "member";
alter tenant "test_ers_tenant1" add user "test_ers_u3" as "test_ers_role1";
-- Dump Tenant test_ms_tenant1 Object
\change_tenant test_ms_tenant1
alter tenant "test_ms_tenant1" add user "test_ms_u1" as "owner";
alter tenant "test_ms_tenant1" add user "test_ms_u2" as "member";
-- Dump Tenant test_rs_tenant1 Object
\change_tenant test_rs_tenant1
create role "test_rs_role1" inherit member;
-- Dump Tenant test_tbls_tenant1 Object
\change_tenant test_tbls_tenant1
create database if not exists "test_tbls_db1" with precision 'NS' max_memcache_size '512 MiB' memcache_partitions 16 wal_max_file_size '128 MiB' wal_sync 'false' strict_write 'false' max_cache_readers 32 ttl 'INF' shard 1 replica 1 vnode_duration '1year';
alter tenant "test_tbls_tenant1" add user "test_tbls_u1" as "owner";
alter tenant "test_tbls_tenant1" add user "test_tbls_u2" as "member";
create table "test_tbls_db1"."test_info_schema_tbl" ("column1" BIGINT, "column2" STRING, "column3" BIGINT UNSIGNED, "column4" BOOLEAN, "column5" DOUBLE, tags ("column6", "column7"));
-- Dump Tenant test_ts_tenant1 Object
\change_tenant test_ts_tenant1
alter tenant "test_ts_tenant1" add user "test_ts_u1" as "owner";
alter tenant "test_ts_tenant1" add user "test_ts_u2" as "member";
-- Dump Tenant test_us_tenant1 Object
\change_tenant test_us_tenant1
alter tenant "test_us_tenant1" add user "test_us_u1" as "owner";
alter tenant "test_us_tenant1" add user "test_us_u2" as "member";

