drop tenant if exists test_tbls_tenant1;
drop user if exists test_tbls_u1;
drop user if exists test_tbls_u2;

create tenant if not exists test_tbls_tenant1;
create user if not exists test_tbls_u1;
create user if not exists test_tbls_u2;

alter tenant test_tbls_tenant1 add user test_tbls_u1 as owner;
alter tenant test_tbls_tenant1 add user test_tbls_u2 as member;

--#TENANT=test_tbls_tenant1
--#USER_NAME=test_tbls_u1
drop database if exists test_tbls_db1;
create database if not exists test_tbls_db1;

CREATE TABLE IF NOT EXISTS test_tbls_db1.test_info_schema_tbl(
    column1 BIGINT CODEC(DELTA),
    column2 STRING CODEC(GZIP),
    column3 BIGINT UNSIGNED CODEC(NULL),
    column4 BOOLEAN,
    column5 DOUBLE CODEC(GORILLA),
    TAGS(column6, column7));

-- CREATE EXTERNAL TABLE test_tbls_db1.readings
-- STORED AS PARQUET LOCATION 'query_server/test/resource/parquet/part-0.parquet';

--#TENANT=test_tbls_tenant1
--#USER_NAME=root
--#SORT=true
select * from information_schema.tables;

--#TENANT=test_tbls_tenant1
--#USER_NAME=test_tbls_u1
--#SORT=true
select * from information_schema.tables;

--#TENANT=test_tbls_tenant1
--#USER_NAME=test_tbls_u2
--#SORT=true
select * from information_schema.tables;

