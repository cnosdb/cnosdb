drop tenant if exists test_cols_tenant1;
drop user if exists test_cols_u1;
drop user if exists test_cols_u2;

create tenant if not exists test_cols_tenant1;
create user if not exists test_cols_u1;
create user if not exists test_cols_u2;

alter tenant test_cols_tenant1 add user root as member;
alter tenant test_cols_tenant1 add user test_cols_u1 as owner;
alter tenant test_cols_tenant1 add user test_cols_u2 as member;

--#TENANT=test_cols_tenant1
--#USER_NAME=root
--#SORT=true
drop database if exists public2;
create database if not exists public2;
CREATE TABLE IF NOT EXISTS public2.test_info_schema_tbl2(
    column1 BIGINT CODEC(DELTA),
    column2 STRING CODEC(GZIP),
    column3 BIGINT UNSIGNED CODEC(NULL),
    column4 BOOLEAN,
    column5 DOUBLE CODEC(GORILLA),
    TAGS(column6, column7));

--#TENANT=test_cols_tenant1
--#USER_NAME=root
--#SORT=true
-- Can see all columns of all tables
select * from information_schema.columns;

--#TENANT=test_cols_tenant1
--#USER_NAME=test_cols_u1
--#SORT=true
-- Can see all columns of all tables
select * from information_schema.columns;

--#TENANT=test_cols_tenant1
--#USER_NAME=test_cols_u2
--#SORT=true
-- Can see all columns of all tables
select * from information_schema.columns;
