drop tenant if exists test_dps_tenant;

drop user if exists test_dps_u0;
drop user if exists test_dps_u1;
drop user if exists test_dps_u2;
drop user if exists test_dps_u3;

create tenant if not exists test_dps_tenant;
create user if not exists test_dps_u0;
create user if not exists test_dps_u1;
create user if not exists test_dps_u2;
create user if not exists test_dps_u3;

alter tenant test_dps_tenant add user test_dps_u0 as owner;

--#TENANT=test_dps_tenant
--#USER_NAME=test_dps_u0
drop role if exists test_dps_role0;
drop role if exists test_dps_role1;
drop role if exists test_dps_role2;
drop role if exists test_dps_role3;

create role if not exists test_dps_role1 inherit member;
create role if not exists test_dps_role2 inherit member;
create role if not exists test_dps_role3 inherit member;

drop database if exists test_dps_db;
create database if not exists test_dps_db with ttl '100000d';

-- role1 read
grant read on database test_dps_db to test_dps_role1;

-- role2 write
grant write on database test_dps_db to role test_dps_role2;

-- role3 all
grant all on database test_dps_db to test_dps_role3;

-- u1 read only
alter tenant test_dps_tenant add user test_dps_u1 as test_dps_role1;
-- u2 write
alter tenant test_dps_tenant add user test_dps_u2 as test_dps_role2;
-- u3 all
alter tenant test_dps_tenant add user test_dps_u3 as test_dps_role3;

--#USER_NAME=root
--#SORT=true
select * from information_schema.DATABASE_PRIVILEGES;

--#USER_NAME=test_dps_u0
select * from information_schema.DATABASE_PRIVILEGES;

--#USER_NAME=test_dps_u1
select * from information_schema.DATABASE_PRIVILEGES;

--#USER_NAME=test_dps_u2
select * from information_schema.DATABASE_PRIVILEGES;

--#USER_NAME=test_dps_u3
select * from information_schema.DATABASE_PRIVILEGES;


--#USER_NAME=test_dps_u0
--#DATABASE=test_dps_db
--#SORT=false
create table test_dps_table(a bigint, tags(b));
insert into table test_dps_table(time, a, b) values (1, 1, '1');

--#USER_NAME=test_dps_u1
--#DATABASE=test_dps_db

-- create database if not exists test_dps_db1 with ttl '100000d';
-- create table test_dps_table1(a bigint, tags(b));
-- insert into test_dps_table(time, a, b) values (2, 2, '2');
-- alter table test_dps_table add field c double codec(default);
-- alter table test_dps_table add tag d;
alter table test_dps_table drop c;
--#SORT=true
select * from test_dps_table;
--#SORT=false

-- drop table test_dps_table;

--#USER_NAME=test_dps_u2
--#DATABASE=test_dps_db

-- create database if not exists test_dps_db1 with ttl '100000d';
-- create table test_dps_table1(a bigint, tags(b));
insert into test_dps_table(time, a, b) values (2, 2, '2');
-- alter table test_dps_table add field c double codec(default);
-- alter table test_dps_table add tag d;
alter table test_dps_table drop c;
--#SORT=true
select * from test_dps_table;
--#SORT=false

-- drop table test_dps_table;


--#USER_NAME=test_dps_u3
--#DATABASE=test_dps_db
-- create database if not exists test_dps_db1 with ttl '100000d';
create table test_dps_table1(a bigint, tags(b));
insert into test_dps_table(time, a, b) values (2, 2, '2');
alter table test_dps_table add field c double codec(default);
alter table test_dps_table add tag d;
alter table test_dps_table drop c;
--#SORT=true
select * from test_dps_table;
--#SORT=false
drop table test_dps_table;



