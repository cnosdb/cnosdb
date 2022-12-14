drop tenant if exists test_dbs_tenant1;
drop user if exists test_dbs_u1;
drop user if exists test_dbs_u2;

create tenant if not exists test_dbs_tenant1;
create user if not exists test_dbs_u1;
create user if not exists test_dbs_u2;

alter tenant test_dbs_tenant1 add user test_dbs_u1 as owner;
alter tenant test_dbs_tenant1 add user test_dbs_u2 as member;

--#TENANT=test_dbs_tenant1
--#USER_NAME=test_dbs_u1
drop database if exists test_dbs_db1;
drop database if exists test_dbs_db2;

create database if not exists test_dbs_db1;
create database if not exists test_dbs_db2;

--#TENANT=test_dbs_tenant1
--#USER_NAME=root
--#SORT=true
select * from information_schema.DATABASES;

--#TENANT=test_dbs_tenant1
--#USER_NAME=test_dbs_u1
--#SORT=true
select * from information_schema.DATABASES;

--#TENANT=test_dbs_tenant1
--#USER_NAME=test_dbs_u2
--#SORT=true
select * from information_schema.DATABASES;
