drop tenant if exists test_dps_tenant1;
drop user if exists test_dps_u1;
drop user if exists test_dps_u2;

create tenant if not exists test_dps_tenant1;
create user if not exists test_dps_u1;
create user if not exists test_dps_u2;

alter tenant test_dps_tenant1 add user test_dps_u1 as owner;
alter tenant test_dps_tenant1 add user test_dps_u2 as member;

--#TENANT=test_dps_tenant1
--#USER_NAME=test_dps_u1
drop role if exists test_dps_role1;
drop role if exists test_dps_role2;
drop database if exists test_dps_db1;
drop database if exists test_dps_db2;

create role if not exists test_dps_role1 inherit owner;
create role if not exists test_dps_role2 inherit member;

create database if not exists test_dps_db1;
create database if not exists test_dps_db2;

grant write on database test_dps_db1 to role test_dps_role1;
grant all on database test_dps_db1 to test_dps_role2;
grant all on database test_dps_db2 to test_dps_role2;

--#TENANT=test_dps_tenant1
--#USER_NAME=root
--#SORT=true
select * from information_schema.DATABASE_PRIVILEGES;

--#TENANT=test_dps_tenant1
--#USER_NAME=test_dps_u1
--#SORT=true
select * from information_schema.DATABASE_PRIVILEGES;

--#TENANT=test_dps_tenant1
--#USER_NAME=test_dps_u2
--#SORT=true
select * from information_schema.DATABASE_PRIVILEGES;
