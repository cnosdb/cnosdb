drop tenant if exists test_rs_tenant1;
drop user if exists test_rs_u1;
drop user if exists test_rs_u2;

create tenant if not exists test_rs_tenant1;
create user if not exists test_rs_u1;
create user if not exists test_rs_u2;

alter tenant test_rs_tenant1 add user test_rs_u1 as owner;
alter tenant test_rs_tenant1 add user test_rs_u2 as member;

--#TENANT=test_rs_tenant1
--#USER_NAME=test_rs_u1
create role if not exists test_rs_role1 inherit member;

--#TENANT=test_rs_tenant1
--#USER_NAME=root
--#SORT=true
select * from information_schema.ROLES;

--#TENANT=test_rs_tenant1
--#USER_NAME=test_rs_u1
--#SORT=true
select * from information_schema.ROLES;

--#TENANT=test_rs_tenant1
--#USER_NAME=test_rs_u2
--#SORT=true
select * from information_schema.ROLES;

--#TENANT=test_rs_tenant1
--#USER_NAME=root
alter tenant test_rs_tenant1 remove user test_rs_u1;
alter tenant test_rs_tenant1 remove user test_rs_u2;

--#TENANT=test_rs_tenant1
--#USER_NAME=root
--#SORT=true
select * from information_schema.ROLES;

