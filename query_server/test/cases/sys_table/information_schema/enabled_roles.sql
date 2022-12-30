drop tenant if exists test_ers_tenant1;
drop user if exists test_ers_u1;
drop user if exists test_ers_u2;
drop user if exists test_ers_u3;

create tenant if not exists test_ers_tenant1;
create user if not exists test_ers_u1;
create user if not exists test_ers_u2;
create user if not exists test_ers_u3;

alter tenant test_ers_tenant1 add user test_ers_u1 as owner;
alter tenant test_ers_tenant1 add user test_ers_u2 as member;

--#TENANT=test_ers_tenant1
--#USER_NAME=test_ers_u1
drop role if exists test_ers_role1;

create role if not exists test_ers_role1 inherit member;

alter tenant test_ers_tenant1 add user test_ers_u3 as test_ers_role1;

--#TENANT=test_ers_tenant1
--#USER_NAME=root
--#SORT=true
select * from information_schema.ENABLED_ROLES;

--#TENANT=test_ers_tenant1
--#USER_NAME=test_ers_u1
--#SORT=true
select * from information_schema.ENABLED_ROLES;

--#TENANT=test_ers_tenant1
--#USER_NAME=test_ers_u2
--#SORT=true
select * from information_schema.ENABLED_ROLES;

--#TENANT=test_ers_tenant1
--#USER_NAME=test_ers_u3
--#SORT=true
select * from information_schema.ENABLED_ROLES;
