drop tenant if exists test_ms_tenant1;
drop user if exists test_ms_u1;
drop user if exists test_ms_u2;

create tenant if not exists test_ms_tenant1;
create user if not exists test_ms_u1;
create user if not exists test_ms_u2;

alter tenant test_ms_tenant1 add user test_ms_u1 as owner;
alter tenant test_ms_tenant1 add user test_ms_u2 as member;

--#TENANT=test_ms_tenant1
--#USER_NAME=root
--#SORT=true
select * from information_schema.MEMBERS;

--#TENANT=test_ms_tenant1
--#USER_NAME=test_ms_u1
--#SORT=true
select * from information_schema.MEMBERS;

--#TENANT=test_ms_tenant1
--#USER_NAME=test_ms_u2
--#SORT=true
select * from information_schema.MEMBERS;
