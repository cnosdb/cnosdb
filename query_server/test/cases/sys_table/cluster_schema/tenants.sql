drop tenant if exists test_ts_tenant1;
drop user if exists test_ts_u1;
drop user if exists test_ts_u2;

create tenant if not exists test_ts_tenant1;
create user if not exists test_ts_u1;
create user if not exists test_ts_u2;

alter tenant cnosdb add user test_ts_u1 as owner;
alter tenant cnosdb add user test_ts_u2 as member;

alter tenant test_ts_tenant1 add user test_ts_u1 as owner;
alter tenant test_ts_tenant1 add user test_ts_u2 as member;

--#TENANT=cnosdb
--#USER_NAME=root
--#SORT=true
select * from cluster_schema.tenants where tenant_name in ('test_ts_tenant1');

--#TENANT=cnosdb
--#USER_NAME=test_ts_u1
--#SORT=true
select * from cluster_schema.tenants where tenant_name in ('test_ts_tenant1');

--#TENANT=cnosdb
--#USER_NAME=test_ts_u2
--#SORT=true
select * from cluster_schema.tenants where tenant_name in ('test_ts_tenant1');

--#TENANT=test_ts_tenant1
--#USER_NAME=root
--#SORT=true
select * from cluster_schema.tenants where tenant_name in ('test_ts_tenant1');

--#TENANT=test_ts_tenant1
--#USER_NAME=test_ts_u1
--#SORT=true
select * from cluster_schema.tenants where tenant_name in ('test_ts_tenant1');

--#TENANT=test_ts_tenant1
--#USER_NAME=test_ts_u2
--#SORT=true
select * from cluster_schema.tenants where tenant_name in ('test_ts_tenant1');
