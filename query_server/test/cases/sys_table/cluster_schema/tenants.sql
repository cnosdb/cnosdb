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

drop tenant cnosdb;

alter tenant test_ts_tenant1 set comment = 'hello world';
alter tenant test_ts_tenant1 set _limiter = '{"object_config":{"max_users_number":1,"max_databases":3,"max_shard_number":2,"max_replicate_number":2,"max_retention_time":30},"request_config":{"data_in":{"remote_bucket":{"max":100,"initial":0,"refill":100,"interval":100},"local_bucket":{"max":100,"initial":0}},"data_out":{"remote_bucket":{"max":100,"initial":0,"refill":100,"interval":100},"local_bucket":{"max":100,"initial":0}},"queries":null,"writes":null}}';

select * from cluster_schema.tenants where tenant_name in ('test_ts_tenant1');

alter tenant test_ts_tenant1 unset comment;
alter tenant test_ts_tenant1 unset _limiter;

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
