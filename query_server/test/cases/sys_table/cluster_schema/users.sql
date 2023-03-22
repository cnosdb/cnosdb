drop tenant if exists test_us_tenant1;
drop user if exists test_us_u1;
drop user if exists test_us_u2;

create tenant if not exists test_us_tenant1;
create user if not exists test_us_u1;
create user if not exists test_us_u2;

alter user test_us_u1 set comment = 'test comment';

select * from cluster_schema.users where user_name = 'test_us_u1';

alter tenant cnosdb add user test_us_u1 as owner;
alter tenant cnosdb add user test_us_u2 as member;

alter tenant test_us_tenant1 add user test_us_u1 as owner;
alter tenant test_us_tenant1 add user test_us_u2 as member;

--#TENANT=cnosdb
--#USER_NAME=root
--#SORT=true
select * from cluster_schema.users where user_name in ('root', 'test_us_u1', 'test_us_u2');

--#TENANT=cnosdb
--#USER_NAME=test_us_u1
--#SORT=true
select * from cluster_schema.users where user_name in ('root', 'test_us_u1', 'test_us_u2');

--#TENANT=cnosdb
--#USER_NAME=test_us_u2
--#SORT=true
select * from cluster_schema.users where user_name in ('root', 'test_us_u1', 'test_us_u2');

--#TENANT=test_us_tenant1
--#USER_NAME=root
--#SORT=true
select * from cluster_schema.users where user_name in ('root', 'test_us_u1', 'test_us_u2');

--#TENANT=test_us_tenant1
--#USER_NAME=test_us_u1
--#SORT=true
select * from cluster_schema.users where user_name in ('root', 'test_us_u1', 'test_us_u2');

--#TENANT=test_us_tenant1
--#USER_NAME=test_us_u2
--#SORT=true
select * from cluster_schema.users where user_name in ('root', 'test_us_u1', 'test_us_u2');
