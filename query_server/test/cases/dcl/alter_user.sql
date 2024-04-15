drop user if exists test_au_u1;
drop user if exists test_au_u2;

create user if not exists test_au_u1;
create user if not exists test_au_u2;

alter tenant cnosdb add user test_au_u1 as member;
alter tenant cnosdb add user test_au_u2 as member;

--#TENANT=cnosdb
--#USER_NAME=root
--#SORT=true
select * from cluster_schema.users where user_name in ('root', 'test_au_u1', 'test_au_u2');

--#TENANT=cnosdb
--#USER_NAME=test_au_u1
alter user test_au_u1 set granted_admin = true;

--#TENANT=cnosdb
--#USER_NAME=root
alter user test_au_u1 set granted_admin = true;

--#TENANT=cnosdb
--#USER_NAME=root
--#SORT=true
select * from cluster_schema.users where user_name in ('root', 'test_au_u1', 'test_au_u2');

--#TENANT=cnosdb
--#USER_NAME=test_au_u1
--#SORT=true
alter user test_au_u2 set granted_admin = true;

--#USER_NAME=test_au_u2
alter user test_au_u2 set must_change_password = true;

--#TENANT=cnosdb
--#USER_NAME=root
--#SORT=true
select * from cluster_schema.users where user_name in ('root', 'test_au_u1', 'test_au_u2');

--#TENANT=cnosdb
--#USER_NAME=root
alter user test_au_u1 set granted_admin = false;

--#TENANT=cnosdb
--#USER_NAME=test_au_u1
alter user test_au_u2 set granted_admin = false;

--#USER_NAME=root
drop user if exists u12;

create user u12;

--#SLEEP=100

ALTER TENANT cnosdb ADD USER u12 AS owner;

alter user u12 set granted_admin = true;

--#USER_NAME=u12
alter user root set comment='test';

alter user root set password='asd';

alter user root set granted_admin=false;
