create user test_usage_schema_user;

create role write_usage_schema_role inherit member;

grant write on database usage_schema to role write_usage_schema_role;

alter tenant cnosdb add user test_usage_schema_user as write_usage_schema_role;

create table usage_schema.test_privilege (a bigint);

--#USER_NAME=test_usage_schema_user
insert into usage_schema.test_privilege (time, a) values (1, 1);

--#USER_NAME=root
drop table usage_schema.test_privilege;

alter tenant cnosdb remove user test_usage_schema_user;

drop role write_usage_schema_role;

drop user test_usage_schema_user;

insert into usage_schema.coord_data_in (time, database, tenant, value) values (1, 'invalid', 'invalid', 1);

