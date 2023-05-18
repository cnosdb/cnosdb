--#DATABASE = usage_schema
--#SLEEP = 10000
DESCRIBE DATABASE usage_schema;

DESCRIBE TABLE coord_data_in;

drop tenant if exists test_coord_data_in;
drop user if exists test_cdi_u1;

create tenant if not exists test_coord_data_in;
create user if not exists test_cdi_u1;
alter tenant test_coord_data_in add user test_cdi_u1 as owner;
alter tenant cnosdb add user test_cdi_u1 as owner;

--#TENANT=cnosdb
--#USER_NAME=root
--#SORT=true
select * from usage_schema.coord_data_in where false;

--#TENANT=cnosdb
--#USER_NAME=test_cdi_u1
--#SORT=true
select * from usage_schema.coord_data_in where false;

--#TENANT=test_coord_data_in
--#USER_NAME=test_cdi_u1
--#SORT=true
select * from usage_schema.coord_data_in where false;

--#TENANT=test_coord_data_in
--#DATABASE=usage_schema
--#USER_NAME=test_cdi_u1
--#SORT=true
select * from coord_data_in where false;
