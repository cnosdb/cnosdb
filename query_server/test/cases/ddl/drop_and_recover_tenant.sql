drop tenant if exists tenant_delay;

create tenant tenant_delay;

--#TENANT=tenant_delay
--#SLEEP=100

SHOW DATABASES;

--#TENANT=cnosdb
--#SLEEP=100

drop tenant tenant_delay after '3m';

--#TENANT=tenant_delay
--#SLEEP=100

SHOW DATABASES;

--#TENANT=cnosdb
--#SLEEP=100

select name,action,try_count,status from information_schema.resource_status where name = 'tenant_delay';

recover tenant tenant_delay;

select name,action,try_count,status from information_schema.resource_status where name = 'tenant_delay';

--#TENANT=tenant_delay
--#SLEEP=100

SHOW DATABASES;

--#TENANT=cnosdb
--#SLEEP=100

drop tenant tenant_delay after '1m';

select name,action,try_count,status from information_schema.resource_status where name = 'tenant_delay';

--#SLEEP=120000

select name,action,try_count,status from information_schema.resource_status where name = 'tenant_delay';