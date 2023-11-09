drop tenant if exists tenant_delay;

create tenant tenant_delay with drop_after='7';

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

create tenant tenant_delay1 with drop_after='7';
alter tenant tenant_delay1 set drop_after='1m';

drop tenant tenant_delay after '1m';
drop tenant tenant_delay1;

select name,action,try_count,status from information_schema.resource_status where name in ('tenant_delay', 'tenant_delay1') order by name;

--#SLEEP=120000

select name,action,try_count,status from information_schema.resource_status where name in ('tenant_delay', 'tenant_delay1') order by name;
