-- Dump Global Object
create tenant if not exists "test1";
-- Dump Tenant test1 Object
\change_tenant test1
-- Dump Global Object
create tenant if not exists "test2";
-- Dump Tenant test2 Object
\change_tenant test2

