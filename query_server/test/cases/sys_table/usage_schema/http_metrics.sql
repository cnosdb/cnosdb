--#DATABASE = usage_schema
--#SLEEP = 100
DESCRIBE DATABASE usage_schema;

select column_name, column_type, data_type from information_schema.columns
where column_name != 'database'
and database_name = 'usage_schema'
and table_name = 'http_data_in'
order by column_name;

select column_name, column_type, data_type from information_schema.columns
where column_name != 'database'
and database_name = 'usage_schema'
and table_name = 'http_data_out'
order by column_name;

select column_name, column_type, data_type from information_schema.columns
where column_name != 'database'
and database_name = 'usage_schema'
and table_name = 'http_queries'
order by column_name;

select column_name, column_type, data_type from information_schema.columns
where column_name != 'database'
and database_name = 'usage_schema'
and table_name = 'http_writes'
order by column_name;
