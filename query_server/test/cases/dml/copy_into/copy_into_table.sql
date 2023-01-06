-- csv start
--#DATABASE=ci_table_db
--#SLEEP=100
DROP DATABASE IF EXISTS ci_table_db;
CREATE DATABASE ci_table_db with TTL '3650d';

DROP TABLE IF EXISTS inner_csv;

CREATE TABLE inner_csv(
    bigint_c BIGINT,
    string_c STRING,
    ubigint_c BIGINT UNSIGNED,
    boolean_c BOOLEAN,
    double_c DOUBLE,
    TAGS(tag1, tag2)
    );

COPY INTO inner_csv
FROM 'query_server/test/resource/data_type/csv/full_data_type.csv'
file_format = (type = 'csv');

select * from inner_csv order by time;

CREATE TABLE inner_csv_v2(
    string_c STRING,
    bigint_c BIGINT,
    boolean_c BOOLEAN,
    ubigint_c BIGINT UNSIGNED,
    double_c DOUBLE,
    TAGS(tag1, tag2)
);

select * from inner_csv_v2 limit 1;

-- time,tag1,tag2,bigint_c,string_c,ubigint_c,boolean_c,double_c
-- 2022-12-22 09:26:56,tt1,tt2,-512512,hello word,512,true,1.11
COPY INTO inner_csv_v2(time, tag1, tag2, bigint_c, string_c, ubigint_c, boolean_c, double_c)
FROM 'query_server/test/resource/data_type/csv/full_data_type.csv'
file_format = (type = 'csv');

-- error
COPY INTO inner_csv_v2
FROM 'query_server/test/resource/data_type/csv/full_data_type.csv'
file_format = (type = 'csv');
-- csv end

-- parquet start
drop table if EXISTS inner_parquet;

create table inner_parquet(
  latitude double,
  longitude double,
  elevation double,
  velocity double,
  heading double,
  grade double,
  fuel_consumption double,
  load_capacity double,
  fuel_capacity double,
  nominal_fuel_consumption double,
  tags(name, fleet, driver, model, device_version)
);

COPY INTO inner_parquet
FROM 'query_server/test/resource/parquet/part-0.parquet'
file_format = (type = 'parquet');

select count(time) from inner_parquet;
-- parquet end

copy into inner_parquet
from 'query_server/test/resource/csv/part-0.csv' 
file_format = (type = 'csv');

copy into inner_parquet
from 'query_server/test/resource/json/part-0.json' 
file_format = (type = 'json');

copy into inner_parquet
from 'query_server/test/resource/parquet/part-0.parquet' 
file_format = (type = 'parquet');

-- error: Insert columns and Source columns not match
copy into inner_parquet
from 'query_server/test/resource/json/part-0.json' 
file_format = (type = 'json')
copy_options = (auto_infer_schema = true);

-- error: Insert columns and Source columns not match
copy into inner_parquet
from 'query_server/test/resource/json/part-0.json' 
file_format = (type = 'json')
copy_options = (auto_infer_schema = true);

-- ok, parquet has metadata
copy into inner_parquet
from 'query_server/test/resource/parquet/part-0.parquet' 
file_format = (type = 'parquet')
copy_options = (auto_infer_schema = true);
