--#SLEEP=100
DROP TABLE IF EXISTS ci_location_tbl;
DROP TABLE IF EXISTS ci_location_tbl_ext;
DROP TABLE IF EXISTS ci_location_tbl2_ext;
DROP TABLE IF EXISTS ci_location_tbl_ext_csv;
DROP TABLE IF EXISTS ci_location_tbl_ext_json;

CREATE EXTERNAL TABLE ci_location_tbl
STORED AS PARQUET
LOCATION 'query_server/test/resource/parquet/part-0.parquet';

select * from ci_location_tbl order by time,name limit 10;

-- Export data from subquery to external path
COPY INTO 'file:///tmp/data/parquet_out1/' 
FROM (select time, name from ci_location_tbl)
file_format = (type = 'parquet');

-- Create external table validation data
CREATE EXTERNAL TABLE ci_location_tbl_ext
STORED AS PARQUET
LOCATION 'file:///tmp/data/parquet_out1/';

select * from ci_location_tbl_ext order by time,name limit 10;

-- Export data from table to external path
COPY INTO 'file:///tmp/data/parquet_out2/' 
FROM ci_location_tbl
file_format = (type = 'parquet');

-- Create external table validation data
CREATE EXTERNAL TABLE ci_location_tbl2_ext
STORED AS PARQUET
LOCATION 'file:///tmp/data/parquet_out2/';

select * from ci_location_tbl2_ext order by time,name limit 10;


---- csv start
copy into 'file:///tmp/data/csv_out/' 
FROM (select time, name from ci_location_tbl)
file_format = (type = 'csv');

-- Create external table validation data
CREATE EXTERNAL TABLE ci_location_tbl_ext_csv
STORED AS CSV
WITH HEADER ROW
LOCATION 'file:///tmp/data/csv_out/';

select * from ci_location_tbl_ext_csv order by time,name limit 10;


---- json start
copy into 'file:///tmp/data/json_out/' 
FROM (select time, name from ci_location_tbl)
file_format = (type = 'json');

-- Create external table validation data
CREATE EXTERNAL TABLE ci_location_tbl_ext_json
STORED AS JSON
LOCATION 'file:///tmp/data/json_out/';

select * from ci_location_tbl_ext_json order by time,name limit 10;
