explain
COPY INTO 'file:///tmp/data/parquet_out2/' 
FROM (select 1 as col1, 'xx' as col2)
file_format = (type = 'parquet');
