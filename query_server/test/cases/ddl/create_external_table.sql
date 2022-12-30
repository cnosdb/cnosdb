--#DATABASE=create_external_table
--#SORT = true
--#SLEEP=100
DROP DATABASE IF EXISTS create_external_table;
CREATE DATABASE create_external_table WITH TTL '100000d';

CREATE EXTERNAL TABLE
    cpu (
        cpu_hz  DECIMAL(10,6) NOT NULL,
        temp  DOUBLE NOT NULL,
        version_num  BIGINT NOT NULL,
        is_old  BOOLEAN NOT NULL,
        weight  DECIMAL(12,7) NOT NULL
    )
    STORED AS CSV
    WITH HEADER ROW
    LOCATION 'query_server/query/tests/data/csv/decimal_data.csv';

DESCRIBE TABLE create_external_table.cpu;
