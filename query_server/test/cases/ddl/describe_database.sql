--#SORT=true
--#SLEEP=100
DROP DATABASE IF EXISTS test1;

DESCRIBE DATABASE test1;

CREATE DATABASE IF NOT EXISTS test1;

DESCRIBE DATABASE test1;

CREATE DATABASE IF NOT EXISTS describetest2;

DESCRIBE DATABASE describetest2;

DROP DATABASE IF EXISTS describetest2;

DROP DATABASE IF EXISTS test1;

DROP DATABASE IF EXISTS describe_database;

CREATE DATABASE IF NOT EXISTS describe_database with ttl '10d';
--#DATABASE=describe_database
CREATE TABLE test0(
                      column1 BIGINT CODEC(DELTA),
                      column2 STRING CODEC(GZIP),
                      column3 BIGINT UNSIGNED CODEC(NULL),
                      column4 BOOLEAN,
                      column5 DOUBLE CODEC(GORILLA),
                      TAGS(column6, column7));

insert describe_database.test0(TIME, column1, column2, column3, column4, column5, column6, column7) values (100, -1234, 'hello', 1234, false, 1.2, 'beijing', 'shanghai');
