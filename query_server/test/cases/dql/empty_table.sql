--#DATABASE=empty_table
--#SLEEP=0
CREATE DATABASE empty_table;

CREATE TABLE empty (
    f DOUBLE,
    TAGS(t));

SELECT * FROM empty;
