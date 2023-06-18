--#DATABASE=cli_precision
--#precision=us
--#SLEEP=100
DROP DATABASE IF EXISTS cli_precision;
CREATE DATABASE cli_precision WITH TTL '100000d' precision 'us';

--#LP_BEGIN
test1,ta=a1,tb=b1 fa=1,fb=2 1667456411000001
--#LP_END

--#OPENTSDB_BEGIN
test2 1667456411000001 1 ta=a1 tb=b1
--#OPENTSDB_END

--#OPENTSDB_JSON_BEGIN
[
    {
        "metric": "nice",
        "timestamp": 1667456411000001,
        "value": 18,
        "tags": {
           "host": "web01",
           "dc": "lga"
        }
    },
    {
        "metric": "nice",
        "timestamp": 1667456411000002,
        "value": 9,
        "tags": {
           "host": "web02",
           "dc": "lga"
        }
    }
]
--#OPENTSDB_JSON_END

select * from test1;

select * from test2;

select * from nice order by host;

drop database cli_precision;

CREATE DATABASE cli_precision WITH TTL '100000d' precision 'ms';

--#LP_BEGIN
test1,ta=a1,tb=b1 fa=1,fb=2 1667456411000001
--#LP_END

--#OPENTSDB_BEGIN
test2 1667456411000001 1 ta=a1 tb=b1
--#OPENTSDB_END

--#OPENTSDB_JSON_BEGIN
[
    {
        "metric": "nice",
        "timestamp": 1667456411000001,
        "value": 18,
        "tags": {
           "host": "web01",
           "dc": "lga"
        }
    },
    {
        "metric": "nice",
        "timestamp": 1667456411000002,
        "value": 9,
        "tags": {
           "host": "web02",
           "dc": "lga"
        }
    }
]
--#OPENTSDB_JSON_END

select * from test1;

select * from test2;

select * from nice order by host;
