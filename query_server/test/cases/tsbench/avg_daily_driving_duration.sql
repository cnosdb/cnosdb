--#DATABASE=avg_daily_driving_duration
--#SLEEP=100
----------------------------------------------------------------------------
-- AvgDailyDrivingDuration finds the average driving duration per driver. --
----------------------------------------------------------------------------
DROP DATABASE IF EXISTS avg_daily_driving_duration;
CREATE DATABASE avg_daily_driving_duration with TTL '3650d';

drop table if EXISTS readings;
CREATE EXTERNAL TABLE readings
STORED AS PARQUET
LOCATION 'query_server/test/resource/parquet/part-0.parquet';

-- external table & date_bin
SELECT 
count("mv")/ 6 as "hours driven"
FROM 
(
    SELECT 
    DATE_BIN(
        INTERVAL '10 minutes', time, TIMESTAMP '1970-01-01T00:00:00Z'
    ) as "time", 
    "fleet", 
    "name", 
    "driver", 
    avg("velocity") as "mv" 
    FROM 
    "readings"
    WHERE 
    time > '2022-01-01T00:00:00Z' 
    AND time < '2022-02-01T00:00:00Z' 
    GROUP BY 
    DATE_BIN(
    INTERVAL '10 minutes', time, TIMESTAMP '1970-01-01T00:00:00Z'
    ), 
    "fleet", 
    "name", 
    "driver"
)
GROUP BY 
DATE_BIN(
    INTERVAL '1 day', time, TIMESTAMP '1970-01-01T00:00:00Z'
), 
"fleet", 
"name", 
"driver"
order by fleet,name,driver;

-- external table & time_window
SELECT 
count("mv")/ 6 as "hours driven"
FROM 
(
    SELECT 
    time_window(time, '10m') as "time", 
    "fleet", 
    "name", 
    "driver", 
    avg("velocity") as "mv" 
    FROM 
    "readings"
    WHERE 
    time > '2022-01-01T00:00:00Z' 
    AND time < '2022-02-01T00:00:00Z' 
    GROUP BY 
    time_window(time, '10m'), 
    "fleet", 
    "name", 
    "driver"
)
GROUP BY 
time_window(time.start, '1d'), 
"fleet", 
"name", 
"driver"
order by fleet,name,driver;


-- inner table
drop table if exists readings_kv;
create table readings_kv(
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

insert into readings_kv select * from readings;

-- inner table & date_bin
SELECT 
count("mv")/ 6 as "hours driven"
FROM 
(
    SELECT 
    DATE_BIN(
        INTERVAL '10 minutes', time, TIMESTAMP '1970-01-01T00:00:00Z'
    ) as "time", 
    "fleet", 
    "name", 
    "driver", 
    avg("velocity") as "mv" 
    FROM 
    "readings_kv"
    WHERE 
    time > '2022-01-01T00:00:00Z' 
    AND time < '2022-02-01T00:00:00Z' 
    GROUP BY 
    DATE_BIN(
    INTERVAL '10 minutes', time, TIMESTAMP '1970-01-01T00:00:00Z'
    ), 
    "fleet", 
    "name", 
    "driver"
)
GROUP BY 
DATE_BIN(
    INTERVAL '1 day', time, TIMESTAMP '1970-01-01T00:00:00Z'
), 
"fleet", 
"name", 
"driver"
order by fleet,name,driver;

-- inner table & time_window
SELECT 
count("mv")/ 6 as "hours driven"
FROM 
(
    SELECT 
    time_window(time, '10m') as "time", 
    "fleet", 
    "name", 
    "driver", 
    avg("velocity") as "mv" 
    FROM 
    "readings_kv"
    WHERE 
    time > '2022-01-01T00:00:00Z' 
    AND time < '2022-02-01T00:00:00Z' 
    GROUP BY 
    time_window(time, '10m'), 
    "fleet", 
    "name", 
    "driver"
)
GROUP BY 
time_window(time.start, '1d'), 
"fleet", 
"name", 
"driver"
order by fleet,name,driver;
