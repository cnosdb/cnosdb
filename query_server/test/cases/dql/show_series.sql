--#DATABASE=show_series
--#SLEEP=100
CREATE DATABASE show_series WITH TTL '100000d';


--#LP_BEGIN
test,t0=a,t1=b,t2=c f0=1,f1="2" 0
test,t0=a f0=1 1
test,t1=b f1="2" 2
test,t2=c f0=1 3
test,t0=a,t1=b f0=1 4
test,t1=b,t2=c f0=1 5
--#LP_END

INSERT INTO test(TIME, t0, f0) VALUES (6, '', 1);

SHOW SERIES;
SHOW SERIES ON public FROM show_series.test;
--#SORT=true
SHOW SERIES FROM test;
SHOW SERIES ON show_series FROM test;
--#SORT=false

SHOW SERIES FROM test ORDER BY f0;
SHOW SERIES FROM test ORDER BY time;
SHOW SERIES FROM test ORDER BY key;
SHOW SERIES FROM test ORDER BY key ASC;
SHOW SERIES FROM test ORDER BY key DESC;
SHOW SERIES FROM test WHERE time < now() ORDER BY key;
SHOW SERIES FROM test WHERE f1 IS NOT NULL ORDER BY key;
SHOW SERIES FROM test WHERE t0 != '' ORDER BY key;
SHOW SERIES FROM test WHERE t0 IS NOT NULL ORDER BY key;
SHOW SERIES FROM test WHERE t1 = 'b' ORDER BY key;

