--#DATABASE=show_tag_values
--#SLEEP=100
CREATE DATABASE show_tag_values WITH TTL '100000d';;


--#LP_BEGIN
test,t0=a,t1=b,t2=c f0=1,f1="2" 0
test,t0=a f0=1 1
test,t1=b f1="2" 2
test,t2=c f0=1 3
test,t0=a,t1=b f0=1 4
test,t1=b,t2=c f0=1 5
--#LP_END

INSERT INTO test(TIME, t0, f0) VALUES (6, '', 1);

SHOW TAG VALUES;
SHOW TAG VALUES ON public FROM show_series.test;
SHOW TAG VALUES FROM test;

--#SORT=true
SHOW TAG VALUES FROM test WITH KEY IN (t0, t1, t2);
SHOW TAG VALUES ON show_tag_values FROM test WITH KEY IN (t0, t1, t2);
--#SORT=false

SHOW TAG VALUES FROM test WITH KEY IN (t0, t1, t2) ORDER BY f0;
SHOW TAG VALUES FROM test WITH KEY IN (t0, t1, t2) ORDER BY time;
SHOW TAG VALUES FROM test WITH KEY IN (t0, t1, t2) ORDER BY key, value;
SHOW TAG VALUES FROM test WITH KEY IN (t0, t1, t2) ORDER BY key ASC;
SHOW TAG VALUES FROM test WITH KEY IN (t0, t1, t2) ORDER BY key DESC;
SHOW TAG VALUES FROM test WITH KEY IN (t0, t1, t2) ORDER BY key, value;
SHOW TAG VALUES FROM test WITH KEY IN (t0, t1, t2) ORDER BY key ASC,  value DESC;
SHOW TAG VALUES FROM test WITH KEY IN (t0, t1, t2) ORDER BY key DESC, value ASC;
SHOW TAG VALUES FROM test WITH KEY IN (t0, t1, t2) ORDER BY key, value;
SHOW TAG VALUES FROM test WITH KEY = "t0" ORDER BY key, value;
SHOW TAG VALUES FROM test WITH KEY != "t0" ORDER BY key, value;
SHOW TAG VALUES FROM test WITH KEY NOT IN (t0, t1, t2) ORDER BY key, value;
SHOW TAG VALUES FROM test WITH KEY NOT IN (t0) ORDER BY key, value;
SHOW TAG VALUES FROM test WITH KEY IN (t0, t1, t2) WHERE time < now() ORDER BY key, value;
SHOW TAG VALUES FROM test WITH KEY IN (t0, t1, t2) WHERE f1 IS NOT NULL ORDER BY key, value;
SHOW TAG VALUES FROM test WITH KEY IN (t0, t1, t2) WHERE t0 != '' ORDER BY key, value;
SHOW TAG VALUES FROM test WITH KEY IN (t0, t1, t2) WHERE t0 IS NOT NULL ORDER BY key, value;
SHOW TAG VALUES FROM test WITH KEY IN (t0, t1, t2) WHERE t1 = 'b' ORDER BY key, value;
