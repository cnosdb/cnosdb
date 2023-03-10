--#DATABASE=filter_push_down
--#SLEEP=100
--#SORT=true
DROP DATABASE IF EXISTS filter_push_down;
CREATE DATABASE filter_push_down WITH TTL '100000d';

CREATE TABLE m0(
    f0 BIGINT CODEC(DELTA),
    f1 STRING CODEC(GZIP),
    f2 BIGINT UNSIGNED CODEC(NULL),
    f3 BOOLEAN,
    f4 DOUBLE CODEC(GORILLA),
    TAGS(t0, t1));

INSERT m0(TIME, f4, t0) VALUES(1041670293467254361, 0.507623643211476, '916053861');
INSERT m0(TIME, f0) VALUES(2079939785551584142, NULL), (1243152233754651379, 12321); -- 0ms;
INSERT m0(TIME, f1) VALUES(631407052613557553, 'TRUE'), (7486831592909450783, 'TRUE'); -- 0ms;
INSERT m0(TIME, f2) VALUES(5867172425191822176, 888), (3986678807649375642, 999); -- 0ms;
INSERT m0(TIME, f3) VALUES(7488251815539246350, FALSE); -- 0ms;
INSERT m0(TIME, f4) VALUES(5414775681413349294, 1.111); -- 1ms;
INSERT m0(TIME, t0) VALUES(5414775681413349294, 't000'); -- 1ms;
INSERT m0(TIME, t1) VALUES(5414775681413349294, 't111'); -- 1ms;

INSERT m0(TIME, t0, t1, f0, f1, f2, f3, f4)
VALUES
    (1, 'a', 'b', 11, '11', 11, true, 11.11),
    (2, 'a', 'c', 12, '11', 11, false, 11.11),
    (3, 'b', 'b', 13, '11', 11, false, 11.11),
    (4, 'b', 'a', 14, '11', 11, true, 11.11),
    (5, 'a', 'a', 11, '11', 11, true, 11.11),
    (6, 'b', 'c', 15, '11', 11, false, 11.11); -- 1ms;

select * from m0 order by time, t0, t1, f0;

-- not support push_down 'not' expr
SELECT ALL * FROM m0 AS M0 WHERE NOT ((('TOk')=(m0.t0)))
UNION ALL
SELECT ALL * FROM m0 AS M0  WHERE NOT (NOT ((('TOk')=(m0.t0))))
UNION ALL
SELECT ALL * FROM m0 AS M0  WHERE (NOT ((('TOk')=(m0.t0)))) IS NULL;


select * from m0 
where time = 0;

select * from m0 
where time > 3;

select * from m0 
where t0 = 'xx';

select * from m0 
where t0 = 'a';

select * from m0 
where t0 = 'a' and t1 = 'b';

-- not support push down
select * from m0 
where t0 = 'a' or t1 = 'b';

select * from m0 
where t0 = 'a' and f0 = 11;

select * from m0 
where t0 = 'a' and f0 > 12;

select * from m0 
where t0 = 'a' or f0 = 11;

select * from m0 
where t0 = 'a' or f0 > 12;

select * from m0 
where t0 = 'a' and f0 = 11 and time > 3;

-- not support push down
select * from m0 
where t0 = 'a' and f0 = 11 or time > 3;

explain
select * from m0 
where t0 = null;

explain
select * from m0 
where t0 > null;
