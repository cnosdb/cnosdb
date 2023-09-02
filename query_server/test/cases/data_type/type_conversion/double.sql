-- normal
--#SLEEP=100
-- public.test(time timestamp(ns), ta string, tb string, fa double, fb double)
drop table if exists test_double_conv;
alter database public set ttl '1000000d';
--#LP_BEGIN
test_double_conv,ta=a1,tb=b1 fa=1,fb=2 3
--#LP_END

--#LP_BEGIN
test_double_conv,ta=a1,tb=b1 fa=1,fb=2 1667456411000000000
--#LP_END

-- binary
---- utf8 to float64
explain
select time
from test_double_conv
where fa = '12345678865';

explain
select time
from test_double_conv
where fa <> '12345678865';

explain
select time
from test_double_conv
where fa != '12345678865';

explain
select time
from test_double_conv
where fa < '12345678865';

explain
select time
from test_double_conv
where fa <= '12345678865';

explain
select time
from test_double_conv
where fa > '12345678865';

explain
select time
from test_double_conv
where fa >= '12345678865';

explain
select time
from test_double_conv
where fa >= '12345678865';

---- error start
explain
select time
from test_double_conv
where fa >= '1997-01-31';

explain
select time
from test_double_conv
where fa >= 'xxx';
---- error end

-- between and
---- normal
explain
select time
from test_double_conv
where fa between '12345678865' and 12345678869;

explain
select time
from test_double_conv
where fa between 12345678865 and 12345678869;

---- error start
explain
select time
from test_double_conv
where fa between 12345678865 and 'xxx';
---- error end

-- in list
explain
select time
from test_double_conv
where fa in (12345678865, '12345678869');

---- error start
explain
select time
from test_double_conv
where fa in (12345678865, 'xx');
---- error end

-- issue: https://github.com/apache/arrow-datafusion/issues/6001
explain select sum(case when fa < 50 then fa else 0 end)
    over (partition by ta) from test_double_conv;
