-- normal
--#SLEEP=100
-- public.test(time i64, ta utf8, tb utf8, fa , fb)
--#LP_BEGIN
test_timestamp_conv,ta=a1,tb=b1 fa=1,fb=2 3
--#LP_END

--#LP_BEGIN
-- 2022-11-03 14:20:11
test_timestamp_conv,ta=a1,tb=b1 fa=1,fb=2 1667456411000000000
--#LP_END

-- binary
---- int64 to timestamp
explain
select fa
from test_timestamp_conv
where time = 12345678865;

explain
select fa
from test_timestamp_conv
where time = 1667456411000000000;

explain
select fa
from test_timestamp_conv
where time <> 12345678865;

explain
select fa
from test_timestamp_conv
where time != 12345678865;

explain
select fa
from test_timestamp_conv
where time < 12345678865;

explain
select fa
from test_timestamp_conv
where time < 1667456412000000000;

explain
select fa
from test_timestamp_conv
where time <= 12345678865;

explain
select fa
from test_timestamp_conv
where time <= 1667456412000000000;

explain
select fa
from test_timestamp_conv
where time > 12345678865;

explain
select fa
from test_timestamp_conv
where time >= 12345678865;

explain
select fa
from test_timestamp_conv
where time >= 12345678865;

---- utf8 to timestamp
explain
select fa
from test_timestamp_conv
where time = '1997-01-31 09:26:56';

explain
select fa
from test_timestamp_conv
where time = '2022-11-03 14:20:11';

explain
select fa
from test_timestamp_conv
where time <> '1997-01-31 09:26:56.123';

explain
select fa
from test_timestamp_conv
where time != '1997-01-31T09:26:56.123';

explain
select fa
from test_timestamp_conv
where time < '1997-01-31 09:26:56.123-05:00';

explain
select fa
from test_timestamp_conv
where time < '2022-11-03T14:20:12.123-05:00';

explain
select fa
from test_timestamp_conv
where time <= '1997-01-31T09:26:56.123-05:00';

explain
select fa
from test_timestamp_conv
where time <= '2022-11-03T14:20:12.123-05:00';

explain
select fa
from test_timestamp_conv
where time > '1997-01-31T09:26:56.123Z';

explain
select fa
from test_timestamp_conv
where time >= '1997-01-31 09:26:56';

---- error start
explain
select fa
from test_timestamp_conv
where time >= '1997-01-31';

explain
select fa
from test_timestamp_conv
where time >= 'xxx';
---- error end

-- between and
---- normal
explain
select fa
from test_timestamp_conv
where time between '1997-01-31 09:26:56' and '1997-03-31T09:26:56.123Z';

explain
select fa
from test_timestamp_conv
where time between '2022-11-03 14:20:10' and '2022-11-03T14:20:12.123Z';

explain
select fa
from test_timestamp_conv
where time between 12345678865 and 12345678869;

explain
select fa
from test_timestamp_conv
where time between 1667456410000000000 and 1667456412000000000;

explain
select fa
from test_timestamp_conv
where time between 12345678865 and '1997-03-31T09:26:56.123Z';

explain
select fa
from test_timestamp_conv
where time between 1667456410000000000 and '2022-11-03T14:20:12.123Z';


---- error start
explain
select fa
from test_timestamp_conv
where time between 12345678865 and '1997-03-31';

explain
select fa
from test_timestamp_conv
where time between 12345678865 and 'xxxxx';
---- error end

-- in list
explain
select fa
from test_timestamp_conv
where time in (12345678865, '1997-03-31T09:26:56.123Z');

---- error start
explain
select fa
from test_timestamp_conv
where time in (12345678865, 'xx');
---- error end
