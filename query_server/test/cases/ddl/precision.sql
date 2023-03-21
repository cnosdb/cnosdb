--#DATABASE=precision
--#SLEEP=100
CREATE DATABASE precision WITH TTL '100000d' precision 'us';

--#LP_BEGIN
test,ta=a1,tb=b1 fa=1,fb=2 1667456411000001
--#LP_END

select * from test;

drop database precision;

CREATE DATABASE precision WITH TTL '100000d' precision 'ms';

create table test(col bigint,);

insert into test(time, col) values (1667456411001, 10);

select * from test;

alter database precision set precision 'us';
