--#DATABASE=db_precision
--#SLEEP=100
CREATE DATABASE db_precision WITH TTL '100000d' precision 'us';

--#LP_BEGIN
test,ta=a1,tb=b1 fa=1,fb=2 1667456411000010001
--#LP_END

select * from test;

drop database db_precision;

CREATE DATABASE db_precision WITH TTL '100000d' precision 'ms';

create table test(col bigint,);

insert into test(time, col) values (1667456411001, 10);

select * from test;

alter database db_precision set precision 'us';
