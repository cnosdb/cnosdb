--#DATABASE=db_precision
--#SLEEP=100
DROP DATABASE IF EXISTS db_precision;
CREATE DATABASE db_precision WITH TTL '100000d' precision 'us';

--#LP_BEGIN
test,ta=a1,tb=b1 fa=1,fb=2 1667456411000010001
--#LP_END

select * from test;

drop database db_precision;

CREATE DATABASE db_precision WITH TTL '100000d' precision 'ms';

create table test(col bigint);

insert into test(time, col) values (1667456411001, 10);

select * from test;

alter database db_precision set precision 'us';

create table ms_t (value BIGINT,tags(str));
insert into ms_t (time,str,value) values (1682049219172428000,'asd',1);
select * from ms_t;
