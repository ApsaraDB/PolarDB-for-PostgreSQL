CREATE EXTENSION bloom;

create table bloom_tbl (i1 int, i2 int , i3 int, i4 int, i5 int, i6 int);

insert into bloom_tbl
select i as i1, (i*2) as i2, (i*3) as i3, (i*4) as i4, (i*5) as i5, (i*6) as i6
from generate_series(1,100000) as i;

select * from bloom_tbl where i1=1000 and i2=2000 and i3=3000;
vacuum analyze bloom_tbl;

select * from bloom_tbl where i1=1000 and i2=2000 and i3=3000;
CREATE INDEX bloomidx ON bloom_tbl USING bloom (i1,i2,i3)
       WITH (length=80, col1=2, col2=2, col3=4);

-- explain (costs off)
select * from bloom_tbl where i1=1000 and i2=2000 and i3=3000;

select * from bloom_tbl where i1=1000 and i2=2000 and i3=3000;
select count(*) from bloom_tbl where i2<10000;

delete from bloom_tbl where i2 < 1000;

select * from bloom_tbl where i1=1000 and i2=2000 and i3=3000;
select count(*) from bloom_tbl where i2<10000;

vacuum bloom_tbl;

select * from bloom_tbl where i1=1000 and i2=2000 and i3=3000;
select count(*) from bloom_tbl where i2<10000;
drop index bloomidx;

CREATE INDEX bloomidx ON bloom_tbl USING bloom (i1, i2, i3, i4, i5, i6);
SELECT pg_size_pretty(pg_relation_size('bloomidx'));

-- explain (costs off)
select * from bloom_tbl where i1=1000 and i2=2000 and i3=3000;

select * from bloom_tbl where i1=1000 and i2=2000 and i3=3000;

select count(*) from bloom_tbl where i2<10000;

