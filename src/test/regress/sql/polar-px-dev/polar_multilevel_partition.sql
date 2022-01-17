-- partition by range 
/*--EXPLAIN_QUERY_BEGIN*/
select 'partition by range' as current_query;
SET client_min_messages TO 'warning';
DROP TABLE IF EXISTS sales cascade;
RESET client_min_messages;
CREATE OR REPLACE FUNCTION create_huge_partitions()
RETURNS VOID AS $$
DECLARE
	partition_num   INT := 1000;
	i INT := 0;
	last_num INT := 0;
BEGIN
	EXECUTE 'CREATE TABLE sales (id int, date date, amt decimal(10,2)) PARTITION BY RANGE (id)';

	LOOP
		EXIT WHEN i = partition_num;
		i := i + 1;
		EXECUTE format('CREATE TABLE sales_part_%s PARTITION of sales for values FROM (%s) to (%s)', i - 1, last_num, last_num + 100);
		last_num := last_num + 100;
	END LOOP;

	EXECUTE format('create table sales_part_default partition of sales  DEFAULT');

	insert into sales select x, '2021-04-27', 111.1  from  generate_series(1,999999) x;
END;
$$ LANGUAGE plpgsql;


-- multi-level range partition

CREATE TABLE range_list (a int,b timestamp,c varchar(10)) PARTITION BY RANGE (b);
 
CREATE TABLE range_pa1 PARTITION OF range_list FOR VALUES from ('2000-01-01') TO ('2010-01-01')  PARTITION BY RANGE (a);
CREATE TABLE range_pa2 PARTITION OF range_list FOR VALUES from ('2010-01-01') TO ('2020-01-01')  PARTITION BY RANGE (a);

CREATE TABLE range_list_2000_2010_1_10  PARTITION OF range_pa1 FOR VALUES from (1) TO (10);
CREATE TABLE range_list_2000_2010_10_20 PARTITION OF range_pa1 FOR VALUES from (11) TO (20);

CREATE TABLE range_list_2010_2020_1_10  PARTITION OF range_pa2 FOR VALUES from (1) TO (10);
CREATE TABLE range_list_2010_2020_10_20 PARTITION OF range_pa2 FOR VALUES from (11) TO (20);

insert into range_list values(1,'2005-01-05 5:05');

create or replace function get_random_timestamp(start_date date,end_date date) returns timestamp as
$BODY$
declare
    interval_days integer;
    random_days integer;
    random_date date;
    result_date text; 
    result_time text; 
begin
    interval_days := end_date - start_date;
    random_days := trunc(random() * (interval_days + 1));
    random_date := start_date + random_days;
    result_date := date_part('year',random_date)|| '-' || date_part('month',random_date)|| '-' || date_part('day',random_date);
    result_time := (mod((random()*100)::integer,23)+1)||':'||(mod((random()*100)::integer,59)+1)||':'||(mod((random()*100)::integer,59)+1);
    return to_timestamp(result_date||' '||result_time,'YYYY-MM-DD HH24:MI:SS');
    end;
$BODY$ language plpgsql;

insert into range_list select round(random()*8) + 1, get_random_timestamp('2000-01-01', '2010-01-01') from generate_series(1,100000);
insert into range_list select round(random()*8) + 11, get_random_timestamp('2000-01-01', '2010-01-01') from generate_series(1,100000);

insert into range_list select round(random()*8) + 1, get_random_timestamp('2010-01-01', '2019-12-01') from generate_series(1,100000);
insert into range_list select round(random()*8) + 11, get_random_timestamp('2010-01-01', '2019-12-01') from generate_series(1,100000);

analyze range_list;

set polar_px_optimizer_multilevel_partitioning=1;

-- top table
select tableoid::regclass from range_list where a = 1 and b < '2009-01-01 00:00:00' limit 1;

-- partitioned table
select tableoid::regclass from range_pa1 where a = 1 and b < '2009-01-01 00:00:00' limit 1;

-- partitioned table，no result
select tableoid::regclass from range_pa2 where a = 1 and b < '2009-01-01 00:00:00' limit 1;

-- paritioned table
select tableoid::regclass from range_list_2000_2010_1_10 where a = 1 and b < '2009-01-01 00:00:00' limit 1;

-- create partitioned index
create index on range_list using btree(a);

-- top table with index 
select tableoid::regclass from range_list where a = 1 and b < '2009-01-01 00:00:00' limit 1;

-- partitioned table with index 
select tableoid::regclass from range_pa1 where a = 1 and b < '2009-01-01 00:00:00' limit 1;

-- partitioned table with index ，no result
select tableoid::regclass from range_pa2 where a = 1 and b < '2009-01-01 00:00:00' limit 1;

-- paritioned table with index 
select tableoid::regclass from range_list_2000_2010_1_10 where a = 1 and b < '2009-01-01 00:00:00' limit 1;


set polar_px_optimizer_enable_indexscan to off;

-- top table with index 
select tableoid::regclass from range_list where a = 1 and b < '2009-01-01 00:00:00' limit 1;

-- partitioned table with index 
select tableoid::regclass from range_pa1 where a = 1 and b < '2009-01-01 00:00:00' limit 1;

-- partitioned table with index ，no result
select tableoid::regclass from range_pa2 where a = 1 and b < '2009-01-01 00:00:00' limit 1;

-- paritioned table with index 
select tableoid::regclass from range_list_2000_2010_1_10 where a = 1 and b < '2009-01-01 00:00:00' limit 1;

set polar_px_optimizer_enable_bitmapscan to off;

-- top table with index 
select tableoid::regclass from range_list where a = 1 and b < '2009-01-01 00:00:00' limit 1;

-- partitioned table with index 
select tableoid::regclass from range_pa1 where a = 1 and b < '2009-01-01 00:00:00' limit 1;

-- partitioned table with index ，no result
select tableoid::regclass from range_pa2 where a = 1 and b < '2009-01-01 00:00:00' limit 1;

-- paritioned table with index 
select tableoid::regclass from range_list_2000_2010_1_10 where a = 1 and b < '2009-01-01 00:00:00' limit 1;

drop table range_list cascade;


-- 3 level partitions with range & list; range-range-list

CREATE TABLE range_list (a int,b timestamp,c varchar(10)) PARTITION BY RANGE (b);
 
CREATE TABLE range_pa1 PARTITION OF range_list FOR VALUES from ('2000-01-01') TO ('2010-01-01')  PARTITION BY RANGE (a);
CREATE TABLE range_pa2 PARTITION OF range_list FOR VALUES from ('2010-01-01') TO ('2020-01-01')  PARTITION BY RANGE (a);

CREATE TABLE range_pa1_1  PARTITION OF range_pa1 FOR VALUES from (1) TO (10) PARTITION BY LIST(c);
CREATE TABLE range_pa1_2  PARTITION OF range_pa1 FOR VALUES from (11) TO (20) PARTITION BY LIST(c);
CREATE TABLE range_pa1_1_1 PARTITION OF range_pa1_1 FOR VALUES IN ('F');
CREATE TABLE range_pa1_1_2 PARTITION OF range_pa1_1 FOR VALUES IN ('M');
CREATE TABLE range_pa1_1_3 PARTITION OF range_pa1_1 default;
CREATE TABLE range_pa1_2_1 PARTITION OF range_pa1_2 FOR VALUES IN ('F');
CREATE TABLE range_pa1_2_2 PARTITION OF range_pa1_2 FOR VALUES IN ('M');
CREATE TABLE range_pa1_2_3 PARTITION OF range_pa1_2 default;


CREATE TABLE range_pa2_1  PARTITION OF range_pa2 FOR VALUES from (1) TO (10) PARTITION BY LIST(c);
CREATE TABLE range_pa2_2  PARTITION OF range_pa2 FOR VALUES from (11) TO (20) PARTITION BY LIST(c);
CREATE TABLE range_pa2_1_1 PARTITION OF range_pa2_1 FOR VALUES IN ('F');
CREATE TABLE range_pa2_1_2 PARTITION OF range_pa2_1 FOR VALUES IN ('M');
CREATE TABLE range_pa2_1_3 PARTITION OF range_pa2_1 default;
CREATE TABLE range_pa2_2_1 PARTITION OF range_pa2_2 FOR VALUES IN ('F');
CREATE TABLE range_pa2_2_2 PARTITION OF range_pa2_2 FOR VALUES IN ('M');
CREATE TABLE range_pa2_2_3 PARTITION OF range_pa2_2 default;

insert into range_list values(1,'2005-01-05 5:05','M');
insert into range_list values(1,'2005-01-05 5:05','F');
insert into range_list values(1,'2005-01-05 5:05','C');

insert into range_list values(11,'2005-01-05 5:05','M');
insert into range_list values(11,'2005-01-05 5:05','F');
insert into range_list values(11,'2005-01-05 5:05','C');

insert into range_list values(1,'2015-01-05 5:05','M');
insert into range_list values(1,'2015-01-05 5:05','F');
insert into range_list values(1,'2015-01-05 5:05','C');

insert into range_list values(11,'2015-01-05 5:05','M');
insert into range_list values(11,'2015-01-05 5:05','F');
insert into range_list values(11,'2015-01-05 5:05','C');

analyze range_list;

select * from range_list;

-- top table
select tableoid::regclass from range_list where a = 1 and b < '2009-01-01 00:00:00' order by tableoid::regclass;

-- partitioned table
select tableoid::regclass from range_pa1 where a = 1 and b < '2009-01-01 00:00:00' order by tableoid::regclass;

-- partitioned table，no result
select tableoid::regclass from range_pa2 where a = 1 and b < '2009-01-01 00:00:00' order by tableoid::regclass;

-- create partitioned index
create index on range_list using btree(a);
create index on range_list using btree(b);
create index on range_list using btree(c);

-- top table
select tableoid::regclass from range_list where a = 1 and b < '2009-01-01 00:00:00' order by tableoid::regclass;

-- partitioned table
select tableoid::regclass from range_pa1 where a = 1 and b < '2009-01-01 00:00:00' order by tableoid::regclass;

-- partitioned table，no result
select tableoid::regclass from range_pa2 where a = 1 and b < '2009-01-01 00:00:00' order by tableoid::regclass;


-- disable multi-level partition
set polar_px_optimizer_multilevel_partitioning=0;

select * from range_list;
-- top table
select tableoid::regclass from range_list where a = 1 and b < '2009-01-01 00:00:00' order by tableoid::regclass;

-- partitioned table
select tableoid::regclass from range_pa1 where a = 1 and b < '2009-01-01 00:00:00' order by tableoid::regclass;

-- partitioned table，no result
select tableoid::regclass from range_pa2 where a = 1 and b < '2009-01-01 00:00:00' order by tableoid::regclass;

set polar_px_optimizer_multilevel_partitioning=1;

drop table range_list cascade;


-- from orca
-- 3 level partitions, range-list-range
-- Check for all the different number of partition selections
DROP TABLE IF EXISTS DATE_PARTS;
CREATE TABLE DATE_PARTS (id int, year int, month int, day int, region text)
PARTITION BY RANGE (year);

-- 1 
CREATE TABLE DATE_PARTS_1 PARTITION OF DATE_PARTS FOR VALUES from ('2002') TO ('2006') PARTITION BY LIST (month);
CREATE TABLE DATE_PARTS_2 PARTITION OF DATE_PARTS FOR VALUES from ('2006') TO ('2010') PARTITION BY LIST (month);
CREATE TABLE DATE_PARTS_3 PARTITION OF DATE_PARTS FOR VALUES from ('2010') TO ('2012') PARTITION BY LIST (month);
CREATE TABLE DATE_PARTS_4 PARTITION OF DATE_PARTS default PARTITION BY LIST (month);

-- 2
CREATE TABLE DATE_PARTS_1_1 PARTITION OF DATE_PARTS_1 FOR VALUES IN (1,2,3) PARTITION BY RANGE (day);
CREATE TABLE DATE_PARTS_1_2 PARTITION OF DATE_PARTS_1 FOR VALUES IN (4,5,6) PARTITION BY RANGE (day);
CREATE TABLE DATE_PARTS_1_3 PARTITION OF DATE_PARTS_1 FOR VALUES IN (7,8,9) PARTITION BY RANGE (day);
CREATE TABLE DATE_PARTS_1_4 PARTITION OF DATE_PARTS_1 FOR VALUES IN (10,11,12) PARTITION BY RANGE (day);
CREATE TABLE DATE_PARTS_1_5 PARTITION OF DATE_PARTS_1 default PARTITION BY RANGE (day);

-- 3
CREATE TABLE DATE_PARTS_1_1_1 PARTITION OF DATE_PARTS_1_1 FOR VALUES from (1) TO (10);
CREATE TABLE DATE_PARTS_1_1_2 PARTITION OF DATE_PARTS_1_1 FOR VALUES from (10) TO (20);
CREATE TABLE DATE_PARTS_1_1_3 PARTITION OF DATE_PARTS_1_1 FOR VALUES from (20) TO (32);
CREATE TABLE DATE_PARTS_1_1_4 PARTITION OF DATE_PARTS_1_1 default;
CREATE TABLE DATE_PARTS_1_2_1 PARTITION OF DATE_PARTS_1_2 FOR VALUES from (1) TO (10);
CREATE TABLE DATE_PARTS_1_2_2 PARTITION OF DATE_PARTS_1_2 FOR VALUES from (10) TO (20);
CREATE TABLE DATE_PARTS_1_2_3 PARTITION OF DATE_PARTS_1_2 FOR VALUES from (20) TO (32);
CREATE TABLE DATE_PARTS_1_2_4 PARTITION OF DATE_PARTS_1_2 default;
CREATE TABLE DATE_PARTS_1_3_1 PARTITION OF DATE_PARTS_1_3 FOR VALUES from (1) TO (10);
CREATE TABLE DATE_PARTS_1_3_2 PARTITION OF DATE_PARTS_1_3 FOR VALUES from (10) TO (20);
CREATE TABLE DATE_PARTS_1_3_3 PARTITION OF DATE_PARTS_1_3 FOR VALUES from (20) TO (32);
CREATE TABLE DATE_PARTS_1_3_4 PARTITION OF DATE_PARTS_1_3 default;
CREATE TABLE DATE_PARTS_1_4_1 PARTITION OF DATE_PARTS_1_4 FOR VALUES from (1) TO (10);
CREATE TABLE DATE_PARTS_1_4_2 PARTITION OF DATE_PARTS_1_4 FOR VALUES from (10) TO (20);
CREATE TABLE DATE_PARTS_1_4_3 PARTITION OF DATE_PARTS_1_4 FOR VALUES from (20) TO (32);
CREATE TABLE DATE_PARTS_1_4_4 PARTITION OF DATE_PARTS_1_4 default;
CREATE TABLE DATE_PARTS_1_5_1 PARTITION OF DATE_PARTS_1_5 FOR VALUES from (1) TO (10);
CREATE TABLE DATE_PARTS_1_5_2 PARTITION OF DATE_PARTS_1_5 FOR VALUES from (10) TO (20);
CREATE TABLE DATE_PARTS_1_5_3 PARTITION OF DATE_PARTS_1_5 FOR VALUES from (20) TO (32);
CREATE TABLE DATE_PARTS_1_5_4 PARTITION OF DATE_PARTS_1_5 default;
-- 2
CREATE TABLE DATE_PARTS_2_1 PARTITION OF DATE_PARTS_2 FOR VALUES IN (1,2,3) PARTITION BY RANGE (day);
CREATE TABLE DATE_PARTS_2_2 PARTITION OF DATE_PARTS_2 FOR VALUES IN (4,5,6) PARTITION BY RANGE (day);
CREATE TABLE DATE_PARTS_2_3 PARTITION OF DATE_PARTS_2 FOR VALUES IN (7,8,9) PARTITION BY RANGE (day);
CREATE TABLE DATE_PARTS_2_4 PARTITION OF DATE_PARTS_2 FOR VALUES IN (10,11,12) PARTITION BY RANGE (day);
CREATE TABLE DATE_PARTS_2_5 PARTITION OF DATE_PARTS_2 default PARTITION BY RANGE (day);

-- 3
CREATE TABLE DATE_PARTS_2_1_1 PARTITION OF DATE_PARTS_2_1 FOR VALUES from (1) TO (10);
CREATE TABLE DATE_PARTS_2_1_2 PARTITION OF DATE_PARTS_2_1 FOR VALUES from (10) TO (20);
CREATE TABLE DATE_PARTS_2_1_3 PARTITION OF DATE_PARTS_2_1 FOR VALUES from (20) TO (32);
CREATE TABLE DATE_PARTS_2_1_4 PARTITION OF DATE_PARTS_2_1 default;
CREATE TABLE DATE_PARTS_2_2_1 PARTITION OF DATE_PARTS_2_2 FOR VALUES from (1) TO (10);
CREATE TABLE DATE_PARTS_2_2_2 PARTITION OF DATE_PARTS_2_2 FOR VALUES from (10) TO (20);
CREATE TABLE DATE_PARTS_2_2_3 PARTITION OF DATE_PARTS_2_2 FOR VALUES from (20) TO (32);
CREATE TABLE DATE_PARTS_2_2_4 PARTITION OF DATE_PARTS_2_2 default;
CREATE TABLE DATE_PARTS_2_3_1 PARTITION OF DATE_PARTS_2_3 FOR VALUES from (1) TO (10);
CREATE TABLE DATE_PARTS_2_3_2 PARTITION OF DATE_PARTS_2_3 FOR VALUES from (10) TO (20);
CREATE TABLE DATE_PARTS_2_3_3 PARTITION OF DATE_PARTS_2_3 FOR VALUES from (20) TO (32);
CREATE TABLE DATE_PARTS_2_3_4 PARTITION OF DATE_PARTS_2_3 default;
CREATE TABLE DATE_PARTS_2_4_1 PARTITION OF DATE_PARTS_2_4 FOR VALUES from (1) TO (10);
CREATE TABLE DATE_PARTS_2_4_2 PARTITION OF DATE_PARTS_2_4 FOR VALUES from (10) TO (20);
CREATE TABLE DATE_PARTS_2_4_3 PARTITION OF DATE_PARTS_2_4 FOR VALUES from (20) TO (32);
CREATE TABLE DATE_PARTS_2_4_4 PARTITION OF DATE_PARTS_2_4 default;
CREATE TABLE DATE_PARTS_2_5_1 PARTITION OF DATE_PARTS_2_5 FOR VALUES from (1) TO (10);
CREATE TABLE DATE_PARTS_2_5_2 PARTITION OF DATE_PARTS_2_5 FOR VALUES from (10) TO (20);
CREATE TABLE DATE_PARTS_2_5_3 PARTITION OF DATE_PARTS_2_5 FOR VALUES from (20) TO (32);
CREATE TABLE DATE_PARTS_2_5_4 PARTITION OF DATE_PARTS_2_5 default;

-- 2
CREATE TABLE DATE_PARTS_3_1 PARTITION OF DATE_PARTS_3 FOR VALUES IN (1,2,3) PARTITION BY RANGE (day);
CREATE TABLE DATE_PARTS_3_2 PARTITION OF DATE_PARTS_3 FOR VALUES IN (4,5,6) PARTITION BY RANGE (day);
CREATE TABLE DATE_PARTS_3_3 PARTITION OF DATE_PARTS_3 FOR VALUES IN (7,8,9) PARTITION BY RANGE (day);
CREATE TABLE DATE_PARTS_3_4 PARTITION OF DATE_PARTS_3 FOR VALUES IN (10,11,12) PARTITION BY RANGE (day);
CREATE TABLE DATE_PARTS_3_5 PARTITION OF DATE_PARTS_3 default PARTITION BY RANGE (day);

-- 3
CREATE TABLE DATE_PARTS_3_1_1 PARTITION OF DATE_PARTS_3_1 FOR VALUES from (1) TO (10);
CREATE TABLE DATE_PARTS_3_1_2 PARTITION OF DATE_PARTS_3_1 FOR VALUES from (10) TO (20);
CREATE TABLE DATE_PARTS_3_1_3 PARTITION OF DATE_PARTS_3_1 FOR VALUES from (20) TO (32);
CREATE TABLE DATE_PARTS_3_1_4 PARTITION OF DATE_PARTS_3_1 default;
CREATE TABLE DATE_PARTS_3_2_1 PARTITION OF DATE_PARTS_3_2 FOR VALUES from (1) TO (10);
CREATE TABLE DATE_PARTS_3_2_2 PARTITION OF DATE_PARTS_3_2 FOR VALUES from (10) TO (20);
CREATE TABLE DATE_PARTS_3_2_3 PARTITION OF DATE_PARTS_3_2 FOR VALUES from (20) TO (32);
CREATE TABLE DATE_PARTS_3_2_4 PARTITION OF DATE_PARTS_3_2 default;
CREATE TABLE DATE_PARTS_3_3_1 PARTITION OF DATE_PARTS_3_3 FOR VALUES from (1) TO (10);
CREATE TABLE DATE_PARTS_3_3_2 PARTITION OF DATE_PARTS_3_3 FOR VALUES from (10) TO (20);
CREATE TABLE DATE_PARTS_3_3_3 PARTITION OF DATE_PARTS_3_3 FOR VALUES from (20) TO (32);
CREATE TABLE DATE_PARTS_3_3_4 PARTITION OF DATE_PARTS_3_3 default;
CREATE TABLE DATE_PARTS_3_4_1 PARTITION OF DATE_PARTS_3_4 FOR VALUES from (1) TO (10);
CREATE TABLE DATE_PARTS_3_4_2 PARTITION OF DATE_PARTS_3_4 FOR VALUES from (10) TO (20);
CREATE TABLE DATE_PARTS_3_4_3 PARTITION OF DATE_PARTS_3_4 FOR VALUES from (20) TO (32);
CREATE TABLE DATE_PARTS_3_4_4 PARTITION OF DATE_PARTS_3_4 default;
CREATE TABLE DATE_PARTS_3_5_1 PARTITION OF DATE_PARTS_3_5 FOR VALUES from (1) TO (10);
CREATE TABLE DATE_PARTS_3_5_2 PARTITION OF DATE_PARTS_3_5 FOR VALUES from (10) TO (20);
CREATE TABLE DATE_PARTS_3_5_3 PARTITION OF DATE_PARTS_3_5 FOR VALUES from (20) TO (32);
CREATE TABLE DATE_PARTS_3_5_4 PARTITION OF DATE_PARTS_3_5 default;

-- 2
CREATE TABLE DATE_PARTS_4_1 PARTITION OF DATE_PARTS_4 FOR VALUES IN (1,2,3) PARTITION BY RANGE (day);
CREATE TABLE DATE_PARTS_4_2 PARTITION OF DATE_PARTS_4 FOR VALUES IN (4,5,6) PARTITION BY RANGE (day);
CREATE TABLE DATE_PARTS_4_3 PARTITION OF DATE_PARTS_4 FOR VALUES IN (7,8,9) PARTITION BY RANGE (day);
CREATE TABLE DATE_PARTS_4_4 PARTITION OF DATE_PARTS_4 FOR VALUES IN (10,11,12) PARTITION BY RANGE (day);
CREATE TABLE DATE_PARTS_4_5 PARTITION OF DATE_PARTS_4 default PARTITION BY RANGE (day);

-- 3
CREATE TABLE DATE_PARTS_4_1_1 PARTITION OF DATE_PARTS_4_1 FOR VALUES from (1) TO (10);
CREATE TABLE DATE_PARTS_4_1_2 PARTITION OF DATE_PARTS_4_1 FOR VALUES from (10) TO (20);
CREATE TABLE DATE_PARTS_4_1_3 PARTITION OF DATE_PARTS_4_1 FOR VALUES from (20) TO (32);
CREATE TABLE DATE_PARTS_4_1_4 PARTITION OF DATE_PARTS_4_1 default;
CREATE TABLE DATE_PARTS_4_2_1 PARTITION OF DATE_PARTS_4_2 FOR VALUES from (1) TO (10);
CREATE TABLE DATE_PARTS_4_2_2 PARTITION OF DATE_PARTS_4_2 FOR VALUES from (10) TO (20);
CREATE TABLE DATE_PARTS_4_2_3 PARTITION OF DATE_PARTS_4_2 FOR VALUES from (20) TO (32);
CREATE TABLE DATE_PARTS_4_2_4 PARTITION OF DATE_PARTS_4_2 default;
CREATE TABLE DATE_PARTS_4_3_1 PARTITION OF DATE_PARTS_4_3 FOR VALUES from (1) TO (10);
CREATE TABLE DATE_PARTS_4_3_2 PARTITION OF DATE_PARTS_4_3 FOR VALUES from (10) TO (20);
CREATE TABLE DATE_PARTS_4_3_3 PARTITION OF DATE_PARTS_4_3 FOR VALUES from (20) TO (32);
CREATE TABLE DATE_PARTS_4_3_4 PARTITION OF DATE_PARTS_4_3 default;
CREATE TABLE DATE_PARTS_4_4_1 PARTITION OF DATE_PARTS_4_4 FOR VALUES from (1) TO (10);
CREATE TABLE DATE_PARTS_4_4_2 PARTITION OF DATE_PARTS_4_4 FOR VALUES from (10) TO (20);
CREATE TABLE DATE_PARTS_4_4_3 PARTITION OF DATE_PARTS_4_4 FOR VALUES from (20) TO (32);
CREATE TABLE DATE_PARTS_4_4_4 PARTITION OF DATE_PARTS_4_4 default;
CREATE TABLE DATE_PARTS_4_5_1 PARTITION OF DATE_PARTS_4_5 FOR VALUES from (1) TO (10);
CREATE TABLE DATE_PARTS_4_5_2 PARTITION OF DATE_PARTS_4_5 FOR VALUES from (10) TO (20);
CREATE TABLE DATE_PARTS_4_5_3 PARTITION OF DATE_PARTS_4_5 FOR VALUES from (20) TO (32);
CREATE TABLE DATE_PARTS_4_5_4 PARTITION OF DATE_PARTS_4_5 default;

insert into DATE_PARTS select i, extract(year from dt), extract(month from dt), extract(day from dt), NULL from (select i, '2002-01-01'::date + i * interval '1 day' day as dt from generate_series(1, 3650) as i) as t;

-- Expected total parts 
select * from DATE_PARTS where month between 1 and 3 order by id;

-- Expected total parts
select * from DATE_PARTS where month between 1 and 4 order by id;

-- Expected total parts
select * from DATE_PARTS where year = 2003 and month between 1 and 4 order by id;

-- Only default for year
select * from DATE_PARTS where year = 1999 order by id;

-- Only default for month
select * from DATE_PARTS where month = 13 order by id;

-- Default for both year and month
select * from DATE_PARTS where year = 1999 and month = 13 order by id;

--  Only default part for day
select * from DATE_PARTS where day = 40 order by id;

-- General predicate
select * from DATE_PARTS where month = 1 union all select * from DATE_PARTS where month > 3 union all select * from DATE_PARTS where month in (0,1,2) union all select * from DATE_PARTS where month is null;

-- Equality predicate
 select * from DATE_PARTS where month = 3 order by id;

insert into DATE_PARTS values (-1, 2004, 11, 30, NULL);
select * from date_parts_1 where id < 0;
select * from date_parts_1_4 where id < 0;
select * from date_parts_1_4_4 where id < 0;
select * from date_parts_1_4_3 where id < 0;

-- Default year
insert into DATE_PARTS values (-2, 1999, 11, 30, NULL);
select * from date_parts_4 where id < 0;
select * from date_parts_4_4 where id < 0;
select * from date_parts_4_4_4 where id < 0;
select * from date_parts_4_4_3 where id < 0;

-- Default month
insert into DATE_PARTS values (-3, 2004, 20, 30, NULL);
select * from date_parts_1 where id < 0;
select * from date_parts_1_5 where id < 0;
select * from date_parts_1_5_2 where id < 0;
select * from date_parts_1_5_3 where id < 0;

-- Default day
insert into DATE_PARTS values (-4, 2004, 10, 50, NULL);
select * from date_parts_1 where id < 0;
select * from date_parts_1_4 where id < 0;
select * from date_parts_1_4_1 where id < 0;
select * from date_parts_1_4_4 where id < 0;

-- Default everything
insert into DATE_PARTS values (-5, 1999, 20, 50, NULL);
select * from date_parts_4 where id < 0;
select * from date_parts_4_5 where id < 0;
select * from date_parts_4_5_1 where id < 0;
select * from date_parts_4_5_4 where id < 0;

-- Default month + day but not year
insert into DATE_PARTS values (-6, 2002, 20, 50, NULL);
select * from date_parts_1 where id < 0;
select * from date_parts_1_5 where id < 0;
select * from date_parts_1_5_1 where id < 0;
select * from date_parts_1_5_4 where id < 0;




-- 3 level partitions, range-list-range
-- Check for all the different number of partition selections
DROP TABLE IF EXISTS DATE_PARTS;
CREATE TABLE DATE_PARTS (id int, year int, month int, day int, region text)
PARTITION BY RANGE (year);

-- 1 
CREATE TABLE DATE_PARTS_1 PARTITION OF DATE_PARTS FOR VALUES from ('2002') TO ('2006') PARTITION BY LIST (month);
CREATE TABLE DATE_PARTS_2 PARTITION OF DATE_PARTS FOR VALUES from ('2006') TO ('2010') PARTITION BY LIST (month);
CREATE TABLE DATE_PARTS_3 PARTITION OF DATE_PARTS FOR VALUES from ('2010') TO ('2012') PARTITION BY LIST (month);
CREATE TABLE DATE_PARTS_4 PARTITION OF DATE_PARTS default PARTITION BY LIST (month);

-- 2
CREATE TABLE DATE_PARTS_1_1 PARTITION OF DATE_PARTS_1 FOR VALUES IN (1,2,3) PARTITION BY HASH (day);
CREATE TABLE DATE_PARTS_1_2 PARTITION OF DATE_PARTS_1 FOR VALUES IN (4,5,6) PARTITION BY HASH (day);
CREATE TABLE DATE_PARTS_1_3 PARTITION OF DATE_PARTS_1 FOR VALUES IN (7,8,9) PARTITION BY HASH (day);
CREATE TABLE DATE_PARTS_1_4 PARTITION OF DATE_PARTS_1 FOR VALUES IN (10,11,12) PARTITION BY HASH (day);
CREATE TABLE DATE_PARTS_1_5 PARTITION OF DATE_PARTS_1 default PARTITION BY HASH (day);

-- 3
CREATE TABLE DATE_PARTS_1_1_1 PARTITION OF DATE_PARTS_1_1 FOR VALUES WITH (modulus 3, remainder 0);
CREATE TABLE DATE_PARTS_1_1_2 PARTITION OF DATE_PARTS_1_1 FOR VALUES WITH (modulus 3, remainder 1);
CREATE TABLE DATE_PARTS_1_1_3 PARTITION OF DATE_PARTS_1_1 FOR VALUES WITH (modulus 3, remainder 2);
CREATE TABLE DATE_PARTS_1_2_1 PARTITION OF DATE_PARTS_1_2 FOR VALUES WITH (modulus 3, remainder 0);
CREATE TABLE DATE_PARTS_1_2_2 PARTITION OF DATE_PARTS_1_2 FOR VALUES WITH (modulus 3, remainder 1);
CREATE TABLE DATE_PARTS_1_2_3 PARTITION OF DATE_PARTS_1_2 FOR VALUES WITH (modulus 3, remainder 2);
CREATE TABLE DATE_PARTS_1_3_1 PARTITION OF DATE_PARTS_1_3 FOR VALUES WITH (modulus 3, remainder 0);
CREATE TABLE DATE_PARTS_1_3_2 PARTITION OF DATE_PARTS_1_3 FOR VALUES WITH (modulus 3, remainder 1);
CREATE TABLE DATE_PARTS_1_3_3 PARTITION OF DATE_PARTS_1_3 FOR VALUES WITH (modulus 3, remainder 2);
CREATE TABLE DATE_PARTS_1_4_1 PARTITION OF DATE_PARTS_1_4 FOR VALUES WITH (modulus 3, remainder 0);
CREATE TABLE DATE_PARTS_1_4_2 PARTITION OF DATE_PARTS_1_4 FOR VALUES WITH (modulus 3, remainder 1);
CREATE TABLE DATE_PARTS_1_4_3 PARTITION OF DATE_PARTS_1_4 FOR VALUES WITH (modulus 3, remainder 2);
CREATE TABLE DATE_PARTS_1_5_1 PARTITION OF DATE_PARTS_1_5 FOR VALUES WITH (modulus 3, remainder 0);
CREATE TABLE DATE_PARTS_1_5_2 PARTITION OF DATE_PARTS_1_5 FOR VALUES WITH (modulus 3, remainder 1);
CREATE TABLE DATE_PARTS_1_5_3 PARTITION OF DATE_PARTS_1_5 FOR VALUES WITH (modulus 3, remainder 2);

-- 2
CREATE TABLE DATE_PARTS_2_1 PARTITION OF DATE_PARTS_2 FOR VALUES IN (1,2,3) PARTITION BY HASH (day);
CREATE TABLE DATE_PARTS_2_2 PARTITION OF DATE_PARTS_2 FOR VALUES IN (4,5,6) PARTITION BY HASH (day);
CREATE TABLE DATE_PARTS_2_3 PARTITION OF DATE_PARTS_2 FOR VALUES IN (7,8,9) PARTITION BY HASH (day);
CREATE TABLE DATE_PARTS_2_4 PARTITION OF DATE_PARTS_2 FOR VALUES IN (10,11,12) PARTITION BY HASH (day);
CREATE TABLE DATE_PARTS_2_5 PARTITION OF DATE_PARTS_2 default PARTITION BY HASH (day);

-- 3
CREATE TABLE DATE_PARTS_2_1_1 PARTITION OF DATE_PARTS_2_1 FOR VALUES WITH (modulus 3, remainder 0);
CREATE TABLE DATE_PARTS_2_1_2 PARTITION OF DATE_PARTS_2_1 FOR VALUES WITH (modulus 3, remainder 1);
CREATE TABLE DATE_PARTS_2_1_3 PARTITION OF DATE_PARTS_2_1 FOR VALUES WITH (modulus 3, remainder 2);
CREATE TABLE DATE_PARTS_2_2_1 PARTITION OF DATE_PARTS_2_2 FOR VALUES WITH (modulus 3, remainder 0);
CREATE TABLE DATE_PARTS_2_2_2 PARTITION OF DATE_PARTS_2_2 FOR VALUES WITH (modulus 3, remainder 1);
CREATE TABLE DATE_PARTS_2_2_3 PARTITION OF DATE_PARTS_2_2 FOR VALUES WITH (modulus 3, remainder 2);
CREATE TABLE DATE_PARTS_2_3_1 PARTITION OF DATE_PARTS_2_3 FOR VALUES WITH (modulus 3, remainder 0);
CREATE TABLE DATE_PARTS_2_3_2 PARTITION OF DATE_PARTS_2_3 FOR VALUES WITH (modulus 3, remainder 1);
CREATE TABLE DATE_PARTS_2_3_3 PARTITION OF DATE_PARTS_2_3 FOR VALUES WITH (modulus 3, remainder 2);
CREATE TABLE DATE_PARTS_2_4_1 PARTITION OF DATE_PARTS_2_4 FOR VALUES WITH (modulus 3, remainder 0);
CREATE TABLE DATE_PARTS_2_4_2 PARTITION OF DATE_PARTS_2_4 FOR VALUES WITH (modulus 3, remainder 1);
CREATE TABLE DATE_PARTS_2_4_3 PARTITION OF DATE_PARTS_2_4 FOR VALUES WITH (modulus 3, remainder 2);
CREATE TABLE DATE_PARTS_2_5_1 PARTITION OF DATE_PARTS_2_5 FOR VALUES WITH (modulus 3, remainder 0);
CREATE TABLE DATE_PARTS_2_5_2 PARTITION OF DATE_PARTS_2_5 FOR VALUES WITH (modulus 3, remainder 1);
CREATE TABLE DATE_PARTS_2_5_3 PARTITION OF DATE_PARTS_2_5 FOR VALUES WITH (modulus 3, remainder 2);

-- 2
CREATE TABLE DATE_PARTS_3_1 PARTITION OF DATE_PARTS_3 FOR VALUES IN (1,2,3) PARTITION BY HASH (day);
CREATE TABLE DATE_PARTS_3_2 PARTITION OF DATE_PARTS_3 FOR VALUES IN (4,5,6) PARTITION BY HASH (day);
CREATE TABLE DATE_PARTS_3_3 PARTITION OF DATE_PARTS_3 FOR VALUES IN (7,8,9) PARTITION BY HASH (day);
CREATE TABLE DATE_PARTS_3_4 PARTITION OF DATE_PARTS_3 FOR VALUES IN (10,11,12) PARTITION BY HASH (day);
CREATE TABLE DATE_PARTS_3_5 PARTITION OF DATE_PARTS_3 default PARTITION BY HASH (day);

-- 3
CREATE TABLE DATE_PARTS_3_1_1 PARTITION OF DATE_PARTS_3_1 FOR VALUES WITH (modulus 3, remainder 0);
CREATE TABLE DATE_PARTS_3_1_2 PARTITION OF DATE_PARTS_3_1 FOR VALUES WITH (modulus 3, remainder 1);
CREATE TABLE DATE_PARTS_3_1_3 PARTITION OF DATE_PARTS_3_1 FOR VALUES WITH (modulus 3, remainder 2);
CREATE TABLE DATE_PARTS_3_2_1 PARTITION OF DATE_PARTS_3_2 FOR VALUES WITH (modulus 3, remainder 0);
CREATE TABLE DATE_PARTS_3_2_2 PARTITION OF DATE_PARTS_3_2 FOR VALUES WITH (modulus 3, remainder 1);
CREATE TABLE DATE_PARTS_3_2_3 PARTITION OF DATE_PARTS_3_2 FOR VALUES WITH (modulus 3, remainder 2);
CREATE TABLE DATE_PARTS_3_3_1 PARTITION OF DATE_PARTS_3_3 FOR VALUES WITH (modulus 3, remainder 0);
CREATE TABLE DATE_PARTS_3_3_2 PARTITION OF DATE_PARTS_3_3 FOR VALUES WITH (modulus 3, remainder 1);
CREATE TABLE DATE_PARTS_3_3_3 PARTITION OF DATE_PARTS_3_3 FOR VALUES WITH (modulus 3, remainder 2);
CREATE TABLE DATE_PARTS_3_4_1 PARTITION OF DATE_PARTS_3_4 FOR VALUES WITH (modulus 3, remainder 0);
CREATE TABLE DATE_PARTS_3_4_2 PARTITION OF DATE_PARTS_3_4 FOR VALUES WITH (modulus 3, remainder 1);
CREATE TABLE DATE_PARTS_3_4_3 PARTITION OF DATE_PARTS_3_4 FOR VALUES WITH (modulus 3, remainder 2);
CREATE TABLE DATE_PARTS_3_5_1 PARTITION OF DATE_PARTS_3_5 FOR VALUES WITH (modulus 3, remainder 0);
CREATE TABLE DATE_PARTS_3_5_2 PARTITION OF DATE_PARTS_3_5 FOR VALUES WITH (modulus 3, remainder 1);
CREATE TABLE DATE_PARTS_3_5_3 PARTITION OF DATE_PARTS_3_5 FOR VALUES WITH (modulus 3, remainder 2);

-- 2
CREATE TABLE DATE_PARTS_4_1 PARTITION OF DATE_PARTS_4 FOR VALUES IN (1,2,3) PARTITION BY HASH (day);
CREATE TABLE DATE_PARTS_4_2 PARTITION OF DATE_PARTS_4 FOR VALUES IN (4,5,6) PARTITION BY HASH (day);
CREATE TABLE DATE_PARTS_4_3 PARTITION OF DATE_PARTS_4 FOR VALUES IN (7,8,9) PARTITION BY HASH (day);
CREATE TABLE DATE_PARTS_4_4 PARTITION OF DATE_PARTS_4 FOR VALUES IN (10,11,12) PARTITION BY HASH (day);
CREATE TABLE DATE_PARTS_4_5 PARTITION OF DATE_PARTS_4 default PARTITION BY HASH (day);

-- 3
CREATE TABLE DATE_PARTS_4_1_1 PARTITION OF DATE_PARTS_4_1 FOR VALUES WITH (modulus 3, remainder 0);
CREATE TABLE DATE_PARTS_4_1_2 PARTITION OF DATE_PARTS_4_1 FOR VALUES WITH (modulus 3, remainder 1);
CREATE TABLE DATE_PARTS_4_1_3 PARTITION OF DATE_PARTS_4_1 FOR VALUES WITH (modulus 3, remainder 2);
CREATE TABLE DATE_PARTS_4_2_1 PARTITION OF DATE_PARTS_4_2 FOR VALUES WITH (modulus 3, remainder 0);
CREATE TABLE DATE_PARTS_4_2_2 PARTITION OF DATE_PARTS_4_2 FOR VALUES WITH (modulus 3, remainder 1);
CREATE TABLE DATE_PARTS_4_2_3 PARTITION OF DATE_PARTS_4_2 FOR VALUES WITH (modulus 3, remainder 2);
CREATE TABLE DATE_PARTS_4_3_1 PARTITION OF DATE_PARTS_4_3 FOR VALUES WITH (modulus 3, remainder 0);
CREATE TABLE DATE_PARTS_4_3_2 PARTITION OF DATE_PARTS_4_3 FOR VALUES WITH (modulus 3, remainder 1);
CREATE TABLE DATE_PARTS_4_3_3 PARTITION OF DATE_PARTS_4_3 FOR VALUES WITH (modulus 3, remainder 2);
CREATE TABLE DATE_PARTS_4_4_1 PARTITION OF DATE_PARTS_4_4 FOR VALUES WITH (modulus 3, remainder 0);
CREATE TABLE DATE_PARTS_4_4_2 PARTITION OF DATE_PARTS_4_4 FOR VALUES WITH (modulus 3, remainder 1);
CREATE TABLE DATE_PARTS_4_4_3 PARTITION OF DATE_PARTS_4_4 FOR VALUES WITH (modulus 3, remainder 2);
CREATE TABLE DATE_PARTS_4_5_1 PARTITION OF DATE_PARTS_4_5 FOR VALUES WITH (modulus 3, remainder 0);
CREATE TABLE DATE_PARTS_4_5_2 PARTITION OF DATE_PARTS_4_5 FOR VALUES WITH (modulus 3, remainder 1);
CREATE TABLE DATE_PARTS_4_5_3 PARTITION OF DATE_PARTS_4_5 FOR VALUES WITH (modulus 3, remainder 2);

insert into DATE_PARTS select i, extract(year from dt), extract(month from dt), extract(day from dt), NULL from (select i, '2002-01-01'::date + i * interval '1 day' day as dt from generate_series(1, 3650) as i) as t;

-- Expected total parts 
select * from DATE_PARTS where month between 1 and 3 order by id;

-- Expected total parts 
select * from DATE_PARTS where month between 1 and 4 order by id;

-- Expected total parts
select * from DATE_PARTS where year = 2003 and month between 1 and 4 order by id;

--  Only default for year
select * from DATE_PARTS where year = 1999 order by id;

--  Only default for month
select * from DATE_PARTS where month = 13 order by id;

--  Default for both year and month
select * from DATE_PARTS where year = 1999 and month = 13 order by id;

--  Only default part for day
select * from DATE_PARTS where day = 40 order by id;

-- General predicate
select * from DATE_PARTS where month = 1 union all select * from DATE_PARTS where month > 3 union all select * from DATE_PARTS where month in (0,1,2) union all select * from DATE_PARTS where month is null;

-- Equality predicate
 select * from DATE_PARTS where month = 3 order by id;

insert into DATE_PARTS values (-1, 2004, 11, 30, NULL);
select * from date_parts_1 where id < 0;
select * from date_parts_1_4 where id < 0;
select * from date_parts_1_4_4 where id < 0;
select * from date_parts_1_4_1 where id < 0;

-- Default year
insert into DATE_PARTS values (-2, 1999, 11, 30, NULL);
select * from date_parts_4 where id < 0;
select * from date_parts_4_4 where id < 0;
select * from date_parts_4_4_4 where id < 0;
select * from date_parts_4_4_1 where id < 0;

-- Default month
insert into DATE_PARTS values (-3, 2004, 20, 30, NULL);
select * from date_parts_1 where id < 0;
select * from date_parts_1_5 where id < 0;
select * from date_parts_1_5_2 where id < 0;
select * from date_parts_1_5_1 where id < 0;

-- Default day
insert into DATE_PARTS values (-4, 2004, 10, 50, NULL);
select * from date_parts_1 where id < 0;
select * from date_parts_1_4 where id < 0;
select * from date_parts_1_4_1 where id < 0;
select * from date_parts_1_4_3 where id < 0;

-- Default everything
insert into DATE_PARTS values (-5, 1999, 20, 50, NULL);
select * from date_parts_4 where id < 0;
select * from date_parts_4_5 where id < 0;
select * from date_parts_4_5_1 where id < 0;
select * from date_parts_4_5_3 where id < 0;

-- Default month + day but not year
insert into DATE_PARTS values (-6, 2002, 20, 50, NULL);
select * from date_parts_1 where id < 0;
select * from date_parts_1_5 where id < 0;
select * from date_parts_1_5_1 where id < 0;
select * from date_parts_1_5_3 where id < 0;

DROP TABLE IF EXISTS DATE_PARTS;


CREATE TABLE range_list (a int,b timestamp,c varchar(10)) PARTITION BY HASH (a);
 
CREATE TABLE range_pa1 PARTITION OF range_list FOR VALUES WITH (modulus 3, remainder 0)  PARTITION BY RANGE (b);
CREATE TABLE range_pa2 PARTITION OF range_list FOR VALUES WITH (modulus 3, remainder 1)  PARTITION BY RANGE (b);
CREATE TABLE range_pa3 PARTITION OF range_list FOR VALUES WITH (modulus 3, remainder 2)  PARTITION BY RANGE (b);

CREATE TABLE range_pa1_1  PARTITION OF range_pa1 FOR VALUES from ('2000-01-01') TO ('2010-01-01')  PARTITION BY LIST(c);
CREATE TABLE range_pa1_2  PARTITION OF range_pa1 FOR VALUES from ('2010-01-01') TO ('2011-01-01')  PARTITION BY LIST(c);
CREATE TABLE range_pa1_1_1 PARTITION OF range_pa1_1 FOR VALUES IN ('F');
CREATE TABLE range_pa1_1_2 PARTITION OF range_pa1_1 FOR VALUES IN ('M');
CREATE TABLE range_pa1_1_3 PARTITION OF range_pa1_1 default;
CREATE TABLE range_pa1_2_1 PARTITION OF range_pa1_2 FOR VALUES IN ('F');
CREATE TABLE range_pa1_2_2 PARTITION OF range_pa1_2 FOR VALUES IN ('M');
CREATE TABLE range_pa1_2_3 PARTITION OF range_pa1_2 default;

CREATE TABLE range_pa2_1  PARTITION OF range_pa2 FOR VALUES from ('2000-01-01') TO ('2010-01-01') PARTITION BY LIST(c);
CREATE TABLE range_pa2_2  PARTITION OF range_pa2 FOR VALUES from ('2010-01-01') TO ('2011-01-01')  PARTITION BY LIST(c);
CREATE TABLE range_pa2_1_1 PARTITION OF range_pa2_1 FOR VALUES IN ('F');
CREATE TABLE range_pa2_1_2 PARTITION OF range_pa2_1 FOR VALUES IN ('M');
CREATE TABLE range_pa2_1_3 PARTITION OF range_pa2_1 default;
CREATE TABLE range_pa2_2_1 PARTITION OF range_pa2_2 FOR VALUES IN ('F');
CREATE TABLE range_pa2_2_2 PARTITION OF range_pa2_2 FOR VALUES IN ('M');
CREATE TABLE range_pa2_2_3 PARTITION OF range_pa2_2 default;

CREATE TABLE range_pa3_1  PARTITION OF range_pa3 FOR VALUES from ('2000-01-01') TO ('2010-01-01') PARTITION BY LIST(c);
CREATE TABLE range_pa3_2  PARTITION OF range_pa3 FOR VALUES from ('2010-01-01') TO ('2011-01-01') PARTITION BY LIST(c);
CREATE TABLE range_pa3_1_1 PARTITION OF range_pa3_1 FOR VALUES IN ('F');
CREATE TABLE range_pa3_1_2 PARTITION OF range_pa3_1 FOR VALUES IN ('M');
CREATE TABLE range_pa3_1_3 PARTITION OF range_pa3_1 default;
CREATE TABLE range_pa3_2_1 PARTITION OF range_pa3_2 FOR VALUES IN ('F');
CREATE TABLE range_pa3_2_2 PARTITION OF range_pa3_2 FOR VALUES IN ('M');
CREATE TABLE range_pa3_2_3 PARTITION OF range_pa3_2 default;

insert into range_list values(1,'2005-01-05 5:05','M');
insert into range_list values(1,'2005-01-05 5:05','F');
insert into range_list values(1,'2005-01-05 5:05','C');

insert into range_list values(11,'2005-01-05 5:05','M');
insert into range_list values(11,'2005-01-05 5:05','F');
insert into range_list values(11,'2005-01-05 5:05','C');

insert into range_list values(1,'2015-01-05 5:05','M');
insert into range_list values(1,'2015-01-05 5:05','F');
insert into range_list values(1,'2015-01-05 5:05','C');

insert into range_list values(11,'2015-01-05 5:05','M');
insert into range_list values(11,'2015-01-05 5:05','F');
insert into range_list values(11,'2015-01-05 5:05','C');

analyze range_list;

select * from range_list;

-- top table
select tableoid::regclass from range_list where a = 1 and b < '2009-01-01 00:00:00' order by tableoid::regclass;

-- partitioned table
select tableoid::regclass from range_pa1 where a = 1 and b < '2009-01-01 00:00:00' order by tableoid::regclass;

select tableoid::regclass from range_pa1_1 where a = 1 and b < '2009-01-01 00:00:00' order by tableoid::regclass;

select tableoid::regclass from range_pa1_1_2 where a = 1 and b < '2009-01-01 00:00:00' order by tableoid::regclass;

-- partitioned table，no result
select tableoid::regclass from range_pa2 where a = 1 and b < '2009-01-01 00:00:00' order by tableoid::regclass;

-- create partitioned index
create index on range_list using btree(a);
create index on range_list using btree(b);
create index on range_list using btree(c);

-- top table
select tableoid::regclass from range_list where a = 1 and b < '2009-01-01 00:00:00' order by tableoid::regclass;

-- partitioned table
select tableoid::regclass from range_pa1 where a = 1 and b < '2009-01-01 00:00:00' order by tableoid::regclass;


select tableoid::regclass from range_pa1_1 where a = 1 and b < '2009-01-01 00:00:00' order by tableoid::regclass;

select tableoid::regclass from range_pa1_1_2 where a = 1 and b < '2009-01-01 00:00:00' order by tableoid::regclass;

-- partitioned table，no result
select tableoid::regclass from range_pa2 where a = 1 and b < '2009-01-01 00:00:00' order by tableoid::regclass;

drop table if exists range_list;
set polar_px_optimizer_multilevel_partitioning=0;