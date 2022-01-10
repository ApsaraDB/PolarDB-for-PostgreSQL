--
-- orca_linlj tests
--

-- Mask out Log & timestamp for orca message that has feature not supported.
-- start_matchsubs
-- m/^LOG.*\"Feature/
-- s/^LOG.*\"Feature/\"Feature/
-- end_matchsubs

/*--EXPLAIN_QUERY_BEGIN*/
create schema orca_linlj;
SET search_path to orca_linlj;


drop table t3 cascade;
CREATE TABLE t3 (c1 int, c2 character varying(100) NOT NULL, c3 character varying(100) NOT NULL, c4 character varying(100) NOT NULL, c5 character varying(100) NOT NULL, c6 character varying(100) NOT NULL, c7 character varying(100) NOT NULL, c8 character varying(100) NOT NULL, c9 character varying(100) NOT NULL, c10 character varying(100) NOT NULL);
insert into t3 select generate_series(1,1000),repeat('hello',20),repeat('hello',20),repeat('hello',20),repeat('hello',20),repeat('hello',20),repeat('hello',20),repeat('hello',20),repeat('hello',20),repeat('hello',20);


drop table t4 cascade;
CREATE TABLE t4 (c1 int, c2 character varying(100) NOT NULL, c3 character varying(100) NOT NULL, c4 character varying(100) NOT NULL, c5 character varying(100) NOT NULL, c6 character varying(100) NOT NULL, c7 character varying(100) NOT NULL, c8 character varying(100) NOT NULL, c9 character varying(100) NOT NULL, c10 char(100));
insert into t4 select generate_series(5,1000),repeat('hello',20),repeat('hello',20),repeat('hello',20),repeat('hello',20),repeat('hello',20),repeat('hello',20),repeat('hello',20),repeat('hello',20),repeat('hello',20);
--select 1.0*pg_relation_size('t4') /1024/1024;


ANALYZE t3;
ANALYZE t4;

set polar_px_scan_unit_size=1;
set polar_px_optimizer_enable_hashjoin=0;
set polar_px_optimizer_enable_bitmapscan=0;
set polar_px_enable_left_index_nestloop_join=1;

--left
SELECT t31.c1 as t3_1, t4.c1 as t4_1
FROM (
  select * 
  from t3 
  where c4='hellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohello' 
  and c1 < 100
) t31 
left join t4 
on t31.c2=t4.c2 and t31.c1=t4.c1 
where t31.c2 ='hellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohello' 
order by t31.c1, t4.c1 
limit 10;

SELECT t31.c1 as t3_1, t4.c1 as t4_1
FROM (
  select * 
  from t3 
  where c4='hellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohello' 
  and c1 < 100
) t31 
left join t4 
on t31.c2=t4.c2 and t31.c1=t4.c1 
--where t31.c2 ='hellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohello' 
order by t31.c1, t4.c1 
limit 10;

--inner
SELECT t31.c1 as t3_1, t4.c1 as t4_1
FROM (
  select * 
  from t3 
  where c4='hellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohello' 
  and c1 < 100
) t31 
join t4 
on t31.c2=t4.c2 and t31.c1=t4.c1 
where t31.c2 ='hellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohello' 
order by t31.c1, t4.c1 
limit 10;

SELECT t31.c1 as t3_1, t4.c1 as t4_1
FROM (
  select * 
  from t3 
  where c4='hellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohello' 
  and c1 < 100
) t31 
join t4 
on t31.c2=t4.c2 and t31.c1=t4.c1 
--where t31.c2 ='hellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohello' 
order by t31.c1, t4.c1 
limit 10;



--right
SELECT t31.c1 as t3_1, t4.c1 as t4_1
FROM (
  select * 
  from t3 
  where c4='hellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohello' 
  and c1 < 100
) t31 
right join t4 
on t31.c2=t4.c2 and t31.c1=t4.c1 
where t31.c2 ='hellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohello' 
order by t31.c1, t4.c1 
limit 10;

SELECT t31.c1 as t3_1, t4.c1 as t4_1
FROM (
  select * 
  from t3 
  where c4='hellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohello' 
  and c1 < 100
) t31 
right join t4 
on t31.c2=t4.c2 and t31.c1=t4.c1 
--where t31.c2 ='hellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohello' 
order by t31.c1, t4.c1 
limit 10;






--use index
CREATE INDEX t3_id4 ON t3 USING btree (c2, c3, c4);
CREATE INDEX t4_id4 ON t4 USING btree (c2);

ANALYZE t3;
ANALYZE t4;

--left
SELECT t31.c1 as t3_1, t4.c1 as t4_1
FROM (
  select * 
  from t3 
  where c4='hellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohello' 
  and c1 < 100
) t31 
left join t4 
on t31.c2=t4.c2 and t31.c1=t4.c1 
where t31.c2 ='hellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohello' 
order by t31.c1, t4.c1 
limit 10;

SELECT t31.c1 as t3_1, t4.c1 as t4_1
FROM (
  select * 
  from t3 
  where c4='hellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohello' 
  and c1 < 100
) t31 
left join t4 
on t31.c2=t4.c2 and t31.c1=t4.c1 
--where t31.c2 ='hellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohello' 
order by t31.c1, t4.c1 
limit 10;

--inner
SELECT t31.c1 as t3_1, t4.c1 as t4_1
FROM (
  select * 
  from t3 
  where c4='hellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohello' 
  and c1 < 100
) t31 
join t4 
on t31.c2=t4.c2 and t31.c1=t4.c1 
where t31.c2 ='hellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohello' 
order by t31.c1, t4.c1 
limit 10;

SELECT t31.c1 as t3_1, t4.c1 as t4_1
FROM (
  select * 
  from t3 
  where c4='hellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohello' 
  and c1 < 100
) t31 
join t4 
on t31.c2=t4.c2 and t31.c1=t4.c1 
--where t31.c2 ='hellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohello' 
order by t31.c1, t4.c1 
limit 10;



--right
SELECT t31.c1 as t3_1, t4.c1 as t4_1
FROM (
  select * 
  from t3 
  where c4='hellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohello' 
  and c1 < 100
) t31 
right join t4 
on t31.c2=t4.c2 and t31.c1=t4.c1 
where t31.c2 ='hellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohello' 
order by t31.c1, t4.c1 
limit 10;

SELECT t31.c1 as t3_1, t4.c1 as t4_1
FROM (
  select * 
  from t3 
  where c4='hellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohello' 
  and c1 < 100
) t31 
right join t4 
on t31.c2=t4.c2 and t31.c1=t4.c1 
--where t31.c2 ='hellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohello' 
order by t31.c1, t4.c1 
limit 10;

-- start_ignore
set client_min_messages='warning';
DROP SCHEMA orca_linlj CASCADE;
-- end_ignore