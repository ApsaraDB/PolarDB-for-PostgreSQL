create extension if not exists pageinspect;

-- config
/*--POLAR_ENABLE_PX*/
set polar_enable_px = on;
alter system set polar_px_enable_replay_wait = on;
select pg_reload_conf();
select pg_sleep(1);

drop table if exists px_test cascade;
create table px_test(id int);
insert into px_test select generate_series(1,1000);


-- use polar_px_enable_create_table_as guc
-- 1 CREATE MATERIALIZED VIEW
drop materialized view if exists px_mv_1;
drop materialized view if exists px_mv_2;

set polar_px_enable_create_table_as = 1;
explain create materialized view px_mv_1 as select * from px_test;
create materialized view px_mv_1 as select * from px_test;

set polar_px_enable_create_table_as = 0;
explain create materialized view px_mv_2 as select * from px_test;
create materialized view px_mv_2 as select * from px_test;

select count(*) from heap_page_items(get_raw_page('px_mv_1', 0)) as a, heap_page_items(get_raw_page('px_mv_2', 0)) as b where a.lp = b.lp and a.t_data = b.t_data;
select count(*) from heap_page_items(get_raw_page('px_mv_1', 1)) as a, heap_page_items(get_raw_page('px_mv_2', 1)) as b where a.lp = b.lp and a.t_data = b.t_data;
select count(*) from heap_page_items(get_raw_page('px_mv_1', 2)) as a, heap_page_items(get_raw_page('px_mv_2', 2)) as b where a.lp = b.lp and a.t_data = b.t_data;
select count(*) from heap_page_items(get_raw_page('px_mv_1', 3)) as a, heap_page_items(get_raw_page('px_mv_2', 3)) as b where a.lp = b.lp and a.t_data = b.t_data;
select count(*) from heap_page_items(get_raw_page('px_mv_1', 4)) as a, heap_page_items(get_raw_page('px_mv_2', 4)) as b where a.lp = b.lp and a.t_data = b.t_data;


-- 2 CREATE MATERIALIZED VIEW with aggregate functions
drop materialized view if exists px_mv_1;
drop materialized view if exists px_mv_2;

set polar_px_enable_create_table_as = 1;
explain create materialized view px_mv_1(id_sum, id_count) as select sum(id), count(id) from px_test;
create materialized view px_mv_1(id_sum, id_count) as select sum(id), count(id) from px_test;

set polar_px_enable_create_table_as = 0;
explain create materialized view px_mv_2(id_sum, id_count) as select sum(id), count(id) from px_test;
create materialized view px_mv_2(id_sum, id_count) as select sum(id), count(id) from px_test;

select count(*) from heap_page_items(get_raw_page('px_mv_1', 0)) as a, heap_page_items(get_raw_page('px_mv_2', 0)) as b where a.lp = b.lp and a.t_data = b.t_data;


-- 3 CREATE TABLE AS
drop table if exists px_t_1;
drop table if exists px_t_2;

set polar_px_enable_create_table_as = 1;
explain create table px_t_1 as select * from px_test;
create table px_t_1 as select * from px_test;

set polar_px_enable_create_table_as = 0;
explain create table px_t_2 as select * from px_test;
create table px_t_2 as select * from px_test;

select count(*) from heap_page_items(get_raw_page('px_t_1', 0)) as a, heap_page_items(get_raw_page('px_t_2', 0)) as b where a.lp = b.lp and a.t_data = b.t_data;
select count(*) from heap_page_items(get_raw_page('px_t_1', 1)) as a, heap_page_items(get_raw_page('px_t_2', 1)) as b where a.lp = b.lp and a.t_data = b.t_data;
select count(*) from heap_page_items(get_raw_page('px_t_1', 2)) as a, heap_page_items(get_raw_page('px_t_2', 2)) as b where a.lp = b.lp and a.t_data = b.t_data;
select count(*) from heap_page_items(get_raw_page('px_t_1', 3)) as a, heap_page_items(get_raw_page('px_t_2', 3)) as b where a.lp = b.lp and a.t_data = b.t_data;
select count(*) from heap_page_items(get_raw_page('px_t_1', 4)) as a, heap_page_items(get_raw_page('px_t_2', 4)) as b where a.lp = b.lp and a.t_data = b.t_data;


-- 4 SELECT INTO
drop table if exists px_t_1;
drop table if exists px_t_2;

set polar_px_enable_create_table_as = 1;
explain select * into px_t_1 from px_test;
select * into px_t_1 from px_test;

set polar_px_enable_create_table_as = 0;
explain select * into px_t_2 from px_test;
select * into px_t_2 from px_test;

select count(*) from heap_page_items(get_raw_page('px_t_1', 0)) as a, heap_page_items(get_raw_page('px_t_2', 0)) as b where a.lp = b.lp and a.t_data = b.t_data;
select count(*) from heap_page_items(get_raw_page('px_t_1', 1)) as a, heap_page_items(get_raw_page('px_t_2', 1)) as b where a.lp = b.lp and a.t_data = b.t_data;
select count(*) from heap_page_items(get_raw_page('px_t_1', 2)) as a, heap_page_items(get_raw_page('px_t_2', 2)) as b where a.lp = b.lp and a.t_data = b.t_data;
select count(*) from heap_page_items(get_raw_page('px_t_1', 3)) as a, heap_page_items(get_raw_page('px_t_2', 3)) as b where a.lp = b.lp and a.t_data = b.t_data;
select count(*) from heap_page_items(get_raw_page('px_t_1', 4)) as a, heap_page_items(get_raw_page('px_t_2', 4)) as b where a.lp = b.lp and a.t_data = b.t_data;


-- use polar_enable_create_table_as_bulk_insert guc
-- 5 polar_enable_create_table_as_bulk_insert
drop materialized view if exists px_mv_1;
drop materialized view if exists px_mv_2;
set polar_px_enable_create_table_as = 0;

set polar_enable_create_table_as_bulk_insert = 0;
create materialized view px_mv_1 as select * from px_test;
set polar_enable_create_table_as_bulk_insert = 1;
create materialized view px_mv_2 as select * from px_test;

select count(*) from heap_page_items(get_raw_page('px_mv_1', 0)) as a, heap_page_items(get_raw_page('px_mv_2', 0)) as b where a.lp = b.lp and a.t_data = b.t_data;
select count(*) from heap_page_items(get_raw_page('px_mv_1', 1)) as a, heap_page_items(get_raw_page('px_mv_2', 1)) as b where a.lp = b.lp and a.t_data = b.t_data;
select count(*) from heap_page_items(get_raw_page('px_mv_1', 2)) as a, heap_page_items(get_raw_page('px_mv_2', 2)) as b where a.lp = b.lp and a.t_data = b.t_data;
select count(*) from heap_page_items(get_raw_page('px_mv_1', 3)) as a, heap_page_items(get_raw_page('px_mv_2', 3)) as b where a.lp = b.lp and a.t_data = b.t_data;
select count(*) from heap_page_items(get_raw_page('px_mv_1', 4)) as a, heap_page_items(get_raw_page('px_mv_2', 4)) as b where a.lp = b.lp and a.t_data = b.t_data;


-- 6 polar_px_enable_create_table_as + polar_enable_create_table_as_bulk_insert
drop materialized view if exists px_mv_1;
drop materialized view if exists px_mv_2;

set polar_enable_create_table_as_bulk_insert = 0;
set polar_px_enable_create_table_as = 0;
explain create materialized view px_mv_1 as select * from px_test;
create materialized view px_mv_1 as select * from px_test;

set polar_px_enable_create_table_as = 1;
set polar_enable_create_table_as_bulk_insert = 1;
explain create materialized view px_mv_2 as select * from px_test;
create materialized view px_mv_2 as select * from px_test;

select count(*) from heap_page_items(get_raw_page('px_mv_1', 0)) as a, heap_page_items(get_raw_page('px_mv_2', 0)) as b where a.lp = b.lp and a.t_data = b.t_data;
select count(*) from heap_page_items(get_raw_page('px_mv_1', 1)) as a, heap_page_items(get_raw_page('px_mv_2', 1)) as b where a.lp = b.lp and a.t_data = b.t_data;
select count(*) from heap_page_items(get_raw_page('px_mv_1', 2)) as a, heap_page_items(get_raw_page('px_mv_2', 2)) as b where a.lp = b.lp and a.t_data = b.t_data;
select count(*) from heap_page_items(get_raw_page('px_mv_1', 3)) as a, heap_page_items(get_raw_page('px_mv_2', 3)) as b where a.lp = b.lp and a.t_data = b.t_data;
select count(*) from heap_page_items(get_raw_page('px_mv_1', 4)) as a, heap_page_items(get_raw_page('px_mv_2', 4)) as b where a.lp = b.lp and a.t_data = b.t_data;


-- refresh materialized view testing
set polar_enable_create_table_as_bulk_insert = 0;
set polar_px_enable_create_table_as = 0;
refresh materialized view px_mv_1;

set polar_px_enable_create_table_as = 1;
set polar_enable_create_table_as_bulk_insert = 1;
refresh materialized view px_mv_2;

select count(*) from heap_page_items(get_raw_page('px_mv_1', 0)) as a, heap_page_items(get_raw_page('px_mv_2', 0)) as b where a.lp = b.lp and a.t_data = b.t_data;
select count(*) from heap_page_items(get_raw_page('px_mv_1', 1)) as a, heap_page_items(get_raw_page('px_mv_2', 1)) as b where a.lp = b.lp and a.t_data = b.t_data;
select count(*) from heap_page_items(get_raw_page('px_mv_1', 2)) as a, heap_page_items(get_raw_page('px_mv_2', 2)) as b where a.lp = b.lp and a.t_data = b.t_data;
select count(*) from heap_page_items(get_raw_page('px_mv_1', 3)) as a, heap_page_items(get_raw_page('px_mv_2', 3)) as b where a.lp = b.lp and a.t_data = b.t_data;
select count(*) from heap_page_items(get_raw_page('px_mv_1', 4)) as a, heap_page_items(get_raw_page('px_mv_2', 4)) as b where a.lp = b.lp and a.t_data = b.t_data;

-- create table as with oid (fall back from PX)
explain create table ctas with oids as select * from px_test;
create table ctas with oids as select * from px_test;
drop table ctas;

-- create table as with a table with oids
create table t_withoids (id int) with oids;
insert into t_withoids select generate_series(1,1000);

explain create table ctas_1 as select * from t_withoids;
create table ctas_1 as select * from t_withoids;

set polar_px_enable_create_table_as = 0;
explain create table ctas_2 as select * from t_withoids;
create table ctas_2 as select * from t_withoids;
set polar_px_enable_create_table_as = 1;

select count(*) from heap_page_items(get_raw_page('ctas_1', 0)) as a, heap_page_items(get_raw_page('ctas_2', 0)) as b where a.lp = b.lp and a.t_data = b.t_data;
select count(*) from heap_page_items(get_raw_page('ctas_1', 1)) as a, heap_page_items(get_raw_page('ctas_2', 1)) as b where a.lp = b.lp and a.t_data = b.t_data;
select count(*) from heap_page_items(get_raw_page('ctas_1', 2)) as a, heap_page_items(get_raw_page('ctas_2', 2)) as b where a.lp = b.lp and a.t_data = b.t_data;
select count(*) from heap_page_items(get_raw_page('ctas_1', 3)) as a, heap_page_items(get_raw_page('ctas_2', 3)) as b where a.lp = b.lp and a.t_data = b.t_data;
select count(*) from heap_page_items(get_raw_page('ctas_1', 4)) as a, heap_page_items(get_raw_page('ctas_2', 4)) as b where a.lp = b.lp and a.t_data = b.t_data;

drop table ctas_1;
drop table ctas_2;
drop table t_withoids;

-- create table as without oid
explain create table ctas as select * from px_test;
create table ctas as select * from px_test;
drop table ctas;

-- reset GUCs
set polar_enable_create_table_as_bulk_insert = 0;
set polar_px_enable_create_table_as = 0;
set polar_enable_px = off;
alter system reset polar_px_enable_replay_wait;
select pg_reload_conf();
select pg_sleep(1);

drop extension pageinspect;

