/*--EXPLAIN_QUERY_BEGIN*/
create extension if not exists pageinspect;

show polar_bt_write_page_buffer_size;
alter system set polar_bt_write_page_buffer_size = 8;
select pg_reload_conf();
select pg_sleep(1);
show polar_bt_write_page_buffer_size;

-- use polar_px_enable_btbuild guc
-- 1
drop table if exists px_test;
drop table if exists px_test_1;
select id into px_test from generate_series(1, 1000) as id order by id desc;
alter system set polar_px_enable_btbuild = 1;
select pg_reload_conf();
select pg_sleep(1);
create index px_t on px_test(id) with(px_build=on);
select itemoffset, data from bt_page_items('px_t', 1);
select itemoffset, data from bt_page_items('px_t', 2);
select itemoffset, data from bt_page_items('px_t', 3);
select itemoffset, data from bt_page_items('px_t', 4);
alter system reset polar_px_enable_btbuild;
select pg_reload_conf();
select id into px_test_1 from generate_series(1, 1000) as id order by id desc;
create index px_t_1 on px_test_1(id);
select itemoffset, data from bt_page_items('px_t_1', 1);
select itemoffset, data from bt_page_items('px_t_1', 2);
select itemoffset, data from bt_page_items('px_t_1', 3);
select itemoffset, data from bt_page_items('px_t_1', 4);
select count(*) from bt_page_items('px_t', 1) as a, bt_page_items('px_t_1', 1) as b where a.itemoffset = b.itemoffset and a.data = b.data;
select count(*) from bt_page_items('px_t', 2) as a, bt_page_items('px_t_1', 2) as b where a.itemoffset = b.itemoffset and a.data = b.data;
select count(*) from bt_page_items('px_t', 3) as a, bt_page_items('px_t_1', 3) as b where a.itemoffset = b.itemoffset and a.data = b.data;
select count(*) from bt_page_items('px_t', 4) as a, bt_page_items('px_t_1', 4) as b where a.itemoffset = b.itemoffset and a.data = b.data;
drop index px_t;
drop index px_t_1;

-- 2
drop table if exists px_test;
drop table if exists px_test_1;
create table px_test(id1 int, id2 int, id3 int, id4 int);
insert into px_test(id1, id2, id3, id4) select generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000);
alter system set polar_px_enable_btbuild = 1;
select pg_reload_conf();
select pg_sleep(1);
create index px_t on px_test(id1, id2, id3, id4) with(px_build=on);
select itemoffset, data from bt_page_items('px_t', 1);
select itemoffset, data from bt_page_items('px_t', 2);
select itemoffset, data from bt_page_items('px_t', 3);
select itemoffset, data from bt_page_items('px_t', 4);
alter system reset polar_px_enable_btbuild;
select pg_reload_conf();
create table px_test_1(id1 int, id2 int, id3 int, id4 int);
insert into px_test_1(id1, id2, id3, id4) select generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000);
create index px_t_1 on px_test_1(id1, id2, id3, id4);
select itemoffset, data from bt_page_items('px_t_1', 1);
select itemoffset, data from bt_page_items('px_t_1', 2);
select itemoffset, data from bt_page_items('px_t_1', 3);
select itemoffset, data from bt_page_items('px_t_1', 4);
select count(*) from bt_page_items('px_t', 1) as a, bt_page_items('px_t_1', 1) as b where a.itemoffset = b.itemoffset and a.data = b.data;
select count(*) from bt_page_items('px_t', 2) as a, bt_page_items('px_t_1', 2) as b where a.itemoffset = b.itemoffset and a.data = b.data;
select count(*) from bt_page_items('px_t', 3) as a, bt_page_items('px_t_1', 3) as b where a.itemoffset = b.itemoffset and a.data = b.data;
select count(*) from bt_page_items('px_t', 4) as a, bt_page_items('px_t_1', 4) as b where a.itemoffset = b.itemoffset and a.data = b.data;

-- 3 INDEX_MAX_KEYS
drop table if exists px_test;
drop table if exists px_test_1;
create table px_test(id1 int, id2 int, id3 int, id4 int, id5 int, id6 int, id7 int, id8 int, id9 int, id10 int,id11 int, id12 int, id13 int, id14 int, id15 int, id16 int, id17 int, id18 int, id19 int, id20 int,id21 int, id22 int, id23 int, id24 int, id25 int, id26 int, id27 int, id28 int, id29 int, id30 int, id31 int, id32 int);
insert into px_test(id1, id2, id3, id4, id5, id6, id7, id8, id9, id10, id11, id12, id13, id14, id15, id16, id17, id18, id19, id20, id21, id22, id23, id24, id25, id26, id27, id28, id29, id30, id31, id32) select generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000);
alter system set polar_px_enable_btbuild = 1;
select pg_reload_conf();
select pg_sleep(1);
create index px_t on px_test(id1, id2, id3, id4, id5, id6, id7, id8, id9, id10, id11, id12, id13, id14, id15, id16, id17, id18, id19, id20, id21, id22, id23, id24, id25, id26, id27, id28, id29, id30, id31, id32) with(px_build=on);
select itemoffset, data from bt_page_items('px_t', 1);
select itemoffset, data from bt_page_items('px_t', 2);
select itemoffset, data from bt_page_items('px_t', 3);
select itemoffset, data from bt_page_items('px_t', 4);
alter system reset polar_px_enable_btbuild;
select pg_reload_conf();
create table px_test_1(id1 int, id2 int, id3 int, id4 int, id5 int, id6 int, id7 int, id8 int, id9 int, id10 int,id11 int, id12 int, id13 int, id14 int, id15 int, id16 int, id17 int, id18 int, id19 int, id20 int,id21 int, id22 int, id23 int, id24 int, id25 int, id26 int, id27 int, id28 int, id29 int, id30 int, id31 int, id32 int);
insert into px_test_1(id1, id2, id3, id4, id5, id6, id7, id8, id9, id10, id11, id12, id13, id14, id15, id16, id17, id18, id19, id20, id21, id22, id23, id24, id25, id26, id27, id28, id29, id30, id31, id32) select generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000);
create index px_t_1 on px_test_1(id1, id2, id3, id4, id5, id6, id7, id8, id9, id10, id11, id12, id13, id14, id15, id16, id17, id18, id19, id20, id21, id22, id23, id24, id25, id26, id27, id28, id29, id30, id31, id32);
select itemoffset, data from bt_page_items('px_t_1', 1);
select itemoffset, data from bt_page_items('px_t_1', 2);
select itemoffset, data from bt_page_items('px_t_1', 3);
select itemoffset, data from bt_page_items('px_t_1', 4);
select count(*) from bt_page_items('px_t', 1) as a, bt_page_items('px_t_1', 1) as b where a.itemoffset = b.itemoffset and a.data = b.data;
select count(*) from bt_page_items('px_t', 2) as a, bt_page_items('px_t_1', 2) as b where a.itemoffset = b.itemoffset and a.data = b.data;
select count(*) from bt_page_items('px_t', 3) as a, bt_page_items('px_t_1', 3) as b where a.itemoffset = b.itemoffset and a.data = b.data;
select count(*) from bt_page_items('px_t', 4) as a, bt_page_items('px_t_1', 4) as b where a.itemoffset = b.itemoffset and a.data = b.data;

-- int + text
drop table if exists px_test;
drop table if exists px_test_1;
create table px_test(id1 int, id2 int, id3 text, id4 text);
insert into px_test(id1, id2, id3, id4) select generate_series(1, 1000), generate_series(1, 1000), 'test'||generate_series(1, 1000), 'test'||generate_series(2000, 3000);
alter system set polar_px_enable_btbuild = 1;
select pg_reload_conf();
select pg_sleep(1);
create index px_t on px_test(id1, id2, id3, id4) with(px_build=on);
select itemoffset, data from bt_page_items('px_t', 1);
select itemoffset, data from bt_page_items('px_t', 2);
select itemoffset, data from bt_page_items('px_t', 3);
select itemoffset, data from bt_page_items('px_t', 4);
alter system reset polar_px_enable_btbuild;
select pg_reload_conf();
create table px_test_1(id1 int, id2 int, id3 text, id4 text);
insert into px_test_1(id1, id2, id3, id4) select generate_series(1, 1000), generate_series(1, 1000), 'test'||generate_series(1, 1000), 'test'||generate_series(2000, 3000);
create index px_t_1 on px_test_1(id1, id2, id3, id4);
select itemoffset, data from bt_page_items('px_t_1', 1);
select itemoffset, data from bt_page_items('px_t_1', 2);
select itemoffset, data from bt_page_items('px_t_1', 3);
select itemoffset, data from bt_page_items('px_t_1', 4);
select count(*) from bt_page_items('px_t', 1) as a, bt_page_items('px_t_1', 1) as b where a.itemoffset = b.itemoffset and a.data = b.data;
select count(*) from bt_page_items('px_t', 2) as a, bt_page_items('px_t_1', 2) as b where a.itemoffset = b.itemoffset and a.data = b.data;
select count(*) from bt_page_items('px_t', 3) as a, bt_page_items('px_t_1', 3) as b where a.itemoffset = b.itemoffset and a.data = b.data;
select count(*) from bt_page_items('px_t', 4) as a, bt_page_items('px_t_1', 4) as b where a.itemoffset = b.itemoffset and a.data = b.data;

-- int + text INDEX_MAX_KEYS
drop table if exists px_test;
drop table if exists px_test_1;
create table px_test(id1 int, id2 int, id3 int, id4 int, id5 int, id6 int, id7 int, id8 int, id9 int, id10 int,id11 int, id12 int, id13 int, id14 int, id15 int, id16 int, id17 text, id18 text, id19 text, id20 text,id21 text, id22 text, id23 text, id24 text, id25 text, id26 text, id27 text, id28 text, id29 text, id30 text, id31 text, id32 text);
insert into px_test(id1, id2, id3, id4, id5, id6, id7, id8, id9, id10, id11, id12, id13, id14, id15, id16, id17, id18, id19, id20, id21, id22, id23, id24, id25, id26, id27, id28, id29, id30, id31, id32) select generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), 'test'||generate_series(1, 1000), 'test'||generate_series(1, 1000), 'test'||generate_series(1, 1000), 'test'||generate_series(1, 1000), 'test'||generate_series(1, 1000), 'test'||generate_series(1, 1000), 'test'||generate_series(1, 1000), 'test'||generate_series(1, 1000), 'test'||generate_series(1, 1000), 'test'||generate_series(1, 1000), 'test'||generate_series(1, 1000), 'test'||generate_series(1, 1000), 'test'||generate_series(1, 1000), 'test'||generate_series(1, 1000), 'test'||generate_series(1, 1000), 'test'||generate_series(1, 1000);
alter system set polar_px_enable_btbuild = 1;
select pg_reload_conf();
select pg_sleep(1);
create index px_t on px_test(id1, id2, id3, id4, id5, id6, id7, id8, id9, id10, id11, id12, id13, id14, id15, id16, id17, id18, id19, id20, id21, id22, id23, id24, id25, id26, id27, id28, id29, id30, id31, id32) with(px_build=on);
select itemoffset, data from bt_page_items('px_t', 1);
select itemoffset, data from bt_page_items('px_t', 2);
select itemoffset, data from bt_page_items('px_t', 3);
select itemoffset, data from bt_page_items('px_t', 4);
alter system reset polar_px_enable_btbuild;
select pg_reload_conf();
create table px_test_1(id1 int, id2 int, id3 int, id4 int, id5 int, id6 int, id7 int, id8 int, id9 int, id10 int,id11 int, id12 int, id13 int, id14 int, id15 int, id16 int, id17 text, id18 text, id19 text, id20 text,id21 text, id22 text, id23 text, id24 text, id25 text, id26 text, id27 text, id28 text, id29 text, id30 text, id31 text, id32 text);
insert into px_test_1(id1, id2, id3, id4, id5, id6, id7, id8, id9, id10, id11, id12, id13, id14, id15, id16, id17, id18, id19, id20, id21, id22, id23, id24, id25, id26, id27, id28, id29, id30, id31, id32) select generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), 'test'||generate_series(1, 1000), 'test'||generate_series(1, 1000), 'test'||generate_series(1, 1000), 'test'||generate_series(1, 1000), 'test'||generate_series(1, 1000), 'test'||generate_series(1, 1000), 'test'||generate_series(1, 1000), 'test'||generate_series(1, 1000), 'test'||generate_series(1, 1000), 'test'||generate_series(1, 1000), 'test'||generate_series(1, 1000), 'test'||generate_series(1, 1000), 'test'||generate_series(1, 1000), 'test'||generate_series(1, 1000), 'test'||generate_series(1, 1000), 'test'||generate_series(1, 1000);
create index px_t_1 on px_test_1(id1, id2, id3, id4, id5, id6, id7, id8, id9, id10, id11, id12, id13, id14, id15, id16, id17, id18, id19, id20, id21, id22, id23, id24, id25, id26, id27, id28, id29, id30, id31, id32);
select itemoffset, data from bt_page_items('px_t_1', 1);
select itemoffset, data from bt_page_items('px_t_1', 2);
select itemoffset, data from bt_page_items('px_t_1', 3);
select itemoffset, data from bt_page_items('px_t_1', 4);
select count(*) from bt_page_items('px_t', 1) as a, bt_page_items('px_t_1', 1) as b where a.itemoffset = b.itemoffset and a.data = b.data;
select count(*) from bt_page_items('px_t', 2) as a, bt_page_items('px_t_1', 2) as b where a.itemoffset = b.itemoffset and a.data = b.data;
select count(*) from bt_page_items('px_t', 3) as a, bt_page_items('px_t_1', 3) as b where a.itemoffset = b.itemoffset and a.data = b.data;
select count(*) from bt_page_items('px_t', 4) as a, bt_page_items('px_t_1', 4) as b where a.itemoffset = b.itemoffset and a.data = b.data;

-- parse detect
alter system set polar_px_enable_btbuild = 0;
select pg_reload_conf();
select pg_sleep(1);
drop table if exists px_test;
create table px_test(id int);
create index px_t on px_test(id) with(px_build=on);
alter system set polar_px_enable_btbuild = 1;
select pg_reload_conf();
select pg_sleep(1);
drop table if exists px_test;
create table px_test(id int);
create index px_t on px_test(id) with(px_build=off);
drop table if exists px_test;
create table px_test(id int);
create index px_t on px_test(id) with(px_build=finish);
drop table if exists px_test;
create table px_test(id int);
create index px_t on px_test(id) with(px_build=true);
drop table if exists px_test;
create table px_test(id int);
create index px_t on px_test(id) with(px_build='true');
alter system reset polar_px_enable_btbuild;
select pg_reload_conf();
select pg_sleep(1);
drop table if exists px_test;
drop table if exists px_test_1;

alter system reset polar_bt_write_page_buffer_size;
select pg_reload_conf();
select pg_sleep(1);

drop extension pageinspect;