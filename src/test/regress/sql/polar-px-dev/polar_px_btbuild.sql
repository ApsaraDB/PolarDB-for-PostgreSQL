/*--EXPLAIN_QUERY_BEGIN*/
create extension if not exists pageinspect;

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

-- int + text, asc/desc, nulls first/last
drop table if exists px_test;
drop table if exists px_test_1;

create table px_test(id1 int, id2 int, id3 text, id4 text);
insert into px_test(id1, id2, id3, id4) select
    generate_series(1, 500),
    generate_series(1, 500),
    'test'||generate_series(1, 500),
    'test'||generate_series(200, 700);
update px_test set id1 = null where id1 % 5 = 0;
update px_test set id2 = null where id2 % 3 = 0;
update px_test set id3 = null where substring(id3, 5, 1) = '5';
update px_test set id4 = null where substring(id4, 7, 1) = '7';

create table px_test_1(id1 int, id2 int, id3 text, id4 text);
insert into px_test_1(id1, id2, id3, id4) select
    generate_series(1, 500),
    generate_series(1, 500),
    'test'||generate_series(1, 500),
    'test'||generate_series(200, 700);
update px_test_1 set id1 = null where id1 % 5 = 0;
update px_test_1 set id2 = null where id2 % 3 = 0;
update px_test_1 set id3 = null where substring(id3, 5, 1) = '5';
update px_test_1 set id4 = null where substring(id4, 7, 1) = '7';

alter system set polar_px_enable_btbuild = 1;
select pg_reload_conf();
select pg_sleep(1);
create index px_t on px_test(
    -- id1 asc nulls first,
    id1 nulls first
) with(px_build=on);
alter system reset polar_px_enable_btbuild;
select pg_reload_conf();
create index px_t_1 on px_test_1(
    id1 asc nulls first
);
select count(*) from bt_page_items('px_t_1', 1);
select count(*) from bt_page_items('px_t_1', 2);
select count(*) from bt_page_items('px_t_1', 3);
select count(*) from bt_page_items('px_t', 1) as a, bt_page_items('px_t_1', 1) as b where a.itemoffset = b.itemoffset and a.data = b.data;
select count(*) from bt_page_items('px_t', 2) as a, bt_page_items('px_t_1', 2) as b where a.itemoffset = b.itemoffset and a.data = b.data;
select count(*) from bt_page_items('px_t', 3) as a, bt_page_items('px_t_1', 3) as b where a.itemoffset = b.itemoffset and a.data = b.data;
drop index px_t;
drop index px_t_1;

alter system set polar_px_enable_btbuild = 1;
select pg_reload_conf();
select pg_sleep(1);
create index px_t on px_test(
    -- id2 desc nulls first,
    id2 desc
) with(px_build=on);
alter system reset polar_px_enable_btbuild;
select pg_reload_conf();
create index px_t_1 on px_test_1(
    id2 desc nulls first
);
select count(*) from bt_page_items('px_t_1', 1);
select count(*) from bt_page_items('px_t_1', 2);
select count(*) from bt_page_items('px_t_1', 3);
select count(*) from bt_page_items('px_t', 1) as a, bt_page_items('px_t_1', 1) as b where a.itemoffset = b.itemoffset and a.data = b.data;
select count(*) from bt_page_items('px_t', 2) as a, bt_page_items('px_t_1', 2) as b where a.itemoffset = b.itemoffset and a.data = b.data;
select count(*) from bt_page_items('px_t', 3) as a, bt_page_items('px_t_1', 3) as b where a.itemoffset = b.itemoffset and a.data = b.data;
drop index px_t;
drop index px_t_1;

alter system set polar_px_enable_btbuild = 1;
select pg_reload_conf();
select pg_sleep(1);
create index px_t on px_test(
    -- id3 asc nulls last,
    id3
) with(px_build=on);
alter system reset polar_px_enable_btbuild;
select pg_reload_conf();
create index px_t_1 on px_test_1(
    id3 asc nulls last
);
select count(*) from bt_page_items('px_t_1', 1);
select count(*) from bt_page_items('px_t_1', 2);
select count(*) from bt_page_items('px_t_1', 3);
select count(*) from bt_page_items('px_t', 1) as a, bt_page_items('px_t_1', 1) as b where a.itemoffset = b.itemoffset and a.data = b.data;
select count(*) from bt_page_items('px_t', 2) as a, bt_page_items('px_t_1', 2) as b where a.itemoffset = b.itemoffset and a.data = b.data;
select count(*) from bt_page_items('px_t', 3) as a, bt_page_items('px_t_1', 3) as b where a.itemoffset = b.itemoffset and a.data = b.data;
drop index px_t;
drop index px_t_1;

alter system set polar_px_enable_btbuild = 1;
select pg_reload_conf();
select pg_sleep(1);
create index px_t on px_test(
    id4 desc nulls last
) with(px_build=on);
alter system reset polar_px_enable_btbuild;
select pg_reload_conf();
create index px_t_1 on px_test_1(
    id4 desc nulls last
);
select count(*) from bt_page_items('px_t_1', 1);
select count(*) from bt_page_items('px_t_1', 2);
select count(*) from bt_page_items('px_t_1', 3);
select count(*) from bt_page_items('px_t', 1) as a, bt_page_items('px_t_1', 1) as b where a.itemoffset = b.itemoffset and a.data = b.data;
select count(*) from bt_page_items('px_t', 2) as a, bt_page_items('px_t_1', 2) as b where a.itemoffset = b.itemoffset and a.data = b.data;
select count(*) from bt_page_items('px_t', 3) as a, bt_page_items('px_t_1', 3) as b where a.itemoffset = b.itemoffset and a.data = b.data;
drop index px_t;
drop index px_t_1;

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

-- test index for other types
alter system set polar_px_enable_btbuild = 1;
select pg_reload_conf();
select pg_sleep(1);
show polar_px_enable_btbuild;

-- brin index
create table brin_test(id int);
-- fail
create index brin_px_i on brin_test using brin(id) with(px_build=on);
-- success
create index brin_px_i on brin_test using brin(id) with(pages_per_range=10);
drop table brin_test;

-- gin index
create table gin_test(id int4[]);
-- fail
create index gin_px_i on gin_test using gin(id) with(px_build=on);
-- success
create index gin_px_i on gin_test using gin(id) with(gin_pending_list_limit = 4096);
drop table gin_test;

-- gist index
create table gist_test(p point);
-- fail
create index gist_px_i on gist_test using gist(p) with(px_build = on);
-- success
create index gist_px_i on gist_test using gist(p) with(buffering = off);
drop table gist_test;

-- spgist index
create table spgist_test(p point);
-- fail
create index spgist_px_i on spgist_test using spgist(p) with(px_build = on);
-- success
create index spgist_px_i on spgist_test using spgist(p) with(fillfactor = 20);
drop table spgist_test;

-- hash index
create table hash_test(id int);
-- fail
create index hash_px_i on hash_test using hash(id) with(px_build = on);
-- success
create index hash_px_i on hash_test using hash(id) with(fillfactor = 20);
drop table hash_test;

alter system reset polar_px_enable_btbuild;
select pg_reload_conf();

drop extension pageinspect;