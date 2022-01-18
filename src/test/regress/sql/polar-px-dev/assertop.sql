
-- simple SQL with assert
-- 
/*--EXPLAIN_QUERY_BEGIN*/
explain (costs off) select (select 1 union select 2);
-- expect assertion failure because more than 1 row returned
select (select 1 union select 2);
-- 
explain (costs off) select (select generate_series(1,5));
-- expect assertion failure because more than 1 row returned
select (select generate_series(1,5));
-- 
explain (costs off) select (select generate_series(1,1)) as series;
-- expect assertion pass
select (select generate_series(1,1)) as series;


-- PX query with assert
-- 
drop table if exists assert_test1;
create table assert_test1(id int);
insert into assert_test1(id) select generate_series(1, 1000) as id order by id desc;
-- expect assertion failure because more than 1 row returned
select * from assert_test1 where id = (select id from assert_test1);
-- expect assertion pass because only 1 row returned
select * from assert_test1 where id = (select id from assert_test1 where id = 1);
-- 
drop table if exists assert_test2;
create table assert_test2(id1 int, id2 int, id3 int, id4 int);
insert into assert_test2(id1, id2, id3, id4) select
    generate_series(1, 1000),
    generate_series(1, 1000),
    generate_series(1, 1000),
    generate_series(1, 1000);
-- expect assertion failure because more than 1 row returned
select * from assert_test2 where id2 = (select id3 from assert_test2);
-- expect assertion pass because only 1 row returned
select * from assert_test2 where id2 = (select id3 from assert_test2 where id4 = 1);

-- More DML with assert to be test seperately...
