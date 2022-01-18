-- Delete without subplan
EXPLAIN (VERBOSE, COSTS OFF) delete from px_parallel_delete_t1 where c1 < 30;
delete from px_parallel_delete_t1 where c1 < 30;
select count(*) from px_parallel_delete_t1;
EXPLAIN (VERBOSE, COSTS OFF) delete from px_parallel_delete_t1 where c1 > 970;
delete from px_parallel_delete_t1 where c1 > 970;
select count(*) from px_parallel_delete_t1;

-- Delete with subplan
EXPLAIN (VERBOSE, COSTS OFF) delete from px_parallel_delete_t2 where c1 in (select c1 from px_parallel_delete_t1 where c1 < 50);
delete from px_parallel_delete_t2 where c1 in (select c1 from px_parallel_delete_t1 where c1 < 50);
select count(*) from px_parallel_delete_t2;
EXPLAIN (VERBOSE, COSTS OFF) delete from px_parallel_delete_t2 where c1 in (select c1 from px_parallel_delete_t1 where c1 > 950);
delete from px_parallel_delete_t2 where c1 in (select c1 from px_parallel_delete_t1 where c1 > 950);
select count(*) from px_parallel_delete_t2;


-- Delete Partitiion Table, should be fallback
EXPLAIN (VERBOSE, COSTS OFF) delete from px_parallel_delete_t1_range where id < 50;
EXPLAIN (VERBOSE, COSTS OFF) delete from px_parallel_delete_t1_range where id in (select c1 from px_parallel_delete_t3 where c1 > 960);
delete from px_parallel_delete_t1_range where id < 50;
select count(*) from px_parallel_delete_t1_range;
delete from px_parallel_delete_t1_range where id in (select c1 from px_parallel_delete_t3 where c1 > 960);
select count(*) from px_parallel_delete_t1_range;
-- select Range Partition Table, use PX
EXPLAIN (VERBOSE, COSTS OFF) delete from px_parallel_delete_t3 where c1 in (select id from px_parallel_delete_t1_range where id > 900);
delete from px_parallel_delete_t3 where c1 in (select id from px_parallel_delete_t1_range where id > 900);
select count(*) from px_parallel_delete_t3;
-- select Hash Partition Table, use PX
EXPLAIN (VERBOSE, COSTS OFF) delete from px_parallel_delete_t3 where c1 in (select id from px_parallel_delete_t0_hash where id > 800);
delete from px_parallel_delete_t3 where c1 in (select id from px_parallel_delete_t0_hash where id > 800);
select count(*) from px_parallel_delete_t3;
-- select List Partition Table, use PX
EXPLAIN (VERBOSE, COSTS OFF) delete from px_parallel_delete_t3 where c1 in (select pvalue from px_parallel_delete_t2_list where job = 'student');
delete from px_parallel_delete_t3 where c1 in (select pvalue from px_parallel_delete_t2_list where job = 'student');
select count(*) from px_parallel_delete_t3;


-- Delete With Index
EXPLAIN (VERBOSE, COSTS OFF) delete from px_parallel_delete_t4 where c1 < 50;
EXPLAIN (VERBOSE, COSTS OFF) delete from px_parallel_delete_t4 where c1 in (select c1 from px_parallel_delete_t3 where c1 > 960);
delete from px_parallel_delete_t4 where c1 < 50;
select count(*) from px_parallel_delete_t4;
delete from px_parallel_delete_t4 where c1 in (select c1 from px_parallel_delete_t15 where c1 > 960);
select count(*) from px_parallel_delete_t4;


-- Delete with CTE
EXPLAIN (VERBOSE, COSTS OFF) WITH delete_move_rows AS (
    delete from px_parallel_delete_t12 where c1 < 30 RETURNING c2
)
delete from px_parallel_delete_t13 where c1 in (select c1 from delete_move_rows);



-- Delete with VIEW
EXPLAIN (VERBOSE, COSTS OFF) delete from px_parallel_delete_view;
delete from px_parallel_delete_view;
select count(*) from px_parallel_delete_t11;
select count(*) from px_parallel_delete_t11;


-- Delete twice case
EXPLAIN(VERBOSE, COSTS OFF) delete from px_parallel_delete_t14 where c2 in (select c1 from px_parallel_delete_t15);
delete from px_parallel_delete_t14 where c2 in (select c1 from px_parallel_delete_t15);
select count(*) from px_parallel_delete_t14;


-- Delete with returning
EXPLAIN(VERBOSE, COSTS OFF) delete from px_parallel_delete_t15 where c2 < 10 returning *;
delete from px_parallel_delete_t15 where c2 < 10 returning *;


-- Delete with trigger
EXPLAIN(VERBOSE, COSTS OFF) delete from px_parallel_delete_t16 where c1 < 50;
delete from px_parallel_delete_t16 where c1 < 50;
select count(*) from px_parallel_delete_t16;
select count(*) from px_parallel_delete_audit;