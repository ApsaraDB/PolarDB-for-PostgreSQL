-- Update without subplan
EXPLAIN (COSTS OFF) update px_parallel_update_t1 set c1 = 3;
update px_parallel_update_t1 set c1 = 3;
EXPLAIN (COSTS OFF) update px_parallel_update_t2 set c1 = 4;
update px_parallel_update_t2 set c1 = 4;
select max(c1) from px_parallel_update_t1;
select max(c1) from px_parallel_update_t2;

-- Update with subplan
EXPLAIN (COSTS OFF) update px_parallel_update_t3 set c2=(select MAX(c2) from px_parallel_update_t1);
EXPLAIN (COSTS OFF) update px_parallel_update_t3 set c1=(select AVG(c2) from px_parallel_update_t2);
update px_parallel_update_t3 set c2=(select MAX(c2) from px_parallel_update_t1);
update px_parallel_update_t3 set c1=(select AVG(c2) from px_parallel_update_t2);
select AVG(c1) from px_parallel_update_t3;
select AVG(c2) from px_parallel_update_t3;

-- Update vector
EXPLAIN (COSTS OFF) update px_parallel_update_t3 set c1=10, c2=(select MAX(c2) from px_parallel_update_t2);
update px_parallel_update_t3 set c1=10, c2=(select MAX(c2) from px_parallel_update_t2);
select AVG(c1) + MAX(c2) from px_parallel_update_t3;

-- Update Partition Table, should be fallback
EXPLAIN (COSTS OFF) update px_parallel_update_t1_range set id = 3;
EXPLAIN (COSTS OFF) update px_parallel_update_t1_range set id =(select max(c2) from px_parallel_update_t2);
EXPLAIN (COSTS OFF) update px_parallel_update_t1_range set id =(select max(c2) from px_parallel_update_t2), value = 5;

-- Update With Index
EXPLAIN (COSTS OFF) update px_parallel_update_t4 set c1 = 5;
EXPLAIN (COSTS OFF) update px_parallel_update_t3 set c2 = (select min(c1) from px_parallel_update_t4);
update px_parallel_update_t3 set c2 = (select min(c1) from px_parallel_update_t4);
update px_parallel_update_t4 set c1 = 5;


-- Update not NULL CONSTRAIN
insert into px_parallel_update_t5 select generate_series(1,1000),generate_series(1,1000);
EXPLAIN (COSTS OFF) update px_parallel_update_t5 set c2 = NULL;


-- Update with CHECK OPTION
insert into px_parallel_update_t7 select generate_series(1,999),generate_series(1,999);
EXPLAIN (COSTS OFF) update px_parallel_update_t7 set c2 = 1000;
EXPLAIN (COSTS OFF) update px_parallel_update_t7 set c2 = 500;


-- Update with TRIGGER
EXPLAIN (COSTS OFF) update px_parallel_update_t10 set NAME='123456';
update px_parallel_update_t10 set NAME='123456';
select * from px_parallel_update_t10 order by ID limit 1;
select * from px_parallel_update_audit order by EMP_ID limit 1;

-- Update with VIEW
EXPLAIN (COSTS OFF) update px_parallel_update_view set c2 = -100, c3 = 100;
update px_parallel_update_view set c2 = -100, c3 = 100;
select * from px_parallel_update_view order by c1 ASC limit 10;
select * from px_parallel_update_t11 order by c1 ASC limit 10;


-- Update with CTE
EXPLAIN (COSTS OFF) WITH update_move_rows AS (
   update px_parallel_update_t12 set c1=3 RETURNING *
)
update px_parallel_update_t13 set c1=(select count(*) from update_move_rows);


-- Update double delete error case
EXPLAIN (COSTS OFF) UPDATE px_parallel_update_t14 set c1 = px_parallel_update_t15.c2 from px_parallel_update_t15
where px_parallel_update_t14.c2 = px_parallel_update_t15.c1;

UPDATE px_parallel_update_t14 set c1 = px_parallel_update_t15.c2 from px_parallel_update_t15
where px_parallel_update_t14.c2 = px_parallel_update_t15.c1;

select * from px_parallel_update_t14;
