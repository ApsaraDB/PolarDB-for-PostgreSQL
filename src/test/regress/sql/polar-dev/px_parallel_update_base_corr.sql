-- Update without subplan
update px_parallel_corr_update_t1 set c1 = 3;
update px_parallel_corr_update_t2 set c1 = 4;
select max(c1) from px_parallel_corr_update_t1;
select max(c1) from px_parallel_corr_update_t2;

-- Update with subplan
update px_parallel_corr_update_t3 set c2=(select MAX(c2) from px_parallel_corr_update_t1);
update px_parallel_corr_update_t3 set c1=(select AVG(c2) from px_parallel_corr_update_t2);
select AVG(c1) from px_parallel_corr_update_t3;
select AVG(c2) from px_parallel_corr_update_t3;

-- Update vector
update px_parallel_corr_update_t3 set c1=10, c2=(select MAX(c2) from px_parallel_corr_update_t2);
select AVG(c1) + MAX(c2) from px_parallel_corr_update_t3;


-- Update Partition Table, should be fallback
update px_parallel_corr_update_t1_range set id = 3;
update px_parallel_corr_update_t1_range set id =(select max(c2) from px_parallel_corr_update_t2);
update px_parallel_corr_update_t1_range set id =(select max(c2) from px_parallel_corr_update_t2), value = 5;
select max(id), max(value) from px_parallel_corr_update_t1_range;

-- Update With Index
update px_parallel_corr_update_t3 set c2 = (select min(c1) from px_parallel_corr_update_t4);
update px_parallel_corr_update_t4 set c1 = 5;
select * from px_parallel_corr_update_t4 where c1 <> 5;




-- Update with TRIGGER
update px_parallel_corr_update_t10 set NAME='123456';
select * from px_parallel_corr_update_t10 order by ID limit 1;
select * from px_parallel_corr_update_audit order by EMP_ID limit 1;

-- Update with VIEW
update px_parallel_corr_update_view set c2 = -100, c3 = 100;
select * from px_parallel_corr_update_view order by c1 ASC limit 10;
select * from px_parallel_corr_update_t11 order by c1 ASC limit 10;

-- Update with CTE
WITH update_move_rows_corr AS (
   update px_parallel_corr_update_t12 set c1=3 RETURNING *
)
update px_parallel_corr_update_t13 set c1=(select count(*) from update_move_rows_corr);
select max(c1) from px_parallel_corr_update_t13;
select max(c1) from px_parallel_corr_update_t12;