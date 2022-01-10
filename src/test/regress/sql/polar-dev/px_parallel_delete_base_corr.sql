-- Delete without subplan
delete from px_parallel_delete_corr_t1 where c1 < 30;
select count(*) from px_parallel_delete_corr_t1;
delete from px_parallel_delete_corr_t1 where c1 > 970;
select count(*) from px_parallel_delete_corr_t1;

-- Delete with subplan
delete from px_parallel_delete_corr_t2 where c1 in (select c1 from px_parallel_delete_corr_t1 where c1 < 50);
select count(*) from px_parallel_delete_corr_t2;
delete from px_parallel_delete_corr_t2 where c1 in (select c1 from px_parallel_delete_corr_t1 where c1 > 970);
select count(*) from px_parallel_delete_corr_t2;



-- Delete Partitiion Table, should be fallback
delete from px_parallel_delete_corr_t1_range where id < 50;
select count(*) from px_parallel_delete_corr_t1_range;
delete from px_parallel_delete_corr_t1_range where id in (select c1 from px_parallel_delete_corr_t3 where c1 > 960);
select count(*) from px_parallel_delete_corr_t1_range;
-- select Range Partition Table, use PX
delete from px_parallel_delete_corr_t3 where c1 in (select id from px_parallel_delete_corr_t1_range where id > 900);
select count(*) from px_parallel_delete_corr_t3;
-- select Hash Partition Table, use PX
delete from px_parallel_delete_corr_t3 where c1 in (select id from px_parallel_delete_corr_t0_hash where id > 800);
select count(*) from px_parallel_delete_corr_t3;
-- select List Partition Table, use PX
delete from px_parallel_delete_corr_t3 where c1 in (select pvalue from px_parallel_delete_corr_t2_list where job = 'student');
select count(*) from px_parallel_delete_corr_t3;

-- Delete With Index
delete from px_parallel_delete_corr_t4 where c1 < 50;
select count(*) from px_parallel_delete_corr_t4;
delete from px_parallel_delete_corr_t4 where c1 in (select c1 from px_parallel_delete_corr_t15 where c1 > 960);
select count(*) from px_parallel_delete_corr_t4;


-- Delete with CTE
WITH delete_move_rows AS (
    delete from px_parallel_delete_corr_t12 where c1 < 30 RETURNING c2
)
delete from px_parallel_delete_corr_t13 where c1 in (select c1 from delete_move_rows);

-- Delete with VIEW
delete from px_parallel_delete_corr_view;
select count(*) from px_parallel_delete_corr_t11;
select count(*) from px_parallel_delete_corr_t11;


-- Delete twice case
delete from px_parallel_delete_corr_t14 where c2 in (select c1 from px_parallel_delete_corr_t15);
select count(*) from px_parallel_delete_corr_t14;


-- Delete with returning
delete from px_parallel_delete_corr_t15 where c2 < 10 returning *;

-- Delete with trigger
delete from px_parallel_delete_corr_t16 where c1 < 50;
select count(*) from px_parallel_delete_corr_t16;
select count(*) from px_parallel_delete_corr_audit;