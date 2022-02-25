set polar_max_hashagg_mem=128;
set polar_max_setop_mem=128;
set polar_max_subplan_mem=128;
set polar_max_recursiveunion_mem=128;

-- HashAgg
explain (costs off) select count(*) from (select s from generate_series(1, 1000) s group by s) g;
select count(*) from (select s from generate_series(1, 1000) s group by s) g;
set polar_max_hashagg_mem=1024;
select count(*) from (select s from generate_series(1, 1000) s group by s) g;
-- 0 means no limit
set polar_max_hashagg_mem=0;
select count(*) from (select s from generate_series(1, 1000) s group by s) g;

-- SetOp
explain (costs off) select generate_series(1,1000) except select generate_series(1,1000);
select generate_series(1,1000) except select generate_series(1,1000);
set polar_max_setop_mem=1024;
select generate_series(1,1000) except select generate_series(1,1000);

-- Subplan
explain (costs off) select * from generate_series(1,1001) as s where s not in (select * from generate_series(1,1000));
select * from generate_series(1,1001) as s where s not in (select * from generate_series(1,1000));
set polar_max_subplan_mem=1024;
select * from generate_series(1,1001) as s where s not in (select * from generate_series(1,1000));

-- RecursiveUnion
explain with recursive x(a) as
(select 1 as a
union
select a + 1 from x where a < 2000)
select count(*) from x;
with recursive x(a) as
(select 1 as a
union
select a + 1 from x where a < 2000)
select count(*) from x;
set polar_max_recursiveunion_mem=1024;
with recursive x(a) as
(select 1 as a
union
select a + 1 from x where a < 2000)
select count(*) from x;
