-- config
/*--POLAR_ENABLE_PX*/
set polar_enable_px = 1;
set polar_px_enable_insert_select = 1;
set polar_px_optimizer_enable_dml_constraints = 1;
set polar_px_enable_insert_from_tableless = 1;


-- create table
\i sql/polar-dev/px_parallel_dml_init_corr.sql


-- Slow SQL, Only once
-- Delete

delete from px_parallel_dml_corr_t2 as px_parallel_dml_corr_t0 where px_parallel_dml_corr_t0.c1 < (select c2 from px_parallel_dml_corr_t1 order by c2 LIMIT 1);

delete from px_parallel_dml_corr_t1 as px_parallel_dml_corr_t0 where px_parallel_dml_corr_t0.c1 < (select c2 from px_parallel_dml_corr_t2 order by c2 LIMIT 1);
select count(*) from px_parallel_dml_corr_t1;
select count(*) from px_parallel_dml_corr_t2;

-- Update

update px_parallel_dml_corr_t2 set c1 = px_parallel_dml_corr_t1.c2 from px_parallel_dml_corr_t1;

update px_parallel_dml_corr_t1 set c1 = px_parallel_dml_corr_t2.c2 from px_parallel_dml_corr_t2;
select count(*) from px_parallel_dml_corr_t1;
select count(*) from px_parallel_dml_corr_t2;

update px_parallel_dml_corr_t2 set c2 = px_parallel_dml_corr_t1.c2 from px_parallel_dml_corr_t1;

update px_parallel_dml_corr_t1 set c2 = px_parallel_dml_corr_t2.c2 from px_parallel_dml_corr_t2;
select count(*) from px_parallel_dml_corr_t1;
select count(*) from px_parallel_dml_corr_t2;


-- One Writer worker
set polar_px_insert_dop_num = 1;
\i sql/polar-dev/px_parallel_dml_init_corr.sql
\i sql/polar-dev/px_parallel_dml_base_corr.sql

-- Writer workers < Reader workers
set polar_px_insert_dop_num = 3;
\i sql/polar-dev/px_parallel_dml_init_corr.sql
\i sql/polar-dev/px_parallel_dml_base_corr.sql

-- Writer workers == Read workers
set polar_px_insert_dop_num = 6;
\i sql/polar-dev/px_parallel_dml_init_corr.sql
\i sql/polar-dev/px_parallel_dml_base_corr.sql

-- Writer workers > Read workers
set polar_px_insert_dop_num = 10;
\i sql/polar-dev/px_parallel_dml_init_corr.sql
\i sql/polar-dev/px_parallel_dml_base_corr.sql

set polar_px_enable_insert_from_tableless = 0;