-- config
/*--POLAR_ENABLE_PX*/
set polar_enable_px = 1;
set polar_px_enable_insert_select = 1;
set polar_px_optimizer_enable_dml_constraints = 1;
set polar_px_enable_insert_from_tableless = 1;

-- create table
\i sql/polar-dev/px_parallel_dml_init_corr.sql


-- Constrain Test Init
drop table if exists px_parallel_corr_update_t5 cascade;
CREATE TABLE px_parallel_corr_update_t5 (c1 int, c2 int not NULL) ;
drop table if exists px_parallel_corr_update_t6 cascade;
CREATE TABLE px_parallel_corr_update_t6 (c1 int, c2 int) ;


drop table if exists px_parallel_corr_update_t7 cascade;
CREATE TABLE px_parallel_corr_update_t7 (c1 int, c2 int CHECK (c2 < 1000)) ;


-- One Writer worker
set polar_px_insert_dop_num = 1;
\i sql/polar-dev/px_parallel_dml_error_base.sql

-- Three Writer worker
set polar_px_insert_dop_num = 3;
\i sql/polar-dev/px_parallel_dml_error_base.sql

-- 6 Writer worker
set polar_px_insert_dop_num = 6;
\i sql/polar-dev/px_parallel_dml_error_base.sql

-- 9 Writer worker
set polar_px_insert_dop_num = 9;
\i sql/polar-dev/px_parallel_dml_error_base.sql

set polar_px_enable_insert_from_tableless = 0;