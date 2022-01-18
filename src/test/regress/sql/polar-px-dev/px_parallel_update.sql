-- config
/*--POLAR_ENABLE_PX*/
set polar_enable_px = 1;
set polar_px_optimizer_enable_dml_constraints = 1;




set polar_px_enable_update = 1;


-- polar_px_update_dop_num = 1
set polar_px_update_dop_num = 1;
\i sql/polar-px-dev/px_parallel_update_init.sql
\i sql/polar-px-dev/px_parallel_update_base.sql


-- polar_px_update_dop_num = 6
set polar_px_update_dop_num = 6;
\i sql/polar-px-dev/px_parallel_update_init.sql
\i sql/polar-px-dev/px_parallel_update_base.sql

-- polar_px_update_dop_num = 9
set polar_px_update_dop_num = 9;
\i sql/polar-px-dev/px_parallel_update_init.sql
\i sql/polar-px-dev/px_parallel_update_base.sql