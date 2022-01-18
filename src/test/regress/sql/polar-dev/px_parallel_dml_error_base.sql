
--------------------------------------------------------------------------
-- Insert not NULL Constrains Table
set polar_px_optimizer_enable_dml_constraints='off';
insert into px_parallel_dml_corr_t6 select generate_series(1,1000),NULL;
insert into px_parallel_dml_corr_t5 select * from px_parallel_dml_corr_t6 order by c1;
set polar_px_optimizer_enable_dml_constraints='on';



set polar_px_enable_update=1;
-- Update not NULL CONSTRAIN
insert into px_parallel_corr_update_t5 select generate_series(1,1000),generate_series(1,1000);
update px_parallel_corr_update_t5 set c2 = NULL;


-- Update with CHECK OPTION
insert into px_parallel_corr_update_t7 select generate_series(1,999),generate_series(1,999);
update px_parallel_corr_update_t7 set c2 = 1000;
select max(c2) from px_parallel_corr_update_t7;
update px_parallel_corr_update_t7 set c2 = 500;
select max(c2) from px_parallel_corr_update_t7;