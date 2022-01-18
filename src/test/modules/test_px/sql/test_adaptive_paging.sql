create extension if not exists test_px;
create extension if not exists faultinjector;

select test_px_adaptive_paging();

create table t_px_adaptive_paging(id int);
insert into t_px_adaptive_paging select generate_series(1, 300000);

------------------------------
-- inject fault on qc
------------------------------
select inject_fault('all', 'reset');
select inject_fault('px_pthread_create', 'enable', '', '', 1, 1, 0);
select count(*) from t_px_adaptive_paging;

------------------------------
-- random fault on qc
------------------------------
set polar_px_enable_tcp_testmode = 1;
select count(*) from t_px_adaptive_paging;

------------------------------
-- inject fault on px manual
-- and run sql on qc
------------------------------
select inject_fault('all', 'reset');
select inject_fault('px_pq_flush', 'enable', '', '', 1, 100, 0);
select count(*) from t_px_adaptive_paging;

select inject_fault('all', 'reset');
select inject_fault('px_pq_getbyte', 'enable', '', '', 1, 100, 0);
select count(*) from t_px_adaptive_paging;

select inject_fault('all', 'reset');
select inject_fault('px_pq_getmessage', 'enable', '', '', 1, 100, 0);
select count(*) from t_px_adaptive_paging;

select inject_fault('all', 'reset');
select inject_fault('px_adaptive_scan_round_delay', 'sleep', '', '', 1, 10, 5);
select count(*) from t_px_adaptive_paging as a, t_px_adaptive_paging as b where a.id < 100 and b.id < 100;

drop table t_px_adaptive_paging;