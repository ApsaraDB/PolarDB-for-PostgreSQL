create table test_cv(a int, b int, c int,  d int);
create index test_cv_a_b on test_cv(a, b);
create index test_cv_a_c on test_cv(a, c);
analyze test_cv;

set polar_stat_stale_cost to 0.01;

explain (costs off) select * from test_cv where a = 1 and b = 100;

drop table test_cv;

