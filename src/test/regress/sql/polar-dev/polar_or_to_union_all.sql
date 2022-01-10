create table unionor_t1 (a int, b int);
create table unionor_t2 (a int, b int);
create table unionor_t3 (a int, b int);
insert into unionor_t1 values (1, 1), (1, 1);
insert into unionor_t2 values (1, 1), (1, 2);
insert into unionor_t3 values (1, 1), (1, 2);

set polar_enable_convert_or_to_union_all = off;
select * from unionor_t1 t1, unionor_t2 t2 where t1.a = t2.a and (t1.a = 1 or t2.b = 2);
explain (costs false) select * from unionor_t1 t1, unionor_t2 t2 where t1.a = t2.a and (t1.a = 1 or t2.b = 2);

set polar_enable_convert_or_to_union_all = on;
select * from unionor_t1 t1, unionor_t2 t2 where t1.a = t2.a and (t1.a = 1 or t2.b = 2);
explain (costs false) select * from unionor_t1 t1, unionor_t2 t2 where t1.a = t2.a and (t1.a = 1 or t2.b = 2);


-- testing cost base plan selection
insert into unionor_t1 select generate_series(3, 10000), generate_series(3, 10000);
insert into unionor_t2 select generate_series(3, 10000), generate_series(3, 10000);
insert into unionor_t3 select generate_series(3, 10000), generate_series(3, 10000);
create index idx_unionor_t1_a on unionor_t1(a);
create index idx_unionor_t2_a on unionor_t2(a);
create index idx_unionor_t3_a on unionor_t3(a);

analyze unionor_t1;
analyze unionor_t2;
analyze unionor_t3;
-- union all plan
explain (costs false) select * from unionor_t1 t1, unionor_t2 t2 where t1.a = t2.a and (t1.a = 5000 or t2.a = 5000);
-- join plan
explain (costs false) select * from unionor_t1 t1, unionor_t2 t2 where t1.a = t2.a and (t1.a > 5000 or t2.a > 5000);

-- for code coverage test
explain (costs false) select * from unionor_t1 t1, unionor_t2 t2 
where t1.a = t2.a and (t1.a = 5000 or t2.b = 5000 or t2.b + t1.a = 5000);

explain (costs false) select * from unionor_t1 t1, unionor_t2 t2 
where t1.a = t2.a and (t1.a = 5000 or t2.b = 5000) and random() * 1000 > t1.a;

explain (costs false) select count(*) from unionor_t1 t1 full join unionor_t2 t2 
on true where (t1.a = 5000 or t2.b = 5000);

explain (costs false) select * from unionor_t1 t1, unionor_t2 t2 
where t1.a = t2.a and (t1.a = 5000 or t2.a = 5000 or t1.a = 3000);

-- additional test
explain (costs false) select * from unionor_t1 t1, unionor_t2 t2
where t1.a = t2.a and (t1.a = 5000 or t1.b = 2000 or t2.a = 5000);

explain (costs false) select * from unionor_t1 t1, unionor_t2 t2
where t1.a = t2.a and (t1.a = 5000 or t2.a = 2000) and (t1.a = 5000 or t2.a=3000);

explain (costs false) select * from unionor_t1 t1, unionor_t2 t2, unionor_t3 t3
where t1.a = t2.a and t2.a = t3.a and (t1.a = 1000 or t2.b = 2000 or t3.a = 3000);

explain (costs false) select count(*) from unionor_t1 t1 full join unionor_t2 t2 on true full join unionor_t3 t3
on true where t1.a = t3.b and (t1.a = 1000 or t2.b = 2000 or t3.a = 3000);

-- result of union all
select * from unionor_t1 t1, unionor_t2 t2
where t1.a = t2.a and (t1.a = 5000 or t1.b = 2000 or t2.a = 5000);

select * from unionor_t1 t1, unionor_t2 t2
where t1.a = t2.a and (t1.a = 5000 or t2.a = 2000) and (t1.a = 5000 or t2.a=3000);

select * from unionor_t1 t1, unionor_t2 t2, unionor_t3 t3
where t1.a = t2.a and t2.a = t3.a and (t1.a = 1000 or t2.b = 2000 or t3.a = 3000);

select count(*) from unionor_t1 t1 full join unionor_t2 t2 on true full join unionor_t3 t3
on true where t1.a = t3.b and (t1.a = 1000 or t2.b = 2000 or t3.a = 3000);

set polar_enable_convert_or_to_union_all = off;

-- result of join
select * from unionor_t1 t1, unionor_t2 t2
where t1.a = t2.a and (t1.a = 5000 or t1.b = 2000 or t2.a = 5000);

select * from unionor_t1 t1, unionor_t2 t2
where t1.a = t2.a and (t1.a = 5000 or t2.a = 2000) and (t1.a = 5000 or t2.a=3000);

select * from unionor_t1 t1, unionor_t2 t2, unionor_t3 t3
where t1.a = t2.a and t2.a = t3.a and (t1.a = 1000 or t2.b = 2000 or t3.a = 3000);

select count(*) from unionor_t1 t1 full join unionor_t2 t2 on true full join unionor_t3 t3
on true where t1.a = t3.b and (t1.a = 1000 or t2.b = 2000 or t3.a = 3000);

drop table unionor_t1;
drop table unionor_t2;
drop table unionor_t3;