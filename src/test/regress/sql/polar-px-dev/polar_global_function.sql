
/*--EXPLAIN_QUERY_BEGIN*/
create schema polar_global_function;
-- start_ignore
SET search_path to polar_global_function;
-- end_ignore

-- test polar global function
select * from generate_series(1,10);
select * from polar_global_function('generate_series',1,10);


CREATE OR REPLACE FUNCTION getrngfunc2() RETURNS setof int AS 'SELECT 100;' LANGUAGE SQL;
SELECT * FROM getrngfunc2();
select * from polar_global_function('getrngfunc2');

drop table t1;
create table t1 (c1  int, c2 varchar(20));
insert into t1 values(1,'BeiJing'),(2,'NewYork'),(3,'ShaingHai');

create or replace function getTable1() returns setof t1 as 
$$
begin
return query select * from t1;
end;
$$
language plpgsql;

select * from getTable1();
select * from polar_global_function('getTable1');
select * from polar_global_function('gettable1');


create or replace function getTable2(int) returns setof t1 as 
$$
begin
return query select * from t1 where c1 = $1;
end;
$$
language plpgsql IMMUTABLE;

select * from getTable2(1);
select * from polar_global_function('getTable2', 1);
select * from polar_global_function('gettable2', 1);


CREATE OR REPLACE FUNCTION getrngfunc1(int) RETURNS int AS 'SELECT $1 + 10;' LANGUAGE SQL IMMUTABLE;
SELECT * FROM getrngfunc1(1);
--error
select * from polar_global_function('getrngfunc1', 1);
select * from polar_global_function('getrngfunc1');
select * from polar_global_function('getrngfunc1', 1, 2);

--由于当前px并不支持view, 这里手动在should_px_planner中放开, 测试case验证通过. 当前结果集在支持view后需要更新下
select * from (select * from polar_global_function('generate_series',1,10)) t11;
select * from (select * from (select * from polar_global_function('generate_series',1,10)) t11) t12;
create or replace view v1 as select * from polar_global_function('generate_series',1,10);
\d+ v1;
select * from v1;

create or replace view v1 as select * from (select * from polar_global_function('generate_series',1,10)) t11;
\d+ v1;
select * from v1;

create or replace view v1 as select * from (select * from (select * from polar_global_function('generate_series',1,10)) t11) t12;
\d+ v1;
select * from v1;

drop view v1;

-- start_ignore
set client_min_messages='warning';
DROP SCHEMA polar_global_function CASCADE;
-- end_ignore