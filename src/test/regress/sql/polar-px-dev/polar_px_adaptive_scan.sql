-------- empty table ----------
/*--EXPLAIN_QUERY_BEGIN*/
create table t_paging_0 (id int, info text, c_time timestamp);

set polar_px_enable_adps = off;
select count(*) from t_paging_0;

set polar_px_enable_adps = on;
select count(*) from t_paging_0;

set polar_px_scan_unit_size = 1;
select count(*) from t_paging_0;

set polar_px_scan_unit_size = 512;
select count(*) from t_paging_0;

drop table t_paging_0;
-------- only 1 page ----------
create table t_paging_1 (id int, info text, c_time timestamp);
insert into  t_paging_1 select generate_series(1,2), md5(random()::text), clock_timestamp();

set polar_px_enable_adps = off;
select count(*) from t_paging_1;

set polar_px_enable_adps = on;
select count(*) from t_paging_1;

set polar_px_scan_unit_size = 1;
select count(*) from t_paging_1;

set polar_px_scan_unit_size = 512;
select count(*) from t_paging_1;

drop table t_paging_1;
-------- many page ----------
create table t_paging_2 (id int, info text, c_time timestamp);
insert into  t_paging_2 select generate_series(1,100000), md5(random()::text), clock_timestamp();

set polar_px_enable_adps = off;
select count(*) from t_paging_2;

set polar_px_enable_adps = on;
select count(*) from t_paging_2;

set polar_px_scan_unit_size = 1;
select count(*) from t_paging_2;

set polar_px_scan_unit_size = 512;
select count(*) from t_paging_2;

-- nest loop join, scan many rounds
select count(*) from t_paging_2 as a, t_paging_2 as b where a.id <= 100 and b.id <= 100;

drop table t_paging_2;

-------- px + spi ----------
create table adaptive_scan_ta (id int);
create table adaptive_scan_tb (id int);

insert into adaptive_scan_ta select generate_series(1,20000);
insert into adaptive_scan_tb select generate_series(1,20000);

create or replace function f(int) returns int as $$
declare
    val int;
begin
    select count(*) into val from adaptive_scan_tb;
    return val;
end;
$$ LANGUAGE 'plpgsql';

create or replace function g() returns int as $$
declare
    val int;
begin
    val = 0;
    for i in 1..3 loop
        val = val + f(i);
    end loop;
    return val;
end;
$$ LANGUAGE 'plpgsql' immutable;

-- should return 10, because f(id) always > 0
select count(*) from adaptive_scan_ta where id <= 10 and f(id) != 0;

-- should return 20000*3
select g();

-- explain analyze
-- Try EXPLAIN ANALYZE SELECT, but hide the output since it won't
-- be stable, only display the analyze_count
set polar_px_enable_adps_explain_analyze = on;
create or replace function explain_analyze_count() returns int language plpgsql as
$$
declare
	ln text;
	adaptive_scan_count int;
begin
    adaptive_scan_count=0;
    for ln in
        explain (analyze) select * from adaptive_scan_ta
    loop
        if ln like '%Dynamic Pages Per Worker%' then
            adaptive_scan_count=adaptive_scan_count+1;
		end if;
    end loop;
	return adaptive_scan_count;
end;
$$;
select explain_analyze_count();

drop table adaptive_scan_ta, adaptive_scan_tb;