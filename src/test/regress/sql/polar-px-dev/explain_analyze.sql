--
-- explain analyze for px
--

-- create table 
drop table if exists px_analyze_t1;
create table px_analyze_t1 (a int, b int);

-- Try EXPLAIN ANALYZE SELECT, but hide the output since it won't
-- be stable, only display the analyze_count
create function explain_analyze_count() returns int language plpgsql as
$$
declare 
	ln text; 
	actual_time_count int;
begin
    actual_time_count=0;
    for ln in
        explain (analyze)  select * from px_analyze_t1
    loop
        if ln like '%actual time%' then
            actual_time_count=actual_time_count+1;
		end if;
    end loop;
	return actual_time_count;
end;
$$;

-- empty table
select explain_analyze_count();

-- insert one tuple
insert into px_analyze_t1 values(1,1);
select explain_analyze_count();

-- create index
insert into px_analyze_t1(a) SELECT * FROM generate_series(1, 100000);
create index on px_analyze_t1(a);
select explain_analyze_count();

drop function explain_analyze_count;

-- analyze off,costs on,buffers on
create function explain_analyze_count() returns int language plpgsql as
$$
declare 
	ln text; 
	actual_time_count int;
begin
    actual_time_count=0;
    for ln in
        explain (analyze off, costs on, buffers on)  select * from px_analyze_t1
    loop
        if ln like '%actual time%' then
            actual_time_count=actual_time_count+1;
		end if;
    end loop;
	return actual_time_count;
end;
$$;

select explain_analyze_count();

-- json format for explain 
drop function find_seq();
create or replace function find_seq(plan json,nodeType text)
returns json language plpgsql
as
$$
declare
  x json;
  child json;
begin
  if plan->>'Node Type' = nodeType then
    return plan;
  else
    for child in select json_array_elements(plan->'Plans')
    loop
      x := find_seq(child,nodeType);
      if x is not null then
        return x;
      end if;
    end loop;
    return null;
  end if;
end;
$$;

drop function explain_analyze_count;
create function explain_analyze_count() returns int language plpgsql as
$$
declare 
	whole_plan json;
    seq_node json;
begin
    for whole_plan in
        explain (analyze on, costs on, buffers on, format "json")  select * from px_analyze_t1
    loop
        seq_node := find_seq(json_extract_path(whole_plan, '0', 'Plan'),'Seq Scan');
        return seq_node->>'Actual Loops';
    end loop;
end;
$$;

select explain_analyze_count();

drop table px_analyze_t1;