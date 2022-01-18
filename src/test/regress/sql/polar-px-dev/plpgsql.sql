--
-- Test for plpgsql in parallel mode
--
/*--EXPLAIN_QUERY_BEGIN*/
drop table px_plpgsql_table;
create table px_plpgsql_table (a int, b int);

-- query in trigger will not use px
create or replace function px_trigger()
  returns trigger language plpgsql as
$$
declare
    x integer;
    id integer;
begin
    select _px_worker_id, count(*) into id, x from px_plpgsql_table group by 1;
    if id = -1 then
        RAISE NOTICE 'trigger is not parallel';
    end if;
    return new;
end;
$$;
create trigger px_trig after insert on px_plpgsql_table
  for each row execute procedure px_trigger();

insert into px_plpgsql_table values (1,1),(2,2),(3,3),(4,4);

-- IMMUTABLE function will use px
create or replace function count_only() returns int as
$$
declare
    x integer;
    id integer;
begin
    select _px_worker_id, count(*) into id, x from px_plpgsql_table group by 1;
    if id <> -1 then
        RAISE NOTICE 'IMMUTABLE function is parallel';
    end if;
    return x;
end
$$
language plpgsql 
IMMUTABLE;

select count_only();
select * from px_plpgsql_table where a + count_only() < 6;

-- IMMUTABLE function will use px
create or replace function count2() returns int as
$$
declare
    x integer;
    id integer;
begin
    select _px_worker_id, count(*) into id, x from px_plpgsql_table group by 1;
    if id <> -1 then
        RAISE NOTICE 'IMMUTABLE function is parallel';
    end if;
    return x;
end
$$
language plpgsql ;

select count2();
select * from px_plpgsql_table where a + count2() < 6;

-- normal function will not use px
create or replace function both_rw() returns int as
$$
declare
    x integer;
    id integer;
begin
    insert into px_plpgsql_table values (5,5),(6,6);
    select _px_worker_id, count(*) into id, x from px_plpgsql_table group by 1;
    if id = -1 then
        RAISE NOTICE 'NORMAL function is not parallel';
    end if;
    return x;
end
$$
language plpgsql;

select both_rw();
select * from px_plpgsql_table where a + both_rw() < 6;

create or replace function count3() returns int as
$$
declare
    x integer;
    id integer;
    rec1 record;
begin
    for rec1 in select * from px_plpgsql_table loop
        select _px_worker_id, count(*) into id, x from px_plpgsql_table group by 1;
    if id <> -1 then
        RAISE NOTICE 'IMMUTABLE function is parallel';
    end if;
    end loop;
    return x;
end
$$
language plpgsql IMMUTABLE;

select count3();

drop table px_plpgsql_table;
