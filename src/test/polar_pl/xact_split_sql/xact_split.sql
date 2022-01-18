create schema xact_split;

set search_path = xact_split; /* POLAR_XACT_SPLIT */

create table t(id int);

-- DML tests
insert into t values(1);

select * from t; /* POLAR_XACT_SPLIT */

begin; /* POLAR_XACT_SPLIT */
insert into t values(1);
select * from t; /* POLAR_XACT_SPLIT */
end; /* POLAR_XACT_SPLIT */

select * from t; /* POLAR_XACT_SPLIT */

begin; /* POLAR_XACT_SPLIT */
insert into t select generate_series(1,100000);
select count(*) from t; /* POLAR_XACT_SPLIT */
update t set id = 100 where id < 100;
select count(*) from t where id = 100; /* POLAR_XACT_SPLIT */
end; /* POLAR_XACT_SPLIT */

select count(*) from t; /* POLAR_XACT_SPLIT */

truncate t;

begin; /* POLAR_XACT_SPLIT */
insert into t select generate_series(1,100000);
select count(*) from t; /* POLAR_XACT_SPLIT */
rollback;

select count(*) from t; /* POLAR_XACT_SPLIT */

-- savepoint tests
begin; /* POLAR_XACT_SPLIT */
insert into t select generate_series(1,10);
savepoint a;
select * from t; /* POLAR_XACT_SPLIT */
insert into t select generate_series(11,20);
savepoint b;
select * from t; /* POLAR_XACT_SPLIT */
rollback to savepoint a;
select * from t; /* POLAR_XACT_SPLIT */
end; /* POLAR_XACT_SPLIT */

truncate t;

begin; /* POLAR_XACT_SPLIT */
insert into t select generate_series(1,10);
savepoint a;
wrong statement;
rollback to savepoint a;
select count(*) from t; /* POLAR_XACT_SPLIT */
end; /* POLAR_XACT_SPLIT */

select count(*) from t; /* POLAR_XACT_SPLIT */

-- exception tests
CREATE OR REPLACE FUNCTION GETEXCEPTION(v_phone text) RETURNS void AS
$$
BEGIN
    IF v_phone = 'iphone' THEN
       RAISE EXCEPTION 'it''s gonna run out of battery';
    ELSIF  v_phone = 'samsung' THEN
       RAISE EXCEPTION 'it''s gonna explosion';
    else
       RETURN;
    END IF;
    EXCEPTION
    WHEN others THEN
    RAISE EXCEPTION '(%)', SQLERRM;
END
$$ LANGUAGE PLPGSQL;

select GETEXCEPTION('samsung'); /* POLAR_XACT_SPLIT */

-- Transactions level tests
-- set transactions level inside function will report:
-- SET TRANSACTION ISOLATION LEVEL must be called before any query
-- NO TEST for it
begin; /* POLAR_XACT_SPLIT */
set transaction isolation level repeatable read;
insert into t select 1;
select count(*) from t; /* POLAR_XACT_SPLIT */
rollback;

-- DDL tests
begin; /* POLAR_XACT_SPLIT */
create table t1(id int, name varchar(20));
select count(*) from pg_class; /* POLAR_XACT_SPLIT */
select count(*) from pg_attribute; /* POLAR_XACT_SPLIT */
select count(*) from pg_depend; /* POLAR_XACT_SPLIT */
drop table t1;
select count(*) from pg_class; /* POLAR_XACT_SPLIT */
select count(*) from pg_attribute; /* POLAR_XACT_SPLIT */
select count(*) from pg_depend; /* POLAR_XACT_SPLIT */
end; /* POLAR_XACT_SPLIT */

-- Multi-xact tests
begin; /* POLAR_XACT_SPLIT */
select * from t for share;
select count(*) from t; /* POLAR_XACT_SPLIT */
select * from t for update;
select count(*) from t; /* POLAR_XACT_SPLIT */
select * from t for share;
end; /* POLAR_XACT_SPLIT */

drop schema xact_split cascade;
reset search_path;
