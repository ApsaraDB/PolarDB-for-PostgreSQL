--
-- ORCA tests
--

-- Mask out Log & timestamp for orca message that has feature not supported.
-- start_matchsubs
-- m/^LOG.*\"Feature/
-- s/^LOG.*\"Feature/\"Feature/
-- end_matchsubs

/*--EXPLAIN_QUERY_BEGIN*/
create schema tableless_scan;
-- start_ignore
SET search_path to tableless_scan;
-- end_ignore

CREATE TABLE csq_r(a int);

INSERT INTO csq_r VALUES (1);

SELECT * FROM csq_r WHERE a IN (SELECT * FROM csq_f(csq_r.a));

set polar_px_enable_tableless_scan=true;
select * from ((select a as x from csq_r) union (select 1 as x )) as foo order by x;

explain (costs off, verbose)
select (select generate_series(1,5));
select (select generate_series(1,5));

explain (costs off, verbose)
select 'foo'::text in (select 'bar'::name union all select 'bar'::name);
select 'foo'::text in (select 'bar'::name union all select 'bar'::name);

reset polar_px_enable_tableless_scan;
select * from ((select a as x from csq_r) union (select 1 as x )) as foo order by x;

explain (costs off, verbose)
select (select generate_series(1,5));

explain (costs off, verbose)
select 'foo'::text in (select 'bar'::name union all select 'bar'::name);
select 'foo'::text in (select 'bar'::name union all select 'bar'::name);

-- start_ignore
set client_min_messages='warning';
DROP SCHEMA tableless_scan CASCADE;
-- end_ignore