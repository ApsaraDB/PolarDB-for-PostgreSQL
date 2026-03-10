create schema test_control_force_append_parallel;
set search_path to test_control_force_append_parallel;
set client_min_messages to error;
load 'pg_hint_plan';

create table hint_plan_t( a int,b int);create table hint_plan_t1( a int,b int);
create index on hint_plan_t1(a);
create index on hint_plan_t(a);
analyze hint_plan_t;
analyze hint_plan_t1;
-- parallel append
set pg_hint_plan.polar_enable_force_append_turn_on_parallel TO on;
explain(COSTS false) /*+ Set(enable_bitmapscan off) */ select * from (select * from hint_plan_t UNION ALL select * from hint_plan_t1) s where a = 1;
explain(COSTS false) /*+ Set() */ select * from (select * from hint_plan_t UNION ALL select * from hint_plan_t1) s where a = 1;
explain(COSTS false) /*+ Set(enable_seqscan off) */ select * from (select * from hint_plan_t UNION ALL select * from hint_plan_t1) s where a = 1;
-- non-parallel
set pg_hint_plan.polar_enable_force_append_turn_on_parallel TO off;
explain(COSTS false) /*+ Set(enable_bitmapscan off) */ select * from (select * from hint_plan_t UNION ALL select * from hint_plan_t1) s where a = 1;
explain(COSTS false) /*+ Set() */ select * from (select * from hint_plan_t UNION ALL select * from hint_plan_t1) s where a = 1;
explain(COSTS false) /*+ Set(enable_seqscan off) */ select * from (select * from hint_plan_t UNION ALL select * from hint_plan_t1) s where a = 1;

drop schema test_control_force_append_parallel cascade;