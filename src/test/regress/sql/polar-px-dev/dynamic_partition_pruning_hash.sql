
/*--EXPLAIN_QUERY_BEGIN*/
/*--EXPLAIN_ANALYZE_BEGIN*/
set polar_px_enable_adps = false;

drop schema if exists dpe_single cascade;
create schema dpe_single;
set search_path='dpe_single';

drop table if exists pt;
drop table if exists pt1;
drop table if exists t;
drop table if exists t1;

create table pt(dist int, pt1 text, pt2 text, pt3 text, ptid int) 
PARTITION BY hash(ptid);
CREATE TABLE pt_p0 PARTITION OF pt FOR VALUES WITH (modulus 7, remainder 0);
CREATE TABLE pt_p1 PARTITION OF pt FOR VALUES WITH (modulus 7, remainder 1);
CREATE TABLE pt_p2 PARTITION OF pt FOR VALUES WITH (modulus 7, remainder 2);
CREATE TABLE pt_p3 PARTITION OF pt FOR VALUES WITH (modulus 7, remainder 3);
CREATE TABLE pt_p4 PARTITION OF pt FOR VALUES WITH (modulus 7, remainder 4);
CREATE TABLE pt_p5 PARTITION OF pt FOR VALUES WITH (modulus 7, remainder 5);
CREATE TABLE pt_p6 PARTITION OF pt FOR VALUES WITH (modulus 7, remainder 6);


-- pt1 table is originally created distributed randomly
-- But a random policy impacts data distribution which
-- might lead to unstable stats info. Some test cases
-- test plan thus become flaky. We avoid flakiness by
-- creating the table distributed hashly and after
-- loading all the data, changing policy to randomly without
-- data movement. Thus every time we will have a static
-- data distribution plus randomly policy.
create table pt1(dist int, pt1 text, pt2 text, pt3 text, ptid int) 
PARTITION BY hash(ptid);
CREATE TABLE pt1_p0 PARTITION OF pt1 FOR VALUES WITH (modulus 7, remainder 0);
CREATE TABLE pt1_p1 PARTITION OF pt1 FOR VALUES WITH (modulus 7, remainder 1);
CREATE TABLE pt1_p2 PARTITION OF pt1 FOR VALUES WITH (modulus 7, remainder 2);
CREATE TABLE pt1_p3 PARTITION OF pt1 FOR VALUES WITH (modulus 7, remainder 3);
CREATE TABLE pt1_p4 PARTITION OF pt1 FOR VALUES WITH (modulus 7, remainder 4);
CREATE TABLE pt1_p5 PARTITION OF pt1 FOR VALUES WITH (modulus 7, remainder 5);
CREATE TABLE pt1_p6 PARTITION OF pt1 FOR VALUES WITH (modulus 7, remainder 6);


create table t(dist int, tid int, t1 text, t2 text);

create index pt1_idx on pt using btree (pt1);
create index ptid_idx on pt using btree (ptid);

insert into pt select i, 'hello' || i, 'world', 'drop this', i % 6 from generate_series(0,53) i;

insert into t select i, i % 6, 'hello' || i, 'bar' from generate_series(0,1) i;

create table t1(dist int, tid int, t1 text, t2 text);
insert into t1 select i, i % 6, 'hello' || i, 'bar' from generate_series(1,2) i;

insert into pt1 select * from pt;
insert into pt1 select dist, pt1, pt2, pt3, ptid-100 from pt;

analyze pt;
analyze pt1;
analyze t;
analyze t1;

--
-- Simple positive cases
--

explain (costs off, timing off, summary off) select * from t, pt where tid = ptid;

select * from t, pt where tid = ptid;

explain (costs off, timing off, summary off) select * from t, pt where tid + 1 = ptid;

select * from t, pt where tid + 1 = ptid;

explain (costs off, timing off, summary off) select * from t, pt where tid = ptid and t1 = 'hello' || tid;

select * from t, pt where tid = ptid and t1 = 'hello' || tid;

explain (costs off, timing off, summary off) select * from t, pt where t1 = pt1 and ptid = tid;

select * from t, pt where t1 = pt1 and ptid = tid;

--
-- in and exists clauses
--

explain (costs off, timing off, summary off) select * from pt where ptid in (select tid from t where t1 = 'hello' || tid);

select * from pt where ptid in (select tid from t where t1 = 'hello' || tid);

-- start_ignore
-- Known_opt_diff: MPP-21320
-- end_ignore
explain (costs off, timing off, summary off) select * from pt where exists (select 1 from t where tid = ptid and t1 = 'hello' || tid);

select * from pt where exists (select 1 from t where tid = ptid and t1 = 'hello' || tid);

--
-- group-by on top
--

explain (costs off, timing off, summary off) select count(*) from t, pt where tid = ptid;

select count(*) from t, pt where tid = ptid;

--
-- window function on top
--

explain (costs off, timing off, summary off) select *, rank() over (order by ptid,pt1) from t, pt where tid = ptid;

select *, rank() over (order by ptid,pt1) from t, pt where tid = ptid;

--
-- set ops
--

explain (costs off, timing off, summary off) select * from t, pt where tid = ptid
	  union all
	  select * from t, pt where tid + 2 = ptid;

select * from t, pt where tid = ptid
	  union all
	  select * from t, pt where tid + 2 = ptid;

--
-- set-ops
--

explain (costs off, timing off, summary off) select count(*) from
	( select * from t, pt where tid = ptid
	  union all
	  select * from t, pt where tid + 2 = ptid
	  ) foo;

select count(*) from
	( select * from t, pt where tid = ptid
	  union all
	  select * from t, pt where tid + 2 = ptid
	  ) foo;


--
-- other join types (NL)
--
set polar_px_optimizer_enable_hashjoin=off;
set polar_px_optimizer_enable_nestloopjoin=on;
set polar_px_optimizer_enable_mergejoin=off;

explain (costs off, timing off, summary off) select * from t, pt where tid = ptid;

select * from t, pt where tid = ptid;

--
-- index scan
--

set polar_px_optimizer_enable_nestloopjoin=on;
set polar_px_optimizer_enable_seqscan=off;
set polar_px_optimizer_enable_indexscan=on;
set polar_px_optimizer_enable_bitmapscan=off;
set polar_px_optimizer_enable_hashjoin=off;

-- start_ignore
-- Known_opt_diff: MPP-21322
-- end_ignore
explain (costs off, timing off, summary off) select * from t, pt where tid = ptid and pt1 = 'hello0';

select * from t, pt where tid = ptid and pt1 = 'hello0';

--
-- NL Index Scan
--
set polar_px_optimizer_enable_nestloopjoin=on;
set polar_px_optimizer_enable_indexscan=on;
set polar_px_optimizer_enable_seqscan=off;
set polar_px_optimizer_enable_hashjoin=off;

explain (costs off, timing off, summary off) select * from t, pt where tid = ptid;

select * from t, pt where tid = ptid;

--
-- Negative test cases where transform does not apply
--

set polar_px_optimizer_enable_indexscan=off;
set polar_px_optimizer_enable_seqscan=on;
set polar_px_optimizer_enable_hashjoin=on;
set polar_px_optimizer_enable_nestloopjoin=off;

explain (costs off, timing off, summary off) select * from t, pt where t1 = pt1;

select * from t, pt where t1 = pt1;

explain (costs off, timing off, summary off) select * from t, pt where tid < ptid;

select * from t, pt where tid < ptid;

reset polar_px_optimizer_enable_indexscan;
reset polar_px_optimizer_enable_seqscan;
reset polar_px_optimizer_enable_hashjoin;
reset polar_px_optimizer_enable_nestloopjoin;

--
-- multiple joins
--

-- one of the joined tables can be used for partition elimination, the other can not
explain (costs off, timing off, summary off) select * from t, t1, pt where t1.t2 = t.t2 and t1.tid = ptid;

select * from t, t1, pt where t1.t2 = t.t2 and t1.tid = ptid;

-- Both joined tables can be used for partition elimination. Only partitions
-- that contain matching rows for both joins need to be scanned.

-- have to do some tricks to coerce the planner to choose the plan we want.
begin;
insert into t select i, -100, 'dummy' from generate_series(1,10) i;
insert into t1 select i, -100, 'dummy' from generate_series(1,10) i;
analyze t;
analyze t1;

explain (costs off, timing off, summary off) select * from t, t1, pt where t1.tid = ptid and t.tid = ptid;

select * from t, t1, pt where t1.tid = ptid and t.tid = ptid;

rollback;

-- One non-joined table contributing to partition elimination in two different
-- partitioned tables
begin;
-- have to force the planner for it to consider the kind of plan we want
-- to test
set local from_collapse_limit = 1;
set local join_collapse_limit = 1;
explain (costs off, timing off, summary off) select * from t1 inner join (select pt1.*, pt2.ptid as ptid2 from pt as pt1, pt as pt2 WHERE pt1.ptid <= pt2.ptid and pt1.dist = pt2.dist ) as ptx ON t1.dist = ptx.dist and t1.tid = ptx.ptid and t1.tid = ptx.ptid2;
rollback;


--
-- Partitioned table on both sides of the join. This will create a result node as Append node is
-- not projection capable.
--

explain (costs off, timing off, summary off) select * from pt, pt1 where pt.ptid = pt1.ptid and pt.pt1 = 'hello0' order by pt1.dist;

select * from pt, pt1 where pt.ptid = pt1.ptid and pt.pt1 = 'hello0' order by pt1.dist;

explain (costs off, timing off, summary off) select count(*) from pt, pt1 where pt.ptid = pt1.ptid and pt.pt1 = 'hello0';

select count(*) from pt, pt1 where pt.ptid = pt1.ptid and pt.pt1 = 'hello0';

-- clean up
drop index pt1_idx ;
drop index ptid_idx ;

--
-- Partition Selector under Material in NestLoopJoin inner side
--

drop table if exists pt;
drop table if exists t;

create table t(id int, a int);
create table pt(id int, b int) 
PARTITION BY hash(b);
CREATE TABLE pt_p0 PARTITION OF pt FOR VALUES WITH (modulus 7, remainder 0);
CREATE TABLE pt_p1 PARTITION OF pt FOR VALUES WITH (modulus 7, remainder 1);
CREATE TABLE pt_p2 PARTITION OF pt FOR VALUES WITH (modulus 7, remainder 2);
CREATE TABLE pt_p3 PARTITION OF pt FOR VALUES WITH (modulus 7, remainder 3);
CREATE TABLE pt_p4 PARTITION OF pt FOR VALUES WITH (modulus 7, remainder 4);
CREATE TABLE pt_p5 PARTITION OF pt FOR VALUES WITH (modulus 7, remainder 5);
CREATE TABLE pt_p6 PARTITION OF pt FOR VALUES WITH (modulus 7, remainder 6);



insert into t select i, i from generate_series(0,4) i;
insert into pt select i, i from generate_series(0,4) i;
analyze t;
analyze pt;

begin;
set polar_px_optimizer_enable_hashjoin=off;
set polar_px_optimizer_enable_seqscan=on;
set polar_px_optimizer_enable_nestloopjoin=on;

explain (costs off, timing off, summary off) select * from t, pt where a = b;
select * from t, pt where a = b;
rollback;

--
-- partition selector with 0 tuples and 0 matched partitions
--

drop table if exists t;
drop table if exists pt;
create table t(a int);
create table pt(b int)  
PARTITION BY hash(b);
CREATE TABLE pt_p0 PARTITION OF pt FOR VALUES WITH (modulus 7, remainder 0);
CREATE TABLE pt_p1 PARTITION OF pt FOR VALUES WITH (modulus 7, remainder 1);
CREATE TABLE pt_p2 PARTITION OF pt FOR VALUES WITH (modulus 7, remainder 2);
CREATE TABLE pt_p3 PARTITION OF pt FOR VALUES WITH (modulus 7, remainder 3);
CREATE TABLE pt_p4 PARTITION OF pt FOR VALUES WITH (modulus 7, remainder 4);
CREATE TABLE pt_p5 PARTITION OF pt FOR VALUES WITH (modulus 7, remainder 5);
CREATE TABLE pt_p6 PARTITION OF pt FOR VALUES WITH (modulus 7, remainder 6);


begin;
set polar_px_optimizer_enable_hashjoin=off; -- foring nestloop join
set polar_px_optimizer_enable_nestloopjoin=on;
set polar_px_optimizer_enable_seqscan=on;

-- 7 in seg1, 8 in seg2, no data in seg0
insert into t select i from generate_series(7,8) i;
-- 0~2 in seg0, 3~4 in seg 1, no data in seg2
insert into pt select i from generate_series(0,4) i;

-- Insert some more rows to coerce the planner to put 'pt' on the outer
-- side of the join.
insert into t select i from generate_series(7,8) i;
insert into pt select 0 from generate_series(1,1000) g;

analyze t;
analyze pt;

explain (costs off, timing off, summary off) select * from t, pt where a = b;
select * from t, pt where a = b;
rollback;

--
-- Multi-level partitions
--

drop schema if exists dpe_multi cascade;
create schema dpe_multi;
set search_path='dpe_multi';

create table dim1(dist int, pid int, code text, t1 text);

insert into dim1 values (1, 0, 'OH', 'world1');
insert into dim1 values (1, 1, 'OH', 'world2');
insert into dim1 values (1, 100, 'GA', 'world2'); -- should not have a match at all
analyze dim1;

create table fact1(dist int, pid int, code text, u int)
PARTITION BY hash(pid);
CREATE TABLE fact1_p0 PARTITION OF fact1 FOR VALUES WITH (modulus 7, remainder 0);
CREATE TABLE fact1_p1 PARTITION OF fact1 FOR VALUES WITH (modulus 7, remainder 1);
CREATE TABLE fact1_p2 PARTITION OF fact1 FOR VALUES WITH (modulus 7, remainder 2);
CREATE TABLE fact1_p3 PARTITION OF fact1 FOR VALUES WITH (modulus 7, remainder 3);
CREATE TABLE fact1_p4 PARTITION OF fact1 FOR VALUES WITH (modulus 7, remainder 4);
CREATE TABLE fact1_p5 PARTITION OF fact1 FOR VALUES WITH (modulus 7, remainder 5);
CREATE TABLE fact1_p6 PARTITION OF fact1 FOR VALUES WITH (modulus 7, remainder 6);


insert into fact1 select 1, i % 4 , 'OH', i from generate_series (1,100) i;
insert into fact1 select 1, i % 4 , 'CA', i + 10000 from generate_series (1,100) i;

--
-- Join on all partitioning columns
--

set polar_px_optimizer_enable_partition_selection=off;
explain (costs off, timing off, summary off) select * from dim1 inner join fact1 on (dim1.pid=fact1.pid and dim1.code=fact1.code) order by fact1.u;
select * from dim1 inner join fact1 on (dim1.pid=fact1.pid and dim1.code=fact1.code) order by fact1.u;

set polar_px_optimizer_enable_partition_selection=on;
explain (costs off, timing off, summary off) select * from dim1 inner join fact1 on (dim1.pid=fact1.pid and dim1.code=fact1.code) order by fact1.u;
select * from dim1 inner join fact1 on (dim1.pid=fact1.pid and dim1.code=fact1.code) order by fact1.u;

--
-- Join on one of the partitioning columns
--

set polar_px_optimizer_enable_partition_selection=off;
explain (costs off, timing off, summary off) select * from dim1 inner join fact1 on (dim1.pid=fact1.pid) order by fact1.u;
select * from dim1 inner join fact1 on (dim1.pid=fact1.pid) order by fact1.u;

set polar_px_optimizer_enable_partition_selection=on;
explain (costs off, timing off, summary off) select * from dim1 inner join fact1 on (dim1.pid=fact1.pid) order by fact1.u;
select * from dim1 inner join fact1 on (dim1.pid=fact1.pid) order by fact1.u;

--
-- Join on the subpartitioning column only
--

set polar_px_optimizer_enable_partition_selection=off;
explain (costs off, timing off, summary off)
select * from dim1 inner join fact1 on (dim1.dist = fact1.dist and dim1.code=fact1.code);
select * from dim1 inner join fact1 on (dim1.dist = fact1.dist and dim1.code=fact1.code);

set polar_px_optimizer_enable_partition_selection=on;
explain (costs off, timing off, summary off)
select * from dim1 inner join fact1 on (dim1.dist = fact1.dist and dim1.code=fact1.code);
select * from dim1 inner join fact1 on (dim1.dist = fact1.dist and dim1.code=fact1.code);

--
-- Join on one of the partitioning columns and static elimination on other
--

set polar_px_optimizer_enable_partition_selection=off;
explain (costs off, timing off, summary off) select * from dim1 inner join fact1 on (dim1.pid=fact1.pid) and fact1.code = 'OH' order by fact1.u;
select * from dim1 inner join fact1 on (dim1.pid=fact1.pid) and fact1.code = 'OH' order by fact1.u;

set polar_px_optimizer_enable_partition_selection=on;
explain (costs off, timing off, summary off) select * from dim1 inner join fact1 on (dim1.pid=fact1.pid) and fact1.code = 'OH' order by fact1.u;
select * from dim1 inner join fact1 on (dim1.pid=fact1.pid) and fact1.code = 'OH' order by fact1.u;

--
-- add aggregates
--

set polar_px_optimizer_enable_partition_selection=off;
explain (costs off, timing off, summary off) select fact1.code, count(*) from dim1 inner join fact1 on (dim1.pid=fact1.pid) group by 1 order by 1;
select fact1.code, count(*) from dim1 inner join fact1 on (dim1.pid=fact1.pid) group by 1 order by 1;

set polar_px_optimizer_enable_partition_selection=on;
explain (costs off, timing off, summary off) select fact1.code, count(*) from dim1 inner join fact1 on (dim1.pid=fact1.pid) group by 1 order by 1;
select fact1.code, count(*) from dim1 inner join fact1 on (dim1.pid=fact1.pid) group by 1 order by 1;


--
-- multi-attribute list partitioning
--
-- Before GPDB 7, we used to support multi-column list partitions natively,
-- and these queries did partition elimination. We don't support that anymore,
-- but emulate that by using a row expression as the partitioning key. You
-- don't get partition elimination with that, however, so these tests are not
-- very interesting anymore.
--
drop schema if exists dpe_malp cascade;
create schema dpe_malp;
set search_path='dpe_malp';

create type malp_key as (i int, j int);

create table malp (i int, j int, t text) 
partition by list ((row(i, j)::malp_key));

create table malp_p1 partition of malp for values in (row(1, 10));
create table malp_p2 partition of malp for values in (row(2, 20));
create table malp_p3 partition of malp for values in (row(3, 30));

insert into malp select 1, 10, 'hello1';
insert into malp select 1, 10, 'hello2';
insert into malp select 1, 10, 'hello3';
insert into malp select 2, 20, 'hello4';
insert into malp select 2, 20, 'hello5';
insert into malp select 3, 30, 'hello6';

create table dim(i int, j int)
distributed randomly;

insert into dim values(1, 10);

analyze malp;
analyze dim;

-- ORCA doesn't do multi-attribute partitioning currently,so this falls
-- back to the Postgres planner
explain (costs off, timing off, summary off) select * from dim inner join malp on (dim.i = malp.i);

set polar_px_optimizer_enable_partition_selection = off;
select * from dim inner join malp on (dim.i = malp.i);

set polar_px_optimizer_enable_partition_selection = on;
select * from dim inner join malp on (dim.i = malp.i);

set polar_px_optimizer_enable_partition_selection = on;
-- if only the planner was smart enough, one partition would be chosen
select * from dim inner join malp on (dim.i = malp.i and dim.j = malp.j);


--
-- Plan where the Append that the PartitionSelector affects is not the immediate child
-- of the join.
--
create table apart (id int4, t text) 
range (id);
CREATE TABLE apart_p1 PARTITION of apart for values from (0) to (200);
CREATE TABLE apart_p2 PARTITION of apart for values from (200) to (400);
CREATE TABLE apart_p3 PARTITION of apart for values from (400) to (600);
CREATE TABLE apart_p4 PARTITION of apart for values from (600) to (800);
CREATE TABLE apart_p5 PARTITION of apart for values from (800) to (1000);

create table b (id int4, t text);
create table c (id int4, t text);

insert into apart select g, g from generate_series(1, 999) g;
insert into b select g, g from generate_series(1, 5) g;
insert into c select g, g from generate_series(1, 20) g;

analyze apart;
analyze b;
analyze c;

set polar_px_optimizer_enable_partition_selection = off;
explain (costs off, timing off, summary off) select * from apart as a, b, c where a.t = b.t and a.id = c.id;
select * from apart as a, b, c where a.t = b.t and a.id = c.id;

set polar_px_optimizer_enable_partition_selection = on;
explain (costs off, timing off, summary off) select * from apart as a, b, c where a.t = b.t and a.id = c.id;
select * from apart as a, b, c where a.t = b.t and a.id = c.id;


--
-- DPE: assertion failed with window function
--

drop schema if exists dpe_bugs cascade;
create schema dpe_bugs;
set search_path='dpe_bugs';

create table pat(a int, b date) partition by range (b);
CREATE TABLE pat_p1 PARTITION of pat for values from ('2010-01-01') to ('2010-01-02');
CREATE TABLE pat_p2 PARTITION of pat for values from ('2010-01-02') to ('2010-01-03');
CREATE TABLE pat_p3 PARTITION of pat for values from ('2010-01-03') to ('2010-01-04');
CREATE TABLE pat_p4 PARTITION of pat for values from ('2010-01-04') to ('2010-01-05');
CREATE TABLE pat_p5 PARTITION of pat for values from ('2010-01-01') to ('2010-01-02');
CREATE TABLE pat_p6 PARTITION of pat default;

insert into pat select i,date '2010-01-01' + i from generate_series(1, 10)i;  
create table jpat(a int, b date);
insert into jpat values(1, '2010-01-02');
analyze jpat;
-- start_ignore
-- Known_opt_diff: MPP-21323
-- end_ignore
explain (costs off, timing off, summary off) select * from (select count(*) over (order by a rows between 1 preceding and 1 following), a, b from jpat)jpat inner join pat using(b);

select * from (select count(*) over (order by a rows between 1 preceding and 1 following), a, b from jpat)jpat inner join pat using(b);


--
-- Partitioning on an expression
--
drop table if exists t;
drop table if exists pt;

create table t(id int, b int);
create table pt(id int, b int) 
PARTITION BY hash(id);
CREATE TABLE pt_p1 PARTITION OF pt FOR VALUES WITH (modulus 3, remainder 0);
CREATE TABLE pt_p2 PARTITION OF pt FOR VALUES WITH (modulus 3, remainder 1);
CREATE TABLE pt_p3 PARTITION OF pt FOR VALUES WITH (modulus 3, remainder 2);



create table ptx (id int, b int) partition by list (((b) % 2));
create table ptx_even partition of ptx for values in (0);
create table ptx_odd partition of ptx for values in (1);
alter table pt attach partition ptx for values from (4) to (20);

insert into t values (1, 1);
insert into t values (2, 2);
insert into pt select i, i from generate_series(1,7) i;
analyze t;
analyze pt;

-- Prune on the simple partition columns, but not on the expression
explain (costs off, timing off, summary off)
select * from pt, t where t.id = pt.id;

insert into t values (4, 4), (6, 6), (8, 8), (10, 10);

explain (costs off, timing off, summary off)
select * from pt, t where t.id = pt.id;

-- Plan-time pruning based on the 'id' partitioning column, and
-- run-time join pruning based on the expression
explain (costs off, timing off, summary off)
select * from pt, t where pt.id = 4 and t.id = 4 and (t.b % 2) = (pt.b % 2);

-- Mixed case
insert into pt values (4, 5);

explain (costs off, timing off, summary off)
select * from pt, t where t.id = pt.id and (t.b % 2) = (pt.b % 2);

--
-- Join pruning on an inequality qual
--
drop table if exists t;
drop table if exists pt;

create table t(dist int, tid int) ;
create table pt(dist int, ptid int)  
PARTITION BY hash(ptid);
CREATE TABLE pt_p0 PARTITION OF pt FOR VALUES WITH (modulus 7, remainder 0);
CREATE TABLE pt_p1 PARTITION OF pt FOR VALUES WITH (modulus 7, remainder 1);
CREATE TABLE pt_p2 PARTITION OF pt FOR VALUES WITH (modulus 7, remainder 2);
CREATE TABLE pt_p3 PARTITION OF pt FOR VALUES WITH (modulus 7, remainder 3);
CREATE TABLE pt_p4 PARTITION OF pt FOR VALUES WITH (modulus 7, remainder 4);
CREATE TABLE pt_p5 PARTITION OF pt FOR VALUES WITH (modulus 7, remainder 5);
CREATE TABLE pt_p6 PARTITION OF pt FOR VALUES WITH (modulus 7, remainder 6);


insert into t values (0, 4);
insert into t values (0, 3);
insert into pt select 0, i from generate_series(1,9) i;
analyze t;
analyze pt;

explain (costs off, timing off, summary off)
select * from pt, t where t.dist = pt.dist and t.tid < pt.ptid;


--
-- Test join pruning with a MergeAppend
--
drop table if exists t;
drop table if exists pt;


create table t(dist int, tid int, sk int);
create table pt(dist int, ptid int, sk int)  
PARTITION BY hash(ptid);
CREATE TABLE pt_p0 PARTITION OF pt FOR VALUES WITH (modulus 7, remainder 0);
CREATE TABLE pt_p1 PARTITION OF pt FOR VALUES WITH (modulus 7, remainder 1);
CREATE TABLE pt_p2 PARTITION OF pt FOR VALUES WITH (modulus 7, remainder 2);
CREATE TABLE pt_p3 PARTITION OF pt FOR VALUES WITH (modulus 7, remainder 3);
CREATE TABLE pt_p4 PARTITION OF pt FOR VALUES WITH (modulus 7, remainder 4);
CREATE TABLE pt_p5 PARTITION OF pt FOR VALUES WITH (modulus 7, remainder 5);
CREATE TABLE pt_p6 PARTITION OF pt FOR VALUES WITH (modulus 7, remainder 6);

insert into t values (1, 1, 1);
insert into t values (2, 2, 2);
insert into t select i, i, i from generate_series(5,100) i;

insert into pt select i, i, i from generate_series(1,7) i;
insert into pt select i, i, i from generate_series(1000, 1100) i;

analyze t;
analyze pt;

create index pt_ptid_idx on pt (ptid, sk);

set polar_px_optimizer_enable_mergejoin=on;
set polar_px_optimizer_enable_seqscan=off;

-- force_explain
explain (timing off, summary off, costs off)
select * from pt, t where t.dist = pt.dist and t.tid = pt.ptid order by t.tid, t.sk;

set client_min_messages='warning';
drop schema dpe_single cascade;
drop index  pt_ptid_idx;
reset polar_px_enable_adps;