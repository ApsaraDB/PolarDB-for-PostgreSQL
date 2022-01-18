/*--EXPLAIN_QUERY_BEGIN*/
create schema polar_partition;
SET search_path to polar_partition;

------------------------------------------------------------------------
--hash partition
--Partition constraint: satisfies_hash_partition('16384'::oid, 3, 1, id)
drop table t0_hash cascade;
CREATE TABLE t0_hash (id int, value int) PARTITION BY HASH(id);
CREATE TABLE t0_hash_p1 PARTITION OF t0_hash FOR VALUES WITH (modulus 3, remainder 0);
CREATE TABLE t0_hash_p2 PARTITION OF t0_hash FOR VALUES WITH (modulus 3, remainder 1);
CREATE TABLE t0_hash_p3 PARTITION OF t0_hash FOR VALUES WITH (modulus 3, remainder 2);
insert into t0_hash select generate_series(1,1000),generate_series(1,1000);

------------------------------------------------------------------------
--range partition
--Partition constraint: ((x IS NOT NULL) AND (x >= 10) AND (x < 20))
drop table t1_range cascade;
CREATE TABLE t1_range(id int, value int) PARTITION BY RANGE(id);
CREATE TABLE t1_range_p1 PARTITION OF t1_range FOR VALUES FROM (1) TO (10);
CREATE TABLE t1_range_p2 PARTITION OF t1_range FOR VALUES FROM (10) TO (100);
CREATE TABLE t1_range_p3 PARTITION OF t1_range DEFAULT;
insert into t1_range select generate_series(1,200, 2);

------------------------------------------------------------------------
--list
--Partition constraint: ((b IS NOT NULL) AND (b = ANY (ARRAY[1, 3])))
drop table t2_list cascade;
create table t2_list(id int, value int) partition by list (id);
CREATE TABLE t2_list_p1 PARTITION OF t2_list FOR VALUES IN (1, 2, 3, 4, 5, 6, 7, 8, 9);
CREATE TABLE t2_list_p2 PARTITION OF t2_list FOR VALUES IN (11, 12, 13, 14, 15, 16, 17, 18, 19);
CREATE TABLE t2_list_p3 PARTITION OF t2_list DEFAULT;
insert into t2_list select generate_series(1,50, 2);


------------------------------------------------------------------------
--join normal
--hash join normal
--normal table
drop table t0 cascade;
CREATE TABLE t0 (id int, value int);
insert into t0 select generate_series(1,100, 2);

analyze;

set polar_px_dop_per_node =1;
set polar_px_optimizer_enable_hashjoin=0;

\i sql/polar-px-dev/polar_partition_query.sql
\i sql/polar-px-dev/polar_partition_join.sql


CREATE INDEX idx_t0_hash ON t0_hash USING btree (id);
CREATE INDEX idx_t1_range ON t1_range USING btree (id);
CREATE INDEX idx_t2_list ON t2_list USING btree (id);
CREATE INDEX idx_t0 ON t0 USING btree (id);
analyze;

\i sql/polar-px-dev/polar_partition_query.sql
\i sql/polar-px-dev/polar_partition_join.sql


set polar_px_optimizer_share_tablescan_factor=0.9;
set polar_px_optimizer_share_indexscan_factor=0.9;
\i sql/polar-px-dev/polar_partition_join.sql

set polar_px_optimizer_enable_dynamicindexscan=0;
set polar_px_optimizer_enable_shareindexscan=0;
set polar_px_optimizer_enable_dynamicshareindexscan=0;
\i sql/polar-px-dev/polar_partition_join.sql

set client_min_messages='warning';
drop schema polar_partition cascade;
