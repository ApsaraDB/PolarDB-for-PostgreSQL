create extension if not exists test_px;
drop table if exists t;
CREATE TABLE t (id int) PARTITION BY RANGE (id);
CREATE TABLE t_0 PARTITION OF t FOR VALUES FROM (     0) TO (100000);
CREATE TABLE t_1 PARTITION OF t FOR VALUES FROM (100000) TO (200000);
CREATE TABLE t_2 PARTITION OF t FOR VALUES FROM (200000) TO (300000);
CREATE TABLE t_3 PARTITION OF t FOR VALUES FROM (300000) TO (400000);
CREATE TABLE t_4 PARTITION OF t FOR VALUES FROM (400000) TO (500001);

insert into t select generate_series(0, 500000);
create index on t(id);

-- rel oid
select test_px_partition_is_partition_rel('t'::regclass::oid);
select test_px_partition_is_partition_index('t'::regclass::oid);
select test_px_partition_child_index('t'::regclass::oid);

-- index oid
select test_px_partition_is_partition_rel('t_id_idx'::regclass::oid);
select test_px_partition_is_partition_index('t_id_idx'::regclass::oid);
select test_px_partition_child_index('t_id_idx'::regclass::oid);

-- sub rel
select test_px_partition_is_partition_rel('t_0'::regclass::oid);
select test_px_partition_is_partition_index('t_0'::regclass::oid);
select test_px_partition_child_index('t_0'::regclass::oid);

-- sub index
select test_px_partition_is_partition_rel('t_0_id_idx'::regclass::oid);
select test_px_partition_is_partition_index('t_0_id_idx'::regclass::oid);
select test_px_partition_child_index('t_0_id_idx'::regclass::oid);

-- close polar_px_enable_partition
set polar_px_enable_partition = 0;
explain select * from t where id < 100;