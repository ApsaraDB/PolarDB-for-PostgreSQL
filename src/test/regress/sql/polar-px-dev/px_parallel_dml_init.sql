-- create normal table
drop table if exists px_parallel_dml_t1 cascade;
CREATE TABLE px_parallel_dml_t1 (c1 int, c2 int) ;
insert into px_parallel_dml_t1 select generate_series(1,100),generate_series(1,100);

drop table if exists px_parallel_dml_t2 cascade;
CREATE TABLE px_parallel_dml_t2 (c1 int, c2 int) ;
insert into px_parallel_dml_t2 select generate_series(1,100),generate_series(1,100);

drop table if exists px_parallel_dml_t3 cascade;
CREATE TABLE px_parallel_dml_t3 (c1 int, c2 int) ;
insert into px_parallel_dml_t3 select generate_series(1,1000),generate_series(1,1000);

select count(*) from px_parallel_dml_t1;
select count(*) from px_parallel_dml_t2;

-- create partition table

drop table if exists px_parallel_dml_t0_hash_insert cascade;
CREATE TABLE px_parallel_dml_t0_hash_insert (id int, value int) PARTITION BY HASH(id);
drop table if exists px_parallel_dml_t0_insert cascade;
CREATE TABLE px_parallel_dml_t0_insert (id int, value int);
drop table if exists px_parallel_dml_t0_select_table cascade;
CREATE TABLE px_parallel_dml_t0_select_table (id int, value int);
insert into px_parallel_dml_t0_select_table select generate_series(1,12000),generate_series(1,12000);
drop table if exists px_parallel_dml_t0_select_list_table cascade;
CREATE TABLE px_parallel_dml_t0_select_list_table (status character varying(30), value int);


-- PX Insert... Select from Partition Table
------------------------------------------------------------------------
-- Create partition table
drop table if exists px_parallel_dml_t0_hash cascade;
CREATE TABLE px_parallel_dml_t0_hash (id int, value int) PARTITION BY HASH(id);
CREATE TABLE px_parallel_dml_t0_hash_p1 PARTITION OF px_parallel_dml_t0_hash FOR VALUES WITH (modulus 3, remainder 0);
CREATE TABLE px_parallel_dml_t0_hash_p2 PARTITION OF px_parallel_dml_t0_hash FOR VALUES WITH (modulus 3, remainder 1);
CREATE TABLE px_parallel_dml_t0_hash_p3 PARTITION OF px_parallel_dml_t0_hash FOR VALUES WITH (modulus 3, remainder 2);
insert into px_parallel_dml_t0_hash select generate_series(1,30000),generate_series(1,30000);

------------------------------------------------------------------------
--range partition
--Partition constraint: ((x IS NOT NULL) AND (x >= 10) AND (x < 20))
drop table if exists px_parallel_dml_t1_range cascade;
CREATE TABLE px_parallel_dml_t1_range(id int, value int) PARTITION BY RANGE(id);
CREATE TABLE px_parallel_dml_t1_range_p1 PARTITION OF px_parallel_dml_t1_range FOR VALUES FROM (1) TO (10000);
CREATE TABLE px_parallel_dml_t1_range_p2 PARTITION OF px_parallel_dml_t1_range FOR VALUES FROM (10000) TO (20000);
CREATE TABLE px_parallel_dml_t1_range_p3 PARTITION OF px_parallel_dml_t1_range DEFAULT;
insert into px_parallel_dml_t1_range select generate_series(1,30000, 2);

--range partition with INDEX
drop table if exists px_parallel_dml_t1_range_index cascade;
CREATE TABLE px_parallel_dml_t1_range_index(id int, value int) PARTITION BY RANGE(id);
CREATE TABLE px_parallel_dml_t1_range_p1_index PARTITION OF px_parallel_dml_t1_range_index FOR VALUES FROM (1) TO (10000);
CREATE TABLE px_parallel_dml_t1_range_p2_index PARTITION OF px_parallel_dml_t1_range_index FOR VALUES FROM (10000) TO (20000);
CREATE TABLE px_parallel_dml_t1_range_p3_index PARTITION OF px_parallel_dml_t1_range_index DEFAULT;
insert into px_parallel_dml_t1_range_index select generate_series(1,30000, 2);
CREATE INDEX px_parallel_dml_t1_partition_index on px_parallel_dml_t1_range_index(id);



------------------------------------------------------------------------
--list
--Partition constraint: ((b IS NOT NULL) AND (b = ANY (ARRAY[1, 3])))
drop table if exists px_parallel_dml_t2_list cascade;
create table px_parallel_dml_t2_list(job character varying(30), pvalue int) partition by list (job);
CREATE TABLE px_parallel_dml_t2_list_p1 PARTITION OF px_parallel_dml_t2_list FOR VALUES IN ('student');
CREATE TABLE px_parallel_dml_t2_list_p2 PARTITION OF px_parallel_dml_t2_list FOR VALUES IN ('teacher');
CREATE TABLE px_parallel_dml_t2_list_p3 PARTITION OF px_parallel_dml_t2_list DEFAULT;
insert into px_parallel_dml_t2_list select 'student',generate_series(1,10000);
insert into px_parallel_dml_t2_list select 'teacher',generate_series(10000,20000);
insert into px_parallel_dml_t2_list select 'other',generate_series(20000,30000);

-- Index Test Init
drop table if exists px_parallel_dml_t4 cascade;
CREATE TABLE px_parallel_dml_t4 (c1 int, c2 int) ;
insert into px_parallel_dml_t4 select generate_series(1,1000),generate_series(1,1000);
CREATE INDEX t_index_plan on px_parallel_dml_t4(c1);


-- Constrain Test Init
drop table if exists px_parallel_dml_t5 cascade;
CREATE TABLE px_parallel_dml_t5 (c1 int, c2 int not NULL) ;
drop table if exists px_parallel_dml_t6 cascade;
CREATE TABLE px_parallel_dml_t6 (c1 int, c2 int) ;


---- Insert into table with default num
drop table if exists px_parallel_dml_t8 cascade;
CREATE TABLE px_parallel_dml_t8(
c1 int,
c2 int DEFAULT 99999,
c3 varchar DEFAULT NULL,
c4 timestamp default '2016-06-22 19:10:25-07'
);

drop table if exists px_parallel_dml_t9 cascade;
CREATE TABLE px_parallel_dml_t9 (c1 int, c2 int) ;
insert into px_parallel_dml_t9 select generate_series(1,20),generate_series(1,20);