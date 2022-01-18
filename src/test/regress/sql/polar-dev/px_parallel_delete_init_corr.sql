-- create normal table
drop table if exists px_parallel_delete_corr_t1 cascade;
CREATE TABLE px_parallel_delete_corr_t1 (c1 int, c2 int) ;
insert into px_parallel_delete_corr_t1 select generate_series(1,100),generate_series(1,100);

drop table if exists px_parallel_delete_corr_t2 cascade;
CREATE TABLE px_parallel_delete_corr_t2 (c1 int, c2 int) ;
insert into px_parallel_delete_corr_t2 select generate_series(1,100),generate_series(1,100);

drop table if exists px_parallel_delete_corr_t3 cascade;
CREATE TABLE px_parallel_delete_corr_t3 (c1 int, c2 int) ;
insert into px_parallel_delete_corr_t3 select generate_series(1,1000),generate_series(1,1000);

select count(*) from px_parallel_delete_corr_t1;
select count(*) from px_parallel_delete_corr_t2;



------------------------------------------------------------------------
--range partition
--Partition constraint: ((x IS NOT NULL) AND (x >= 10) AND (x < 20))
drop table if exists px_parallel_delete_corr_t1_range cascade;
CREATE TABLE px_parallel_delete_corr_t1_range(id int, value int) PARTITION BY RANGE(id);
CREATE TABLE px_parallel_delete_corr_t1_range_p1 PARTITION OF px_parallel_delete_corr_t1_range FOR VALUES FROM (1) TO (10000);
CREATE TABLE px_parallel_delete_corr_t1_range_p2 PARTITION OF px_parallel_delete_corr_t1_range FOR VALUES FROM (10000) TO (20000);
CREATE TABLE px_parallel_delete_corr_t1_range_p3 PARTITION OF px_parallel_delete_corr_t1_range DEFAULT;
insert into px_parallel_delete_corr_t1_range select generate_series(1,30000, 2);

-- Hash partition table
drop table if exists px_parallel_delete_corr_t0_hash cascade;
CREATE TABLE px_parallel_delete_corr_t0_hash (id int, value int) PARTITION BY HASH(id);
CREATE TABLE px_parallel_delete_corr_t0_hash_p1 PARTITION OF px_parallel_delete_corr_t0_hash FOR VALUES WITH (modulus 3, remainder 0);
CREATE TABLE px_parallel_delete_corr_t0_hash_p2 PARTITION OF px_parallel_delete_corr_t0_hash FOR VALUES WITH (modulus 3, remainder 1);
CREATE TABLE px_parallel_delete_corr_t0_hash_p3 PARTITION OF px_parallel_delete_corr_t0_hash FOR VALUES WITH (modulus 3, remainder 2);
insert into px_parallel_delete_corr_t0_hash select generate_series(1,30000),generate_series(1,30000);

-- List partition table
drop table if exists px_parallel_delete_corr_t2_list cascade;
create table px_parallel_delete_corr_t2_list(job character varying(30), pvalue int) partition by list (job);
CREATE TABLE px_parallel_delete_corr_t2_list_p1 PARTITION OF px_parallel_delete_corr_t2_list FOR VALUES IN ('student');
CREATE TABLE px_parallel_delete_corr_t2_list_p2 PARTITION OF px_parallel_delete_corr_t2_list FOR VALUES IN ('teacher');
CREATE TABLE px_parallel_delete_corr_t2_list_p3 PARTITION OF px_parallel_delete_corr_t2_list DEFAULT;
insert into px_parallel_delete_corr_t2_list select 'student',generate_series(1,10000);
insert into px_parallel_delete_corr_t2_list select 'teacher',generate_series(10000,20000);
insert into px_parallel_delete_corr_t2_list select 'other',generate_series(20000,30000);


-- Index Test Init
drop table if exists px_parallel_delete_corr_t4 cascade;
CREATE TABLE px_parallel_delete_corr_t4 (c1 int, c2 int) ;
insert into px_parallel_delete_corr_t4 select generate_series(1,1000),generate_series(1,1000);
CREATE INDEX t_index_delete_corr_plan on px_parallel_delete_corr_t4(c1);


-- Constrain Test Init
drop table if exists px_parallel_delete_corr_t5 cascade;
CREATE TABLE px_parallel_delete_corr_t5 (c1 int, c2 int not NULL);
drop table if exists px_parallel_delete_corr_t6 cascade;
CREATE TABLE px_parallel_delete_corr_t6 (c1 int, c2 int);


drop table if exists px_parallel_delete_corr_t7 cascade;
CREATE TABLE px_parallel_delete_corr_t7 (c1 int, c2 int CHECK (c2 < 1000));

drop table if exists px_parallel_delete_corr_t10 cascade;
CREATE TABLE px_parallel_delete_corr_t10(
   ID INT PRIMARY KEY     NOT NULL,
   NAME           TEXT    NOT NULL
);
insert into px_parallel_delete_corr_t10 select generate_series(1,1000), 'hello';


drop table if exists px_parallel_delete_corr_audit cascade;
CREATE TABLE px_parallel_delete_corr_audit(
   EMP_ID INT NOT NULL,
   ENTRY_DATE TEXT NOT NULL
);
insert into px_parallel_delete_corr_audit select generate_series(1,1000), 'world';

-- Create Updatable View
drop table if exists px_parallel_delete_corr_t11 cascade;
CREATE TABLE px_parallel_delete_corr_t11 (c1 int, c2 int, c3 int) ;
insert into px_parallel_delete_corr_t11 select generate_series(1,1000), generate_series(1,1000), generate_series(1,1000);

CREATE VIEW px_parallel_delete_corr_view AS select
c1,
c2,
c3
from px_parallel_delete_corr_t11
where c1 < 200;

-- Create with CTE
drop table if exists px_parallel_delete_corr_t12 cascade;
CREATE TABLE px_parallel_delete_corr_t12 (c1 int, c2 int, c3 int) ;
insert into px_parallel_delete_corr_t12 select generate_series(1,1000), generate_series(1,1000), generate_series(1,1000);
drop table if exists px_parallel_delete_corr_t13 cascade;
CREATE TABLE px_parallel_delete_corr_t13 (c1 int, c2 int, c3 int) ;
insert into px_parallel_delete_corr_t13 select generate_series(1,1000), generate_series(1,1000), generate_series(1,1000);

-- Delete twice
drop table if exists px_parallel_delete_corr_t14 cascade;
CREATE TABLE px_parallel_delete_corr_t14 (c1 int, c2 int);
insert into px_parallel_delete_corr_t14 VALUES(1,1),(1,1);
drop table if exists px_parallel_delete_corr_t15 cascade;
CREATE TABLE px_parallel_delete_corr_t15 (c1 int, c2 int);
insert into px_parallel_delete_corr_t15 select generate_series(1,1000), generate_series(1,1000);
update px_parallel_delete_corr_t15 set c1=1;


-- Delete with trigger
drop table if exists px_parallel_delete_corr_t16 cascade;
CREATE TABLE px_parallel_delete_corr_t16(c1 int, c2 int);
insert into px_parallel_delete_corr_t16 select generate_series(1,1000),generate_series(1,1000);
drop table if exists px_parallel_delete_corr_audit cascade;
CREATE TABLE px_parallel_delete_corr_audit(
   EMP_ID INT NOT NULL,
   ENTRY_DATE TEXT NOT NULL
);
insert into px_parallel_delete_corr_audit select generate_series(1,1000), 'world';

CREATE OR REPLACE FUNCTION auditlogfunc_delete_corr() RETURNS TRIGGER AS $example_table$
   BEGIN
      DELETE FROM px_parallel_delete_corr_audit where EMP_ID < 20;
      RETURN NEW;
   END;
$example_table$ LANGUAGE plpgsql;

CREATE TRIGGER px_parallel_delete_corr_trigger AFTER DELETE ON px_parallel_delete_corr_t16 FOR EACH STATEMENT EXECUTE PROCEDURE auditlogfunc_delete_corr();

ANALYZE;