-- create normal table
drop table if exists px_parallel_update_t1 cascade;
CREATE TABLE px_parallel_update_t1 (c1 int, c2 int) ;
insert into px_parallel_update_t1 select generate_series(1,100),generate_series(1,100);

drop table if exists px_parallel_update_t2 cascade;
CREATE TABLE px_parallel_update_t2 (c1 int, c2 int) ;
insert into px_parallel_update_t2 select generate_series(1,100),generate_series(1,100);

drop table if exists px_parallel_update_t3 cascade;
CREATE TABLE px_parallel_update_t3 (c1 int, c2 int) ;
insert into px_parallel_update_t3 select generate_series(1,1000),generate_series(1,1000);

select count(*) from px_parallel_update_t1;
select count(*) from px_parallel_update_t2;


------------------------------------------------------------------------
--range partition
--Partition constraint: ((x IS NOT NULL) AND (x >= 10) AND (x < 20))
drop table if exists px_parallel_update_t1_range cascade;
CREATE TABLE px_parallel_update_t1_range(id int, value int) PARTITION BY RANGE(id);
CREATE TABLE px_parallel_update_t1_range_p1 PARTITION OF px_parallel_update_t1_range FOR VALUES FROM (1) TO (10000);
CREATE TABLE px_parallel_update_t1_range_p2 PARTITION OF px_parallel_update_t1_range FOR VALUES FROM (10000) TO (20000);
CREATE TABLE px_parallel_update_t1_range_p3 PARTITION OF px_parallel_update_t1_range DEFAULT;
insert into px_parallel_update_t1_range select generate_series(1,30000, 2);


-- Index Test Init
drop table if exists px_parallel_update_t4 cascade;
CREATE TABLE px_parallel_update_t4 (c1 int, c2 int) ;
insert into px_parallel_update_t4 select generate_series(1,1000),generate_series(1,1000);
CREATE INDEX t_index_update_plan on px_parallel_update_t4(c1);


-- Constrain Test Init
drop table if exists px_parallel_update_t5 cascade;
CREATE TABLE px_parallel_update_t5 (c1 int, c2 int not NULL) ;
drop table if exists px_parallel_update_t6 cascade;
CREATE TABLE px_parallel_update_t6 (c1 int, c2 int) ;


drop table if exists px_parallel_update_t7 cascade;
CREATE TABLE px_parallel_update_t7 (c1 int, c2 int CHECK (c2 < 1000)) ;

drop table if exists px_parallel_update_t10 cascade;
CREATE TABLE px_parallel_update_t10(
   ID INT PRIMARY KEY     NOT NULL,
   NAME           TEXT    NOT NULL
);
insert into px_parallel_update_t10 select generate_series(1,1000), 'hello';


drop table if exists px_parallel_update_audit cascade;
CREATE TABLE px_parallel_update_audit(
   EMP_ID INT NOT NULL,
   ENTRY_DATE TEXT NOT NULL
);
insert into px_parallel_update_audit select generate_series(1,1000), 'world';

CREATE OR REPLACE FUNCTION auditlogfunc() RETURNS TRIGGER AS $example_table$
   BEGIN
      UPDATE px_parallel_update_audit set EMP_ID=new.ID, ENTRY_DATE='xxxx';
      RETURN NEW;
   END;
$example_table$ LANGUAGE plpgsql;

CREATE TRIGGER px_parallel_update_trigger AFTER UPDATE ON px_parallel_update_t10 FOR EACH ROW EXECUTE PROCEDURE auditlogfunc();





-- Create Updatable View
drop table if exists px_parallel_update_t11 cascade;
CREATE TABLE px_parallel_update_t11 (c1 int, c2 int, c3 int) ;
insert into px_parallel_update_t11 select generate_series(1,1000), generate_series(1,1000), generate_series(1,1000);

CREATE VIEW px_parallel_update_view AS select
c1,
c2,
c3
from px_parallel_update_t11
where c1 < 200;

-- Create with CTE
drop table if exists px_parallel_update_t12 cascade;
CREATE TABLE px_parallel_update_t12 (c1 int, c2 int, c3 int) ;
insert into px_parallel_update_t12 select generate_series(1,1000), generate_series(1,1000), generate_series(1,1000);
drop table if exists px_parallel_update_t13 cascade;
CREATE TABLE px_parallel_update_t13 (c1 int, c2 int, c3 int) ;
insert into px_parallel_update_t13 select generate_series(1,1000), generate_series(1,1000), generate_series(1,1000);

-- Create for double delete error case
drop table if exists px_parallel_update_t14 cascade;
CREATE TABLE px_parallel_update_t14 (c1 int, c2 int);
insert into px_parallel_update_t14 VALUES(1,1);
drop table if exists px_parallel_update_t15 cascade;
CREATE TABLE px_parallel_update_t15 (c1 int, c2 int);
insert into px_parallel_update_t15 VALUES(1,2),(1,7);


ANALYZE;