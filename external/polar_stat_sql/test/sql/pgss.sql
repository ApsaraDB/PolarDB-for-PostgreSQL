CREATE EXTENSION pg_stat_statements;
CREATE EXTENSION polar_stat_sql;

-- populate test data
create table company(
   id int primary key     not null,
   name           text    not null,
   age            int     not null,
   address        char(50),
   salary         real
);
insert into company (id,name,age,address,salary)
values (1, 'paul', 32, 'california', 20000.00 );

insert into company (id,name,age,address,salary)
values (2, 'allen', 25, 'texas', 15000.00 );

insert into company (id,name,age,address,salary)
values (3, 'teddy', 23, 'norway', 20000.00 );

insert into company (id,name,age,address,salary)
values (4, 'mark', 25, 'rich-mond ', 65000.00 );

insert into company (id,name,age,address,salary)
values (5, 'david', 27, 'texas', 85000.00 );

insert into company (id,name,age,address,salary)
values (6, 'kim', 22, 'south-hall', 45000.00 );

insert into company values (7, 'james', 24, 'houston', 10000.00 );

create table department(
   id int primary key      not null,
   dept           char(50) not null,
   emp_id         int      not null
);

insert into department (id, dept, emp_id) values (1, 'it billing', 1 );

insert into department (id, dept, emp_id) values (2, 'engineering', 2 );

insert into department (id, dept, emp_id) values (3, 'finance', 7 );

insert into department (id, dept, emp_id) values (4, 'hr', 15 );


select emp_id, name, dept from company inner join department on company.id = department.emp_id ;
select emp_id,sum(emp_id) from department group by emp_id limit 1 offset 1;

with depInfo as (select emp_id, name, dept from company inner join department on company.id = department.emp_id) select * from depInfo where emp_id >= 2 order by name;

with depInfo as (select emp_id, name, dept from company inner join department on company.id = department.emp_id) select name,sum(emp_id) from (select emp_id, name, dept from depInfo where emp_id >= 2 order by name) as temp group by name limit 10;

-- dummy query
SELECT 1 AS dummy;

SELECT scan_rows,scan_count,join_rows,join_count,sort_rows,sort_count,group_rows,group_count,hash_rows,hash_memory,hash_count FROM polar_stat_sql;

SELECT count(*) FROM polar_stat_sql WHERE datname = current_database();

SELECT count(*) FROM polar_stat_sql WHERE datname = current_database() AND (query = 'SELECT $1 AS dummy' OR query = 'SELECT ? AS dummy;');

SELECT reads, reads_blks, writes, writes_blks FROM polar_stat_sql WHERE datname = current_database() AND (query = 'SELECT $1 AS dummy' OR query = 'SELECT ? AS dummy;');

-- dummy table
CREATE TABLE test AS SELECT i FROM generate_series(1, 1000) i;

-- dummy query again
SELECT count(*) FROM test;

SELECT user_time + system_time > 0 AS cpu_time_ok FROM polar_stat_sql WHERE datname = current_database() AND query LIKE 'SELECT count(*) FROM test%';

-- test sql time 
SELECT SUM(parse_time) >0 as res from polar_stat_sql;
SELECT SUM(analyze_time) >0 as res from polar_stat_sql;
SELECT SUM(plan_time) >0 as res from polar_stat_sql;
SELECT SUM(execute_time) >0 as res from polar_stat_sql;

-- qps monitor test
create table deltb(id int primary key not null, v int not null);
alter table deltb drop column v;
drop table deltb;
create table tb(id int primary key not null, v int not null);
comment on table tb is 'a test table';
grant all on tb to postgres;
revoke all on tb from postgres;
insert into tb (id, v) values (0, 1), (1, 3);
explain (costs false) select * from tb;
update tb set v = 2 where id = 1;
delete from tb where id = 1;
alter table tb rename to newtb;
begin;
lock table newtb in access exclusive mode;
end;
truncate table newtb;
select count(*) from polar_stat_query_count;