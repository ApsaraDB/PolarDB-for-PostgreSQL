-- superuser create temp table no-use tablespace
create temp table test(id int);
select pg_relation_filepath('test') ~ 'pg_tblspc';
insert into test values(generate_series(1,10));
select * from test;
update test set id = 0 where id = 10;
truncate test;
drop table test;

-- superuser create temp table use-tablespace
show temp_tablespaces;
\! rm -rf /tmp/tablespace_temp
\! mkdir /tmp/tablespace_temp
create tablespace tbs_tmp location '/tmp/tablespace_temp';
set session temp_tablespaces = 'tbs_tmp';
show temp_tablespaces;
select pg_sleep(5);
create temp table test(id int);
select pg_relation_filepath('test') ~ 'pg_tblspc';
insert into test values(generate_series(1,10));
select * from test;
update test set id = 0 where id = 10;
truncate test;
drop table test;
drop tablespace tbs_tmp;
\! ls /tmp/tablespace_temp | wc -l
\! rm -rf /tmp/tablespace_temp

-- polar superuser create tablespace
create user temp_table_local_user with polar_superuser;
\c - temp_table_local_user
\! rm -rf /tmp/tablespace_temp
\! mkdir /tmp/tablespace_temp
create tablespace tbs_tmp location '/tmp/tablespace_temp';
drop tablespace tbs_tmp;
\! ls /tmp/tablespace_temp | wc -l
\! rm -rf /tmp/tablespace_temp
\c - postgres
drop user temp_table_local_user;