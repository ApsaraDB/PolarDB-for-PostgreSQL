

CREATE SCHEMA IF NOT EXISTS polar_replica;
set search_path=polar_replica,sys;

alter system set polar_create_table_with_full_replica_identity = off;
select pg_reload_conf();

select pg_sleep(2);

show polar_create_table_with_full_replica_identity;

create table t_a(a int, b int);
create table t_b(a int primary key, b int);

\d+ t_a
\d+ t_b

alter system set polar_create_table_with_full_replica_identity = on;
select pg_reload_conf();

select pg_sleep(2);

show polar_create_table_with_full_replica_identity;

create table t_c(a int, b int);
create table t_d(a int primary key, b int);

\d+ t_c
\d+ t_d

alter system reset polar_create_table_with_full_replica_identity;
select pg_reload_conf();

reset search_path;
drop schema polar_replica cascade;
