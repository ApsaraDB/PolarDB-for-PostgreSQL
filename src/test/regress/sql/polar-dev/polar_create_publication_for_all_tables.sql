-- Test create publication for all tables for polar_superuser 
--
-- Clean up in case a prior regression run failed
-- Suppress NOTICE messages when users/groups don't exist
SET client_min_messages TO 'panic';
DROP ROLE IF EXISTS regressioncleanupuser;
DROP ROLE IF EXISTS polarsuper;
DROP ROLE IF EXISTS normaluser;
RESET client_min_messages;

-- create polar superuser and normal user
create user regressioncleanupuser superuser;
create user polarsuper polar_superuser login replication;
create user normaluser login createdb;

-- test create publication for all tables for polar_superuser
\c - polarsuper
create database test_publication;
\c test_publication
create table pub1(id integer, str text);
create table pub2(id integer primary key);
create publication puball for all tables;

-- failed case
\c - normaluser
create database fail_publication;
\c fail_publication
create table fail1(id integer, str text);
create table fail2(id integer primary key);
create publication pubfail for all tables;

--cleanup
\c - regressioncleanupuser
\c postgres
SET client_min_messages TO 'warning';
DROP DATABASE test_publication;
DROP DATABASE fail_publication;
DROP ROLE IF EXISTS polarsuper;
DROP ROLE IF EXISTS normaluser;
RESET client_min_messages;
