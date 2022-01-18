
-- Test select pg_settings where group is Encryption 
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

-- test create publication for all tables for superuser, polarsuper, normaluser
\c - regressioncleanupuser
select name, vartype, enumvals, category, short_desc from pg_settings where name = 'polar_cluster_passphrase_command';
select name from pg_settings where category = 'Encryption for TDE';
select name, category from pg_settings where category like 'Encryption for TDE%';
\c - polarsuper
select name, vartype, enumvals, category, short_desc from pg_settings where name = 'polar_cluster_passphrase_command';
select name from pg_settings where category = 'Encryption for TDE';
select name, category from pg_settings where category like 'Encryption for TDE%';
\c - normaluser
select name, vartype, enumvals, category, short_desc from pg_settings where name = 'polar_cluster_passphrase_command';
select name from pg_settings where category = 'Encryption for TDE';
select name, category from pg_settings where category like 'Encryption for TDE%';

--cleanup
\c - regressioncleanupuser
\c postgres
SET client_min_messages TO 'warning';
DROP ROLE IF EXISTS polarsuper;
DROP ROLE IF EXISTS normaluser;
RESET client_min_messages;
