-- Test access privileges
--
-- Clean up in case a prior regression run failed
-- Suppress NOTICE messages when users/groups don't exist
SET client_min_messages TO 'panic';
DROP ROLE IF EXISTS regressioncleanupuser;
DROP ROLE IF EXISTS regressionuser1;
DROP ROLE IF EXISTS regressionuser2;
DROP ROLE IF EXISTS regressionuser3;
DROP ROLE IF EXISTS polarsuper1;
RESET client_min_messages;
--setup
create user regressioncleanupuser superuser;
create user regressionuser1 createdb;
create user regressionuser2;
create user regressionuser3;
alter role regressionuser1 password 'yyy';
alter role regressionuser2 password 'xxx';

-- create polar superuser
create user polarsuper1 polar_superuser;
create database testpolar1;

-- test database acl 
\c - polarsuper1
alter database testpolar1 set work_mem = '4MB'; --fail
\c - regressionuser1
create database testpolar2;
\c - polarsuper1
alter database testpolar2 set work_mem = '4MB'; --ok
\c - regressioncleanupuser
drop database testpolar1;
drop database testpolar2;

-- test schema owner
create schema polarschema1;
create schema polarschema2;
\c - polarsuper1
alter schema polarschema1 owner to polarsuper1; -- fail

\c - regressioncleanupuser
alter schema polarschema2 owner to regressionuser1; -- ok

\c - polarsuper1
alter schema polarschema2 owner to polarsuper1; -- fail database permission

\c - regressioncleanupuser
drop schema polarschema1;
drop schema polarschema2;

-- test vacuum acl
\c - regressionuser1
SET client_min_messages TO 'panic';
vacuum; -- warning
RESET client_min_messages;
\c - polarsuper1
SET client_min_messages TO 'panic';
vacuum;
RESET client_min_messages;

--test set session auth
\c - regressionuser1
set session authorization regressioncleanupuser; -- fail
\c - polarsuper1
set session authorization regressioncleanupuser; -- fail
set session authorization regressionuser1; -- ok

-- test select inet_server_addr();
\c - polarsuper1
select inet_server_addr();
select inet_server_port();

-- test duration
\c - regressioncleanupuser
alter system set log_min_duration_statement = '1s';
select pg_reload_conf();
select pg_sleep(2);
alter system reset log_min_duration_statement;
select pg_reload_conf();

-- test polar_force_trans_ro_non_sup
show polar_force_trans_ro_non_sup;

-- test extension
\c - regressionuser1
create database extensiondb;

\c extensiondb regressioncleanupuser
drop extension if exists plpgsql;

\c - regressionuser1
create extension plpgsql; -- no super extension
drop extension if exists plpgsql;

\c - polarsuper1
create extension plpgsql;
drop extension if exists plpgsql;

\c - regressioncleanupuser
create extension plpgsql;
\c regression regressioncleanupuser
drop database extensiondb;

-- test polar_superuser
\c - polarsuper1
analyze pg_class;
\c - regressionuser1
analyze pg_class; -- warning

-- test tablespace pg_default
\c - regressionuser1
create database testpolar3 tablespace pg_default ;
drop database testpolar3;

-- test polar_forbidden_functions_ext
\c - regressionuser1
show polar_forbidden_functions_ext;
create table testtable1(id int);
create rule "_RETURN" as on select to testtable1 do instead select 1 as id from  pg_sleep(1); -- ok
drop view testtable1;

\c - regressioncleanupuser
alter system set polar_forbidden_functions_ext='pg_sleep';
select pg_reload_conf();
\c - regressionuser1
create table testtable1(id int);
create rule "_RETURN" as on select to testtable1 do instead select 1 as id from  pg_sleep(1); -- fail
drop table testtable1;
\c - regressioncleanupuser
alter system reset polar_forbidden_functions_ext;
select pg_reload_conf();

-- test session_preload_libraries
\c - regressioncleanupuser
set session_preload_libraries='plpgsql';
set session_preload_libraries='xxx,yyyy';
reset session_preload_libraries;

--cleanup
\c - regressioncleanupuser
SET client_min_messages TO 'warning';
DROP ROLE IF EXISTS regressionuser1;
DROP ROLE IF EXISTS regressionuser2;
DROP ROLE IF EXISTS regressionuser3;
DROP ROLE IF EXISTS polarsuper1;
RESET client_min_messages;

