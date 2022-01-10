-- Test access privileges
--
-- Clean up in case a prior regression run failed
-- Suppress NOTICE messages when users/groups don't exist
SET client_min_messages TO 'panic';
DROP ROLE IF EXISTS regressioncleanupuser;
DROP ROLE IF EXISTS regressionuser1;
DROP ROLE IF EXISTS regressionuser2;
DROP ROLE IF EXISTS regressionuser3;
RESET client_min_messages;
--setup
create user regressioncleanupuser superuser;
create user regressionuser1;
create user regressionuser2;
create user regressionuser3;
alter role regressionuser1 password 'yyy';
alter role regressionuser2 password 'xxx';

--we can not show, because different versions may have different results 
--show polar_release_date;
set polar_release_date="20150101";
--show polar_release_date;
reset all;
--show polar_release_date;
reset polar_release_date;
--show polar_release_date;

--cleanup
\c - regressioncleanupuser
SET client_min_messages TO 'warning';
DROP ROLE IF EXISTS regressionuser1;
DROP ROLE IF EXISTS regressionuser2;
DROP ROLE IF EXISTS regressionuser3;
RESET client_min_messages;

