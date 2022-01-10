-- Test access privileges
--
-- Clean up in case a prior regression run failed
-- Suppress NOTICE messages when users/groups don't exist
SET client_min_messages TO 'panic';
DROP ROLE IF EXISTS regressionuser1;
DROP ROLE IF EXISTS regressionuser2;
RESET client_min_messages;

--setup
create user regressionuser1;
create user regressionuser2;
alter role regressionuser1 password 'yyy';
alter role regressionuser2 password 'xxx';

--test begins
set polar_max_non_super_conns=100;
set polar_max_non_super_conns=1;
set polar_max_non_super_conns=-1;


--cleanup
SET client_min_messages TO 'warning';
DROP ROLE IF EXISTS regressionuser1;
DROP ROLE IF EXISTS regressionuser2;
RESET client_min_messages;

