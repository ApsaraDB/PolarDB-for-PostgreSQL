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

--error
alter system set polar_max_normal_backends_factor=2.1;
alter system set polar_max_normal_backends_factor=-1;
--ok
alter system set polar_max_normal_backends_factor=0;
alter system set polar_max_normal_backends_factor=2.0;
alter system set polar_max_normal_backends_factor=1.2;
alter system reset polar_max_normal_backends_factor;
show polar_max_normal_backends_factor;

--cleanup
SET client_min_messages TO 'warning';
DROP ROLE IF EXISTS regressionuser1;
DROP ROLE IF EXISTS regressionuser2;
RESET client_min_messages;

