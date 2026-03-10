-----------------------------------------------------------------------------------
-- POLAR INIT REPLICATION USER
-- grant
-----------------------------------------------------------------------------------
create user test_rep; 
create user test_user REPLICATION;

\c - test_user

-- ok
SELECT 'init' FROM pg_create_logical_replication_slot('test_slots', 'test_decoding');
SELECT pg_drop_replication_slot('test_slots');

\c - postgres
ALTER USER test_rep REPLICATION;
\c - test_rep

-- ok
SELECT 'init' FROM pg_create_logical_replication_slot('test_slots', 'test_decoding');
SELECT pg_drop_replication_slot('test_slots');

\c - postgres
ALTER USER test_rep NOREPLICATION;

\c - test_rep
-- error
SELECT 'init' FROM pg_create_logical_replication_slot('test_slots', 'test_decoding');

\c - postgres
drop user test_rep;
drop user test_user;

-----------------------------------------------------------------------------------
-- POLAR INIT REPLICATION USER
-- alter 
-----------------------------------------------------------------------------------

-- error
create user pg_abc;

-- ok
set polar_enable_using_reserved_name = true;
create user pg_abc;
drop user pg_abc;
