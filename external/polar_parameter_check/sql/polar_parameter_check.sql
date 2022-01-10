-- create extension
SET client_min_messages TO 'error'; 
DROP EXTENSION IF EXISTS polar_parameter_check;
RESET client_min_messages;
CREATE EXTENSION polar_parameter_check;

-- name check
-- positive case
select * from polar_parameter_name_check('enable_hashjoin');
select * from polar_parameter_name_check('max_connections');
select * from polar_parameter_name_check('synchronous_standby_names');
select * from polar_parameter_name_check('wal_level');
-- negetive case
select * from polar_parameter_name_check('.');
select * from polar_parameter_name_check('xxxx');
select * from polar_parameter_name_check('0');
select * from polar_parameter_name_check('');
select * from polar_parameter_name_check(NULL);

-- value check
-- test grammar valid case

-- 1. PGC_BOOL
-- positive case
select * from polar_parameter_value_check('enable_hashjoin', 'true');
select * from polar_parameter_value_check('enable_hashjoin', 'false');
select * from polar_parameter_value_check('enable_hashjoin', '1');
select * from polar_parameter_value_check('enable_hashjoin', '0');
select * from polar_parameter_value_check('enable_hashjoin', 'on');
select * from polar_parameter_value_check('enable_hashjoin', 'off');
-- negetive case
select * from polar_parameter_value_check('enable_hashjoin', 'xx');
select * from polar_parameter_value_check('enable_hashjoin', '2');
select * from polar_parameter_value_check('enable_hashjoin', '..');
select * from polar_parameter_value_check('enable_hashjoin', '-1');

-- 2. PGC_INT
-- positive case
select * from polar_parameter_value_check('max_connections', '1');
select * from polar_parameter_value_check('max_connections', '2');
select * from polar_parameter_value_check('max_connections', '10');
select * from polar_parameter_value_check('max_connections', '100');
select * from polar_parameter_value_check('max_connections', '100000');
-- negetive case
select * from polar_parameter_value_check('max_connections', '0');
select * from polar_parameter_value_check('max_connections', '9999999');
select * from polar_parameter_value_check('max_connections', '-1');
select * from polar_parameter_value_check('max_connections', 'xx');

-- 3. PGC_REAL
-- positive case
select * from polar_parameter_value_check('jit_above_cost', '1.0');
select * from polar_parameter_value_check('jit_above_cost', '3.14');
select * from polar_parameter_value_check('jit_above_cost', '1');
select * from polar_parameter_value_check('jit_above_cost', '20.9');
-- negetive case
select * from polar_parameter_value_check('jit_above_cost', 'xx');
select * from polar_parameter_value_check('jit_above_cost', 'test_case');

-- 4. PGC_STRING
-- positive case
select * from polar_parameter_value_check('synchronous_standby_names', 'xxx');
select * from polar_parameter_value_check('synchronous_standby_names', '123');
select * from polar_parameter_value_check('synchronous_standby_names', '_standby');
select * from polar_parameter_value_check('synchronous_standby_names', 'WhUhKhSn');

select * from polar_parameter_value_check('log_line_prefix', '%m [%p]');
select * from polar_parameter_value_check('log_line_prefix', 'xxxxx');
select * from polar_parameter_value_check('log_line_prefix', '%s');

select * from polar_parameter_value_check('TimeZone', 'GMT');
select * from polar_parameter_value_check('TimeZone', 'America/Atikokan');
select * from polar_parameter_value_check('TimeZone', 'Europe/Dublin');
select * from polar_parameter_value_check('TimeZone', 'Asia/Jerusalem');

-- negetive case
select * from polar_parameter_value_check('TimeZone', 'xxxx');
select * from polar_parameter_value_check('TimeZone', 'beijing');
select * from polar_parameter_value_check('TimeZone', '090909');

-- 5. PGC_ENUM
-- positive case
select * from polar_parameter_value_check('backslash_quote', 'safe_encoding');
select * from polar_parameter_value_check('backslash_quote', 'on');
select * from polar_parameter_value_check('backslash_quote', 'off');
select * from polar_parameter_value_check('backslash_quote', 'true');
select * from polar_parameter_value_check('backslash_quote', 'false');
-- negetive case
select * from polar_parameter_value_check('backslash_quote', '2');
select * from polar_parameter_value_check('backslash_quote', 'xx');
select * from polar_parameter_value_check('backslash_quote', '234');
select * from polar_parameter_value_check('backslash_quote', '.');
select * from polar_parameter_value_check('backslash_quote', 'yueo');

-- positive case
select * from polar_parameter_value_check('wal_level', 'minimal');
select * from polar_parameter_value_check('wal_level', 'replica');
select * from polar_parameter_value_check('wal_level', 'archive');
select * from polar_parameter_value_check('wal_level', 'hot_standby');
select * from polar_parameter_value_check('wal_level', 'logical');
-- negetive case
select * from polar_parameter_value_check('wal_level', '0');
select * from polar_parameter_value_check('wal_level', 'x');
select * from polar_parameter_value_check('wal_level', '234');
select * from polar_parameter_value_check('wal_level', '.');
select * from polar_parameter_value_check('wal_level', 'yueo');

-- 6. nonexistent variable
select * from polar_parameter_value_check('abc', 'true');
select * from polar_parameter_value_check('123', 'false');

-- test poar_ or rds_ case
select * from polar_parameter_value_check('max_connections', '1');
select * from polar_parameter_value_check('polar_max_connections', '1');
select * from polar_parameter_value_check('rds_max_connections', '1');

select * from polar_parameter_value_check('max_connections', '9999');
select * from polar_parameter_value_check('polar_max_connections', '9999');
select * from polar_parameter_value_check('rds_max_connections', '9999');


select * from polar_parameter_value_check('max_connections', '1000000000');
select * from polar_parameter_value_check('polar_max_connections', '1000000000');
select * from polar_parameter_value_check('rds_max_connections', '1000000000');

-- test grammar invalid case
select * from polar_parameter_value_check('max_connections', 1111);
select * from polar_parameter_value_check('max_connections', 123);
select * from polar_parameter_value_check(123, 'xx');
select * from polar_parameter_value_check(123, 123);

-- test special case
select * from polar_parameter_value_check('max_connections', NULL);
select * from polar_parameter_value_check(NULL, NULL);
select * from polar_parameter_value_check(NULL, '1');

select * from polar_parameter_value_check('max_connections', '');
select * from polar_parameter_value_check('', '1');
select * from polar_parameter_value_check('', '');