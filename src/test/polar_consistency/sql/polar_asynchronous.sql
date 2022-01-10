--
-- replicas SELECT while master DROP/ALTER/...
--

CREATE TABLE foo1 (f1 int);
create function insert_data(_tb regclass) returns void as $$
declare
begin
  for i in 1 .. 10000 loop
	EXECUTE format('INSERT INTO %s VALUES (floor(random()*(1000000-1)+1));', _tb);
  end loop;
end
$$ language plpgsql;
-- POLAR_END_FUNC

-- POLAR_TAG: REPLICA_ERR
SELECT insert_data('foo1');

-- POLAR_ASYNCHRONOUS_BEGIN
-- POLAR_SESSION_BEGIN: master
DROP TABLE foo1;
-- POLAR_SESSION_END

-- POLAR_SESSION_BEGIN: replica
-- POLAR_RESULT_STDOUT: .*SELECT\s+count.*FROM\s+foo1;\s+\d*\s*
-- POLAR_RESULT_STDERR: (.*ERROR:\s+relation.*does\s+not\s+exist.*)|(.*ERROR:\s+could\s+not\s+read\s+block.*in\s+file.*)|(.*ERROR:\s+canceling\s+statement\s+due\s+to\s+conflict\s+with\s+recovery.*)
SELECT count(*) FROM foo1;
-- POLAR_SESSION_END
-- POLAR_ASYNCHRONOUS_END

CREATE table list_parted_tbl (a int,b int) partition by list (a);
CREATE table list_parted_tbl1 partition of list_parted_tbl
  for values in (1) partition by list(b);
-- POLAR_ASYNCHRONOUS_BEGIN
-- POLAR_SESSION_BEGIN: replica
-- POLAR_RESULT_STDOUT: .*SELECT\s+count\(\*\)\s+FROM\s+list_parted_tbl;\s+.*\s*
-- POLAR_RESULT_STDERR: (.*ERROR:\s+relation.*does\s+not\s+exist.*)|(.*ERROR:\s+could\s+not\s+read\s+block.*in\s+file.*)|(.*ERROR:\s+canceling\s+statement\s+due\s+to\s+conflict\s+with\s+recovery.*)
SELECT count(*) FROM list_parted_tbl;
-- POLAR_SESSION_END
-- POLAR_SESSION_BEGIN: master
DROP TABLE list_parted_tbl;
-- POLAR_SESSION_END
-- POLAR_ASYNCHRONOUS_END
DROP function insert_data;

