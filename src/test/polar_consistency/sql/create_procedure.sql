CALL nonexistent();  -- error
-- Not support POLARDB ORA
-- POLAR_TAG: REPLICA_IGNORE
CALL random();  -- error

CREATE FUNCTION cp_testfunc1(a int) RETURNS int LANGUAGE SQL AS $$ SELECT a $$;
-- POLAR_END_FUNC
CREATE TABLE cp_test (a int, b text);

CREATE PROCEDURE ptest1(x text)
LANGUAGE SQL
AS $$
INSERT INTO cp_test VALUES (1, x);
$$;
-- POLAR_END_PROC

\df ptest1
SELECT pg_get_functiondef('ptest1'::regproc);

-- show only normal functions
\dfn public.*test*1

-- show only procedures
\dfp public.*test*1

-- Not support POLARDB ORA
-- POLAR_TAG: REPLICA_IGNORE
SELECT ptest1('x');  -- error
-- POLAR_TAG: REPLICA_ERR
CALL ptest1('a');  -- ok
-- POLAR_TAG: REPLICA_ERR
CALL ptest1('xy' || 'zzy');  -- ok, constant-folded arg
-- POLAR_TAG: REPLICA_ERR
CALL ptest1(substring(random()::numeric(20,15)::text, 1, 1));  -- ok, volatile arg

SELECT * FROM cp_test ORDER BY b COLLATE "C";


CREATE PROCEDURE ptest2()
LANGUAGE SQL
AS $$
SELECT 5;
$$;
-- POLAR_END_PROC

CALL ptest2();


-- nested CALL
TRUNCATE cp_test;

CREATE PROCEDURE ptest3(y text)
LANGUAGE SQL
AS $$
CALL ptest1(y);
CALL ptest1($1);
$$;
-- POLAR_END_PROC
-- POLAR_TAG: REPLICA_ERR
CALL ptest3('b');

SELECT * FROM cp_test;


-- output arguments

CREATE PROCEDURE ptest4a(INOUT a int, INOUT b int)
LANGUAGE SQL
AS $$
SELECT 1, 2;
$$;
-- POLAR_END_PROC

CALL ptest4a(NULL, NULL);

CREATE PROCEDURE ptest4b(INOUT b int, INOUT a int)
LANGUAGE SQL
AS $$
CALL ptest4a(a, b);  -- error, not supported
$$;
-- POLAR_END_PROC

DROP PROCEDURE ptest4a;


-- named and default parameters

CREATE OR REPLACE PROCEDURE ptest5(a int, b text, c int default 100)
LANGUAGE SQL
AS $$
INSERT INTO cp_test VALUES(a, b);
INSERT INTO cp_test VALUES(c, b);
$$;
-- POLAR_END_PROC

TRUNCATE cp_test;

-- POLAR_TAG: REPLICA_ERR
CALL ptest5(10, 'Hello', 20);
-- POLAR_TAG: REPLICA_ERR
CALL ptest5(10, 'Hello');
-- POLAR_TAG: REPLICA_ERR
CALL ptest5(10, b => 'Hello');
-- POLAR_TAG: REPLICA_ERR
CALL ptest5(b => 'Hello', a => 10);

SELECT * FROM cp_test;


-- polymorphic types

CREATE PROCEDURE ptest6(a int, b anyelement)
LANGUAGE SQL
AS $$
SELECT NULL::int;
$$;
-- POLAR_END_PROC

CALL ptest6(1, 2);


-- collation assignment

CREATE PROCEDURE ptest7(a text, b text)
LANGUAGE SQL
AS $$
SELECT a = b;
$$;
-- POLAR_END_PROC

CALL ptest7(least('a', 'b'), 'a');


-- various error cases

CALL version();  -- error: not a procedure
CALL sum(1);  -- error: not a procedure

CREATE PROCEDURE ptestx() LANGUAGE SQL WINDOW AS $$ INSERT INTO cp_test VALUES (1, 'a') $$;
-- POLAR_END_PROC
CREATE PROCEDURE ptestx() LANGUAGE SQL STRICT AS $$ INSERT INTO cp_test VALUES (1, 'a') $$;
-- POLAR_END_PROC
CREATE PROCEDURE ptestx(OUT a int) LANGUAGE SQL AS $$ INSERT INTO cp_test VALUES (1, 'a') $$;
-- POLAR_END_PROC

ALTER PROCEDURE ptest1(text) STRICT;
ALTER FUNCTION ptest1(text) VOLATILE;  -- error: not a function
ALTER PROCEDURE cp_testfunc1(int) VOLATILE;  -- error: not a procedure
ALTER PROCEDURE nonexistent() VOLATILE;

DROP FUNCTION ptest1(text);  -- error: not a function
DROP PROCEDURE cp_testfunc1(int);  -- error: not a procedure
DROP PROCEDURE nonexistent();


-- privileges

CREATE USER regress_cp_user1;
GRANT INSERT ON cp_test TO regress_cp_user1;
REVOKE EXECUTE ON PROCEDURE ptest1(text) FROM PUBLIC;
SET ROLE regress_cp_user1;
CALL ptest1('a');  -- error
RESET ROLE;
GRANT EXECUTE ON PROCEDURE ptest1(text) TO regress_cp_user1;
SET ROLE regress_cp_user1;
-- POLAR_TAG: REPLICA_ERR
CALL ptest1('a');  -- ok
RESET ROLE;


-- ROUTINE syntax

ALTER ROUTINE cp_testfunc1(int) RENAME TO cp_testfunc1a;
ALTER ROUTINE cp_testfunc1a RENAME TO cp_testfunc1;

ALTER ROUTINE ptest1(text) RENAME TO ptest1a;
ALTER ROUTINE ptest1a RENAME TO ptest1;

DROP ROUTINE cp_testfunc1(int);


-- cleanup

DROP PROCEDURE ptest1;
DROP PROCEDURE ptest2;

DROP TABLE cp_test;

DROP USER regress_cp_user1;
