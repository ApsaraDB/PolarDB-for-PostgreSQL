--
-- WITH-clause tests
--
-- These test queries are run twice in the test suite, with CTE inlining or
-- sharing, and without those options. This file is included by two launcher
-- scripts, qp_with_functional_noinlining.sql, and
-- qp_with_functional_inlining.sql, which set the desired options before
-- including this file.
--
-- The output is duplicated for both tests, since there's no include
-- mechanism for outputs in pg_regress or gpdiff. There are no differences,
-- apart from the different GUCs set by the launcher scripts, between the
-- expected outputs. Please try to keep it that way!
--
-- start_matchsubs
-- # The error message you get when you have a UDF that tries to do SQL access
-- # depends on the plan, and when the error is caught. Mask out that
-- # difference.
-- m/.*ERROR:.*function cannot execute on a QE slice because it accesses relation.*/
-- s/.*ERROR:.*/ERROR: error message might be different for CTE/
--
-- m/.*ERROR:  query plan with multiple segworker groups is not supported.*/
-- s/.*ERROR:.*/ERROR: error message might be different for CTE/
--
-- # This test file is included in the launcher script,
-- # qp_with_function.sql. psql prepends any ERRORs and NOTICEs from included
-- # files with the source filename and number. Scrub them out.
-- m/psql:sql\/qp_with_functional.sql:\d+: /
-- s/psql:sql\/qp_with_functional.sql:\d+: //
--
-- m/qp_with_functional_inlining/
-- s/qp_with_functional_inlining/qp_with_functional/
-- m/qp_with_functional_noinlining/
-- s/qp_with_functional_noinlining/qp_with_functional/
--
-- end_matchsubs

-- start_ignore
/*--EXPLAIN_QUERY_BEGIN*/
CREATE LANGUAGE plpython3u;
-- end_ignore

CREATE TABLE foo AS SELECT i as a, i+1 as b from generate_series(1,10)i;
CREATE TABLE bar AS SELECT i as c, i+1 as d from generate_series(1,10)i;

CREATE TABLE foo_ao(a int, b int) WITH ( appendonly = true);
INSERT INTO foo_ao SELECT i as a, i+1 as b FROM generate_series(1,10)i;

CREATE TABLE bar_co(c int, d int) WITH ( appendonly = true, orientation = column);
INSERT INTO bar_co SELECT i as c, i+1 as d FROM generate_series(1,10)i;

CREATE TABLE foobar (c int, d int);
INSERT INTO foobar select i, i+1 from generate_series(1,10) i;

-- @description test1: Single producer and single consumer
WITH v AS (SELECT a, b FROM foo WHERE b < 5)
SELECT * FROM v WHERE a = 1 ORDER BY 1;

-- @description test2: Single producer and multiple consumers
WITH v AS (SELECT a, b FROM foo WHERE b < 5)
SELECT v1.a, v2.b
FROM v AS v1, v AS v2
WHERE v1.a < v2.a ORDER BY 1,2;

-- @description test3: Single producer and multiple consumers, with a predicate that can be pushed down one of the consumers
WITH v AS (SELECT a, b FROM foo WHERE b < 5)
SELECT v1.a, v2.b
FROM v AS v1, v AS v2
WHERE v1.a < v2.a
AND v1.a < 10 ORDER BY 1,2;

-- @description test4: Multiple CTEs defined at the same level with no dependencies
WITH v AS (SELECT a, b FROM foo WHERE b < 5),
     w AS (SELECT c, d FROM bar WHERE c > 8)
SELECT v1.a, w1.c, w2.d
FROM v AS v1, v as v2, w AS w1, w AS w2
WHERE v1.a = v2.b
AND v1.a < w1.c
AND v1.b < w2.d ORDER BY 1,2,3;

-- @description test5: Multiple CTEs defined at the same level with dependencies
WITH v AS (SELECT a, b FROM foo WHERE b < 5),
     w AS (SELECT * FROM v WHERE a > 2)
SELECT v1.a, w1.b b1, w2.b b2
FROM v AS v1, v as v2, w AS w1, w AS w2
WHERE v1.a = v2.b
AND v1.a < w1.a
AND v1.b < w2.b ORDER BY 1;

-- @description test6: CTE defined inside a subexpression (in the FROM clause)
WITH w AS (SELECT a, b from foo where b < 5)
SELECT *
FROM foo,
     (WITH v AS (SELECT c, d FROM bar, w WHERE c = w.a AND c < 2)
      SELECT v1.c, v1.d FROM v v1, v v2 WHERE v1.c = v2.c AND v1.d > 1
     ) x
WHERE foo.a = x.c ORDER BY 1;
-- @description test7a: CTE defined inside a subquery (in the WHERE clause)
SELECT *
FROM foo 
WHERE a = (WITH v as (SELECT * FROM bar WHERE c < 2)
		    SELECT max(v1.c) FROM v v1, v v2 WHERE v1.c = v2.c) ORDER BY 1;

-- @description test7b: CTE defined inside a subquery (in the WHERE clause)
SELECT *
FROM foo
WHERE a IN (WITH v as (SELECT * FROM bar WHERE c < 2) 
            SELECT v1.c FROM v v1, v v2 WHERE v1.c = v2.c) ORDER BY 1;

-- @description test7c: CTE defined inside a subquery (in the WHERE clause)

SELECT *
FROM foo
WHERE a IN (WITH v as (SELECT * FROM bar WHERE c < 2)
            SELECT v1.c FROM v v1, v v2 WHERE v1.c = v2.c) ORDER BY 1;

-- @description test8b: CTE defined in the HAVING clause
WITH w AS (SELECT a, b FROM foo where b < 5)
SELECT a, sum(b) FROM foo
WHERE b > 1
GROUP BY a
HAVING sum(b) < ( WITH z AS (SELECT c FROM bar, w WHERE c = w.a AND c < 2) SELECT c+2 FROM z) ORDER BY 1;

-- @description test8b: CTE defined in the HAVING clause
WITH w AS (SELECT a, b FROM foo where b < 5)
SELECT a, sum(b) FROM foo
WHERE b > 1
GROUP BY a
HAVING sum(b) < ( WITH z AS (SELECT c FROM bar, w WHERE c = w.a AND c < 2) SELECT c+2 FROM z) ORDER BY 1;

-- @description test9: CTE defined inside another CTE
WITH v AS (WITH w AS (SELECT a, b FROM foo WHERE b < 5) 
SELECT w1.a, w2.b from w w1, w w2 WHERE w1.a = w2.a AND w1.a > 2)
SELECT v1.a, v2.a, v2.b
FROM v as v1, v as v2
WHERE v1.a = v2.a ORDER BY 1;

-- @description test10: Multi-level nesting
WITH v as (WITH x as (
                       SELECT * FROM foo WHERE b < 5
                     ) 
           SELECT x1.a ,x1.b FROM x x1, x x2 
           WHERE x1.a = x2.a AND x1.a = (WITH y as (
						     SELECT * FROM x
                                                   ) 
					SELECT max(y1.b) FROM y y1, y y2 WHERE y1.a < y2.a)) 
SELECT * FROM v v1, v v2 WHERE v1.a < v2.b ORDER BY 1;

-- @description test11: CTE that is defined but never used
WITH v AS (SELECT a, b FROM foo WHERE b < 5)
SELECT * FROM bar WHERE c = 8 ORDER BY 1;

-- @description test12: Full outer join query (generates a plan with CTEs)
SELECT * FROM foo FULL OUTER JOIN bar ON (foo.a = bar.c) ORDER BY 1;

-- @description test13: Query with grouping sets (generates a plan with CTEs)
SELECT a, count(*)
FROM foo GROUP BY GROUPING SETS ((),(a), (a,b)) ORDER BY 1;

-- @description test14: CTE with limit
WITH v AS (SELECT * FROM foo WHERE a < 10)
SELECT * FROM v v1, v v2 ORDER BY 1,2,3,4 LIMIT 1;

-- @description test15a: CTE with a user-defined function [IMMUTABLE NO SQL]
CREATE OR REPLACE FUNCTION cte_func1(a int) RETURNS int 
LANGUAGE plpgsql
IMMUTABLE NO SQL
AS $$
BEGIN
RETURN a + 10;
END
$$;

WITH v(a, b) AS (SELECT cte_func1(a), b FROM foo WHERE b < 5)
SELECT v1.a, v2.b
FROM v AS v1, v AS v2
WHERE v1.a < v2.a ORDER BY 1,2;

-- @description test15b: CTE with a user-defined function [IMMUTABLE CONTAINS SQL]
CREATE OR REPLACE FUNCTION cte_func1(a int) RETURNS int 
LANGUAGE plpgsql
IMMUTABLE CONTAINS SQL 
AS $$
DECLARE
    r int;
BEGIN
    SELECT $1 + 1 INTO r;
    RETURN r;
END
$$;
WITH v(a, b) AS (SELECT cte_func1(a), b FROM foo WHERE b < 5)
SELECT v1.a, v2.b
FROM v AS v1, v AS v2
WHERE v1.a < v2.a ORDER BY 1,2;

-- @description test15c: CTE with a user-defined function [STABLE NO SQL]
CREATE OR REPLACE FUNCTION cte_func1(a int) RETURNS int 
LANGUAGE plpgsql
STABLE NO SQL
AS $$
BEGIN
RETURN a + 10;
END
$$;

WITH v(a, b) AS (SELECT cte_func1(a), b FROM foo WHERE b < 5)
SELECT v1.a, v2.b
FROM v AS v1, v AS v2
WHERE v1.a < v2.a ORDER BY 1,2;

-- @description test15d: CTE with a user-defined function [STABLE CONTAINS SQL]
CREATE OR REPLACE FUNCTION cte_func1(a int) RETURNS int 
LANGUAGE plpgsql
STABLE CONTAINS SQL
AS $$
DECLARE
    r int;
BEGIN
    SELECT $1 + 1 INTO r;
    RETURN r;
END
$$;

WITH v(a, b) AS (SELECT cte_func1(a), b FROM foo WHERE b < 5)
SELECT v1.a, v2.b
FROM v AS v1, v AS v2
WHERE v1.a < v2.a ORDER BY 1,2;

-- @description test15d: CTE with a user-defined function [STABLE MODIFIES SQL DATA]
CREATE OR REPLACE FUNCTION cte_func1(a int) RETURNS int
LANGUAGE plpgsql
STABLE MODIFIES SQL DATA
AS $$
BEGIN
UPDATE foobar SET d = d+1 WHERE c = $1;
RETURN $1 + 1;
END
$$;

WITH v(a, b) AS (SELECT cte_func1(a), b FROM foo WHERE b < 5)
SELECT v1.a, v2.b
FROM v AS v1, v AS v2
WHERE v1.a < v2.a ORDER BY 1,2;

-- @description test15e: CTE with a user-defined function [STABLE READS SQL DATA]
CREATE OR REPLACE FUNCTION cte_func1(a int) RETURNS int
LANGUAGE plpgsql
STABLE READS SQL DATA
AS $$
DECLARE
    r int;
BEGIN
    SELECT d FROM foobar WHERE c = $1 LIMIT 1 INTO r;
    RETURN r;
END
$$;

WITH v(a, b) AS (SELECT cte_func1(a), b FROM foo WHERE b < 5)
SELECT v1.a, v2.b
FROM v AS v1, v AS v2
WHERE v1.a < v2.a ORDER BY 1,2;

-- @description test15g: CTE with a user-defined function [VOLATILE NO SQL]
CREATE OR REPLACE FUNCTION cte_func1(a int) RETURNS int 
LANGUAGE plpgsql
VOLATILE NO SQL
AS $$
BEGIN
RETURN a + 10;
END
$$;

WITH v(a, b) AS (SELECT cte_func1(a), b FROM foo WHERE b < 5)
SELECT v1.a, v2.b
FROM v AS v1, v AS v2
WHERE v1.a < v2.a ORDER BY 1,2;

-- @description test15h: CTE with a user-defined function [VOLATILE CONTAINS SQL]
CREATE OR REPLACE FUNCTION cte_func1(a int) RETURNS int 
LANGUAGE plpgsql
VOLATILE CONTAINS SQL
AS $$
DECLARE
    r int;
BEGIN
    SELECT $1 + 1 INTO r;
    RETURN r;
END
$$;

WITH v(a, b) AS (SELECT cte_func1(a), b FROM foo WHERE b < 5)
SELECT v1.a, v2.b
FROM v AS v1, v AS v2
WHERE v1.a < v2.a ORDER BY 1,2;

-- @description test15i: CTE with a user-defined function [VOLATILE READS SQL DATA]
CREATE OR REPLACE FUNCTION cte_func1(a int) RETURNS int 
LANGUAGE plpgsql
VOLATILE READS SQL DATA
AS $$
DECLARE
    r int;
BEGIN
    SELECT d FROM foobar WHERE c = $1 LIMIT 1 INTO r;
    RETURN r;
END
$$;

WITH v(a, b) AS (SELECT cte_func1(a), b FROM foo WHERE b < 5)
SELECT v1.a, v2.b
FROM v AS v1, v AS v2
WHERE v1.a < v2.a ORDER BY 1,2;

-- @description test15j: CTE with a user-defined function [VOLATILE MODIFIES SQL DATA]
CREATE OR REPLACE FUNCTION cte_func1(a int) RETURNS int 
LANGUAGE plpgsql
VOLATILE MODIFIES SQL DATA
AS $$
BEGIN
UPDATE foobar SET d = d+1 WHERE c = $1;
RETURN $1 + 1;
END
$$;

WITH v(a, b) AS (SELECT cte_func1(a), b FROM foo WHERE b < 5)
SELECT v1.a, v2.b
FROM v AS v1, v AS v2
WHERE v1.a < v2.a ORDER BY 1,2;

-- @description test16a: CTE within a user-defined function

-- Hide Traceback and error context information. This can throw a different
-- error depending on chosen access plan, and we mask out the error message
-- with a matchsubs block (see top of file), but the context lines are
-- easiest to mask out with this.
\set VERBOSITY terse

CREATE OR REPLACE FUNCTION cte_func3()
RETURNS SETOF int 
RETURNS NULL ON NULL INPUT
AS $$
    id = []
    rv = plpy.execute("SELECT * FROM (WITH v AS (SELECT a, b FROM foo WHERE b < 9)\
SELECT v1.a FROM v AS v1, v as v2 WHERE v1.a = v2.b)OUTERFOO ORDER BY 1", 5)
    for i in range(0,5):
        val = rv[i]["a"]
        id.append(val)
    return id
$$ LANGUAGE plpython3u READS SQL DATA;

WITH v(a, b) AS (SELECT a,b FROM foo WHERE b < 5)
SELECT * from v where b in ( select * from cte_func3()) ORDER BY 1;

\unset VERBOSITY

-- @description test16b: CTE within a user-defined function
CREATE OR REPLACE FUNCTION cte_func2()
RETURNS int 
as $$
Declare
    rcount INTEGER;
Begin
RETURN (SELECT COUNT(*) FROM (WITH v AS (SELECT a, b FROM foo WHERE b < 9),
w AS (SELECT * FROM v WHERE a < 5)
SELECT v1.a, w1.b b1, w2.b b2
FROM v AS v1, v as v2, w AS w1, w AS w2
WHERE v1.a = v2.b)foo);
End;
$$ language plpgsql READS SQL DATA;

WITH v(a, b) AS (SELECT cte_func2() as a, b FROM foo WHERE b < 5)
SELECT * from v ORDER BY 1;

-- @description test17a: CTE and views [View with a single CTE]
DROP VIEW IF EXISTS cte_view;
CREATE VIEW cte_view as 
(WITH cte(e)AS
(
	    SELECT d FROM bar
    INTERSECT 
    SELECT a FROM foo limit 10
)SELECT * FROM CTE);

\d+ cte_view

SELECT * FROM cte_view ORDER BY 1;

-- @description test17b: CTE and views [View with multiple CTE’s]
DROP VIEW IF EXISTS cte_view;
CREATE VIEW cte_view as 
( 
 WITH cte(e,f) AS (SELECT a,d FROM bar, foo WHERE foo.a = bar.d ),
      cte2(e,f) AS (SELECT e,d FROM bar, cte WHERE cte.e = bar.c )
SELECT cte2.e,cte.f FROM cte,cte2 where cte.e = cte2.e
);
\d+ cte_view

SELECT * FROM cte_view ORDER BY 1;

-- @description test18: CTE with WINDOW function
WITH CTE(a,b) AS
(SELECT a,d FROM foo, bar WHERE foo.a = bar.d),
CTE1(e,f) AS
( SELECT foo.a, rank() OVER (PARTITION BY foo.b ORDER BY CTE.a) FROM foo,CTE )
SELECT * FROM CTE1,CTE WHERE CTE.a = CTE1.f and CTE.a = 2 ORDER BY 1;

-- @description test19a :CTE with set operations [UNION]
WITH ctemax(a,b) AS
(
SELECT a,b FROM foo
),
cte(e) AS
( SELECT b FROM ctemax
UNION SELECT a FROM foo)
SELECT e FROM cte ORDER BY 1;

-- @description test19b :CTE with set operations [UNION ALL]
WITH Results_CTE AS (
    SELECT t2.a, ROW_NUMBER() OVER (ORDER BY b) AS RowNum FROM foo t2 LEFT JOIN bar ON bar.d = t2.b
UNION ALL 
    SELECT t1.b, ROW_NUMBER() OVER (ORDER BY a) AS RowNum FROM foo t1
LEFT JOIN bar ON bar.c = t1.a
 ) 
SELECT * FROM Results_CTE a INNER JOIN bar ON a.a = bar.d WHERE RowNum >= 0 AND RowNum <= 10 ORDER BY 1,2,3,4;

-- @description test19c :CTE with set operations [INTERSECT]
WITH ctemax(a,b) AS
(
    SELECT a,b FROM foo 
),
    cte(e) AS
(SELECT b FROM ctemax
INTERSECT
SELECT a FROM foo)
SELECT e FROM cte ORDER BY 1;

-- @description test19d :CTE with set operations [INTERSECT ALL]
WITH ctemax(a,b) AS( SELECT a,b FROM foo ),
    cte(e) AS(SELECT b FROM ctemax
              INTERSECT ALL
              SELECT a FROM foo)
SELECT e FROM cte ORDER BY 1;

-- @description test19e :CTE with set operations [EXCEPT]
WITH ctemax(a,b) AS
(
SELECT a,b FROM foo
),
cte(e) AS
( SELECT b FROM ctemax
EXCEPT
SELECT a FROM foo)
SELECT e FROM cte ORDER BY 1;

-- @description test19f :CTE with set operations [EXCEPT ALL]
WITH ctemax(a,b) AS
(
SELECT a,b FROM foo
),
cte(e) AS
( SELECT b FROM ctemax
EXCEPT ALL
SELECT a FROM foo)
SELECT e FROM cte ORDER BY 1;

-- @description test20: Common name for CTE and table 
CREATE TABLE v as SELECT generate_series(1,10)a;
WITH v AS (SELECT c, d FROM bar, v WHERE c = v.a ) SELECT v1.c, v1.d FROM v v1, v v2 WHERE v1.c = v2.c AND v1.d <10  ORDER BY 1;
DROP TABLE v;

-- @description test21a: Common name for CTEs and subquery alias
WITH v1 AS (SELECT a, b FROM foo WHERE a < 6), 
     v2 AS (SELECT * FROM v1 WHERE a < 3)
SELECT * 
FROM (
        SELECT * FROM v1 WHERE b < 5) v1,
       (SELECT * FROM v1) v2
WHERE v1.a =v2.b  ORDER BY 1;

-- @description test21b: Common name for table , CTE and sub-query alias
WITH foo AS (SELECT a, b FROM foo WHERE a < 5), 
     bar AS (SELECT c, d FROM bar WHERE c < 4)
SELECT * 
FROM (
        SELECT * FROM foo WHERE b < 5) foo,
       (SELECT * FROM bar) bar
WHERE foo.a =bar.d ORDER BY 1;

-- @description test22: Nested sub-query with same CTE name
SELECT avg(a3),b3
FROM
(
	WITH foo(b1,a1) AS (SELECT a,b FROM foo where a >= 1)
SELECT b3,a3 FROM
	(
 		WITH foo(b2,a2) AS ( SELECT a1,b1 FROM foo where a1 >= 1 )
  		SELECT b3,a3 FROM
 		(
			WITH foo(b3,a3) AS ( SELECT a2,b2 FROM foo where a2 >= 1 )
 			SELECT s1.b3,s1.a3 FROM foo s1,foo s2
  		) foo2
) foo1
) foo0 
GROUP BY b3 ORDER BY 1,2;

-- @description test23: CTE with Percentile function
WITH v AS (SELECT a, b FROM foo WHERE b < 5) select median(a) from v;

-- @description test24a: CTE with CSQ [ANY]
WITH newfoo AS (SELECT * FROM foo WHERE foo.a = any (SELECT bar.d FROM bar WHERE bar.d = foo.a) ORDER BY 1,2)
SELECT foo.a,newfoo.b FROM foo,newfoo WHERE foo.a = newfoo.a ORDER BY 1;

-- @description test24b: CTE with CSQ[EXISTS]  
WITH newfoo AS
	(
	     SELECT foo.* FROM foo WHERE EXISTS(SELECT bar.c FROM bar WHERE foo.b = bar.c) ORDER BY foo.b
)
SELECT
( SELECT max(CNT) FROM (SELECT count(*) CNT,nf1.b FROM newfoo nf1, newfoo nf2
WHERE nf1.a = nf2.a group by nf1.b) FOO
), * FROM newfoo ORDER BY 1,2,3;

-- @description test24c: CTE with CSQ [NOT EXISTS] 
WITH newfoo AS (
SELECT b FROM foo WHERE NOT EXISTS (SELECT * FROM bar WHERE d=a) LIMIT 1
)
SELECT foo.a,newfoo.b FROM foo,newfoo WHERE foo.a = newfoo.b ORDER BY 1;

-- @description test24d: CTE with CSQ [NOT IN] 
WITH newfoo AS (
SELECT foo.a FROM foo group by foo.a having min(foo.a) not in (SELECT bar.c FROM bar WHERE foo.a = bar.d) ORDER BY foo.a
) 
    SELECT foo.a,newfoo.a FROM foo,newfoo WHERE foo.a = newfoo.a ORDER BY 1;

-- @description test25a: CTE with different column List [Multiple CTE]
WITH CTE("A","B") as
	(SELECT c , d FROM bar WHERE c > 1),
CTE2("A","B") as
(SELECT a,b FROM foo WHERE a >6)
SELECT "A","B" from CTE2 order by "A";

-- @description test25b: CTE with different column List [Multiple CTE with dependency]
WITH CTE("A","B") as
(SELECT c , d FROM bar WHERE c > 1),
CTE2("A","B")  AS (SELECT "A","B" FROM CTE WHERE "A">6)
SELECT "A","B" from CTE2 order by "A";

-- @description test25c: Negative test - CTE with different column List , No quotes in column name
WITH CTE("A","B") as
(SELECT c , d FROM bar WHERE c > 1),
CTE2("A","B") as
(SELECT a,b FROM foo WHERE a >6)
SELECT A,B from CTE2 ORDER BY 1;

-- @description test25d: Negative Case - CTE with different column List, Ambiguous Column reference
WITH CTE(a,b) as
(SELECT c , d FROM bar WHERE c > 1)
SELECT a,b FROM CTE,foo WHERE CTE.a = foo.b ORDER BY 1;

-- @description test26a: CTE with CTAS
WITH CTE(c,d) as 
(
	SELECT a,b FROM foo WHERE a > 1
) SELECT CTE.c,bar.d FROM  CTE,bar where CTE.c = bar.d ORDER BY 1;

CREATE TABLE newfoo as 
(
	WITH CTE(c,d) as 
	(
		SELECT a,b FROM foo WHERE a > 1
	) SELECT CTE.c,bar.d FROM  CTE,bar where CTE.c = bar.d
);


SELECT * FROM newfoo ORDER BY 1;
DROP TABLE newfoo;

-- @description test26b: CTE with CTAS, sub-query
WITH CTE(a,b) as 
(
        SELECT a,b FROM foo WHERE a > 1
) 
SELECT SUBFOO.c,CTE.a FROM 
(SELECT CTE.a,bar.c FROM CTE,bar WHERE CTE.a = bar.c) SUBFOO,
CTE WHERE SUBFOO.c = CTE.b ORDER BY 1;

CREATE TABLE newfoo as 
(
	WITH CTE(a,b) as 
	(
		SELECT a,b FROM foo WHERE a > 1
	) 

	SELECT SUBFOO.c,CTE.a FROM 
		(
			SELECT CTE.a,bar.c FROM CTE,bar WHERE CTE.a = bar.c
		) SUBFOO,CTE WHERE SUBFOO.c = CTE.b
);

SELECT * FROM newfoo ORDER BY 1;

DROP TABLE newfoo;

-- @description test26c: CTE with CTAS , CTE and sub-query having same name
WITH CTE(a,b) as 

(
	SELECT a,b FROM foo WHERE a > 1
) 
SELECT CTE.* FROM 
( SELECT CTE.a,bar.c FROM CTE,bar WHERE CTE.a = bar.c) CTE ORDER BY 1;

CREATE TABLE newfoo as 
(
	WITH CTE(a,b) as 
	(
		SELECT a,b FROM foo WHERE a > 1
	) 

	SELECT CTE.* FROM 
		(
			SELECT CTE.a,bar.c FROM CTE,bar WHERE CTE.a = bar.c
		) CTE
);

SELECT * FROM newfoo ORDER BY 1;

DROP TABLE newfoo;

-- @description test27a: DML with CTE [INSERT]
CREATE TABLE newfoo (a int, b int);

WITH CTE(c,d) as 
(
	SELECT a,b FROM foo WHERE a > 1
) SELECT CTE.c,bar.d FROM  CTE,bar where CTE.c = bar.d ORDER BY 1;

INSERT INTO newfoo
(
	WITH CTE(c,d) as 
	(
		SELECT a,b FROM foo WHERE a > 1
	) SELECT CTE.c,bar.d FROM  CTE,bar where CTE.c = bar.d
);

SELECT * FROM newfoo ORDER BY 1;

DROP TABLE newfoo;

-- @description test27b: DML with CTE [INSERT with CTE and sub-query alias]
CREATE TABLE newfoo (a int, b int);
WITH CTE(a,b) as 
	(
		SELECT a,b FROM foo WHERE a > 1
	) 
SELECT SUBFOO.c,CTE.a FROM 
		(
			SELECT CTE.a,bar.c FROM CTE,bar WHERE CTE.a = bar.c
) SUBFOO,CTE  WHERE SUBFOO.c = CTE.b ORDER BY 1;


INSERT INTO newfoo
(
	WITH CTE(a,b) as 
	(
		SELECT a,b FROM foo WHERE a > 1
	) 
SELECT SUBFOO.c,CTE.a FROM 
		(
			SELECT CTE.a,bar.c FROM CTE,bar WHERE CTE.a = bar.c
		) SUBFOO,CTE  WHERE SUBFOO.c = CTE.b
);

SELECT * FROM newfoo ORDER BY 1;
DROP TABLE newfoo;

-- @description test27c: DML with CTE [INSERT with CTE and sub-query alias having common name]
CREATE TABLE newfoo (a int, b int);
WITH CTE(a,b) as 
	(
		SELECT a,b FROM foo WHERE a > 1
	) 

SELECT CTE.* FROM ( SELECT CTE.a,bar.c FROM CTE,bar WHERE CTE.a = bar.c
) CTE ORDER BY 1;

INSERT INTO newfoo
(
	WITH CTE(a,b) as 
	(
		SELECT a,b FROM foo WHERE a > 1
	) 

	SELECT CTE.* FROM 
		(
			SELECT CTE.a,bar.c FROM CTE,bar WHERE CTE.a = bar.c
		) CTE
);

SELECT * FROM newfoo ORDER BY 1;
DROP TABLE newfoo;

-- @description test27g: DML with CTE [ DELETE ]
CREATE TABLE newfoo (a int, b int);
INSERT INTO newfoo SELECT i as a, i+1 as b from generate_series(1,10)i;
SELECT * FROM newfoo ORDER BY 1;

WITH CTE(c,d) as
(
    SELECT a,b FROM foo WHERE a > 1
) 
SELECT CTE.c,bar.d FROM  CTE,bar where CTE.c = bar.d
ORDER BY 1;

DELETE FROM newfoo using(
WITH CTE(c,d) as
	(
	SELECT a,b FROM foo WHERE a > 1
	) 
	SELECT CTE.c,bar.d FROM  CTE,bar where CTE.c = bar.d
)sub;

SELECT * FROM newfoo;

DROP TABLE newfoo;

-- @description test27h: DML with CTE [ DELETE with CTE and sub-query alias]
CREATE TABLE newfoo (a int, b int);
INSERT INTO newfoo SELECT i as a, i+1 as b from generate_series(1,10)i;

SELECT * FROM newfoo ORDER BY 1;

WITH CTE(a,b) as 
(
    SELECT a,b FROM foo WHERE a > 1
) 
	SELECT SUBFOO.c,CTE.a FROM 
( SELECT CTE.a,bar.c FROM CTE,bar WHERE CTE.a = bar.c
) SUBFOO,CTE  WHERE SUBFOO.c = CTE.b ORDER BY 1;

DELETE FROM newfoo using(
	WITH CTE(a,b) as 
	(
		SELECT a,b FROM foo WHERE a > 1
	) 
	SELECT SUBFOO.c,CTE.a FROM 
		(
			SELECT CTE.a,bar.c FROM CTE,bar WHERE CTE.a = bar.c
		) SUBFOO,CTE  WHERE SUBFOO.c = CTE.b
)sub;
SELECT * FROM newfoo;

DROP TABLE newfoo;

-- @description test27i: DML with CTE [ DELETE with CTE and sub-query alias having common name]
CREATE TABLE newfoo (a int, b int);
INSERT INTO newfoo SELECT i as a, i+1 as b from generate_series(1,10)i;

SELECT * FROM newfoo ORDER BY 1;

WITH CTE(a,b) as 
(
	SELECT a,b FROM foo WHERE a > 1
) 
SELECT CTE.* FROM 
( SELECT CTE.a,bar.c FROM CTE,bar WHERE CTE.a = bar.c
) CTE ORDER BY 1;

DELETE FROM newfoo using(
	WITH CTE(a,b) as 
	(
		SELECT a,b FROM foo WHERE a > 1
	) 
	SELECT CTE.* FROM 
		(
	SELECT CTE.a,bar.c FROM CTE,bar WHERE CTE.a = bar.c
		) CTE
) sub;

SELECT * FROM newfoo;

DROP TABLE newfoo;

-- @description test28a: CTE with AO/CO tables
-- FIXME: This deadlocks with gp_cte_sharing=on, so disable that temporarily.
-- See https://github.com/greenplum-db/gpdb/issues/1967
begin;
set local gp_cte_sharing=off;
WITH v AS (SELECT a, b FROM foo_ao WHERE b < 5),
     w AS (SELECT c, d FROM bar_co WHERE c < 9)
SELECT v1.a, w1.c, w2.d
FROM v AS v1, v as v2, w AS w1, w AS w2
WHERE v1.a = v2.b
AND v1.a = w1.c
AND v1.b = w2.d ORDER BY 1;
rollback;

-- @description test28b: CTE with AO/CO tables[ Multiple CTE with dependency]
WITH v AS (SELECT a, b FROM foo_ao WHERE b < 5),
     w AS (SELECT * FROM v WHERE a < 2)
SELECT w.a, bar_co.d 
FROM w,bar_co
WHERE w.a = bar_co.c ORDER BY 1;

DROP TABLE IF EXISTS v;

-- @description test29: Negative Test - Forward Reference
WITH v AS (SELECT c, d FROM bar, v WHERE c = v.a AND c < 2) SELECT v1.c, v1.d FROM v v1, v v2 WHERE v1.c = v2.c AND v1.d > 7;

-- @description test8a: CTE defined in the HAVING clause
WITH w AS (SELECT a, b from foo where b < 5)
SELECT a, sum(b) FROM foo WHERE b > 1 GROUP BY a HAVING sum(b) < (SELECT d FROM bar, w WHERE c = w.a AND c > 2) ORDER BY 1;



-- @description MPP-15087: Executor: Nested loops in subquery scan for a CTE returns incorrect results

set enable_nestloop=on;
set enable_hashjoin=off;
set enable_mergejoin=off;
create table testtab(code char(3), n numeric);
insert into testtab values ('abc',1);
insert into testtab values ('xyz',2);
insert into testtab values ('def',3);

with cte as (
  select code, n, x 
  from testtab, (select 100 as x) d
)
select code from testtab t where 1= (select count(*) from cte where cte.code::text=t.code::text or cte.code::text = t.code::text);

with cte as (
  select count(*) from (
    select code, n, x
    from testtab, (select 100 as x) d
  ) FOO
)
select code from testtab t where 1= (select * from cte);

with cte as (
  select count(*) from (
    select code, n, x
    from testtab, (select 100 as x) d
  ) FOO
)
select code from testtab t where 1= (select count(*) from cte);

reset enable_nestloop;
reset enable_hashjoin;
reset enable_mergejoin;

-- @description MPP-19271: Unexpected internal error when we issue CTE with CSQ when we disable inlining of CTE
WITH cte AS (
    SELECT code, n, x from testtab t , (SELECT 100 as x) d ) 
SELECT code FROM testtab t WHERE (
    SELECT count(*) FROM cte WHERE cte.code::text=t.code::text
) = 1 ORDER BY 1;

-- @description MPP-19436
WITH t AS (
 SELECT e.*,f.*
 FROM (SELECT * FROM foo WHERE a < 10) e
 LEFT OUTER JOIN (SELECT * FROM bar WHERE c < 10) f ON e.a = f.d ) 
SELECT t.a,t.d, count(*) over () AS window
FROM t 
GROUP BY t.a,t.d ORDER BY t.a,t.d LIMIT 2;

WITH t(a,b,d) AS (
  SELECT foo.a,foo.b,bar.d FROM foo,bar WHERE foo.a = bar.d
)
SELECT t.b,avg(t.a), rank() OVER (PARTITION BY t.a ORDER BY t.a) FROM foo,t GROUP BY foo.a,foo.b,t.b,t.a ORDER BY 1,2,3 LIMIT 5;

WITH t(a,b,d) AS (
  SELECT foo.a,foo.b,bar.d FROM foo,bar WHERE foo.a = bar.d
)
SELECT cup.*, SUM(t.d) OVER(PARTITION BY t.b)
FROM (
  SELECT bar.*, AVG(t.b) OVER(PARTITION BY t.a ORDER BY t.b desc) AS e FROM t,bar
) AS cup, t
WHERE cup.e < 10
GROUP BY cup.c,cup.d, cup.e ,t.d, t.b
ORDER BY 1,2,3,4
LIMIT 10;

WITH t(a,b,d) AS (
  SELECT foo.a,foo.b,bar.d FROM foo,bar WHERE foo.a = bar.d
)
SELECT cup.*, SUM(t.d) FROM ( 
  SELECT bar.*, count(*) OVER() AS e FROM t,bar WHERE t.a = bar.c
) AS cup, t
GROUP BY cup.c,cup.d, cup.e,t.a
HAVING AVG(t.d) < 10 ORDER BY 1,2,3,4 LIMIT 10;

WITH t(a,b,d) AS (
  SELECT foo.a,foo.b,bar.d FROM foo,bar WHERE foo.a = bar.d
)
SELECT cup.*, SUM(t.d) OVER(PARTITION BY t.b) FROM ( 
  SELECT bar.c as e,r.d
  FROM (
    SELECT t.d, avg(t.a) over() FROM t
  ) r, bar
) AS cup,
t WHERE cup.e < 10
GROUP BY cup.d, cup.e, t.d, t.b
ORDER BY 1,2,3 
LIMIT 10;

-- @description MPP-19696
CREATE TABLE r(a int, b int);
INSERT INTO r SELECT i,i FROM generate_series(1,5)i;

WITH v1 AS (SELECT b FROM r), v2 as (SELECT b FROM v1) SELECT * FROM v2 WHERE b < 5 ORDER BY 1;

-- @description Mpp-19991
CREATE TABLE x AS SELECT generate_series(1,10);
CREATE TABLE y AS SELECT generate_series(1,10);

with v1 as (select * from x), v2 as (select * from y) select * from v1;
