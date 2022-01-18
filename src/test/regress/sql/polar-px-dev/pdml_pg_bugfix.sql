/*--POLAR_ENABLE_PX*/
set polar_enable_px = 1;
set polar_px_enable_insert_select = 1;
set polar_px_optimizer_enable_dml_constraints = 1;
set polar_px_enable_update = 1;
set polar_px_enable_delete = 1;
set polar_px_enable_insert_from_tableless = 1;
SET polar_px_optimizer_trace_fallback=1;
set client_min_messages = 'FATAL';




/*
    abstime case
*/
CREATE TABLE ABSTIME_TBL (f1 abstime);

BEGIN;
INSERT INTO ABSTIME_TBL (f1) VALUES (abstime 'now');
INSERT INTO ABSTIME_TBL (f1) VALUES (abstime 'now');
SELECT count(*) AS two FROM ABSTIME_TBL WHERE f1 = 'now' ;
END;

DELETE FROM ABSTIME_TBL;

INSERT INTO ABSTIME_TBL (f1) VALUES ('Jan 14, 1973 03:14:21');
INSERT INTO ABSTIME_TBL (f1) VALUES (abstime 'Mon May  1 00:30:30 1995');
INSERT INTO ABSTIME_TBL (f1) VALUES (abstime 'epoch');
INSERT INTO ABSTIME_TBL (f1) VALUES (abstime 'infinity');
INSERT INTO ABSTIME_TBL (f1) VALUES (abstime '-infinity');
INSERT INTO ABSTIME_TBL (f1) VALUES (abstime 'May 10, 1947 23:59:12');

-- what happens if we specify slightly misformatted abstime?
INSERT INTO ABSTIME_TBL (f1) VALUES ('Feb 35, 1946 10:00:00');
INSERT INTO ABSTIME_TBL (f1) VALUES ('Feb 28, 1984 25:08:10');

-- badly formatted abstimes:  these should result in invalid abstimes
INSERT INTO ABSTIME_TBL (f1) VALUES ('bad date format');
EXPLAIN (VERBOSE, COSTS OFF) INSERT INTO ABSTIME_TBL (f1) VALUES ('Jun 10, 1843');
INSERT INTO ABSTIME_TBL (f1) VALUES ('Jun 10, 1843');

-- test abstime operators

SELECT '' AS eight, * FROM ABSTIME_TBL;

SELECT '' AS six, * FROM ABSTIME_TBL
   WHERE ABSTIME_TBL.f1 < abstime 'Jun 30, 2001';

SELECT '' AS six, * FROM ABSTIME_TBL
   WHERE ABSTIME_TBL.f1 > abstime '-infinity';

SELECT '' AS six, * FROM ABSTIME_TBL
   WHERE abstime 'May 10, 1947 23:59:12' <> ABSTIME_TBL.f1;

SELECT '' AS three, * FROM ABSTIME_TBL
   WHERE abstime 'epoch' >= ABSTIME_TBL.f1;

SELECT '' AS four, * FROM ABSTIME_TBL
   WHERE ABSTIME_TBL.f1 <= abstime 'Jan 14, 1973 03:14:21';

SELECT '' AS four, * FROM ABSTIME_TBL
  WHERE ABSTIME_TBL.f1 <?>
        tinterval '["Apr 1 1950 00:00:00" "Dec 30 1999 23:00:00"]';

SELECT '' AS four, f1 AS abstime,
  date_part('year', f1) AS year, date_part('month', f1) AS month,
  date_part('day',f1) AS day, date_part('hour', f1) AS hour,
  date_part('minute', f1) AS minute, date_part('second', f1) AS second
  FROM ABSTIME_TBL
  WHERE isfinite(f1)
  ORDER BY abstime;


/* 
 * index include
*/
drop table tbl;
CREATE TABLE tbl (c1 int, c2 int, c3 int, c4 box);
INSERT INTO tbl SELECT x, 2*x, 3*x, box('4,4,4,4') FROM generate_series(1,10) AS x;
CREATE UNIQUE INDEX tbl_idx_unique ON tbl using btree(c1, c2) INCLUDE (c3,c4);
-- should pass
EXPLAIN (COSTS OFF) UPDATE tbl SET c1 = 100 WHERE c1 = 2;
UPDATE tbl SET c1 = 100 WHERE c1 = 2;
EXPLAIN (COSTS OFF) SELECT * from tbl where c1 < 100;
SELECT * from tbl where c1 < 100;
EXPLAIN (COSTS OFF) UPDATE tbl SET c1 = 1 WHERE c1 = 3;
UPDATE tbl SET c1 = 1 WHERE c1 = 3;
EXPLAIN (COSTS OFF) SELECT * from tbl where c1 < 100 order by c2;
SELECT * from tbl where c1 < 100 order by c2;
-- should fail
UPDATE tbl SET c2 = 2 WHERE c1 = 1;
UPDATE tbl SET c3 = 1;
DELETE FROM tbl WHERE c1 = 5 OR c3 = 12;
select * from tbl order by c2;
DROP TABLE tbl;

/*
 * update trigger
*/

CREATE TABLE transition_table_level1
(
      level1_no serial NOT NULL ,
      level1_node_name varchar(255),
       PRIMARY KEY (level1_no)
) WITHOUT OIDS;

CREATE TABLE transition_table_level2
(
      level2_no serial NOT NULL ,
      parent_no int NOT NULL,
      level1_node_name varchar(255),
       PRIMARY KEY (level2_no)
) WITHOUT OIDS;

CREATE TABLE transition_table_status
(
      level int NOT NULL,
      node_no int NOT NULL,
      status int,
       PRIMARY KEY (level, node_no)
) WITHOUT OIDS;

CREATE FUNCTION transition_table_level1_ri_parent_del_func()
  RETURNS TRIGGER
  LANGUAGE plpgsql
AS $$
  DECLARE n bigint;
  BEGIN
    PERFORM FROM p JOIN transition_table_level2 c ON c.parent_no = p.level1_no;
    IF FOUND THEN
      RAISE EXCEPTION 'RI error';
    END IF;
    RETURN NULL;
  END;
$$;

CREATE TRIGGER transition_table_level1_ri_parent_del_trigger
  AFTER DELETE ON transition_table_level1
  REFERENCING OLD TABLE AS p
  FOR EACH STATEMENT EXECUTE PROCEDURE
    transition_table_level1_ri_parent_del_func();

CREATE FUNCTION transition_table_level1_ri_parent_upd_func()
  RETURNS TRIGGER
  LANGUAGE plpgsql
AS $$
  DECLARE
    x int;
  BEGIN
    WITH p AS (SELECT level1_no, sum(delta) cnt
                 FROM (SELECT level1_no, 1 AS delta FROM i
                       UNION ALL
                       SELECT level1_no, -1 AS delta FROM d) w
                 GROUP BY level1_no
                 HAVING sum(delta) < 0)
    SELECT level1_no
      FROM p JOIN transition_table_level2 c ON c.parent_no = p.level1_no
      INTO x;
    IF FOUND THEN
      RAISE EXCEPTION 'RI error';
    END IF;
    RETURN NULL;
  END;
$$;

CREATE TRIGGER transition_table_level1_ri_parent_upd_trigger
  AFTER UPDATE ON transition_table_level1
  REFERENCING OLD TABLE AS d NEW TABLE AS i
  FOR EACH STATEMENT EXECUTE PROCEDURE
    transition_table_level1_ri_parent_upd_func();

CREATE FUNCTION transition_table_level2_ri_child_insupd_func()
  RETURNS TRIGGER
  LANGUAGE plpgsql
AS $$
  BEGIN
    PERFORM FROM i
      LEFT JOIN transition_table_level1 p
        ON p.level1_no IS NOT NULL AND p.level1_no = i.parent_no
      WHERE p.level1_no IS NULL;
    IF FOUND THEN
      RAISE EXCEPTION 'RI error';
    END IF;
    RETURN NULL;
  END;
$$;

CREATE TRIGGER transition_table_level2_ri_child_ins_trigger
  AFTER INSERT ON transition_table_level2
  REFERENCING NEW TABLE AS i
  FOR EACH STATEMENT EXECUTE PROCEDURE
    transition_table_level2_ri_child_insupd_func();

CREATE TRIGGER transition_table_level2_ri_child_upd_trigger
  AFTER UPDATE ON transition_table_level2
  REFERENCING NEW TABLE AS i
  FOR EACH STATEMENT EXECUTE PROCEDURE
    transition_table_level2_ri_child_insupd_func();

-- create initial test data
INSERT INTO transition_table_level1 (level1_no)
  SELECT generate_series(1,200);
ANALYZE transition_table_level1;

INSERT INTO transition_table_level2 (level2_no, parent_no)
  SELECT level2_no, level2_no / 50 + 1 AS parent_no
    FROM generate_series(1,9999) level2_no;
ANALYZE transition_table_level2;

INSERT INTO transition_table_status (level, node_no, status)
  SELECT 1, level1_no, 0 FROM transition_table_level1;

INSERT INTO transition_table_status (level, node_no, status)
  SELECT 2, level2_no, 0 FROM transition_table_level2;
ANALYZE transition_table_status;

INSERT INTO transition_table_level1(level1_no)
  SELECT generate_series(201,1000);
ANALYZE transition_table_level1;

-- behave reasonably if someone tries to modify a transition table
CREATE FUNCTION transition_table_level2_bad_usage_func()
  RETURNS TRIGGER
  LANGUAGE plpgsql
AS $$
  BEGIN
    INSERT INTO dx VALUES (1000000, 1000000, 'x');
    RETURN NULL;
  END;
$$;

CREATE TRIGGER transition_table_level2_bad_usage_trigger
  AFTER DELETE ON transition_table_level2
  REFERENCING OLD TABLE AS dx
  FOR EACH STATEMENT EXECUTE PROCEDURE
    transition_table_level2_bad_usage_func();

DELETE FROM transition_table_level2
  WHERE level2_no BETWEEN 301 AND 305;

DROP TRIGGER transition_table_level2_bad_usage_trigger
  ON transition_table_level2;

-- attempt modifications which would break RI (should all fail)
DELETE FROM transition_table_level1
  WHERE level1_no = 25;

UPDATE transition_table_level1 SET level1_no = -1
  WHERE level1_no = 30;

INSERT INTO transition_table_level2 (level2_no, parent_no)
  VALUES (10000, 10000);

UPDATE transition_table_level2 SET parent_no = 2000
  WHERE level2_no = 40;

EXPLAIN (VERBOSE, COSTS OFF) DELETE FROM transition_table_level1
  WHERE level1_no BETWEEN 201 AND 1000;

DELETE FROM transition_table_level1
  WHERE level1_no BETWEEN 201 AND 1000;


/* 
 * privilege bug
*/
--
-- Test access privileges
--

-- Clean up in case a prior regression run failed

-- Suppress NOTICE messages when users/groups don't exist
SET client_min_messages TO 'warning';

DROP ROLE IF EXISTS regress_priv_group1;
DROP ROLE IF EXISTS regress_priv_group2;

DROP ROLE IF EXISTS regress_priv_user1;
DROP ROLE IF EXISTS regress_priv_user2;
DROP ROLE IF EXISTS regress_priv_user3;
DROP ROLE IF EXISTS regress_priv_user4;
DROP ROLE IF EXISTS regress_priv_user5;
DROP ROLE IF EXISTS regress_priv_user6;

SELECT lo_unlink(oid) FROM pg_largeobject_metadata WHERE oid >= 1000 AND oid < 3000 ORDER BY oid;

RESET client_min_messages;

-- test proper begins here

CREATE USER regress_priv_user1;
CREATE USER regress_priv_user2;
CREATE USER regress_priv_user3;
CREATE USER regress_priv_user4;
CREATE USER regress_priv_user5;
CREATE USER regress_priv_user5;	-- duplicate

CREATE GROUP regress_priv_group1;
CREATE GROUP regress_priv_group2 WITH USER regress_priv_user1, regress_priv_user2;

ALTER GROUP regress_priv_group1 ADD USER regress_priv_user4;

ALTER GROUP regress_priv_group2 ADD USER regress_priv_user2;	-- duplicate
ALTER GROUP regress_priv_group2 DROP USER regress_priv_user2;
GRANT regress_priv_group2 TO regress_priv_user4 WITH ADMIN OPTION;

-- test owner privileges

SET SESSION AUTHORIZATION regress_priv_user1;
SELECT session_user, current_user;

CREATE TABLE atest1 ( a int, b text );
SELECT * FROM atest1;
INSERT INTO atest1 VALUES (1, 'one');
DELETE FROM atest1;
UPDATE atest1 SET a = 1 WHERE b = 'blech';
TRUNCATE atest1;
BEGIN;
LOCK atest1 IN ACCESS EXCLUSIVE MODE;
COMMIT;

REVOKE ALL ON atest1 FROM PUBLIC;
SELECT * FROM atest1;

GRANT ALL ON atest1 TO regress_priv_user2;
GRANT SELECT ON atest1 TO regress_priv_user3, regress_priv_user4;
SELECT * FROM atest1;

CREATE TABLE atest2 (col1 varchar(10), col2 boolean);
GRANT SELECT ON atest2 TO regress_priv_user2;
GRANT UPDATE ON atest2 TO regress_priv_user3;
GRANT DELETE ON atest2 TO regress_priv_user3;
GRANT INSERT ON atest2 TO regress_priv_user4;
GRANT TRUNCATE ON atest2 TO regress_priv_user5;


SET SESSION AUTHORIZATION regress_priv_user2;
SELECT session_user, current_user;

-- try various combinations of queries on atest1 and atest2

SELECT * FROM atest1; -- ok
SELECT * FROM atest2; -- ok
INSERT INTO atest1 VALUES (2, 'two'); -- ok
INSERT INTO atest2 VALUES ('foo', true); -- fail
INSERT INTO atest1 SELECT 1, b FROM atest1; -- ok
UPDATE atest1 SET a = 1 WHERE a = 2; -- ok
UPDATE atest2 SET col2 = NOT col2; -- fail
SELECT * FROM atest1 FOR UPDATE; -- ok
SELECT * FROM atest2 FOR UPDATE; -- fail
DELETE FROM atest2; -- fail
TRUNCATE atest2; -- fail
BEGIN;
LOCK atest2 IN ACCESS EXCLUSIVE MODE; -- fail
COMMIT;
COPY atest2 FROM stdin; -- fail
GRANT ALL ON atest1 TO PUBLIC; -- fail

-- checks in subquery, both ok
SELECT * FROM atest1 WHERE ( b IN ( SELECT col1 FROM atest2 ) );
SELECT * FROM atest2 WHERE ( col1 IN ( SELECT b FROM atest1 ) );


SET SESSION AUTHORIZATION regress_priv_user3;
SELECT session_user, current_user;

SELECT * FROM atest1; -- ok
SELECT * FROM atest2; -- fail
INSERT INTO atest1 VALUES (2, 'two'); -- fail
INSERT INTO atest2 VALUES ('foo', true); -- fail
INSERT INTO atest1 SELECT 1, b FROM atest1; -- fail
UPDATE atest1 SET a = 1 WHERE a = 2; -- fail
EXPLAIN (VERBOSE, COSTS OFF) UPDATE atest2 SET col2 = NULL;
UPDATE atest2 SET col2 = NULL; -- ok
EXPLAIN (VERBOSE, COSTS OFF) Delete from atest2 where col2 = false;
Delete from atest2 where col2 = false;  -- ok
select * from atest2 order by col1;



reset client_min_messages;
reset polar_px_enable_insert_select;
reset polar_px_optimizer_enable_dml_constraints;
reset polar_px_enable_update;
reset polar_px_enable_delete;
reset polar_px_enable_insert_from_tableless;
reset polar_px_optimizer_trace_fallback;
