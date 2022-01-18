/*--EXPLAIN_QUERY_BEGIN*/
drop table t1 cascade;
CREATE TABLE t1 (c1 int, c2 character varying(100));
insert into t1 select generate_series(1,100), repeat('hello',20);
update t1 set c2 = 'world' where c1 < 10;
update t1 set c2 = null where c1 < 5;

drop table t2 cascade;
CREATE TABLE t2 (c1 int, c2 character varying(100));
insert into t2 select * from t1;

set polar_px_optimizer_enable_crossproduct=1;

SELECT    c1, c2
FROM t2 AS OUTR
WHERE NOT EXISTS
    (SELECT INNR.c1 AS Y
    FROM t1 AS INNR
    WHERE OUTR.c2 <= 'wor' ) AND OUTR.c1<=39 and OUTR.c2 is null;


SELECT    c1, c2
FROM t2 AS OUTR
WHERE  EXISTS
    (SELECT INNR.c1 AS Y
    FROM t1 AS INNR
    WHERE OUTR.c2 <= 'wor' ) AND OUTR.c1<=39 and OUTR.c2 is null;


SELECT    c1, c2
FROM t2 AS OUTR
WHERE NOT EXISTS
    (SELECT INNR.c1 AS Y
    FROM t1 AS INNR) AND OUTR.c1<=39 and OUTR.c2 is null;

SELECT    c1, c2
FROM t2 AS OUTR
WHERE  EXISTS
    (SELECT INNR.c1 AS Y
    FROM t1 AS INNR) AND OUTR.c1<=39 and OUTR.c2 is null;



set polar_px_optimizer_enable_crossproduct=0;
SELECT    c1, c2
FROM t2 AS OUTR
WHERE NOT EXISTS
    (SELECT INNR.c1 AS Y
    FROM t1 AS INNR
    WHERE OUTR.c2 <= 'wor' ) AND OUTR.c1<=39 and OUTR.c2 is null;

SELECT    c1, c2
FROM t2 AS OUTR
WHERE  EXISTS
    (SELECT INNR.c1 AS Y
    FROM t1 AS INNR
    WHERE OUTR.c2 <= 'wor' ) AND OUTR.c1<=39 and OUTR.c2 is null;


SELECT    c1, c2
FROM t2 AS OUTR
WHERE NOT EXISTS
    (SELECT INNR.c1 AS Y
    FROM t1 AS INNR) AND OUTR.c1<=39 and OUTR.c2 is null;

SELECT    c1, c2
FROM t2 AS OUTR
WHERE  EXISTS
    (SELECT INNR.c1 AS Y
    FROM t1 AS INNR) AND OUTR.c1<=39 and OUTR.c2 is null;



set polar_px_optimizer_enable_lasj_notin=0;
SELECT    c1, c2
FROM t2 AS OUTR
WHERE OUTR.c1 NOT IN
    (SELECT INNR.c1 AS Y
    FROM t1 AS INNR) AND OUTR.c1<=39 and OUTR.c2 is null;

SELECT    c1, c2
FROM t2 AS OUTR
WHERE OUTR.c1  IN
    (SELECT INNR.c1 AS Y
    FROM t1 AS INNR) AND OUTR.c1<=39 and OUTR.c2 is null;

SELECT    c1, c2
FROM t2 AS OUTR
WHERE OUTR.c1 NOT IN
    (SELECT INNR.c1 AS Y
    FROM t1 AS INNR
    WHERE OUTR.c2 <= 'wor' ) AND OUTR.c1<=39 and OUTR.c2 is null;

SELECT    c1, c2
FROM t2 AS OUTR
WHERE OUTR.c1  IN
    (SELECT INNR.c1 AS Y
    FROM t1 AS INNR 
    WHERE OUTR.c2 <= 'wor' ) AND OUTR.c1<=39 and OUTR.c2 is null;


set polar_px_optimizer_enable_lasj_notin=1;
SELECT    c1, c2
FROM t2 AS OUTR
WHERE OUTR.c1 NOT IN
    (SELECT INNR.c1 AS Y
    FROM t1 AS INNR) AND OUTR.c1<=39 and OUTR.c2 is null;

SELECT    c1, c2
FROM t2 AS OUTR
WHERE OUTR.c1  IN
    (SELECT INNR.c1 AS Y
    FROM t1 AS INNR) AND OUTR.c1<=39 and OUTR.c2 is null;

SELECT    c1, c2
FROM t2 AS OUTR
WHERE OUTR.c1 NOT IN
    (SELECT INNR.c1 AS Y
    FROM t1 AS INNR
    WHERE OUTR.c2 <= 'wor' ) AND OUTR.c1<=39 and OUTR.c2 is null;

SELECT    c1, c2
FROM t2 AS OUTR
WHERE OUTR.c1  IN
    (SELECT INNR.c1 AS Y
    FROM t1 AS INNR 
    WHERE OUTR.c2 <= 'wor' ) AND OUTR.c1<=39 and OUTR.c2 is null;