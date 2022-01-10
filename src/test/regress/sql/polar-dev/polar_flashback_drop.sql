SET polar_enable_flashback_drop=on;
SET client_min_messages = error;
--create table 
CREATE TABLE part(id INT PRIMARY KEY,name TEXT);

DO $$
BEGIN
FOR i IN 1..10 LOOP
    INSERT INTO part VALUES(i,'polar');
END LOOP;
END;$$;

CREATE TABLE employee(id INT PRIMARY KEY   NOT NULL,name TEXT);

DO $$
BEGIN
FOR i IN 11..20 LOOP
    INSERT INTO employee VALUES(i,'cc');
END LOOP;
END;$$;

--create new schema
CREATE SCHEMA schematest;
SET search_path TO schematest;

CREATE TABLE part_1(id INT PRIMARY KEY,name TEXT);

DO $$
BEGIN
FOR i IN 21..30 LOOP
    INSERT INTO part_1 VALUES(i,'postgre');
END LOOP;
END;$$;

CREATE TABLE employee_1(id INT PRIMARY KEY   NOT NULL,name TEXT);

DO $$
BEGIN
FOR i IN 31..40 LOOP
    INSERT INTO employee_1 VALUES(i,'lu');
END LOOP;
END;$$;

SET search_path TO public;
--drop table
DROP TABLE part;
DROP TABLE IF EXISTS employee;
DROP TABLE schematest.part_1;--drop table in schematest

DROP TABLE noexist;--drop no exist table

SELECT * FROM part;
SELECT * FROM employee;

--set into schema schema_test
SET search_path TO schematest;
DROP TABLE IF EXISTS employee_1;

SELECT * FROM part_1;
SELECT * FROM employee_1;


FLASHBACK TABLE part TO BEFORE DROP;--fail

FLASHBACK TABLE public.part TO BEFORE DROP;--success

--success
FLASHBACK TABLE part_1 TO BEFORE DROP;
FLASHBACK TABLE employee_1 TO BEFORE DROP RENAME TO employee_2;--rename

SELECT * FROM part;
SELECT * FROM part_1;
SELECT * FROM employee_2;

--drop purge
DROP TABLE employee_2 PURGE;
FLASHBACK TABLE employee_2 TO BEFORE DROP;--fail

--check data completeness
SELECT count(*) FROM part;
SELECT count(*) FROM part_1; --10

DROP TABLE part PURGE;
DROP TABLE part_1;

SET search_path TO public;

--drop recyclebin table belong to other schema
PURGE TABLE part_1; --fail

PURGE TABLE schematest.part_1,employee;--fail

PURGE TABLE schematest.part_1;

--drop recyclebin table directly
DO $$
DECLARE 
   table_nm varchar(100);
BEGIN 
    SELECT into table_nm ('drop table ' || 'recyclebin.'|| table_name) from information_schema.tables where table_schema='recyclebin';
    execute table_nm;
END;$$;

FLASHBACK TABLE employee TO BEFORE DROP;--fail

--clean up recyclebin
PURGE recyclebin;

--check recyclebin
SHOW recyclebin;

--
DROP TABLE recyclebin.test1;
FLASHBACK TABLE recyclebin.test2 TO BEFORE DROP;

--test flashback/purge order
CREATE TABLE order_test(i INT);
INSERT INTO order_test VALUES(2);
DROP TABLE order_test;
CREATE TABLE order_test(i INT);
INSERT INTO order_test VALUES(1);
DROP TABLE order_test;
CREATE TABLE order_test(i INT);
INSERT INTO order_test VALUES(3);
DROP TABLE order_test;

FLASHBACK TABLE order_test TO BEFORE DROP;
SELECT * FROM order_test;--3

FLASHBACK TABLE order_test TO BEFORE DROP RENAME TO order_test_1;
SELECT * FROM order_test_1;--1

FLASHBACK TABLE order_test TO BEFORE DROP RENAME TO order_test_2;
SELECT * FROM order_test_2;--2

DROP TABLE order_test,order_test_1,order_test_2 PURGE;

--purge order
CREATE TABLE order_test(i INT);
INSERT INTO order_test VALUES(2);
DROP TABLE order_test;
CREATE TABLE order_test(i INT);
INSERT INTO order_test VALUES(1);
DROP TABLE order_test;
CREATE TABLE order_test(i INT);
INSERT INTO order_test VALUES(3);
DROP TABLE order_test;

PURGE TABLE order_test;

FLASHBACK TABLE order_test TO BEFORE DROP;
SELECT * FROM order_test;--3

FLASHBACK TABLE order_test TO BEFORE DROP RENAME TO order_test_1;
SELECT * FROM order_test_1;--1

DROP TABLE order_test;

ALTER TABLE order_test_1 RENAME TO order_test;
DROP TABLE order_test;

PURGE TABLE order_test;
FLASHBACK TABLE order_test TO BEFORE DROP;
SELECT * FROM order_test;--1
DROP TABLE order_test PURGE;


--test depend table
CREATE TABLE a_f(a int);
CREATE TABLE b_s(b int) INHERITS (a_f);
DROP TABLE a_f;
DROP TABLE a_f CASCADE;

--test temp table
CREATE TEMP TABLE test(id INT,txt TEXT);
DROP TABLE test; 
SHOW recyclebin;

--test role authority
CREATE USER user1;
CREATE USER user2;

SET SESSION ROLE user1;
CREATE TABLE test(id INT,txt TEXT);
DROP TABLE test;

SET SESSION ROLE user2;
FLASHBACK TABLE test TO BEFORE DROP;

PURGE recyclebin;--fail Permission denied

SET SESSION ROLE postgres;
PURGE TABLE test;
PURGE recyclebin;

DROP ROLE user1;
DROP ROLE user2;

--test transaction
CREATE TABLE tran_test(id INT);
INSERT INTO tran_test VALUES(1);

BEGIN;
DROP TABLE tran_test;
ABORT;

SHOW recyclebin;
SELECT * FROM tran_test;
DROP TABLE tran_test PURGE;


--create partition table
CREATE TABLE orders (
    id serial,
    user_id int4,
    create_time timestamp(0) 
) PARTITION BY RANGE(create_time);

CREATE TABLE orders_history PARTITION OF orders FOR VALUES FROM ('2000-01-01') TO ('2020-03-01');
CREATE TABLE orders_202003 PARTITION OF orders FOR VALUES FROM ('2020-03-01') TO ('2020-04-01');
CREATE TABLE orders_202004 PARTITION OF orders FOR VALUES FROM ('2020-04-01') TO ('2020-05-01');
CREATE TABLE orders_202005 PARTITION OF orders FOR VALUES FROM ('2020-05-01') TO ('2020-06-01');
CREATE TABLE orders_202006 PARTITION OF orders FOR VALUES FROM ('2020-06-01') TO ('2020-07-01');
INSERT INTO orders (user_id, create_time) select 1000, generate_series('2020-01-01'::date, '2020-05-31'::date, '1 minute');

-- test partion table
SELECT count(*) FROM orders_202003;
DROP TABLE orders_202003;
FLASHBACK TABLE orders_202003 TO BEFORE DROP;--fail

--test father table
SELECT count(*) FROM orders;
DROP TABLE orders;
FLASHBACK TABLE orders TO BEFORE DROP;--fail

DROP TABLE test;--no table

SET polar_enable_flashback_drop = off;
SET client_min_messages = notice;

CREATE TABLE ll(i INT);

DROP TABLE ll PURGE;
FLASHBACK TABLE ll TO BEFORE DROP;

DROP TABLE ll;