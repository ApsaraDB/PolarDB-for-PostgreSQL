CREATE EXTENSION IF NOT EXISTS test_flashback_table;

-- test fast recovery area interface
SELECT test_fast_recovery_area();

-- setup
CREATE ROLE fb_test with login;
DROP TABLE IF EXISTS old_rel;
DROP TABLE IF EXISTS expected_rel;
CREATE TABLE old_rel(id int PRIMARY KEY, first_name varchar(6), last_name varchar(6));
SELECT clean_fb_rel('old_rel');

-- delete and flashback table
-- let flashback logindex has something not inserted
ALTER SYSTEM SET polar_flashback_log_insert_list_delay = 10000;
SELECT * FROM pg_reload_conf();
INSERT INTO old_rel SELECT *, fb_random_string(6), fb_random_string(6) FROM generate_series(1,10000);
CHECKPOINT;
CREATE TABLE expected_rel as SELECT * FROM old_rel;
SELECT pg_sleep(10);
DELETE FROM old_rel;
FLASHBACK TABLE old_rel to timestamp now() - interval '10s';
-- check the data and clean
SELECT fb_check_data('old_rel', 'expected_rel');
SELECT clean_fb_rel('old_rel');
DROP TABLE expected_rel;
ALTER SYSTEM RESET polar_flashback_log_insert_list_delay;
SELECT * FROM pg_reload_conf();

-- insert and flashback table
SET polar_workers_per_flashback_table = 0;
CREATE TABLE expected_rel as SELECT * FROM old_rel;
SELECT pg_sleep(10);
INSERT INTO old_rel SELECT *, fb_random_string(6), fb_random_string(6) FROM generate_series(1,10000);
FLASHBACK TABLE old_rel to timestamp now() - interval '10s';
-- check the data and clean
SELECT fb_check_data('old_rel', 'expected_rel');
SELECT clean_fb_rel('old_rel');
DROP TABLE expected_rel;
RESET polar_workers_per_flashback_table;

-- update and flashback table
CREATE TABLE expected_rel as SELECT * FROM old_rel;
SELECT pg_sleep(10);
UPDATE old_rel set first_name='fbtest';
FLASHBACK TABLE old_rel to timestamp now() - interval '10s';
-- check the data and clean
SELECT fb_check_data('old_rel', 'expected_rel');
SELECT clean_fb_rel('old_rel');
DROP TABLE expected_rel;

-- alter add check and flashback table
CREATE TABLE expected_rel as SELECT * FROM old_rel;
SELECT pg_sleep(10);
DELETE from old_rel where id < 3;
ALTER TABLE old_rel ADD CHECK (id >= 3);
FLASHBACK TABLE old_rel to timestamp now() - interval '10s';
-- check the data and clean
SELECT fb_check_data('old_rel', 'expected_rel');
SELECT clean_fb_rel('old_rel');
DROP TABLE expected_rel;

-- alter drop column and flashback table without rewrite table
CREATE TABLE expected_rel(id int, last_name varchar(6));
INSERT INTO expected_rel SELECT id, last_name FROM old_rel;
SELECT pg_sleep(10);
ALTER TABLE old_rel DROP COLUMN first_name;
DELETE FROM old_rel;
FLASHBACK TABLE old_rel to timestamp now() - interval '10s';
-- check the data and clean
SELECT fb_check_data('old_rel', 'expected_rel');
SELECT clean_fb_rel('old_rel');
DROP TABLE expected_rel;

-- alter add column and flashback table without rewrite table
INSERT INTO old_rel SELECT *, fb_random_string(6) FROM generate_series(3,10000);
CREATE TABLE expected_rel as SELECT * FROM old_rel;
SELECT pg_sleep(10);
ALTER TABLE old_rel ADD COLUMN first_name varchar(6);
UPDATE old_rel SET first_name='test';
FLASHBACK TABLE old_rel to timestamp now() - interval '10s';
-- check the data and clean
CREATE TABLE expected_fb_rel (LIKE old_rel);
INSERT INTO  expected_fb_rel select id, last_name from expected_rel;
SELECT fb_check_data('old_rel', 'expected_fb_rel');
SELECT clean_fb_rel('old_rel');
DROP TABLE expected_rel;
DROP TABLE expected_fb_rel;

-- alter column type and flashback table without rewrite table
CREATE DOMAIN int32 AS int;
CREATE TABLE expected_rel as SELECT * FROM old_rel;
SELECT pg_sleep(10);
ALTER TABLE old_rel ALTER id TYPE int32;
FLASHBACK TABLE old_rel to timestamp now() - interval '10s';
-- check the data and clean
SELECT fb_check_data('old_rel', 'expected_rel');
SELECT clean_fb_rel('old_rel');
DROP TABLE expected_rel;

-- flashback table out of rotation
FLASHBACK TABLE old_rel to timestamp '1990-01-01 00:00:00';
FLASHBACK TABLE old_rel to timestamp '2999-01-01 00:00:00';

--flashback table with a role which is not its owner
SET SESSION AUTHORIZATION fb_test;
FLASHBACK TABLE old_rel to timestamp now() - interval '10s';
RESET SESSION AUTHORIZATION;

-- flashback index 
CREATE INDEX id_int_index on old_rel(id);
FLASHBACK TABLE id_int_index to timestamp now() - interval '10s';

-- flashback table has a toast table
CREATE TABLE fb_toast(id int, name text);
FLASHBACK TABLE fb_toast to timestamp now() - interval '10s';
DROP TABLE fb_toast;

-- flashback partitioned and partition table
CREATE TABLE fb_partitioned (
    id int,
    create_time timestamp(0) 
) PARTITION BY RANGE(create_time);
CREATE TABLE fb_partition_202003 PARTITION OF fb_partitioned FOR VALUES FROM ('2020-03-01') TO ('2020-04-01');
FLASHBACK TABLE fb_partitioned to timestamp now() - interval '10s';
FLASHBACK TABLE fb_partition_202003 to timestamp now() - interval '10s';
DROP TABLE fb_partitioned;

-- flashback partitioned and partition table
FLASHBACK TABLE pg_class to timestamp now() - interval '10s';

-- flashback view and materialized view
CREATE VIEW fb_view AS SELECT * FROM old_rel;
FLASHBACK TABLE fb_view to timestamp now() - interval '10s';
DROP VIEW fb_view;
CREATE MATERIALIZED VIEW fb_materialized_view AS SELECT * FROM old_rel;
FLASHBACK TABLE fb_materialized_view to timestamp now() - interval '10s';
DROP MATERIALIZED VIEW fb_materialized_view;

-- alter table with oids and flashback table with rewrite table
SELECT pg_sleep(10);
ALTER TABLE old_rel SET WITH OIDS;
FLASHBACK TABLE old_rel to timestamp now() - interval '10s';

-- alter table without oids and flashback table with rewrite table
SELECT pg_sleep(10);
ALTER TABLE old_rel SET WITHOUT OIDS;
FLASHBACK TABLE old_rel to timestamp now() - interval '10s';

-- alter add column with identity and flashback table with rewrite table
SELECT pg_sleep(10);
ALTER TABLE old_rel ADD COLUMN test_identity int GENERATED ALWAYS AS IDENTITY;
FLASHBACK TABLE old_rel to timestamp now() - interval '10s';

-- alter add column type with check and flashback table with rewrite table
SELECT pg_sleep(10);
CREATE DOMAIN age AS int CHECK(VALUE >=0  AND VALUE <= 200); 
ALTER TABLE old_rel ADD COLUMN man_age age;
FLASHBACK TABLE old_rel to timestamp now() - interval '10s';

-- alter add column with default contain volatile function and flashback table with rewrite table
SELECT pg_sleep(10);
ALTER TABLE old_rel ADD COLUMN user_id int DEFAULT random()*10000;
FLASHBACK TABLE old_rel to timestamp now() - interval '10s';

-- alter table unlooged and flashback table with rewrite table
SELECT pg_sleep(10);
ALTER TABLE old_rel SET UNLOGGED;
FLASHBACK TABLE old_rel to timestamp now() - interval '10s';

-- alter table logged and flashback table with rewrite table
SELECT pg_sleep(10);
ALTER TABLE old_rel SET LOGGED;
FLASHBACK TABLE old_rel to timestamp now() - interval '10s';

-- vacuum full table and flashback table with rewrite table
SELECT pg_sleep(10);
VACUUM FULL old_rel;
FLASHBACK TABLE old_rel to timestamp now() - interval '10s';

-- truncate table and flashback table with rewrite table
SELECT pg_sleep(10);
TRUNCATE TABLE old_rel;
FLASHBACK TABLE old_rel to timestamp now() - interval '10s';

-- alter table add a foregin key and flashback table
INSERT INTO old_rel SELECT *, fb_random_string(6), fb_random_string(6) FROM generate_series(3,10000);
CREATE TABLE expected_rel as SELECT * FROM old_rel;
SELECT pg_sleep(10);
CREATE TABLE fb_foreign_key(nick_name varchar(6) PRIMARY KEY);
DELETE FROM old_rel;
ALTER TABLE old_rel ADD CONSTRAINT fb_foreign FOREIGN KEY (first_name) REFERENCES fb_foreign_key (nick_name);
FLASHBACK TABLE old_rel to timestamp now() - interval '10s';
-- check the data and clean
SELECT fb_check_data('old_rel', 'expected_rel');
SELECT clean_fb_rel('old_rel');
DROP TABLE expected_rel;

-- clean up
DROP TABLE IF EXISTS old_rel;
DROP TABLE IF EXISTS fb_foreign_key;
DROP DOMAIN int32;
DROP DOMAIN age;
DROP EXTENSION test_flashback_table;
