-- start_matchsubs
-- m/NOTICE:  One or more columns in the following table\(s\) do not have statistics: /
-- s/.//gs
-- m/HINT:  For non-partitioned tables, run analyze .+\. For partitioned tables, run analyze rootpartition .+\. See log for columns missing statistics\./
-- s/.//gs
-- end_matchsubs
-- partition_list_index.sql

-- Test the 'px_build' state of CREATE INDEX with PX.
/*--EXPLAIN_QUERY_BEGIN*/
ALTER SYSTEM SET polar_px_enable_btbuild = ON;
SELECT pg_reload_conf();
SELECT pg_sleep(1);
SHOW polar_px_enable_btbuild;

DROP TABLE IF EXISTS mpp3033a;
CREATE TABLE mpp3033a (
        unique1         int4,
        unique2         int4
) PARTITION BY list (unique1);

CREATE TABLE mpp3033a_1 PARTITION OF mpp3033a FOR VALUES IN (1,2,3,4,5,6,7,8,9,10);
CREATE TABLE mpp3033a_2 PARTITION OF mpp3033a FOR VALUES IN (11,12,13,14,15,16,17,18,19,20);
CREATE TABLE mpp3033a_3 PARTITION OF mpp3033a DEFAULT;

CREATE INDEX mpp3033a_unique1 ON mpp3033a(unique1) WITH (px_build=on);

SELECT relname, reloptions FROM pg_class
        WHERE relname='mpp3033a_unique1'
        AND '{px_build=finish}' @> reloptions;
SELECT relname, reloptions FROM pg_class
        WHERE relname='mpp3033a_1_unique1_idx'
        AND '{px_build=finish}' @> reloptions;
SELECT relname, reloptions FROM pg_class
        WHERE relname='mpp3033a_2_unique1_idx'
        AND '{px_build=finish}' @> reloptions;
SELECT relname, reloptions FROM pg_class
        WHERE relname='mpp3033a_3_unique1_idx'
        AND '{px_build=finish}' @> reloptions;

DROP INDEX mpp3033a_unique1;

-- Test partition with CREATE INDEX
DROP TABLE if exists mpp3033a;

CREATE TABLE mpp3033a (
        unique1         int4,
        unique2         int4,
        two                     int4,
        four            int4,
        ten                     int4,
        twenty          int4,
        hundred         int4,
        thousand        int4,
        twothousand     int4,
        fivethous       int4,
        tenthous        int4,
        odd                     int4,
        even            int4,
        stringu1        name,
        stringu2        name,
        string4         name
)  partition by list (unique1);

CREATE TABLE mpp3033a_1 PARTITION OF mpp3033a FOR VALUES IN (1,2,3,4,5,6,7,8,9,10);
CREATE TABLE mpp3033a_2 PARTITION OF mpp3033a FOR VALUES IN (11,12,13,14,15,16,17,18,19,20);
CREATE TABLE mpp3033a_3 PARTITION OF mpp3033a DEFAULT;

\copy mpp3033a from 'data/onek.data';

CREATE INDEX mpp3033a_unique1 ON mpp3033a USING btree(unique1 int4_ops);
CREATE INDEX mpp3033a_unique2 ON mpp3033a USING btree(unique2 int4_ops);
CREATE INDEX mpp3033a_hundred ON mpp3033a USING btree(hundred int4_ops);
CREATE INDEX mpp3033a_stringu1 ON mpp3033a USING btree(stringu1 name_ops);



select count(*) from mpp3033a;

reindex index mpp3033a_unique1;
reindex index mpp3033a_unique2;
reindex index mpp3033a_hundred;
reindex index mpp3033a_stringu1;


select count(*) from mpp3033a;

drop index mpp3033a_unique1;
drop index mpp3033a_unique2;
drop index mpp3033a_hundred;
drop index mpp3033a_stringu1;



CREATE INDEX mpp3033a_unique1 ON mpp3033a (unique1);
CREATE INDEX mpp3033a_unique2 ON mpp3033a (unique2);
CREATE INDEX mpp3033a_hundred ON mpp3033a (hundred);
CREATE INDEX mpp3033a_stringu1 ON mpp3033a (stringu1);



select count(*) from mpp3033a;

reindex index mpp3033a_unique1;
reindex index mpp3033a_unique2;
reindex index mpp3033a_hundred;
reindex index mpp3033a_stringu1;


select count(*) from mpp3033a;

drop index mpp3033a_unique1;
drop index mpp3033a_unique2;
drop index mpp3033a_hundred;
drop index mpp3033a_stringu1;


CREATE UNIQUE INDEX mpp3033a_unique1 ON mpp3033a (unique1);
CREATE UNIQUE INDEX mpp3033a_unique2 ON mpp3033a (unique2);
CREATE UNIQUE INDEX mpp3033a_hundred ON mpp3033a (hundred);
CREATE UNIQUE INDEX mpp3033a_stringu1 ON mpp3033a (stringu1);



select count(*) from mpp3033a;

reindex index mpp3033a_unique1;
reindex index mpp3033a_unique2;
reindex index mpp3033a_hundred;
reindex index mpp3033a_stringu1;


drop index mpp3033a_unique1;
drop index mpp3033a_unique2;
drop index mpp3033a_hundred;
drop index mpp3033a_stringu1;

CREATE INDEX mpp3033a_unique1 ON mpp3033a USING bitmap (unique1);
CREATE INDEX mpp3033a_unique2 ON mpp3033a USING bitmap (unique2);
CREATE INDEX mpp3033a_hundred ON mpp3033a USING bitmap (hundred);
CREATE INDEX mpp3033a_stringu1 ON mpp3033a USING bitmap (stringu1);


select count(*) from mpp3033a;

reindex index mpp3033a_unique1;
reindex index mpp3033a_unique2;
reindex index mpp3033a_hundred;
reindex index mpp3033a_stringu1;


select count(*) from mpp3033a;

-- partition_range_index.sql
-- Test partition with CREATE INDEX
DROP TABLE if exists mpp3033a;

CREATE TABLE mpp3033a (
        unique1         int4,
        unique2         int4,
        two                     int4,
        four            int4,
        ten                     int4,
        twenty          int4,
        hundred         int4,
        thousand        int4,
        twothousand     int4,
        fivethous       int4,
        tenthous        int4,
        odd                     int4,
        even            int4,
        stringu1        name,
        stringu2        name,
        string4         name
)  partition by range (unique1);
CREATE TABLE mpp3033a_1 PARTITION of mpp3033a for values from (0) to (100);
CREATE TABLE mpp3033a_2 PARTITION of mpp3033a for values from (100) to (200);
CREATE TABLE mpp3033a_3 PARTITION of mpp3033a for values from (200) to (300);
CREATE TABLE mpp3033a_4 PARTITION of mpp3033a for values from (300) to (400);
CREATE TABLE mpp3033a_5 PARTITION of mpp3033a for values from (400) to (500);
CREATE TABLE mpp3033a_6 PARTITION of mpp3033a for values from (500) to (600);
CREATE TABLE mpp3033a_7 PARTITION of mpp3033a for values from (600) to (700);
CREATE TABLE mpp3033a_8 PARTITION of mpp3033a for values from (700) to (800);
CREATE TABLE mpp3033a_9 PARTITION of mpp3033a for values from (800) to (900);
CREATE TABLE mpp3033a_10 PARTITION of mpp3033a for values from (900) to (1000);
CREATE TABLE mpp3033a_11 PARTITION of mpp3033a default;


\copy mpp3033a from 'data/onek.data';

drop index if exists mpp3033a_unique1;
drop index if exists mpp3033a_unique2;
drop index if exists mpp3033a_hundred;
drop index if exists mpp3033a_stringu1;






CREATE INDEX mpp3033a_unique1 ON mpp3033a USING btree(unique1 int4_ops);
CREATE INDEX mpp3033a_unique2 ON mpp3033a USING btree(unique2 int4_ops);
CREATE INDEX mpp3033a_hundred ON mpp3033a USING btree(hundred int4_ops);
CREATE INDEX mpp3033a_stringu1 ON mpp3033a USING btree(stringu1 name_ops);







select count(*) from mpp3033a;

reindex index mpp3033a_unique1;
reindex index mpp3033a_unique2;
reindex index mpp3033a_hundred;
reindex index mpp3033a_stringu1;







select count(*) from mpp3033a;

drop index mpp3033a_unique1;
drop index mpp3033a_unique2;
drop index mpp3033a_hundred;
drop index mpp3033a_stringu1;







CREATE INDEX mpp3033a_unique1 ON mpp3033a (unique1);
CREATE INDEX mpp3033a_unique2 ON mpp3033a (unique2);
CREATE INDEX mpp3033a_hundred ON mpp3033a (hundred);
CREATE INDEX mpp3033a_stringu1 ON mpp3033a (stringu1);







select count(*) from mpp3033a;


reindex index mpp3033a_unique1;
reindex index mpp3033a_unique2;
reindex index mpp3033a_hundred;
reindex index mpp3033a_stringu1;







select count(*) from mpp3033a;


drop index mpp3033a_unique1;
drop index mpp3033a_unique2;
drop index mpp3033a_hundred;
drop index mpp3033a_stringu1;






CREATE UNIQUE INDEX mpp3033a_unique1 ON mpp3033a (unique1);
CREATE UNIQUE INDEX mpp3033a_unique2 ON mpp3033a (unique2);
CREATE UNIQUE INDEX mpp3033a_hundred ON mpp3033a (hundred);
CREATE UNIQUE INDEX mpp3033a_stringu1 ON mpp3033a (stringu1);







select count(*) from mpp3033a;


reindex index mpp3033a_unique1;
reindex index mpp3033a_unique2;
reindex index mpp3033a_hundred;
reindex index mpp3033a_stringu1;







select count(*) from mpp3033a;


drop index mpp3033a_unique1;
drop index mpp3033a_unique2;
drop index mpp3033a_hundred;
drop index mpp3033a_stringu1;






CREATE INDEX mpp3033a_unique1 ON mpp3033a USING bitmap (unique1);
CREATE INDEX mpp3033a_unique2 ON mpp3033a USING bitmap (unique2);
CREATE INDEX mpp3033a_hundred ON mpp3033a USING bitmap (hundred);
CREATE INDEX mpp3033a_stringu1 ON mpp3033a USING bitmap (stringu1);







select count(*) from mpp3033a;


reindex index mpp3033a_unique1;
reindex index mpp3033a_unique2;
reindex index mpp3033a_hundred;
reindex index mpp3033a_stringu1;







select count(*) from mpp3033a;

