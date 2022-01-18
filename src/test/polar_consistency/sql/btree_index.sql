--
-- BTREE_INDEX
-- test retrieval of min/max keys for each index
--

SELECT b.*
   FROM bt_i4_heap b
   WHERE b.seqno < 1;

SELECT b.*
   FROM bt_i4_heap b
   WHERE b.seqno >= 9999;

SELECT b.*
   FROM bt_i4_heap b
   WHERE b.seqno = 4500;

SELECT b.*
   FROM bt_name_heap b
   WHERE b.seqno < '1'::name;

SELECT b.*
   FROM bt_name_heap b
   WHERE b.seqno >= '9999'::name;

SELECT b.*
   FROM bt_name_heap b
   WHERE b.seqno = '4500'::name;

SELECT b.*
   FROM bt_txt_heap b
   WHERE b.seqno < '1'::text;

SELECT b.*
   FROM bt_txt_heap b
   WHERE b.seqno >= '9999'::text;

SELECT b.*
   FROM bt_txt_heap b
   WHERE b.seqno = '4500'::text;

SELECT b.*
   FROM bt_f8_heap b
   WHERE b.seqno < '1'::float8;

SELECT b.*
   FROM bt_f8_heap b
   WHERE b.seqno >= '9999'::float8;

SELECT b.*
   FROM bt_f8_heap b
   WHERE b.seqno = '4500'::float8;

--
-- Check correct optimization of LIKE (special index operator support)
-- for both indexscan and bitmapscan cases
--

set enable_seqscan to false;
set enable_indexscan to true;
set enable_bitmapscan to false;
select proname from pg_proc where proname like E'RI\\_FKey%del' order by 1;

set enable_indexscan to false;
set enable_bitmapscan to true;
select proname from pg_proc where proname like E'RI\\_FKey%del' order by 1;


--
-- Test B-tree page deletion. In particular, deleting a non-leaf page.
--

-- First create a tree that's at least four levels deep. The text inserted
-- is long and poorly compressible. That way only a few index tuples fit on
-- each page, allowing us to get a tall tree with fewer pages.
create table btree_tall_tbl(id int4, t text);
create index btree_tall_idx on btree_tall_tbl (id, t) with (fillfactor = 10);
insert into btree_tall_tbl
  select g, g::text || '_' ||
          (select string_agg(md5(i::text), '_') from generate_series(1, 50) i)
from generate_series(1, 100) g;

set enable_seqscan to false;
set enable_indexscan to true;
set enable_bitmapscan to false;
-- explain (costs off)
select count(*) from btree_tall_tbl;
select count(*) from btree_tall_tbl;
set enable_seqscan to false;
set enable_indexscan to false;
set enable_bitmapscan to true;
-- explain (costs off)
select count(*) from btree_tall_tbl;
select count(*) from btree_tall_tbl;

-- Delete most entries, and vacuum. This causes page deletions.
delete from btree_tall_tbl where id < 950;

set enable_seqscan to false;
set enable_indexscan to true;
set enable_bitmapscan to false;
-- explain (costs off)
select count(*) from btree_tall_tbl;
select count(*) from btree_tall_tbl;
set enable_seqscan to false;
set enable_indexscan to false;
set enable_bitmapscan to true;
-- explain (costs off)
select count(*) from btree_tall_tbl;
select count(*) from btree_tall_tbl;

vacuum btree_tall_tbl;

set enable_seqscan to false;
set enable_indexscan to true;
set enable_bitmapscan to false;
-- explain (costs off)
select count(*) from btree_tall_tbl;
select count(*) from btree_tall_tbl;
set enable_seqscan to false;
set enable_indexscan to false;
set enable_bitmapscan to true;
-- explain (costs off)
select count(*) from btree_tall_tbl;
select count(*) from btree_tall_tbl;

--
-- Test B-tree insertion with a metapage update (XLOG_BTREE_INSERT_META
-- WAL record type). This happens when a "fast root" page is split.
--

-- The vacuum above should've turned the leaf page into a fast root. We just
-- need to insert some rows to cause the fast root page to split.
insert into btree_tall_tbl (id, t)
  select g, repeat('x', 100) from generate_series(1, 500) g;

set enable_seqscan to false;
set enable_indexscan to true;
set enable_bitmapscan to false;
-- explain (costs off)
select count(*) from btree_tall_tbl;
select count(*) from btree_tall_tbl;
set enable_seqscan to false;
set enable_indexscan to false;
set enable_bitmapscan to true;
-- explain (costs off)
select count(*) from btree_tall_tbl;
select count(*) from btree_tall_tbl;

--
-- Test vacuum_cleanup_index_scale_factor
--

-- Simple create
create table btree_test(a int);
create index btree_idx1 on btree_test(a) with (vacuum_cleanup_index_scale_factor = 40.0);
select reloptions from pg_class WHERE oid = 'btree_idx1'::regclass;

-- Fail while setting improper values
create index btree_idx_err on btree_test(a) with (vacuum_cleanup_index_scale_factor = -10.0);
create index btree_idx_err on btree_test(a) with (vacuum_cleanup_index_scale_factor = 100.0);
create index btree_idx_err on btree_test(a) with (vacuum_cleanup_index_scale_factor = 'string');
create index btree_idx_err on btree_test(a) with (vacuum_cleanup_index_scale_factor = true);

-- Simple ALTER INDEX
alter index btree_idx1 set (vacuum_cleanup_index_scale_factor = 70.0);
select reloptions from pg_class WHERE oid = 'btree_idx1'::regclass;
