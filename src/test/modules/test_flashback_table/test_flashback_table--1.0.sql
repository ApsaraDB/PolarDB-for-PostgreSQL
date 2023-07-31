/* src/test/modules/test_flashback_table/test_flashback_table--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_flashback_table" to load this file. \quit

CREATE FUNCTION test_fast_recovery_area()
RETURNS pg_catalog.void STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE OR REPLACE FUNCTION fb_random_string(
  num INTEGER,
  chars TEXT default 'abcdefghijklmnopqrstuvwxyz'
) RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
  res_str TEXT := '';
BEGIN
  IF num < 1 THEN
      RAISE EXCEPTION 'Invalid length';
  END IF;
  FOR __ IN 1..num LOOP
    res_str := res_str || substr(chars, floor(random() * length(chars))::int + 1, 1);
  END LOOP;
  RETURN res_str;
END $$;

CREATE OR REPLACE FUNCTION fb_check_data(
  old_relname TEXT,
  expected_relname TEXT
) RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
  flashback_relname TEXT;
  old_relid OID;
  count_rel_expected INTEGER;
  count_rel_flashback INTEGER;
BEGIN
  execute format('select oid from pg_class where relname=''%I''', old_relname) into old_relid;
  flashback_relname := 'polar_flashback_' || old_relid;
  execute 'select count(*) from ' || expected_relname into count_rel_expected; 
  execute 'select count(*) from ' || flashback_relname || ',' || expected_relname || ' where md5(CAST((' || flashback_relname || '.*)AS text)) = md5(CAST((' || expected_relname || '.*)AS text))' into count_rel_flashback;

  IF count_rel_expected = count_rel_flashback THEN
  	  RETURN TRUE;
  ELSE
      RETURN FALSE;
  END IF;
END $$;

CREATE OR REPLACE FUNCTION clean_fb_rel(
  old_relname TEXT
) RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
  flashback_relname TEXT;
  old_relid OID;
BEGIN
  execute format('select oid from pg_class where relname=''%I''', old_relname) into old_relid;
  flashback_relname := 'polar_flashback_' || old_relid;
  execute 'DROP TABLE IF EXISTS ' || flashback_relname;
  RETURN TRUE;
END $$;