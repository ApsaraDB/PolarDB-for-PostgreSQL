/*
 * external/polar_advisor/polar_advisor--1.0.sql
 * Define the tables, functions for maintenance window. Support vacuum analyze in this version.
 * The objects exist in some old instances because we created them manually and we have to check
 * whether they exist and add them to the extension if needed.
 */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION polar_advisor" to load this file. \quit

--
-- Schema
--
-- we cannot use CREATE SCHEMA IF NOT EXISTS because it results in
-- ERROR:  schema polar_advisor is not a member of extension "polar_advisor"
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n WHERE n.nspname = 'polar_advisor') THEN
        CREATE SCHEMA polar_advisor;
    END IF;
END
$$ LANGUAGE PLpgSQL;

--
-- Type
--
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE t.typname = 'action_type' AND n.nspname = 'polar_advisor'
    ) THEN
        CREATE TYPE polar_advisor.action_type AS ENUM ('VACUUM ANALYZE', 'VACUUM', 'ANALYZE', 'REINDEX', 'REPACK');
    END IF;
END
$$ LANGUAGE PLpgSQL;

ALTER TYPE polar_advisor.action_type ADD VALUE IF NOT EXISTS 'VACUUM ANALYZE';
ALTER TYPE polar_advisor.action_type ADD VALUE IF NOT EXISTS 'VACUUM';
ALTER TYPE polar_advisor.action_type ADD VALUE IF NOT EXISTS 'ANALYZE';
ALTER TYPE polar_advisor.action_type ADD VALUE IF NOT EXISTS 'REINDEX';
ALTER TYPE polar_advisor.action_type ADD VALUE IF NOT EXISTS 'REPACK';

--
-- Sequence
--
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_class WHERE relname = 'advisor_exec_id_seq' AND relkind = 'S'
            AND relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'polar_advisor')
    ) THEN
        -- super_pg uses this sequence to identify each round work.
        CREATE SEQUENCE polar_advisor.advisor_exec_id_seq;
    END IF;
END
$$ LANGUAGE PLpgSQL;

--
-- Window info, valid in db postgres only.
--
-- The advisor window information, super_pg client read the information from db postgres.
-- Never change the table name since UE relies on it.
-- No new column should be added, all the new attribute should be added to `others`.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_class WHERE relname = 'advisor_window' AND relkind = 'r'
            AND relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'polar_advisor')
    ) THEN
        CREATE TABLE polar_advisor.advisor_window (
            start_time          TIME WITH TIME ZONE NOT NULL,
            end_time            TIME WITH TIME ZONE NOT NULL,
            enabled             BOOLEAN NOT NULL DEFAULT FALSE,
            last_error_time     TIMESTAMP WITH TIME ZONE,
            last_error_detail   TEXT,
            others              JSONB
        );
    END IF;
END
$$ LANGUAGE PLpgSQL;
ALTER TABLE polar_advisor.advisor_window ADD COLUMN IF NOT EXISTS others JSONB;

-- Initialize the window, disabled by default.
-- The window (02:00:00+08, 03:00:00+08) is the same as the default window in PolarDB console.
-- super_pg client cannot update the last_error_detail if there's no row in the table.
-- If the user want to set a self defined window info, just update `start_time`, `end_time` and `enabled`.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM polar_advisor.advisor_window) THEN
        INSERT INTO polar_advisor.advisor_window (start_time, end_time, enabled) VALUES ('02:00:00+08', '03:00:00+08', FALSE);
    ELSE
        RAISE WARNING 'The table polar_advisor.advisor_window already has data';
    END IF;
END
$$ LANGUAGE PLpgSQL;

-- Function to enable advisor window
-- We cannot use CREATE OR REPLACE FUNCTION because it results in
-- ERROR:  function polar_advisor.enable_advisor_window() is not a member of extension "polar_advisor"
-- so we have to drop it at first.
DROP FUNCTION IF EXISTS polar_advisor.enable_advisor_window;
CREATE OR REPLACE FUNCTION polar_advisor.enable_advisor_window ()
RETURNS VOID AS $$
DECLARE
    rows_updated INT;
BEGIN
    -- set enabled info
    UPDATE polar_advisor.advisor_window SET enabled = true;
    GET DIAGNOSTICS rows_updated = ROW_COUNT;
    IF rows_updated = 0 THEN
        RAISE EXCEPTION 'there is no window info, please set it by polar_advisor.set_advisor_window(start_time, end_time) at first';
    END IF;
END;
$$ LANGUAGE PLpgSQL;

-- Function to disable advisor window
DROP FUNCTION IF EXISTS polar_advisor.disable_advisor_window;
CREATE OR REPLACE FUNCTION polar_advisor.disable_advisor_window ()
RETURNS VOID AS $$
DECLARE
    rows_updated INT;
BEGIN
    -- set enabled info
    UPDATE polar_advisor.advisor_window SET enabled = false;
    GET DIAGNOSTICS rows_updated = ROW_COUNT;
    IF rows_updated = 0 THEN
        RAISE EXCEPTION 'there is no window info, please set it by polar_advisor.set_advisor_window(start_time, end_time) at first';
    END IF;
END;
$$ LANGUAGE PLpgSQL;

-- Function to set advisor window
DROP FUNCTION IF EXISTS polar_advisor.set_advisor_window;
CREATE OR REPLACE FUNCTION polar_advisor.set_advisor_window(
    start_time TIME WITH TIME ZONE DEFAULT '02:00:00+08',
    end_time   TIME WITH TIME ZONE DEFAULT '03:00:00+08',
    -- Enable the window or not, default TRUE to make it easier to use.
    -- This is ok because only vacuum is enabled when window is enabled and that's safe.
    enable     BOOLEAN DEFAULT TRUE
)
RETURNS VOID AS $$
DECLARE
    rows_updated INT;
    window_len INT;
    min_window_len_min INT := 30;
    start_tz_offset INT := EXTRACT(TIMEZONE FROM start_time);
    end_tz_offset INT := EXTRACT(TIMEZONE FROM end_time);
BEGIN
    IF (SELECT COUNT(*) FROM polar_advisor.advisor_window) > 1 THEN
        RAISE EXCEPTION 'there are more than 1 window info in table, please delete them at first';
    END IF;

    IF start_tz_offset != end_tz_offset THEN
        RAISE EXCEPTION 'Window start time and end time must be in the same timezone';
    END IF;

    IF start_tz_offset != (EXTRACT(TIMEZONE FROM now())::INT) THEN
        RAISE EXCEPTION 'Window start/end time timezone must match the database timezone %', pg_catalog.current_setting('timezone');
    END IF;

    -- check window length
    SELECT
        CASE WHEN EXTRACT(epoch FROM $2) - EXTRACT(epoch FROM $1) >= 0
        THEN EXTRACT(epoch FROM $2) - EXTRACT(epoch FROM $1)
        ELSE 24 * 60 * 60 - EXTRACT(epoch FROM $1) + EXTRACT(epoch FROM $2)
        END INTO window_len;
    RAISE NOTICE 'window length: % minutes', window_len / 60;
    IF window_len < min_window_len_min * 60 THEN
        RAISE EXCEPTION 'advisor window time range should be no less than % mins', min_window_len_min;
    END IF;

    -- We cannot do delete because there could be some config info in column others
    UPDATE polar_advisor.advisor_window SET start_time = $1, end_time = $2, enabled = $3;
    GET DIAGNOSTICS rows_updated = ROW_COUNT;
    IF rows_updated = 0 THEN
        INSERT INTO polar_advisor.advisor_window (start_time, end_time, enabled) VALUES ($1, $2, $3);
    END IF;
END;
$$ LANGUAGE PLpgSQL;


-- Function to get advisor window
DROP FUNCTION IF EXISTS polar_advisor.get_advisor_window;
CREATE OR REPLACE FUNCTION polar_advisor.get_advisor_window()
RETURNS SETOF polar_advisor.advisor_window AS $$
    SELECT * FROM polar_advisor.advisor_window;
$$ LANGUAGE sql;

-- Function to check whether the window is enabled
DROP FUNCTION IF EXISTS polar_advisor.is_window_enabled;
CREATE OR REPLACE FUNCTION polar_advisor.is_window_enabled()
RETURNS BOOLEAN AS $$
DECLARE
    rows_num INT := 0;
BEGIN
    SELECT COUNT(*) FROM polar_advisor.advisor_window INTO rows_num;
    -- The window is valid if and only if there's 1 row in the table
    -- If there are multiple rows, we don't know which window range to use.
    IF rows_num <> 1 THEN
        RETURN FALSE;
    END IF;

    -- Check whether the window is enabled
    IF EXISTS (SELECT 1 FROM polar_advisor.advisor_window WHERE enabled = TRUE) THEN
        RETURN TRUE;
    ELSE
        RETURN FALSE;
    END IF;
END;
$$ LANGUAGE PLpgSQL;

-- Function to check exception in window
DROP FUNCTION IF EXISTS polar_advisor.advisor_window_has_exception;
CREATE OR REPLACE FUNCTION polar_advisor.advisor_window_has_exception ()
RETURNS BOOLEAN AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM polar_advisor.advisor_window w
        WHERE w.last_error_detail IS NOT NULL OR w.last_error_time IS NOT NULL
    ) THEN
        RETURN TRUE;
    ELSE
        RETURN FALSE;
    END IF;
END;
$$ LANGUAGE PLpgSQL;

-- Function to report exception info
DROP FUNCTION IF EXISTS polar_advisor.report_exception;
CREATE OR REPLACE FUNCTION polar_advisor.report_exception (
    exception_detail TEXT,
    exception_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
)
RETURNS VOID AS $$
BEGIN
    UPDATE polar_advisor.advisor_window SET enabled = false, last_error_detail = $1, last_error_time = $2;
END;
$$ LANGUAGE PLpgSQL;

-- Function to clear exception info
DROP FUNCTION IF EXISTS polar_advisor.clear_exception;
CREATE OR REPLACE FUNCTION polar_advisor.clear_exception ()
RETURNS VOID AS $$
BEGIN
    UPDATE polar_advisor.advisor_window SET last_error_detail = NULL, last_error_time = NULL;
END;
$$ LANGUAGE PLpgSQL;

-- Function to get the length of the window
DROP FUNCTION IF EXISTS polar_advisor.get_advisor_window_length;
CREATE OR REPLACE FUNCTION polar_advisor.get_advisor_window_length()
RETURNS NUMERIC AS $$
DECLARE
    window_len NUMERIC;
BEGIN
    SELECT
        CASE WHEN EXTRACT(epoch FROM end_time) - EXTRACT(epoch FROM start_time) >= 0
        THEN EXTRACT(epoch FROM end_time) - EXTRACT(epoch FROM start_time)
        -- window cross 00:00
        ELSE 24 * 60 * 60 - EXTRACT(epoch FROM start_time) + EXTRACT(epoch FROM end_time)
        END INTO window_len
    FROM polar_advisor.advisor_window LIMIT 1;

    IF window_len IS NULL THEN
        window_len := 0;
    END IF;

    RETURN window_len;
END;
$$ LANGUAGE PLpgSQL;

-- Function to get the seconds to the window start
DROP FUNCTION IF EXISTS polar_advisor.get_secs_to_advisor_window_start;
CREATE OR REPLACE FUNCTION polar_advisor.get_secs_to_advisor_window_start(
    current_t TIME WITH TIME ZONE DEFAULT CURRENT_TIME
)
RETURNS TABLE (
    secs_to_window_start FLOAT,
    time_now TIME WITH TIME ZONE,
    window_start TIME WITH TIME ZONE,
    window_end TIME WITH TIME ZONE
)
AS $$
DECLARE
    current_tz_offset INT := EXTRACT(TIMEZONE FROM $1);
    start_tz_offset INT := EXTRACT(TIMEZONE FROM (SELECT start_time FROM polar_advisor.advisor_window LIMIT 1));
BEGIN
    IF (SELECT COUNT(*) FROM polar_advisor.advisor_window) <> 1 THEN
        RAISE EXCEPTION 'There must be one window row!';
    END IF;

    IF start_tz_offset != current_tz_offset THEN
        RAISE EXCEPTION 'The parameter timezone must match the window timezone';
    END IF;

    IF current_tz_offset != (EXTRACT(TIMEZONE FROM now())::INT) THEN
        RAISE EXCEPTION 'The parameter timezone must match the database timezone %', pg_catalog.current_setting('timezone');
    END IF;

    RETURN QUERY
    SELECT
        CASE
            WHEN (EXTRACT(epoch FROM w.start_time) - EXTRACT(epoch FROM $1)) >= 0
            -- the start time today
            THEN EXTRACT(epoch FROM w.start_time) - EXTRACT(epoch FROM $1)
            -- the start time tomorrow
            ELSE EXTRACT(epoch FROM w.start_time) - EXTRACT(epoch FROM $1) + 24 * 60 * 60
        END::FLOAT AS secs_to_window_start,
        $1 AS current_time,
        w.start_time AS start_time,
        w.end_time AS start_end
    FROM polar_advisor.advisor_window w;
END;
$$ LANGUAGE PLpgSQL;

-- Function to get the seconds to the window end
DROP FUNCTION IF EXISTS polar_advisor.get_secs_to_advisor_window_end;
CREATE OR REPLACE FUNCTION polar_advisor.get_secs_to_advisor_window_end (
    current_t TIME WITH TIME ZONE DEFAULT CURRENT_TIME,
    time_reserved INTERVAL DEFAULT INTERVAL '10 mins'
)
RETURNS TABLE (
    secs_to_window_end FLOAT,
    time_now TIME WITH TIME ZONE,
    window_start TIME WITH TIME ZONE,
    window_end TIME WITH TIME ZONE
)
AS $$
DECLARE
    current_tz_offset INT := EXTRACT(TIMEZONE FROM $1);
    end_tz_offset INT := EXTRACT(TIMEZONE FROM (SELECT end_time FROM polar_advisor.advisor_window LIMIT 1));
BEGIN
    IF (SELECT COUNT(*) FROM polar_advisor.advisor_window) <> 1 THEN
        RAISE EXCEPTION 'There must be one window row!';
    END IF;

    IF end_tz_offset != current_tz_offset THEN
        RAISE EXCEPTION 'The parameter timezone must match the window timezone';
    END IF;

    IF current_tz_offset != (EXTRACT(TIMEZONE FROM now())::INT) THEN
        RAISE EXCEPTION 'The parameter timezone must match the database timezone %', pg_catalog.current_setting('timezone');
    END IF;

    RETURN QUERY
    WITH calculated_secs AS (
        SELECT (EXTRACT(EPOCH FROM (w.end_time - $2)) - EXTRACT(EPOCH FROM $1))::FLOAT AS secs
        FROM polar_advisor.advisor_window w
    )
    SELECT
        CASE
            WHEN secs < 0 THEN secs + 24 * 3600
            ELSE secs
        END AS secs_to_window_end,
        $1 AS time_now,
        (SELECT start_time FROM polar_advisor.advisor_window LIMIT 1) AS window_start,
        (SELECT end_time FROM polar_advisor.advisor_window LIMIT 1) AS window_end
    FROM
        calculated_secs;
END;
$$ LANGUAGE PLpgSQL;

-- Function to check whether the current time is in the window
DROP FUNCTION IF EXISTS polar_advisor.is_in_advisor_window;
CREATE OR REPLACE FUNCTION polar_advisor.is_in_advisor_window(
    current_t       TIME WITH TIME ZONE DEFAULT CURRENT_TIME,
    time_reserved   INTERVAL DEFAULT '10 mins'
)
RETURNS BOOLEAN AS $$
DECLARE
    is_in_window BOOLEAN;
    current_tz_offset INT := EXTRACT(TIMEZONE FROM $1);
    start_tz_offset INT := EXTRACT(TIMEZONE FROM (SELECT start_time FROM polar_advisor.advisor_window LIMIT 1));
    end_tz_offset INT := EXTRACT(TIMEZONE FROM (SELECT end_time FROM polar_advisor.advisor_window LIMIT 1));
BEGIN
    IF (SELECT COUNT(*) FROM polar_advisor.advisor_window) > 1 THEN
        RAISE WARNING 'There are more than one window row!';
        RETURN FALSE;
    END IF;

    IF start_tz_offset != end_tz_offset THEN
        RAISE WARNING 'Window start time and end time must be in the same timezone';
        RETURN FALSE;
    END IF;

    IF start_tz_offset != current_tz_offset THEN
        RAISE EXCEPTION 'The parameter timezone offset % hour must match the window timezone offset % hour', current_tz_offset / 3600.0, start_tz_offset / 3600.0;
        RETURN FALSE;
    END IF;

    IF current_tz_offset != (EXTRACT(TIMEZONE FROM now())::INT) THEN
        RAISE EXCEPTION 'The parameter timezone offset % hour must match the database timezone %', current_tz_offset / 3600.0, pg_catalog.current_setting('timezone');
        RETURN FALSE;
    END IF;

    SELECT $1 BETWEEN w.start_time AND (w.end_time - $2)
        -- the window crosses 00:00
        OR (w.start_time > w.end_time AND w.start_time <= $1)
        OR (w.start_time > w.end_time AND $1 <= (w.end_time + INTERVAL '24 HOURS' - $2))
        INTO is_in_window
    FROM polar_advisor.advisor_window w;
    
    IF is_in_window IS NULL THEN
        RETURN FALSE;
    ELSE
        RETURN is_in_window;
    END IF;
END;
$$ LANGUAGE PLpgSQL;


--
-- Log table. Record the advisor operation and it's benefit.
-- Valid in db postgres only.
--

-- Relation level log.
-- Never change the table name since UE relies on it.
-- No new column should be added, all the new attribute should be added to `others`
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_class WHERE relname = 'advisor_log' AND relkind = 'r'
            AND relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'polar_advisor')
    ) THEN
        CREATE TABLE polar_advisor.advisor_log (
            id                      BIGSERIAL PRIMARY KEY,
            exec_id                 BIGINT,
            start_time              TIMESTAMP WITH TIME ZONE,
            end_time                TIMESTAMP WITH TIME ZONE,
            db_name                 NAME,
            schema_name             NAME,
            relation_name           NAME,
            event_type              VARCHAR(100),
            sql_cmd                 TEXT,
            detail                  TEXT,
            tuples_deleted          BIGINT,
            tuples_dead_now         BIGINT,
            tuples_now              BIGINT,
            pages_scanned           BIGINT,
            pages_pinned            BIGINT,
            pages_frozen_now        BIGINT,
            pages_truncated         BIGINT,
            pages_now               BIGINT,
            idx_tuples_deleted      BIGINT,
            idx_tuples_now          BIGINT,
            idx_pages_now           BIGINT,
            idx_pages_deleted       BIGINT,
            idx_pages_reusable      BIGINT,
            size_before             BIGINT,
            size_now                BIGINT,
            age_decreased           BIGINT,
            others                  JSONB
        );
    END IF;
END
$$ LANGUAGE PLpgSQL;
ALTER TABLE polar_advisor.advisor_log ADD COLUMN IF NOT EXISTS others JSONB;

-- Universe Explorer connect to db postgres and collect data from table.
-- It need to filter with start_time/end_time, so we create indexes for start_time/end_time to boost collection.
CREATE INDEX IF NOT EXISTS advisor_log_start_time_idx ON polar_advisor.advisor_log (start_time);
CREATE INDEX IF NOT EXISTS advisor_log_end_time_idx ON polar_advisor.advisor_log (end_time);

-- Function to insert log
CREATE OR REPLACE FUNCTION polar_advisor.action_log_start (
    exec_id                 BIGINT,
    db_name                 NAME,
    schema_name             NAME,
    relation_name           NAME,
    event_type              polar_advisor.action_type,
    sql_cmd                 TEXT,
    size_before             BIGINT,
    start_time              TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
)
RETURNS BIGINT AS $$
    INSERT INTO polar_advisor.advisor_log (
        exec_id, db_name, schema_name, relation_name, event_type,
        sql_cmd, size_before, start_time
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    RETURNING id;
$$ LANGUAGE SQL;

-- Function to update log end time and other info
CREATE OR REPLACE FUNCTION polar_advisor.action_log_end (
    id                      BIGINT,
    detail                  TEXT,
    tuples_deleted          BIGINT,
    tuples_dead_now         BIGINT,
    tuples_now              BIGINT,
    pages_scanned           BIGINT,
    pages_pinned            BIGINT,
    pages_frozen_now        BIGINT,
    pages_truncated         BIGINT,
    pages_now               BIGINT,
    idx_tuples_deleted      BIGINT,
    idx_tuples_now          BIGINT,
    idx_pages_now           BIGINT,
    idx_pages_deleted       BIGINT,
    idx_pages_reusable      BIGINT,
    size_now                BIGINT,
    age_decreased           BIGINT,
    end_time                TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
)
RETURNS BIGINT AS $$
DECLARE
    row_count BIGINT;
BEGIN
    UPDATE polar_advisor.advisor_log l
    SET detail = $2, tuples_deleted = $3, tuples_dead_now = $4, tuples_now = $5, pages_scanned = $6,
        pages_pinned = $7, pages_frozen_now = $8, pages_truncated = $9, pages_now = $10, idx_tuples_deleted = $11,
        idx_tuples_now = $12, idx_pages_now = $13, idx_pages_deleted = $14, idx_pages_reusable = $15, size_now = $16,
        age_decreased = $17, end_time = $18
    WHERE l.id = $1;

    GET DIAGNOSTICS row_count = ROW_COUNT;

    RETURN row_count;
END;
$$ LANGUAGE PLpgSQL;

-- Function to delete old log
CREATE OR REPLACE FUNCTION polar_advisor.delete_old_log (
    interval_to_keep    INTERVAL
)
RETURNS BIGINT AS $$
DECLARE
    row_count BIGINT;
BEGIN
    -- Use start_time instead of end_time because end_time is NULL if
    -- we fail to update it and the record could be left all the time.
    DELETE FROM polar_advisor.advisor_log l
    WHERE l.start_time < (CURRENT_TIMESTAMP - interval_to_keep)
        OR l.end_time < (CURRENT_TIMESTAMP - interval_to_keep);

    GET DIAGNOSTICS row_count = ROW_COUNT;
    RETURN row_count;
END;
$$ LANGUAGE PLpgSQL;


-- Database level log
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_class WHERE relname = 'db_level_advisor_log' AND relkind = 'r'
            AND relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'polar_advisor')
    ) THEN
        CREATE TABLE polar_advisor.db_level_advisor_log (
            id                      BIGSERIAL PRIMARY KEY,
            exec_id                 BIGINT,
            start_time              TIMESTAMP WITH TIME ZONE,
            end_time                TIMESTAMP WITH TIME ZONE,
            db_name                 NAME,
            event_type              VARCHAR(100),
            total_relation          BIGINT,
            acted_relation          BIGINT,
            age_before              BIGINT,
            age_after               BIGINT,
            others                  JSONB
        );
    END IF;
END
$$ LANGUAGE PLpgSQL;
ALTER TABLE polar_advisor.db_level_advisor_log ADD COLUMN IF NOT EXISTS others JSONB;

CREATE INDEX IF NOT EXISTS db_level_advisor_log_start_time_idx ON polar_advisor.db_level_advisor_log (start_time);
CREATE INDEX IF NOT EXISTS db_level_advisor_log_end_time_idx ON polar_advisor.db_level_advisor_log (end_time);

-- Function to insert db level log
CREATE OR REPLACE FUNCTION polar_advisor.action_db_log_start (
    exec_id                 BIGINT,
    db_name                 NAME,
    event_type              polar_advisor.action_type,
    total_relation          BIGINT,
    age_before              BIGINT,
    db_size_before          BIGINT,
    cluster_age_before      BIGINT,
    start_time              TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
)
RETURNS BIGINT AS $$
    INSERT INTO polar_advisor.db_level_advisor_log (
        exec_id, db_name, event_type, total_relation, age_before, others,
        start_time
    ) VALUES (
        $1, $2, $3, $4, $5, 
        pg_catalog.jsonb_build_object(
            'db_size_before', $6, 
            'cluster_age_before', $7
        ),
        $8
    )
    RETURNING id;
$$ LANGUAGE SQL;

-- Function to update db level log
CREATE OR REPLACE FUNCTION polar_advisor.action_db_log_end (
    id                      BIGINT,
    acted_relation          BIGINT,
    age_after               BIGINT,
    db_size_after           BIGINT,
    cluster_age_after       BIGINT,
    end_time                TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
)
RETURNS BIGINT AS $$
DECLARE
    row_count BIGINT;
BEGIN
    UPDATE polar_advisor.db_level_advisor_log l
    SET acted_relation = $2, 
        age_after = $3,
        others = COALESCE(others, '{}') || pg_catalog.jsonb_build_object(
            'db_size_after', $4,
            'cluster_age_after', $5
        ),
        end_time = $6
    WHERE l.id = $1;

    GET DIAGNOSTICS row_count = ROW_COUNT;
    RETURN row_count;
END;
$$ LANGUAGE PLpgSQL;

-- Function to delete old db log
CREATE OR REPLACE FUNCTION polar_advisor.delete_old_db_log (
    interval_to_keep    INTERVAL
)
RETURNS BIGINT AS $$
DECLARE
    row_count BIGINT;
BEGIN
    -- Use both start_time and end_time because end_time is NULL if
    -- we fail to update it and the record could be left all the time.
    DELETE FROM polar_advisor.db_level_advisor_log l
    WHERE l.start_time < (CURRENT_TIMESTAMP - interval_to_keep)
        OR l.end_time < (CURRENT_TIMESTAMP - interval_to_keep);

    GET DIAGNOSTICS row_count = ROW_COUNT;
    RETURN row_count;
END;
$$ LANGUAGE PLpgSQL;

--
-- Blacklist, valid in all databases.
--

-- Black list table
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_class WHERE relname = 'blacklist_relations' AND relkind = 'r'
            AND relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'polar_advisor')
    ) THEN
        CREATE TABLE polar_advisor.blacklist_relations (
            schema_name         NAME,
            relation_name       NAME,
            action_type polar_advisor.action_type DEFAULT 'VACUUM ANALYZE',
            CONSTRAINT blacklist_schema_table_pk PRIMARY KEY (schema_name, relation_name)
        );
    END IF;
END
$$ LANGUAGE PLpgSQL;
-- The default action type is 'VACUUM ANALYZE' because there are existed blacklist in old version
-- and they are all in type 'VACUUM ANALYZE'.
ALTER TABLE polar_advisor.blacklist_relations ADD COLUMN IF NOT EXISTS action_type polar_advisor.action_type DEFAULT 'VACUUM ANALYZE';

-- Function to set vacuum analyze black list
DROP FUNCTION IF EXISTS polar_advisor.add_relation_to_vacuum_analyze_blacklist;
CREATE OR REPLACE FUNCTION polar_advisor.add_relation_to_vacuum_analyze_blacklist (
    schema_name             NAME,
    relation_name           NAME
)
RETURNS BOOLEAN AS $$
BEGIN
    RETURN polar_advisor.add_relation_to_blacklist_internal($1, $2, 'VACUUM ANALYZE');
END;
$$ LANGUAGE PLpgSQL;

-- Function to set reindex black list
DROP FUNCTION IF EXISTS polar_advisor.add_relation_to_reindex_blacklist;
CREATE OR REPLACE FUNCTION polar_advisor.add_relation_to_reindex_blacklist (
    schema_name             NAME,
    relation_name           NAME
)
RETURNS BOOLEAN AS $$
BEGIN
    RETURN polar_advisor.add_relation_to_blacklist_internal($1, $2, 'REINDEX');
END;
$$ LANGUAGE PLpgSQL;

-- Common function to set black list
DROP FUNCTION IF EXISTS polar_advisor.add_relation_to_blacklist_internal;
CREATE OR REPLACE FUNCTION polar_advisor.add_relation_to_blacklist_internal (
    schema_name             NAME,
    relation_name           NAME,
    action_type             polar_advisor.action_type
)
RETURNS BOOLEAN AS $$
DECLARE
    rel_exists    BOOLEAN;
BEGIN
    SELECT COUNT(*) > 0 INTO rel_exists
    FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
    WHERE n.nspname = $1 AND c.relname = $2;
    IF NOT rel_exists THEN
        RAISE EXCEPTION 'relation "%.%" does not exist', $1, $2;
    END IF;

    SELECT COUNT(*) > 0 INTO rel_exists FROM polar_advisor.blacklist_relations t
    WHERE t.schema_name = $1 AND t.relation_name = $2 AND t.action_type = $3;
    IF rel_exists THEN
        RAISE WARNING 'relation "%.%" already exists in blacklist of type %, skip it', $1, $2, $3;
        RETURN FALSE;
    END IF;

    -- Insert a record into the table
    INSERT INTO polar_advisor.blacklist_relations (schema_name, relation_name, action_type) VALUES ($1, $2, $3);
    RETURN TRUE;
END;
$$ LANGUAGE PLpgSQL;

-- Function to delete from vacuum analyze black list
DROP FUNCTION IF EXISTS polar_advisor.delete_relation_from_vacuum_analyze_blacklist;
CREATE OR REPLACE FUNCTION polar_advisor.delete_relation_from_vacuum_analyze_blacklist (
    schema_name                 NAME,
    relation_name               NAME
)
RETURNS BOOLEAN AS $$
BEGIN
    RETURN polar_advisor.delete_relation_from_blacklist_internal($1, $2, 'VACUUM ANALYZE');
END;
$$ LANGUAGE PLpgSQL;

-- Function to delete from reindex black list
DROP FUNCTION IF EXISTS polar_advisor.delete_relation_from_reindex_blacklist;
CREATE OR REPLACE FUNCTION polar_advisor.delete_relation_from_reindex_blacklist (
    schema_name                 NAME,
    relation_name               NAME
)
RETURNS BOOLEAN AS $$
BEGIN
    RETURN polar_advisor.delete_relation_from_blacklist_internal($1, $2, 'REINDEX');
END;
$$ LANGUAGE PLpgSQL;

-- Common function to delete from black list
DROP FUNCTION IF EXISTS polar_advisor.delete_relation_from_blacklist_internal;
CREATE OR REPLACE FUNCTION polar_advisor.delete_relation_from_blacklist_internal (
    schema_name                 NAME,
    relation_name               NAME,
    action_type                 polar_advisor.action_type
)
RETURNS BOOLEAN AS $$
DECLARE
    row_data        RECORD;
    cur             REFCURSOR;
    window_num      INT4 := 0;
BEGIN
    OPEN cur FOR SELECT * FROM polar_advisor.blacklist_relations b
    WHERE b.schema_name = $1 AND b.relation_name = $2 AND b.action_type = $3;
    LOOP
        FETCH NEXT FROM cur INTO row_data;
        EXIT WHEN NOT FOUND;
        window_num := window_num + 1;
        RAISE NOTICE 'delete relation "%.%" from blacklist of type %', row_data.schema_name, row_data.relation_name, row_data.action_type;
        DELETE FROM polar_advisor.blacklist_relations WHERE CURRENT OF cur;
    END LOOP;
    CLOSE cur;

    IF window_num = 0 THEN
        RAISE NOTICE 'no relation to delete';
        RETURN FALSE;
    END IF;
    RETURN TRUE;
END;
$$ LANGUAGE PLpgSQL;

-- Function to check in vacuum analyze blacklist
DROP FUNCTION IF EXISTS polar_advisor.is_relation_in_vacuum_analyze_blacklist;
CREATE OR REPLACE FUNCTION polar_advisor.is_relation_in_vacuum_analyze_blacklist (
    schema_name             NAME,
    relation_name           NAME
)
RETURNS BOOLEAN AS $$
BEGIN
    RETURN polar_advisor.is_relation_in_blacklist_internal($1, $2, 'VACUUM ANALYZE');
END;
$$ LANGUAGE PLpgSQL;

-- Function to check in reindex blacklist
DROP FUNCTION IF EXISTS polar_advisor.is_relation_in_reindex_blacklist;
CREATE OR REPLACE FUNCTION polar_advisor.is_relation_in_reindex_blacklist (
    schema_name             NAME,
    relation_name           NAME
)
RETURNS BOOLEAN AS $$
BEGIN
    RETURN polar_advisor.is_relation_in_blacklist_internal($1, $2, 'REINDEX');
END;
$$ LANGUAGE PLpgSQL;

-- Common function to check in blacklist
DROP FUNCTION IF EXISTS polar_advisor.is_relation_in_blacklist_internal;
CREATE OR REPLACE FUNCTION polar_advisor.is_relation_in_blacklist_internal (
    schema_name             NAME,
    relation_name           NAME,
    action_type             polar_advisor.action_type
)
RETURNS BOOLEAN AS $$
DECLARE
    is_blacklisted    BOOLEAN;
BEGIN
    SELECT COUNT(*) > 0 INTO is_blacklisted FROM polar_advisor.blacklist_relations t
    WHERE t.schema_name = $1 AND t.relation_name = $2 AND t.action_type = $3;

    RETURN is_blacklisted;
END;
$$ LANGUAGE PLpgSQL;

-- Function to get vacuum analyze black list
DROP FUNCTION IF EXISTS polar_advisor.get_vacuum_analyze_blacklist;
CREATE OR REPLACE FUNCTION polar_advisor.get_vacuum_analyze_blacklist()
RETURNS TABLE(schema_name NAME, relation_name NAME, action_type polar_advisor.action_type) AS $$
BEGIN
    RETURN QUERY SELECT * FROM polar_advisor.blacklist_relations b WHERE b.action_type = 'VACUUM ANALYZE';
END;
$$ LANGUAGE PLpgSQL;

-- Function to get reindex black list
DROP FUNCTION IF EXISTS polar_advisor.get_reindex_blacklist;
CREATE OR REPLACE FUNCTION polar_advisor.get_reindex_blacklist()
RETURNS TABLE(schema_name NAME, relation_name NAME, action_type polar_advisor.action_type) AS $$
BEGIN
    RETURN QUERY SELECT * FROM polar_advisor.blacklist_relations b WHERE b.action_type = 'REINDEX';
END;
$$ LANGUAGE PLpgSQL;


--
-- Whitelist, valid in all databases.
--

-- White list table
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_class WHERE relname = 'whitelist_relations' AND relkind = 'r'
            AND relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'polar_advisor')
    ) THEN
        CREATE TABLE polar_advisor.whitelist_relations (
            schema_name         NAME,
            relation_name       NAME,
            action_type polar_advisor.action_type,
            CONSTRAINT whitelist_schema_table_pk PRIMARY KEY (schema_name, relation_name)
        );
    END IF;
END
$$ LANGUAGE PLpgSQL;

-- Functions to set white list
DROP FUNCTION IF EXISTS polar_advisor.add_relation_to_whitelist_internal;
CREATE OR REPLACE FUNCTION polar_advisor.add_relation_to_whitelist_internal (
    schema_name             NAME,
    relation_name           NAME,
    action_type             polar_advisor.action_type
)
RETURNS BOOLEAN AS $$
DECLARE
    rel_exists    BOOLEAN;
BEGIN
    SELECT COUNT(*) > 0 INTO rel_exists
    FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
    WHERE n.nspname = $1 AND c.relname = $2;
    IF NOT rel_exists THEN
        RAISE EXCEPTION 'relation "%.%" does not exist', $1, $2;
    END IF;

    SELECT COUNT(*) > 0 INTO rel_exists FROM polar_advisor.whitelist_relations t
    WHERE t.schema_name = $1 AND t.relation_name = $2 AND t.action_type = $3;
    IF rel_exists THEN
        RAISE WARNING 'relation "%.%" already exists in whitelist of type %, skip it', $1, $2, $3;
        RETURN FALSE;
    END IF;

    -- Insert a record into the table
    INSERT INTO polar_advisor.whitelist_relations (schema_name, relation_name, action_type) VALUES ($1, $2, $3);
    RETURN TRUE;
END;
$$ LANGUAGE PLpgSQL;

-- Function to delete from whitelist
DROP FUNCTION IF EXISTS polar_advisor.delete_relation_from_whitelist_internal;
CREATE OR REPLACE FUNCTION polar_advisor.delete_relation_from_whitelist_internal (
    schema_name                 NAME,
    relation_name               NAME,
    action_type                 polar_advisor.action_type
)
RETURNS BOOLEAN AS $$
DECLARE
    row_data        RECORD;
    cur             REFCURSOR;
    window_num      INT4 := 0;
BEGIN
    OPEN cur FOR SELECT * FROM polar_advisor.whitelist_relations b
    WHERE b.schema_name = $1 AND b.relation_name = $2 AND b.action_type = $3;
    LOOP
        FETCH NEXT FROM cur INTO row_data;
        EXIT WHEN NOT FOUND;
        window_num := window_num + 1;
        RAISE NOTICE 'delete relation "%.%" from whitelist of type %', row_data.schema_name, row_data.relation_name, row_data.action_type;
        DELETE FROM polar_advisor.whitelist_relations WHERE CURRENT OF cur;
    END LOOP;
    CLOSE cur;

    IF window_num = 0 THEN
        RAISE NOTICE 'no relation to delete';
        RETURN FALSE;
    END IF;
    RETURN TRUE;
END;
$$ LANGUAGE PLpgSQL;

-- Common function to check in whitelist
DROP FUNCTION IF EXISTS polar_advisor.is_relation_in_whitelist_internal;
CREATE OR REPLACE FUNCTION polar_advisor.is_relation_in_whitelist_internal (
    schema_name             NAME,
    relation_name           NAME,
    action_type             polar_advisor.action_type
)
RETURNS BOOLEAN AS $$
DECLARE
    is_whitelisted    BOOLEAN;
BEGIN
    SELECT COUNT(*) > 0 INTO is_whitelisted FROM polar_advisor.whitelist_relations t
    WHERE t.schema_name = $1 AND t.relation_name = $2 AND t.action_type = $3;

    RETURN is_whitelisted;
END;
$$ LANGUAGE PLpgSQL;


--
-- Monitor. super_pg client uses these functions to monitor the conn number and memory.
--
-- Function to set user active connection number limit if the default limit does not work
DROP FUNCTION IF EXISTS polar_advisor.set_active_user_conn_num_limit;
CREATE OR REPLACE FUNCTION polar_advisor.set_active_user_conn_num_limit (
    active_user_conn_limit INT
)
RETURNS VOID AS $$
DECLARE
    rows_updated INT;
BEGIN
    IF $1 <= 0 THEN
        RAISE EXCEPTION 'parameter active_user_conn_limit should be a positive integer value';
    END IF;
    UPDATE polar_advisor.advisor_window
    SET others =
        CASE WHEN others IS NULL THEN pg_catalog.jsonb_build_object('active_user_conn_limit', $1)
        ELSE others || pg_catalog.jsonb_build_object('active_user_conn_limit', $1) END;
    GET DIAGNOSTICS rows_updated = ROW_COUNT;
    IF rows_updated = 0 THEN
        RAISE EXCEPTION 'there is no window info, please set it by polar_advisor.set_advisor_window(start_time, end_time) at first';
    END IF;
END;
$$ LANGUAGE PLpgSQL;

-- Function to unset user active connection number limit
DROP FUNCTION IF EXISTS polar_advisor.unset_active_user_conn_num_limit;
CREATE OR REPLACE FUNCTION polar_advisor.unset_active_user_conn_num_limit()
RETURNS VOID AS $$
DECLARE
    rows_updated INT;
BEGIN
    UPDATE polar_advisor.advisor_window
    SET others = others - 'active_user_conn_limit';
    GET DIAGNOSTICS rows_updated = ROW_COUNT;
    IF rows_updated = 0 THEN
        RAISE EXCEPTION 'there is no window info, please set it by polar_advisor.set_advisor_window(start_time, end_time) at first';
    END IF;
END;
$$ LANGUAGE PLpgSQL;

-- Function to get user active connection number limit
DROP FUNCTION IF EXISTS polar_advisor.get_active_user_conn_num_limit;
CREATE OR REPLACE FUNCTION polar_advisor.get_active_user_conn_num_limit (
    default_limit INT DEFAULT 8,
    min_limit INT DEFAULT 5,
    max_limit INT DEFAULT 10,
    -- Reserve some conns: 1 for monitor, 1 for advisor action, 1 for CM, 1 for aurora, 1 for UE,
    -- other conns for users
    reserved_conns INT DEFAULT 5
)
RETURNS INT AS $$
DECLARE
    conn_limit INT := -1;
BEGIN
    -- 1. get user config from advisor window table if user set it by function polar_advisor.set_active_user_conn_num_limit()
    SELECT (others->>'active_user_conn_limit')::INT INTO conn_limit
    FROM polar_advisor.advisor_window LIMIT 1;
    -- It's ok if the result is NULL because NULL is never bigger than 0
    IF conn_limit > 0 THEN
        RAISE NOTICE 'get active user conn limit from table';
        RETURN conn_limit;
    END IF;

    IF min_limit > max_limit THEN
        RAISE EXCEPTION 'min_limit should be no more than max_limit';
    END IF;
    IF $1 < min_limit OR $1 > max_limit THEN
        RAISE EXCEPTION 'default_limit should be between min_limit and max_limit';
    END IF;

    -- 2. get by cpu cores number
    RAISE NOTICE 'get active user conn limit by CPU cores number';
    -- XXX: polar_instance_spec_cpu could be float number for serverless and it cannot be cast to INT directly
    SELECT setting::NUMERIC::INT - $4 INTO conn_limit FROM pg_catalog.pg_settings
    WHERE name = 'polar_instance_spec_cpu';
    IF conn_limit IS NULL THEN
        RAISE NOTICE 'guc polar_instance_spec_cpu does not exist, use default value';
        conn_limit := $1;
    ELSIF conn_limit <= $2 THEN
        RAISE NOTICE 'guc polar_instance_spec_cpu value less than min value, use min value as limit';
        conn_limit := $2;
    ELSIF conn_limit > $3 THEN
        RAISE NOTICE 'guc polar_instance_spec_cpu value exceeds max limit, use max value as limit';
        conn_limit := $3;
    END IF;

    RETURN conn_limit;
END;
$$ LANGUAGE PLpgSQL;


--
-- Super PG version info. super_pg client uses these functions to update the version in the window table
-- and UE collect the window info so we can show the versions online by GAWR.
--
-- Function to set super PG version info
DROP FUNCTION IF EXISTS polar_advisor.set_super_pg_client_version_release_date;
CREATE OR REPLACE FUNCTION polar_advisor.set_super_pg_client_version_release_date (
    super_pg_version TEXT,
    super_pg_release_date INT
)
RETURNS VOID AS $$
DECLARE
    rows_updated INT;
BEGIN
    UPDATE polar_advisor.advisor_window
    SET others = COALESCE(others, '{}'::jsonb) ||
        jsonb_build_object(
            'super_pg_version', $1,
            'super_pg_release_date', $2
        );
    GET DIAGNOSTICS rows_updated = ROW_COUNT;
    IF rows_updated = 0 THEN
        RAISE EXCEPTION 'there is no window info, please set it by polar_advisor.set_advisor_window(start_time, end_time) at first';
    END IF;
END;
$$ LANGUAGE PLpgSQL;

-- Function to get super PG version info
DROP FUNCTION IF EXISTS polar_advisor.get_super_pg_version;
CREATE OR REPLACE FUNCTION polar_advisor.get_super_pg_version()
RETURNS TEXT AS $$
DECLARE
    version TEXT;
BEGIN
    SELECT (others ->> 'super_pg_version') INTO version
    FROM polar_advisor.advisor_window LIMIT 1;
    RETURN version;
END;
$$ LANGUAGE PLpgSQL;

-- Function to get super PG release date
DROP FUNCTION IF EXISTS polar_advisor.get_super_pg_release_date;
CREATE OR REPLACE FUNCTION polar_advisor.get_super_pg_release_date()
RETURNS INT AS $$
DECLARE
    release_date INT;
BEGIN
    SELECT (others ->> 'super_pg_release_date')::INT INTO release_date
    FROM polar_advisor.advisor_window LIMIT 1;
    RETURN release_date;
END;
$$ LANGUAGE PLpgSQL;


--
-- Vacuum
--

-- Function to check if vacuum analyze is enabled
DROP FUNCTION IF EXISTS polar_advisor.is_vacuum_analyze_enabled;
CREATE OR REPLACE FUNCTION polar_advisor.is_vacuum_analyze_enabled ()
RETURNS BOOLEAN AS $$
DECLARE
    vacuum_enabled BOOLEAN := FALSE;
BEGIN
    IF NOT polar_advisor.is_window_enabled() THEN
        RETURN FALSE;
    END IF;

    -- Get user config from window table if user set it by polar_advisor.enable_vacuum_analyze()
    SELECT (others->>'vacuum_analyze_enabled')::BOOLEAN INTO vacuum_enabled
    FROM polar_advisor.advisor_window LIMIT 1;
    IF vacuum_enabled = TRUE THEN
        RETURN TRUE;
    -- If user has not set it, vacuum analyze is enabled by default because it's safe
    ELSIF vacuum_enabled IS NULL THEN
        RETURN TRUE;
    ELSE
        RETURN FALSE;
    END IF;
END;
$$ LANGUAGE PLpgSQL;

-- Function to enable vacuum analyze
DROP FUNCTION IF EXISTS polar_advisor.enable_vacuum_analyze;
CREATE OR REPLACE FUNCTION polar_advisor.enable_vacuum_analyze ()
RETURNS VOID AS $$
DECLARE
    rows_updated INT := 0;
BEGIN
    UPDATE polar_advisor.advisor_window
    SET others =
        CASE WHEN others IS NULL THEN pg_catalog.jsonb_build_object('vacuum_analyze_enabled', TRUE)
        ELSE others || pg_catalog.jsonb_build_object('vacuum_analyze_enabled', TRUE) END;
    GET DIAGNOSTICS rows_updated = ROW_COUNT;
    IF rows_updated = 0 THEN
        RAISE EXCEPTION 'there is no window info, please set it by polar_advisor.set_advisor_window(start_time, end_time) at first';
    END IF;
END;
$$ LANGUAGE PLpgSQL;

-- Function to disable vacuum analyze
DROP FUNCTION IF EXISTS polar_advisor.disable_vacuum_analyze;
CREATE OR REPLACE FUNCTION polar_advisor.disable_vacuum_analyze ()
RETURNS VOID AS $$
DECLARE
    rows_updated INT := 0;
BEGIN
    UPDATE polar_advisor.advisor_window
    SET others =
        CASE WHEN others IS NULL THEN pg_catalog.jsonb_build_object('vacuum_analyze_enabled', FALSE)
        ELSE others || pg_catalog.jsonb_build_object('vacuum_analyze_enabled', FALSE) END;
    GET DIAGNOSTICS rows_updated = ROW_COUNT;
    IF rows_updated = 0 THEN
        RAISE EXCEPTION 'there is no window info, please set it by polar_advisor.set_advisor_window(start_time, end_time) at first';
    END IF;
END;
$$ LANGUAGE PLpgSQL;

-- Function to get vacuum command prefix
CREATE OR REPLACE FUNCTION polar_advisor.get_vacuum_cmd_prefix()
RETURNS TEXT AS $$
BEGIN
    -- It's ok to do TRUNCATE even if it blocks read/write because we do it in non-peak time.
    -- Parallel 2 is safe.
    -- Never add FULL option because it's dangerous!
    RETURN 'VACUUM (VERBOSE, FREEZE, ANALYZE, SKIP_LOCKED, TRUNCATE, PARALLEL 2, PROCESS_TOAST, INDEX_CLEANUP AUTO) ';
END;
$$ LANGUAGE PLpgSQL;

-- Function to get analyze command prefix
CREATE OR REPLACE FUNCTION polar_advisor.get_analyze_cmd_prefix()
RETURNS TEXT AS $$
BEGIN
    RETURN 'ANALYZE (VERBOSE, SKIP_LOCKED) ';
END;
$$ LANGUAGE PLpgSQL;

-- Function to get relations to vacuum analyze
DROP FUNCTION IF EXISTS polar_advisor.get_relations_to_vacuum_analyze;
CREATE OR REPLACE FUNCTION polar_advisor.get_relations_to_vacuum_analyze (
    result_rows INT DEFAULT 1000,
    -- Do not vacuum the same table too frequently
    sleep_interval INTERVAL DEFAULT INTERVAL '5 hours',
    -- The table should exceed at least 1 limit below
    table_age_min_limit INT DEFAULT 100000,
    table_dead_tup_min_limit INT DEFAULT 1000,
    vacuum_prefix TEXT DEFAULT polar_advisor.get_vacuum_cmd_prefix(),
    analyze_prefix TEXT DEFAULT polar_advisor.get_analyze_cmd_prefix()
    -- new parameters should be added to the last because we use the id of param in sql
)
RETURNS TABLE (
    schema_name         NAME,
    rel_name            NAME,
    vacuum_cmd          TEXT,
    age                 INT
) AS $$
DECLARE
    rows_returned INT;
    total_rows_returned INT;
    limit_rows INT := $1;
BEGIN
    -- 1. vacuum normal tables and partitions without global index at first
    RETURN QUERY
    WITH RECURSIVE partitioned_table_root(relroot, relid, relkind) AS (
        -- the root for partitioned table
        SELECT oid AS relroot, oid AS relid, relkind FROM pg_catalog.pg_class WHERE NOT relispartition AND relkind = 'p'
        -- all the children
        UNION SELECT RT.relroot AS relroot, INH.inhrelid AS relid, C.relkind
        FROM pg_catalog.pg_inherits INH INNER JOIN pg_catalog.pg_class C ON C.oid = INH.inhrelid
        INNER JOIN partitioned_table_root RT ON INH.inhparent = RT.relid
    ),
    grand_children_of_blacklist AS (
        -- the parent
        SELECT C.oid AS child_oid, N.nspname AS schema_name, C.relname AS rel_name
        FROM pg_catalog.pg_class C
        JOIN pg_catalog.pg_namespace N ON C.relnamespace = N.oid
        JOIN polar_advisor.blacklist_relations B ON B.schema_name = N.nspname AND B.relation_name = C.relname
            AND B.action_type = 'VACUUM ANALYZE'
        UNION
        -- the children and grand children
        SELECT C.oid AS child_oid, N.nspname AS schema_name, C.relname AS rel_name
        FROM pg_catalog.pg_inherits INH
        JOIN pg_catalog.pg_class C ON INH.inhrelid = C.oid
        JOIN pg_catalog.pg_namespace N ON C.relnamespace = N.oid
        JOIN grand_children_of_blacklist PT ON INH.inhparent = PT.child_oid
    ),
    grand_parents_of_blacklist AS (
        -- the son
        SELECT C.oid AS parent_oid, N.nspname AS schema_name, C.relname AS rel_name
        FROM pg_catalog.pg_class C
        JOIN pg_catalog.pg_namespace N ON C.relnamespace = N.oid
        JOIN polar_advisor.blacklist_relations B ON B.schema_name = N.nspname AND B.relation_name = C.relname
            AND B.action_type = 'VACUUM ANALYZE'
        UNION
        -- the parents and grand parents
        SELECT C.oid AS parent_oid, N.nspname AS schema_name, C.relname AS rel_name
        FROM pg_catalog.pg_inherits INH
        JOIN pg_catalog.pg_class C ON INH.inhparent = C.oid
        JOIN pg_catalog.pg_namespace N ON C.relnamespace = N.oid
        JOIN grand_parents_of_blacklist PT ON INH.inhrelid = PT.parent_oid
    )
    SELECT N.nspname AS schemaname,
        C.relname AS relname,
        -- use quote_ident for upper case relation name
        $5 || ' ' || pg_catalog.quote_ident(N.nspname) || '.' || pg_catalog.quote_ident(C.relname) AS vacuum_cmd,
        pg_catalog.age(C.relfrozenxid) AS age
    FROM pg_catalog.pg_class C JOIN pg_catalog.pg_namespace N ON N.oid = C.relnamespace
    LEFT JOIN partitioned_table_root RT ON C.oid = RT.relid
    -- only normal table and mat view, no temp table, no toast
    WHERE C.relkind IN ('r', 'm') AND C.relpersistence = 'p' AND N.nspname !~ '^pg_toast' AND
        -- parent table does not have global index
        NOT polar_advisor.table_has_global_index(RT.relroot) AND
        -- age or dead tuples exceed limit, 1 limit is enough
        (pg_catalog.age(C.relfrozenxid) >= $3 OR pg_stat_get_dead_tuples(C.oid) >= $4) AND
        COALESCE(pg_stat_get_last_vacuum_time(C.oid), NOW() - INTERVAL '12 hours') + $2 < NOW()
        -- not in blacklist
        AND (N.nspname IS NULL OR C.relname IS NULL OR (N.nspname, C.relname) NOT IN (
                SELECT DISTINCT b.schema_name, b.rel_name FROM grand_children_of_blacklist b ORDER BY schema_name, rel_name)
        ) AND (N.nspname IS NULL OR C.relname IS NULL OR (N.nspname, C.relname) NOT IN (
                SELECT DISTINCT b.schema_name, b.rel_name FROM grand_parents_of_blacklist b ORDER BY schema_name, rel_name))
    ORDER BY pg_catalog.age(C.relfrozenxid) DESC LIMIT limit_rows;

    GET DIAGNOSTICS rows_returned = ROW_COUNT;
    total_rows_returned := rows_returned;
    RAISE NOTICE 'got % normal tables to vacuum', rows_returned;
    IF total_rows_returned >= $1 THEN
        RETURN;
    ELSE
        limit_rows := $1 - total_rows_returned;
        RAISE NOTICE 'try to get % more partitioned tables with global index to vacuum at most', limit_rows;
    END IF;

    -- 2. vacuum partitioned table with global index after normal tables because partitioned tables
    -- usually have big size. There are statistics in partitioned table with global index,
    -- so we have to vacuum the partitioned table rather than partitions.
    RETURN QUERY
    WITH RECURSIVE partitioned_table_root(relroot, relid, relkind) AS (
        -- the root for partitioned table
        SELECT oid AS relroot, oid AS relid, relkind FROM pg_catalog.pg_class WHERE NOT relispartition AND relkind = 'p'
        -- all the children
        UNION SELECT RT.relroot AS relroot, INH.inhrelid AS relid, C.relkind
        FROM pg_catalog.pg_inherits INH INNER JOIN pg_catalog.pg_class C ON C.oid = INH.inhrelid
        INNER JOIN partitioned_table_root RT ON INH.inhparent = RT.relid
    ),
    grand_children_of_blacklist AS (
        -- the parent
        SELECT C.oid AS child_oid, N.nspname AS schema_name, C.relname AS rel_name
        FROM pg_catalog.pg_class C
        JOIN pg_catalog.pg_namespace N ON C.relnamespace = N.oid
        JOIN polar_advisor.blacklist_relations B ON B.schema_name = N.nspname AND B.relation_name = C.relname
            AND B.action_type = 'VACUUM ANALYZE'
        UNION
        -- the children and grand children
        SELECT C.oid AS child_oid, N.nspname AS schema_name, C.relname AS rel_name
        FROM pg_catalog.pg_inherits INH
        JOIN pg_catalog.pg_class C ON INH.inhrelid = C.oid
        JOIN pg_catalog.pg_namespace N ON C.relnamespace = N.oid
        JOIN grand_children_of_blacklist PT ON INH.inhparent = PT.child_oid
    ),
    grand_parents_of_blacklist AS (
        -- the son
        SELECT C.oid AS parent_oid, N.nspname AS schema_name, C.relname AS rel_name
        FROM pg_catalog.pg_class C
        JOIN pg_catalog.pg_namespace N ON C.relnamespace = N.oid
        JOIN polar_advisor.blacklist_relations B ON B.schema_name = N.nspname AND B.relation_name = C.relname
            AND B.action_type = 'VACUUM ANALYZE'
        UNION
        -- the parents and grand parents
        SELECT C.oid AS parent_oid, N.nspname AS schema_name, C.relname AS rel_name
        FROM pg_catalog.pg_inherits INH
        JOIN pg_catalog.pg_class C ON INH.inhparent = C.oid
        JOIN pg_catalog.pg_namespace N ON C.relnamespace = N.oid
        JOIN grand_parents_of_blacklist PT ON INH.inhrelid = PT.parent_oid
    )
    SELECT PN.nspname AS parent_schemaname,
        PC.relname AS parent_relname,
        -- use quote_ident for upper case relation name
        $5 || ' ' || pg_catalog.quote_ident(PN.nspname) || '.' || pg_catalog.quote_ident(PC.relname) AS vacuum_cmd,
        MAX(pg_catalog.age(C.relfrozenxid)) AS age
    FROM pg_catalog.pg_class PC JOIN pg_catalog.pg_namespace PN ON PN.oid = PC.relnamespace
    JOIN partitioned_table_root RT ON PC.oid = RT.relroot
    INNER JOIN pg_catalog.pg_class C ON C.oid = RT.relid
    JOIN pg_catalog.pg_namespace N ON N.oid = C.relnamespace
        -- only show the root of bottom partitions, do nothing if there are only root/branches but no leaves
    WHERE RT.relkind = 'r' AND polar_advisor.table_has_global_index(PC.oid) AND
        -- not in blacklist
        (N.nspname IS NULL OR C.relname IS NULL OR (N.nspname, C.relname) NOT IN (
            SELECT DISTINCT b.schema_name, b.rel_name FROM grand_children_of_blacklist b ORDER BY schema_name, rel_name)
        ) AND (PN.nspname IS NULL OR PC.relname IS NULL OR (PN.nspname, PC.relname) NOT IN (
            SELECT DISTINCT b.schema_name, b.rel_name FROM grand_parents_of_blacklist b ORDER BY schema_name, rel_name))
    GROUP BY parent_schemaname, parent_relname, vacuum_cmd
    -- relfrozenxid is 0 and age is the max value for partitioned table, so we have to use the partitions' age.
    -- partitioned table's dead tuple number is always 0 and count all the partitions' dead tuple instead
    HAVING MIN(COALESCE(pg_stat_get_last_vacuum_time(C.oid), NOW() - INTERVAL '12 hours')) + $2 < NOW()
        AND (SUM(pg_stat_get_dead_tuples(C.oid)) >= $4 OR MAX(pg_catalog.age(C.relfrozenxid)) >= $3)
    ORDER BY MAX(pg_catalog.age(C.relfrozenxid)) LIMIT limit_rows;

    GET DIAGNOSTICS rows_returned = ROW_COUNT;
    total_rows_returned := total_rows_returned + rows_returned;
    RAISE NOTICE 'got % partitioned tables with global index to vacuum', rows_returned;
    IF total_rows_returned >= $1 THEN
        RETURN;
    ELSE
        limit_rows := $1 - total_rows_returned;
        RAISE NOTICE 'try to get % more partitioned tables with global index to analyze at most', limit_rows;
    END IF;

    -- 3. analyze partitioned table without global index. The partitioned tables with global index have been
    -- chosen to do vacuum analyze in step 2 and here we only do vacuum for those who don't have global index.
    RETURN QUERY
    WITH RECURSIVE partitioned_table_root(relroot, relid, relkind) AS (
        -- the root for partitioned table
        SELECT oid AS relroot, oid AS relid, relkind FROM pg_catalog.pg_class WHERE NOT relispartition AND relkind = 'p'
        -- all the children
        UNION SELECT RT.relroot AS relroot, INH.inhrelid AS relid, C.relkind
        FROM pg_catalog.pg_inherits INH INNER JOIN pg_catalog.pg_class C ON C.oid = INH.inhrelid
        INNER JOIN partitioned_table_root RT ON INH.inhparent = RT.relid
    ),
    grand_children_of_blacklist AS (
        -- the parent
        SELECT C.oid AS child_oid, N.nspname AS schema_name, C.relname AS rel_name
        FROM pg_catalog.pg_class C
        JOIN pg_catalog.pg_namespace N ON C.relnamespace = N.oid
        JOIN polar_advisor.blacklist_relations B ON B.schema_name = N.nspname AND B.relation_name = C.relname
            AND B.action_type = 'VACUUM ANALYZE'
        UNION
        -- the children and grand children
        SELECT C.oid AS child_oid, N.nspname AS schema_name, C.relname AS rel_name
        FROM pg_catalog.pg_inherits INH
        JOIN pg_catalog.pg_class C ON INH.inhrelid = C.oid
        JOIN pg_catalog.pg_namespace N ON C.relnamespace = N.oid
        JOIN grand_children_of_blacklist PT ON INH.inhparent = PT.child_oid
    ),
    grand_parents_of_blacklist AS (
        -- the son
        SELECT C.oid AS parent_oid, N.nspname AS schema_name, C.relname AS rel_name
        FROM pg_catalog.pg_class C
        JOIN pg_catalog.pg_namespace N ON C.relnamespace = N.oid
        JOIN polar_advisor.blacklist_relations B ON B.schema_name = N.nspname AND B.relation_name = C.relname
            AND B.action_type = 'VACUUM ANALYZE'
        UNION
        -- the parents and grand parents
        SELECT C.oid AS parent_oid, N.nspname AS schema_name, C.relname AS rel_name
        FROM pg_catalog.pg_inherits INH
        JOIN pg_catalog.pg_class C ON INH.inhparent = C.oid
        JOIN pg_catalog.pg_namespace N ON C.relnamespace = N.oid
        JOIN grand_parents_of_blacklist PT ON INH.inhrelid = PT.parent_oid
    )
    SELECT PN.nspname AS parent_schemaname,
        PC.relname AS parent_relname,
        -- use quote_ident for upper case relation name
        $6 || ' ' || pg_catalog.quote_ident(PN.nspname) || '.' || pg_catalog.quote_ident(PC.relname) AS analyze_cmd,
        MAX(pg_catalog.age(C.relfrozenxid)) AS age
    FROM pg_catalog.pg_class PC JOIN pg_catalog.pg_namespace PN ON PN.oid = PC.relnamespace
    JOIN partitioned_table_root RT ON PC.oid = RT.relroot
    INNER JOIN pg_catalog.pg_class C ON C.oid = RT.relid
    JOIN pg_catalog.pg_namespace N ON N.oid = C.relnamespace
    -- only show the root of bottom partitions, do nothing if there are only root/branches but no leaves
    WHERE RT.relkind = 'r' AND NOT polar_advisor.table_has_global_index(PC.oid) AND
        -- not in blacklist
        (N.nspname IS NULL OR C.relname IS NULL OR (N.nspname, C.relname) NOT IN (
            SELECT DISTINCT b.schema_name, b.rel_name FROM grand_children_of_blacklist b ORDER BY schema_name, rel_name)
        ) AND (PN.nspname IS NULL OR PC.relname IS NULL OR (PN.nspname, PC.relname) NOT IN (
            SELECT DISTINCT b.schema_name, b.rel_name FROM grand_parents_of_blacklist b ORDER BY schema_name, rel_name))
    GROUP BY parent_schemaname, parent_relname, analyze_cmd
    -- in some old versions the partitioned tables don't have last analyze info and we use the min value of the partitions
    HAVING MIN(COALESCE(pg_stat_get_last_analyze_time(C.oid), NOW() - INTERVAL '12 hours')) + $2 < NOW()
    -- do not set dead tuples number and age limit for analyze
    ORDER BY MIN(COALESCE(pg_stat_get_last_analyze_time(C.oid), NOW() - INTERVAL '12 hours')) LIMIT limit_rows;

    GET DIAGNOSTICS rows_returned = ROW_COUNT;
    RAISE NOTICE 'got % partitioned tables without global index to analyze', rows_returned;
    total_rows_returned := total_rows_returned + rows_returned;
    RAISE NOTICE 'got % tables in total', total_rows_returned;
END;
$$ LANGUAGE PLpgSQL;

-- Function to check whether the index is global index.
-- This works for PG11 and PG14, also works for global partitioned index
DROP FUNCTION IF EXISTS polar_advisor.is_global_index;
CREATE OR REPLACE FUNCTION polar_advisor.is_global_index (index_oid OID)
RETURNS BOOLEAN AS $$
BEGIN
    IF EXISTS (
        SELECT c.relname AS index_name
        FROM pg_catalog.pg_class c
        WHERE c.oid = $1 AND 'global_index=true' = ANY(c.reloptions)
    ) THEN
        RETURN TRUE;
    END IF;
    RETURN FALSE;
END;
$$ LANGUAGE PLpgSQL;

DROP FUNCTION IF EXISTS polar_advisor.table_has_global_index;
CREATE OR REPLACE FUNCTION polar_advisor.table_has_global_index (table_oid OID)
RETURNS BOOLEAN AS $$
BEGIN
    IF EXISTS (
        SELECT ic.relname AS index_name
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_index i ON c.oid = i.indrelid
        JOIN pg_catalog.pg_class ic ON i.indexrelid = ic.oid
        WHERE c.oid = $1 AND 'global_index=true' = ANY(ic.reloptions)
    ) THEN
        RETURN TRUE;
    END IF;
    RETURN FALSE;
END;
$$ LANGUAGE PLpgSQL;


--
-- Reindex
--

-- Function to check whether the reindex is enabled.
DROP FUNCTION IF EXISTS polar_advisor.is_reindex_enabled;
CREATE OR REPLACE FUNCTION polar_advisor.is_reindex_enabled ()
RETURNS BOOLEAN AS $$
DECLARE
    reindex_enabled BOOLEAN := FALSE;
BEGIN
    -- REINDEX CONCURRENTLY is only supported in PG11 and earlier versions
    IF pg_catalog.current_setting('server_version_num')::INT < 120000 THEN
        RETURN FALSE;
    END IF;

    IF NOT polar_advisor.is_window_enabled() THEN
        RETURN FALSE;
    END IF;

    -- Get user config from window table if user set it by polar_advisor.enable_vacuum_analyze()
    SELECT (others->>'reindex_enabled')::BOOLEAN INTO reindex_enabled
    FROM polar_advisor.advisor_window LIMIT 1;
    IF reindex_enabled = TRUE THEN
        RETURN TRUE;
    -- If user has not set it, reindex is DISABLED by default since it could fail
    -- and it's no as safe as vacuum.
    ELSIF reindex_enabled IS NULL THEN
        RETURN FALSE;
    ELSE
        RETURN FALSE;
    END IF;
END;
$$ LANGUAGE PLpgSQL;

-- Function to enable reindex.
DROP FUNCTION IF EXISTS polar_advisor.enable_reindex;
CREATE OR REPLACE FUNCTION polar_advisor.enable_reindex ()
RETURNS VOID AS $$
DECLARE
    rows_updated INT := 0;
BEGIN
    IF pg_catalog.current_setting('server_version_num')::INT < 120000 THEN
        RAISE EXCEPTION 'REINDEX CONCURRENTLY is only supported in PG11 and earlier versions';
    END IF;

    UPDATE polar_advisor.advisor_window
    SET others =
        CASE WHEN others IS NULL THEN pg_catalog.jsonb_build_object('reindex_enabled', TRUE)
        ELSE others || pg_catalog.jsonb_build_object('reindex_enabled', TRUE) END;
    GET DIAGNOSTICS rows_updated = ROW_COUNT;
    IF rows_updated = 0 THEN
        RAISE EXCEPTION 'there is no window info, please set it by polar_advisor.set_advisor_window(start_time, end_time) at first';
    END IF;
END;
$$ LANGUAGE PLpgSQL;

-- Function to disable reindex.
DROP FUNCTION IF EXISTS polar_advisor.disable_reindex;
CREATE OR REPLACE FUNCTION polar_advisor.disable_reindex ()
RETURNS VOID AS $$
DECLARE
    rows_updated INT := 0;
BEGIN
    UPDATE polar_advisor.advisor_window
    SET others =
        CASE WHEN others IS NULL THEN pg_catalog.jsonb_build_object('reindex_enabled', FALSE)
        ELSE others || pg_catalog.jsonb_build_object('reindex_enabled', FALSE) END;
    GET DIAGNOSTICS rows_updated = ROW_COUNT;
    IF rows_updated = 0 THEN
        RAISE EXCEPTION 'there is no window info, please set it by polar_advisor.set_advisor_window(start_time, end_time) at first';
    END IF;
END;
$$ LANGUAGE PLpgSQL;

-- Function to get indexes to reindex.
DROP FUNCTION IF EXISTS polar_advisor.get_indexes_to_reindex;
CREATE OR REPLACE FUNCTION polar_advisor.get_indexes_to_reindex (
    result_rows INT DEFAULT 1000,
    reindex_prefix TEXT DEFAULT 'REINDEX (VERBOSE) INDEX CONCURRENTLY',
    min_index_page_num INT DEFAULT 12800, -- 12800 * 8KB = 100MB
    min_bloat_ratio NUMERIC DEFAULT 0.5
    -- new parameters should be added to the last because we use the id of param in sql
)
RETURNS TABLE (
    schema_name         NAME,
    index_name          NAME,
    reindex_cmd         TEXT,
    bloat_ratio         FLOAT,
    index_size          BIGINT
)
-- This function uses pg_statistics which is only accessible to superuser.
-- To let the normal user to call this function, we should use SECURITY DEFINER
-- because the extension is usually created during initdb by superuser.
-- XXX: SECURITY DEFINER is forbidden
-- SECURITY DEFINER
AS $$
BEGIN
    RETURN QUERY
    -- Level 5, get index name and bloat ratio
    WITH RECURSIVE grand_children_of_blacklist AS (
        -- the parent
        SELECT C.oid AS child_oid, N.nspname AS schema_name, C.relname AS rel_name
        FROM pg_catalog.pg_class C
        JOIN pg_catalog.pg_namespace N ON C.relnamespace = N.oid
        JOIN polar_advisor.blacklist_relations B ON B.schema_name = N.nspname AND B.relation_name = C.relname
            AND B.action_type = 'REINDEX'
        UNION
        -- the children and grand children
        SELECT C.oid AS child_oid, N.nspname AS schema_name, C.relname AS rel_name
        FROM pg_catalog.pg_inherits INH
        JOIN pg_catalog.pg_class C ON INH.inhrelid = C.oid
        JOIN pg_catalog.pg_namespace N ON C.relnamespace = N.oid
        JOIN grand_children_of_blacklist PT ON INH.inhparent = PT.child_oid
    ),
    grand_parents_of_blacklist AS (
        -- the son
        SELECT C.oid AS parent_oid, N.nspname AS schema_name, C.relname AS rel_name
        FROM pg_catalog.pg_class C
        JOIN pg_catalog.pg_namespace N ON C.relnamespace = N.oid
        JOIN polar_advisor.blacklist_relations B ON B.schema_name = N.nspname AND B.relation_name = C.relname
            AND B.action_type = 'REINDEX'
        UNION
        -- the parents and grand parents
        SELECT C.oid AS parent_oid, N.nspname AS schema_name, C.relname AS rel_name
        FROM pg_catalog.pg_inherits INH
        JOIN pg_catalog.pg_class C ON INH.inhparent = C.oid
        JOIN pg_catalog.pg_namespace N ON C.relnamespace = N.oid
        JOIN grand_parents_of_blacklist PT ON INH.inhrelid = PT.parent_oid
    )
    SELECT n.nspname AS schema_name, page_stats.index_name,
        $2 || ' ' || pg_catalog.quote_ident(n.nspname) || '.' || pg_catalog.quote_ident(page_stats.index_name) AS reindex_cmd,
        (actual_page_num - full_page_num) / actual_page_num AS bloat_ratio,
        pg_catalog.pg_relation_size(index_id) AS index_size
    FROM (
        -- Level 4, get estimated full page number
        SELECT block_size, index_id, tuple_stats.index_name, schema_id, actual_page_num,
            coalesce(tuple_num * index_tuple_size / (index_fillfactor / 100.0 * block_data_size), 0) AS full_page_num
        FROM (
            -- Level 3, get index tuple size by index tuple header size
            SELECT index_id, rows_tuple_stats.index_name, schema_id, tuple_num, actual_page_num,
                index_fillfactor, block_data_size, block_size,
                (
                    -- what is 4???
                    4 + index_tuple_header_size +
                    -- Add padding to the index tuple header
                    max_align - CASE
                        WHEN index_tuple_header_size % max_align = 0 THEN max_align
                        ELSE index_tuple_header_size % max_align
                    END
                    -- Add padding to the index tuple data
                    + index_tuple_data_size + max_align - CASE
                        WHEN index_tuple_data_size = 0 THEN 0
                        WHEN cast(index_tuple_data_size AS INT) % max_align = 0 THEN max_align
                        ELSE cast(index_tuple_data_size AS INT) % max_align
                    END
                )::NUMERIC AS index_tuple_size
            FROM (
                -- Level 2, get index tuple header size, block size, block data size
                SELECT i.index_id, i.index_name, i.schema_id, i.tuple_num, i.actual_page_num, i.index_fillfactor,
                    8 AS max_align, -- 8 on 64 bits system
                    -- per tuple header
                    CASE WHEN MAX(coalesce(s.stanullfrac, 0)) = 0
                        THEN 8 -- IndexTupleData size
                        ELSE 8 + (32 + 8 - 1) / 8 -- add IndexAttributeBitMapData size if any column is null-able
                    END AS index_tuple_header_size,
                    -- data width without null values
                    SUM((1 - coalesce(s.stanullfrac, 0)) * coalesce(s.stawidth, 1024)) AS index_tuple_data_size,
                    pg_catalog.current_setting('block_size')::NUMERIC AS block_size,
                    -- per page header is 24, per page btree opaque data is 16
                    (pg_catalog.current_setting('block_size')::NUMERIC - 24 - 16) AS block_data_size
                FROM (
                    -- Level 1, add attnum of the index column from pg_attribute
                    SELECT ic.index_name, ic.schema_id, ic.col_id, ic.tuple_num, ic.actual_page_num, ic.orig_table_id, ic.index_id, ic.index_fillfactor,
                        coalesce(a1.attnum, a2.attnum) AS attnum
                    FROM (
                        -- Level 0, raw index info from pg_index and pg_class
                        SELECT ci.reltuples AS tuple_num, ci.relpages AS actual_page_num, i.indrelid AS orig_table_id,
                            i.indexrelid AS index_id, ci.relname AS index_name, ci.relnamespace AS schema_id,
                            pg_catalog.generate_series(1, i.indnatts) AS col_id,
                            pg_catalog.string_to_array(
                                pg_catalog.textin(pg_catalog.int2vectorout(i.indkey)), ' '
                            )::INT[] AS index_key,
                            coalesce(
                                substring(pg_catalog.array_to_string(ci.reloptions, ' ') FROM 'fillfactor=([0-9]+)')::SMALLINT,
                                -- Default fillfactor is 90 for btree index
                                90
                            ) AS index_fillfactor
                        FROM pg_catalog.pg_index i
                        JOIN pg_catalog.pg_class ci ON ci.oid = i.indexrelid
                        JOIN pg_catalog.pg_namespace n ON n.oid = ci.relnamespace
                        -- Ignore small indexes
                        WHERE ci.relpages >= $3
                        -- Only btree index
                        AND ci.relam = (SELECT oid FROM pg_catalog.pg_am WHERE amname = 'btree')
                        -- Only normal index, ignore partitioned index
                        AND ci.relkind = 'i'
                        -- Ignore global index
                        AND (ci.reloptions IS NULL OR NOT ('global_index=true' = ANY (ci.reloptions)))
                        -- Ignore system and toast indexes since:
                        -- a) pg_catalog index doesn't support reindex concurrently
                        -- b) it's dangerous to operate system index
                        -- c) pg_class.reltuples of pg_toast index is always 0
                        AND n.nspname NOT IN ('pg_catalog', 'polar_catalog', 'sys', 'information_schema', 'pg_toast')
                        -- Ignore the schema of extensions because some extensions like dbms_xxx are
                        -- created by system and do not operate them. It's ok because these tables are often
                        -- small.
                        AND n.nspname NOT IN (
                            SELECT n.nspname
                            FROM pg_catalog.pg_namespace n, pg_catalog.pg_depend d, pg_catalog.pg_extension e
                            WHERE d.deptype = 'e' AND d.refobjid = e.oid AND d.objid = n.oid
                        )
                    ) AS ic
                    LEFT JOIN pg_catalog.pg_attribute a1 ON
                        a1.attrelid = ic.orig_table_id AND
                        a1.attnum = ic.index_key[ic.col_id] AND
                        -- Non-0 means normal index
                        ic.index_key[ic.col_id] <> 0
                    LEFT JOIN pg_catalog.pg_attribute a2 ON
                        a2.attrelid = ic.index_id AND
                        a2.attnum = ic.col_id AND
                        -- 0 means expression index
                        ic.index_key[ic.col_id] = 0
                ) i
                -- XXX: only superuser can see the stats
                JOIN pg_catalog.pg_statistic s
                ON s.starelid = i.orig_table_id
                AND s.staattnum = i.attnum
                GROUP BY i.index_id, i.index_name, i.schema_id, i.tuple_num, i.actual_page_num, i.index_fillfactor
            ) AS rows_tuple_stats
        ) AS tuple_stats
    ) AS page_stats
    JOIN pg_catalog.pg_namespace n ON n.oid = page_stats.schema_id
    WHERE 1.0 * (actual_page_num - full_page_num) / actual_page_num >= $4
        -- not in blacklist
        AND (n.nspname, page_stats.index_name) NOT IN (
            SELECT DISTINCT b.schema_name, b.rel_name FROM grand_children_of_blacklist b ORDER BY schema_name, rel_name)
        AND (n.nspname, page_stats.index_name) NOT IN (
            SELECT DISTINCT b.schema_name, b.rel_name FROM grand_parents_of_blacklist b ORDER BY schema_name, rel_name)
    ORDER BY bloat_ratio DESC
    LIMIT $1;
END;
$$ LANGUAGE PLpgSQL;


--
-- Stats
--

-- Function to get cluster age
CREATE OR REPLACE FUNCTION polar_advisor.get_cluster_age()
RETURNS BIGINT AS $$
    SELECT MAX(pg_catalog.age(datfrozenxid)) AS cluster_age FROM pg_catalog.pg_database;
$$ LANGUAGE SQL;

-- Function to get stats of current database
CREATE OR REPLACE FUNCTION polar_advisor.get_db_stats(
    db_name NAME DEFAULT pg_catalog.current_database(),
    age OUT BIGINT,
    db_size OUT BIGINT
)
RETURNS RECORD AS $$
BEGIN
    SELECT pg_catalog.age(D.datfrozenxid)::INT,
        pg_catalog.pg_database_size(D.datname)::BIGINT INTO age, db_size
    FROM pg_catalog.pg_database D WHERE D.datname = $1;
END;
$$ LANGUAGE PLpgSQL;

-- Function to get the total relations number that can be processed with VACUUM command.
-- Including normal table, partitioned table and mat view. Toast tables are not counted
-- because it will be processed together with the main table.
-- This value is just an estimated value for statistics and it's ok to be not accurate.
CREATE OR REPLACE FUNCTION polar_advisor.get_total_relation_num_for_vacuum()
RETURNS BIGINT AS $$
    SELECT count(*) AS total_relation FROM pg_catalog.pg_class C
    WHERE C.relkind IN ('r', 'p', 'm');
$$ LANGUAGE SQL;

-- Function to get vacuum stats of relation.
-- Get the statistics related to vacuum from pg_catalog:
-- 1. relation size, for partitioned tables, it is the sum of the size of all partitions;
-- 2. age of relations, for partitioned tables, it is the age of the oldest partition;
CREATE OR REPLACE FUNCTION polar_advisor.get_vacuum_stats_of_relation(
    schema_name NAME,
    rel_name NAME,
    total_relation_size OUT BIGINT,
    age OUT BIGINT
)
RETURNS RECORD AS $$
BEGIN
    WITH RECURSIVE grand_children_of_table AS (
        -- the parent
        SELECT C.oid AS child_oid, N.nspname AS schema_name, C.relname AS rel_name, C.relfrozenxid AS relfrozenxid, C.relkind AS relkind
        FROM pg_catalog.pg_class C
        JOIN pg_catalog.pg_namespace N ON C.relnamespace = N.oid
        WHERE N.nspname = $1 AND C.relname = $2
        UNION
        -- the children and grand children
        SELECT C.oid AS child_oid, N.nspname AS schema_name, C.relname AS rel_name, C.relfrozenxid AS relfrozenxid, C.relkind AS relkind
        FROM pg_catalog.pg_inherits INH
        JOIN pg_catalog.pg_class C ON INH.inhrelid = C.oid
        JOIN pg_catalog.pg_namespace N ON C.relnamespace = N.oid
        JOIN grand_children_of_table PT ON INH.inhparent = PT.child_oid
    )
    SELECT SUM(pg_catalog.pg_total_relation_size(G.child_oid)),
        MAX(pg_catalog.age(G.relfrozenxid)) INTO total_relation_size, age
    FROM grand_children_of_table G
    -- only count partitions because partitioned table has max age and 0 size
    WHERE G.relkind <> 'p';
END;
$$ LANGUAGE PLpgSQL;

--
-- Add all existed objects to extension.
-- In some old version instances, the polar_advisor objects were created by super_pg client
-- and now we need to add them to extension.
--

-- Add schemas to extension
DO $$
DECLARE
    schema_rec RECORD;
    extension_oid OID;
BEGIN
    SELECT oid INTO extension_oid FROM pg_catalog.pg_extension WHERE extname = 'polar_advisor';
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_depend WHERE refobjid = extension_oid
            AND classid = 'pg_namespace'::regclass AND objid = 'polar_advisor'::regnamespace
    ) THEN
        RAISE NOTICE 'Schema polar_advisor is already a member of the extension';
    ELSE
        RAISE NOTICE 'Add schema polar_advisor to extension';
        EXECUTE 'ALTER EXTENSION polar_advisor ADD schema polar_advisor';
    END IF;
END
$$ LANGUAGE PLpgSQL;


-- Add tables to extension
DO $$
DECLARE
    table_rec RECORD;
    extension_oid OID;
    table_oid OID;
BEGIN
    SELECT oid INTO extension_oid FROM pg_catalog.pg_extension WHERE extname = 'polar_advisor';
    FOR table_rec IN
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'polar_advisor' AND table_type = 'BASE TABLE'
    LOOP
        SELECT oid INTO table_oid FROM pg_catalog.pg_class
        WHERE relname = table_rec.table_name AND relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'polar_advisor');

        IF EXISTS (
            SELECT 1 FROM pg_catalog.pg_depend WHERE refobjid = extension_oid AND classid = 'pg_class'::regclass AND objid = table_oid
        ) THEN
            RAISE NOTICE 'Table polar_advisor.% is already a member of the extension', table_rec.table_name;
        ELSE
            RAISE NOTICE 'Add table polar_advisor.% to extension', table_rec.table_name;
            EXECUTE format('ALTER EXTENSION polar_advisor ADD TABLE polar_advisor.%I;', table_rec.table_name);
        END IF;
    END LOOP;
END
$$ LANGUAGE PLpgSQL;

-- Add functions to extension
DO $$
DECLARE
    extension_oid OID;
    proc record;
BEGIN
    SELECT oid INTO extension_oid FROM pg_catalog.pg_extension WHERE extname = 'polar_advisor';

    FOR proc IN
        SELECT p.proname, p.oid FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'polar_advisor'
    LOOP
        IF EXISTS (
            SELECT 1 FROM pg_catalog.pg_depend WHERE refobjid = extension_oid AND classid = 'pg_proc'::regclass AND objid = proc.oid
        ) THEN
            RAISE NOTICE 'Function polar_advisor.% is already a member of the extension', proc.proname;
        ELSE
            RAISE NOTICE 'Add function polar_advisor.% to extension', proc.proname;
            EXECUTE format('ALTER EXTENSION polar_advisor ADD FUNCTION %s;', proc.oid::regprocedure::TEXT);
        END IF;
    END LOOP;
END
$$ LANGUAGE PLpgSQL;

-- Add types to extension
DO $$
DECLARE
    extension_oid OID;
    typ record;
BEGIN
    SELECT oid INTO extension_oid FROM pg_catalog.pg_extension WHERE extname = 'polar_advisor';

    FOR typ IN
        SELECT * FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
            WHERE n.nspname = 'polar_advisor'
    LOOP
        IF EXISTS (
            SELECT 1 FROM pg_catalog.pg_depend WHERE refobjid = extension_oid AND classid = 'pg_type'::regclass AND objid = typ.oid
        ) THEN
            RAISE NOTICE 'Type polar_advisor.% is already a member of the extension', typ.typname;
        ELSE
            RAISE NOTICE 'Add type polar_advisor.% to', typ.typname;
            EXECUTE format('ALTER EXTENSION polar_advisor ADD TYPE polar_advisor.%I;', typ.typname);
        END IF;
    END LOOP;
END
$$ LANGUAGE PLpgSQL;

-- Add sequences to extension
DO $$
DECLARE
    extension_oid OID;
    seq record;
BEGIN
    SELECT oid INTO extension_oid FROM pg_catalog.pg_extension WHERE extname = 'polar_advisor';
    FOR seq IN
        SELECT sequence_name AS seqname FROM information_schema.sequences
        WHERE sequence_schema = 'polar_advisor'
    LOOP
        IF EXISTS (
            SELECT 1 FROM pg_catalog.pg_depend JOIN pg_catalog.pg_class ON pg_class.oid = objid
            WHERE refobjid = extension_oid AND classid = 'pg_class'::regclass AND relkind = 'S' AND relname = seq.seqname
            AND pg_class.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'polar_advisor')
        ) THEN
            RAISE NOTICE 'Sequence polar_advisor.% is already a member of the extension', seq.seqname;
        ELSE
            RAISE NOTICE 'Add sequence polar_advisor.% to extension', seq.seqname;
            EXECUTE format('ALTER EXTENSION polar_advisor ADD SEQUENCE polar_advisor.%I;', seq.seqname);
        END IF;
    END LOOP;
END
$$ LANGUAGE PLpgSQL;


--
-- GRANT permissions for users
--
GRANT USAGE ON SCHEMA polar_advisor TO PUBLIC;
-- To call functions
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA polar_advisor TO PUBLIC;
-- To get window, log, blacklist and whitelist info
GRANT SELECT ON ALL TABLES IN SCHEMA polar_advisor TO PUBLIC;
-- To insert log and the primary key sequence increases
GRANT SELECT, USAGE ON ALL SEQUENCES IN SCHEMA polar_advisor TO PUBLIC;
-- To init, update and delete window info
GRANT INSERT, UPDATE, DELETE ON polar_advisor.advisor_window TO public;
-- To insert, update and delete log
GRANT INSERT, UPDATE, DELETE ON polar_advisor.advisor_log TO public;
GRANT INSERT, UPDATE, DELETE ON polar_advisor.db_level_advisor_log TO public;
-- To set/delete black list
GRANT INSERT, DELETE ON polar_advisor.blacklist_relations TO public;
-- To set/delete white list
GRANT INSERT, DELETE ON polar_advisor.whitelist_relations TO public;
