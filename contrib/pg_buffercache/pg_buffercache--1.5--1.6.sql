/* contrib/pg_buffercache/pg_buffercache--1.5--1.6.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION pg_buffercache UPDATE TO '1.6'" to load this file. \quit

DROP FUNCTION pg_buffercache_evict(integer);
CREATE FUNCTION pg_buffercache_evict(
    IN int,
    OUT buffer_evicted boolean,
    OUT buffer_flushed boolean)
AS 'MODULE_PATHNAME', 'pg_buffercache_evict'
LANGUAGE C PARALLEL SAFE VOLATILE STRICT;

CREATE FUNCTION pg_buffercache_evict_relation(
    IN regclass,
    OUT buffers_evicted int4,
    OUT buffers_flushed int4,
    OUT buffers_skipped int4)
AS 'MODULE_PATHNAME', 'pg_buffercache_evict_relation'
LANGUAGE C PARALLEL SAFE VOLATILE STRICT;

CREATE FUNCTION pg_buffercache_evict_all(
    OUT buffers_evicted int4,
    OUT buffers_flushed int4,
    OUT buffers_skipped int4)
AS 'MODULE_PATHNAME', 'pg_buffercache_evict_all'
LANGUAGE C PARALLEL SAFE VOLATILE;

-- Functions to mark buffers as dirty.
CREATE FUNCTION pg_buffercache_mark_dirty(
    IN int,
    OUT buffer_dirtied boolean,
    OUT buffer_already_dirty boolean)
AS 'MODULE_PATHNAME', 'pg_buffercache_mark_dirty'
LANGUAGE C PARALLEL SAFE VOLATILE STRICT;

CREATE FUNCTION pg_buffercache_mark_dirty_relation(
    IN regclass,
    OUT buffers_dirtied int4,
    OUT buffers_already_dirty int4,
    OUT buffers_skipped int4)
AS 'MODULE_PATHNAME', 'pg_buffercache_mark_dirty_relation'
LANGUAGE C PARALLEL SAFE VOLATILE STRICT;

CREATE FUNCTION pg_buffercache_mark_dirty_all(
    OUT buffers_dirtied int4,
    OUT buffers_already_dirty int4,
    OUT buffers_skipped int4)
AS 'MODULE_PATHNAME', 'pg_buffercache_mark_dirty_all'
LANGUAGE C PARALLEL SAFE VOLATILE;
