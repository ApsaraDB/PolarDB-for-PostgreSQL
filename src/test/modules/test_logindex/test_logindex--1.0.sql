/* src/test/modules/test_logindex/test_logindex--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_logindex" to load this file. \quit

CREATE FUNCTION test_bitpos()
RETURNS pg_catalog.void STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_ringbuf()
RETURNS pg_catalog.void STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_mini_trans()
RETURNS pg_catalog.void STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_logindex()
RETURNS pg_catalog.void STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_fullpage()
RETURNS pg_catalog.void STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_polar_rel_size_cache()
RETURNS pg_catalog.void STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;
