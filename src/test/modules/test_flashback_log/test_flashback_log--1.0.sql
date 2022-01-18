/* src/test/modules/test_flashback_log/test_flashback_log--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_flashback_log" to load this file. \quit

CREATE FUNCTION test_flashback_log()
RETURNS pg_catalog.void STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;