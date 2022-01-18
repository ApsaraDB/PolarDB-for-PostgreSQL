/* src/test/modules/test_read_core_pattern/test_read_core_pattern--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_read_core_pattern" to load this file. \quit

CREATE FUNCTION test_read_core_pattern()
RETURNS pg_catalog.void STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;
