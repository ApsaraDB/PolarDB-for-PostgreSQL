/* src/test/modules/test_multi_version_snapshot/test_multi_version_snapshot--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_multi_version_snapshot" to load this file. \quit

CREATE FUNCTION test_multi_version_snapshot()
RETURNS pg_catalog.void STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;