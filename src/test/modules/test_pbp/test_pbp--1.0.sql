/* src/test/modules/test_buffer/test_pbp--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_pbp" to load this file. \quit

CREATE FUNCTION test_pbp()
RETURNS pg_catalog.void STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;
