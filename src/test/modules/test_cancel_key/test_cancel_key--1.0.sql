/* src/test/modules/test_cancel_key/test_cancel_key--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_cancel_key" to load this file. \quit

CREATE FUNCTION test_cancel_key(enable bool)
RETURNS pg_catalog.void STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;
