/* src/test/modules/test_logindex/test_logindex--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_page_outdate" to load this file. \quit

CREATE FUNCTION test_page_outdate()
RETURNS pg_catalog.void STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;
