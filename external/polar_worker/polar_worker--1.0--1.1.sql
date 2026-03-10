/* external/polar_worker/polar_worker--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION polar_worker" to load this file. \quit

CREATE OR REPLACE FUNCTION polar_compression_info(
	sub_directory TEXT DEFAULT 'base',
	compression_method TEXT DEFAULT 'lz4',
	unit_size INT DEFAULT 4096,
	unit_count INT DEFAULT 4,
	step_factor INT DEFAULT 10,
	fit_nblocks INT DEFAULT 0,
	lowerbound INT DEFAULT 30,
	extra_clen BIGINT DEFAULT 0,
	extra_slen BIGINT DEFAULT 0,
	ignore_size BIGINT DEFAULT 4194304,
	ignore_catalog_heap BOOLEAN DEFAULT TRUE,
	ignore_extended_heap BOOLEAN DEFAULT TRUE)
RETURNS SETOF RECORD
AS 'MODULE_PATHNAME', 'polar_compression_info'
LANGUAGE C PARALLEL SAFE;

CREATE OR REPLACE FUNCTION polar_compression_ratio(
	sub_directory TEXT DEFAULT 'base',
	compression_method TEXT DEFAULT 'lz4',
	unit_size INT DEFAULT 4096,
	unit_count INT DEFAULT 4,
	step_factor INT DEFAULT 10,
	fit_nblocks INT DEFAULT 0,
	lowerbound INT DEFAULT 30,
	extra_clen BIGINT DEFAULT 0,
	extra_slen BIGINT DEFAULT 0,
	ignore_size BIGINT DEFAULT 4194304,
	ignore_catalog_heap BOOLEAN DEFAULT TRUE,
	ignore_extended_heap BOOLEAN DEFAULT TRUE)
RETURNS FLOAT8
AS 'SELECT P.compression_ratio
	FROM polar_compression_info($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	AS P(original_size BIGINT, compression_size BIGINT, compression_ratio FLOAT8)'
LANGUAGE SQL VOLATILE STRICT;

CREATE VIEW polar_compression_info AS
	SELECT P.* FROM polar_compression_info() AS P
	(original_size BIGINT, compression_size BIGINT, compression_ratio FLOAT8);
