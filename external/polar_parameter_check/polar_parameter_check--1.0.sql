/* external/polar_parameter_check/polar_parameter_check--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION polar_parameter_check" to load this file. \quit

CREATE FUNCTION polar_parameter_name_check(
            IN parameter_name text,
            OUT parameter_valid bool
)
AS 'MODULE_PATHNAME', 'polar_parameter_name_check'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION polar_parameter_value_check(
            IN parameter_name text,
            IN parameter_value text,
            OUT parameter_valid bool
)
AS 'MODULE_PATHNAME', 'polar_parameter_value_check'
LANGUAGE C PARALLEL SAFE;