/* external/polar_tde_utils/polar_tde_utils--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION polar_tde_utils" to load this file. \quit

CREATE FUNCTION polar_tde_update_kmgr_file (text)
RETURNS bool
AS 'MODULE_PATHNAME','polar_tde_update_kmgr_file'
LANGUAGE C STRICT;

CREATE FUNCTION polar_tde_kmgr_info_view (OUT kmgr_version_no int4,
                                          OUT data_encryption_cipher text, 
                                          OUT rdek_key_hex text,
                                          OUT wdek_key_hex text,
                                          OUT kek_enckey_hex text
)
RETURNS SETOF record
AS 'MODULE_PATHNAME','polar_tde_kmgr_info_view'
LANGUAGE C STRICT;

CREATE FUNCTION polar_tde_check_kmgr_file ()
RETURNS bool
AS 'MODULE_PATHNAME','polar_tde_check_kmgr_file'
LANGUAGE C STRICT;