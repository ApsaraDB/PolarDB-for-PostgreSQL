-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION polar_masking" to load this file. \quit

CREATE SCHEMA if NOT EXISTS polar_masking; 

CREATE TABLE polar_masking.polar_masking_label_tab(labelid INT, relid OID);
CREATE UNIQUE INDEX polar_masking_label_tab_relid_idx ON polar_masking.polar_masking_label_tab (relid);

CREATE TABLE polar_masking.polar_masking_label_col(labelid INT, relid OID, colid SMALLINT);
CREATE UNIQUE INDEX polar_masking_label_col_relid_colid_idx ON polar_masking.polar_masking_label_col (relid, colid);

CREATE TABLE polar_masking.polar_masking_policy(labelid INT, name TEXT, operator SMALLINT);
CREATE UNIQUE INDEX polar_masking_policy_labelid_idx ON polar_masking.polar_masking_policy (labelid);

CREATE SEQUENCE polar_masking.polar_masking_labelid_sequence
    START WITH 1
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 2147483647
    NO CYCLE;

CREATE FUNCTION polar_masking.polar_masking_create_label(label_name text) RETURNS VOID
VOLATILE LANGUAGE C AS 'MODULE_PATHNAME', 'polar_masking_create_label';

CREATE FUNCTION polar_masking.polar_masking_drop_label(label_name text) RETURNS VOID
VOLATILE LANGUAGE C AS 'MODULE_PATHNAME', 'polar_masking_drop_label';
