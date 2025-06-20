-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION masking" to load this file. \quit

create schema if not exists polar_security; 
REVOKE ALL ON SCHEMA polar_security FROM PUBLIC;

create table polar_security.polar_masking_label_tab(labelid INT, relid OID);
CREATE UNIQUE INDEX polar_masking_label_tab_relid_idx ON polar_security.polar_masking_label_tab (relid);
REVOKE ALL ON TABLE polar_security.polar_masking_label_tab FROM PUBLIC;

create table polar_security.polar_masking_label_col(labelid INT, relid OID, colid SMALLINT);
CREATE UNIQUE INDEX polar_masking_label_col_relid_colid_idx ON polar_security.polar_masking_label_col (relid, colid);
REVOKE ALL ON TABLE polar_security.polar_masking_label_col FROM PUBLIC;

create table polar_security.polar_masking_policy(labelid INT, name text, operator SMALLINT);
CREATE UNIQUE INDEX polar_masking_policy_labelid_idx ON polar_security.polar_masking_policy (labelid);
REVOKE ALL ON TABLE polar_security.polar_masking_policy FROM PUBLIC;

CREATE SEQUENCE polar_security.polar_masking_labelid_sequence
    START WITH 1
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 2147483647
    NO CYCLE;
REVOKE ALL ON SEQUENCE polar_security.polar_masking_labelid_sequence FROM PUBLIC;

create or replace function polar_security.creditcardmasking(col text, letter char default 'x') RETURNS text AS $$
declare 
    size INTEGER := 4;
begin
    return CASE WHEN pg_catalog.length(col) >= size THEN
        pg_catalog.REGEXP_REPLACE(pg_catalog.left(col, size*(-1)), '[\d+]', letter, 'g') || pg_catalog.right(col, size)
    ELSE
        col
    end;
end;
$$ LANGUAGE plpgsql;

create or replace function polar_security.basicemailmasking(col text, letter char default 'x') RETURNS text AS $$
declare 
    pos INTEGER := position('@' in col);
begin
    return CASE WHEN pos > 1 THEN
    pg_catalog.repeat(letter, pos - 1) || pg_catalog.substring(col, pos, pg_catalog.length(col) - pos +1)
    ELSE
        col
    end;
end;
$$ LANGUAGE plpgsql;

create or replace function polar_security.fullemailmasking(col text, letter char default 'x') RETURNS text AS $$
declare 
    pos INTEGER := position('@' in col);
    dot_pos INTEGER := pg_catalog.length(col) - position('.' in pg_catalog.reverse(col)) + 1;
    d_pos INTEGER := position('.' in pg_catalog.reverse(col));
begin
    return CASE WHEN pos > 0 and dot_pos > pos and d_pos > 0 THEN
    pg_catalog.repeat(letter, pos - 1) || '@' || pg_catalog.repeat(letter,  dot_pos - pos - 1) || pg_catalog.substring(col, dot_pos, pg_catalog.length(col) - dot_pos +1)
    WHEN pos = 0 and d_pos > 0 THEN
    pg_catalog.repeat(letter,  dot_pos - 1) || pg_catalog.substring(col, dot_pos, pg_catalog.length(col) - dot_pos +1)
    ELSE
        col
    end;
end;
$$ LANGUAGE plpgsql;

create or replace function polar_security.alldigitsmasking(col text, letter char default '0') RETURNS text AS $$
begin
    return pg_catalog.REGEXP_REPLACE(col, '[\d+]', letter, 'g');
end;
$$ LANGUAGE plpgsql;

create or replace function polar_security.shufflemasking(col text) RETURNS text AS $$
declare 
    rd INTEGER;
    size INTEGER := pg_catalog.length(col);
    tmp text := col;
    res text := '';
begin
    while size > 0 loop 
        rd := pg_catalog.floor(pg_catalog.random() * pg_catalog.length(tmp) + 1);
        res := res || pg_catalog.right(pg_catalog.left(tmp, rd), 1);
        tmp := pg_catalog.left(tmp, rd - 1) || pg_catalog.right(tmp, pg_catalog.length(tmp) - rd);
        size := size - 1;
    END loop;
    return res;
end;
$$ LANGUAGE plpgsql;

create or replace function polar_security.randommasking(col text) RETURNS text AS $$	
begin
    return pg_catalog.left(pg_catalog.MD5(pg_catalog.random()::text), pg_catalog.length(col));
end;
$$ LANGUAGE plpgsql;

create or replace function polar_security.maskall(col text, letter char default 'x') RETURNS text AS $$
declare 
    size INTEGER := pg_catalog.length(col);
begin
    return pg_catalog.repeat(letter, size);
end;
$$ LANGUAGE plpgsql;
