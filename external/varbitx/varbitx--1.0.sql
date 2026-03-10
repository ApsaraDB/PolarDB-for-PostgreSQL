/* external/varbitx/varbitx--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION varbitx" to load this file. \quit

create or replace function bit_posite(varbit,int,bool) returns int[] as 'MODULE_PATHNAME', 'bitpositebit' language C STRICT;
create or replace function bit_posite(varbit,int,int,bool) returns int[] as 'MODULE_PATHNAME', 'bitpositebit' language C STRICT;
create or replace function get_bit(varbit,int,int) returns varbit as 'MODULE_PATHNAME', 'bitgetbit_array' language C STRICT;
create or replace function set_bit_array(varbit,int,int,int[]) returns varbit as 'MODULE_PATHNAME', 'bitsetbit_array' language C STRICT;
create or replace function bit_count(varbit,int,int,int) returns int as 'MODULE_PATHNAME', 'bitcountbit' language C STRICT;
create or replace function bit_count(varbit,int) returns int as 'MODULE_PATHNAME', 'bitcountbit' language C STRICT;
create or replace function bit_fill(int,int) returns varbit as 'MODULE_PATHNAME', 'bitfillbit' language C STRICT;

-- improvement
create or replace function get_bit_array(varbit,int,int,int) returns int[] as 'MODULE_PATHNAME', 'get_bit_array' language C STRICT;
create or replace function get_bit_array(varbit,int,int[]) returns int[] as 'MODULE_PATHNAME', 'get_bit_array' language C STRICT;
create or replace function set_bit_array(varbit,int,int,int[],int) returns varbit as 'MODULE_PATHNAME', 'bitsetbit_array' language C STRICT;
create or replace function set_bit_array_record(IN in_varbit varbit,
						IN targetbit int,
						IN forcebit int,
						IN in_setarray int[],
						OUT out_varbit varbit,
						OUT out_setarray int[])
	RETURNS SETOF record
	as 'MODULE_PATHNAME', 'bitsetbit_array_rec' language C STRICT;
create or replace function set_bit_array_record(IN in_varbit varbit,
						IN targetbit int,
						IN forcebit int,
						IN in_setarray int[],
						IN count int,
						OUT out_varbit varbit,
						OUT out_setarray int[])
	RETURNS SETOF record
	as 'MODULE_PATHNAME', 'bitsetbit_array_rec' language C STRICT;
create or replace function bit_count_array(varbit,int,int[]) returns int as 'MODULE_PATHNAME', 'bitcountarray' language C STRICT;
