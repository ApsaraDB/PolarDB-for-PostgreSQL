-- ALTER FUNCTION XXX PARALLEL SAFE

ALTER FUNCTION bit_posite(varbit,int,bool) PARALLEL SAFE;
ALTER FUNCTION bit_posite(varbit,int,int,bool) PARALLEL SAFE;
ALTER FUNCTION get_bit(varbit,int,int) PARALLEL SAFE;
ALTER FUNCTION set_bit_array(varbit,int,int,int[]) PARALLEL SAFE;
ALTER FUNCTION bit_count(varbit,int,int,int) PARALLEL SAFE;
ALTER FUNCTION bit_count(varbit,int) PARALLEL SAFE;
ALTER FUNCTION bit_fill(int,int) PARALLEL SAFE;
ALTER FUNCTION get_bit_array(varbit,int,int,int) PARALLEL SAFE;
ALTER FUNCTION get_bit_array(varbit,int,int[]) PARALLEL SAFE;
ALTER FUNCTION set_bit_array(varbit,int,int,int[],int) PARALLEL SAFE;
ALTER FUNCTION set_bit_array_record(IN in_varbit varbit,
						IN targetbit int,
						IN forcebit int,
						IN in_setarray int[],
						OUT out_varbit varbit,
						OUT out_setarray int[]) PARALLEL SAFE;
ALTER FUNCTION set_bit_array_record(IN in_varbit varbit,
						IN targetbit int,
						IN forcebit int,
						IN in_setarray int[],
						IN count int,
						OUT out_varbit varbit,
						OUT out_setarray int[]) PARALLEL SAFE;
ALTER FUNCTION bit_count_array(varbit,int,int[]) PARALLEL SAFE;