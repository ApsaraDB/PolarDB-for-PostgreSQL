-- Create our own copies of these test tables in our own
-- schema (see qp_functions_in_contexts_setup.sql)
/*--EXPLAIN_QUERY_BEGIN*/
CREATE SCHEMA qp_funcs_in_with;
set search_path='qp_funcs_in_with', 'qp_funcs_in_contexts';

CREATE TABLE foo (a int, b int);
INSERT INTO foo select i, i+1 from generate_series(1,10) i;
CREATE TABLE bar (c int, d int);
INSERT INTO bar select i, i+1 from generate_series(1,10) i;

-- @description function_in_with_0.sql
WITH v(a, b) AS (SELECT func1_nosql_vol(a), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_1.sql
WITH v(a, b) AS (SELECT func1_nosql_stb(a), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_2.sql
WITH v(a, b) AS (SELECT func1_nosql_imm(a), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_3.sql
WITH v(a, b) AS (SELECT func1_sql_int_vol(a), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_4.sql
WITH v(a, b) AS (SELECT func1_sql_int_stb(a), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_5.sql
WITH v(a, b) AS (SELECT func1_sql_int_imm(a), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_6.sql
WITH v(a, b) AS (SELECT func1_sql_setint_vol(a), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_7.sql
WITH v(a, b) AS (SELECT func1_sql_setint_stb(a), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_8.sql
WITH v(a, b) AS (SELECT func1_sql_setint_imm(a), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_11.sql
WITH v(a, b) AS (SELECT func1_read_setint_sql_vol(a), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_12.sql
WITH v(a, b) AS (SELECT func1_read_setint_sql_stb(a), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_15.sql
begin;
WITH v(a, b) AS (SELECT func1_mod_setint_vol(a), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;
rollback;

-- @description function_in_with_16.sql
begin;
WITH v(a, b) AS (SELECT func1_mod_setint_stb(a), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;
rollback;

-- @description function_in_with_withfunc2_0.sql
WITH v(a, b) AS (SELECT func1_nosql_vol(func2_nosql_vol(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_1.sql
WITH v(a, b) AS (SELECT func1_nosql_vol(func2_nosql_stb(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_2.sql
WITH v(a, b) AS (SELECT func1_nosql_vol(func2_nosql_imm(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_3.sql
WITH v(a, b) AS (SELECT func1_nosql_vol(func2_sql_int_vol(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_4.sql
WITH v(a, b) AS (SELECT func1_nosql_vol(func2_sql_int_stb(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_5.sql
WITH v(a, b) AS (SELECT func1_nosql_vol(func2_sql_int_imm(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_10.sql
WITH v(a, b) AS (SELECT func1_nosql_stb(func2_nosql_vol(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_11.sql
WITH v(a, b) AS (SELECT func1_nosql_stb(func2_nosql_stb(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_12.sql
WITH v(a, b) AS (SELECT func1_nosql_stb(func2_nosql_imm(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_13.sql
WITH v(a, b) AS (SELECT func1_nosql_stb(func2_sql_int_vol(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_14.sql
WITH v(a, b) AS (SELECT func1_nosql_stb(func2_sql_int_stb(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_15.sql
WITH v(a, b) AS (SELECT func1_nosql_stb(func2_sql_int_imm(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_20.sql
WITH v(a, b) AS (SELECT func1_nosql_imm(func2_nosql_vol(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_21.sql
WITH v(a, b) AS (SELECT func1_nosql_imm(func2_nosql_stb(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_22.sql
WITH v(a, b) AS (SELECT func1_nosql_imm(func2_nosql_imm(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_23.sql
WITH v(a, b) AS (SELECT func1_nosql_imm(func2_sql_int_vol(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_24.sql
WITH v(a, b) AS (SELECT func1_nosql_imm(func2_sql_int_stb(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_25.sql
WITH v(a, b) AS (SELECT func1_nosql_imm(func2_sql_int_imm(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_30.sql
WITH v(a, b) AS (SELECT func1_sql_int_vol(func2_nosql_vol(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_31.sql
WITH v(a, b) AS (SELECT func1_sql_int_vol(func2_nosql_stb(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_32.sql
WITH v(a, b) AS (SELECT func1_sql_int_vol(func2_nosql_imm(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_33.sql
WITH v(a, b) AS (SELECT func1_sql_int_vol(func2_sql_int_vol(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_34.sql
WITH v(a, b) AS (SELECT func1_sql_int_vol(func2_sql_int_stb(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_35.sql
WITH v(a, b) AS (SELECT func1_sql_int_vol(func2_sql_int_imm(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_40.sql
WITH v(a, b) AS (SELECT func1_sql_int_stb(func2_nosql_vol(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_41.sql
WITH v(a, b) AS (SELECT func1_sql_int_stb(func2_nosql_stb(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_42.sql
WITH v(a, b) AS (SELECT func1_sql_int_stb(func2_nosql_imm(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_43.sql
WITH v(a, b) AS (SELECT func1_sql_int_stb(func2_sql_int_vol(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_44.sql
WITH v(a, b) AS (SELECT func1_sql_int_stb(func2_sql_int_stb(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_45.sql
WITH v(a, b) AS (SELECT func1_sql_int_stb(func2_sql_int_imm(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_50.sql
WITH v(a, b) AS (SELECT func1_sql_int_imm(func2_nosql_vol(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_51.sql
WITH v(a, b) AS (SELECT func1_sql_int_imm(func2_nosql_stb(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_52.sql
WITH v(a, b) AS (SELECT func1_sql_int_imm(func2_nosql_imm(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_53.sql
WITH v(a, b) AS (SELECT func1_sql_int_imm(func2_sql_int_vol(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_54.sql
WITH v(a, b) AS (SELECT func1_sql_int_imm(func2_sql_int_stb(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_55.sql
WITH v(a, b) AS (SELECT func1_sql_int_imm(func2_sql_int_imm(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_60.sql
WITH v(a, b) AS (SELECT func1_sql_setint_vol(func2_nosql_vol(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_61.sql
WITH v(a, b) AS (SELECT func1_sql_setint_vol(func2_nosql_stb(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_62.sql
WITH v(a, b) AS (SELECT func1_sql_setint_vol(func2_nosql_imm(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_63.sql
WITH v(a, b) AS (SELECT func1_sql_setint_vol(func2_sql_int_vol(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_64.sql
WITH v(a, b) AS (SELECT func1_sql_setint_vol(func2_sql_int_stb(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_65.sql
WITH v(a, b) AS (SELECT func1_sql_setint_vol(func2_sql_int_imm(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_70.sql
WITH v(a, b) AS (SELECT func1_sql_setint_stb(func2_nosql_vol(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_71.sql
WITH v(a, b) AS (SELECT func1_sql_setint_stb(func2_nosql_stb(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_72.sql
WITH v(a, b) AS (SELECT func1_sql_setint_stb(func2_nosql_imm(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_73.sql
WITH v(a, b) AS (SELECT func1_sql_setint_stb(func2_sql_int_vol(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_74.sql
WITH v(a, b) AS (SELECT func1_sql_setint_stb(func2_sql_int_stb(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_75.sql
WITH v(a, b) AS (SELECT func1_sql_setint_stb(func2_sql_int_imm(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_80.sql
WITH v(a, b) AS (SELECT func1_sql_setint_imm(func2_nosql_vol(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_81.sql
WITH v(a, b) AS (SELECT func1_sql_setint_imm(func2_nosql_stb(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_82.sql
WITH v(a, b) AS (SELECT func1_sql_setint_imm(func2_nosql_imm(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_83.sql
WITH v(a, b) AS (SELECT func1_sql_setint_imm(func2_sql_int_vol(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_84.sql
WITH v(a, b) AS (SELECT func1_sql_setint_imm(func2_sql_int_stb(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_85.sql
WITH v(a, b) AS (SELECT func1_sql_setint_imm(func2_sql_int_imm(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_110.sql
WITH v(a, b) AS (SELECT func1_read_setint_sql_vol(func2_nosql_vol(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_111.sql
WITH v(a, b) AS (SELECT func1_read_setint_sql_vol(func2_nosql_stb(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_112.sql
WITH v(a, b) AS (SELECT func1_read_setint_sql_vol(func2_nosql_imm(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_113.sql
WITH v(a, b) AS (SELECT func1_read_setint_sql_vol(func2_sql_int_vol(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_114.sql
WITH v(a, b) AS (SELECT func1_read_setint_sql_vol(func2_sql_int_stb(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_115.sql
WITH v(a, b) AS (SELECT func1_read_setint_sql_vol(func2_sql_int_imm(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_120.sql
WITH v(a, b) AS (SELECT func1_read_setint_sql_stb(func2_nosql_vol(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_121.sql
WITH v(a, b) AS (SELECT func1_read_setint_sql_stb(func2_nosql_stb(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_122.sql
WITH v(a, b) AS (SELECT func1_read_setint_sql_stb(func2_nosql_imm(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_123.sql
WITH v(a, b) AS (SELECT func1_read_setint_sql_stb(func2_sql_int_vol(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_124.sql
WITH v(a, b) AS (SELECT func1_read_setint_sql_stb(func2_sql_int_stb(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_125.sql
WITH v(a, b) AS (SELECT func1_read_setint_sql_stb(func2_sql_int_imm(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;  

-- @description function_in_with_withfunc2_150.sql
begin;
WITH v(a, b) AS (SELECT func1_mod_setint_vol(func2_nosql_vol(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;
rollback;

-- @description function_in_with_withfunc2_151.sql
begin;
WITH v(a, b) AS (SELECT func1_mod_setint_vol(func2_nosql_stb(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;
rollback;

-- @description function_in_with_withfunc2_152.sql
begin;
WITH v(a, b) AS (SELECT func1_mod_setint_vol(func2_nosql_imm(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;
rollback;

-- @description function_in_with_withfunc2_153.sql
begin;
WITH v(a, b) AS (SELECT func1_mod_setint_vol(func2_sql_int_vol(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;
rollback;

-- @description function_in_with_withfunc2_154.sql
begin;
WITH v(a, b) AS (SELECT func1_mod_setint_vol(func2_sql_int_stb(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;
rollback;

-- @description function_in_with_withfunc2_155.sql
begin;
WITH v(a, b) AS (SELECT func1_mod_setint_vol(func2_sql_int_imm(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;
rollback;

-- @description function_in_with_withfunc2_160.sql
begin;
WITH v(a, b) AS (SELECT func1_mod_setint_stb(func2_nosql_vol(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;
rollback;

-- @description function_in_with_withfunc2_161.sql
begin;
WITH v(a, b) AS (SELECT func1_mod_setint_stb(func2_nosql_stb(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;
rollback;

-- @description function_in_with_withfunc2_162.sql
begin;
WITH v(a, b) AS (SELECT func1_mod_setint_stb(func2_nosql_imm(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;
rollback;

-- @description function_in_with_withfunc2_163.sql
begin;
WITH v(a, b) AS (SELECT func1_mod_setint_stb(func2_sql_int_vol(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;
rollback;

-- @description function_in_with_withfunc2_164.sql
begin;
WITH v(a, b) AS (SELECT func1_mod_setint_stb(func2_sql_int_stb(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;
rollback;

-- @description function_in_with_withfunc2_165.sql
begin;
WITH v(a, b) AS (SELECT func1_mod_setint_stb(func2_sql_int_imm(a)), b FROM foo WHERE b < 5) SELECT v1.a, v2.b FROM v AS v1, v AS v2 WHERE v1.a < v2.a order by v1.a, v2.b;
rollback;
