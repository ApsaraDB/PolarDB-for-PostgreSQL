

--bug px insert oid table
-- config
/*--POLAR_ENABLE_PX*/
/*--EXPLAIN_QUERY_BEGIN*/
set polar_enable_px = 1;
set polar_px_enable_insert_select = 1;
set polar_px_optimizer_enable_dml_constraints = 1;
set polar_px_enable_insert_order_sensitive = 0;

CREATE TABLE t1_withoids (
    c1 integer,
    c2 integer
) with oids;

CREATE TABLE t1_withoutoids (
    c1 integer,
    c2 integer
) without oids;


CREATE TABLE t2_withoids (
    pk numeric(10,0) NOT NULL,
    varchar_test character varying(255) DEFAULT NULL::character varying,
    integer_test integer,
    char_test character varying(255) DEFAULT NULL::character varying,
    tinyint_test integer,
    tinyint_1bit_test integer,
    smallint_test integer,
    mediumint_test integer,
    bigint_test numeric(10,0) DEFAULT NULL::numeric,
    double_test double precision,
    decimal_test numeric(10,0) DEFAULT NULL::numeric,
    date_test timestamp(6) without time zone DEFAULT NULL::timestamp without time zone,
    time_test timestamp(6) without time zone DEFAULT NULL::timestamp without time zone,
    datetime_test timestamp(6) without time zone DEFAULT NULL::timestamp without time zone,
    timestamp_test timestamp(6) without time zone DEFAULT NULL::timestamp without time zone,
    year_test integer
) with oids;

CREATE TABLE t2_withoutoids (
    pk numeric(10,0) NOT NULL,
    varchar_test character varying(255) DEFAULT NULL::character varying,
    integer_test integer,
    char_test character varying(255) DEFAULT NULL::character varying,
    tinyint_test integer,
    tinyint_1bit_test integer,
    smallint_test integer,
    mediumint_test integer,
    bigint_test numeric(10,0) DEFAULT NULL::numeric,
    double_test double precision,
    decimal_test numeric(10,0) DEFAULT NULL::numeric,
    date_test timestamp(6) without time zone DEFAULT NULL::timestamp without time zone,
    time_test timestamp(6) without time zone DEFAULT NULL::timestamp without time zone,
    datetime_test timestamp(6) without time zone DEFAULT NULL::timestamp without time zone,
    timestamp_test timestamp(6) without time zone DEFAULT NULL::timestamp without time zone,
    year_test integer
) without oids;

--
-- Data for Name: select_one; Type: TABLE DATA; Schema: public; Owner: zx_test
--

COPY t2_withoids (pk, varchar_test, integer_test, char_test, tinyint_test, tinyint_1bit_test, smallint_test, mediumint_test, bigint_test, double_test, decimal_test, date_test, time_test, datetime_test, timestamp_test, year_test) FROM stdin;
1	word23	12	word23	14	1	7	11	10	35.1477999999999966	1000	2028-12-16 13:45:00	2028-12-16 13:45:00	2028-12-16 13:45:00	2028-12-16 13:45:00	2012
2	feed32feed	13	feed32feed	15	1	8	12	11	38.4879000000000033	10000	2019-01-15 07:45:09	2019-01-15 07:45:09	2019-01-15 07:45:09	2019-01-15 07:45:09	2013
3	nihaore	14	nihaore	16	1	9	13	12	40.4784500000000023	100000	2018-08-15 07:45:09	2018-08-15 07:45:09	2018-08-15 07:45:09	2018-08-15 07:45:09	2014
4	afdaewer	15	afdaewer	17	1	10	14	13	48.1477999999999966	1000000	2012-08-20 01:45:09	2012-08-20 01:45:09	2012-08-20 01:45:09	2012-08-20 01:45:09	2015
5	hellorew	16	hellorew	18	1	11	15	14	50.4874500000000026	10000000	2010-08-25 01:40:09	2010-08-25 01:40:09	2010-08-25 01:40:09	2010-08-25 01:40:09	2016
6	abdfeed	17	abdfeed	19	1	12	16	15	55.1477999999999966	100000000	2020-12-18 13:45:00	2020-12-18 13:45:00	2020-12-18 13:45:00	2020-12-18 13:45:00	2017
7	cdefeed	18	cdefeed	20	1	13	17	16	58.1244999999999976	100000000	2008-12-08 13:45:00	2008-12-08 13:45:00	2008-12-08 13:45:00	2008-12-08 13:45:00	2009
8	adaabcwer	19	adaabcwer	21	1	14	18	17	80.457800000000006	1000000000	2016-12-15 14:45:09	2016-12-15 14:45:09	2016-12-15 14:45:09	2016-12-15 14:45:09	2007
9	\N	20	afsabcabcd	22	1	15	19	18	90.4587446999999969	10	2028-12-16 13:45:00	2028-12-16 13:45:00	2028-12-16 13:45:00	2028-12-16 13:45:00	2010
10	sfdeiekd	21	sfdeiekd	23	1	16	20	19	180.457844999999992	100	2019-01-15 07:45:09	2019-01-15 07:45:09	2019-01-15 07:45:09	2019-01-15 07:45:09	2011
11	einoejk	22	einoejk	24	1	17	21	20	200.487869999999987	1000	2018-08-15 07:45:09	2018-08-15 07:45:09	2018-08-15 07:45:09	2018-08-15 07:45:09	2012
12	kisfe	23	kisfe	25	1	18	22	21	250.487400000000008	10000	2012-08-20 01:45:09	2012-08-20 01:45:09	2012-08-20 01:45:09	2012-08-20 01:45:09	2013
13	safdwe	24	safdwe	26	1	19	23	22	301.456999999999994	100000	2010-08-25 01:40:09	2010-08-25 01:40:09	2010-08-25 01:40:09	2010-08-25 01:40:09	2014
14	zhuoXUE	25	hello1234	27	1	20	24	23	800.147000000000048	1000000	2020-12-18 13:45:00	2020-12-18 13:45:00	2020-12-18 13:45:00	2020-12-18 13:45:00	2015
15	zhuoxue_yll	26	hello1234	28	1	21	25	24	1110.47469999999998	10000000	2008-12-08 13:45:00	2008-12-08 13:45:00	2008-12-08 13:45:00	2008-12-08 13:45:00	2016
16	zhuoxue%yll	27	he343243	29	1	22	26	25	1414.14747000000011	100000000	2016-12-15 14:45:09	2016-12-15 14:45:09	2016-12-15 14:45:09	2016-12-15 14:45:09	2017
17	hello1234	28	word23	30	1	23	27	26	1825.47484000000009	100000000	2028-12-16 13:45:00	2028-12-16 13:45:00	2028-12-16 13:45:00	2028-12-16 13:45:00	2009
18	hello1234	29	feed32feed	31	1	24	28	27	2000.23232000000007	1000000000	2019-01-15 07:45:09	2019-01-15 07:45:09	2019-01-15 07:45:09	2019-01-15 07:45:09	2007
19	\N	30	nihaore	32	1	25	29	28	10.2144999999999992	10	2018-08-15 07:45:09	2018-08-15 07:45:09	2018-08-15 07:45:09	2018-08-15 07:45:09	2010
20	word23	31	afdaewer	33	1	26	30	29	21.2579999999999991	100	2012-08-20 01:45:09	2012-08-20 01:45:09	2012-08-20 01:45:09	2012-08-20 01:45:09	2011
21	feed32feed	32	hellorew	34	1	27	31	30	35.1477999999999966	1000	2010-08-25 01:40:09	2010-08-25 01:40:09	2010-08-25 01:40:09	2010-08-25 01:40:09	2012
\.

COPY t2_withoutoids (pk, varchar_test, integer_test, char_test, tinyint_test, tinyint_1bit_test, smallint_test, mediumint_test, bigint_test, double_test, decimal_test, date_test, time_test, datetime_test, timestamp_test, year_test) FROM stdin;
1	word23	12	word23	14	1	7	11	10	35.1477999999999966	1000	2028-12-16 13:45:00	2028-12-16 13:45:00	2028-12-16 13:45:00	2028-12-16 13:45:00	2012
2	feed32feed	13	feed32feed	15	1	8	12	11	38.4879000000000033	10000	2019-01-15 07:45:09	2019-01-15 07:45:09	2019-01-15 07:45:09	2019-01-15 07:45:09	2013
3	nihaore	14	nihaore	16	1	9	13	12	40.4784500000000023	100000	2018-08-15 07:45:09	2018-08-15 07:45:09	2018-08-15 07:45:09	2018-08-15 07:45:09	2014
4	afdaewer	15	afdaewer	17	1	10	14	13	48.1477999999999966	1000000	2012-08-20 01:45:09	2012-08-20 01:45:09	2012-08-20 01:45:09	2012-08-20 01:45:09	2015
5	hellorew	16	hellorew	18	1	11	15	14	50.4874500000000026	10000000	2010-08-25 01:40:09	2010-08-25 01:40:09	2010-08-25 01:40:09	2010-08-25 01:40:09	2016
6	abdfeed	17	abdfeed	19	1	12	16	15	55.1477999999999966	100000000	2020-12-18 13:45:00	2020-12-18 13:45:00	2020-12-18 13:45:00	2020-12-18 13:45:00	2017
7	cdefeed	18	cdefeed	20	1	13	17	16	58.1244999999999976	100000000	2008-12-08 13:45:00	2008-12-08 13:45:00	2008-12-08 13:45:00	2008-12-08 13:45:00	2009
8	adaabcwer	19	adaabcwer	21	1	14	18	17	80.457800000000006	1000000000	2016-12-15 14:45:09	2016-12-15 14:45:09	2016-12-15 14:45:09	2016-12-15 14:45:09	2007
9	\N	20	afsabcabcd	22	1	15	19	18	90.4587446999999969	10	2028-12-16 13:45:00	2028-12-16 13:45:00	2028-12-16 13:45:00	2028-12-16 13:45:00	2010
10	sfdeiekd	21	sfdeiekd	23	1	16	20	19	180.457844999999992	100	2019-01-15 07:45:09	2019-01-15 07:45:09	2019-01-15 07:45:09	2019-01-15 07:45:09	2011
11	einoejk	22	einoejk	24	1	17	21	20	200.487869999999987	1000	2018-08-15 07:45:09	2018-08-15 07:45:09	2018-08-15 07:45:09	2018-08-15 07:45:09	2012
12	kisfe	23	kisfe	25	1	18	22	21	250.487400000000008	10000	2012-08-20 01:45:09	2012-08-20 01:45:09	2012-08-20 01:45:09	2012-08-20 01:45:09	2013
13	safdwe	24	safdwe	26	1	19	23	22	301.456999999999994	100000	2010-08-25 01:40:09	2010-08-25 01:40:09	2010-08-25 01:40:09	2010-08-25 01:40:09	2014
14	zhuoXUE	25	hello1234	27	1	20	24	23	800.147000000000048	1000000	2020-12-18 13:45:00	2020-12-18 13:45:00	2020-12-18 13:45:00	2020-12-18 13:45:00	2015
15	zhuoxue_yll	26	hello1234	28	1	21	25	24	1110.47469999999998	10000000	2008-12-08 13:45:00	2008-12-08 13:45:00	2008-12-08 13:45:00	2008-12-08 13:45:00	2016
16	zhuoxue%yll	27	he343243	29	1	22	26	25	1414.14747000000011	100000000	2016-12-15 14:45:09	2016-12-15 14:45:09	2016-12-15 14:45:09	2016-12-15 14:45:09	2017
17	hello1234	28	word23	30	1	23	27	26	1825.47484000000009	100000000	2028-12-16 13:45:00	2028-12-16 13:45:00	2028-12-16 13:45:00	2028-12-16 13:45:00	2009
18	hello1234	29	feed32feed	31	1	24	28	27	2000.23232000000007	1000000000	2019-01-15 07:45:09	2019-01-15 07:45:09	2019-01-15 07:45:09	2019-01-15 07:45:09	2007
19	\N	30	nihaore	32	1	25	29	28	10.2144999999999992	10	2018-08-15 07:45:09	2018-08-15 07:45:09	2018-08-15 07:45:09	2018-08-15 07:45:09	2010
20	word23	31	afdaewer	33	1	26	30	29	21.2579999999999991	100	2012-08-20 01:45:09	2012-08-20 01:45:09	2012-08-20 01:45:09	2012-08-20 01:45:09	2011
21	feed32feed	32	hellorew	34	1	27	31	30	35.1477999999999966	1000	2010-08-25 01:40:09	2010-08-25 01:40:09	2010-08-25 01:40:09	2010-08-25 01:40:09	2012
\.

explain (verbose, costs off)
insert into t1_withoids  select integer_test, tinyint_test from t2_withoids order by integer_test desc,tinyint_test;
insert into t1_withoids  select integer_test, tinyint_test from t2_withoids order by integer_test desc,tinyint_test;
select * from t1_withoids order by c1, c2;

truncate t1_withoids;
explain (verbose, costs off)
insert into t1_withoids  select integer_test, tinyint_test from t2_withoutoids order by integer_test desc,tinyint_test;
insert into t1_withoids  select integer_test, tinyint_test from t2_withoutoids order by integer_test desc,tinyint_test;
select * from t1_withoids order by c1, c2;

truncate t1_withoutoids;
explain (verbose, costs off)
insert into t1_withoutoids  select integer_test, tinyint_test from t2_withoids order by integer_test desc,tinyint_test;
insert into t1_withoutoids  select integer_test, tinyint_test from t2_withoids order by integer_test desc,tinyint_test;
select * from t1_withoutoids order by c1, c2;

truncate t1_withoutoids;
explain (verbose, costs off)
insert into t1_withoutoids  select integer_test, tinyint_test from t2_withoutoids order by integer_test desc,tinyint_test;
insert into t1_withoutoids  select integer_test, tinyint_test from t2_withoutoids order by integer_test desc,tinyint_test;
select * from t1_withoutoids order by c1, c2;

--bug px use single worker
SET default_with_oids = true;
CREATE TABLE public.select_hash2_tb_part_9 (
    pk numeric(5,0) DEFAULT NULL::numeric NOT NULL,
    varchar2_test character varying(100) DEFAULT NULL::character varying,
    nvarchar2_test character varying(100) DEFAULT NULL::character varying,
    number_test numeric(10,2) DEFAULT NULL::numeric,
    smallint_test numeric(5,0) DEFAULT NULL::numeric,
    integer_test integer,
    decimal_test numeric(10,2) DEFAULT NULL::numeric,
    float_test numeric(10,2) DEFAULT NULL::numeric,
    double_test double precision,
    timestamp_test timestamp(6) without time zone DEFAULT NULL::timestamp without time zone,
    char_size_test character(100) DEFAULT NULL::bpchar,
    nchar_test character(10) DEFAULT NULL::bpchar
);

COPY public.select_hash2_tb_part_9 (pk, varchar2_test, nvarchar2_test, number_test, smallint_test, integer_test, decimal_test, float_test, double_test, timestamp_test, char_size_test, nchar_test) FROM stdin;
2	feed32feed	feed32feed	4.23	11	11	38.49	4.23	38.4879000000000033	2018-08-15 07:45:09	feed32feed                                                                                          	feed32feed
8	adaabcwer	adaabcwer	0.45	17	17	80.46	0.45	80.457800000000006	2028-12-16 13:45:00	adaabcwer                                                                                           	adaabcwer
35	zhuoxue%yll	zhuoxue%yll	5.78	44	44	1110.47	5.78	1110.47469999999998	2012-08-20 01:45:09	nihaore                                                                                             	nihaore
42	afdaewer	afdaewer	4.56	51	51	38.49	4.56	38.4879000000000033	2018-08-15 07:45:09	sfdeiekd                                                                                            	sfdeiekd
47	afsabcabcd	afsabcabcd	4.51	56	56	58.12	4.51	58.1244999999999976	2016-12-15 14:45:09	hello1234                                                                                           	hello1234
68	einoejk	einoejk	45.23	77	77	80.46	45.23	80.457800000000006	2010-08-25 01:40:09	afdaewer                                                                                            	afdaewer
83	cdefeed	cdefeed	4.55	92	92	40.48	4.55	40.4784500000000023	2012-08-20 01:45:09	nihaore                                                                                             	nihaore
100	hellorew	hellorew	4.23	9	9	21.26	4.23	21.2579999999999991	2010-08-25 01:40:09	afdaewer                                                                                            	afdaewer
103	adaabcwer	adaabcwer	4.51	12	12	40.48	4.51	40.4784500000000023	2016-12-15 14:45:09	cdefeed                                                                                             	cdefeed
110	zhuoxue_yll	zhuoxue_yll	45.23	19	19	180.46	45.23	180.457844999999992	2008-12-08 13:45:00	\N	\N
111	zhuoxue%yll	zhuoxue%yll	4.55	20	20	200.49	4.55	200.487869999999987	2016-12-15 14:45:09	hello1234                                                                                           	hello1234
130	zhuoxue%yll	zhuoxue%yll	6.78	39	39	180.46	6.78	180.457844999999992	2018-08-15 07:45:09	feed32feed                                                                                          	feed32feed
145	kisfe	kisfe	4.51	54	54	50.49	4.51	50.4874500000000026	2019-01-15 07:45:09	word23                                                                                              	word23
148	zhuoxue_yll	zhuoxue_yll	0.45	57	57	80.46	0.45	80.457800000000006	2010-08-25 01:40:09	afdaewer                                                                                            	afdaewer
152	he343243	he343243	45.23	61	61	250.49	45.23	250.487400000000008	2028-12-16 13:45:00	adaabcwer                                                                                           	adaabcwer
162	sfdeiekd	sfdeiekd	0.45	71	71	38.49	0.45	38.4879000000000033	2018-08-15 07:45:09	feed32feed                                                                                          	feed32feed
187	zhuoxue%yll	zhuoxue%yll	4.51	96	96	58.12	4.51	58.1244999999999976	2012-08-20 01:45:09	einoejk                                                                                             	einoejk
209	he343243	he343243	4.55	18	18	90.46	4.55	90.4587446999999969	2019-01-15 07:45:09	word23                                                                                              	word23
214	hellorew	hellorew	6.78	23	23	800.15	6.78	800.147000000000048	2008-12-08 13:45:00	abdfeed                                                                                             	abdfeed
230	feed32feed	feed32feed	7.34	39	39	180.46	7.34	180.457844999999992	2008-12-08 13:45:00	abdfeed                                                                                             	abdfeed
232	afdaewer	afdaewer	0.45	41	41	250.49	0.45	250.487400000000008	2028-12-16 13:45:00	adaabcwer                                                                                           	adaabcwer
239	einoejk	einoejk	4.32	48	48	10.21	4.32	10.2144999999999992	2016-12-15 14:45:09	hello1234                                                                                           	hello1234
250	nihaore	nihaore	45.23	59	59	180.46	45.23	180.457844999999992	2018-08-15 07:45:09	sfdeiekd                                                                                            	sfdeiekd
252	hellorew	hellorew	4.56	61	61	250.49	4.56	250.487400000000008	2010-08-25 01:40:09	kisfe                                                                                               	kisfe
262	zhuoxue_yll	zhuoxue_yll	2.34	71	71	38.49	2.34	38.4879000000000033	2008-12-08 13:45:00	abdfeed                                                                                             	abdfeed
309	hellorew	hellorew	4.32	18	18	90.46	4.32	90.4587446999999969	2020-12-18 13:45:00	hellorew                                                                                            	hellorew
316	kisfe	kisfe	0.45	25	25	1414.15	0.45	1414.14747000000011	2010-08-25 01:40:09	kisfe                                                                                               	kisfe
342	he343243	he343243	7.34	51	51	38.49	7.34	38.4879000000000033	2008-12-08 13:45:00	abdfeed                                                                                             	abdfeed
355	safdwe	safdwe	4.51	64	64	1110.47	4.51	1110.47469999999998	2012-08-20 01:45:09	nihaore                                                                                             	nihaore
371	sfdeiekd	sfdeiekd	5.78	80	80	200.49	5.78	200.487869999999987	2012-08-20 01:45:09	nihaore                                                                                             	nihaore
383	nihaore	nihaore	4.51	92	92	40.48	4.51	40.4784500000000023	2016-12-15 14:45:09	hello1234                                                                                           	hello1234
388	adaabcwer	adaabcwer	2.34	97	97	80.46	2.34	80.457800000000006	2010-08-25 01:40:09	afdaewer                                                                                            	afdaewer
401	feed32feed	feed32feed	7.89	10	10	35.15	7.89	35.1477999999999966	2019-01-15 07:45:09	word23                                                                                              	word23
413	zhuoXUE	zhuoXUE	5.78	22	22	301.46	5.78	301.456999999999994	2020-12-18 13:45:00	safdwe                                                                                              	safdwe
431	safdwe	safdwe	4.12	40	40	200.49	4.12	200.487869999999987	2016-12-15 14:45:09	hello1234                                                                                           	hello1234
434	zhuoxue%yll	zhuoxue%yll	4.56	43	43	800.15	4.56	800.147000000000048	2018-08-15 07:45:09	feed32feed                                                                                          	feed32feed
446	afsabcabcd	afsabcabcd	45.23	55	55	55.15	45.23	55.1477999999999966	2008-12-08 13:45:00	\N	\N
449	kisfe	kisfe	4.32	58	58	90.46	4.32	90.4587446999999969	2019-01-15 07:45:09	word23                                                                                              	word23
460	afdaewer	afdaewer	45.23	69	69	21.26	45.23	21.2579999999999991	2010-08-25 01:40:09	kisfe                                                                                               	kisfe
472	zhuoxue%yll	zhuoxue%yll	2.34	81	81	250.49	2.34	250.487400000000008	2028-12-16 13:45:00	adaabcwer                                                                                           	adaabcwer
490	zhuoxue_yll	zhuoxue_yll	4.56	99	99	180.46	4.56	180.457844999999992	2018-08-15 07:45:09	sfdeiekd                                                                                            	sfdeiekd
499	hellorew	hellorew	7.89	8	8	10.21	7.89	10.2144999999999992	2012-08-20 01:45:09	nihaore                                                                                             	nihaore
526	safdwe	safdwe	0.45	35	35	55.15	0.45	55.1477999999999966	2008-12-08 13:45:00	\N	\N
532	he343243	he343243	4.56	41	41	250.49	4.56	250.487400000000008	2010-08-25 01:40:09	afdaewer                                                                                            	afdaewer
542	sfdeiekd	sfdeiekd	2.34	51	51	38.49	2.34	38.4879000000000033	2008-12-08 13:45:00	\N	\N
546	zhuoXUE	zhuoXUE	4.56	55	55	55.15	4.56	55.1477999999999966	2018-08-15 07:45:09	feed32feed                                                                                          	feed32feed
556	hellorew	hellorew	2.34	65	65	1414.15	2.34	1414.14747000000011	2010-08-25 01:40:09	kisfe                                                                                               	kisfe
557	abdfeed	abdfeed	4.12	66	66	1825.47	4.12	1825.47484000000009	2020-12-18 13:45:00	safdwe                                                                                              	safdwe
560	afsabcabcd	afsabcabcd	4.56	69	69	21.26	4.56	21.2579999999999991	2028-12-16 13:45:00	he343243                                                                                            	he343243
577	cdefeed	cdefeed	5.34	86	86	1825.47	5.34	1825.47484000000009	2019-01-15 07:45:09	word23                                                                                              	word23
581	einoejk	einoejk	5.78	90	90	35.15	5.78	35.1477999999999966	2020-12-18 13:45:00	hellorew                                                                                            	hellorew
588	hello1234	hello1234	4.56	97	97	80.46	4.56	80.457800000000006	2010-08-25 01:40:09	kisfe                                                                                               	kisfe
603	zhuoXUE	zhuoXUE	4.32	12	12	40.48	4.32	40.4784500000000023	2012-08-20 01:45:09	einoejk                                                                                             	einoejk
609	word23	word23	5.78	18	18	90.46	5.78	90.4587446999999969	2019-01-15 07:45:09	word23                                                                                              	word23
614	abdfeed	abdfeed	45.23	23	23	800.15	45.23	800.147000000000048	2008-12-08 13:45:00	abdfeed                                                                                             	abdfeed
616	adaabcwer	adaabcwer	4.56	25	25	1414.15	4.56	1414.14747000000011	2028-12-16 13:45:00	adaabcwer                                                                                           	adaabcwer
629	feed32feed	feed32feed	4.55	38	38	90.46	4.55	90.4587446999999969	2020-12-18 13:45:00	hellorew                                                                                            	hellorew
659	safdwe	safdwe	4.32	68	68	10.21	4.32	10.2144999999999992	2012-08-20 01:45:09	nihaore                                                                                             	nihaore
677	kisfe	kisfe	4.51	86	86	1825.47	4.51	1825.47484000000009	2020-12-18 13:45:00	hellorew                                                                                            	hellorew
682	\N	\N	2.34	91	91	38.49	2.34	38.4879000000000033	2018-08-15 07:45:09	sfdeiekd                                                                                            	sfdeiekd
711	adaabcwer	adaabcwer	4.12	20	20	200.49	4.12	200.487869999999987	2016-12-15 14:45:09	cdefeed                                                                                             	cdefeed
750	afsabcabcd	afsabcabcd	0.45	59	59	180.46	0.45	180.457844999999992	2008-12-08 13:45:00	\N	\N
758	\N	\N	4.23	67	67	2000.23	4.23	2000.23232000000007	2008-12-08 13:45:00	abdfeed                                                                                             	abdfeed
771	einoejk	einoejk	4.32	80	80	200.49	4.32	200.487869999999987	2012-08-20 01:45:09	nihaore                                                                                             	nihaore
781	feed32feed	feed32feed	4.12	90	90	35.15	4.12	35.1477999999999966	2020-12-18 13:45:00	safdwe                                                                                              	safdwe
783	afdaewer	afdaewer	4.55	92	92	40.48	4.55	40.4784500000000023	2016-12-15 14:45:09	hello1234                                                                                           	hello1234
791	kisfe	kisfe	5.78	0	0	200.49	5.78	200.487869999999987	2016-12-15 14:45:09	cdefeed                                                                                             	cdefeed
805	cdefeed	cdefeed	5.78	14	14	50.49	5.78	50.4874500000000026	2020-12-18 13:45:00	hellorew                                                                                            	hellorew
806	adaabcwer	adaabcwer	0.45	15	15	55.15	0.45	55.1477999999999966	2008-12-08 13:45:00	abdfeed                                                                                             	abdfeed
817	he343243	he343243	4.51	26	26	1825.47	4.51	1825.47484000000009	2019-01-15 07:45:09	word23                                                                                              	word23
838	feed32feed	feed32feed	45.23	47	47	2000.23	45.23	2000.23232000000007	2008-12-08 13:45:00	abdfeed                                                                                             	abdfeed
841	hellorew	hellorew	4.32	50	50	35.15	4.32	35.1477999999999966	2019-01-15 07:45:09	afsabcabcd                                                                                          	afsabcabcd
842	abdfeed	abdfeed	4.23	51	51	38.49	4.23	38.4879000000000033	2018-08-15 07:45:09	sfdeiekd                                                                                            	sfdeiekd
848	kisfe	kisfe	0.45	57	57	80.46	0.45	80.457800000000006	2028-12-16 13:45:00	he343243                                                                                            	he343243
863	adaabcwer	adaabcwer	7.89	72	72	40.48	7.89	40.4784500000000023	2016-12-15 14:45:09	hello1234                                                                                           	hello1234
865	sfdeiekd	sfdeiekd	4.12	74	74	50.49	4.12	50.4874500000000026	2019-01-15 07:45:09	word23                                                                                              	word23
869	zhuoXUE	zhuoXUE	4.32	78	78	90.46	4.32	90.4587446999999969	2020-12-18 13:45:00	hellorew                                                                                            	hellorew
911	hello1234	hello1234	4.32	20	20	200.49	4.32	200.487869999999987	2016-12-15 14:45:09	hello1234                                                                                           	hello1234
927	zhuoxue_yll	zhuoxue_yll	5.34	36	36	58.12	5.34	58.1244999999999976	2016-12-15 14:45:09	hello1234                                                                                           	hello1234
946	zhuoxue_yll	zhuoxue_yll	0.45	55	55	55.15	0.45	55.1477999999999966	2018-08-15 07:45:09	feed32feed                                                                                          	feed32feed
955	hellorew	hellorew	5.34	64	64	1110.47	5.34	1110.47469999999998	2012-08-20 01:45:09	einoejk                                                                                             	einoejk
973	afdaewer	afdaewer	5.78	82	82	301.46	5.78	301.456999999999994	2020-12-18 13:45:00	safdwe                                                                                              	safdwe
986	\N	\N	7.34	95	95	55.15	7.34	55.1477999999999966	2018-08-15 07:45:09	sfdeiekd                                                                                            	sfdeiekd
997	afsabcabcd	afsabcabcd	5.34	6	6	1825.47	5.34	1825.47484000000009	2020-12-18 13:45:00	hellorew                                                                                            	hellorew
\.

set polar_px_dop_per_node=1;
SELECT DATE '2001-09-28' + 90, SUM(number_test) OVER (PARTITION BY float_test ) AS ratio FROM select_hash2_tb_part_9 WHERE 89 NOT IN (1, 2, 34) ORDER BY char_size_test limit 10;


--- bug share scan

SET default_with_oids = true;
drop table student;

CREATE TABLE student (
    id numeric(10,0) NOT NULL,
    name character varying(255) DEFAULT NULL::character varying,
    unit integer NOT NULL,
    second_partition_cloumn_name character varying(10) DEFAULT NULL::character varying
);

INSERT INTO student VALUES (1, 'student_1', 1, NULL);
INSERT INTO student VALUES (1, 'student_1', 2, NULL);
INSERT INTO student VALUES (1, 'student_1', 3, NULL);
INSERT INTO student VALUES (1, 'student_1', 4, NULL);
INSERT INTO student VALUES (1, 'student_1', 5, NULL);
INSERT INTO student VALUES (1, 'student_1', 6, NULL);
INSERT INTO student VALUES (2, 'student_2', 1, NULL);
INSERT INTO student VALUES (2, 'student_2', 2, NULL);
INSERT INTO student VALUES (2, 'student_2', 3, NULL);
INSERT INTO student VALUES (2, 'student_2', 4, NULL);
INSERT INTO student VALUES (2, 'student_2', 5, NULL);
INSERT INTO student VALUES (2, 'student_2', 6, NULL);
INSERT INTO student VALUES (3, 'student_3', 1, NULL);
INSERT INTO student VALUES (3, 'student_3', 2, NULL);
INSERT INTO student VALUES (3, 'student_3', 3, NULL);
INSERT INTO student VALUES (3, 'student_3', 4, NULL);
INSERT INTO student VALUES (3, 'student_3', 5, NULL);
INSERT INTO student VALUES (3, 'student_3', 6, NULL);
INSERT INTO student VALUES (4, 'student_4', 1, NULL);
INSERT INTO student VALUES (4, 'student_4', 2, NULL);
INSERT INTO student VALUES (4, 'student_4', 3, NULL);
INSERT INTO student VALUES (4, 'student_4', 4, NULL);
INSERT INTO student VALUES (4, 'student_4', 5, NULL);
INSERT INTO student VALUES (4, 'student_4', 6, NULL);
INSERT INTO student VALUES (5, 'student_5', 1, NULL);
INSERT INTO student VALUES (5, 'student_5', 2, NULL);
INSERT INTO student VALUES (5, 'student_5', 3, NULL);
INSERT INTO student VALUES (5, 'student_5', 4, NULL);
INSERT INTO student VALUES (5, 'student_5', 5, NULL);
INSERT INTO student VALUES (5, 'student_5', 6, NULL);
INSERT INTO student VALUES (6, 'student_6', 1, NULL);
INSERT INTO student VALUES (6, 'student_6', 2, NULL);
INSERT INTO student VALUES (6, 'student_6', 3, NULL);
INSERT INTO student VALUES (6, 'student_6', 4, NULL);
INSERT INTO student VALUES (6, 'student_6', 5, NULL);
INSERT INTO student VALUES (6, 'student_6', 6, NULL);
INSERT INTO student VALUES (7, 'student_7', 1, NULL);
INSERT INTO student VALUES (7, 'student_7', 2, NULL);
INSERT INTO student VALUES (7, 'student_7', 3, NULL);
INSERT INTO student VALUES (7, 'student_7', 4, NULL);
INSERT INTO student VALUES (7, 'student_7', 5, NULL);
INSERT INTO student VALUES (7, 'student_7', 6, NULL);
INSERT INTO student VALUES (8, 'student_8', 1, NULL);
INSERT INTO student VALUES (8, 'student_8', 2, NULL);
INSERT INTO student VALUES (8, 'student_8', 3, NULL);
INSERT INTO student VALUES (8, 'student_8', 4, NULL);
INSERT INTO student VALUES (8, 'student_8', 5, NULL);
INSERT INTO student VALUES (8, 'student_8', 6, NULL);
INSERT INTO student VALUES (9, 'student_9', 1, NULL);
INSERT INTO student VALUES (9, 'student_9', 2, NULL);
INSERT INTO student VALUES (9, 'student_9', 3, NULL);
INSERT INTO student VALUES (9, 'student_9', 4, NULL);
INSERT INTO student VALUES (9, 'student_9', 5, NULL);
INSERT INTO student VALUES (9, 'student_9', 6, NULL);


--
-- Name: pg_oid_690920_index; Type: INDEX; Schema:  Owner: zx_test
--

CREATE UNIQUE INDEX pg_oid_690920_index ON student USING btree (oid);

drop table student_hobby;
CREATE TABLE student_hobby (
    id numeric(10,0) DEFAULT NULL::numeric,
    sid numeric(10,0) NOT NULL,
    unit integer NOT NULL,
    coid character varying(1) NOT NULL
);

INSERT INTO student_hobby VALUES (1, 1, 1, 'A');
INSERT INTO student_hobby VALUES (2, 1, 1, 'B');
INSERT INTO student_hobby VALUES (3, 1, 2, 'A');
INSERT INTO student_hobby VALUES (4, 1, 2, 'B');
INSERT INTO student_hobby VALUES (5, 2, 2, 'A');
INSERT INTO student_hobby VALUES (6, 2, 5, 'a');
INSERT INTO student_hobby VALUES (7, 3, 4, 'b');
INSERT INTO student_hobby VALUES (8, 3, 5, 'C');

--
-- Name: pg_oid_691526_index; Type: INDEX; Schema:  Owner: zx_test
--

CREATE UNIQUE INDEX pg_oid_691526_index ON student_hobby USING btree (oid);


drop table community;
CREATE TABLE community (
    id numeric(10,0) DEFAULT NULL::numeric,
    coid character varying(1) DEFAULT NULL::character varying,
    coname character varying(2000) NOT NULL,
    co_status character varying(1) DEFAULT NULL::character varying
);

INSERT INTO community VALUES (2, 'A', 'ABC2DEF g', 'O');
INSERT INTO community VALUES (1, 'a', 'abcdef g', 'I');
INSERT INTO community VALUES (4, 'B', 'BCD2EF g', 'O');
INSERT INTO community VALUES (3, 'b', 'bidef g', 'I');
INSERT INTO community VALUES (5, NULL, 'cde2 hh', 'O');
INSERT INTO community VALUES (6, 'C', 'CDEF g', NULL);


ALTER TABLE ONLY community ADD CONSTRAINT community_pkey PRIMARY KEY (coname);


CREATE UNIQUE INDEX pg_oid_402684_index ON community USING btree (oid);


drop table community_part;
CREATE TABLE community_part (
    id numeric(10,0) DEFAULT NULL::numeric,
    coid character varying(1) DEFAULT NULL::character varying,
    coname character varying(2000) NOT NULL,
    co_status character varying(1) DEFAULT NULL::character varying
) partition by hash(id);

CREATE TABLE community_part0 PARTITION OF community_part FOR VALUES WITH (modulus 2, remainder 0);
CREATE TABLE community_part1 PARTITION OF community_part FOR VALUES WITH (modulus 2, remainder 1);

INSERT INTO community_part VALUES (2, 'A', 'ABC2DEF g', 'O');
INSERT INTO community_part VALUES (1, 'a', 'abcdef g', 'I');
INSERT INTO community_part VALUES (4, 'B', 'BCD2EF g', 'O');
INSERT INTO community_part VALUES (3, 'b', 'bidef g', 'I');
INSERT INTO community_part VALUES (5, NULL, 'cde2 hh', 'O');
INSERT INTO community_part VALUES (6, 'C', 'CDEF g', NULL);

--
-- Name: community community_pkey; Type: CONSTRAINT; Schema:  Owner: zx_test
--

select tmp.name,tmp.unit ,c.coname from (select a.name, a.id, a.unit,b.coid from student a join student_hobby b on a.id=b.sid and a.unit = b.unit) tmp right outer join community c on tmp.coid=c.coid order by 1,2,3;
select tmp.name,tmp.unit ,c.coname from (select a.name, a.id, a.unit,b.coid from student a join student_hobby b on a.id=b.sid and a.unit = b.unit) tmp right outer join community_part c on tmp.coid=c.coid order by 1,2,3;
