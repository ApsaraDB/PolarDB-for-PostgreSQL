---
author: digoal
date: 2023/02/03
minute: 20
---

# 索引加速"数组、JSON"内元素的模糊搜索

<ArticleInfo :frontmatter=$frontmatter></ArticleInfo>

## 背景

PolarDB 的云原生存算分离架构, 具备低廉的数据存储、高效扩展弹性、高速多机并行计算能力、高速数据搜索和处理; PolarDB 与计算算法结合, 将实现双剑合璧, 推动业务数据的价值产出, 将数据变成生产力.

本文将介绍 PolarDB 开源版通过 parray_gin 实现高效率 数组、JSON 内元素的模糊搜索

测试环境为 macos+docker, polardb 部署请参考:

- [《如何用 PolarDB 证明巴菲特的投资理念 - 包括 PolarDB 简单部署》](https://github.com/digoal/blog/blob/master/202209/20220908_02.md)

## 测试

PG 模糊搜索采用 GIN 倒排索引, 使用 pg_trgm 插件将字符串前 1 后 2 加上空格后, 按连续 3 个字符切分, 并对切分后的 token 建立 token,ctid 的倒排索引.

在模糊搜索时, 可以将搜索字符串按同样方式切分, 根据倒排搜索快速的定位到对应的 ctid.

- [《PostgreSQL 数组或 JSON 内容的模糊匹配索引插件: parray_gin》](https://github.com/digoal/blog/blob/master/202110/20211005_01.md)
- [《重新发现 PostgreSQL 之美 - 16 like '%西出函谷关%' 模糊查询》](https://github.com/digoal/blog/blob/master/202106/20210607_01.md)
- [《PostgreSQL 应用开发解决方案最佳实践系列课程 - 1. 中文分词与模糊查询》](https://github.com/digoal/blog/blob/master/202105/20210502_01.md)
- [《[直播]在数据库中跑全文检索、模糊查询 SQL 会不会被开除?》](https://github.com/digoal/blog/blob/master/202009/20200913_01.md)
- [《PostgreSQL 模糊查询、相似查询 (like '%xxx%') pg_bigm 比 pg_trgm 优势在哪?》](https://github.com/digoal/blog/blob/master/202009/20200912_01.md)
- [《PostgreSQL 模糊查询增强插件 pgroonga , pgbigm (含单字、双字、多字、多字节字符) - 支持 JSON 模糊查询等》](https://github.com/digoal/blog/blob/master/202003/20200330_01.md)
- [《PostgreSQL ghtree 实现的海明距离排序索引, 性能不错(模糊图像) - pg-knn_hamming - bit string 比特字符串 相似度搜索》](https://github.com/digoal/blog/blob/master/202003/20200326_08.md)
- [《PostgreSQL VagueGeometry vague spatial data - VASA (Vague Spatial Algebra) for PG - 模糊空间数据》](https://github.com/digoal/blog/blob/master/202003/20200326_02.md)
- [《PostgreSQL bktree 索引 using gist 例子 - 海明距离检索 - 短文相似、模糊图像搜索 - bit string 比特字符串 相似度搜索》](https://github.com/digoal/blog/blob/master/202003/20200324_29.md)
- [《PostgreSQL+MySQL 联合解决方案 - 第 12 课视频 - 全文检索、中文分词、模糊查询、相似文本查询》](https://github.com/digoal/blog/blob/master/202001/20200116_01.md)
- [《PostgreSQL 数组里面的元素，模糊搜索，模糊查询，like，前后百分号，正则查询，倒排索引》](https://github.com/digoal/blog/blob/master/201903/20190320_01.md)
- [《PostgreSQL 一复合查询 SQL 优化例子 - (多个 exists , 范围检索 , IN 检索 , 模糊检索 组合)》](https://github.com/digoal/blog/blob/master/201806/20180612_01.md)
- [《PostgreSQL 模糊查询+大量重复值匹配 实践 - 分区索引 = any (array())》](https://github.com/digoal/blog/blob/master/201805/20180502_01.md)
- [《PostgreSQL 模糊查询 与 正则匹配 性能差异与 SQL 优化建议》](https://github.com/digoal/blog/blob/master/201801/20180118_03.md)
- [《用 PostgreSQL 做实时高效 搜索引擎 - 全文检索、模糊查询、正则查询、相似查询、ADHOC 查询》](https://github.com/digoal/blog/blob/master/201712/20171205_02.md)
- [《HTAP 数据库 PostgreSQL 场景与性能测试之 12 - (OLTP) 字符串搜索 - 前后模糊查询》](https://github.com/digoal/blog/blob/master/201711/20171107_13.md)
- [《HTAP 数据库 PostgreSQL 场景与性能测试之 9 - (OLTP) 字符串模糊查询 - 含索引实时写入》](https://github.com/digoal/blog/blob/master/201711/20171107_10.md)
- [《多国语言字符串的加密、全文检索、模糊查询的支持》](https://github.com/digoal/blog/blob/master/201710/20171020_01.md)
- [《Greenplum 模糊查询 实践》](https://github.com/digoal/blog/blob/master/201710/20171016_04.md)
- [《PostgreSQL 中英文混合分词特殊规则(中文单字、英文单词) - 中英分明》](https://github.com/digoal/blog/blob/master/201711/20171104_03.md)
- [《PostgreSQL 模糊查询最佳实践 - (含单字、双字、多字模糊查询方法)》](https://github.com/digoal/blog/blob/master/201704/20170426_01.md)
- [《PostgreSQL 全表 全字段 模糊查询的毫秒级高效实现 - 搜索引擎颤抖了》](https://github.com/digoal/blog/blob/master/201701/20170106_04.md)
- [《从难缠的模糊查询聊开 - PostgreSQL 独门绝招之一 GIN , GiST , SP-GiST , RUM 索引原理与技术背景》](https://github.com/digoal/blog/blob/master/201612/20161231_01.md)
- [《中文模糊查询性能优化 by PostgreSQL trgm》](https://github.com/digoal/blog/blob/master/201605/20160506_02.md)
- [《PostgreSQL 百亿数据 秒级响应 正则及模糊查询》](https://github.com/digoal/blog/blob/master/201603/20160302_01.md)

即使没有 parray_gin, 我们也可以将数组或 JSON 格式化处理后, 用大字符串和 pg_trgm 来实现元素模糊搜索. 例如

```
array['abc','aaa','hello']

把元素内容的sep char和quote char转义, 然后直接把 'abc','aaa','hello'当成字符串处理. 建立pg_trgm gin索引.

搜索元素时如果需要指定元素前缀或后缀搜索, 那么带上sep char和quote char即可.
```

使用 parray_gin 就简单多了, 不需要处理那么多.

下面测试 PolarDB+parray_gin 实现数组内元素的模糊搜索.

```
git clone --depth 1 http://github.com/theirix/parray_gin/

cd parray_gin/

USE_PGXS=1 make

USE_PGXS=1 make install

export PGHOST=localhost

[postgres@1bbb8082aa60 parray_gin]$ psql
psql (11.9)
Type "help" for help.

postgres=# \q
[postgres@1bbb8082aa60 parray_gin]$ USE_PGXS=1 make installcheck
/home/postgres/tmp_basedir_polardb_pg_1100_bld/lib/pgxs/src/makefiles/../../src/test/regress/pg_regress --inputdir=./ --bindir='/home/postgres/tmp_basedir_polardb_pg_1100_bld/bin'      --inputdir=test --dbname=contrib_regression op index
(using postmaster on localhost, default port)
============== dropping database "contrib_regression" ==============
NOTICE:  database "contrib_regression" does not exist, skipping
DROP DATABASE
============== creating database "contrib_regression" ==============
CREATE DATABASE
ALTER DATABASE
============== running regression test queries        ==============
test op                           ... ok
test index                        ... ok


==========================================================
 All 2 tests passed.

 POLARDB:
 All 2 tests, 0 tests in ignore, 0 tests in polar ignore.
==========================================================
```

```
create table t (id int, info text[]);

create or replace function gen_text_arr(int) returns text[] as $$
  select array(select md5(random()::text) from generate_series(1,$1));
$$ language sql strict;

postgres=# select gen_text_arr(10);
-[ RECORD 1 ]+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
gen_text_arr | {4134ee81fcdc29da486df37a1725e1cc,d0bb424307f93a6374d1af5a4b1c0451,def4b4bc24bc6aefb084df8a1571d773,aff17d39b2c3e8ccebf1059c2cd466dc,3988cb3f89372081c6444b7f8a825cf6,77d3a12d9a5159bd2e11fac1782eaf90,0ecac2cd508f60221b31934ea1128223,622819cfa7c3e3e600f70ed90265edaa,e9311e8d6f23be74b2e73eae4408aaa8,207eb23a50212cb101f83a6041211d90}

postgres=# insert into t select id , gen_text_arr(10) from generate_series(1,1000) id;
INSERT 0 1000

postgres=# select * from t where info @@> array['%4b1%'];
 id  |
          info

-----+----------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------------------
-------------------
  14 | {745b761d7145edb79904c5217c0ec0b4,eab9d9d4de9afc8c7a2bc4cdcd3bcb2a,3116cd48046936709c56e952f5d50380,642eec5d3c17721dadb89759ac116821,49ba14c3c71b73c0a3b8
6aa6f20a4f9c,01632c5889d4ae642422fea8620187e1,078ea7bf29a6f8bf53c6abcec98df5ad,2548e08ad3cb87dfcfe55a86e47cc60f,0c7002203e72d854f9c0643bec6c59b7,cfdd57d32f4bcee
8b4b1adfe11a08a81}
  33 | {639e7f990ef271b24b1ac1a1f154476b,5c0dd44f87821cf555fb579f2dd9871d,b3118d34a6f788ad9c9d3343743900bc,798abd4aece1cbe604e608294227dde6,f08757d02fd0db9d08c9
2240c55ec14b,54f206220cf2097f0e2a6f630a7871be,585d04664a022ab49607d0d6ff18fc89,f5681d20b2b923973652f9952df6b71d,1d204241c105c78ba0514bdf1dba6bbb,5f427b5c2b65e0d
e41b70e804dfcc41d}
...

postgres=# select * from t where info @@> array['%4b1ac%'];
 id |
         info

----+-----------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------
 33 | {639e7f990ef271b24b1ac1a1f154476b,5c0dd44f87821cf555fb579f2dd9871d,b3118d34a6f788ad9c9d3343743900bc,798abd4aece1cbe604e608294227dde6,f08757d02fd0db9d08c92
240c55ec14b,54f206220cf2097f0e2a6f630a7871be,585d04664a022ab49607d0d6ff18fc89,f5681d20b2b923973652f9952df6b71d,1d204241c105c78ba0514bdf1dba6bbb,5f427b5c2b65e0de
41b70e804dfcc41d}
(1 row)

postgres=# select * from t where info @@> array['%4b1acd%'];
 id | info
----+------
(0 rows)

postgres=# explain select * from t where info @@> array['%4b1ac%'];
                                QUERY PLAN
--------------------------------------------------------------------------
 Bitmap Heap Scan on t  (cost=28.01..32.02 rows=1 width=36)
   Recheck Cond: (info @@> '{%4b1ac%}'::text[])
   ->  Bitmap Index Scan on t_info_idx  (cost=0.00..28.01 rows=1 width=0)
         Index Cond: (info @@> '{%4b1ac%}'::text[])
(4 rows)

postgres=# explain (analyze,verbose,timing,costs,buffers) select * from t where info @@> array['%4b1ac%', '%8fc89'];
                                                     QUERY PLAN
--------------------------------------------------------------------------------------------------------------------
 Bitmap Heap Scan on public.t  (cost=60.01..64.02 rows=1 width=36) (actual time=0.121..0.122 rows=1 loops=1)
   Output: id, info
   Recheck Cond: (t.info @@> '{%4b1ac%,%8fc89}'::text[])
   Heap Blocks: exact=1
   Buffers: shared hit=16
   ->  Bitmap Index Scan on t_info_idx  (cost=0.00..60.01 rows=1 width=0) (actual time=0.109..0.109 rows=1 loops=1)
         Index Cond: (t.info @@> '{%4b1ac%,%8fc89}'::text[])
         Buffers: shared hit=15
 Planning Time: 0.075 ms
 Execution Time: 0.144 ms
(10 rows)

Time: 0.699 ms
postgres=# select * from t where info @@> array['%4b1ac%', '%8fc89'];
 id |
         info

----+-----------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------
 33 | {639e7f990ef271b24b1ac1a1f154476b,5c0dd44f87821cf555fb579f2dd9871d,b3118d34a6f788ad9c9d3343743900bc,798abd4aece1cbe604e608294227dde6,f08757d02fd0db9d08c92
240c55ec14b,54f206220cf2097f0e2a6f630a7871be,585d04664a022ab49607d0d6ff18fc89,f5681d20b2b923973652f9952df6b71d,1d204241c105c78ba0514bdf1dba6bbb,5f427b5c2b65e0de
41b70e804dfcc41d}
(1 row)

Time: 0.733 ms

postgres=# insert into t select id , gen_text_arr(10) from generate_series(1,120000) id;
INSERT 0 100000
Time: 9242.877 ms (00:09.243)
postgres=# \dt+
                   List of relations
 Schema | Name | Type  |  Owner   | Size  | Description
--------+------+-------+----------+-------+-------------
 public | t    | table | postgres | 50 MB |
(1 row)



postgres=# select * from t where info @@> array['%4b1ac%', '%8fc89'];
 id |
         info

----+-----------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------
 33 | {639e7f990ef271b24b1ac1a1f154476b,5c0dd44f87821cf555fb579f2dd9871d,b3118d34a6f788ad9c9d3343743900bc,798abd4aece1cbe604e608294227dde6,f08757d02fd0db9d08c92
240c55ec14b,54f206220cf2097f0e2a6f630a7871be,585d04664a022ab49607d0d6ff18fc89,f5681d20b2b923973652f9952df6b71d,1d204241c105c78ba0514bdf1dba6bbb,5f427b5c2b65e0de
41b70e804dfcc41d}
(1 row)

Time: 4.783 ms

postgres=# explain (analyze,timing,costs,buffers,verbose) select * from t where info @@> array['%4b1ac%', '%8fc89'];
                                                      QUERY PLAN
----------------------------------------------------------------------------------------------------------------------
 Bitmap Heap Scan on public.t  (cost=96.94..529.04 rows=121 width=36) (actual time=4.114..4.115 rows=1 loops=1)
   Output: id, info
   Recheck Cond: (t.info @@> '{%4b1ac%,%8fc89}'::text[])
   Heap Blocks: exact=1
   Buffers: shared hit=48
   ->  Bitmap Index Scan on t_info_idx  (cost=0.00..96.91 rows=121 width=0) (actual time=4.103..4.103 rows=1 loops=1)
         Index Cond: (t.info @@> '{%4b1ac%,%8fc89}'::text[])
         Buffers: shared hit=47
 Planning Time: 0.090 ms
 Execution Time: 4.170 ms
(10 rows)
```

全表扫描性能差了几十倍

```
postgres=# set enable_bitmapscan =off;
SET
Time: 0.473 ms
postgres=# explain (analyze,timing,costs,buffers,verbose) select * from t where info @@> array['%4b1ac%', '%8fc89'];
                                                QUERY PLAN
----------------------------------------------------------------------------------------------------------
 Seq Scan on public.t  (cost=0.00..7881.50 rows=121 width=36) (actual time=0.632..193.929 rows=1 loops=1)
   Output: id, info
   Filter: (t.info @@> '{%4b1ac%,%8fc89}'::text[])
   Rows Removed by Filter: 120999
   Buffers: shared hit=6229 read=140
 Planning Time: 0.081 ms
 Execution Time: 193.947 ms
(7 rows)

Time: 194.697 ms
postgres=# select * from t where info @@> array['%4b1ac%', '%8fc89'];
 id |
         info

----+-----------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------
 33 | {639e7f990ef271b24b1ac1a1f154476b,5c0dd44f87821cf555fb579f2dd9871d,b3118d34a6f788ad9c9d3343743900bc,798abd4aece1cbe604e608294227dde6,f08757d02fd0db9d08c92
240c55ec14b,54f206220cf2097f0e2a6f630a7871be,585d04664a022ab49607d0d6ff18fc89,f5681d20b2b923973652f9952df6b71d,1d204241c105c78ba0514bdf1dba6bbb,5f427b5c2b65e0de
41b70e804dfcc41d}
(1 row)

Time: 199.342 ms
```

有了 parray_gin, 在设计数据结构时, 可以更加灵活, 例如将“一个时间段、一个组、一个对象”的“多个标签、多个信息”打包成 1 行数组存储, 对数组进行元素搜索, 则可以快速匹配到符合条件的“一个时间段、一个组、一个对象”.

## 参考

http://github.com/theirix/parray_gin/

[《PostgreSQL 数组或 JSON 内容的模糊匹配索引插件: parray_gin》](https://github.com/digoal/blog/blob/master/202110/20211005_01.md)
