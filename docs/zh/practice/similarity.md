---
author: digoal
date: 2023/02/03
minute: 20
---

# 通过 pg_similarity 实现 17 种文本相似搜索算法

<ArticleInfo :frontmatter=$frontmatter></ArticleInfo>

## 背景

PolarDB 的云原生存算分离架构, 具备低廉的数据存储、高效扩展弹性、高速多机并行计算能力、高速数据搜索和处理; PolarDB 与计算算法结合, 将实现双剑合璧, 推动业务数据的价值产出, 将数据变成生产力.

本文将介绍 PolarDB 开源版通过 pg_similarity 实现 17 种文本相似搜索 - token 归一切分, 根据文本相似度检索相似文本.

测试环境为 macos+docker, polardb 部署请参考:

- [《如何用 PolarDB 证明巴菲特的投资理念 - 包括 PolarDB 简单部署》](https://github.com/digoal/blog/blob/master/202209/20220908_02.md)

## pg_similarity for PolarDB

pg_similarity 支持 17 种相似算法

- L1 Distance (as known as City Block or Manhattan Distance);
- Cosine Distance;
- Dice Coefficient;
- Euclidean Distance;
- Hamming Distance;
- Jaccard Coefficient;
- Jaro Distance;
- Jaro-Winkler Distance;
- Levenshtein Distance;
- Matching Coefficient;
- Monge-Elkan Coefficient;
- Needleman-Wunsch Coefficient;
- Overlap Coefficient;
- Q-Gram Distance;
- Smith-Waterman Coefficient;
- Smith-Waterman-Gotoh Coefficient;
- Soundex Distance.

以上大多数相似算法支持索引操作. 详见: https://github.com/eulerto/pg_similarity

需要注意

- token 切分归一化的算法由参数设置, 如果你的数据写入时参数是 a, 那么写入的文本会按 a 来切分, 如果未来又改成了 b, 那么未来的切分和之前的切分算法可能不一样, 当然如果业务允许也 OK.
- 在比对文本相似性时亦如此.

## 部署 pg_similarity for PolarDB

1、下载并编译

```
git clone --depth 1 https://github.com/eulerto/pg_similarity.git


cd pg_similarity/

USE_PGXS=1 make
USE_PGXS=1 make install
```

```
export PGHOST=127.0.0.1

[postgres@67e1eed1b4b6 pg_similarity]$ USE_PGXS=1 make installcheck
/home/postgres/tmp_basedir_polardb_pg_1100_bld/lib/pgxs/src/makefiles/../../src/test/regress/pg_regress --inputdir=./ --bindir='/home/postgres/tmp_basedir_polardb_pg_1100_bld/bin'      --dbname=contrib_regression test1 test2 test3 test4
(using postmaster on 127.0.0.1, default port)
============== dropping database "contrib_regression" ==============
DROP DATABASE
============== creating database "contrib_regression" ==============
CREATE DATABASE
ALTER DATABASE
============== running regression test queries        ==============
test test1                        ... ok
test test2                        ... ok
test test3                        ... ok
test test4                        ... ok


==========================================================
 All 4 tests passed.

 POLARDB:
 All 4 tests, 0 tests in ignore, 0 tests in polar ignore.
==========================================================
```

2、加载 pg_similarity 插件

```
postgres=# create database db1;
CREATE DATABASE

postgres=# \c db1
You are now connected to database "db1" as user "postgres".
db1=# create extension pg_similarity ;
CREATE EXTENSION
```

3、pg_similarity 插件会新增一些函数和操作符, 用于相似搜索.

```
db1=# \df
                                                             List of functions
 Schema |          Name           | Result data type |                              Argument data types                              | Type
--------+-------------------------+------------------+-------------------------------------------------------------------------------+------
 public | block                   | double precision | text, text                                                                    | func
 public | block_op                | boolean          | text, text                                                                    | func
 public | cosine                  | double precision | text, text                                                                    | func
 public | cosine_op               | boolean          | text, text                                                                    | func
 public | dice                    | double precision | text, text                                                                    | func
 public | dice_op                 | boolean          | text, text                                                                    | func
 public | euclidean               | double precision | text, text                                                                    | func
 public | euclidean_op            | boolean          | text, text                                                                    | func
 public | gin_extract_query_token | internal         | internal, internal, smallint, internal, internal, internal, internal          | func
 public | gin_extract_value_token | internal         | internal, internal, internal                                                  | func
 public | gin_token_consistent    | boolean          | internal, smallint, internal, integer, internal, internal, internal, internal | func
 public | hamming                 | double precision | bit varying, bit varying                                                      | func
 public | hamming_op              | boolean          | bit varying, bit varying                                                      | func
 public | hamming_text            | double precision | text, text                                                                    | func
 public | hamming_text_op         | boolean          | text, text                                                                    | func
 public | jaccard                 | double precision | text, text                                                                    | func
 public | jaccard_op              | boolean          | text, text                                                                    | func
 public | jaro                    | double precision | text, text                                                                    | func
 public | jaro_op                 | boolean          | text, text                                                                    | func
 public | jarowinkler             | double precision | text, text                                                                    | func
 public | jarowinkler_op          | boolean          | text, text                                                                    | func
 public | lev                     | double precision | text, text                                                                    | func
 public | lev_op                  | boolean          | text, text                                                                    | func
 public | matchingcoefficient     | double precision | text, text                                                                    | func
 public | matchingcoefficient_op  | boolean          | text, text                                                                    | func
 public | mongeelkan              | double precision | text, text                                                                    | func
 public | mongeelkan_op           | boolean          | text, text                                                                    | func
 public | needlemanwunsch         | double precision | text, text                                                                    | func
 public | needlemanwunsch_op      | boolean          | text, text                                                                    | func
 public | overlapcoefficient      | double precision | text, text                                                                    | func
 public | overlapcoefficient_op   | boolean          | text, text                                                                    | func
 public | qgram                   | double precision | text, text                                                                    | func
 public | qgram_op                | boolean          | text, text                                                                    | func
 public | smithwaterman           | double precision | text, text                                                                    | func
 public | smithwaterman_op        | boolean          | text, text                                                                    | func
 public | smithwatermangotoh      | double precision | text, text                                                                    | func
 public | smithwatermangotoh_op   | boolean          | text, text                                                                    | func
 public | soundex                 | double precision | text, text                                                                    | func
 public | soundex_op              | boolean          | text, text                                                                    | func
(39 rows)

db1=# \do
                             List of operators
 Schema | Name | Left arg type | Right arg type | Result type | Description
--------+------+---------------+----------------+-------------+-------------
 public | ~!!  | text          | text           | boolean     |
 public | ~!~  | text          | text           | boolean     |
 public | ~##  | text          | text           | boolean     |
 public | ~#~  | text          | text           | boolean     |
 public | ~%%  | text          | text           | boolean     |
 public | ~**  | text          | text           | boolean     |
 public | ~*~  | text          | text           | boolean     |
 public | ~++  | text          | text           | boolean     |
 public | ~-~  | text          | text           | boolean     |
 public | ~==  | text          | text           | boolean     |
 public | ~=~  | text          | text           | boolean     |
 public | ~??  | text          | text           | boolean     |
 public | ~@@  | text          | text           | boolean     |
 public | ~@~  | text          | text           | boolean     |
 public | ~^^  | text          | text           | boolean     |
 public | ~||  | text          | text           | boolean     |
 public | ~~~  | text          | text           | boolean     |
(17 rows)
```

4、pg_similarity 的常用配置, 我们只需将 pg_similarity 配置到 shared_preload_libraries 即可开始测试.

```
[postgres@67e1eed1b4b6 pg_similarity]$ cat pg_similarity.conf.sample
#-----------------------------------------------------------------------
# postgresql.conf
#-----------------------------------------------------------------------
# the former needs a restart every time you upgrade pg_similarity and
# the later needs that you create a $libdir/plugins directory and move
# pg_similarity.so to it (it doesn't require a restart; just open a new
# connection).
#shared_preload_libraries = 'pg_similarity'
# - or -
#local_preload_libraries = 'pg_similarity'

#-----------------------------------------------------------------------
# pg_similarity
#-----------------------------------------------------------------------

# - Block -
#pg_similarity.block_tokenizer = 'alnum'	# alnum, camelcase, gram, or word
#pg_similarity.block_threshold = 0.7		# 0.0 .. 1.0
#pg_similarity.block_is_normalized = true

# - Cosine -
#pg_similarity.cosine_tokenizer = 'alnum'
#pg_similarity.cosine_threshold = 0.7
#pg_similarity.cosine_is_normalized = true

# - Dice -
#pg_similarity.dice_tokenizer = 'alnum'
#pg_similarity.dice_threshold = 0.7
#pg_similarity.dice_is_normalized = true

# - Euclidean -
#pg_similarity.euclidean_tokenizer = 'alnum'
#pg_similarity.euclidean_threshold = 0.7
#pg_similarity.euclidean_is_normalized = true

# - Hamming -
#pg_similarity.hamming_threshold = 0.7
#pg_similarity.hamming_is_normalized = true

# - Jaccard -
#pg_similarity.jaccard_tokenizer = 'alnum'
#pg_similarity.jaccard_threshold = 0.7
#pg_similarity.jaccard_is_normalized = true

# - Jaro -
#pg_similarity.jaro_threshold = 0.7
#pg_similarity.jaro_is_normalized = true

# - Jaro -
#pg_similarity.jaro_threshold = 0.7
#pg_similarity.jaro_is_normalized = true

# - Jaro-Winkler -
#pg_similarity.jarowinkler_threshold = 0.7
#pg_similarity.jarowinkler_is_normalized = true

# - Levenshtein -
#pg_similarity.levenshtein_threshold = 0.7
#pg_similarity.levenshtein_is_normalized = true

# - Matching Coefficient -
#pg_similarity.matching_tokenizer = 'alnum'
#pg_similarity.matching_threshold = 0.7
#pg_similarity.matching_is_normalized = true

# - Monge-Elkan -
#pg_similarity.mongeelkan_tokenizer = 'alnum'
#pg_similarity.mongeelkan_threshold = 0.7
#pg_similarity.mongeelkan_is_normalized = true

# - Needleman-Wunsch -
#pg_similarity.nw_threshold = 0.7
#pg_similarity.nw_is_normalized = true

# - Overlap Coefficient -
#pg_similarity.overlap_tokenizer = 'alnum'
#pg_similarity.overlap_threshold = 0.7
#pg_similarity.overlap_is_normalized = true

# - Q-Gram -
#pg_similarity.qgram_tokenizer = 'qgram'
#pg_similarity.qgram_threshold = 0.7
#pg_similarity.qgram_is_normalized = true

# - Smith-Waterman -
#pg_similarity.sw_threshold = 0.7
#pg_similarity.sw_is_normalized = true

# - Smith-Waterman-Gotoh -
#pg_similarity.swg_threshold = 0.7
#pg_similarity.swg_is_normalized = true
```

5、测试相似搜索, 导入测试数据

```
[postgres@67e1eed1b4b6 ~]$ cd pg_similarity/
[postgres@67e1eed1b4b6 pg_similarity]$ psql
psql (11.9)
Type "help" for help.

postgres=# CREATE TABLE simtst (a text);
CREATE TABLE
postgres=#
postgres=# INSERT INTO simtst (a) VALUES
postgres-# ('Euler Taveira de Oliveira'),
postgres-# ('EULER TAVEIRA DE OLIVEIRA'),
postgres-# ('Euler T. de Oliveira'),
postgres-# ('Oliveira, Euler T.'),
postgres-# ('Euler Oliveira'),
postgres-# ('Euler Taveira'),
postgres-# ('EULER TAVEIRA OLIVEIRA'),
postgres-# ('Oliveira, Euler'),
postgres-# ('Oliveira, E. T.'),
postgres-# ('ETO');
INSERT 0 10
postgres=#
postgres=# \copy simtst FROM 'data/similarity.data'
COPY 2999
```

6、测试相似搜索, 创建 gin 索引

https://github.com/eulerto/pg_similarity/blob/master/pg_similarity--1.0.sql

以下操作符支持索引检索

```
CREATE OPERATOR CLASS gin_similarity_ops
FOR TYPE text USING gin
AS
    OPERATOR    1   ~++,		-- block
    OPERATOR    2   ~##,		-- cosine
    OPERATOR    3   ~-~,		-- dice
    OPERATOR    4   ~!!,		-- euclidean
    OPERATOR    5   ~??,		-- jaccard
--    OPERATOR    6   ~%%,		-- jaro
--    OPERATOR    7   ~@@,		-- jarowinkler
--    OPERATOR    8   ~==,		-- lev
    OPERATOR    9   ~^^,		-- matchingcoefficient
--    OPERATOR    10  ~||,		-- mongeelkan
--    OPERATOR    11  ~#~,		-- needlemanwunsch
    OPERATOR    12  ~**,		-- overlapcoefficient
    OPERATOR    13  ~~~,		-- qgram
--    OPERATOR    14  ~=~,		-- smithwaterman
--    OPERATOR    15  ~!~,		-- smithwatermangotoh
--    OPERATOR    16  ~*~,		-- soundex
    FUNCTION    1   bttextcmp(text, text),
    FUNCTION    2   gin_extract_value_token(internal, internal, internal),
    FUNCTION    3   gin_extract_query_token(internal, internal, int2, internal, internal, internal, internal),
    FUNCTION    4   gin_token_consistent(internal, int2, internal, int4, internal, internal, internal, internal),
    STORAGE text;
```

```
postgres=# create index on simtst using gin (a gin_similarity_ops);
CREATE INDEX
```

6、测试相似搜索, 使用索引根据相似性高速锁定目标数据.

可以根据 threshold 调整目标数据, 大于等于它的相似度才会被返回.

相似度 threadshold 设置越大, 范围越收敛, 性能越好.

可以放到函数中设置 threadshold, 分阶段返回.

- [《社交、电商、游戏等 推荐系统 (相似推荐) - 阿里云 pase smlar 索引方案对比》](https://github.com/digoal/blog/blob/master/202004/20200421_01.md)

```
postgres=# show pg_similarity.cosine_tokenizer;
 pg_similarity.cosine_tokenizer
--------------------------------
 alnum
(1 row)

postgres=# show pg_similarity.cosine_threshold;
 pg_similarity.cosine_threshold
--------------------------------
 0.7
(1 row)

postgres=# show pg_similarity.cosine_is_normalized;
 pg_similarity.cosine_is_normalized
------------------------------------
 on
(1 row)

postgres=# select *, cosine(a, 'hello')  from simtst where  a ~## 'hello' limit 10;
 a | cosine
---+--------
(0 rows)

postgres=# select *, cosine(a, 'EULER TAVEIRA DE OLIVEI')  from simtst where  a ~## 'EULER TAVEIRA DE OLIVEI' limit 10;
             a             | cosine
---------------------------+--------
 EULER TAVEIRA DE OLIVEIRA |   0.75
(1 row)

postgres=# explain select *, cosine(a, 'EULER TAVEIRA DE OLIVEI')  from simtst where  a ~## 'EULER TAVEIRA DE OLIVEI' limit 10;
                                    QUERY PLAN
----------------------------------------------------------------------------------
 Limit  (cost=36.02..44.29 rows=3 width=40)
   ->  Bitmap Heap Scan on simtst  (cost=36.02..44.29 rows=3 width=40)
         Recheck Cond: (a ~## 'EULER TAVEIRA DE OLIVEI'::text)
         ->  Bitmap Index Scan on simtst_a_idx  (cost=0.00..36.02 rows=3 width=0)
               Index Cond: (a ~## 'EULER TAVEIRA DE OLIVEI'::text)
(5 rows)

postgres=# set pg_similarity.cosine_threshold=0.75;
SET
postgres=# select *, cosine(a, 'EULER TAVEIRA DE OLIVEI')  from simtst where  a ~## 'EULER TAVEIRA DE OLIVEI' limit 10;
             a             | cosine
---------------------------+--------
 EULER TAVEIRA DE OLIVEIRA |   0.75
(1 row)

postgres=# set pg_similarity.cosine_threshold=0.76;
SET
postgres=# select *, cosine(a, 'EULER TAVEIRA DE OLIVEI')  from simtst where  a ~## 'EULER TAVEIRA DE OLIVEI' limit 10;
 a | cosine
---+--------
(0 rows)
```

## 参考

https://github.com/eulerto/pg_similarity
