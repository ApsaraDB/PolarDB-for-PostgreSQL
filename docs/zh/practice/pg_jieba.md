---
author: digoal
date: 2023/02/03
minute: 30
---

# 高性能文本分词搜索

<ArticleInfo :frontmatter=$frontmatter></ArticleInfo>

## 背景

PolarDB 的云原生存算分离架构, 具备低廉的数据存储、高效扩展弹性、高速多机并行计算能力、高速数据搜索和处理; PolarDB 与计算算法结合, 将实现双剑合璧, 推动业务数据的价值产出, 将数据变成生产力.

本文将介绍 PolarDB 结合 jieba 分词, 实现高效率的中文分词以及中文分词搜索.

测试环境为 macos+docker, polardb 部署请参考:

- [《如何用 PolarDB 证明巴菲特的投资理念 - 包括 PolarDB 简单部署》](https://github.com/digoal/blog/blob/master/202209/20220908_02.md)

## 部署 pg_jieba on PolarDB

编译 pg_jieba

```
git clone --depth=1 https://github.com/jaiminpan/pg_jieba

cd pg_jieba

# initilized sub-project
git submodule update --init --recursive

mkdir build
cd build

cmake -DCMAKE_PREFIX_PATH=/home/postgres/tmp_basedir_polardb_pg_1100_bld ..

make
make install
```

安装 pg_jieba 插件:

```
create extension pg_jieba ;
```

测试中文分词:

```
select * from to_tsquery('jiebacfg', '是拖拉机学院手扶拖拉机专业的。不用多久，我就会升职加薪，当上CEO，走上人生巅峰。');
select * from to_tsvector('jiebacfg', '是拖拉机学院手扶拖拉机专业的。不用多久，我就会升职加薪，当上CEO，走上人生巅峰。');
select * from ts_token_type('jieba');
select * from ts_debug('jiebacfg', '是拖拉机学院手扶拖拉机专业的。不用多久，我就会升职加薪，当上CEO，走上人生巅峰。');


postgres=# create extension pg_jieba ;
CREATE EXTENSION
postgres=# select * from to_tsquery('jiebacfg', '是拖拉机学院手扶拖拉机专业的。不用多久，我就会升职加薪，当上CEO，走上人生巅峰。');
                                                            to_tsquery
----------------------------------------------------------------------------------------------------------------------------------
 '拖拉机' & '学院' & '手扶拖拉机' & '专业' & '不用' & '多久' & '会' & '升职' & '加薪' & '当上' & 'ceo' & '走上' & '人生' & '巅峰'
(1 row)

postgres=# select * from to_tsvector('jiebacfg', '是拖拉机学院手扶拖拉机专业的。不用多久，我就会升职加薪，当上CEO，走上人生巅峰。');
                                                                to_tsvector
--------------------------------------------------------------------------------------------------------------------------------------------
 'ceo':18 '不用':8 '专业':5 '人生':21 '会':13 '加薪':15 '升职':14 '多久':9 '学院':3 '巅峰':22 '当上':17 '手扶拖拉机':4 '拖拉机':2 '走上':20
(1 row)

postgres=# select * from ts_token_type('jieba');
 tokid | alias |         description
-------+-------+-----------------------------
     1 | eng   | letter
     2 | nz    | other proper noun
     3 | n     | noun
     4 | m     | numeral
     5 | i     | idiom
     6 | l     | temporary idiom
     7 | d     | adverb
     8 | s     | space
     9 | t     | time
    10 | mq    | numeral-classifier compound
    11 | nr    | person's name
    12 | j     | abbreviate
    13 | a     | adjective
    14 | r     | pronoun
    15 | b     | difference
    16 | f     | direction noun
    17 | nrt   | nrt
    18 | v     | verb
    19 | z     | z
    20 | ns    | location
    21 | q     | quantity
    22 | vn    | vn
    23 | c     | conjunction
    24 | nt    | organization
    25 | u     | auxiliary
    26 | o     | onomatopoeia
    27 | zg    | zg
    28 | nrfg  | nrfg
    29 | df    | df
    30 | p     | prepositional
    31 | g     | morpheme
    32 | y     | modal verbs
    33 | ad    | ad
    34 | vg    | vg
    35 | ng    | ng
    36 | x     | unknown
    37 | ul    | ul
    38 | k     | k
    39 | ag    | ag
    40 | dg    | dg
    41 | rr    | rr
    42 | rg    | rg
    43 | an    | an
    44 | vq    | vq
    45 | e     | exclamation
    46 | uv    | uv
    47 | tg    | tg
    48 | mg    | mg
    49 | ud    | ud
    50 | vi    | vi
    51 | vd    | vd
    52 | uj    | uj
    53 | uz    | uz
    54 | h     | h
    55 | ug    | ug
    56 | rz    | rz
(56 rows)

postgres=#  select * from ts_debug('jiebacfg', '是拖拉机学院手扶拖拉机专业的。不用多久，我就会升职加薪，当上CEO，走上人生巅峰。');
 alias |  description  |   token    | dictionaries | dictionary |   lexemes
-------+---------------+------------+--------------+------------+--------------
 v     | verb          | 是         | {jieba_stem} | jieba_stem | {}
 n     | noun          | 拖拉机     | {jieba_stem} | jieba_stem | {拖拉机}
 n     | noun          | 学院       | {jieba_stem} | jieba_stem | {学院}
 n     | noun          | 手扶拖拉机 | {jieba_stem} | jieba_stem | {手扶拖拉机}
 n     | noun          | 专业       | {jieba_stem} | jieba_stem | {专业}
 uj    | uj            | 的         | {jieba_stem} | jieba_stem | {}
 x     | unknown       | 。         | {jieba_stem} | jieba_stem | {}
 v     | verb          | 不用       | {jieba_stem} | jieba_stem | {不用}
 m     | numeral       | 多久       | {jieba_stem} | jieba_stem | {多久}
 x     | unknown       | ，         | {jieba_stem} | jieba_stem | {}
 r     | pronoun       | 我         | {jieba_stem} | jieba_stem | {}
 d     | adverb        | 就         | {jieba_stem} | jieba_stem | {}
 v     | verb          | 会         | {jieba_stem} | jieba_stem | {会}
 v     | verb          | 升职       | {jieba_stem} | jieba_stem | {升职}
 nr    | person's name | 加薪       | {jieba_stem} | jieba_stem | {加薪}
 x     | unknown       | ，         | {jieba_stem} | jieba_stem | {}
 t     | time          | 当上       | {jieba_stem} | jieba_stem | {当上}
 eng   | letter        | CEO        | {jieba_stem} | jieba_stem | {ceo}
 x     | unknown       | ，         | {jieba_stem} | jieba_stem | {}
 v     | verb          | 走上       | {jieba_stem} | jieba_stem | {走上}
 n     | noun          | 人生       | {jieba_stem} | jieba_stem | {人生}
 n     | noun          | 巅峰       | {jieba_stem} | jieba_stem | {巅峰}
 x     | unknown       | 。         | {jieba_stem} | jieba_stem | {}
(23 rows)
```

## 分词索引测试

GIN

生成 10 万条随机分词数据

```
create or replace function gen_rand_ts(tslen int) returns tsvector as $$
  select array_to_tsvector(array_agg(substring(md5(random()::text),1,8))) from generate_series(1,$1);
$$ language sql strict;

create unlogged table test (id int, vec tsvector);

insert into test select generate_series(1,100000), gen_rand_ts(24);
```

创建索引

```
create index on test using gin (vec);
```

查询测试:

```
postgres=# select * from test limit 10;
 id |                                                                                                                                   vec

----+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------
  1 | '0eed2ca6' '149ae774' '260d6cae' '2bde3230' '38ce089c' '3fdfb67c' '40bf233a' '41825567' '52de4ebb' '5708b49d' '63bdd9ea' '650f2dbf' '6d35d142' '7c711c0d' '7e4e028f' '913802bf' 'a8a14013' 'b6aa8ba4
' 'd9f595e6' 'dc054607' 'dee1a2f7' 'e5b6d7e8' 'eacb6356' 'eee81aaf'
  2 | '00bf8fbc' '117b7b5c' '1e4c8295' '2d379ff7' '2e263dcb' '48967fe5' '4f20db40' '5f7aefcd' '616cbb8e' '81d4e152' '876b2318' '8c18f4c3' '8e732b6f' '94f6b13b' '9c53cb8e' 'aedca11c' 'b56c7ed4' 'c5008853
' 'cc407ea8' 'd4f3d5a1' 'd63ca731' 'd87514ec' 'f9626af4' 'fa5b7458'
  3 | '0e3b6147' '13674c4d' '16463e9b' '32894aca' '3a15d964' '453c9a26' '54664d82' '5cb0e40d' '62c8ca30' '6d0ebc3a' '6ee0a517' '71ccfeb5' '7e75a9d5' '7f61f401' '87b5f2cb' '8f1c6274' '976dff7f' '9b7a6758
' 'af9c624e' 'e5422d57' 'ed7bb9d4' 'edc039a2' 'efe1e5fa' 'f9db8132'
  4 | '118bf21c' '2087d303' '2579c220' '3733357a' '503b50ec' '56104ea2' '573b9ea9' '58a665af' '59250bad' '86abf8a9' '8a3b5a72' '8d8bb478' 'a16b8bd8' 'ac966a06' 'af4eabd8' 'b09ccbb5' 'b2d7aac4' 'b5134f1b
' 'b5228857' 'b6836add' 'bcafbce0' 'd1ca5a3a' 'e8588e37' 'f6ffe6b0'
  5 | '01876ad5' '07a8a579' '0a33ce9e' '0b5bbdd4' '10b00efe' '118fae91' '1c12acee' '2d74f4eb' '2d99481c' '41483d1c' '6864b85e' '7ba1937f' '8a6ccb01' '9c1ae58b' 'a251fd3d' 'a936eecd' 'b560d231' 'baa6927f
' 'd78f04c6' 'dabff656' 'e5d975c0' 'f0598071' 'f819b029' 'fb202c1a'
  6 | '1c6eea85' '23f37dd9' '28151030' '319fa87f' '447ddc9d' '45dcc30a' '5269c7c2' '77184ff9' '792793c2' '81f63a78' '87b67199' '8ddc346f' '9dbc6f02' 'a4130ee7' 'a4b21300' 'a8ae9afe' 'ae54596a' 'b01e580a
' 'c17caa99' 'c7784bd5' 'd27a19ce' 'df21c10f' 'e383a9d0' 'fde1f572'
  7 | '1c6e6d6e' '209c45cf' '23415a93' '292ba393' '3d64d313' '49cf134a' '4a1a1f0d' '4c7e54a7' '4e74180a' '5054e77e' '5882f01f' '59c25e04' '69eb2f87' '6f2ed6bb' '7c830771' '81c415f5' '975f413a' 'a3dc8375
' 'a5a38d13' 'b1f83c28' 'bb62f740' 'c8bab4d1' 'd947163c' 'f3a81f80'
  8 | '0800c7b2' '0ffbe32e' '19f84945' '1c001bd3' '1f3f5826' '2e13cca1' '36ca5372' '3abc8149' '516878e9' '534357fc' '67cb7af9' '69a7849d' '8c134ad3' '8d87ed42' '96069ef5' '98bfcdbe' 'b4b0ffa1' 'bc61912a
' 'ddf1d8e6' 'e07722ea' 'e68ffbbf' 'f0751b01' 'f12cb4b9' 'fe0a7c4c'
  9 | '14588466' '1b16dfff' '25339aa7' '4874dc00' '4c6bb5bf' '510c8f7b' '59cbfb21' '70372c94' '7db5e3c2' '85f68385' '8b0e7746' '9596e2d0' '997ca4d3' '9f4df7dc' 'b1726109' 'c42ae6e4' 'dc759b2d' 'e378d2d5
' 'e956bc2b' 'ea5c6ed2' 'f0e58f77' 'f24f74b1' 'fa6df884' 'fa8edffb'
 10 | '0188c7ac' '09a75236' '15fb2eee' '1dc80e6e' '2f543594' '3559f46a' '4369adcd' '477410ed' '5df678d0' '799bc453' '80ad7901' '81871ec1' '92faa899' '94c9cf0f' '971d699f' 'a002241a' 'a1636465' 'aee34bbb
' 'b08f2f0c' 'b697c161' 'b8f290b9' 'd0acf8b4' 'd3beb05b' 'f8ca2a66'
(10 rows)

postgres=# explain select * from test where vec @@ '52de4ebb & 41825567'::tsquery ;
                                 QUERY PLAN
----------------------------------------------------------------------------
 Bitmap Heap Scan on test  (cost=36.02..43.91 rows=2 width=300)
   Recheck Cond: (vec @@ '''52de4ebb'' & ''41825567'''::tsquery)
   ->  Bitmap Index Scan on test_vec_idx  (cost=0.00..36.02 rows=2 width=0)
         Index Cond: (vec @@ '''52de4ebb'' & ''41825567'''::tsquery)
(4 rows)

postgres=# select * from test where vec @@ '52de4ebb & 41825567'::tsquery ;
 id |                                                                                                                                   vec

----+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------
  1 | '0eed2ca6' '149ae774' '260d6cae' '2bde3230' '38ce089c' '3fdfb67c' '40bf233a' '41825567' '52de4ebb' '5708b49d' '63bdd9ea' '650f2dbf' '6d35d142' '7c711c0d' '7e4e028f' '913802bf' 'a8a14013' 'b6aa8ba4
' 'd9f595e6' 'dc054607' 'dee1a2f7' 'e5b6d7e8' 'eacb6356' 'eee81aaf'
(1 row)

Time: 0.768 ms


postgres=# set enable_bitmapscan =off;
SET
Time: 0.887 ms
postgres=# explain select * from test where vec @@ '52de4ebb & 41825567'::tsquery ;
                        QUERY PLAN
-----------------------------------------------------------
 Seq Scan on test  (cost=0.00..5417.00 rows=2 width=300)
   Filter: (vec @@ '''52de4ebb'' & ''41825567'''::tsquery)
(2 rows)

Time: 1.136 ms


postgres=# select * from test where vec @@ '52de4ebb & 41825567'::tsquery ;
 id |                                                                                                                                   vec

----+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------
  1 | '0eed2ca6' '149ae774' '260d6cae' '2bde3230' '38ce089c' '3fdfb67c' '40bf233a' '41825567' '52de4ebb' '5708b49d' '63bdd9ea' '650f2dbf' '6d35d142' '7c711c0d' '7e4e028f' '913802bf' 'a8a14013' 'b6aa8ba4
' 'd9f595e6' 'dc054607' 'dee1a2f7' 'e5b6d7e8' 'eacb6356' 'eee81aaf'
(1 row)

Time: 51.815 ms
postgres=# select * from test where vec @@ '52de4ebb & 41825567'::tsquery ;
 id |                                                                                                                                   vec

----+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------
  1 | '0eed2ca6' '149ae774' '260d6cae' '2bde3230' '38ce089c' '3fdfb67c' '40bf233a' '41825567' '52de4ebb' '5708b49d' '63bdd9ea' '650f2dbf' '6d35d142' '7c711c0d' '7e4e028f' '913802bf' 'a8a14013' 'b6aa8ba4
' 'd9f595e6' 'dc054607' 'dee1a2f7' 'e5b6d7e8' 'eacb6356' 'eee81aaf'
(1 row)

Time: 49.055 ms
```

10 万条文本向量, 搜索命中 1 条. 性能参考:

- GIN 索引 0.768 ms VS 全表扫描 49.055 ms

## 参数配置

配置 PolarDB 所有计算节点, 重启 polardb 即可:

```
shared_preload_libraries = 'pg_jieba.so, ... 其他已预加载lib'   # (change requires restart)

# default_text_search_config='pg_catalog.simple'   # default value

default_text_search_config='jiebacfg'       # uncomment to make 'jiebacfg' as default
```

```
cd ~
vi tmp_master_dir_polardb_pg_1100_bld/postgresql.conf
vi tmp_replica_dir_polardb_pg_1100_bld1/postgresql.conf
vi tmp_replica_dir_polardb_pg_1100_bld2/postgresql.conf
```

```
重启polardb, 检查是否加载

postgres=# show shared_preload_libraries ;
                                                                                  shared_preload_libraries
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 pg_jieba.so,$libdir/polar_px,$libdir/polar_vfs,$libdir/polar_worker,$libdir/pg_stat_statements,$libdir/auth_delay,$libdir/auto_explain,$libdir/polar_monitor_preload,$libdir/polar_stat_sql
(1 row)

select * from to_tsvector('是拖拉机学院手扶拖拉机专业的。不用多久，我就会升职加薪，当上CEO，走上人生巅峰。');

postgres=# select * from to_tsvector('是拖拉机学院手扶拖拉机专业的。不用多久，我就会升职加薪，当上CEO，走上人生巅峰。');
                                                                to_tsvector
--------------------------------------------------------------------------------------------------------------------------------------------
 'ceo':18 '不用':8 '专业':5 '人生':21 '会':13 '加薪':15 '升职':14 '多久':9 '学院':3 '巅峰':22 '当上':17 '手扶拖拉机':4 '拖拉机':2 '走上':20
(1 row)
```

感谢志铭贡献的中文分词插件 pg_jieba.

## 参考

https://github.com/jaiminpan/pg_jieba

- [《PostgreSQL 应用开发解决方案最佳实践系列课程 - 1. 中文分词与模糊查询》](https://github.com/digoal/blog/blob/master/202105/20210502_01.md)
- [《PostgreSQL 一种高性能中文分词器 - friso》](https://github.com/digoal/blog/blob/master/202003/20200324_17.md)
- [《PostgreSQL 芬兰语 分词插件 - dict_voikko》](https://github.com/digoal/blog/blob/master/202003/20200324_06.md)
- [《PostgreSQL+MySQL 联合解决方案 - 第 12 课视频 - 全文检索、中文分词、模糊查询、相似文本查询》](https://github.com/digoal/blog/blob/master/202001/20200116_01.md)
- [《PostgreSQL 中英文混合分词特殊规则(中文单字、英文单词) - 中英分明》](https://github.com/digoal/blog/blob/master/201711/20171104_03.md)
- [《如何解决数据库分词的拼写纠正问题 - PostgreSQL Hunspell 字典 复数形容词动词等变异还原》](https://github.com/digoal/blog/blob/master/201612/20161206_01.md)
- [《聊一聊双十一背后的技术 - 毫秒分词算啥, 试试正则和相似度》](https://github.com/digoal/blog/blob/master/201611/20161118_01.md)
- [《聊一聊双十一背后的技术 - 分词和搜索》](https://github.com/digoal/blog/blob/master/201611/20161115_01.md)
- [《PostgreSQL 如何高效解决 按任意字段分词检索的问题 - case 1》](https://github.com/digoal/blog/blob/master/201607/20160725_05.md)
- [《如何加快 PostgreSQL 结巴分词 pg_jieba 加载速度》](https://github.com/digoal/blog/blob/master/201607/20160725_02.md)
- [《使用阿里云 PostgreSQL zhparser 中文分词时不可不知的几个参数》](https://github.com/digoal/blog/blob/master/201603/20160310_01.md)
- [《PostgreSQL Greenplum 结巴分词(by plpython)》](https://github.com/digoal/blog/blob/master/201508/20150824_01.md)
- [《NLPIR 分词准确率接近 98.23%》](https://github.com/digoal/blog/blob/master/201508/20150821_01.md)
- [《PostgreSQL 使用 nlpbamboo chinesecfg 中文分词》](https://github.com/digoal/blog/blob/master/201206/20120621_01.md)

如果想了解 GIN 索引的原理请参考:

- [《PostgreSQL GIN 索引实现原理》](https://github.com/digoal/blog/blob/master/201702/20170204_01.md)
- [《从难缠的模糊查询聊开 - PostgreSQL 独门绝招之一 GIN , GiST , SP-GiST , RUM 索引原理与技术背景》](https://github.com/digoal/blog/blob/master/201612/20161231_01.md)
