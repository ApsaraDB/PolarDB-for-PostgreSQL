---
author: digoal
date: 2023/02/03
minute: 20
---

# 当搜索遇到排序

<ArticleInfo :frontmatter=$frontmatter></ArticleInfo>

## 背景

PolarDB 的云原生存算分离架构, 具备低廉的数据存储、高效扩展弹性、高速多机并行计算能力、高速数据搜索和处理; PolarDB 与计算算法结合, 将实现双剑合璧, 推动业务数据的价值产出, 将数据变成生产力.

本文将介绍 PolarDB 开源版通过 rum 实现高效率搜索和高效率排序的解决方案

测试环境为 macos+docker, polardb 部署请参考:

- [《如何用 PolarDB 证明巴菲特的投资理念 - 包括 PolarDB 简单部署》](https://github.com/digoal/blog/blob/master/202209/20220908_02.md)

## 为什么 DBA 最怕搜索需求

如果你要做个搜索功能, 用户在搜索时通常会有多个满足搜索条件的结果, 这些结果往往还需要根据其他条件进行排序, 这种搜索在一般的数据库中, 只能全表扫描实现, 性能差到极点. 随便几个查询就会把数据库 CPU、内存、IO 资源耗光, 影响业务. 最后一般是 DBA 背锅, 实际上是数据库没有好的算法和存储结构, 所以这锅有点冤枉.

根据代际转移理论, 今天我们都是提前消费未来的创造力, 而以前通过暴力(堆机器)来解决问题的方法, 放在今天必须要有更高效、节能的方式来解决.

这也是今天我想要跟大家介绍的《rum+PolarDB 实现高效率搜索和高效率排序的解决方案》, 性能轻松提升 70 倍(大数据量性能提升还会更多). 以后有这种需求, DBA 就可以硬气了.

## rum 的原理

rum 是个倒排结构, 但是在期 elements 里面又增加了 addon value 的存储, 每个匹配的 element 条目再根据 addon column value 重新排序编排其 ctid(s).

同时由于多值列本身的等值匹配较为常见, 所以 rum 有个变种, element 可以被 hash 化来进行存储, 提高索引存储效率. hash 化之后仅仅支持 element 的等值匹配, 类似的 hash index 也一样.

1、rum storage:

1\.1、ops

- `k,v结构: ele,ctid(s)`

1\.2、addon_ops

- `k,v结构: ele,kv(s)`
  - `kv(s)结构: addon,ctid(s)  (根据addon value构建btree)`

1\.3、hash_ops

- `k,v结构: ele_hash:ctid(s)`

1\.4、hash_addon_ops

- `k,v结构: ele_hash:kv(s)`
  - `kv(s)结构: addon:ctid(s)  (根据addon value构建btree)`

hash ops 和 hash addon ops 不支持按 prefix 搜索 ele, 因为 element 已经转换成 hashvalue 存储在索引中, 只能做等值匹配.

hash ops 支持按距离排序(`<=>`), 因为距离计算取决于等值匹配到的 ele 比例. 可以想象, 不支持有方向的排序和搜索, 例如 prefix search 和 prefix sort (`<=| and |=>`).

2、sort compute:

2\.1、ele key sort:

- `ele distance : dist_fun(ctid , $)`

2\.2、addon ele sort:

- `addon distance : dist_fun(ctid , $)`

按 ele 排序 或 匹配 ele 条件后按 addon 排序

hash ops 可以按 addon 排序(prefix, 相似都支持. 包括 <=>, <=| and |=> ), 因为 addon column 未被 hash 化.

## 将 rum 部署到 PolarDB 中

```
git clone --depth 1 https://github.com/postgrespro/rum

cd rum

USE_PGXS=1 make
USE_PGXS=1 make install
USE_PGXS=1 make installcheck


[postgres@aa25c5be9681 rum]$ USE_PGXS=1 make installcheck
/home/postgres/tmp_basedir_polardb_pg_1100_bld/lib/pgxs/src/makefiles/../../src/test/regress/pg_regress --inputdir=./ --bindir='/home/postgres/tmp_basedir_polardb_pg_1100_bld/bin'      --dbname=contrib_regression security rum rum_validate rum_hash ruminv timestamp orderby orderby_hash altorder altorder_hash limits int2 int4 int8 float4 float8 money oid time timetz date interval macaddr inet cidr text varchar char bytea bit varbit numeric rum_weight array
(using postmaster on 127.0.0.1, default port)
============== dropping database "contrib_regression" ==============
NOTICE:  database "contrib_regression" does not exist, skipping
DROP DATABASE
============== creating database "contrib_regression" ==============
CREATE DATABASE
ALTER DATABASE
============== running regression test queries        ==============
test security                     ... ok
test rum                          ... ok
test rum_validate                 ... ok
test rum_hash                     ... ok
test ruminv                       ... ok
test timestamp                    ... ok
test orderby                      ... ok
test orderby_hash                 ... ok
test altorder                     ... ok
test altorder_hash                ... ok
test limits                       ... ok
test int2                         ... ok
test int4                         ... ok
test int8                         ... ok
test float4                       ... ok
test float8                       ... ok
test money                        ... ok
test oid                          ... ok
test time                         ... ok
test timetz                       ... ok
test date                         ... ok
test interval                     ... ok
test macaddr                      ... ok
test inet                         ... ok
test cidr                         ... ok
test text                         ... ok
test varchar                      ... ok
test char                         ... ok
test bytea                        ... ok
test bit                          ... ok
test varbit                       ... ok
test numeric                      ... ok
test rum_weight                   ... ok
test array                        ... ok


===========================================================
 All 34 tests passed.

 POLARDB:
 All 34 tests, 0 tests in ignore, 0 tests in polar ignore.
===========================================================





psql
create extension rum;
```

## 场景化讲解

画像业务、搜索业务:

- 标签匹配+权重排序
- 标签匹配+时间排序

例如文章搜索, 算法举例:

- 关注文章内容的相关性, 同时要按文章的发布时间顺序排序返回前 10 条.
- 关注文章内容的相关性, 同时要按文章的权重(如按广告费等计算出来的权重)顺序排序返回前 10 条.

1、创建测试表

```
create table tbl (id int, info tsvector, weight float4);
```

2、写入测试数据

```
insert into tbl select id, to_tsvector('hello i am tom lane, i love postgresql'), random()*100 from generate_series(1,100000) id;
insert into tbl select id, to_tsvector('hello i am digoal, i love polardb at aliyun at china.'), random()*100 from generate_series(1,2000) id;
```

3、创建 rum 索引, 将权重 attach 到 rum 索引的 ctid(s)中.

```
create index on tbl using rum (info rum_tsvector_hash_addon_ops, weight) with (attach = 'weight', to = 'info');
```

4、使用 rum 高效搜索和排序

4\.1、匹配字符串, 并且权重越大越好的?

```
select *, weight <=| '100000'::float4 from tbl where info @@ 'digoal&polardb' ORDER BY weight <=| '100000'::float4 limit 10;

  id  |                              info                               | weight  |     ?column?
------+-----------------------------------------------------------------+---------+------------------
 1078 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 | 99.9273 | 99900.0727005005
  877 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 | 99.8128 |  99900.187171936
  118 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 | 99.8049 | 99900.1951217651
  881 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 | 99.4699 | 99900.5300979614
 1257 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 | 99.4317 | 99900.5682678223
  459 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 | 99.3815 | 99900.6185073853
 1306 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 | 99.3271 | 99900.6729354858
  300 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 | 99.2863 |  99900.713722229
  313 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 | 99.2731 | 99900.7268676758
  618 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 | 99.2371 | 99900.7629318237
(10 rows)



select *, weight <=| '100000'::float4 from tbl where info @@ 'digoal' ORDER BY weight <=| '100000'::float4 limit 10;

  id  |                              info                               | weight  |     ?column?
------+-----------------------------------------------------------------+---------+------------------
 1078 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 | 99.9273 | 99900.0727005005
  877 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 | 99.8128 |  99900.187171936
  118 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 | 99.8049 | 99900.1951217651
  881 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 | 99.4699 | 99900.5300979614
 1257 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 | 99.4317 | 99900.5682678223
  459 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 | 99.3815 | 99900.6185073853
 1306 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 | 99.3271 | 99900.6729354858
  300 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 | 99.2863 |  99900.713722229
  313 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 | 99.2731 | 99900.7268676758
  618 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 | 99.2371 | 99900.7629318237
(10 rows)
```

```
postgres=# explain (analyze,verbose,timing,costs,buffers) select *, weight <=| '100000'::float4 from tbl where info @@ 'digoal&polardb' ORDER BY weight <=| '100000'::float4 limit 10;
                                                                QUERY PLAN
------------------------------------------------------------------------------------------------------------------------------------------
 Limit  (cost=24.00..40.06 rows=3 width=48) (actual time=2.081..2.097 rows=10 loops=1)
   Output: id, info, weight, ((weight <=| '100000'::real))
   Buffers: shared hit=21
   ->  Index Scan using tbl_info_weight_idx on public.tbl  (cost=24.00..40.06 rows=3 width=48) (actual time=2.079..2.093 rows=10 loops=1)
         Output: id, info, weight, (weight <=| '100000'::real)
         Index Cond: (tbl.info @@ '''digoal'' & ''polardb'''::tsquery)
         Order By: (tbl.weight <=| '100000'::real)
         Buffers: shared hit=21
 Planning Time: 0.160 ms
 Execution Time: 2.149 ms
(10 rows)



postgres=# explain (analyze,verbose,timing,costs,buffers) select *, weight <=| '100000'::float4 from tbl where info @@ 'digoal' ORDER BY weight <=| '100000'::float4 limit 10;
                                                                  QUERY PLAN
-----------------------------------------------------------------------------------------------------------------------------------------------
 Limit  (cost=12.00..35.81 rows=10 width=89) (actual time=1.598..1.616 rows=10 loops=1)
   Output: id, info, weight, ((weight <=| '100000'::real))
   Buffers: shared hit=15
   ->  Index Scan using tbl_info_weight_idx on public.tbl  (cost=12.00..4869.90 rows=2040 width=89) (actual time=1.596..1.612 rows=10 loops=1)
         Output: id, info, weight, (weight <=| '100000'::real)
         Index Cond: (tbl.info @@ '''digoal'''::tsquery)
         Order By: (tbl.weight <=| '100000'::real)
         Buffers: shared hit=15
 Planning Time: 0.104 ms
 Execution Time: 1.655 ms
(10 rows)
```

4\.2、反过来排, 权重越小越好的?

```
select *, weight |=> '-1'::float4 from tbl where info @@ 'digoal' ORDER BY weight |=> '-1'::float4 limit 10;

  id  |                              info                               |  weight   |     ?column?
------+-----------------------------------------------------------------+-----------+------------------
  554 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 | 0.0363963 | 1.03639627248049
  192 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 | 0.0421133 | 1.04211333394051
  757 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 |  0.124864 | 1.12486390769482
 1855 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 |  0.125145 |  1.1251448392868
  191 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 |  0.134997 |  1.1349972486496
   60 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 |  0.179037 |  1.1790367513895
 1580 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 |   0.21992 | 1.21991994976997
 1432 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 |  0.244062 | 1.24406225979328
  719 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 |  0.244155 |  1.2441546022892
   81 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 |  0.329849 | 1.32984939217567
(10 rows)


postgres=# explain select *, weight |=> '-1'::float4 from tbl where info @@ 'digoal' ORDER BY weight |=> '-1'::float4 limit 10;
                                         QUERY PLAN
---------------------------------------------------------------------------------------------
 Limit  (cost=12.00..35.53 rows=10 width=89)
   ->  Index Scan using tbl_info_weight_idx on tbl  (cost=12.00..4955.27 rows=2101 width=89)
         Index Cond: (info @@ '''digoal'''::tsquery)
         Order By: (weight |=> '-1'::real)
(4 rows)
```

4\.3、或者离某个指定权重点越近越好的?

```
postgres=# select *, weight <=> '50'::float4 from tbl where info @@ 'digoal' ORDER BY weight <=> '50'::float4 limit 10;
  id  |                              info                               | weight  |      ?column?
------+-----------------------------------------------------------------+---------+--------------------
   38 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 | 49.9803 | 0.0197181701660156
 1590 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 | 50.1099 |  0.109916687011719
 1153 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 | 50.1187 |  0.118724822998047
  884 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 | 50.1466 |  0.146591186523438
 1329 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 | 50.1551 |  0.155113220214844
  303 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 | 49.8312 |  0.168792724609375
  568 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 | 50.1816 |  0.181587219238281
 1706 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 | 49.8142 |  0.185768127441406
 1136 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 | 49.8095 |  0.190475463867188
 1838 | 'aliyun':9 'china':11 'digoal':4 'hello':1 'love':6 'polardb':7 | 49.7829 |  0.217105865478516
(10 rows)

postgres=# explain select *, weight <=> '50'::float4 from tbl where info @@ 'digoal' ORDER BY weight <=> '50'::float4 limit 10;
                                         QUERY PLAN
---------------------------------------------------------------------------------------------
 Limit  (cost=12.00..35.53 rows=10 width=89)
   ->  Index Scan using tbl_info_weight_idx on tbl  (cost=12.00..4955.27 rows=2101 width=89)
         Index Cond: (info @@ '''digoal'''::tsquery)
         Order By: (weight <=> '50'::real)
(4 rows)
```

如果没有 rum, 那么这个搜索需要进行全表匹配, 性能非常差.

仅使用 GIN 则只能对多值元素进行搜索, 无法实现权重排序的索引加速, 需要回表后排序, 性能也比较差.

```
postgres=# select relpages from pg_class where relname='tbl';
 relpages
----------
     1462
(1 row)


postgres=# set enable_indexscan = off;

postgres=# set enable_bitmapscan = off;

postgres=# explain (analyze,verbose,timing,costs,buffers) select *, weight <=| '100000'::float4 from tbl where info @@ 'digoal' ORDER BY weight <=| '100000'::float4 limit 10;
                                                         QUERY PLAN
----------------------------------------------------------------------------------------------------------------------------
 Limit  (cost=2787.65..2787.68 rows=10 width=89) (actual time=27.140..27.144 rows=10 loops=1)
   Output: id, info, weight, ((weight <=| '100000'::real))
   Buffers: shared hit=1462
   ->  Sort  (cost=2787.65..2792.91 rows=2101 width=89) (actual time=27.139..27.141 rows=10 loops=1)
         Output: id, info, weight, ((weight <=| '100000'::real))
         Sort Key: ((tbl.weight <=| '100000'::real))
         Sort Method: top-N heapsort  Memory: 27kB
         Buffers: shared hit=1462
         ->  Seq Scan on public.tbl  (cost=0.00..2742.25 rows=2101 width=89) (actual time=25.883..26.810 rows=2000 loops=1)
               Output: id, info, weight, (weight <=| '100000'::real)
               Filter: (tbl.info @@ '''digoal'''::tsquery)
               Rows Removed by Filter: 100000
               Buffers: shared hit=1462
 Planning Time: 0.078 ms
 Execution Time: 27.171 ms
(15 rows)
```

rum 实现了精准多值列搜索, 同时支持索引内排序, 性能最佳.

以上例子才 10.2 万条记录, 使用 rum 扫描的数据块已减少 70 倍, 数据量再增大, 扫描的数据块将更少, 性能提升将会更加明显.

## 参考

- [《重新发现 PostgreSQL 之美 - 9 面向多值列的倒排索引 GIN|RUM》](https://github.com/digoal/blog/blob/master/202105/20210531_02.md)
- [《PostgreSQL RUM 索引原理》](https://github.com/digoal/blog/blob/master/202011/20201128_02.md)
- [《PostgreSQL 任意字段组合搜索 - rum 或 多字段 bitmapscan 对比》](https://github.com/digoal/blog/blob/master/202005/20200520_02.md)
- [《PostgreSQL rum 索引结构 - 比 gin posting list|tree 的 ctid(行号)多了 addition info》](https://github.com/digoal/blog/blob/master/201907/20190706_01.md)
- [《PostgreSQL 相似搜索插件介绍大汇总 (cube,rum,pg_trgm,smlar,imgsmlr,pg_similarity) (rum,gin,gist)》](https://github.com/digoal/blog/blob/master/201809/20180904_01.md)
- [《PostgreSQL 设计优化 case - 大宽表任意字段组合查询索引如何选择(btree, gin, rum) - (含单个索引列数超过 32 列的方法)》](https://github.com/digoal/blog/blob/master/201808/20180803_01.md)
- [《PostgreSQL ADHoc(任意字段组合)查询(rums 索引加速) - 非字典化，普通、数组等组合字段生成新数组》](https://github.com/digoal/blog/blob/master/201805/20180518_02.md)
- [《PostgreSQL ADHoc(任意字段组合)查询 与 字典化 (rum 索引加速) - 实践与方案 1 - 菜鸟 某仿真系统》](https://github.com/digoal/blog/blob/master/201802/20180228_01.md)
- [《PostgreSQL 结合余弦、线性相关算法 在文本、图片、数组相似 等领域的应用 - 3 rum, smlar 应用场景分析》](https://github.com/digoal/blog/blob/master/201701/20170116_04.md)
- [《从难缠的模糊查询聊开 - PostgreSQL 独门绝招之一 GIN , GiST , SP-GiST , RUM 索引原理与技术背景》](https://github.com/digoal/blog/blob/master/201612/20161231_01.md)
- [《PostgreSQL 全文检索加速 快到没有朋友 - RUM 索引接口(潘多拉魔盒)》](https://github.com/digoal/blog/blob/master/201610/20161019_01.md)
- [《[直播]为什么饿了么网上订餐不会凉凉 & 牛顿发现万有引力有关?》](https://github.com/digoal/blog/blob/master/202010/20201018_01.md)
- [《HTAP 数据库 PostgreSQL 场景与性能测试之 47 - (OLTP 多模优化) 空间应用 - 高并发空间位置更新、多属性 KNN 搜索并测（含空间索引）末端配送、新零售类项目》](https://github.com/digoal/blog/blob/master/201711/20171107_48.md)
