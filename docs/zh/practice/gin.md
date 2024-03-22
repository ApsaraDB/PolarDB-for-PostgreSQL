---
author: digoal
date: 2023/02/03
minute: 20
---

# 用户画像、实时精准营销业务加速实践

<ArticleInfo :frontmatter=$frontmatter></ArticleInfo>

## 背景

PolarDB 的云原生存算分离架构, 具备低廉的数据存储、高效扩展弹性、高速多机并行计算能力、高速数据搜索和处理; PolarDB 与计算算法结合, 将实现双剑合璧, 推动业务数据的价值产出, 将数据变成生产力.

本文将介绍使用 PolarDB 开源版高效率解决用户画像、实时精准营销类业务需求

测试环境为 macOS+docker, PolarDB 部署请参考下文:

- [《如何用 PolarDB 证明巴菲特的投资理念 - 包括 PolarDB 简单部署》](https://github.com/digoal/blog/blob/master/202209/20220908_02.md)

## 原理

1、场景介绍:

用户画像通常被用于精准营销场景, 根据用户的行为分析并给用户打标签, 充分了解用户属性的诉求可以更好的实现供需连, 例如推送用户之所需, 根据市场需求进行备货等等.

数据分析时解决供需问题, 节省社会成本, 提升社会效率的有力手段.

2、难点:

- 标签多, 标签的多少动态增减, 需要大量 DDL, 不适合大宽表
- 标签过滤需要全表扫描, 非常慢
- 标签组合(包含、不包含等等), 过滤效率低
- 每个用户的标签数量可能不一样, 不适合结构化存储

3、PolarDB 如何解决这个问题:

- 画像存储: 采用数组, 解决了动态增减标签, 个性化标签的需求, 不涉及结构变更.
- GIN 索引: 解决高效率组合搜索过滤问题
- fast update: 解决实时打标的效率问题. (后台异步 merge gin index)

## 场景模拟和架构设计实践

1、创建模拟生成标签的函数

```
create or replace function gen_arr(normal int, hot int) returns int[] as $$
  select array(select (100000*random())::int+500 from generate_series(1,$1)) || array(select (500*random())::int from generate_series(1,$2));
$$ language sql strict;
```

体现个性标签+热门标签, 个性标签 10 万个, 热门标签 500 个.

例如 20 个个性标签+10 个热门标签, 组成了某个人的画像.

```
postgres=# select gen_arr(22,10);
                                                                                 gen_arr
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 {84735,45437,35238,71110,22339,86790,89232,8340,851,50577,6600,53760,63854,95377,28505,12781,34180,56262,10835,53417,42865,67843,235,401,265,372,304,132,309,140,38,254}
(1 row)
```

2、创建测试表, 生产 500 万用户画像数据, 并创建 gin 索引

```
create table tbl (uid int8, tag int[]);

insert into tbl select uid, gen_arr(22,10) from generate_series(1,5000000) uid;

create index on tbl using gin (tag);
```

3、圈选用户测试, 使用 GIN 倒排索引.

```
postgres=# explain select count(*) from tbl where tag @> '{100}'::int[];
                                      QUERY PLAN
--------------------------------------------------------------------------------------
 Aggregate  (cost=24422.02..24422.03 rows=1 width=8)
   ->  Bitmap Heap Scan on tbl  (cost=210.25..24359.52 rows=25000 width=0)
         Recheck Cond: (tag @> '{100}'::integer[])
         ->  Bitmap Index Scan on tbl_tag_idx  (cost=0.00..204.00 rows=25000 width=0)
               Index Cond: (tag @> '{100}'::integer[])
(5 rows)
```

圈选某个热门标签

```
postgres=# select count(*) from tbl where tag @> '{100}'::int[];
 count
-------
 99427
(1 row)

Time: 693.697 ms
```

圈选某些热门标签

```
postgres=# select count(*) from tbl where tag @> '{100,50}'::int[];
 count
-------
  1841
(1 row)

Time: 19.100 ms
```

圈选某个个性标签

```
postgres=# select count(*) from tbl where tag @> '{600}'::int[];
 count
-------
  1042
(1 row)

Time: 69.772 ms
```

圈选某些个性标签

```
postgres=# select count(*) from tbl where tag @> '{600,700}'::int[];
 count
-------
     0
(1 row)

Time: 11.029 ms

postgres=# select count(*) from tbl where tag @> '{600,680}'::int[];
 count
-------
     0
(1 row)

Time: 1.050 ms
```

圈选某个个性标签, 并排除某个热门标签

```
postgres=# explain select count(*) from tbl where tag @> '{600}'::int[] and not (tag @> '{60}'::int[]);
                                      QUERY PLAN
--------------------------------------------------------------------------------------
 Aggregate  (cost=23537.17..23537.18 rows=1 width=8)
   ->  Bitmap Heap Scan on tbl  (cost=200.64..23478.51 rows=23463 width=0)
         Recheck Cond: (tag @> '{600}'::integer[])
         Filter: (NOT (tag @> '{60}'::integer[]))
         ->  Bitmap Index Scan on tbl_tag_idx  (cost=0.00..194.78 rows=23917 width=0)
               Index Cond: (tag @> '{600}'::integer[])
(6 rows)

Time: 0.570 ms

postgres=# select count(*) from tbl where tag @> '{600}'::int[] and not (tag @> '{60}'::int[]);
 count
-------
  1018
(1 row)

Time: 3.715 ms
```

圈选某个热门标签, 并排除某个个性标签

```
postgres=# explain select count(*) from tbl where tag @> '{60}'::int[] and not (tag @> '{600}'::int[]);
                                      QUERY PLAN
--------------------------------------------------------------------------------------
 Aggregate  (cost=109198.79..109198.80 rows=1 width=8)
   ->  Bitmap Heap Scan on tbl  (cost=778.85..108962.84 rows=94380 width=0)
         Recheck Cond: (tag @> '{60}'::integer[])
         Filter: (NOT (tag @> '{600}'::integer[]))
         ->  Bitmap Index Scan on tbl_tag_idx  (cost=0.00..755.26 rows=94834 width=0)
               Index Cond: (tag @> '{60}'::integer[])
(6 rows)

Time: 1.714 ms

postgres=# select count(*) from tbl where tag @> '{60}'::int[] and not (tag @> '{600}'::int[]);
 count
-------
 98930
(1 row)

Time: 693.436 ms
```

在笔记本上, 性能已经起飞, 何况是高端机器?
