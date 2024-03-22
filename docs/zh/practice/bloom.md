---
author: digoal
date: 2023/02/03
minute: 10
---

# bloom index 实现任意字段组合条件过滤

<ArticleInfo :frontmatter=$frontmatter></ArticleInfo>

## 背景

PolarDB 的云原生存算分离架构, 具备低廉的数据存储、高效扩展弹性、高速多机并行计算能力、高速数据搜索和处理; PolarDB 与计算算法结合, 将实现双剑合璧, 推动业务数据的价值产出, 将数据变成生产力.

本文将介绍使用 PolarDB 开源版 bloom filter index 实现任意字段组合条件过滤

测试环境为 macOS+docker, PolarDB 部署请参考下文:

- [《如何用 PolarDB 证明巴菲特的投资理念 - 包括 PolarDB 简单部署》](https://github.com/digoal/blog/blob/master/202209/20220908_02.md)

## 原理

任意字段组合条件过滤, 这种操作通常出现在数据分析场景, 例如 BI 分析师, 根据需要过滤数, 但是通常需要大量试错, 所以需求就变成了任意组合.

为了加速任意字段过滤, 需要对每一种组合创建索引, 当然 PolarDB PG 也支持多个 btree index 的 bitmap scan, 所以只需要在每个字段上都创建一个索引即可. 但是即使是这样, 弊端依旧存在:

- 每列索引加起来占用空间也是比较大的,
- 而且索引越多, 对 dml 操作带来的 rt 就越大, 影响写入吞吐.

为了解决这个问题, bloom filter 应运而生, 一个索引, 支持所有字段任意组合条件过滤, (注意, bloom filter 仅支持等值组合(原理见下面)).

- bloom filter 为 N bit 的 hash value,
- 每个值经过 hash 后, 映射到 hash value 中的 n 个 bit 内. 例如 2 个 bit, hello (11)映射到 1,8, world (11)映射到 2,8.

查询 hello 是否存在时, 只需要判断对应 bit 上的值是否和 hash value 一致, 即可.

bloom 的 lossy 特性:  
不存在 一定 不存在, 表示有 map bit=0 的, 不存在表示这条记录不包含这个 value.  
存在 不一定 存在, 表示 map bits=1, 但是这些 bits 可能有其他 columns value 映射过去的 1(你也可以理解为 bit 冲突), 所以需要二次 recheck 才能判断一定存在.

为什么会产生 bit 冲突?

- 例如 80 个 bit, 存储 32 列的值, 每列 2 个 bit. hash 时, 不同 column value 可能会同时 mapping 到某一个位置的 bit. 字段越多, 冲突概率越大.

不管怎么样, bloom filter 这种方法在任意字段组合查询的使用中还是非常廉价高效的.

## 场景模拟和架构设计实践

创建测试表, 写入 1000 万条记录

```
=# CREATE TABLE tbloom AS
   SELECT
     (random() * 1000000)::int as i1,
     (random() * 1000000)::int as i2,
     (random() * 1000000)::int as i3,
     (random() * 1000000)::int as i4,
     (random() * 1000000)::int as i5,
     (random() * 1000000)::int as i6
   FROM
  generate_series(1,10000000);
SELECT 10000000
```

全表扫描性能

```
=# EXPLAIN ANALYZE SELECT * FROM tbloom WHERE i2 = 898732 AND i5 = 123451;
                                              QUERY PLAN
-------------------------------------------------------------------​-----------------------------------
 Seq Scan on tbloom  (cost=0.00..2137.14 rows=3 width=24) (actual time=16.971..16.971 rows=0 loops=1)
   Filter: ((i2 = 898732) AND (i5 = 123451))
   Rows Removed by Filter: 100000
 Planning Time: 0.346 ms
 Execution Time: 16.988 ms
(5 rows)
```

创建 bloom index, 覆盖 6 个字段, 仅仅消耗 1.5MB. 查询性能提升近 50 倍.

```
create extension bloom ;

=# CREATE INDEX bloomidx ON tbloom USING bloom (i1, i2, i3, i4, i5, i6);
CREATE INDEX
=# SELECT pg_size_pretty(pg_relation_size('bloomidx'));
 pg_size_pretty
----------------
 1584 kB
(1 row)
=# EXPLAIN ANALYZE SELECT * FROM tbloom WHERE i2 = 898732 AND i5 = 123451;
                                                     QUERY PLAN
-------------------------------------------------------------------​--------------------------------------------------
 Bitmap Heap Scan on tbloom  (cost=1792.00..1799.69 rows=2 width=24) (actual time=0.388..0.388 rows=0 loops=1)
   Recheck Cond: ((i2 = 898732) AND (i5 = 123451))
   Rows Removed by Index Recheck: 29
   Heap Blocks: exact=28
   ->  Bitmap Index Scan on bloomidx  (cost=0.00..1792.00 rows=2 width=0) (actual time=0.356..0.356 rows=29 loops=1)
         Index Cond: ((i2 = 898732) AND (i5 = 123451))
 Planning Time: 0.099 ms
 Execution Time: 0.408 ms
(8 rows)
```

改成普通 btree 索引, 每个字段创建 1 个, 总共 6 个索引. 多个字段组合查询会采用 bitmap and|or scan. 效率最高, 但是占用空间巨大, 同时对写入吞吐性能影响较大.

```
=# CREATE INDEX btreeidx1 ON tbloom (i1);
CREATE INDEX
=# CREATE INDEX btreeidx2 ON tbloom (i2);
CREATE INDEX
=# CREATE INDEX btreeidx3 ON tbloom (i3);
CREATE INDEX
=# CREATE INDEX btreeidx4 ON tbloom (i4);
CREATE INDEX
=# CREATE INDEX btreeidx5 ON tbloom (i5);
CREATE INDEX
=# CREATE INDEX btreeidx6 ON tbloom (i6);
CREATE INDEX
=# EXPLAIN ANALYZE SELECT * FROM tbloom WHERE i2 = 898732 AND i5 = 123451;
                                                        QUERY PLAN
-------------------------------------------------------------------​--------------------------------------------------------
 Bitmap Heap Scan on tbloom  (cost=24.34..32.03 rows=2 width=24) (actual time=0.028..0.029 rows=0 loops=1)
   Recheck Cond: ((i5 = 123451) AND (i2 = 898732))
   ->  BitmapAnd  (cost=24.34..24.34 rows=2 width=0) (actual time=0.027..0.027 rows=0 loops=1)
         ->  Bitmap Index Scan on btreeidx5  (cost=0.00..12.04 rows=500 width=0) (actual time=0.026..0.026 rows=0 loops=1)
               Index Cond: (i5 = 123451)
         ->  Bitmap Index Scan on btreeidx2  (cost=0.00..12.04 rows=500 width=0) (never executed)
               Index Cond: (i2 = 898732)
 Planning Time: 0.491 ms
 Execution Time: 0.055 ms
(9 rows)
```

## 参考

- [《重新发现 PostgreSQL 之美 - 14 bloom 布隆过滤器索引》](https://github.com/digoal/blog/blob/master/202106/20210605_07.md)
- [《PostgreSQL 14 preview - BRIN (典型 IoT 时序场景) 块级索引支持 bloom filter - 随机,大量 distinct value, 等值查询》](https://github.com/digoal/blog/blob/master/202103/20210326_02.md)
- [《PostgreSQL bloom 索引原理》](https://github.com/digoal/blog/blob/master/202011/20201128_04.md)
- [《UID 编码优化 - 用户画像前置规则 (bloom, 固定算法等)》](https://github.com/digoal/blog/blob/master/201911/20191130_01.md)
- [《PostgreSQL bloom filter index 扩展 for bigint》](https://github.com/digoal/blog/blob/master/201810/20181003_02.md)
- [《PostgreSQL 11 preview - bloom filter 误报率评估测试及如何降低误报 - 暨 bloom filter 应用于 HEAP 与 INDEX 的一致性检测》](https://github.com/digoal/blog/blob/master/201804/20180409_01.md)
- [《PostgreSQL 11 preview - BRIN 索引接口功能扩展(BLOOM FILTER、min max 分段)》](https://github.com/digoal/blog/blob/master/201803/20180323_05.md)
- [《PostgreSQL 9.6 黑科技 bloom 算法索引，一个索引支撑任意列组合查询》](https://github.com/digoal/blog/blob/master/201605/20160523_01.md)
- [《PostgreSQL 应用开发解决方案最佳实践系列课程 - 7. 标签搜索和圈选、相似搜索和圈选、任意字段组合搜索和圈选系统》](https://github.com/digoal/blog/blob/master/202105/20210510_01.md)
- [《PostgreSQL 任意字段组合搜索 - rum 或 多字段 bitmapscan 对比》](https://github.com/digoal/blog/blob/master/202005/20200520_02.md)
- [《PostgreSQL+MySQL 联合解决方案 - 第 10 课视频 - 任意字段、维度组合搜索（含 GIS、数组、全文检索等属性）》](https://github.com/digoal/blog/blob/master/202001/20200114_01.md)
- [《PostgreSQL 任意字段组合查询 - 含 128 字段，1 亿记录，任意组合查询，性能》](https://github.com/digoal/blog/blob/master/201903/20190320_02.md)
- [《PostgreSQL 任意字段数组合 AND\OR 条件，指定返回结果条数，构造测试数据算法举例》](https://github.com/digoal/blog/blob/master/201809/20180905_03.md)
- [《PostgreSQL 设计优化 case - 大宽表任意字段组合查询索引如何选择(btree, gin, rum) - (含单个索引列数超过 32 列的方法)》](https://github.com/digoal/blog/blob/master/201808/20180803_01.md)
- [《PostgreSQL ADHoc(任意字段组合)查询(rums 索引加速) - 非字典化，普通、数组等组合字段生成新数组》](https://github.com/digoal/blog/blob/master/201805/20180518_02.md)
- [《PostgreSQL 实践 - 实时广告位推荐 2 (任意字段组合、任意维度组合搜索、输出 TOP-K)》](https://github.com/digoal/blog/blob/master/201804/20180424_04.md)
- [《PostgreSQL 实践 - 实时广告位推荐 1 (任意字段组合、任意维度组合搜索、输出 TOP-K)》](https://github.com/digoal/blog/blob/master/201804/20180420_03.md)
- [《PostgreSQL ADHoc(任意字段组合)查询 与 字典化 (rum 索引加速) - 实践与方案 1 - 菜鸟 某仿真系统》](https://github.com/digoal/blog/blob/master/201802/20180228_01.md)
- [《PostgreSQL 如何高效解决 按任意字段分词检索的问题 - case 1》](https://github.com/digoal/blog/blob/master/201607/20160725_05.md)
