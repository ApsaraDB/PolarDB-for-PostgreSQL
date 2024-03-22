---
author: digoal
date: 2023/02/01
minute: 15
---

# 通过 BRIN 索引实现千分之一的存储空间, 高效率检索时序数据

<ArticleInfo :frontmatter=$frontmatter></ArticleInfo>

## 背景

PolarDB 的云原生存算分离架构, 具备低廉的数据存储、高效扩展弹性、高速多机并行计算能力、高速数据搜索和处理; PolarDB 与计算算法结合, 将实现双剑合璧, 推动业务数据的  
价值产出, 将数据变成生产力.

本文将介绍 PolarDB 开源版通过 brin 索引实现千分之一的存储空间, 高效率检索时序数据

测试环境为 macOS+docker, PolarDB 部署请参考:

- [《如何用 PolarDB 证明巴菲特的投资理念 - 包括 PolarDB 简单部署》](https://github.com/digoal/blog/blob/master/202209/20220908_02.md)

## 原理

PolarDB 的普通表采用堆存储, 最小分配单位为 block, 不够了就在文件末尾追加 block.

所以根据时序数据的 append only 、 时间字段递增特征. 一个 block 内的时间字段的值基本上是相邻的, 相邻的 block 时间值也相邻.

时序数据通常是按片搜索, 例如分钟、小时、天等粒度的片搜索和统计.

怎样高效、低成本的检索时序数据? PolarDB BRIN 块级别范围索引, 千分之一的存储, 实现 btree 同级别的片区搜索性能.

brin 为什么省存储呢? 因为一片 blocks, 只存储其索引字段的 min,max,nullif 的统计值. 所以非常节省空间.

## 模拟测试

1、建立时序表

```
create table tbl (id int, v1 int, v2 int, crt_time timestamp(0));
```

2、写入 500 万条时序数据

```
insert into tbl select id, random()*10, random()*100, now()+(id||'second')::interval from generate_series(1,5000000) id;
```

3、查询时序字段的边界值

```
postgres=# select min(crt_time), max(crt_time) from tbl;
         min         |         max
---------------------+---------------------
 2022-12-21 08:47:19 | 2023-02-17 05:40:38
(1 row)
```

4、普通 btree 索引的测试, 占用空间等.

```
create index on tbl using btree (crt_time);
```

```
postgres=# \dt+
                    List of relations
 Schema | Name | Type  |  Owner   |  Size  | Description
--------+------+-------+----------+--------+-------------
 public | tbl  | table | postgres | 249 MB |
(1 row)

postgres=# \di+
                              List of relations
 Schema |       Name       | Type  |  Owner   | Table |  Size  | Description
--------+------------------+-------+----------+-------+--------+-------------
 public | tbl_crt_time_idx | index | postgres | tbl   | 107 MB |
(1 row)
```

```
postgres=# explain (analyze,verbose,timing,costs,buffers) select count(*) from tbl where crt_time between '2022-12-30' and '2022-12-31';
                                                                                QUERY PLAN
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Aggregate  (cost=32345.17..32345.18 rows=1 width=8) (actual time=19.311..19.317 rows=1 loops=1)
   Output: count(*)
   Buffers: shared hit=790
   ->  Bitmap Heap Scan on public.tbl  (cost=532.68..32282.67 rows=25000 width=0) (actual time=5.786..13.871 rows=86401 loops=1)
         Recheck Cond: ((tbl.crt_time >= '2022-12-30 00:00:00'::timestamp without time zone) AND (tbl.crt_time <= '2022-12-31 00:00:00'::timestamp without time zone))
         Heap Blocks: exact=551
         Buffers: shared hit=790
         ->  Bitmap Index Scan on tbl_crt_time_idx  (cost=0.00..526.43 rows=25000 width=0) (actual time=5.723..5.724 rows=86401 loops=1)
               Index Cond: ((tbl.crt_time >= '2022-12-30 00:00:00'::timestamp without time zone) AND (tbl.crt_time <= '2022-12-31 00:00:00'::timestamp without time zone))
               Buffers: shared hit=239
 Planning Time: 0.081 ms
 Execution Time: 19.550 ms
(12 rows)
```

5、判断时间字段是否适合 brin 索引: 相关性为 1, 表明这个字段有自增属性、而且边界清晰. 非常适合 brin 索引.

相关性的范围是-1 到 1, 越接近 1 或者-1 都适合 brin.

```
postgres=# select correlation from pg_stats where tablename='tbl' and attname='crt_time';
 correlation
-------------
           1
(1 row)
```

6、测试 brin 索引, 观察其占用空间, 查询性能.

```
drop index tbl_crt_time_idx;


create index on tbl using brin (crt_time);


postgres=# explain (analyze,verbose,timing,costs,buffers) select count(*) from tbl where crt_time between '2022-12-30' and '2022-12-31';
                                                                                QUERY PLAN
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Aggregate  (cost=33599.60..33599.61 rows=1 width=8) (actual time=26.022..26.025 rows=1 loops=1)
   Output: count(*)
   Buffers: shared hit=642
   ->  Bitmap Heap Scan on public.tbl  (cost=33.38..33387.41 rows=84878 width=0) (actual time=0.937..18.871 rows=86401 loops=1)
         Recheck Cond: ((tbl.crt_time >= '2022-12-30 00:00:00'::timestamp without time zone) AND (tbl.crt_time <= '2022-12-31 00:00:00'::timestamp without time zone))
         Rows Removed by Index Recheck: 14079
         Heap Blocks: lossy=640
         Buffers: shared hit=642
         ->  Bitmap Index Scan on tbl_crt_time_idx  (cost=0.00..12.16 rows=100402 width=0) (actual time=0.420..0.421 rows=6400 loops=1)
               Index Cond: ((tbl.crt_time >= '2022-12-30 00:00:00'::timestamp without time zone) AND (tbl.crt_time <= '2022-12-31 00:00:00'::timestamp without time zone))
               Buffers: shared hit=2
 Planning Time: 0.168 ms
 Execution Time: 26.162 ms
(13 rows)



postgres=# \di+
                             List of relations
 Schema |       Name       | Type  |  Owner   | Table | Size  | Description
--------+------------------+-------+----------+-------+-------+-------------
 public | tbl_crt_time_idx | index | postgres | tbl   | 48 kB |
(1 row)
```

结论符合预期:

- brin 占用空间只有 btree 的 2000 分之一大小, 但是在进行范围条件搜索时, brin 索引性能相当于 btree, 扫描更少的数据块得到同级别的性能.

## 参考

- [《重新发现 PostgreSQL 之美 - 13 brin 时序索引》](https://github.com/digoal/blog/blob/master/202106/20210605_02.md)
- [《PostGIS 空间索引(GiST、BRIN、R-Tree)选择、优化 - 2》](https://github.com/digoal/blog/blob/master/202105/20210507_05.md)
- [《PostgreSQL 14 preview - BRIN (典型 IoT 时序场景) 块级索引支持 bloom filter - 随机,大量 distinct value, 等值查询》](https://github.com/digoal/blog/blob/master/202103/20210326_02.md)
- [《PostgreSQL 14 preview - BRIN (典型 IoT 时序场景) 块级索引支持 multi-range min-max [s] - 分段范围索引》](https://github.com/digoal/blog/blob/master/202103/20210326_01.md)
- [《PostgreSQL 14 preview - brin 索引内存优化》](https://github.com/digoal/blog/blob/master/202103/20210324_01.md)
- [《PostgreSQL 11 preview - BRIN 索引接口功能扩展(BLOOM FILTER、min max 分段)》](https://github.com/digoal/blog/blob/master/201803/20180323_05.md)
- [《HTAP 数据库 PostgreSQL 场景与性能测试之 24 - (OLTP) 物联网 - 时序数据并发写入(含时序索引 BRIN)》](https://github.com/digoal/blog/blob/master/201711/20171107_25.md)
- [《PostgreSQL BRIN 索引的 pages_per_range 选项优化与内核代码优化思考》](https://github.com/digoal/blog/blob/master/201708/20170824_01.md)
- [《万亿级电商广告 - brin 黑科技带你(最低成本)玩转毫秒级圈人(视觉挖掘姊妹篇) - 阿里云 RDS PostgreSQL, HybridDB for PostgreSQL 最佳实践》](https://github.com/digoal/blog/blob/master/201708/20170823_01.md)
- [《PostGIS 空间索引(GiST、BRIN、R-Tree)选择、优化 - 阿里云 RDS PostgreSQL 最佳实践》](https://github.com/digoal/blog/blob/master/201708/20170820_01.md)
- [《自动选择正确索引访问接口(btree,hash,gin,gist,sp-gist,brin,bitmap...)的方法》](https://github.com/digoal/blog/blob/master/201706/20170617_01.md)
- [《PostgreSQL 并行写入堆表，如何保证时序线性存储 - BRIN 索引优化》](https://github.com/digoal/blog/blob/master/201706/20170611_02.md)
- [《PostgreSQL 10.0 preview 功能增强 - BRIN 索引更新 smooth 化》](https://github.com/digoal/blog/blob/master/201704/20170405_01.md)
- [《PostgreSQL 聚集存储 与 BRIN 索引 - 高并发行为、轨迹类大吞吐数据查询场景解说》](https://github.com/digoal/blog/blob/master/201702/20170219_01.md)
- [《PostgreSQL 物联网黑科技 - 瘦身几百倍的索引(BRIN index)》](https://github.com/digoal/blog/blob/master/201604/20160414_01.md)
- [《PostgreSQL 9.5 new feature - lets BRIN be used with R-Tree-like indexing strategies For "inclusion" opclasses》](https://github.com/digoal/blog/blob/master/201505/20150526_01.md)
- [《PostgreSQL 9.5 new feature - BRIN (block range index) index》](https://github.com/digoal/blog/blob/master/201504/20150419_01.md)
