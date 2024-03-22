---
author: digoal
date: 2023/02/03
minute: 15
---

# 时空轨迹应用实践

<ArticleInfo :frontmatter=$frontmatter></ArticleInfo>

## 背景

PolarDB 的云原生存算分离架构, 具备低廉的数据存储、高效扩展弹性、高速多机并行计算能力、高速数据搜索和处理; PolarDB 与计算算法结合, 将实现双剑合璧, 推动业务数据的价值产出, 将数据变成生产力.

本文将介绍 PolarDB 开源版 轨迹应用实践, 例如:

- 出行、配送、快递等业务的调度
  - 快递员有预规划的配送轨迹(轨迹)
  - 客户有发货需求(时间、位置)
  - 根据轨迹估算最近的位置和时间
- 通过多个嫌疑人的轨迹, 计算嫌疑人接触的地点、时间点
- 根据轨迹, 对传染源进行溯源

测试环境为 macOS+docker, PolarDB 部署请参考下文:

- [《如何用 PolarDB 证明巴菲特的投资理念 - 包括 PolarDB 简单部署》](https://github.com/digoal/blog/blob/master/202209/20220908_02.md)

## 轨迹介绍

轨迹的定义:

- 位置 1、位置 2、...位置 N 组成的线段, 加上 开始时间、结束时间

轨迹的常见计算:

- 两个轨迹何时最接近
- 最近的距离是多少
- 两个轨迹最近时的位置分别是什么

## 相关函数

https://postgis.net/docs/manual-3.3/reference.html#Temporal

8.18. Linear Referencing

- ST_LineInterpolatePoint — Returns a point interpolated along a line at a fractional location.
- ST_3DLineInterpolatePoint — Returns a point interpolated along a 3D line at a fractional location.
- ST_LineInterpolatePoints — Returns points interpolated along a line at a fractional interval.
- ST_LineLocatePoint — Returns the fractional location of the closest point on a line to a point.
- ST_LineSubstring — Returns the part of a line between two fractional locations.
- ST_LocateAlong — Returns the point(s) on a geometry that match a measure value.
- ST_LocateBetween — Returns the portions of a geometry that match a measure range.
- ST_LocateBetweenElevations — Returns the portions of a geometry that lie in an elevation (Z) range.
- ST_InterpolatePoint — Returns the interpolated measure of a geometry closest to a point.
- ST_AddMeasure — Interpolates measures along a linear geometry.
  8.19. Trajectory Functions  
  Abstract  
  These functions support working with trajectories. A trajectory is a linear geometry with increasing measures (M value) on each coordinate. Spatio-temporal data can be modeled by using relative times (such as the epoch) as the measure values.
- ST_IsValidTrajectory — Tests if the geometry is a valid trajectory.
- ST_ClosestPointOfApproach — Returns a measure at the closest point of approach of two trajectories.
- ST_DistanceCPA — Returns the distance between the closest point of approach of two trajectories.
- ST_CPAWithin — Tests if the closest point of approach of two trajectories is within the specified distance.

## 轨迹计算举例

1、构造 3 维轨迹:

```
ST_AddMeasure('LINESTRING Z (0 0 0, 10 0 5, 1 1 1)'::geometry,  -- 三个3维点
    extract(epoch from '2015-05-26 10:00'::timestamptz),  -- 开始时间
    extract(epoch from '2015-05-26 11:00'::timestamptz)   -- 结束时间
)
```

2、构造 2 维轨迹:

```
ST_AddMeasure('LINESTRING (0 0, 10 0, 1 1)'::geometry,  -- 三个2维点
    extract(epoch from '2015-05-26 10:00'::timestamptz),  -- 开始时间
    extract(epoch from '2015-05-26 11:00'::timestamptz)   -- 结束时间
)
```

3、返回 2 条轨迹距离最接近时的第一个时间点(因为 2 条轨迹可能有多个时间处于最近距离, 但是这里只返回最早的时间点, 如果要求后面的时间点, 可以切分线段).

- 两个轨迹何时最接近
- 最近的距离是多少
- 两个轨迹最近时的位置分别是什么

```
-- Return the time in which two objects moving between 10:00 and 11:00
-- are closest to each other and their distance at that point
WITH inp AS ( SELECT
  ST_AddMeasure('LINESTRING Z (0 0 0, 10 0 5)'::geometry,  -- 如果轨迹是一个点, 这里就直接填2个一样位置的点
    extract(epoch from '2015-05-26 10:00'::timestamptz),
    extract(epoch from '2015-05-26 11:00'::timestamptz)
  ) a,
  ST_AddMeasure('LINESTRING Z (0 2 10, 12 1 2, 15 3 5)'::geometry,  -- 两条轨迹的点数可以不一样
    extract(epoch from '2015-05-26 10:00'::timestamptz),
    extract(epoch from '2015-05-26 11:00'::timestamptz)
  ) b
), cpa AS (
  SELECT ST_ClosestPointOfApproach(a,b) m FROM inp  -- 计算a,b 2条轨迹距离最近时的最早时间点
), points AS (
  SELECT ST_Force3DZ(ST_GeometryN(ST_LocateAlong(a,m),1)) pa,   -- ST_LocateAlong(a,m)  计算a轨迹在某个时间点m对应的位置点(集合点)
         ST_Force3DZ(ST_GeometryN(ST_LocateAlong(b,m),1)) pb    -- ST_GeometryN 返回集合的第一个点, 由于a,b线段是3维线段, 所以返回后需要再使用ST_Force3DZ格式化一下?
  FROM inp, cpa
)
SELECT st_astext(pa) pa, st_astext(pb) pb,
       to_timestamp(m) t,  -- a,b线段距离最近时的最早的时间点m
       ST_Distance(pa,pb) distance  -- a,b线段最接近的pa,pb点的距离
FROM points, cpa;

                       pa                        |                               pb                               |               t               |     distance
-------------------------------------------------+----------------------------------------------------------------+-------------------------------+------------------
 POINT Z (5.798478121227689 0 2.899239060613844) | POINT Z (9.041623081002845 1.24653140991643 3.972251279331437) | 2015-05-26 10:34:47.452124+00 | 3.47445388313376
(1 row)
```

以上 SQL 应用场景举例:

1、出行、配送、快递等业务的调度, 例如

- 快递员预规划的配送轨迹(轨迹 a)
- 客户有发货需求(时间、位置)(轨迹 b)

2、多个嫌疑人的轨迹

- 计算嫌疑人接触的地点、时间点

3、根据传染病人的多人多轨迹进行轨迹的碰撞计算, 对传染源进行溯源追踪.

## 参考

- [《使用 PolarDB 开源版 部署 PostGIS 支撑时空轨迹|地理信息|路由等业务》](https://github.com/digoal/blog/blob/master/202212/20221223_02.md)
- [《重新发现 PostgreSQL 之美 - 11 时空轨迹系统 新冠&刑侦&预测》](https://github.com/digoal/blog/blob/master/202106/20210602_01.md)
- [《重新发现 PostgreSQL 之美 - 8 轨迹业务 IO 杀手克星 index include(覆盖索引)》](https://github.com/digoal/blog/blob/master/202105/20210530_02.md)
- [《PostgreSQL 应用开发解决方案最佳实践系列课程 - 6. 时空、时态、时序、日志等轨迹系统》](https://github.com/digoal/blog/blob/master/202105/20210509_01.md)
- [《使用 Postgres，MobilityDB 和 Citus 大规模(百亿级)实时分析 GPS 轨迹》](https://github.com/digoal/blog/blob/master/202011/20201117_01.md)
- [《PostgreSQL index include - 类聚簇表与应用(append only, IoT 时空轨迹, 离散多行扫描与返回)》](https://github.com/digoal/blog/blob/master/201905/20190503_03.md)
- [《PostgreSQL IoT，车联网 - 实时轨迹、行程实践 2 - (含 index only scan 类聚簇表效果)》](https://github.com/digoal/blog/blob/master/201812/20181209_01.md)
- [《PostgreSQL IoT，车联网 - 实时轨迹、行程实践 1》](https://github.com/digoal/blog/blob/master/201812/20181207_01.md)
- [《PostgreSQL pipelinedb 流计算插件 - IoT 应用 - 实时轨迹聚合》](https://github.com/digoal/blog/blob/master/201811/20181101_02.md)
- [《Greenplum 轨迹相似(伴随分析)》](https://github.com/digoal/blog/blob/master/201806/20180607_02.md)
- [《PostgreSQL 实时位置跟踪+轨迹分析系统实践 - 单机顶千亿轨迹/天》](https://github.com/digoal/blog/blob/master/201712/20171231_01.md)
- [《GIS 术语 - POI、AOI、LOI、路径、轨迹》](https://github.com/digoal/blog/blob/master/201712/20171204_01.md)
- [《菜鸟末端轨迹 - 电子围栏(解密支撑每天 251 亿个包裹的数据库) - 阿里云 RDS PostgreSQL 最佳实践》](https://github.com/digoal/blog/blob/master/201708/20170803_01.md)
- [《车联网案例，轨迹清洗 - 阿里云 RDS PostgreSQL 最佳实践 - 窗口函数》](https://github.com/digoal/blog/blob/master/201707/20170722_02.md)
- [《PostgreSQL 物流轨迹系统数据库需求分析与设计 - 包裹侠实时跟踪与召回》](https://github.com/digoal/blog/blob/master/201704/20170418_01.md)
- [《PostgreSQL 聚集存储 与 BRIN 索引 - 高并发行为、轨迹类大吞吐数据查询场景解说》](https://github.com/digoal/blog/blob/master/201702/20170219_01.md)
- [《PostgreSQL 如何轻松搞定行驶、运动轨迹合并和切分》](https://github.com/digoal/blog/blob/master/201606/20160611_02.md)
