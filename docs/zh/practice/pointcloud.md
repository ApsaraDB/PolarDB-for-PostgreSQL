---
author: digoal
date: 2023/02/03
minute: 15
---

# 点云技术与数字孪生场景实践

<ArticleInfo :frontmatter=$frontmatter></ArticleInfo>

## 背景

PolarDB 的云原生存算分离架构, 具备低廉的数据存储、高效扩展弹性、高速多机并行计算能力、高速数据搜索和处理; PolarDB 与计算算法结合, 将实现双剑合璧, 推动业务数据的价值产出, 将数据变成生产力.

本文将介绍 PolarDB 开源版 通过 pgpointcloud 实现高效孪生数据存储和管理 - 支撑工厂、农业等现实世界数字化|数字孪生, 元宇宙相关业务的虚拟现实结合

测试环境为 macOS+docker, PolarDB 部署请参考下文:

- [《如何用 PolarDB 证明巴菲特的投资理念 - 包括 PolarDB 简单部署》](https://github.com/digoal/blog/blob/master/202209/20220908_02.md)

## pgpointcloud 的特性

pgpointcloud 的原理是将很多个点存储到一个值(点集)里面, 点集可以用来表达轨迹、扫描影像, 业务操作通常包含:

- 判断轨迹是否相交(指包住两个轨迹的边界(bound box)是否相交), 实际上使用 PostGIS 的 trajectories 可能更适合.
- 判断轨迹是否落到某个给定区域(指轨迹的边界(bound box)是否和指定几何对象是否相交)
- 指定区域将内部的轨迹抠出(将落在指定几何对象内部的点从点集中抠出)
- 合并轨迹
- 压缩轨迹
- 3D 建模和数据存储
- 3D 影像的抠出

注意这里说的“轨迹”不带时间, 只是为了辅助理解所以称之为轨迹, 实际上 pgpointcloud 不适合轨迹业务. 轨迹建议使用 postgis. 或者阿里云 ganos.

- https://postgis.net/docs/manual-3.3/reference.html#Temporal

pgpointcloud 存储点集的优势:

- 支持压缩, 节省成本
- 片存储, 存取效率高
- 支持内置点云操作算法, 同时可扩展算法, 无需提取数据到本地进行计算, 大幅度提升计算效率
- 支持 GIS, 方便和地理信息结合, 更好的满足虚拟现实|数字孪生业务需求
- 支持索引, 过滤效率高

## 部署 pgpointcloud on PolarDB

```
wget https://github.com/pgpointcloud/pointcloud/archive/refs/tags/v1.2.4.tar.gz

tar -zxvf v1.2.4.tar.gz

cd pointcloud-1.2.4/

./autogen.sh

./configure

make -j 4

sudo make install
```

```
[postgres@1373488a35ab pointcloud-1.2.4]$ psql
psql (11.9)
Type "help" for help.

postgres=# create extension postgis;
CREATE EXTENSION
postgres=# create extension pointcloud;
CREATE EXTENSION
postgres=# create extension pointcloud_postgis ;
CREATE EXTENSION
```

## 点云使用举例

1、点

```
SELECT PC_MakePoint(1, ARRAY[-127, 45, 124.0, 4.0]);

010100000064CEFFFF94110000703000000400
```

Insert some test values into the points table:

```
INSERT INTO points (pt)
SELECT PC_MakePoint(1, ARRAY[x,y,z,intensity])
FROM (
  SELECT
  -127+a/100.0 AS x,
    45+a/100.0 AS y,
         1.0*a AS z,
          a/10 AS intensity
  FROM generate_series(1,100) AS a
) AS values;
```

```
SELECT PC_AsText('010100000064CEFFFF94110000703000000400'::pcpoint);

{"pcid":1,"pt":[-127,45,124,4]}
```

2、点集

```
INSERT INTO patches (pa)
SELECT PC_Patch(pt) FROM points GROUP BY id/10;
```

```
SELECT PC_AsText(PC_MakePatch(1, ARRAY[-126.99,45.01,1,0, -126.98,45.02,2,0, -126.97,45.03,3,0]));

{"pcid":1,"pts":[
 [-126.99,45.01,1,0],[-126.98,45.02,2,0],[-126.97,45.03,3,0]
]}
```

```
SELECT PC_AsText(pa) FROM patches LIMIT 1;

{"pcid":1,"pts":[
 [-126.99,45.01,1,0],[-126.98,45.02,2,0],[-126.97,45.03,3,0],
 [-126.96,45.04,4,0],[-126.95,45.05,5,0],[-126.94,45.06,6,0],
 [-126.93,45.07,7,0],[-126.92,45.08,8,0],[-126.91,45.09,9,0]
]}
```

3、判断轨迹是否相交(指包住两个轨迹的边界(bound box)是否相交), 实际上使用 PostGIS 的 trajectories 可能更适合.

```
-- Patch should intersect itself
SELECT PC_Intersects(
         '01010000000000000001000000C8CEFFFFF8110000102700000A00'::pcpatch,
         '01010000000000000001000000C8CEFFFFF8110000102700000A00'::pcpatch);

t
```

4、判断轨迹是否落到某个给定区域(指轨迹的边界(bound box)是否和指定几何对象是否相交)

```
SELECT PC_Intersects('SRID=4326;POINT(-126.451 45.552)'::geometry, pa)
FROM patches WHERE id = 7;

t
```

5、指定区域将内部的轨迹抠出(将落在指定几何对象内部的点从点集中抠出)

```
SELECT PC_AsText(PC_Explode(PC_Intersection(
      pa,
      'SRID=4326;POLYGON((-126.451 45.552, -126.42 47.55, -126.40 45.552, -126.451 45.552))'::geometry
)))
FROM patches WHERE id = 7;

             pc_astext
--------------------------------------
 {"pcid":1,"pt":[-126.44,45.56,56,5]}
 {"pcid":1,"pt":[-126.43,45.57,57,5]}
 {"pcid":1,"pt":[-126.42,45.58,58,5]}
 {"pcid":1,"pt":[-126.41,45.59,59,5]}
```

6、合并轨迹

聚合函数

```
-- Compare npoints(sum(patches)) to sum(npoints(patches))
SELECT PC_NumPoints(PC_Union(pa)) FROM patches;
SELECT Sum(PC_NumPoints(pa)) FROM patches;

100
```

可变函数

```
create or replace function pcunion (VARIADIC pc pcpatch[]) returns pcpatch as $$
  select PC_Union(pa) from unnest(pc) as pa;
$$ language sql strict;

select pcunion(pc1,pc2,...);
```

7、压缩点集

```
PC_Compress(p pcpatch,global_compression_scheme text,compression_config text) returns pcpatch
```

Allowed global compression schemes are:

- auto: determined by pcid
- laz: no compression config supported
- dimensional: configuration is a comma-separated list of per-dimension compressions from this list
  - auto: determined automatically from values stats
  - zlib: deflate compression
  - sigbits: significant bits removal
  - rle: run-length encoding

8、3D 影像的抠出

```
1 PC_FilterGreaterThan
PC_FilterGreaterThan(p pcpatch, dimname text, float8 value) returns pcpatch:

Returns a patch with only points whose values are greater than the supplied value for the requested dimension.

SELECT PC_AsText(PC_FilterGreaterThan(pa, 'y', 45.57))
FROM patches WHERE id = 7;

 {"pcid":1,"pts":[[-126.42,45.58,58,5],[-126.41,45.59,59,5]]}

2 PC_FilterLessThan
PC_FilterLessThan(p pcpatch, dimname text, float8 value) returns pcpatch:
Returns a patch with only points whose values are less than the supplied value for the requested dimension.

3 PC_FilterBetween
PC_FilterBetween(p pcpatch, dimname text, float8 value1, float8 value2) returns pcpatch:
Returns a patch with only points whose values are between (excluding) the supplied values for the requested dimension.

4 PC_FilterEquals
PC_FilterEquals(p pcpatch, dimname text, float8 value) returns pcpatch:
Returns a patch with only points whose values are the same as the supplied values for the requested dimension.
```

9、返回包住点集的几何"bound box"或"bound box 的对角线"

通常用于在几何图像上创建 geo 索引:

```
SELECT ST_AsText(PC_EnvelopeGeometry(pa)) FROM patches LIMIT 1;
POLYGON((-126.99 45.01,-126.99 45.09,-126.91 45.09,-126.91 45.01,-126.99 45.01))

CREATE INDEX ON patches USING GIST(PC_EnvelopeGeometry(patch));
```

```
SELECT ST_AsText(PC_BoundingDiagonalGeometry(pa)) FROM patches;
                  st_astext
------------------------------------------------
LINESTRING Z (-126.99 45.01 1,-126.91 45.09 9)
LINESTRING Z (-126 46 100,-126 46 100)
LINESTRING Z (-126.2 45.8 80,-126.11 45.89 89)
LINESTRING Z (-126.4 45.6 60,-126.31 45.69 69)
LINESTRING Z (-126.3 45.7 70,-126.21 45.79 79)
LINESTRING Z (-126.8 45.2 20,-126.71 45.29 29)
LINESTRING Z (-126.5 45.5 50,-126.41 45.59 59)
LINESTRING Z (-126.6 45.4 40,-126.51 45.49 49)
LINESTRING Z (-126.9 45.1 10,-126.81 45.19 19)
LINESTRING Z (-126.7 45.3 30,-126.61 45.39 39)
LINESTRING Z (-126.1 45.9 90,-126.01 45.99 99)

CREATE INDEX ON patches USING GIST(PC_BoundingDiagonalGeometry(patch) gist_geometry_ops_nd);
```

## pgpointcloud 函数接口解读

1、点

PC_MakePoint(pcid integer, vals float8[]) returns pcpoint:

- 构建点, pcid 为 schema, 相同 schema 可以表达为某一类点

PC_AsText(p pcpoint) returns text:

- 将二进制点转换成 text 表达

PC_PCId(p pcpoint) returns integer (from 1.1.0):

- 获取点的 schema id

PC_Get(pt pcpoint) returns float8[]:

- 获取点的所有维度值

PC_Get(pt pcpoint, dimname text) returns numeric:

- 获取指定维度值: x,y,z,Intensity

PC_MemSize(pt pcpoint) returns int4:

- 点占据内存大小

2、点集

PC_Patch(pts pcpoint[]) returns pcpatch:

- 将多个点聚合为点集

PC_MakePatch(pcid integer, vals float8[]) returns pcpatch:

- 构造点集

PC_NumPoints(p pcpatch) returns integer:

- 返回点集中有多少点

PC_PCId(p pcpatch) returns integer:

- 返回点集的 schema id

PC_AsText(p pcpatch) returns text:

- 将点集二进制格式转换为文本格式

PC_Summary(p pcpatch) returns text (from 1.1.0):

- 返回点集的统计信息: 点个数, srid, 各个维度的 avg,min,max 统计等

PC_Uncompress(p pcpatch) returns pcpatch:

- 解压点集

PC_Union(p pcpatch[]) returns pcpatch:

- 将多个点集聚合为一个点集

PC_Intersects(p1 pcpatch, p2 pcpatch) returns boolean:

- 判断两个点集的 bound box 是否相交

PC_Explode(p pcpatch) returns SetOf[pcpoint]:

- 将点集展开为点(返回多条记录)

PC_PatchAvg(p pcpatch, dimname text) returns numeric:

- 返回点集指定维度的平均值

PC_PatchMax(p pcpatch, dimname text) returns numeric:

- 返回点集指定维度的最大值

PC_PatchMin(p pcpatch, dimname text) returns numeric:

- 返回点集指定维度的最小值

PC_PatchMin(p pcpatch) returns pcpoint:

- 返回点集所有维度的最小值

PC_PatchAvg(p pcpatch) returns pcpoint:

- 返回点集所有维度的平均值

PC_PatchMax(p pcpatch) returns pcpoint:

- 返回点集所有维度的最大值

PC_FilterGreaterThan(p pcpatch, dimname text, float8 value) returns pcpatch:

- 过滤在某个‘指定维度’上大于‘指定’的点, 构成一个新的点集返回

PC_FilterLessThan(p pcpatch, dimname text, float8 value) returns pcpatch:

- 过滤在某个‘指定维度’上小于‘指定值’的点, 构成一个新的点集返回

PC_FilterBetween(p pcpatch, dimname text, float8 value1, float8 value2) returns pcpatch:

- 过滤在某个‘指定维度’上落在某‘指定值’范围内的点, 构成一个新的点集返回

PC_FilterEquals(p pcpatch, dimname text, float8 value) returns pcpatch:

- 过滤在某个‘指定维度’上等于‘指定值’的点, 构成一个新的点集返回

PC_Compress(p pcpatch,global_compression_scheme text,compression_config text) returns pcpatch:

- 压缩点集

PC_PointN(p pcpatch, n int4) returns pcpoint:

- 返回点集内的第 N 个点

PC_IsSorted(p pcpatch, dimnames text[], strict boolean default true) returns boolean:

- 判断点集在某些维度上是否有序

PC_Sort(p pcpatch, dimnames text[]) returns pcpatch:

- 对点集按指定维度排序. 有点像电子管电视机的扫描顺序的概念

PC_Range(p pcpatch, start int4, n int4) returns pcpatch:

- 返回点集中指定区间的点

PC_SetPCId(p pcpatch, pcid int4, def float8 default 0.0) returns pcpatch:

- 设置点集的 schema id

PC_Transform(p pcpatch, pcid int4, def float8 default 0.0) returns pcpatch:

- 转换点集 schema id

PC_MemSize(p pcpatch) returns int4:

- 返回点集内存占用

3、GIS 互动

PC_Intersects(p pcpatch, g geometry) returns boolean:  
PC_Intersects(g geometry, p pcpatch) returns boolean:

- 判断点集的 bound box 是否与几何对象交叉

PC_Intersection(pcpatch, geometry) returns pcpatch:

- 提取点集内落在几何对象内的点构成新点集并返回

Geometry(pcpoint) returns geometry:  
pcpoint::geometry returns geometry:

- 将点转换为几何对象

PC_EnvelopeGeometry(pcpatch) returns geometry:

- 提取点集的 bound box, 返回由 bound box 四个点组成的 polygon

PC_BoundingDiagonalGeometry(pcpatch) returns geometry:

- 提取点集的 bound box, 返回由 bound box 左下和右上点组成的对角线段

## 参考

- https://pgpointcloud.github.io/pointcloud/concepts/tables.html
- https://pgpointcloud.github.io/pointcloud/functions/index.html
- [《重新发现 PostgreSQL 之美 - 31 激光点云 LiDAR - 一尺之锤日取一半万世不竭》](https://github.com/digoal/blog/blob/master/202106/20210620_01.md)
- [《无人驾驶背后的技术 - PostGIS 点云(pointcloud)应用 - 2》](https://github.com/digoal/blog/blob/master/201705/20170523_01.md)
- [《无人驾驶背后的技术 - PostGIS 点云(pointcloud)应用 - 1》](https://github.com/digoal/blog/blob/master/201705/20170519_02.md)
