---
author: digoal
date: 2023/02/03
minute: 10
---

# 如何解决出行、快递、配送、商旅等场景的路径规划需求

<ArticleInfo :frontmatter=$frontmatter></ArticleInfo>

## 背景

PolarDB 的云原生存算分离架构, 具备低廉的数据存储、高效扩展弹性、高速多机并行计算能力、高速数据搜索和处理; PolarDB 与计算算法结合, 将实现双剑合璧, 推动业务数据的价值产出, 将数据变成生产力.

本文将介绍使用 PolarDB 开源版 部署 pgrouting 支撑出行、快递、配送等商旅问题的路径规划业务

测试环境为 macOS+docker, PolarDB 部署请参考下文:

- [《如何用 PolarDB 证明巴菲特的投资理念 - 包括 PolarDB 简单部署》](https://github.com/digoal/blog/blob/master/202209/20220908_02.md)

## pgrouting 的特性

pgRouting library contains following features:

- All Pairs Shortest Path, Johnson’s Algorithm
- All Pairs Shortest Path, Floyd-Warshall Algorithm
- Shortest Path A\*
- Bi-directional Dijkstra Shortest Path
- Bi-directional A\* Shortest Path
- Shortest Path Dijkstra
- Driving Distance
- K-Shortest Path, Multiple Alternative Paths
- K-Dijkstra, One to Many Shortest Path
- Traveling Sales Person
- Turn Restriction Shortest Path (TRSP)

## 部署 pgrouting on PolarDB

1、boost 依赖

```
wget https://boostorg.jfrog.io/artifactory/main/release/1.69.0/source/boost_1_69_0.tar.bz2

tar -jxvf boost_1_69_0.tar.bz2

cd boost_1_69_0

./bootstrap.sh

sudo ./b2 --prefix=/usr/local/boost -a install
```

2、pgrouting

```
wget https://github.com/pgRouting/pgrouting/archive/refs/tags/v3.4.2.tar.gz

tar -zxvf v3.4.2.tar.gz

cd pgrouting-3.4.2/

// 找到boost定制配置参数
// less -i CMakeLists.txt

mkdir build

cd build

cmake .. -DBOOST_ROOT=/usr/local/boost

make -j 4
sudo make install
```

3、加载 postgis 和 pgrouting 插件 on PolarDB

```
[postgres@1373488a35ab ~]$ psql
psql (11.9)
Type "help" for help.

postgres=# create extension postgis ;
postgres=# create extension pgrouting ;
CREATE EXTENSION
postgres=# \dx
                                 List of installed extensions
   Name    | Version |   Schema   |                        Description
-----------+---------+------------+------------------------------------------------------------
 pgrouting | 3.4.2   | public     | pgRouting Extension
 plpgsql   | 1.0     | pg_catalog | PL/pgSQL procedural language
 postgis   | 3.3.2   | public     | PostGIS geometry and geography spatial types and functions
(3 rows)
```

更多使用方式请参考:  
https://pgrouting.org/documentation.html

- [《PostgreSQL openstreetmap 地图数据、路网数据服务 - 高速下载、导入 gis, pgrouting》](https://github.com/digoal/blog/blob/master/202110/20211008_01.md)
- [《行程规划 , 商旅问题 , 旅游问题 , postgis , pgrouting , postgresql , Traveling Salesman Problem (or TSP)》](https://github.com/digoal/blog/blob/master/202103/20210317_04.md)
- [《[未完待续] pgrouting 在机票业务中的应用 - 实时最佳转机计算》](https://github.com/digoal/blog/blob/master/201711/20171104_01.md)
- [《路径规划应用 pgRouting 实践与开放地图导入 - Openstreetmap PBF》](https://github.com/digoal/blog/blob/master/201508/20150813_03.md)
