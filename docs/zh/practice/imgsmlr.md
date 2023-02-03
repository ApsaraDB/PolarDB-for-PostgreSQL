---
author: digoal
date: 2023/02/03
minute: 20
---

# 图像特征存储与相似搜索

<ArticleInfo :frontmatter=$frontmatter></ArticleInfo>

## 背景

PolarDB 的云原生存算分离架构, 具备低廉的数据存储、高效扩展弹性、高速多机并行计算能力、高速数据搜索和处理; PolarDB 与计算算法结合, 将实现双剑合璧, 推动业务数据的价值产出, 将数据变成生产力.

本文将介绍使用 PolarDB 开源版 和 imgsmlr 存储图像特征值以及快速的进行图像相似搜索

测试环境为 macOS+docker, PolarDB 部署请参考下文:

- [《如何用 PolarDB 证明巴菲特的投资理念 - 包括 PolarDB 简单部署》](https://github.com/digoal/blog/blob/master/202209/20220908_02.md)

## 原理

- [《弱水三千,只取一瓢,当图像搜索遇见 PostgreSQL(Haar wavelet)》](https://github.com/digoal/blog/blob/master/201607/20160726_01.md)

图像数字化的方法很多, 例如将图像划分为`N^2`的宫格, 每个宫格由几个三原色和灰度进行表述, 然后层层缩小(例如从 81 宫格缩小到 9 宫格), 把全副图像再压缩到一个格子, 形成另一个更小的`N^2`宫格数组.

在进行图像相似搜索时, 实际上是比对 2 个`N^2`宫格数组的向量距离.

使用 GIST 索引接口, 可以实现这种向量相似搜索的快速收敛, 例如以中心点为桶的数据划分, 多图层缩略图的压缩搜索算法等. (参考本文后半部分 pase)

本文将介绍使用 PolarDB 开源版 和 imgsmlr 存储图像特征值以及快速的进行图像相似搜索.

1、新增 2 个数据类型, 一个是详图向量, 一个是签名向量(占用空间更小, 查询效率更高). 通常先使用签名向量过滤第一道, 然后再使用详图向量精筛.

| Datatype  | Storage length | Description                                                        |
| --------- | -------------- | ------------------------------------------------------------------ |
| pattern   | 16388 bytes    | Result of Haar wavelet transform on the image                      |
| signature | 64 bytes       | Short representation of pattern for fast search using GiST indexes |

2、新增几个图像转换函数接口

| Function                   | Return type | Description                                         |
| -------------------------- | ----------- | --------------------------------------------------- |
| jpeg2pattern(bytea)        | pattern     | Convert jpeg image into pattern                     |
| png2pattern(bytea)         | pattern     | Convert png image into pattern                      |
| gif2pattern(bytea)         | pattern     | Convert gif image into pattern                      |
| pattern2signature(pattern) | signature   | Create signature from pattern                       |
| shuffle_pattern(pattern)   | pattern     | Shuffle pattern for less sensitivity to image shift |

3、新增 2 个向量距离计算操作符和索引排序支持

| Operator | Left type | Right type | Return type | Description                               |
| -------- | --------- | ---------- | ----------- | ----------------------------------------- |
| `<->`    | pattern   | pattern    | float8      | Eucledian distance between two patterns   |
| `<->`    | signature | signature  | float8      | Eucledian distance between two signatures |

## 部署 imgsmlr on PolarDB

1、安装 png 和 jpeg 的图像库依赖

```
sudo yum install -y libpng-devel
sudo yum install -y libjpeg-turbo-devel



sudo vi /etc/ld.so.conf
# add
/usr/lib64

sudo ldconfig
```

2、安装 gd 库, 用于将 jpeg,png,gif 等图像的序列化转换.

```
git clone --depth 1 https://github.com/libgd/libgd

cd libgd/

mkdir build

cd build

cmake -DENABLE_PNG=1 -DENABLE_JPEG=1 ..

make

sudo make install

...
-- Installing: /usr/local/lib64/libgd.so.3.0.16
-- Installing: /usr/local/lib64/libgd.so.3
...



sudo vi /etc/ld.so.conf
# add
/usr/local/lib64

sudo ldconfig


export LD_LIBRARY_PATH=/usr/local/lib64:$LD_LIBRARY_PATH
```

3、安装 imgsmlr

```
git clone --depth 1 https://github.com/postgrespro/imgsmlr


cd imgsmlr/
USE_PGXS=1 make

USE_PGXS=1 make install
```

```
ldd /home/postgres/tmp_basedir_polardb_pg_1100_bld/lib/imgsmlr.so
	linux-vdso.so.1 =>  (0x00007ffc25d52000)
	libgd.so.3 => /usr/local/lib64/libgd.so.3 (0x00007fd7a4463000)
	libc.so.6 => /lib64/libc.so.6 (0x00007fd7a3ee5000)
	libstdc++.so.6 => /lib64/libstdc++.so.6 (0x00007fd7a3bdd000)
	libm.so.6 => /lib64/libm.so.6 (0x00007fd7a38db000)
	libgcc_s.so.1 => /lib64/libgcc_s.so.1 (0x00007fd7a36c5000)
	/lib64/ld-linux-x86-64.so.2 (0x00007fd7a42b3000)
```

4、加载插件

```
psql

create extension imgsmlr ;
```

## 场景模拟和架构设计实践

生成测试图像

```
cd imgsmlr
USE_PGXS=1 make installcheck
```

图像导入、向量化、图像相似搜索测试

```
psql


-- 创建插件
CREATE EXTENSION imgsmlr;

-- 创建存储原始图像二进制的表
CREATE TABLE image (id integer PRIMARY KEY, data bytea);

-- 创建临时表用于导入
CREATE TABLE tmp (data text);

-- 导入图像
\copy tmp from 'data/1.jpg.hex'
INSERT INTO image VALUES (1, (SELECT decode(string_agg(data, ''), 'hex') FROM tmp));
TRUNCATE tmp;
\copy tmp from 'data/2.png.hex'
INSERT INTO image VALUES (2, (SELECT decode(string_agg(data, ''), 'hex') FROM tmp));
TRUNCATE tmp;
\copy tmp from 'data/3.gif.hex'
INSERT INTO image VALUES (3, (SELECT decode(string_agg(data, ''), 'hex') FROM tmp));
TRUNCATE tmp;
\copy tmp from 'data/4.jpg.hex'
INSERT INTO image VALUES (4, (SELECT decode(string_agg(data, ''), 'hex') FROM tmp));
TRUNCATE tmp;
\copy tmp from 'data/5.png.hex'
INSERT INTO image VALUES (5, (SELECT decode(string_agg(data, ''), 'hex') FROM tmp));
TRUNCATE tmp;
\copy tmp from 'data/6.gif.hex'
INSERT INTO image VALUES (6, (SELECT decode(string_agg(data, ''), 'hex') FROM tmp));
TRUNCATE tmp;
\copy tmp from 'data/7.jpg.hex'
INSERT INTO image VALUES (7, (SELECT decode(string_agg(data, ''), 'hex') FROM tmp));
TRUNCATE tmp;
\copy tmp from 'data/8.png.hex'
INSERT INTO image VALUES (8, (SELECT decode(string_agg(data, ''), 'hex') FROM tmp));
TRUNCATE tmp;
\copy tmp from 'data/9.gif.hex'
INSERT INTO image VALUES (9, (SELECT decode(string_agg(data, ''), 'hex') FROM tmp));
TRUNCATE tmp;
\copy tmp from 'data/10.jpg.hex'
INSERT INTO image VALUES (10, (SELECT decode(string_agg(data, ''), 'hex') FROM tmp));
TRUNCATE tmp;
\copy tmp from 'data/11.png.hex'
INSERT INTO image VALUES (11, (SELECT decode(string_agg(data, ''), 'hex') FROM tmp));
TRUNCATE tmp;
\copy tmp from 'data/12.gif.hex'
INSERT INTO image VALUES (12, (SELECT decode(string_agg(data, ''), 'hex') FROM tmp));
TRUNCATE tmp;

-- 将原始图像转换为图像特征向量和图像签名, 导入新表中

CREATE TABLE pat AS (
    SELECT
        id,
        shuffle_pattern(pattern)::text::pattern AS pattern,
        pattern2signature(pattern)::text::signature AS signature
    FROM (
        SELECT
            id,
            (CASE WHEN id % 3 = 1 THEN jpeg2pattern(data)
                  WHEN id % 3 = 2 THEN png2pattern(data)
                  WHEN id % 3 = 0 THEN gif2pattern(data)
                  ELSE NULL END) AS pattern
        FROM
            image
    ) x
);

-- 添加PK
ALTER TABLE pat ADD PRIMARY KEY (id);

-- 在图像签名字段创建索引
CREATE INDEX pat_signature_idx ON pat USING gist (signature);

-- 自关联, 查询图像相似性(欧氏距离)
SELECT p1.id, p2.id, round((p1.pattern <-> p2.pattern)::numeric, 4) FROM pat p1, pat p2 ORDER BY p1.id, p2.id;
SELECT p1.id, p2.id, round((p1.signature <-> p2.signature)::numeric, 4) FROM pat p1, pat p2 ORDER BY p1.id, p2.id;


-- 使用索引, 快速搜索相似图像
SET enable_seqscan = OFF;

SELECT id FROM pat ORDER BY signature <-> (SELECT signature FROM pat WHERE id = 1) LIMIT 3;
SELECT id FROM pat ORDER BY signature <-> (SELECT signature FROM pat WHERE id = 4) LIMIT 3;
SELECT id FROM pat ORDER BY signature <-> (SELECT signature FROM pat WHERE id = 7) LIMIT 3;
SELECT id FROM pat ORDER BY signature <-> (SELECT signature FROM pat WHERE id = 10) LIMIT 3;
```

结果截取:

```
SELECT p1.id, p2.id, round((p1.signature <-> p2.signature)::numeric, 4) FROM pat p1, pat p2 ORDER BY p1.id, p2.id;
 id | id | round
----+----+--------
  1 |  1 | 0.0000
  1 |  2 | 0.5914
  1 |  3 | 0.6352
  1 |  4 | 1.1431
  1 |  5 | 1.3843
  1 |  6 | 1.5245
  1 |  7 | 3.1489
  1 |  8 | 3.4192
  1 |  9 | 3.4571
  1 | 10 | 4.0683
  1 | 11 | 3.3551
  1 | 12 | 2.4814
  2 |  1 | 0.5914
  2 |  2 | 0.0000
  2 |  3 | 0.7785
  2 |  4 | 1.1414
  2 |  5 | 1.2839
  2 |  6 | 1.4373
  2 |  7 | 3.2969
  2 |  8 | 3.5381
  2 |  9 | 3.5788
  2 | 10 | 4.4256
  2 | 11 | 3.6138
  2 | 12 | 2.7975
  3 |  1 | 0.6352
  3 |  2 | 0.7785
  3 |  3 | 0.0000
  3 |  4 | 1.0552
  3 |  5 | 1.3885
  3 |  6 | 1.4925
  3 |  7 | 3.0224
  3 |  8 | 3.2555
  3 |  9 | 3.2907
  3 | 10 | 4.0521
  3 | 11 | 3.2095
  3 | 12 | 2.4304
  4 |  1 | 1.1431
  4 |  2 | 1.1414
  4 |  3 | 1.0552
  4 |  4 | 0.0000
  4 |  5 | 0.5904
  4 |  6 | 0.7594
  4 |  7 | 2.6952
  4 |  8 | 2.9019
  4 |  9 | 2.9407
  4 | 10 | 3.8655
  4 | 11 | 2.9710
  4 | 12 | 2.1766
  5 |  1 | 1.3843
  5 |  2 | 1.2839
  5 |  3 | 1.3885
  5 |  4 | 0.5904
  5 |  5 | 0.0000
  5 |  6 | 0.7044
  5 |  7 | 2.9206
  5 |  8 | 3.1147
  5 |  9 | 3.1550
  5 | 10 | 4.0454
  5 | 11 | 3.2023
  5 | 12 | 2.3612
  6 |  1 | 1.5245
  6 |  2 | 1.4373
  6 |  3 | 1.4925
  6 |  4 | 0.7594
  6 |  5 | 0.7044
  6 |  6 | 0.0000
  6 |  7 | 2.8572
  6 |  8 | 3.0659
  6 |  9 | 3.1054
  6 | 10 | 3.7803
  6 | 11 | 2.7595
  6 | 12 | 2.0282
  7 |  1 | 3.1489
  7 |  2 | 3.2969
  7 |  3 | 3.0224
  7 |  4 | 2.6952
  7 |  5 | 2.9206
  7 |  6 | 2.8572
  7 |  7 | 0.0000
  7 |  8 | 0.6908
  7 |  9 | 0.7082
  7 | 10 | 4.3939
  7 | 11 | 3.5039
  7 | 12 | 3.2914
  8 |  1 | 3.4192
  8 |  2 | 3.5381
  8 |  3 | 3.2555
  8 |  4 | 2.9019
  8 |  5 | 3.1147
  8 |  6 | 3.0659
  8 |  7 | 0.6908
  8 |  8 | 0.0000
  8 |  9 | 0.0481
  8 | 10 | 4.6824
  8 | 11 | 3.7398
  8 | 12 | 3.5689
  9 |  1 | 3.4571
  9 |  2 | 3.5788
  9 |  3 | 3.2907
  9 |  4 | 2.9407
  9 |  5 | 3.1550
  9 |  6 | 3.1054
  9 |  7 | 0.7082
  9 |  8 | 0.0481
  9 |  9 | 0.0000
  9 | 10 | 4.6921
  9 | 11 | 3.7523
  9 | 12 | 3.5913
 10 |  1 | 4.0683
 10 |  2 | 4.4256
 10 |  3 | 4.0521
 10 |  4 | 3.8655
 10 |  5 | 4.0454
 10 |  6 | 3.7803
 10 |  7 | 4.3939
 10 |  8 | 4.6824
 10 |  9 | 4.6921
 10 | 10 | 0.0000
 10 | 11 | 1.8252
 10 | 12 | 2.0838
 11 |  1 | 3.3551
 11 |  2 | 3.6138
 11 |  3 | 3.2095
 11 |  4 | 2.9710
 11 |  5 | 3.2023
 11 |  6 | 2.7595
 11 |  7 | 3.5039
 11 |  8 | 3.7398
 11 |  9 | 3.7523
 11 | 10 | 1.8252
 11 | 11 | 0.0000
 11 | 12 | 1.2933
 12 |  1 | 2.4814
 12 |  2 | 2.7975
 12 |  3 | 2.4304
 12 |  4 | 2.1766
 12 |  5 | 2.3612
 12 |  6 | 2.0282
 12 |  7 | 3.2914
 12 |  8 | 3.5689
 12 |  9 | 3.5913
 12 | 10 | 2.0838
 12 | 11 | 1.2933
 12 | 12 | 0.0000
(144 rows)


postgres=# SELECT id FROM pat ORDER BY signature <-> (SELECT signature FROM pat WHERE id = 1) LIMIT 3;
 id
----
  1
  2
  3
(3 rows)

postgres=# SELECT id FROM pat ORDER BY signature <-> (SELECT signature FROM pat WHERE id = 4) LIMIT 3;
 id
----
  4
  5
  6
(3 rows)

postgres=# SELECT id FROM pat ORDER BY signature <-> (SELECT signature FROM pat WHERE id = 7) LIMIT 3;
 id
----
  7
  8
  9
(3 rows)

postgres=# SELECT id FROM pat ORDER BY signature <-> (SELECT signature FROM pat WHERE id = 10) LIMIT 3;
 id
----
 10
 11
 12
(3 rows)
```

```
postgres=# explain SELECT id FROM pat ORDER BY signature <-> (SELECT signature FROM pat WHERE id = 10) LIMIT 3;
                                     QUERY PLAN
------------------------------------------------------------------------------------
 Limit  (cost=8.29..10.34 rows=3 width=8)
   InitPlan 1 (returns $0)
     ->  Index Scan using pat_pkey on pat pat_1  (cost=0.14..8.15 rows=1 width=64)
           Index Cond: (id = 10)
   ->  Index Scan using pat_signature_idx on pat  (cost=0.13..8.37 rows=12 width=8)
         Order By: (signature <-> $0)
(6 rows)
```

使用 PolarDB+imgsmlr 实现了图像特征存储, 快速根据图像特征进行相似图像搜索.

## 参考

- [《PostgreSQL 多维、图像 欧式距离、向量距离、向量相似 查询优化 - cube,imgsmlr - 压缩、分段、异步并行》](https://github.com/digoal/blog/blob/master/201811/20181129_01.md)
- [《PostgreSQL 11 相似图像搜索插件 imgsmlr 性能测试与优化 3 - citus 8 机 128shard (4 亿图像)》](https://github.com/digoal/blog/blob/master/201809/20180904_04.md)
- [《PostgreSQL 11 相似图像搜索插件 imgsmlr 性能测试与优化 2 - 单机分区表 (dblink 异步调用并行) (4 亿图像)》](https://github.com/digoal/blog/blob/master/201809/20180904_03.md)
- [《PostgreSQL 11 相似图像搜索插件 imgsmlr 性能测试与优化 1 - 单机单表 (4 亿图像)》](https://github.com/digoal/blog/blob/master/201809/20180904_02.md)
- [《PostgreSQL 相似搜索插件介绍大汇总 (cube,rum,pg_trgm,smlar,imgsmlr,pg_similarity) (rum,gin,gist)》](https://github.com/digoal/blog/blob/master/201809/20180904_01.md)
- [《弱水三千,只取一瓢,当图像搜索遇见 PostgreSQL(Haar wavelet)》](https://github.com/digoal/blog/blob/master/201607/20160726_01.md)
- [《如何用 PolarDB 在不确定世界寻找确定答案 (例如图像相似) - vector|pase》](https://github.com/digoal/blog/blob/master/202212/20221201_02.md)
- [《PostgreSQL + FDW + vector 插件加速向量检索 - 在不确定世界寻找确定答案 (例如图像相似)》](https://github.com/digoal/blog/blob/master/202203/20220302_01.md)
- [《PostgreSQL ghtree 实现的海明距离排序索引, 性能不错(模糊图像) - pg-knn_hamming - bit string 比特字符串 相似度搜索》](https://github.com/digoal/blog/blob/master/202003/20200326_08.md)
- [《PostgreSQL bktree 索引 using gist 例子 - 海明距离检索 - 短文相似、模糊图像搜索 - bit string 比特字符串 相似度搜索》](https://github.com/digoal/blog/blob/master/202003/20200324_29.md)
- [《阿里云 PostgreSQL 案例精选 2 - 图像识别、人脸识别、相似特征检索、相似人群圈选》](https://github.com/digoal/blog/blob/master/202002/20200227_01.md)
- [《PostgreSQL+MySQL 联合解决方案 - 第 11 课视频 - 多维向量相似搜索 - 图像识别、相似人群圈选等》](https://github.com/digoal/blog/blob/master/202001/20200115_01.md)
- [《PostgreSQL 阿里云 rds pg 发布高维向量索引，支持图像识别、人脸识别 - pase 插件》](https://github.com/digoal/blog/blob/master/201912/20191219_02.md)
- [《阿里云 PostgreSQL 向量搜索、相似搜索、图像搜索 插件 palaemon - ivfflat , hnsw , nsg , ssg》](https://github.com/digoal/blog/blob/master/201908/20190815_01.md)
- [《脑王水哥王昱珩惜败人工智能, 这不可能. - 图像识别到底是什么鬼》](https://github.com/digoal/blog/blob/master/201701/20170122_01.md)
- [《(AR 虚拟现实)红包 技术思考 - GIS 与图像识别的完美结合》](https://github.com/digoal/blog/blob/master/201701/20170113_01.md)
- [《PostgreSQL 在视频、图片去重，图像搜索业务中的应用》](https://github.com/digoal/blog/blob/master/201611/20161126_01.md)

https://github.com/postgrespro/imgsmlr

https://github.com/libgd/libgd
