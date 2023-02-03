---
author: digoal
date: 2023/02/03
minute: 15
---

# 向量搜索实践,在不确定世界寻找确定答案 - 例如图像相似、特征相似

<ArticleInfo :frontmatter=$frontmatter></ArticleInfo>

## 背景

世界是确定的吗? 不

就好像我们拍照, 同一个相机, 同一个地点, 同一个时间连拍几张, 结果都不一样. 更不用说时间地点不一样了.

真正确定的数据并不多, 世界充满的是不确定的数据.

例如人脸识别, 存在数据库中的数据可能是曾经的照片, 但是你去比对人脸时是实时的, 角度、化妆、发型都可能不一样.

未来的数据库一定要解决一个问题, 如何在不确定的世界寻找确定的答案?

PolarDB 早几年就发布了 pase 插件, 解决高性能图像识别的问题, 通过将非结构化数据根据特征提取成为一串向量, 然后根据向量进行距离计算, 得到最相似的向量, 从而解决不确定数据的确定性搜索.

后来开源社区也发了一个插件 vector, 知识支持的算法只有 ivfflat.

本文将介绍如何在开源 polardb 中安装和使用 vector 插件, 解决向量数据相似搜索的问题.

## vector on PolarDB

1、启动并进入 polardb 实例

```
IT-C02YW2EFLVDL:~ digoal$ docker ps -a
CONTAINER ID   IMAGE                                    COMMAND                  CREATED        STATUS                        PORTS     NAMES
67e1eed1b4b6   polardb/polardb_pg_local_instance:htap   "/bin/sh -c '~/tmp_b…"   2 months ago   Exited (137) 10 minutes ago             polardb_pg_htap

IT-C02YW2EFLVDL:~ digoal$ docker start 67e1eed1b4b6
67e1eed1b4b6

IT-C02YW2EFLVDL:~ digoal$ docker exec -it 67e1eed1b4b6 bash
[postgres@67e1eed1b4b6 ~]$ which git
/usr/bin/git
```

2、下载 vector 插件

```
[postgres@67e1eed1b4b6 ~]$ git clone --branch v0.3.2 --depth 1 https://github.com/pgvector/pgvector.git
Cloning into 'pgvector'...
remote: Enumerating objects: 80, done.
remote: Counting objects: 100% (80/80), done.
remote: Compressing objects: 100% (62/62), done.
remote: Total 80 (delta 33), reused 29 (delta 15), pack-reused 0
Unpacking objects: 100% (80/80), done.
Note: checking out 'a7f712b5a4724cfe55e2793dd1a4b7d48257fa1e'.

You are in 'detached HEAD' state. You can look around, make experimental
changes and commit them, and you can discard any commits you make in this
state without impacting any branches by performing another checkout.

If you want to create a new branch to retain commits you create, you may
do so (now or later) by using -b with the checkout command again. Example:

  git checkout -b new_branch_name
```

3、安装 vector 插件

```
[postgres@67e1eed1b4b6 ~]$ cd pgvector/
[postgres@67e1eed1b4b6 pgvector]$ ll
total 48
-rw-rw-r-- 1 postgres postgres 1877 Dec  1 09:34 CHANGELOG.md
-rw-rw-r-- 1 postgres postgres  482 Dec  1 09:34 Dockerfile
-rw-rw-r-- 1 postgres postgres 1104 Dec  1 09:34 LICENSE
-rw-rw-r-- 1 postgres postgres 1760 Dec  1 09:34 Makefile
-rw-rw-r-- 1 postgres postgres 1105 Dec  1 09:34 META.json
-rw-rw-r-- 1 postgres postgres 9495 Dec  1 09:34 README.md
drwxrwxr-x 2 postgres postgres 4096 Dec  1 09:34 sql
drwxrwxr-x 2 postgres postgres 4096 Dec  1 09:34 src
drwxrwxr-x 6 postgres postgres 4096 Dec  1 09:34 test
-rw-rw-r-- 1 postgres postgres  135 Dec  1 09:34 vector.control


[postgres@67e1eed1b4b6 pgvector]$ which pg_config
~/tmp_basedir_polardb_pg_1100_bld/bin/pg_config

[postgres@67e1eed1b4b6 pgvector]$ USE_PGXS=1 make
[postgres@67e1eed1b4b6 pgvector]$ USE_PGXS=1 make install
```

4、加载 vector 插件

```
[postgres@67e1eed1b4b6 pgvector]$ psql -h 127.0.0.1
psql (11.9)
Type "help" for help.

postgres=# create extension vector ;
CREATE EXTENSION
```

5、测试向量数据搜索、以及索引加速搜索.

```
postgres=# CREATE TABLE items (embedding vector(3));
CREATE TABLE
postgres=# INSERT INTO items VALUES ('[1,2,3]'), ('[4,5,6]');
INSERT 0 2
postgres=# SELECT * FROM items ORDER BY embedding <-> '[3,1,2]' LIMIT 1;
 embedding
-----------
 [1,2,3]
(1 row)

postgres=# CREATE INDEX ON items USING ivfflat (embedding vector_l2_ops) WITH (lists = 100);
CREATE INDEX

postgres=# SELECT * FROM items ORDER BY embedding <-> '[3,1,2]' LIMIT 1;
 embedding
-----------
 [1,2,3]
(1 row)

postgres=# explain SELECT * FROM items ORDER BY embedding <-> '[3,1,2]' LIMIT 1;
                            QUERY PLAN
------------------------------------------------------------------
 Limit  (cost=1.03..1.04 rows=1 width=40)
   ->  Sort  (cost=1.03..1.04 rows=2 width=40)
         Sort Key: ((embedding <-> '[3,1,2]'::vector))
         ->  Seq Scan on items  (cost=0.00..1.02 rows=2 width=40)
(4 rows)

postgres=# set enable_seqscan=off;
SET
postgres=# explain SELECT * FROM items ORDER BY embedding <-> '[3,1,2]' LIMIT 1;
                                       QUERY PLAN
----------------------------------------------------------------------------------------
 Limit  (cost=4.08..6.09 rows=1 width=40)
   ->  Index Scan using items_embedding_idx on items  (cost=4.08..8.11 rows=2 width=40)
         Order By: (embedding <-> '[3,1,2]'::vector)
(3 rows)

postgres=# SET ivfflat.probes = 10;
SET
postgres=# SELECT * FROM items ORDER BY embedding <-> '[3,1,2]' LIMIT 1;
 embedding
-----------
 [1,2,3]
(1 row)

postgres=# explain SELECT * FROM items ORDER BY embedding <-> '[3,1,2]' LIMIT 1;
                                        QUERY PLAN
------------------------------------------------------------------------------------------
 Limit  (cost=40.80..42.81 rows=1 width=40)
   ->  Index Scan using items_embedding_idx on items  (cost=40.80..44.83 rows=2 width=40)
         Order By: (embedding <-> '[3,1,2]'::vector)
(3 rows)
```

## 参考

[《PostgreSQL + FDW + vector 插件加速向量检索 - 在不确定世界寻找确定答案 (例如图像相似)》](https://github.com/digoal/blog/blob/master/202203/20220302_01.md)

[《PostgreSQL 开源 高维向量相似搜索插件 vector - 关联阿里云 rds pg pase, cube, 人脸识别》](https://github.com/digoal/blog/blob/master/202105/20210514_03.md)

[《PostgreSQL 在资源搜索中的设计 - pase, smlar, pg_trgm - 标签+权重相似排序 - 标签的命中率排序》](https://github.com/digoal/blog/blob/master/202009/20200930_01.md)

[《社交、电商、游戏等 推荐系统 (相似推荐) - 阿里云 pase smlar 索引方案对比》](https://github.com/digoal/blog/blob/master/202004/20200421_01.md)

[《PostgreSQL 向量相似推荐设计 - pase》](https://github.com/digoal/blog/blob/master/202004/20200424_01.md)

[《PostgreSQL 阿里云 rds pg 发布高维向量索引，支持图像识别、人脸识别 - pase 插件》](https://github.com/digoal/blog/blob/master/201912/20191219_02.md)

[《如何用 PolarDB 证明巴菲特的投资理念》](https://github.com/digoal/blog/blob/master/202209/20220908_02.md)

https://github.com/pgvector/pgvector
