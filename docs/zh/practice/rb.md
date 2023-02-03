---
author: digoal
date: 2023/02/03
minute: 30
---

# 通过 roaringbitmap 实现用户画像等基于标签的数据圈选操作

<ArticleInfo :frontmatter=$frontmatter></ArticleInfo>

## 背景

PolarDB 的云原生存算分离架构, 具备低廉的数据存储、高效扩展弹性、高速多机并行计算能力、高速数据搜索和处理; PolarDB 与计算算法结合, 将实现双剑合璧, 推动业务数据的价值产出, 将数据变成生产力.

本文将介绍 PolarDB 开源版通过 roaringbitmap 支持用户画像等标签操作场景。.

测试环境为 macos+docker, polardb 部署请参考:

- [《如何用 PolarDB 证明巴菲特的投资理念 - 包括 PolarDB 简单部署》](https://github.com/digoal/blog/blob/master/202209/20220908_02.md)

## roaringbitmap for PolarDB

roaringbitmap 是 roaring bitmap 库在 PG 数据库中的一种类型实现，支持 roaring bitmap 的存取、集合操作，聚合等运算。

通常被用在用户画像等标签操作场景。

例如，

- 包含某些标签的人群集合，
- 某些人的共同点、不同点，
- 某人是否包含某标签。
- 某标签中是否包含某人。
- 同时包含某些标签的有多少人
- 某个标签有多少人

1、部署

```
git clone --depth 1 https://github.com/ChenHuajun/pg_roaringbitmap

cd pg_roaringbitmap/
USE_PGXS=1 make
USE_PGXS=1 make install
```

2、加载插件成功

```
[postgres@67e1eed1b4b6 pg_roaringbitmap]$ psql -h 127.0.0.1
psql (11.9)
Type "help" for help.

postgres=# create extension roaringbitmap ;
CREATE EXTENSION
postgres=# \q
```

3、插件自测

```
export PGHOST=127.0.0.1

[postgres@67e1eed1b4b6 pg_roaringbitmap]$ psql
psql (11.9)
Type "help" for help.

postgres=# \q



[postgres@67e1eed1b4b6 pg_roaringbitmap]$ USE_PGXS=1 make installcheck
/home/postgres/tmp_basedir_polardb_pg_1100_bld/lib/pgxs/src/makefiles/../../src/test/regress/pg_regress --inputdir=./ --bindir='/home/postgres/tmp_basedir_polardb_pg_1100_bld/bin'      --dbname=contrib_regression roaringbitmap
(using postmaster on 127.0.0.1, default port)
============== dropping database "contrib_regression" ==============
DROP DATABASE
============== creating database "contrib_regression" ==============
CREATE DATABASE
ALTER DATABASE
============== running regression test queries        ==============
test roaringbitmap                ... ok


==========================================================
 All 1 tests passed.

 POLARDB:
 All 1 tests, 0 tests in ignore, 0 tests in polar ignore.
==========================================================
```

## 例子: 存取、集合操作、聚合运算等例子

### 类型

roaringbitmap

### 操作符

与、或、疑惑、加、减、包含、相等、不相等、相交等

### 函数

加、减、包含、提取、剪切、转换等

### 聚合

与、或、异或、集合数量

### 限制

一个 roaring bitmap 可以存储 40 亿个元素，不能超出 int32 范畴。

范围超过 40 亿怎么办，offset 分段。例如使用多个字段表示。

```
create table tags(uid int, tag_off0 roaringbitmap, tag_off1 roaringbitmap)
```

## 设计举例

### 正向关系

user, tags

```
create table t1 (userid int, tags roaringbitmap);
```

### 反向

tag, users

```
create table t2 (tag int, uids roaringbitmap);
```

### 操作

1、roaringbitmap 类型输出形态

```
postgres=# show roaringbitmap.output_format ;
 roaringbitmap.output_format
-----------------------------
 bytea
(1 row)

postgres=# set roaringbitmap.output_format ='a';
psql: ERROR:  22023: invalid value for parameter "roaringbitmap.output_format": "a"
HINT:  Available values: array, bytea.
LOCATION:  parse_and_validate_value, guc.c:6682

postgres=# set roaringbitmap.output_format ='array';
SET
```

2、build roaringbitmap

```
postgres=# select roaringbitmap('{1,2,-1,-10000}');
  roaringbitmap
-----------------
 {1,2,-10000,-1}
(1 row)
```

3、增

```
postgres=# select roaringbitmap('{1,2,-1,-10000}') | 123;
      ?column?
---------------------
 {1,2,123,-10000,-1}
(1 row)

或

postgres=# select rb_add(roaringbitmap('{1,2,-1,-10000}') , 123);
       rb_add
---------------------
 {1,2,123,-10000,-1}
(1 row)
```

4、删

```
postgres=# select roaringbitmap('{1,2,3}') - 3;
 ?column?
----------
 {1,2}
(1 row)


或


postgres=# select  rb_remove(roaringbitmap('{1,2,3}'),1);
 rb_remove
-----------
 {2,3}
(1 row)

postgres=# select  rb_remove(roaringbitmap('{1,2,3}'),4);
 rb_remove
-----------
 {1,2,3}
(1 row)

```

5、判断(包含，相交，相等，不相等)

```
postgres=# select roaringbitmap('{1,2,3,4,5}') @> 3;
 ?column?
----------
 t
(1 row)

postgres=# select roaringbitmap('{1,2,3,4,5}') @> 30;
 ?column?
----------
 f
(1 row)

postgres=# select roaringbitmap('{1,2,3,4,5}') @> roaringbitmap('{1,2,3}');
 ?column?
----------
 t
(1 row)

postgres=# select roaringbitmap('{1,2,3,4,5}') @> roaringbitmap('{1,2,3,9}');
 ?column?
----------
 f
(1 row)

postgres=# select roaringbitmap('{1,2,3,4,5}') && roaringbitmap('{1,2,3,9}');
 ?column?
----------
 t
(1 row)

postgres=# select roaringbitmap('{1,2,3,4,5}') = roaringbitmap('{1,2,3,9}');
 ?column?
----------
 f
(1 row)

postgres=# select roaringbitmap('{1,2,3,4,5}') = roaringbitmap('{1,5,4,3,2,1,1}');
 ?column?
----------
 t
(1 row)

postgres=# select roaringbitmap('{1,2,3,4,5}') <> roaringbitmap('{1}');
 ?column?
----------
 t
(1 row)
```

6、取值

```
postgres=# SELECT rb_iterate('{1,2,3}'::roaringbitmap);
 rb_iterate
------------
          1
          2
          3
(3 rows)

postgres=# SELECT unnest(rb_to_array('{1,2,3}'::roaringbitmap));
 unnest
--------
      1
      2
      3
(3 rows)

postgres=# SELECT unnest(rb_to_array('{1,2,3,3,3}'::roaringbitmap));
 unnest
--------
      1
      2
      3
(3 rows)
```

值的索引(从 0 开始 offset)。顺序：

```
0, 1, ..., 2147483647, -2147483648, -2147483647,..., -1
```

返回有序值存放的 index

```
Return the 0-based index of element in this roaringbitmap, or -1 if do not exsits

postgres=# select  rb_index('{-1,1,2,3}',3);
 rb_index
----------
        2
(1 row)

postgres=# select  rb_index('{100,-1,1,2,3}',3);
 rb_index
----------
        2
(1 row)

postgres=# select  rb_index('{0,100,-1,1,2,3}',3);
 rb_index
----------
        3
(1 row)

postgres=# select  rb_index('{0,100,-1,1,2,3}',-1);
 rb_index
----------
        5
(1 row)

postgres=# select  rb_index('{0,100,-1,1,2,3,-2}',-1);
 rb_index
----------
        6
(1 row)
```

按索引取值(从 0 开始 offset)

顺序：

```
0, 1, ..., 2147483647, -2147483648, -2147483647,..., -1
```

```
Return subset [bitset_offset,bitset_offset+bitset_limit) of bitmap between range [range_start,range_end)

从index 5开始取数，取2个。
postgres=#  select rb_select('{-1,-999,100,1,2,3,102,5,6,7,8,9}',2,5);
 rb_select
-----------
 {7,8}
(1 row)

从index 2开始取数，取5个。
postgres=#  select rb_select('{-1,-999,100,1,2,3,102,5,6,7,8,9}',5,2);
  rb_select
-------------
 {3,5,6,7,8}
(1 row)
```

### 聚合计算

1、多个 rb 共同点集合

```
select rb_and_agg(bitmap)
    from (values (roaringbitmap('{1,2,3}')),
                 (roaringbitmap('{2,3,4}'))
          ) t(bitmap)
```

2、多个 rb 不同点集合

```
select rb_xor_agg(bitmap)
    from (values (roaringbitmap('{1,2,3}')),
                 (roaringbitmap('{2,3,4}'))
          ) t(bitmap)
```

3、多个 rb 集合点

```
select rb_or_agg(bitmap)
    from (values (roaringbitmap('{1,2,3}')),
                 (roaringbitmap('{2,3,4}'))
          ) t(bitmap)
```

4、两个 rb 的差值

```
roaringbitmap('{1,2,3}') - roaringbitmap('{3,4,5}')
```

5、集合有多大

```
select rb_or_cardinality_agg(bitmap)
    from (values (roaringbitmap('{1,2,3}')),
                 (roaringbitmap('{2,3,4}'))
          ) t(bitmap)
```

6、有多少共同点

```
select rb_and_cardinality_agg(bitmap)
    from (values (roaringbitmap('{1,2,3}')),
                 (roaringbitmap('{2,3,4}'))
          ) t(bitmap)
```

7、有多少不同点

```
select rb_xor_cardinality_agg(bitmap)
    from (values (roaringbitmap('{1,2,3}')),
                 (roaringbitmap('{2,3,4}'))
          ) t(bitmap)
```

8、将多个值聚合为 roaringbitmap

```
select rb_build_agg(id)
    from (values (1),(2),(3)) t(id)
```

## 参考

https://github.com/ChenHuajun/pg_roaringbitmap

- [《PostgreSQL roaringbitmap UID 溢出（超出 int4(32 字节)）时的处理方法 - offset》](https://github.com/digoal/blog/blob/master/202001/20200110_03.md)
- [《画像系统标准化设计 - PostgreSQL roaringbitmap, varbitx , 正向关系, 反向关系, 圈选, 相似扩选(向量相似扩选)》](https://github.com/digoal/blog/blob/master/201911/20191128_02.md)
- [《PostgreSQL pg_roaringbitmap - 用户画像、标签、高效检索》](https://github.com/digoal/blog/blob/master/201911/20191118_01.md)
- [《Greenplum roaring bitmap 与业务场景 (类阿里云 RDS PG varbitx, 应用于海量用户 实时画像和圈选、透视)》](https://github.com/digoal/blog/blob/master/201801/20180127_01.md)
- [《PostgreSQL (varbit, roaring bitmap) VS pilosa(bitmap 库)》](https://github.com/digoal/blog/blob/master/201706/20170612_01.md)
