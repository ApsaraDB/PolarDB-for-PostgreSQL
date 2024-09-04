---
author: digoal
date: 2023/02/03
minute: 20
---

# 不用再害怕 like 模糊查询

<ArticleInfo :frontmatter=$frontmatter></ArticleInfo>

## 背景

PolarDB 的云原生存算分离架构, 具备低廉的数据存储、高效扩展弹性、高速多机并行计算能力、高速数据搜索和处理; PolarDB 与计算算法结合, 将实现双剑合璧, 推动业务数据的  
价值产出, 将数据变成生产力.

本文将介绍 PolarDB 开源版通过 pg_trgm GIN 索引实现高效率 `like '%xxx%'` 模糊查询

测试环境为 macos+docker, polardb 部署请参考:

- [《如何用 PolarDB 证明巴菲特的投资理念 - 包括 PolarDB 简单部署》](https://github.com/digoal/blog/blob/master/202209/20220908_02.md)

## 原理

pg_trgm 将字符串前面加 2 个空格, 后面加 1 个空格, 按每连续的 3 个字符为一组进行切分, 生成一堆 tokens, 例如 hello 被切分为`{"  h"," he",ell,hel,llo,"lo "}1`

```
postgres=# select show_trgm('hello');
            show_trgm
---------------------------------
 {"  h"," he",ell,hel,llo,"lo "}
(1 row)
```

对 tokens 创建 gin 索引, 在进行模糊搜索(甚至支持正则搜索)时, 将针对目标字符串条件也进行同样的 token 化处理(只是前后不需要再加空格, 除非输入了前缀或者后缀限定), 可以使用 gin 索引快速匹配到目标行.

更多原理可参考:

- [《重新发现 PostgreSQL 之美 - 16 like '%西出函谷关%' 模糊查询》](https://github.com/digoal/blog/blob/master/202106/20210607_01.md)

## 在 PolarDB 中使用 pg_trgm GIN 索引实现高效率 `like '%xxx%'` 模糊查询

1、建表, 生成 200 万条测试文本

```
create table tbl (id int, info text);

insert into tbl select id, md5(random()::text) from generate_series(1,1000000) id;
insert into tbl select id, md5(random()::text) from generate_series(1,1000000) id;
```

2、在没有索引的情况下, 进行模糊查询, 需要全表扫描, 耗时巨大.

```
explain (analyze,verbose,timing,costs,buffers) select * from tbl where info like '%abcd%';

postgres=# explain (analyze,verbose,timing,costs,buffers) select * from tbl where info like '%abcd%';
                                                  QUERY PLAN
---------------------------------------------------------------------------------------------------------------
 Seq Scan on public.tbl  (cost=0.00..41665.50 rows=200 width=37) (actual time=2.505..522.958 rows=851 loops=1)
   Output: id, info
   Filter: (tbl.info ~~ '%abcd%'::text)
   Rows Removed by Filter: 1999149
   Buffers: shared hit=16645 read=22 dirtied=8334
 Planning Time: 1.643 ms
 Execution Time: 523.138 ms
(7 rows)
```

3、创建 pg_trgm 插件, 以及 gin 索引.

```
postgres=# create extension pg_trgm ;
CREATE EXTENSION

create index on tbl using gin (info gin_trgm_ops);
```

4、使用 pg_trgm GIN 索引实现高效率 `like '%xxx%'` 模糊查询

```
explain (analyze,verbose,timing,costs,buffers) select * from tbl where info like '%abcd%';

postgres=# explain (analyze,verbose,timing,costs,buffers) select * from tbl where info like '%abcd%';
                                                        QUERY PLAN
--------------------------------------------------------------------------------------------------------------------------
 Bitmap Heap Scan on public.tbl  (cost=29.55..762.82 rows=200 width=37) (actual time=2.445..3.962 rows=851 loops=1)
   Output: id, info
   Recheck Cond: (tbl.info ~~ '%abcd%'::text)
   Rows Removed by Index Recheck: 96
   Heap Blocks: exact=926
   Buffers: shared hit=946
   ->  Bitmap Index Scan on tbl_info_idx  (cost=0.00..29.50 rows=200 width=0) (actual time=2.287..2.288 rows=947 loops=1)
         Index Cond: (tbl.info ~~ '%abcd%'::text)
         Buffers: shared hit=20
 Planning Time: 0.239 ms
 Execution Time: 4.112 ms
(11 rows)
```

性能提升 100 多倍.

## 参考

[《重新发现 PostgreSQL 之美 - 16 like '%西出函谷关%' 模糊查询》](https://github.com/digoal/blog/blob/master/202106/20210607_01.md)
