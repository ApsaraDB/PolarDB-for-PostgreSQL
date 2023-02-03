---
author: digoal
date: 2023/02/03
minute: 10
---

# 相似文本搜索、自助选药、相似人群圈选等场景加速实践

<ArticleInfo :frontmatter=$frontmatter></ArticleInfo>

## 背景

PolarDB 的云原生存算分离架构, 具备低廉的数据存储、高效扩展弹性、高速多机并行计算能力、高速数据搜索和处理; PolarDB 与计算算法结合, 将实现双剑合璧, 推动业务数据的价值产出, 将数据变成生产力.

本文将介绍使用 PolarDB 开源版 smlar 插件进行高效率相似文本搜索、自助选药、相似人群圈选等业务

测试环境为 macOS+docker, PolarDB 部署请参考下文:

- [《如何用 PolarDB 证明巴菲特的投资理念 - 包括 PolarDB 简单部署》](https://github.com/digoal/blog/blob/master/202209/20220908_02.md)

## 场景

1、自助匹配药品, 例如用户根据病情描述, 自动匹配相关的药品. 这个属于文本相似范畴. 文本相似性:

注意有语义的情况:

- 感冒,不发烧,咳嗽,无痰,流清涕,肌肉酸痛
- 感冒,发烧,咳嗽,有痰,无鼻涕

将药品主治症状的文本向量化, 存储为文本数组.

根据病人描述, 将文本向量化, 在药品库中进行文本向量的相似匹配, 快速找到最匹配的药品.

2、根据特征进行人群扩选, 例如在数据库中存储了每个用户的特征(使用数组表示)

根据输入的数组(画像)搜索相似人群, 即人群扩选, 业务上进行精准推送.

3、文章相似性搜索, 因为文章关键字很多, 每个关键字的权重也不一样, 不能只按命中多少关键字来决定相似性. 可以借助 tfidf, 结合总文本数, 关键字在所有文本中出现的次数, 命中关键字等进行计算.

在所有文本中出现次数越多的关键字, 根据算法其权重可能越低. 具体算法可参考:

- [《PostgreSQL 结合余弦、线性相关算法 在文本、图片、数组相似 等领域的应用 - 1 文本(关键词)分析理论基础 - TF(Term Frequency 词频)/IDF(Inverse Document Frequency 逆向文本频率)》](https://github.com/digoal/blog/blob/master/201701/20170116_02.md)
- https://www.pgcon.org/2012/schedule/attachments/252_smlar-2012.pdf

## 设计与算法

以上需求实际上都是多值列的相似计算, 使用 smlar 插件即可实现.

数据存储: 多值列(例如数组)

多值列的相似性算法: cosine, overlap, tfidf.

```
	switch(getSmlType())
	{
		case ST_TFIDF:
			PG_RETURN_FLOAT4( TFIDFSml(sa, sb) );
			break;
		case ST_COSINE:
			{
				int				cnt;
				double			power;

				power = ((double)(sa->nelems)) * ((double)(sb->nelems));
				cnt = numOfIntersect(sa, sb);

				PG_RETURN_FLOAT4(  ((double)cnt) / sqrt( power ) );
			}
			break;
		case ST_OVERLAP:
			{
				float4 res = (float4)numOfIntersect(sa, sb);

				PG_RETURN_FLOAT4(res);
			}
			break;
```

元素去重后计算.

```
postgres=# set smlar.type='cosine';
SET
postgres=# SELECT smlar('{1,4,6}'::int[], '{5,4,6}' );
  smlar
----------
 0.666667
(1 row)
postgres=# SELECT smlar('{1,4,6}'::int[], '{5,4,4,6}' );
  smlar
----------
 0.666667
(1 row)
-- 2/sqrt(3*3)

postgres=# set smlar.type='overlap';
SET
postgres=# SELECT smlar('{1,4,6}'::int[], '{5,4,4,6}' );
 smlar
-------
     2
(1 row)
-- 2

postgres=# set smlar.type='tfidf';
SET

-- 设置tfidf表, 这个表可以用采样文档统计得到, 也可以自由定义其内容
set smlar.stattable = 'documents_body_stats';

create table documents_body_stats (  -- tfidf权重表.
  value text unique,  -- value表示的关键字出现在多少篇文档中; value is null的行表示总文档篇数;
  ndoc int not null
);

insert into documents_body_stats values ('0', 1); -- 0 出现在了1篇文章中.
insert into documents_body_stats values ('1', 100); -- 1 出现在了100篇文章中.
insert into documents_body_stats values ('4', 101), ('6', 201);
insert into documents_body_stats values ('5', 1001);
insert into documents_body_stats values (null, 10000);   -- value is null的行表示总文档篇数;

postgres=# SELECT smlar('{1,4,6}'::text[], '{5,4,4,6}' );
  smlar
----------
 0.742594
(1 row)

postgres=# SELECT smlar('{1,4,6}'::text[], '{5,5,5,6,6}' );
 smlar
--------
 0.4436
(1 row)

postgres=# SELECT smlar('{0,1,4,5,6}'::text[], '{0,1,5}' );
  smlar
----------
 0.868165
(1 row)

postgres=# SELECT smlar('{0,1,4,5,6}'::text[], '{1,5,6}' );
  smlar
----------
 0.531762
(1 row)
```

## 加速原理

smlar 对数组支持 gin 和 gist 两个索引接口, 以 gin 为例, 如何快速筛选相似的记录?

例如, 输入条件的数组长度为 6, 使用 overlap 算法, 要求相似度为 4, 那么必须要有 4 个或 4 个以上元素命中的记录才符合要求.

- 在 gin 索引中搜索元素 1, 提取到 ctid 里的 blockid, 每个 blockid +1.
- 在 gin 索引中搜索元素 2, 提取到 ctid 里的 blockid, 每个 blockid +1.
- 在 gin 索引中搜索元素 3, 提取到 ctid 里的 blockid, 每个 blockid +1.
- 在 gin 索引中搜索元素 4, 提取到 ctid 里的 blockid, 每个 blockid +1.
- 在 gin 索引中搜索元素 5, 提取到 ctid 里的 blockid, 每个 blockid +1.
- 在 gin 索引中搜索元素 6, 提取到 ctid 里的 blockid, 每个 blockid +1.

在以上 blockid 中, 数据库只需要回表搜索大于等于 4 的 blockid, recheck 是否满足相似条件.

gin,gist 支持的 operator calss?

GiST/GIN support for % and && operations for:

| Array Type    | GIN operator class    | GiST operator class   |
| ------------- | --------------------- | --------------------- |
| bit[]         | \_bit_sml_ops         |
| bytea[]       | \_bytea_sml_ops       | \_bytea_sml_ops       |
| char[]        | \_char_sml_ops        | \_char_sml_ops        |
| cidr[]        | \_cidr_sml_ops        | \_cidr_sml_ops        |
| date[]        | \_date_sml_ops        | \_date_sml_ops        |
| float4[]      | \_float4_sml_ops      | \_float4_sml_ops      |
| float8[]      | \_float8_sml_ops      | \_float8_sml_ops      |
| inet[]        | \_inet_sml_ops        | \_inet_sml_ops        |
| int2[]        | \_int2_sml_ops        | \_int2_sml_ops        |
| int4[]        | \_int4_sml_ops        | \_int4_sml_ops        |
| int8[]        | \_int8_sml_ops        | \_int8_sml_ops        |
| interval[]    | \_interval_sml_ops    | \_interval_sml_ops    |
| macaddr[]     | \_macaddr_sml_ops     | \_macaddr_sml_ops     |
| money[]       | \_money_sml_ops       |
| numeric[]     | \_numeric_sml_ops     | \_numeric_sml_ops     |
| oid[]         | \_oid_sml_ops         | \_oid_sml_ops         |
| text[]        | \_text_sml_ops        | \_text_sml_ops        |
| time[]        | \_time_sml_ops        | \_time_sml_ops        |
| timestamp[]   | \_timestamp_sml_ops   | \_timestamp_sml_ops   |
| timestamptz[] | \_timestamptz_sml_ops | \_timestamptz_sml_ops |
| timetz[]      | \_timetz_sml_ops      | \_timetz_sml_ops      |
| varbit[]      | \_varbit_sml_ops      |
| varchar[]     | \_varchar_sml_ops     | \_varchar_sml_ops     |

## 例子

1、部署 smlar on PolarDB

```
git clone --depth 1  git://sigaev.ru/smlar.git


cd smlar/

USE_PGXS=1 make
USE_PGXS=1 make install


[postgres@aa25c5be9681 smlar]$ USE_PGXS=1 make installcheck
/home/postgres/tmp_basedir_polardb_pg_1100_bld/lib/pgxs/src/makefiles/../../src/test/regress/pg_regress --inputdir=./ --bindir='/home/postgres/tmp_basedir_polardb_pg_1100_bld/bin'      --dbname=contrib_regression smlar int2 int4 int8 float4 float8 money oid timestamp timestamptz time timetz date interval macaddr	inet cidr text varchar char bytea bit varbit numeric int4g int8g intervalg textg int4i int8i intervali texti composite_int4 composite_text
(using postmaster on 127.0.0.1, default port)
============== dropping database "contrib_regression" ==============
DROP DATABASE
============== creating database "contrib_regression" ==============
CREATE DATABASE
ALTER DATABASE
============== running regression test queries        ==============
test smlar                        ... ok
test int2                         ... ok
test int4                         ... ok
test int8                         ... ok
test float4                       ... ok
test float8                       ... ok
test money                        ... ok
test oid                          ... ok
test timestamp                    ... ok
test timestamptz                  ... ok
test time                         ... ok
test timetz                       ... ok
test date                         ... ok
test interval                     ... ok
test macaddr                      ... ok
test inet                         ... ok
test cidr                         ... ok
test text                         ... ok
test varchar                      ... ok
test char                         ... ok
test bytea                        ... ok
test bit                          ... ok
test varbit                       ... ok
test numeric                      ... ok
test int4g                        ... ok
test int8g                        ... ok
test intervalg                    ... ok
test textg                        ... ok
test int4i                        ... ok
test int8i                        ... ok
test intervali                    ... ok
test texti                        ... ok
test composite_int4               ... ok
test composite_text               ... ok


===========================================================
 All 34 tests passed.

 POLARDB:
 All 34 tests, 0 tests in ignore, 0 tests in polar ignore.
===========================================================
```

2、安装插件

```
postgres=# create extension smlar ;
CREATE EXTENSION
```

3、创建测试表, 写入测试数据

```
create table tbl (id int, propt int[]);

create or replace function gen_arr(normal int, hot int) returns int[] as $$
  select array(select (100000*random())::int+500 from generate_series(1,$1)) || array(select (500*random())::int from generate_series(1,$2));
$$ language sql strict;

insert into tbl select id, gen_arr(22, 10) from generate_series(1,2000000) id;

postgres=# select * from tbl limit 5;
 id |                                                                                     propt
----+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  1 | {1386,57573,55117,44934,83223,3444,77658,49523,85849,62549,99593,40714,53146,32510,68449,33662,45912,70227,64560,78831,86052,56387,157,490,51,484,53,176,273,240,300,277}
  2 | {15075,100383,88390,18019,77540,37413,3368,39590,36506,43582,92236,68516,11532,25398,13927,81259,89457,92259,66811,45344,23676,64902,275,100,375,451,373,116,251,150,141,324}
  3 | {16664,82803,7375,53577,85671,46465,89583,28753,38201,57599,39785,63099,71026,20543,52056,62785,86854,96900,85960,51256,51917,5901,129,208,400,244,459,49,386,283,198,467}
  4 | {47066,46889,24635,93031,35972,52888,30732,93071,92172,93330,63597,12216,44887,25882,98570,41287,11343,49327,92704,16743,75095,34373,481,117,129,30,3,412,228,470,107,461}
  5 | {46010,85290,76290,98398,15522,68861,90070,8352,31959,1786,52739,57341,99856,93526,68184,48683,85730,84427,23278,19603,80575,46747,224,430,234,136,159,204,243,120,406,471}
(5 rows)
```

4、创建索引

```
create index on tbl using gin (propt _int4_sml_ops);
```

5、相似度搜索

overlap

```
postgres=# set smlar.type ='overlap';
SET
postgres=# set smlar.threshold=10;
SET

postgres=# explain analyze select * from tbl where propt % '{157,490,51,484,53,176,273,240,300,277}'::int[];
                                                         QUERY PLAN
-----------------------------------------------------------------------------------------------------------------------------
 Bitmap Heap Scan on tbl  (cost=219.50..6871.30 rows=2000 width=36) (actual time=37.548..37.549 rows=1 loops=1)
   Recheck Cond: (propt % '{157,490,51,484,53,176,273,240,300,277}'::integer[])
   Heap Blocks: exact=1
   ->  Bitmap Index Scan on tbl_propt_idx  (cost=0.00..219.00 rows=2000 width=0) (actual time=37.514..37.515 rows=1 loops=1)
         Index Cond: (propt % '{157,490,51,484,53,176,273,240,300,277}'::integer[])
 Planning Time: 0.161 ms
 Execution Time: 37.593 ms
(7 rows)

Time: 38.683 ms
postgres=# select * from tbl where propt % '{157,490,51,484,53,176,273,240,300,277}'::int[];
 id |                                                                                   propt
----+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  1 | {1386,57573,55117,44934,83223,3444,77658,49523,85849,62549,99593,40714,53146,32510,68449,33662,45912,70227,64560,78831,86052,56387,157,490,51,484,53,176,273,240,300,277}
(1 row)

Time: 38.794 ms

关闭索引, 性能直线下降:
postgres=# set enable_bitmapscan =off;
SET
Time: 0.510 ms
postgres=# select * from tbl where propt % '{157,490,51,484,53,176,273,240,300,277}'::int[];
 id |                                                                                   propt
----+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  1 | {1386,57573,55117,44934,83223,3444,77658,49523,85849,62549,99593,40714,53146,32510,68449,33662,45912,70227,64560,78831,86052,56387,157,490,51,484,53,176,273,240,300,277}
(1 row)

Time: 12553.942 ms (00:12.554)
```

采用 smlar 提速 100 倍以上.

cosine

```
postgres=# set smlar.type ='cosine';
SET

postgres=# set smlar.threshold=0.55;
SET
Time: 1.107 ms
postgres=# select * from tbl where propt % '{157,490,51,484,53,176,273,240,300,277}'::int[];
 id |                                                                                   propt
----+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  1 | {1386,57573,55117,44934,83223,3444,77658,49523,85849,62549,99593,40714,53146,32510,68449,33662,45912,70227,64560,78831,86052,56387,157,490,51,484,53,176,273,240,300,277}
(1 row)

Time: 42.701 ms
```

tfidf

- 例如将所有的药品说明书进行文本向量处理, 提取关键字, 生成 tfidf 表.
- 请自行测试

## 参考

https://github.com/jirutka/smlar

- [《PostgreSQL 在资源搜索中的设计 - pase, smlar, pg_trgm - 标签+权重相似排序 - 标签的命中率排序》](https://github.com/digoal/blog/blob/master/202009/20200930_01.md)
- [《社交、电商、游戏等 推荐系统 (相似推荐) - 阿里云 pase smlar 索引方案对比》](https://github.com/digoal/blog/blob/master/202004/20200421_01.md)
- [《PostgreSQL 相似搜索插件介绍大汇总 (cube,rum,pg_trgm,smlar,imgsmlr,pg_similarity) (rum,gin,gist)》](https://github.com/digoal/blog/blob/master/201809/20180904_01.md)
- [《海量数据,海明(simhash)距离高效检索(smlar) - 阿里云 RDS PosgreSQL 最佳实践 - bit string 比特字符串 相似度搜索》](https://github.com/digoal/blog/blob/master/201708/20170804_01.md)
- [《PostgreSQL 结合余弦、线性相关算法 在文本、图片、数组相似 等领域的应用 - 3 rum, smlar 应用场景分析》](https://github.com/digoal/blog/blob/master/201701/20170116_04.md)
- [《PostgreSQL 结合余弦、线性相关算法 在文本、图片、数组相似 等领域的应用 - 2 smlar 插件详解》](https://github.com/digoal/blog/blob/master/201701/20170116_03.md)
- [《使用 PolarDB 开源版 和 imgsmlr 存储图像特征值以及快速的进行图像相似搜索》](https://github.com/digoal/blog/blob/master/202212/20221222_04.md)
- [《PolarDB 开源版通过 pg_similarity 实现 17 种文本相似搜索 - token 归一切分, 根据文本相似度检索相似文本.》](https://github.com/digoal/blog/blob/master/202212/20221209_01.md)
- [《如何用 PolarDB 在不确定世界寻找确定答案 (例如图像相似) - vector|pase》](https://github.com/digoal/blog/blob/master/202212/20221201_02.md)
- [《DuckDB 字符串相似性计算函数》](https://github.com/digoal/blog/blob/master/202208/20220829_02.md)
- [《JSON 局部相似 搜索例子》](https://github.com/digoal/blog/blob/master/202203/20220323_02.md)
- [《PostgreSQL + FDW + vector 插件加速向量检索 - 在不确定世界寻找确定答案 (例如图像相似)》](https://github.com/digoal/blog/blob/master/202203/20220302_01.md)
- [《PostgreSQL 开源 高维向量相似搜索插件 vector - 关联阿里云 rds pg pase, cube, 人脸识别》](https://github.com/digoal/blog/blob/master/202105/20210514_03.md)
- [《PostgreSQL 应用开发解决方案最佳实践系列课程 - 7. 标签搜索和圈选、相似搜索和圈选、任意字段组合搜索和圈选系统》](https://github.com/digoal/blog/blob/master/202105/20210510_01.md)
- [《PostgreSQL 应用开发解决方案最佳实践系列课程 - 3. 人脸识别和向量相似搜索》](https://github.com/digoal/blog/blob/master/202105/20210506_01.md)
- [《PostgreSQL 文本相似搜索 - pg_trgm_pro - 包含则 1, 不包含则计算 token 相似百分比》](https://github.com/digoal/blog/blob/master/202101/20210103_01.md)
- [《PostgreSQL 在资源搜索中的设计 - pase, smlar, pg_trgm - 标签+权重相似排序 - 标签的命中率排序》](https://github.com/digoal/blog/blob/master/202009/20200930_01.md)
- [《PostgreSQL 模糊查询、相似查询 (like '%xxx%') pg_bigm 比 pg_trgm 优势在哪?》](https://github.com/digoal/blog/blob/master/202009/20200912_01.md)
- [《PostgreSQL 向量相似推荐设计 - pase》](https://github.com/digoal/blog/blob/master/202004/20200424_01.md)
- [《社交、电商、游戏等 推荐系统 (相似推荐) - 阿里云 pase smlar 索引方案对比》](https://github.com/digoal/blog/blob/master/202004/20200421_01.md)
- [《PostgreSQL ghtree 实现的海明距离排序索引, 性能不错(模糊图像) - pg-knn_hamming - bit string 比特字符串 相似度搜索》](https://github.com/digoal/blog/blob/master/202003/20200326_08.md)
- [《PostgreSQL bktree 索引 using gist 例子 - 海明距离检索 - 短文相似、模糊图像搜索 - bit string 比特字符串 相似度搜索》](https://github.com/digoal/blog/blob/master/202003/20200324_29.md)
- [《阿里云 PostgreSQL 案例精选 2 - 图像识别、人脸识别、相似特征检索、相似人群圈选》](https://github.com/digoal/blog/blob/master/202002/20200227_01.md)
- [《PostgreSQL+MySQL 联合解决方案 - 第 12 课视频 - 全文检索、中文分词、模糊查询、相似文本查询》](https://github.com/digoal/blog/blob/master/202001/20200116_01.md)
- [《PostgreSQL+MySQL 联合解决方案 - 第 11 课视频 - 多维向量相似搜索 - 图像识别、相似人群圈选等》](https://github.com/digoal/blog/blob/master/202001/20200115_01.md)
- [《PostgreSQL+MySQL 联合解决方案 - 第 9 课视频 - 实时精准营销(精准圈选、相似扩选、用户画像)》](https://github.com/digoal/blog/blob/master/202001/20200113_01.md)
- [《画像系统标准化设计 - PostgreSQL roaringbitmap, varbitx , 正向关系, 反向关系, 圈选, 相似扩选(向量相似扩选)》](https://github.com/digoal/blog/blob/master/201911/20191128_02.md)
- [《阿里云 PostgreSQL 向量搜索、相似搜索、图像搜索 插件 palaemon - ivfflat , hnsw , nsg , ssg》](https://github.com/digoal/blog/blob/master/201908/20190815_01.md)
- [《PostgreSQL 多维、图像 欧式距离、向量距离、向量相似 查询优化 - cube,imgsmlr - 压缩、分段、异步并行》](https://github.com/digoal/blog/blob/master/201811/20181129_01.md)
- [《PostgreSQL 相似人群圈选，人群扩选，向量相似 使用实践 - cube》](https://github.com/digoal/blog/blob/master/201810/20181011_01.md)
- [《PostgreSQL 11 相似图像搜索插件 imgsmlr 性能测试与优化 3 - citus 8 机 128shard (4 亿图像)》](https://github.com/digoal/blog/blob/master/201809/20180904_04.md)
- [《PostgreSQL 11 相似图像搜索插件 imgsmlr 性能测试与优化 2 - 单机分区表 (dblink 异步调用并行) (4 亿图像)》](https://github.com/digoal/blog/blob/master/201809/20180904_03.md)
- [《PostgreSQL 11 相似图像搜索插件 imgsmlr 性能测试与优化 1 - 单机单表 (4 亿图像)》](https://github.com/digoal/blog/blob/master/201809/20180904_02.md)
- [《PostgreSQL 相似搜索插件介绍大汇总 (cube,rum,pg_trgm,smlar,imgsmlr,pg_similarity) (rum,gin,gist)》](https://github.com/digoal/blog/blob/master/201809/20180904_01.md)
- [《Greenplum 轨迹相似(伴随分析)》](https://github.com/digoal/blog/blob/master/201806/20180607_02.md)
- [《PostgreSQL 相似文本检索与去重 - (银屑病怎么治？银屑病怎么治疗？银屑病怎么治疗好？银屑病怎么能治疗好？)》](https://github.com/digoal/blog/blob/master/201803/20180329_01.md)
- [《PostgreSQL 相似搜索分布式架构设计与实践 - dblink 异步调用与多机并行(远程 游标+记录 UDF 实例)》](https://github.com/digoal/blog/blob/master/201802/20180205_03.md)
- [《PostgreSQL 相似搜索设计与性能 - 地址、QA、POI 等文本 毫秒级相似搜索实践》](https://github.com/digoal/blog/blob/master/201802/20180202_01.md)
- [《PostgreSQL 遗传学应用 - 矩阵相似距离计算 (欧式距离,...XX 距离)》](https://github.com/digoal/blog/blob/master/201712/20171227_01.md)
- [《用 PostgreSQL 做实时高效 搜索引擎 - 全文检索、模糊查询、正则查询、相似查询、ADHOC 查询》](https://github.com/digoal/blog/blob/master/201712/20171205_02.md)
- [《HTAP 数据库 PostgreSQL 场景与性能测试之 17 - (OLTP) 数组相似查询》](https://github.com/digoal/blog/blob/master/201711/20171107_18.md)
- [《HTAP 数据库 PostgreSQL 场景与性能测试之 16 - (OLTP) 文本特征向量 - 相似特征(海明...)查询》](https://github.com/digoal/blog/blob/master/201711/20171107_17.md)
- [《HTAP 数据库 PostgreSQL 场景与性能测试之 13 - (OLTP) 字符串搜索 - 相似查询》](https://github.com/digoal/blog/blob/master/201711/20171107_14.md)
- [《海量数据,海明(simhash)距离高效检索(smlar) - 阿里云 RDS PosgreSQL 最佳实践 - bit string 比特字符串 相似度搜索》](https://github.com/digoal/blog/blob/master/201708/20170804_01.md)
- [《17 种文本相似算法与 GIN 索引 - pg_similarity》](https://github.com/digoal/blog/blob/master/201705/20170524_01.md)
- [《PostgreSQL 结合余弦、线性相关算法 在文本、图片、数组相似 等领域的应用 - 3 rum, smlar 应用场景分析》](https://github.com/digoal/blog/blob/master/201701/20170116_04.md)
- [《PostgreSQL 结合余弦、线性相关算法 在文本、图片、数组相似 等领域的应用 - 2 smlar 插件详解》](https://github.com/digoal/blog/blob/master/201701/20170116_03.md)
- [《PostgreSQL 结合余弦、线性相关算法 在文本、图片、数组相似 等领域的应用 - 1 文本(关键词)分析理论基础 - TF(Term Frequency 词频)/IDF(Inverse Document Frequency 逆向文本频率)》](https://github.com/digoal/blog/blob/master/201701/20170116_02.md)
- [《导购系统 - 电商内容去重\内容筛选应用(实时识别转载\盗图\侵权?) - 文本、图片集、商品集、数组相似判定的优化和索引技术》](https://github.com/digoal/blog/blob/master/201701/20170112_02.md)
- [《从相似度算法谈起 - Effective similarity search in PostgreSQL》](https://github.com/digoal/blog/blob/master/201612/20161222_02.md)
- [《聊一聊双十一背后的技术 - 毫秒分词算啥, 试试正则和相似度》](https://github.com/digoal/blog/blob/master/201611/20161118_01.md)
- [《PostgreSQL 文本数据分析实践之 - 相似度分析》](https://github.com/digoal/blog/blob/master/201608/20160817_01.md)
