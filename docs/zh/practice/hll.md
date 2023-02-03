---
author: digoal
date: 2023/02/03
minute: 30
---

# 通过 HLL 实现滑动分析、实时推荐已读列表过滤

<ArticleInfo :frontmatter=$frontmatter></ArticleInfo>

## 背景

PolarDB 的云原生存算分离架构, 具备低廉的数据存储、高效扩展弹性、高速多机并行计算能力、高速数据搜索和处理; PolarDB 与计算算法结合, 将实现双剑合璧, 推动业务数据的价值产出, 将数据变成生产力.

本文将介绍 PolarDB 开源版通过 postgresql_hll 实现高效率 UV 滑动分析、实时推荐已读列表过滤

测试环境为 macos+docker, polardb 部署请参考:

- [《如何用 PolarDB 证明巴菲特的投资理念 - 包括 PolarDB 简单部署》](https://github.com/digoal/blog/blob/master/202209/20220908_02.md)

## postgresql_hll 简单介绍

postgresql_hll 是高效率存储一堆唯一值的"hash value"的插件:  
可以

- 往这个"hash value"里面追加内容.
- 有多少唯一值
- 两个 hash value 的差值个数
- 两个 hash value 的 union
- 多个 hash value 的 union

一个 hll 可能使用十几 KB 存储数亿唯一值.

常见场景:

1、

- UV
- 滑动窗口 UV
- 新增 UV
- 同比, 环比

2、短视频推荐业务, 只推荐未读的短视频. 使用 postgresql_hll 可以高效率记录和过滤已读列表.

hll 也有点类似 bloom filter:

- 如果判断结果为 val 在 hll 里面, 实际 val 可能不在 hll 里面. 因为是失真存储, 那么多个 val 的占位 bitmask 可能覆盖其他 val 的 bitmask.
- 如果判断结果为 val 不在 hll 里面, 则一定不在.

## postgresql_hll for PolarDB

1、安装部署 postgresql_hll for polardb

```
git clone --depth 1 https://github.com/citusdata/postgresql-hll

export PGHOST=localhost
[postgres@67e1eed1b4b6 ~]$ psql
psql (11.9)
Type "help" for help.

postgres=# \q


cd postgresql-hll/

USE_PGXS=1 make

USE_PGXS=1 make install

USE_PGXS=1 make installcheck
```

2、使用例子

建表, 写入大量 UID 的行为数据. 生成按天的 UV 数据, 使用 hll 存储 uid hash.

```
create table t1 (id int, uid int, info text, crt_time timestamp);
create table t1_hll (dt date, hllval hll);
insert into t1 select id, random()*100000, random()::text, now() from generate_series(1,1000000) id;
insert into t1 select id, random()*100000, random()::text, now()+interval '1 day' from generate_series(1,1000000) id;
insert into t1_hll select date(crt_time), hll_add_agg(hll_hash_integer(uid)) from t1 group by 1;
```

判断 UID 是否在 hll hash 内, 检查 hll 精确性.

```
postgres=# select t1.uid, t2.hllval=hll_add(t2.hllval, hll_hash_integer(t1.uid)) from t1 , t1_hll t2 where t2.dt=date(now()) and t1.crt_time < date(now())+1 limit 10;
  uid  | ?column?
-------+----------
 95912 | t
 69657 | t
 53722 | t
 95821 | t
  2836 | t
 66298 | t
 68466 | t
 10122 | t
 27861 | t
  6824 | t
(10 rows)


select * from
  (select t1.uid, t2.hllval=hll_add(t2.hllval, hll_hash_integer(t1.uid)) as yesorno from t1 , t1_hll t2 where t2.dt=date(now()) and t1.crt_time < date(now())+1) t
where t.yesorno=false;

 uid | yesorno
-----+---------
(0 rows)

-- 完全正确.
```

划窗分析, 例如直接在 hll 的统计表中, 统计任意 7 天的划窗口 uv. 如果没有 HLL, 划窗分析必须去基表进行统计, 性能极差. 而有了 hll, 只需要访问 7 条记录, 聚合即可.

```
## What if you wanted to this week's uniques?

SELECT hll_cardinality(hll_union_agg(users)) FROM daily_uniques WHERE date >= '2012-01-02'::date AND date <= '2012-01-08'::date;

## Or the monthly uniques for this year?

SELECT EXTRACT(MONTH FROM date) AS month, hll_cardinality(hll_union_agg(users))
FROM daily_uniques
WHERE date >= '2012-01-01' AND
      date <  '2013-01-01'
GROUP BY 1;

## Or how about a sliding window of uniques over the past 6 days?

SELECT date, #hll_union_agg(users) OVER seven_days
FROM daily_uniques
WINDOW seven_days AS (ORDER BY date ASC ROWS 6 PRECEDING);

## Or the number of uniques you saw yesterday that you didn't see today?

SELECT date, (#hll_union_agg(users) OVER two_days) - #users AS lost_uniques
FROM daily_uniques
WINDOW two_days AS (ORDER BY date ASC ROWS 1 PRECEDING);
```

## 例子 1: hll 加速短视频推荐

场景:

- 短视频、电商、社交等基于标签的动态推荐.

挑战:

- 标签多, 按标签权重比例每个标签取 N 条, 与数据库的交互次数多.
- 已读列表巨大, 过滤已读采用传统 not in 资源消耗大.
- 效率低下.

解决方案:

- 效率提升 1000 倍, 可能价值 1 个亿.
- 技术点: 数组、自定义 type、partial index 降低无效过滤、hash subplan 优化 not in、array agg, jsonb, hll 压缩存储已读视频 ID

### DEMO

1、视频资源池

1\.1、全国资源池

```
create unlogged table pool_global (   -- 全国资源池
  vid int8,  -- 视频ID
  tag int,   -- 标签
  score float4   -- 标签权重
);
```

写入数据: 1000 万条记录: 100 万个视频, 每个视频 10 个标签, 标签取值空间 1-100.

```
insert into pool_global select generate_series(1,1000000), random()*100, random()*10 from generate_series(1,10);
```

1\.2、省份资源池

```
create unlogged table pool_province (   -- 省份资源池
  pid int,    -- 省份ID
  vid int8,   -- 视频ID
  tag int,    -- 标签
  score float4   -- 标签权重
);
```

写入数据: 1000 万条记录: 100 万个视频, 每个视频 10 个标签, 标签取值空间 1-100.  
pid 取值空间 1-36

```
insert into pool_province select ceil(random()*36), generate_series(1000001,2000000), random()*100, random()*10 from generate_series(1,10);
```

1\.3、城市资源池

```
create unlogged table pool_city (     -- 城市资源池
  cid int,    -- 城市ID
  vid int8,   -- 视频ID
  tag int,    -- 标签
  score float4   -- 标签权重
);
```

写入数据: 1000 万条记录: 100 万个视频, 每个视频 10 个标签, 标签取值空间 1-100.  
cid 取值空间 1-10000

```
insert into pool_city select ceil(random()*10000), generate_series(2000001,3000000), random()*100, random()*10 from generate_series(1,10);
```

1\.4、partial index(分区索引), 避免过滤列表巨大带来巨大的无效扫描, 之前已经讲过  
[《重新发现 PostgreSQL 之美 - 23 彭祖的长寿秘诀》](https://github.com/digoal/blog/blob/master/202106/20210613_02.md)

```
do language plpgsql $$
declare
begin
  for i in 0..49 loop
    execute format('create index idx_pool_global_%s on pool_global (tag,score desc) include (vid) where abs(mod(hashint8(vid),50))=%s', i,i);
    execute format('create index idx_pool_province_%s on pool_province (pid,tag,score desc) include (vid) where abs(mod(hashint8(vid),50))=%s', i,i);
    execute format('create index idx_pool_city_%s on pool_city (cid,tag,score desc) include (vid) where abs(mod(hashint8(vid),50))=%s', i,i);
  end loop;
end;
$$;
```

2、用户标签类型

```
create type tag_score as (
  tag int,       -- 标签
  score float4,  -- 标签权重
  limits int     -- 用这个标签获取多少条VID
);
```

3、用户表

```
create unlogged table users (
  uid int8 primary key,    -- 用户ID
  pid int,  -- 省份ID
  cid int,  -- 城市ID
  tag_scores1 tag_score[],    -- 标签、权重、对应标签获取多少条. 也可以使用jsonb存储
  tag_scores2 tag_score[],    -- 标签、权重、对应标签获取多少条 limit = 0的放这个字段. 业务更新tag_scores根据两个字段的结果来计算. 主要是减少PG计算量.
  readlist jsonb     -- 已读VID, 和分区索引的分区数匹配, 用jsonb数组表示. jsonb[0]表示abs(mod(hashint8(vid),50))=0的vid数组
);
```

写入数据: 1000 万个用户, 每个用户 20 个标签(标签取值空间 1-100), limit 大于 0 的标签 5 个(和为 100). vid 已读列表 5 万条(1-300 万取值空间).  
pid 取值空间 1-36  
cid 取值空间 1-10000

```
insert into users
select generate_series(1,10000000), ceil(random()*36), ceil(random()*10000),
array(
  select row(ceil(random()*100), random()*10, 40)::tag_score
  union all
  select row(ceil(random()*100), random()*10, 20)::tag_score
  union all
  select row(ceil(random()*100), random()*10, 15)::tag_score
  union all
  select row(ceil(random()*100), random()*10, 15)::tag_score
  union all
  select row(ceil(random()*100), random()*10, 10)::tag_score
),
array (
  select row(ceil(random()*100), random()*10, 0)::tag_score from generate_series(1,15)
),
(
select jsonb_agg(x) as readlist from
  (
    select array (select x from
                     (select ceil(random()*3000000)::int8 x from generate_series(1,50000)) t
                   where mod(x,50)=i
                 ) x
    from generate_series(0,49) i
  ) t
) ;
```

4、如何更新用户的标签权重, 对应标签获取多少条?  
原则: 可以在程序中计算并且不会增加程序和数据库交互的, 放在程序内计算.  
取出 UID, 在应用程序中计算的到 tag_scores 结果, 存入数据库 users 表.

5、获取推荐视频 vids SQL:

```
select
  (
    select array_agg(vid) from
    (
      select vid from pool_global t1
      where t1.tag=t.tag
      and t1.vid not in (select jsonb_array_elements_text( readlist[0] )::int8 from users where uid=1)
      and abs(mod(hashint8(vid),50)) = 0
      order by t1.score desc
      limit ceil(t.limits*0.5)
    ) x   -- 全国池 50%
  ) as global_pool,
  (
    select array_agg(vid) from
    (
      select vid from pool_province t1
      where t1.tag=t.tag
      and t1.pid=(select pid from users where uid=1)
      and t1.vid not in (select jsonb_array_elements_text( readlist[0] )::int8 from users where uid=1)
      and abs(mod(hashint8(vid),50)) = 0
      order by t1.score desc
      limit ceil(t.limits*0.3)
    ) x   -- 省份池 30%
  ) as province_pool,
  (
    select array_agg(vid) from
    (
      select vid from pool_city t1
      where t1.tag=t.tag
      and t1.cid=(select cid from users where uid=1)
      and t1.vid not in (select jsonb_array_elements_text( readlist[0] )::int8 from users where uid=1)
      and abs(mod(hashint8(vid),50)) = 0
      order by t1.score desc
      limit ceil(t.limits*0.2)
    ) x    -- 城市池 20%
  ) as city_pool
from
(
  select (unnest(tag_scores1)).tag as tag, (unnest(tag_scores1)).limits as limits from
    users where uid=1
) t;
```

```
-[ RECORD 1 ]-+-------------------------------------------------------------------------------------------------------------------------------------------
global_pool   | {555234,30783,44877,893039,274638,811324,743142,233694,503619,977097,263781,350882,15000,961863,705252,823857,302978,919950,864090,633682}
province_pool | {1381822,1570117,1733796,1802258,1757745,1308796,1296608,1958019,1637076,1626698,1369964,1501167}
city_pool     |
-[ RECORD 2 ]-+-------------------------------------------------------------------------------------------------------------------------------------------
global_pool   | {41356,470712,453025,997172,997806,520315,512094,523652,714477,526433}
province_pool | {1246470,1582571,1154589,1213147,1144821,1498216}
city_pool     |
-[ RECORD 3 ]-+-------------------------------------------------------------------------------------------------------------------------------------------
global_pool   | {951192,518459,830710,34429,113691,362024,578173,574309}
province_pool | {1831028,1060594,1871276,1365273,1092971}
city_pool     | {2100388}
-[ RECORD 4 ]-+-------------------------------------------------------------------------------------------------------------------------------------------
global_pool   | {235601,950720,269682,452868,622511,590893,602110,104605}
province_pool | {1791516,1039665,1669258,1663575,1126127}
city_pool     |
-[ RECORD 5 ]-+-------------------------------------------------------------------------------------------------------------------------------------------
global_pool   | {738016,548948,600423,559686,483213}
province_pool | {1185880,1916542,1336231}
city_pool     |
```

```
                                                                            QUERY PLAN
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Subquery Scan on t  (cost=0.29..246.06 rows=10 width=96) (actual time=2.508..3.754 rows=5 loops=1)
   ->  Result  (cost=0.29..2.71 rows=10 width=8) (actual time=0.020..0.023 rows=5 loops=1)
         ->  ProjectSet  (cost=0.29..2.56 rows=10 width=32) (actual time=0.019..0.020 rows=5 loops=1)
               ->  Index Scan using users_pkey on users  (cost=0.29..2.50 rows=1 width=224) (actual time=0.017..0.018 rows=1 loops=1)
                     Index Cond: (uid = 1)
   SubPlan 2
     ->  Aggregate  (cost=7.08..7.09 rows=1 width=32) (actual time=0.261..0.261 rows=1 loops=5)
           ->  Limit  (cost=5.43..6.76 rows=25 width=12) (actual time=0.254..0.258 rows=10 loops=5)
                 ->  Index Only Scan using idx_pool_global_0 on pool_global t1  (cost=5.43..18.63 rows=248 width=12) (actual time=0.254..0.257 rows=10 loops=5)
                       Index Cond: (tag = t.tag)
                       Filter: (NOT (hashed SubPlan 1))
                       Heap Fetches: 0
                       SubPlan 1
                         ->  Result  (cost=0.29..4.76 rows=100 width=8) (actual time=0.851..1.074 rows=1001 loops=1)
                               ->  ProjectSet  (cost=0.29..3.01 rows=100 width=32) (actual time=0.850..0.938 rows=1001 loops=1)
                                     ->  Index Scan using users_pkey on users users_1  (cost=0.29..2.50 rows=1 width=18) (actual time=0.006..0.006 rows=1 loops=1)
                                           Index Cond: (uid = 1)
   SubPlan 5
     ->  Aggregate  (cost=8.15..8.16 rows=1 width=32) (actual time=0.247..0.247 rows=1 loops=5)
           ->  Limit  (cost=7.93..8.13 rows=1 width=12) (actual time=0.242..0.245 rows=6 loops=5)
                 InitPlan 3 (returns $3)
                   ->  Index Scan using users_pkey on users users_2  (cost=0.29..2.50 rows=1 width=4) (actual time=0.003..0.003 rows=1 loops=1)
                         Index Cond: (uid = 1)
                 ->  Index Only Scan using idx_pool_province_0 on pool_province t1_1  (cost=5.43..6.85 rows=7 width=12) (actual time=0.241..0.243 rows=6 loops=5)
                       Index Cond: ((pid = $3) AND (tag = t.tag))
                       Filter: (NOT (hashed SubPlan 4))
                       Heap Fetches: 0
                       SubPlan 4
                         ->  Result  (cost=0.29..4.76 rows=100 width=8) (actual time=0.813..1.027 rows=1001 loops=1)
                               ->  ProjectSet  (cost=0.29..3.01 rows=100 width=32) (actual time=0.812..0.896 rows=1001 loops=1)
                                     ->  Index Scan using users_pkey on users users_3  (cost=0.29..2.50 rows=1 width=18) (actual time=0.005..0.005 rows=1 loops=1)
                                           Index Cond: (uid = 1)
   SubPlan 8
     ->  Aggregate  (cost=9.07..9.08 rows=1 width=32) (actual time=0.236..0.237 rows=1 loops=5)
           ->  Limit  (cost=7.93..9.05 rows=1 width=12) (actual time=0.235..0.235 rows=0 loops=5)
                 InitPlan 6 (returns $7)
                   ->  Index Scan using users_pkey on users users_4  (cost=0.29..2.50 rows=1 width=4) (actual time=0.002..0.003 rows=1 loops=1)
                         Index Cond: (uid = 1)
                 ->  Index Only Scan using idx_pool_city_0 on pool_city t1_2  (cost=5.43..6.55 rows=1 width=12) (actual time=0.234..0.234 rows=0 loops=5)
                       Index Cond: ((cid = $7) AND (tag = t.tag))
                       Filter: (NOT (hashed SubPlan 7))
                       Heap Fetches: 0
                       SubPlan 7
                         ->  Result  (cost=0.29..4.76 rows=100 width=8) (actual time=0.793..1.011 rows=1001 loops=1)
                               ->  ProjectSet  (cost=0.29..3.01 rows=100 width=32) (actual time=0.792..0.876 rows=1001 loops=1)
                                     ->  Index Scan using users_pkey on users users_5  (cost=0.29..2.50 rows=1 width=18) (actual time=0.006..0.006 rows=1 loops=1)
                                           Index Cond: (uid = 1)
 Planning Time: 0.999 ms
 Execution Time: 3.825 ms
(49 rows)
```

6、压测

```
vi test.sql

\set uid random(1,10000000)
\set mod random(0,49)

select
  (
    select array_agg(vid) from
    (
      select vid from pool_global t1
      where t1.tag=t.tag
      and t1.vid not in (select jsonb_array_elements_text( readlist[:mod] )::int8  from users where uid=:uid)
      and abs(mod(hashint8(vid),50)) = :mod
      order by t1.score desc
      limit ceil(t.limits*0.5)
    ) x   -- 全国池 50%
  ) as global_pool,
  (
    select array_agg(vid) from
    (
      select vid from pool_province t1
      where t1.tag=t.tag
      and t1.pid=(select pid from users where uid=:uid)
      and t1.vid not in (select jsonb_array_elements_text( readlist[:mod] )::int8  from users where uid=:uid)
      and abs(mod(hashint8(vid),50)) = :mod
      order by t1.score desc
      limit ceil(t.limits*0.3)
    ) x   -- 省份池 30%
  ) as province_pool,
  (
    select array_agg(vid) from
    (
      select vid from pool_city t1
      where t1.tag=t.tag
      and t1.cid=(select cid from users where uid=:uid)
      and t1.vid not in (select jsonb_array_elements_text( readlist[:mod] )::int8  from users where uid=:uid)
      and abs(mod(hashint8(vid),50)) = :mod
      order by t1.score desc
      limit ceil(t.limits*0.2)
    ) x    -- 城市池 20%
  ) as city_pool
from
(
  select (unnest(tag_scores1)).tag as tag, (unnest(tag_scores1)).limits as limits from
    users where uid=:uid
) t;
```

```
pgbench -M prepared -n -r -P 1 -f ./test.sql -c 12 -j 12 -T 120

pgbench (PostgreSQL) 14.0
transaction type: ./test.sql
scaling factor: 1
query mode: prepared
number of clients: 12
number of threads: 12
duration: 120 s
number of transactions actually processed: 99824
latency average = 14.426 ms
latency stddev = 12.608 ms
initial connection time = 13.486 ms
tps = 831.235738 (without initial connection time)
```

2018 款 macbook pro i5 系列, 每秒可以推荐多少个视频 ID?

- 83100

### 压缩方法:

使用 roaringbitmap | hll 代替 array 存储已读列表.

## 例子 2: hll 加速滑动窗口分析

场景:

- 游戏、社交、电商场景.
- 流失用户、新增用户、UV 计算.
- 滑动计算, 任意窗口.

挑战:

- 数据量大、计算量大
- 传统方案需要记录明细, 才能支持滑动计算

解决方案:

- 采用 HLL 类型不需要存储明细, 支持滑动, 交、并、差计算.

### DEMO

#### 传统方案:

数据构造:

```
create unlogged table t (
  ts date,  -- 日期
  gid int,  -- 维度1 , 例如城市
  uid int8  -- 用户ID
);
```

写入 1 亿条记录, 跨度 15 天:

```
insert into t
select current_date+(random()*15)::int, random()*100, random()*800000000
from generate_series(1,100000000);


 public | t    | table | postgres | unlogged    | heap          | 4224 MB |
```

1、查询某一天的 UV

```
postgres=# select count(distinct uid) from t where ts=current_date;
  count
---------
 3326250
(1 row)

Time: 7268.339 ms (00:07.268)
```

2、查询某连续 7 天的 UV

```
postgres=# select count(distinct uid) from t where ts >= current_date and ts < current_date+7;
  count
----------
 42180699
(1 row)

Time: 25291.414 ms (00:25.291)
```

3、查询某一天相比前一天的新增用户数

```
postgres=# select count(distinct uid) from (select uid from t where ts=current_date+1 except select uid from t where ts=current_date) t;
  count
---------
 6610943
(1 row)

Time: 19969.067 ms (00:19.969)
```

4、查询某一天相比前一天的流失用户数

```
postgres=# select count(distinct uid) from (select uid from t where ts=current_date except select uid from t where ts=current_date+1) t;
  count
---------
 3298421
(1 row)

Time: 19434.652 ms (00:19.435)
```

5、查询某 7 天相比前 7 天的新增用户数

```
postgres=# select count(distinct uid) from (select uid from t where ts>=current_date+7 and ts<current_date+14 except select uid from t where ts>=current_date and ts<current_date+7) t;
  count
----------
 42945970
(1 row)

Time: 90321.223 ms (01:30.321)
```

6、查询某 7 天相比前 7 天的流失用户数

```
postgres=# select count(distinct uid) from (select uid from t where ts>=current_date and ts<current_date+7 except select uid from t where ts>=current_date+7 and ts<current_date+14) t;
  count
----------
 39791334
(1 row)

Time: 93443.917 ms (01:33.444)
```

7、查询某 14 天的 UV

```
postgres=# select count(distinct uid) from t where ts >= current_date and ts < current_date+14;
  count
----------
 85126669
(1 row)

Time: 48258.861 ms (00:48.259)
```

#### hll 解决方案

数据构造:

```
create unlogged table pt (
  ts date,  -- 日期
  gid int,  -- 维度1 , 例如城市
  uid hll  -- 用户IDs
);
```

每天每个 GID 一条. 不需要原始数据.

```
create extension hll;

insert into pt
select ts, gid, hll_add_agg(hll_hash_bigint(uid)) from
t group by ts,gid;

INSERT 0 1616
Time: 37344.032 ms (00:37.344)

 public | pt   | table | postgres | unlogged    | heap          | 2208 kB |
```

1、查询某一天的 UV

```
postgres=# select # hll_union_agg(uid) from pt where ts=current_date;
      ?column?
--------------------
 3422975.3781066863
(1 row)

Time: 1.530 ms
```

2、查询某连续 7 天的 UV

```
postgres=# select # hll_union_agg(uid) from pt where ts>=current_date and ts<current_date+7;
     ?column?
-------------------
 42551621.27768603
(1 row)

Time: 4.910 ms
```

3、查询某一天相比前一天的新增用户数

```
with
a as ( select hll_union_agg(uid) uid from pt where ts=current_date+1 ),
b as ( select hll_union_agg(uid) uid from pt where ts=current_date )
select (# hll_union(a.uid,b.uid)) - (# b.uid) from a,b;

     ?column?
-------------------
 6731386.388893194
(1 row)

Time: 2.330 ms
```

4、查询某一天相比前一天的流失用户数

```
with
a as ( select hll_union_agg(uid) uid from pt where ts=current_date+1 ),
b as ( select hll_union_agg(uid) uid from pt where ts=current_date )
select (# hll_union(a.uid,b.uid)) - (# a.uid) from a,b;

     ?column?
-------------------
 3290109.808110645
(1 row)

Time: 2.469 ms
```

5、查询某 7 天相比前 7 天的新增用户数

```
with
a as ( select hll_union_agg(uid) uid from pt where ts>=current_date+7 and ts<current_date+14 ),
b as ( select hll_union_agg(uid) uid from pt where ts>=current_date and ts<current_date+7 )
select (# hll_union(a.uid,b.uid)) - (# b.uid) from a,b;

      ?column?
--------------------
 42096480.700727895
(1 row)

Time: 8.762 ms
```

6、查询某 7 天相比前 7 天的流失用户数

```
with
a as ( select hll_union_agg(uid) uid from pt where ts>=current_date+7 and ts<current_date+14 ),
b as ( select hll_union_agg(uid) uid from pt where ts>=current_date and ts<current_date+7 )
select (# hll_union(a.uid,b.uid)) - (# a.uid) from a,b;

      ?column?
--------------------
 38055266.104507476
(1 row)

Time: 8.758 ms
```

7、查询某 14 天的 UV

```
select # hll_union_agg(uid) from pt where ts>=current_date and ts<current_date+14;

     ?column?
-------------------
 84648101.97841392
(1 row)

Time: 8.739 ms
```

### 总结

| 方法     | 存储空间 |
| -------- | -------- |
| 传统方法 | 4224 MB  |
| HLL      | 2 MB     |

| 测试 case                             | 传统方法 速度 | hll 速度 | hll 精度 |
| ------------------------------------- | ------------- | -------- | -------- |
| 1、查询某一天的 UV                    | 7268 ms       | 1 ms     | 97.17%   |
| 2、查询某连续 7 天的 UV               | 25291 ms      | 4 ms     | 99.13%   |
| 3、查询某一天相比前一天的新增用户数   | 19969 ms      | 2 ms     | 98.21%   |
| 4、查询某一天相比前一天的流失用户数   | 19434 ms      | 2 ms     | 100.25%  |
| 5、查询某 7 天相比前 7 天的新增用户数 | 90321 ms      | 8 ms     | 102.02%  |
| 6、查询某 7 天相比前 7 天的流失用户数 | 93443 ms      | 8 ms     | 104.56%  |
| 7、查询某 14 天的 UV                  | 48258 ms      | 8 ms     | 100.57%  |

关于精度:

https://hub.fastgit.org/citusdata/postgresql-hll

## 参考

- [《PostgreSQL HLL 近似计算算法要点》](https://github.com/digoal/blog/blob/master/202010/20201011_02.md)
- [《PostgreSQL 13 & 14 hashagg 性能增强(分组选择性精准度) - 使用 hll 评估 hash 字段的选择性, 而非使用记录数》](https://github.com/digoal/blog/blob/master/202008/20200803_05.md)
- [《PostgreSQL hll 在留存、UV 统计中的通用用法》](https://github.com/digoal/blog/blob/master/202006/20200610_01.md)
- [《PostgreSQL sharding : citus 系列 6 - count(distinct xx) 加速 (use 估值插件 hll|hyperloglog)》](https://github.com/digoal/blog/blob/master/201809/20180913_04.md)
- [《Greenplum 最佳实践 - 估值插件 hll 的使用(以及 hll 分式聚合函数优化)》](https://github.com/digoal/blog/blob/master/201608/20160825_02.md)
- [《PostgreSQL hll (HyperLogLog) extension for "State of The Art Cardinality Estimation Algorithm" - 3》](https://github.com/digoal/blog/blob/master/201302/20130228_01.md)
- [《PostgreSQL hll (HyperLogLog) extension for "State of The Art Cardinality Estimation Algorithm" - 2》](https://github.com/digoal/blog/blob/master/201302/20130227_01.md)
- [《PostgreSQL hll (HyperLogLog) extension for "State of The Art Cardinality Estimation Algorithm" - 1》](https://github.com/digoal/blog/blob/master/201302/20130226_01.md)
- [《重新发现 PostgreSQL 之美 - 14 bloom 布隆过滤器索引》](https://github.com/digoal/blog/blob/master/202106/20210605_07.md)
- [《PostgreSQL 14 preview - BRIN (典型 IoT 时序场景) 块级索引支持 bloom filter - 随机,大量 distinct value, 等值查询》](https://github.com/digoal/blog/blob/master/202103/20210326_02.md)
- [《PostgreSQL bloom 索引原理》](https://github.com/digoal/blog/blob/master/202011/20201128_04.md)
- [《UID 编码优化 - 用户画像前置规则 (bloom, 固定算法等)》](https://github.com/digoal/blog/blob/master/201911/20191130_01.md)
- [《PostgreSQL bloom filter index 扩展 for bigint》](https://github.com/digoal/blog/blob/master/201810/20181003_02.md)
- [《PostgreSQL 11 preview - bloom filter 误报率评估测试及如何降低误报 - 暨 bloom filter 应用于 HEAP 与 INDEX 的一致性检测》](https://github.com/digoal/blog/blob/master/201804/20180409_01.md)
- [《PostgreSQL 11 preview - BRIN 索引接口功能扩展(BLOOM FILTER、min max 分段)》](https://github.com/digoal/blog/blob/master/201803/20180323_05.md)
- [《PostgreSQL 9.6 黑科技 bloom 算法索引，一个索引支撑任意列组合查询》](https://github.com/digoal/blog/blob/master/201605/20160523_01.md)
- [《PostgreSQL x 分组, y 排序, 每组各取(N 动态)条 - 子查询+子查询聚合使用》](https://github.com/digoal/blog/blob/master/202007/20200710_02.md)
- [《重新发现 PostgreSQL 之美 - 23 彭祖的长寿秘诀》](https://github.com/digoal/blog/blob/master/202106/20210613_02.md)
- [《重新发现 PostgreSQL 之美 - 24 滑动窗口分析 2000x》](https://github.com/digoal/blog/blob/master/202106/20210614_01.md)
