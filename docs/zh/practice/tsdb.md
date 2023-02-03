---
author: digoal
date: 2023/02/03
minute: 25
---

# 时序数据高速写入、压缩、实时聚合计算、自动老化等实践

<ArticleInfo :frontmatter=$frontmatter></ArticleInfo>

## 背景

PolarDB 的云原生存算分离架构, 具备低廉的数据存储、高效扩展弹性、高速多机并行计算能力、高速数据搜索和处理; PolarDB 与计算算法结合, 将实现双剑合璧, 推动业务数据的价值产出, 将数据变成生产力.

本文将介绍 PolarDB 开源版 使用 TimescaleDB 实现时序数据高速写入、压缩、实时聚合计算、自动老化等

测试环境为 macOS+docker, PolarDB 部署请参考下文:

- [《如何用 PolarDB 证明巴菲特的投资理念 - 包括 PolarDB 简单部署》](https://github.com/digoal/blog/blob/master/202209/20220908_02.md)

## Timescale DB 部署

目前 PolarDB 开源版本兼容 PG 11, 所以只能使用 TimescaleDB 1.7.x 的版本, 未来 PolarDB 升级到 14 后, 可以使用 TimescaleDB 2.x 的版本.

```
cd ~

git clone -b 1.7.x --depth 1 https://github.com/timescale/timescaledb
cd timescaledb

./bootstrap  -DREGRESS_CHECKS=OFF

cd build && make

sudo make install
```

修改 polardb 配置

```
vi ~/tmp_master_dir_polardb_pg_1100_bld/postgresql.conf

vi ~/tmp_replica_dir_polardb_pg_1100_bld1/postgresql.conf
vi ~/tmp_replica_dir_polardb_pg_1100_bld2/postgresql.conf

shared_preload_libraries = 'timescaledb,......'
```

更多的参数配置和优化建议参考:

- https://github.com/timescale/timescaledb-tune

## 使用 TimescaleDB

```
postgres=# create extension timescaledb ;
WARNING:
WELCOME TO
 _____ _                               _     ____________
|_   _(_)                             | |    |  _  \ ___ \
  | |  _ _ __ ___   ___  ___  ___ __ _| | ___| | | | |_/ /
  | | | |  _ ` _ \ / _ \/ __|/ __/ _` | |/ _ \ | | | ___ \
  | | | | | | | | |  __/\__ \ (_| (_| | |  __/ |/ /| |_/ /
  |_| |_|_| |_| |_|\___||___/\___\__,_|_|\___|___/ \____/
               Running version 1.7.4
For more information on TimescaleDB, please visit the following links:

 1. Getting started: https://docs.timescale.com/getting-started
 2. API reference documentation: https://docs.timescale.com/api
 3. How TimescaleDB is designed: https://docs.timescale.com/introduction/architecture

Note: TimescaleDB collects anonymous reports to better understand and assist our users.
For more information and how to disable, please see our docs https://docs.timescaledb.com/using-timescaledb/telemetry.

CREATE EXTENSION
```

```
postgres=# \dx
                                      List of installed extensions
    Name     | Version |   Schema   |                            Description
-------------+---------+------------+-------------------------------------------------------------------
 plpgsql     | 1.0     | pg_catalog | PL/pgSQL procedural language
 postgis     | 3.3.2   | public     | PostGIS geometry and geography spatial types and functions
 timescaledb | 1.7.5   | public     | Enables scalable inserts and complex queries for time-series data
(3 rows)
```

创建普通时序表

```
-- We start by creating a regular SQL table

CREATE TABLE conditions (
  time        TIMESTAMPTZ       NOT NULL,
  location    TEXT              NOT NULL,
  temperature DOUBLE PRECISION  NULL,
  humidity    DOUBLE PRECISION  NULL
);
```

将普通表转化为 timescale 时序表

```
-- This creates a hypertable that is partitioned by time
--   using the values in the `time` column.

SELECT create_hypertable('conditions', 'time');
```

写入测试数据

```
INSERT INTO conditions(time, location, temperature, humidity)
  VALUES (NOW(), 'office', 70.0, 50.0);
```

查询时序表基表内容

```
SELECT * FROM conditions ORDER BY time DESC LIMIT 100;
```

基表自动分片存储

```
postgres=# \d+ conditions
                                            Table "public.conditions"
   Column    |           Type           | Collation | Nullable | Default | Storage  | Stats target | Description
-------------+--------------------------+-----------+----------+---------+----------+--------------+-------------
 time        | timestamp with time zone |           | not null |         | plain    |              |
 location    | text                     |           | not null |         | extended |              |
 temperature | double precision         |           |          |         | plain    |              |
 humidity    | double precision         |           |          |         | plain    |              |
Indexes:
    "conditions_time_idx" btree ("time" DESC)
Triggers:
    ts_insert_blocker BEFORE INSERT ON conditions FOR EACH ROW EXECUTE PROCEDURE _timescaledb_internal.insert_blocker()
Child tables: _timescaledb_internal._hyper_1_1_chunk
```

```
INSERT INTO conditions(time, location, temperature, humidity)
select now()+(id||' second')::interval,
  md5((random()*1000)::int::text),
  random()*100, random()*100
  from generate_series(1,1000000) id;


postgres=# \d+ conditions
                                            Table "public.conditions"
   Column    |           Type           | Collation | Nullable | Default | Storage  | Stats target | Description
-------------+--------------------------+-----------+----------+---------+----------+--------------+-------------
 time        | timestamp with time zone |           | not null |         | plain    |              |
 location    | text                     |           | not null |         | extended |              |
 temperature | double precision         |           |          |         | plain    |              |
 humidity    | double precision         |           |          |         | plain    |              |
Indexes:
    "conditions_time_idx" btree ("time" DESC)
Triggers:
    ts_insert_blocker BEFORE INSERT ON conditions FOR EACH ROW EXECUTE PROCEDURE _timescaledb_internal.insert_blocker()
Child tables: _timescaledb_internal._hyper_1_1_chunk,
              _timescaledb_internal._hyper_1_2_chunk


postgres=# SELECT * FROM conditions ORDER BY time DESC LIMIT 100;
             time              |             location             |    temperature    |     humidity
-------------------------------+----------------------------------+-------------------+-------------------
 2023-01-16 16:27:12.442233+00 | 70efdf2ec9b086079795c442636b55fb | 0.917297508567572 |  45.0286225881428
 2023-01-16 16:27:11.442233+00 | b056eb1587586b71e2da9acfe4fbd19e |  59.0947337448597 |  49.3321735877544
 2023-01-16 16:27:10.442233+00 | 28dd2c7955ce926456240b2ff0100bde |  26.5667649917305 |  88.5223139543086
 2023-01-16 16:27:09.442233+00 | 1ecfb463472ec9115b10c292ef8bc986 |  12.9402264486998 |   23.304360313341
 2023-01-16 16:27:08.442233+00 | 82161242827b703e6acf9c726942a1e4 |  48.1451884843409 |  97.9283190798014
 2023-01-16 16:27:07.442233+00 | 812b4ba287f5ee0bc9d43bbf5bbe87fb |  76.0097410064191 |  20.2729247976094
 2023-01-16 16:27:06.442233+00 | d645920e395fedad7bbbed0eca3fe2e0 |  97.6623016409576 |  22.9934238363057
 2023-01-16 16:27:05.442233+00 | 0d0fd7c6e093f7b804fa0150b875b868 |  7.43439155630767 |  96.3830435648561
 2023-01-16 16:27:04.442233+00 | 6e2713a6efee97bacb63e52c54f0ada0 |  30.4179009050131 |  36.7151976097375
 2023-01-16 16:27:03.442233+00 | fb7b9ffa5462084c5f4e7e85a093e6d7 |  22.1182454843074 |  23.0733227450401
 2023-01-16 16:27:02.442233+00 | d1f255a373a3cef72e03aa9d980c7eca |  95.6964490003884 |  43.6015542596579
 2023-01-16 16:27:01.442233+00 | 89f0fd5c927d466d6ec9a21b9ac34ffa |  60.8098595868796 |  26.7892859410495
 ...
```

分片字段自动创建索引

```
postgres=# explain SELECT * FROM conditions ORDER BY time DESC LIMIT 100;
                                                            QUERY PLAN
-----------------------------------------------------------------------------------------------------------------------------------
 Limit  (cost=0.42..7.77 rows=100 width=56)
   ->  Custom Scan (ChunkAppend) on conditions  (cost=0.42..32778.88 rows=446297 width=56)
         Order: conditions."time" DESC
         ->  Index Scan using _hyper_1_2_chunk_conditions_time_idx on _hyper_1_2_chunk  (cost=0.42..32778.88 rows=446297 width=56)
         ->  Index Scan using _hyper_1_1_chunk_conditions_time_idx on _hyper_1_1_chunk  (cost=0.42..48162.05 rows=656108 width=56)
(5 rows)
```

## 实时聚合基表数据例子

https://legacy-docs.timescale.com/v1.7/using-timescaledb/continuous-aggregates

创建基表

```
CREATE TABLE conditions (
      time TIMESTAMPTZ NOT NULL,
      device INTEGER NOT NULL,
      temperature FLOAT NOT NULL,
      PRIMARY KEY(time, device)
);
SELECT create_hypertable('conditions', 'time');
```

创建自动聚合视图

```
CREATE VIEW conditions_summary_hourly
WITH (timescaledb.continuous) AS
SELECT device,
       time_bucket(INTERVAL '1 hour', time) AS bucket,
       AVG(temperature),
       MAX(temperature),
       MIN(temperature)
FROM conditions
GROUP BY device, bucket;


CREATE VIEW conditions_summary_daily
WITH (timescaledb.continuous) AS
SELECT device,
       time_bucket(INTERVAL '1 day', time) AS bucket,
       AVG(temperature),
       MAX(temperature),
       MIN(temperature)
FROM conditions
GROUP BY device, bucket;
```

写入测试数据

```
INSERT INTO conditions(time, device, temperature)
select now()+(id||' second')::interval,
  (random()*100)::int,
  random()*100
  from generate_series(1,1000000) id;
```

查询聚合视图

```
SELECT * FROM conditions_summary_daily
WHERE device = 5
  AND bucket >= '2023-01-01' AND bucket < '2023-01-10';


 device |         bucket         |       avg        |       max        |        min
--------+------------------------+------------------+------------------+--------------------
      5 | 2023-01-05 00:00:00+00 | 52.8728757359047 | 99.9651623424143 |  0.113607617095113
      5 | 2023-01-06 00:00:00+00 | 50.9738177677259 | 99.9353400431573 | 0.0549898017197847
      5 | 2023-01-07 00:00:00+00 | 49.2079831483183 | 99.9868880026042 | 0.0576195307075977
      5 | 2023-01-08 00:00:00+00 | 48.3715454505876 | 99.9165495857596 |  0.242615444585681
      5 | 2023-01-09 00:00:00+00 | 49.0718302013499 | 99.7824223246425 | 0.0885920133441687
(5 rows)
```

```
SELECT * FROM conditions_summary_daily
WHERE max - min > 1800
  AND bucket >= '2023-01-01' AND bucket < '2023-04-01'
ORDER BY bucket DESC, device DESC LIMIT 20;
```

修改聚合视图的自动刷新延迟、保留时间窗口、手工基于时间窗口维护保留数据

```
ALTER VIEW conditions_summary_hourly SET (
  timescaledb.refresh_lag = '1 hour'
);


ALTER VIEW conditions_summary_daily SET (
  timescaledb.ignore_invalidation_older_than = '30 days'
);


SELECT drop_chunks(INTERVAL '30 days', 'conditions_summary_daily');
```

修改自动聚合视图风格, 是否只查询已聚合内容、或包含未聚合内容(需实时查询基表进行计算):

```
ALTER VIEW conditions_summary_hourly SET (
    timescaledb.materialized_only = false
);

ALTER VIEW conditions_summary_daily SET (
    timescaledb.materialized_only = false
);
```

## 参考

https://legacy-docs.timescale.com/v1.7/main

https://legacy-docs.timescale.com/v1.7/using-timescaledb/continuous-aggregates

https://github.com/timescale/timescaledb-tune
