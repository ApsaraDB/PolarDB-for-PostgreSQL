# 使用pgbench进行基准测试

## 安装pgbench
进入`contrib/pgbench`目录，获取pgbench源代码。运行`make && make install`命令安装pgbench。

## 初始化pgbench
* 首先，运行pgbench需要一个pgbench数据库：

```bash
$~/pghome/bin/psql -p 5432 -d postgres
psql (11.2)
Type "help" for help.

postgres=# drop database pgbench;
DROP DATABASE
postgres=# create database pgbench;
CREATE DATABASE
```
* 初始化基准测试环境，使用`-s`选项设置比例因子，使用`--help`选项获取更多信息：

```bash
$~/pghome/bin/pgbench -i --unlogged-tables -s 32 -p 5432 -d pgbench
dropping old tables...
NOTICE:  table "pgbench_accounts" does not exist, skipping
NOTICE:  table "pgbench_branches" does not exist, skipping
NOTICE:  table "pgbench_history" does not exist, skipping
NOTICE:  table "pgbench_tellers" does not exist, skipping
creating tables...
generating data...
100000 of 3200000 tuples (3%) done (elapsed 0.09 s, remaining 2.73 s)
200000 of 3200000 tuples (6%) done (elapsed 0.19 s, remaining 2.80 s)
300000 of 3200000 tuples (9%) done (elapsed 0.30 s, remaining 2.85 s)
400000 of 3200000 tuples (12%) done (elapsed 0.39 s, remaining 2.74 s)
500000 of 3200000 tuples (15%) done (elapsed 0.50 s, remaining 2.69 s)
600000 of 3200000 tuples (18%) done (elapsed 0.59 s, remaining 2.57 s)
700000 of 3200000 tuples (21%) done (elapsed 0.70 s, remaining 2.50 s)
800000 of 3200000 tuples (25%) done (elapsed 0.80 s, remaining 2.39 s)
900000 of 3200000 tuples (28%) done (elapsed 0.90 s, remaining 2.30 s)
1000000 of 3200000 tuples (31%) done (elapsed 1.00 s, remaining 2.19 s)
1100000 of 3200000 tuples (34%) done (elapsed 1.10 s, remaining 2.11 s)
1200000 of 3200000 tuples (37%) done (elapsed 1.20 s, remaining 2.01 s)
1300000 of 3200000 tuples (40%) done (elapsed 1.31 s, remaining 1.92 s)
1400000 of 3200000 tuples (43%) done (elapsed 1.41 s, remaining 1.81 s)
1500000 of 3200000 tuples (46%) done (elapsed 1.51 s, remaining 1.71 s)
1600000 of 3200000 tuples (50%) done (elapsed 1.62 s, remaining 1.62 s)
1700000 of 3200000 tuples (53%) done (elapsed 1.73 s, remaining 1.52 s)
1800000 of 3200000 tuples (56%) done (elapsed 1.83 s, remaining 1.42 s)
1900000 of 3200000 tuples (59%) done (elapsed 1.93 s, remaining 1.32 s)
2000000 of 3200000 tuples (62%) done (elapsed 2.03 s, remaining 1.22 s)
2100000 of 3200000 tuples (65%) done (elapsed 2.13 s, remaining 1.12 s)
2200000 of 3200000 tuples (68%) done (elapsed 2.24 s, remaining 1.02 s)
2300000 of 3200000 tuples (71%) done (elapsed 2.34 s, remaining 0.92 s)
2400000 of 3200000 tuples (75%) done (elapsed 2.45 s, remaining 0.82 s)
2500000 of 3200000 tuples (78%) done (elapsed 2.55 s, remaining 0.71 s)
2600000 of 3200000 tuples (81%) done (elapsed 2.66 s, remaining 0.61 s)
2700000 of 3200000 tuples (84%) done (elapsed 2.76 s, remaining 0.51 s)
2800000 of 3200000 tuples (87%) done (elapsed 2.85 s, remaining 0.41 s)
2900000 of 3200000 tuples (90%) done (elapsed 2.96 s, remaining 0.31 s)
3000000 of 3200000 tuples (93%) done (elapsed 3.06 s, remaining 0.20 s)
3100000 of 3200000 tuples (96%) done (elapsed 3.17 s, remaining 0.10 s)
3200000 of 3200000 tuples (100%) done (elapsed 3.27 s, remaining 0.00 s)
vacuuming...
creating primary keys...
done.
```
## 运行基准测试
使用一些选项运行基准测试，使用`--help`选项获取更多信息：
```bash
$~/pghome/bin/pgbench -M prepared -r -c 16 -j 4 -T 30 -p 5432 -d pgbench -l
... ...
client 2 sending P0_10
client 2 receiving
client 2 receiving
client 14 receiving
transaction type: <builtin: TPC-B (sort of)>
scaling factor: 32
query mode: prepared
number of clients: 16
number of threads: 4
duration: 30 s
number of transactions actually processed: 49126
latency average = 9.772 ms
tps = 1637.313156 (including connections establishing)
tps = 1637.438330 (excluding connections establishing)
statement latencies in milliseconds:
         1.128  \set aid random(1, 100000 * :scale)
         0.068  \set bid random(1, 1 * :scale)
         0.040  \set tid random(1, 10 * :scale)
         0.041  \set delta random(-5000, 5000)
         0.104  BEGIN;
         3.815  UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;
         0.590  SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
         1.188  UPDATE pgbench_tellers SET tbalance = tbalance + :delta WHERE tid = :tid;
         1.440  UPDATE pgbench_branches SET bbalance = bbalance + :delta WHERE bid = :bid;
         0.327  INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (:tid, :bid, :aid, :delta, CURRENT_TIMESTAMP);
         0.481  END;
```

___

© 阿里巴巴集团控股有限公司 版权所有
