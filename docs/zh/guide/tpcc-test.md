# PolarDB PG TPC-C 测试

在本节中，我们将学习如何对 PolarDB PG 进行 TPCC 测试，本次实践将基于单机本地存储来运行。

## TPC-C 测试

TPC-C 是一种衡量 OLTP 性能的基准测试。TPC-C 混合了五种不同类型和复杂程度的并发交易，这五种并发交易又包括了在线执行以及排队延迟执行。TPC-C 数据库由九种类型的表组成，以每分钟交易量（tmpC）来衡量具体性能。

TPC-C 的具体说明和排名可以通过官方网站 [TPC-C 官网](https://www.tpc.org/tpcc/) 进行查看。

## 前期准备

### 部署 PolarDB PG

在运行前默认已经通过文档 [PolarDB 编译部署：单机文件系统](./db-localfs.md) 部署好 PolarDB PG 的本地实例。

### 安装 Java 和 Ant

由于 TPC-C 测试工具 benchmarksql 需要通过 Ant 来编译，所以需要安装 Java 和 Ant。这里安装的 Java 版本为 8.0[^java-install]，Ant 版本为 1.9.7[^ant-install]。

::: tip
安装 Java 和 Ant 的时候需要注意修改环境变量。
:::

```bash
# 配置环境变量
vim /etc/profile

# 以下是本人机器上的配置，可以参考，路径需要根据自己机器进行调整
JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.332.b09-1.el7_9.x86_64
JRE_HOME=$JAVA_HOME/jre
CLASSPATH=.:$JRE_HOME/lib/rt.jar:${JAVA_HOME}/lib/dt.jar:${JAVA_HOME}/lib/tools.jar
PATH=$PATH:$JAVA_HOME/bin:$JRE_HOME/bin
export JAVA_HOME JRE_HOME CLASSPATH PATH
#ant environment
export ANT_HOME=/home/postgres/apache-ant-1.9.16
export PATH=$PATH:$ANT_HOME/bin

# 生效
source /etc/profile
```

验证 Java 和 Ant 安装成功。

```bash
# java
$ java -version
openjdk version "1.8.0_332"
OpenJDK Runtime Environment (build 1.8.0_332-b09)
OpenJDK 64-Bit Server VM (build 25.332-b09, mixed mode)

# ant
$ ant -version
Apache Ant(TM) version 1.9.16 compiled on July 10 2021
```

### 安装 benchmarksql

我们将通过 benchmarksql 工具来进行 TPC-C 测试。

::: tip
下面链接中的 benchmarksql 采用的是 5.1 版本。相较于 5.0 版本，5.1 版本可以用 Procedures 性能表现较好。推荐使用 5.1 版本。
:::

```bash
# 下载 benchmarksql
git clone https://github.com/petergeoghegan/benchmarksql

# 编译
cd benchmarksql
ant
```

## 进行 TPC-C 测试

TPC-C 测试主要分为四个步骤：载入数据、预热数据、进行测试、结果分析。下面将分别进行说明。

通过如下命令进入到指定目录：

```bash
cd run
```

### TPC-C 配置文件

在使用 benchmarksql 运行 TPC-C 测试的时候，需要指定配置参数，配置包括要连接的数据库类型（Oracle、PG）、IP、端口等。如下代码块说明了具体的配置字段名以及含义：

::: tip
后续的载入数据、预热数据、进行测试都可以采用该配置文件，后续该配置文件的名称都为 `PolarDB_PG_Run.conf`。
:::

```bash
# 要连接的数据库类型，下面都以 postgres 为例
db=postgres
# 驱动程序
driver=org.postgresql.Driver
# 连接的 IP 为 localhost， 端口为 5432， 数据库为 tpcc
conn=jdbc:postgresql://localhost:5432/tpcc
# 数据库用户名
user=postgres
# 数据库密码
password=postgres

# 仓库数量，相当于测试数据量
warehouses=10
# 装载数据的进程数量，可根据机器核数动态调整
loadWorkers=20
# 运行测试时的并发客户端数量，一般设置为 CPU 线程总数的 2～6 倍。注意不能超过数据库的最大连接数。 最大连接数可以通过 show max_connections; 查看
terminals=20

# 每个终端运行的事务数，如果该值非 0，则运行总事务数为 runTxnsPerTerminal * terminals。注意，runTxnsPerTerminal 不能和 runMins 同时非 0
runTxnsPerTerminal=0

# 运行时间数，单位为分钟。注意，runTxnsPerTerminal 不能和 runMins 同时非 0
runMins=1

# 每分钟执行的最大事务数，设置为0，则表示不加限制 ( Number of total transactions per minute )
limitTxnsPerMin=0


# 终端和仓库的绑定模式，设置为 true 时说明每个终端有一个固定仓库。 一般采用默认值 true
terminalWarehouseFixed=true
# 是否采用存储过程，为 true 则说明使用
useStoredProcedures=false
```

该配置的部分中文解释说明参考 [benchmarksql 使用指南](https://cloud.tencent.com/developer/article/1893777)。

### 载入数据

脚本 `runDatabaseBuild.sh` 用来装载数据。在装载数据前，需要通过 `psql` 命令 `create database tpcc` 创建 tpcc 数据库。

执行如下 bash 命令，执行装载数据：

```bash
./runDatabaseBuild.sh PolarDB_PG_Run.conf
```

执行成功的结果如下所示：

```bash
-- ----
-- Extra commands to run after the tables are created, loaded,
-- indexes built and extra\'s created.
-- PostgreSQL version.
-- ----
vacuum analyze;
```

### 预热数据

脚本 `runBenchmark.sh` 用来执行 TPC-C 测试。通常，在正式压测前会进行一次数据预热。

数据预热的命令如下：

```bash
./runBenchmark.sh PolarDB_PG_Run.conf
```

#### 可能会出现的错误以及解决方法

错误日志：

```bash
ERROR  jTPCC : Term-00, This session ended with errors!
```

解决方法：

该错误说明会话断连，需要通过数据库的日志来定位问题，打印数据库的错误日志命令如下：

```bash
# 文件名需要替换成目录中带 error 的文件名
cat /home/postgres/tmp_master_dir_polardb_pg_1100_bld/pg_log/postgresql-2022-06-29_101344_error.log
```

### 进行测试

数据预热完，就可以进行正式测试。正式测试的命令如下：

```bash
./runBenchmark.sh PolarDB_PG_Run.conf
```

### 结果分析

压测结束后，结果如下所示：

```bash

11:49:15,896 [Thread-9] INFO   jTPCC : Term-00, Measured tpmC (NewOrders) = 71449.03
11:49:15,896 [Thread-9] INFO   jTPCC : Term-00, Measured tpmTOTAL = 164116.88
11:49:15,896 [Thread-9] INFO   jTPCC : Term-00, Session Start     = 2022-06-29 11:48:15
11:49:15,896 [Thread-9] INFO   jTPCC : Term-00, Session End       = 2022-06-29 11:49:15
11:49:15,896 [Thread-9] INFO   jTPCC : Term-00, Transaction Count = 164187
```

- Running Average tpmTOTAL / Measured tpmTOTAL：每分钟平均执行事务数（所有事务）
- Memory Usage：客户端内存使用情况
- Measured tpmC (NewOrders) ：每分钟执行的事务数（只统计 NewOrders 事务）
- Transaction Count：执行的交易总数量

该结果的部分解释参考 [benchmarksql 使用指南](https://cloud.tencent.com/developer/article/1893777)。

## 如何在不同场景下测试

本文档主要提供了 PolarDB PG 通用的 TPC-C 测试方式，如果需要进行不同场景下的测试，比如：三节点、PFS 文件系统、Ceph 共享存储等。需要通过对应文档创建数据库实例，然后修改数据库的配置。修改配置方式有两个：

1. 可以通过 psql 命令 `alter system set ...;` 和 `select pg_reload_conf();` 来修改配置。
2. 二是修改数据库配置文件来实现修改配置，命令如下：

   ```bash
   # 修改配置文件
   vim /home/postgres/tmp_master_dir_polardb_pg_1100_bld/postgresql.auto.conf
   # 重启数据库
   /home/postgres/tmp_basedir_polardb_pg_1100_bld/bin/pg_ctl -D /home/postgres/tmp_master_dir_polardb_pg_1100_bld restart
   ```

后续，再根据该文档进行 TPC-C 测试。

[^java-install]: [Java 8 安装流程](https://blog.csdn.net/Sanayeah/article/details/118721863)
[^ant-install]: [Ant 安装流程](https://blog.csdn.net/downing114/article/details/51470743)。
