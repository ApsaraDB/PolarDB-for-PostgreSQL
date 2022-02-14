![PolarDB图标](https://github.com/alibaba/PolarDB-for-PostgreSQL/blob/distributed/polardb.png)

## 什么是PolarDB PostgreSQL

PolarDB PostgreSQL（下文简称为PolarDB）是阿里云提供的一款基于PostgreSQL的开源数据库系统。PolarDB将PostgreSQL扩展为Shared-Nothing分布式数据库，支持**全局数据一致性**和**跨数据库节点事务管理，保证事务ACID，即保证事务的原子性（Atomicity）、一致性（Consistency）、隔离性（Isolation）和持久性（Durability）**。PolarDB还提供**分布式SQL处理能力**，以及基于Paxos的复制能力实现**数据冗余**和**高可用性**。PolarDB旨在为PostgreSQL在高性能、高扩展性、高可用性和极致弹性等维度增加更多功能和价值。同时，PolarDB尽可能地保留了单节点PostgreSQL数据库的SQL兼容性。

PolarDB主要通过两个方面来提升PostgreSQL数据库的功能和特性：扩展组件和PostgreSQL补丁。扩展组件包括在PostgreSQL内核之外实现的组件，如分布式事务管理、全局或分布式时间服务、分布式SQL处理、附加元数据、内部函数、以及用于管理数据库集群和进行容错或恢复的工具。PolarDB在PostgreSQL的基础上，扩展了PostgreSQL数据库的功能，使PostgreSQL数据库**易于升级**、**轻松迁移**和**快速适应**。PolarDB提供针对PostgreSQL内核所需的补丁，例如针对不同隔离级别的分布式多版本并发控制（MVCC）补丁。补丁仅涉及部分功能和代码。因此，PolarDB数据库可以轻松升级到新版本的PostgreSQL，并保持与PostgreSQL的100%兼容。

- [PolarDB快速入门](#PolarDB快速入门)
- [产品架构和版本规划](#产品架构和版本规划)
- [文档](#文档)
- [贡献](#贡献)
- [软件许可说明](#软件许可说明)
- [致谢](#致谢)
- [帮助与分享](#帮助与分享)

## PolarDB快速入门

您可以通过三种方法来快速试用PolarDB：阿里云服务、使用Docker镜像部署和使用源代码部署。

### 阿里云服务
阿里云云原生关系型数据库PolarDB PostgreSQL引擎：[官网地址](https://www.aliyun.com/product/polardb)。

### 使用Docker镜像部署
本节介绍如何创建一个PolarDB-for-Postgresql镜像以及如何使用该镜像快速部署PolarDB数据库。

* 创建一个PolarDB-for-Postgresql镜像。

```bash
docker build -t polardb-for-postgresql -f ./docker/Dockerfile .
```

* 在10001端口上运行PolarDB-for-Postgresql镜像。

```bash
docker run --name polardb -p 10001:10001 -d polardb-for-postgresql:latest
```

* 在本地通过psql命令访问PolarDB-for-Postgresql镜像。

```bash
psql -d postgres -U postgres -h localhost -p 10001
```

* 如果您本地没有安装psql，可以使用以下命令登录到容器，再使用psql。

```bash
docker exec -it polardb /bin/bash
```

### 准备工作

* 下载源代码。下载地址：<https://github.com/alibaba/PolarDB-for-PostgreSQL>。

* 安装依赖包（以CentOS为例）。

```bash
sudo yum install bison flex libzstd-devel libzstd zstd cmake openssl-devel protobuf-devel readline-devel libxml2-devel libxslt-devel zlib-devel bzip2-devel lz4-devel snappy-devel python-devel
```
* 创建授权密钥用以快速访问数据库。

使用ssh-copy-id命令创建一个授权密钥。使用授权密钥就可以通过pgxc_ctl访问数据库，而无需每次登录都要使用数据库账号对应的密码。

```bash
ssh-copy-id username@IP
```

* 设置环境变量。

```bash
vi ~/.bashrc
export PATH="$HOME/polardb/polardbhome/bin:$PATH"
export LD_LIBRARY_PATH="$HOME/polardb/polardbhome/lib:$LD_LIBRARY_PATH"
source ~/.bashrc
```

### 快速部署（onekey.sh）
onekey.sh脚本使用默认配置来编译PolarDB，部署二进制包，并创建一个由一个主节点和两个只读节点组成的PolarDB集群。运行此脚本前，请先根据“准备工作”章节的内容，检查环境变量、依赖包和授权密钥是否正确配置。

* 运行onekey.sh脚本

```bash
./onekey.sh all
```

* 检查节点（包括一个主节点和两个只读节点）的进程运行状态及其对应副本的角色和状态。

```bash
ps -ef|grep polardb
psql -p 10001 -d postgres -c "select * from pg_stat_replication;"
psql -p 10001 -d postgres -c "select * from polar_dma_cluster_status;"
```


### 使用源代码部署

您可以使用pgxc_ctl来管理集群，如配置集群、更新集群配置、初始化集群、启动或停止节点和容灾切换。pgxc_ctl是基于Postgres-XC/Postgres-XL（PG-XC/XL）开源项目的集群管理工具。有关pgxc_ctl的使用方法详情，请参见[部署](doc-CN/deployment.md)。

* 创建并安装二进制包。

您可以使用build.sh脚本来创建。如果创建失败，请参考[部署](doc-CN/deployment.md)排查失败原因。

```bash
./build.sh
```

* 生成默认配置文件。

```bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf prepare standalone
```

* 部署二进制文件。

```bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf deploy all
```

* 清除安装包残留并初始化集群。

```bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf clean all
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf init all
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf monitor all
```

* 安装集群管理依赖包。

```bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf deploy cm
```

* 启动集群或节点。

```bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf start all
```

* 停止集群或节点。

```bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf stop all
```

* 切换故障节点。

以下示例展示如何在polardb_paxos.conf文件中配置datanode_1为故障切换节点。

```bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf failover datanode datanode_1
```

* 检查集群健康状况。

检查集群状态。

```bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf healthcheck all
```

* 其他命令示例。

```bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf kill all
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf log var datanodeNames
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf show configuration all
```

* 检查和测试。

```bash
ps -ef | grep postgres
psql -p 10001 -d postgres -c "create table t1(a int primary key, b int);"
createdb test -p 10001
psql -p 10001 -d test -c "select version();"
```

具体部署指南，请参见[部署](doc-CN/deployment.md)。

点击[这里](doc-CN/regress.md)查看回归测试和其他测试详情。点击[这里](doc-CN/benchmark.md)查看基准测试示例。

## 产品架构和版本规划

PolarDB采用Shared-Nothing架构。每个节点存储数据并执行查询。节点之间通过消息交互进行协作。该架构支持通过向集群添加更多节点的方式来扩展数据库。

PolarDB对主键进行哈希运算，根据运算结果拆分表。您可以手动配置分片数量。分片存储在PolarDB集群的节点中。当一个查询涉及多个节点中的分片时，分布式事务和事务协调器可保证跨节点事务ACID。

每个分片可以有三个副本，每个副本存储在不同的节点上，即同一个分片的数据同时存储在三个节点上。为节约成本，您也可以部署其中两个节点存储完整数据。另一个节点仅存储预写日志 (WAL)。该节点参与主节点选举，但不能被选举为主节点。

更多详情，请参见[架构设计](doc-CN/arch.md)。

## 文档

* [架构设计](doc-CN/arch.md)
* [版本规划](doc-CN/roadmap.md)
* PolarDB PostgreSQL 1.0版本的功能及其设计
   * [基于Paxos复制](doc-CN/ha_paxos.md)
   * [集群管理](doc-CN/deployment.md)
   * [基于时间戳的MVCC](doc-CN/cts.md)
   * [并行重做](doc-CN/parallel_redo.md)
   * [远程恢复](doc-CN/no_fpw.md)


## 贡献

PolarDB基于开源项目，并扩展了开源PostgreSQL。我们欢迎并感谢您贡献代码。了解如何开始编辑代码和提交Pull Request (PR)，请参见[贡献](doc-CN/contributing.md)。

## 软件许可说明

PolarDB代码的发布基于Apache 2.0版本和PostgreSQL代码的的软件许可。

您可以在每个文件顶部的注释中找到相关许可证。

有关许可说明详情，请参见[许可证](https://github.com/ApsaraDB/PolarDB-for-PostgreSQL/blob/distributed/LICENSE)和[通告](https://github.com/ApsaraDB/PolarDB-for-PostgreSQL/blob/distributed/NOTICE)。

## 致谢

部分代码和设计思路参考了其他开源项目，例如PG-XC/XL (pgxc_ctl)、TBase（部分基于时间戳的vacuum和MVCC）和Citus (pg_cron)。感谢以上开源项目的贡献。


## 帮助与分享

* 请加入PolarDB PostgreSQL的Slack群组：
   <https://app.slack.com/client/T023NM10KGE/C023VEMKS02>

* 请使用钉钉扫描如下二维码，加入PolarDB技术推广组钉钉群。
   ![PolarDB技术推广小组](https://github.com/alibaba/PolarDB-for-PostgreSQL/blob/distributed/polardb_group.png)

___

 © 阿里巴巴集团控股有限公司 版权所有
