[![logo](doc/PolarDB-CN/pic/PolarDB_logo.png)](https://developer.aliyun.com/topic/polardb-for-pg)

[![Build and test PolarDB](https://github.com/alibaba/PolarDB-for-PostgreSQL/actions/workflows/build-and-test.yml/badge.svg)](https://github.com/alibaba/PolarDB-for-PostgreSQL/actions/workflows/build-and-test.yml) [![GitHub issues](https://img.shields.io/github/issues/alibaba/PolarDB-for-PostgreSQL)](https://GitHub.com/alibaba/PolarDB-for-PostgreSQL/issues/) [![GitHub pull-requests](https://img.shields.io/github/issues-pr/alibaba/PolarDB-for-PostgreSQL)](https://GitHub.com/alibaba/PolarDB-for-PostgreSQL/pull/) [![GitHub forks](https://badgen.net/github/forks/alibaba/PolarDB-for-PostgreSQL)](https://github.com/alibaba/PolarDB-for-PostgreSQL/network/members) [![GitHub stars](https://badgen.net/github/stars/alibaba/PolarDB-for-PostgreSQL)](https://github.com/alibaba/PolarDB-for-PostgreSQL/stargazers) [![GitHub contributors](https://badgen.net/github/contributors/alibaba/PolarDB-for-PostgreSQL)](https://github.com/alibaba/PolarDB-for-PostgreSQL/graphs/contributors) [![license](https://img.shields.io/badge/license-Apache--2.0-blue)](doc/PolarDB-CN/LICENSE.txt)

# 什么是 PolarDB PostgreSQL

![image.png](doc/PolarDB-CN/pic/1_polardb_architecture.png)

PolarDB PostgreSQL（下文简称为 PolarDB）是一款阿里云自主研发的云原生数据库产品，100% 兼容 PostgreSQL，采用基于 Shared-Storage 的存储计算分离架构，具有极致弹性、毫秒级延迟、HTAP 的能力。

1. 极致弹性：存储与计算能力均可独立地横向扩展。
    - 当计算能力不够时，可以单独扩展计算集群，数据无需复制。
    - 当存储容量或 I/O 不够时，可以单独扩展存储集群，而不中断业务。
2. 毫秒级延迟：
    - WAL 日志存储在共享存储上，RW 到所有 RO 之间仅复制 WAL 的元数据。
    - 独创的 *LogIndex* 技术，实现了 Lazy 回放和 Parallel 回放，理论上最大程度地缩小了 RW 和 RO 节点间的延迟。
3. HTAP 能力：基于 Shared-Storage 的分布式并行执行框架，加速在 OLTP 场景下的 OLAP 查询。一套 OLTP 型的数据，可支持 2 套计算引擎：
    - 单机执行引擎：处理高并发的 TP 型负载。
    - 分布式执行引擎：处理大查询的 AP 型负载。

PolarDB 还支持时空、GIS、图像、向量、搜索、图谱等多模创新特性，应对企业对数据处理日新月异的需求。

另外，除了上述 Shared-Storage 云原生的模式，PolarDB 还支持以 Shared-Nothing 模式部署，详见 **distribute** 分支的 [README](https://github.com/alibaba/PolarDB-for-PostgreSQL/blob/distributed/README.md)。

# 分支说明

PolarDB 后续默认分支为 main 分支，支持存储计算分离的形态。分布式形态由 distribute 分支支持（对应之前的 master 分支）。

# 产品架构和版本规划

PolarDB 采用了基于 Shared-Storage 的存储计算分离架构。数据库由传统的 Share-Nothing 架构，转变成了 Shared-Storage 架构。由原来的 N 份计算 + N 份存储，转变成了 N 份计算 + 1 份存储。虽然共享存储上数据是一份，但是数据在各节点内存中的状态是不同的，需要通过内存状态的同步来维护数据的一致性；同时主节点在刷脏时也需要做协调，避免只读节点读取到超前的 **“未来页面”**，也要避免只读节点读取到过时的没有在内存中被正确回放的 **“过去页面”**。为了解决该问题，PolarDB 创造性地设计了 *LogIndex* 数据结构来维护页面的回放历史，该结构能够实现主节点与只读节点之间的同步。

在存储计算分离后，I/O 单路延迟变大的同时，I/O 的吞吐也变大了。在处理分析型查询时，仅使用单个只读节点无法发挥出存储侧的大 I/O 带宽优势，也无法利用其他只读节点的 CPU、内存和 I/O 资源。为了解决该问题，PolarDB 研发了基于 Shared-Storage 的并行执行引擎，能够在 SQL 级别上弹性利用任意数目的 CPU 来加速分析查询，支持 HTAP 的混合负载场景。

详情请查阅 [产品架构](/doc/PolarDB-CN/Architecture.md) 和 [版本规划](/doc/PolarDB-CN/Roadmap.md)。

# 文档

- [产品架构](/doc/PolarDB-CN/Architecture.md)
- [版本规划](/doc/PolarDB-CN/Roadmap.md)
- PolarDB PostgreSQL 1.0 版本功能特性
    - [Buffer 管理](/doc/PolarDB-CN/Buffer_Management.md)
    - [DDL 同步](/doc/PolarDB-CN/DDL_Synchronization.md)
    - [LogIndex](/doc/PolarDB-CN/LogIndex.md)
- 数据库监控（即将上线）
- PolarStack（即将上线）
- PolarVFS（即将上线）

# 快速入门

我们提供了三种途径来使用PolarDB数据库：阿里巴巴云服务、搭建本地存储的实例、搭建基于 PFS 共享存储的实例（即将上线）。

## 阿里巴巴云服务

阿里云云原生关系型数据库 PolarDB PostgreSQL 引擎：[官网地址](https://www.aliyun.com/product/polardb)。

## 搭建本地存储的实例
我们提供了一键部署脚本，助您快速编译PolarDB内核并搭建本地实例。本节介绍了如何通过提供的一键部署脚本，快速搭建存储为本地磁盘的PolarDB实例。  
**操作系统要求**：CentOS 7.5及以上。以下步骤在CentOS 7.5上通过测试。  
**说明**：请使用同一个用户进行以下步骤。请勿使用root用户搭建实例。

我们提供了一键部署脚本，助您快速编译 PolarDB 内核并搭建本地实例。本节介绍了如何通过提供的一键部署脚本，快速搭建存储为本地磁盘的 PolarDB 实例。

**操作系统要求**：CentOS 7.5 及以上。以下步骤在 CentOS 7.5 上通过测试。

> 说明：请使用同一个用户进行以下步骤。请勿使用 *root* 用户搭建实例。

1. 下载 PolarDB 源代码，地址：[https://github.com/alibaba/PolarDB-for-PostgreSQL/tree/main](https://github.com/alibaba/PolarDB-for-PostgreSQL/tree/main)。
2. 安装相关依赖：

    ```bash
    sudo yum install readline-devel zlib-devel perl-CPAN bison flex
    sudo cpan -fi Test::More IPC::Run
    ```
3. 根据不同的搭建场景，可选择不同的脚本执行命令：
    - 只编译数据库源码，不创建本地实例：

        ```bash
        ./polardb_build.sh --noinit
        ```
    - 编译并创建本地单节点实例，节点为主节点（端口为 5432）：

        ```bash
        ./polardb_build.sh
        ```
    - 编译并创建本地多节点实例，节点包括：
        - 1 个主节点（端口为 5432）
        - 1 个只读节点（端口为 5433）

        ```bash
        ./polardb_build.sh --withrep --repnum=1
        ```
    - 编译并创建本地多节点实例，节点包括：
        - 1 个主节点（端口为 5432）
        - 1 个只读节点（端口为 5433）
        - 1 个备库节点（端口为 5434）

        ```bash
        ./polardb_build.sh --withrep --repnum=1 --withstandby
        ```
    - 编译并创建本地多节点实例，节点包括：
        - 1 个主节点（端口为 5432）
        - 2 个只读节点（端口分别为 5433 与 5434）
        - 1 个备库节点（端口为 5435）

        ```bash
        ./polardb_build.sh --withrep --repnum=2 --withstandby
        ```
4. 部署完成后，需要进行实例检查和测试，确保部署正确。
    - 实例检查：

        ```bash
        $HOME/tmp_basedir_polardb_pg_1100_bld/bin/psql -p 5432 -c 'select version();'
        $HOME/tmp_basedir_polardb_pg_1100_bld/bin/psql -p 5432 -c 'select * from pg_replication_slots;'
        ```
    - 一键执行全量回归测试：

        ```bash
        ./polardb_build.sh --withrep --repnum=1 --withstandby -r-check-all -e -r-contrib -r-pl -r-external -r-installcheck-all
        ```

# 软件许可说明

PolarDB 的代码的发布基于 Apache 2.0 版本和 PostgreSQL 代码的软件许可。相关的许可说明可参见 [LICENSE](doc/PolarDB-CN/LICENSE.txt) 和 [NOTICE](doc/PolarDB-CN/NOTICE.txt)。

# 致谢

部分代码和设计思路参考了其他开源项目，例如：PG-XC/XL (pgxc_ctl)、TBase (部分基于时间戳的 vacuum 和 MVCC)、Greenplum 以及 Citus (pg_cron)。感谢以上开源项目的贡献。

# 联系我们

- PolarDB PostgreSQL Slack：[https://app.slack.com/client/T023NM10KGE/C023VEMKS02](https://app.slack.com/client/T023NM10KGE/C023VEMKS02)
- 使用钉钉扫描如下二维码，加入PolarDB技术推广组钉钉群

    ![polardb_group](doc/PolarDB-CN/pic/polardb_group.png)

---

Copyright © Alibaba Group, Inc.

