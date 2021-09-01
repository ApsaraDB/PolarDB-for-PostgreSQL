## ![image.png](https://intranetproxy.alipay.com/skylark/lark/0/2021/png/28269/1627440541586-876ff662-e1ba-487f-87f6-a907b4778b08.png#clientId=u6f88ee31-9aa9-4&from=paste&height=100&id=u85491bb9&margin=%5Bobject%20Object%5D&name=image.png&originHeight=200&originWidth=233&originalType=binary&ratio=1&size=8137&status=done&style=none&taskId=uf531dbe0-78ea-4138-927c-db0f683bd12&width=116.5)
## What is PolarDB？
![image.png](https://intranetproxy.alipay.com/skylark/lark/0/2021/png/28269/1627440447630-0b63e6d2-dd2a-4727-af39-230848e571cb.png#clientId=uf923a3ed-57dc-4&from=paste&height=185&id=ubff3a3b0&margin=%5Bobject%20Object%5D&name=image.png&originHeight=369&originWidth=757&originalType=binary&ratio=1&size=33631&status=done&style=none&taskId=ufe16bf57-7998-4b09-98b3-fad6a6c6025&width=378.5)
PolarDB for PostgreSQL集中式版本是一款阿里云自主研发的云原生数据库产品，100%兼容PostgreSQL，采用基于Shared-Storage的存储计算分离架构，具有极致弹性，毫秒级延迟，HTAP的能力：

1. 极致弹性：存储与计算能力均可独立的横向扩展
   1. 当计算能力不够时，可以单独扩展计算集群，数据无需复制；
   1. 当计存储容量/IO不够时，可以单独扩展存储集群，而不中断业务；
2. 毫秒级延迟：
   1. WAL日志存储在共享存储上，RW/RO间仅复制WAL的meta；
   1. 研发了独创的LogIndex技术，实现了Lazy回放和Parallel回放，理论上最大程度的缩小了RW和RO节点间的延迟；
3. HTAP能力：研发了基于Shared-Storage的分布式并行执行框架，来加速在TP场景下的AP查询。在一套TP型的数据，支持2套计算引擎：
   1. 单机执行引擎：处理高并发的TP型负载；
   1. 分布式执行引擎：处理大查询的AP型负载；

​

另外，支持时空、GIS、图像、向量、搜索、图谱等多模创新特性，应对企业对数据处理日新月异的需求。
​

## Quick Start（[@学弈(rogers.ww)](/rogers.ww)）
## Architecture & Roadmap
PolarDB集中式版本采用基于Shared-Storage的存储计算分离架构。数据库由传统的share nothing，转变成了shared storage架构。由原来的N份计算+N份存储，转变成了N份计算+1份存储。虽然共享存储上数据是一份，但是内存中的状态是不同的，需要做内存状态的同步来维护数据的一致性；同时Master在刷脏时也需要做协调，避免Replica节点读取到超前的“未来页面”，也要避免Replica节点读取到过时的没有在内存中被正确回放的“过去页面”。为了解决上述问题创造性的设计了LogIndex数据结构来维护页面的回放历史，该结构能够在Master/Replica上进行同步。
在存储计算分离后，IO单路延迟变大同时IO的吞吐也变大了。在处理分析型查询时仅使用单个Replica无法发挥出存储侧的大IO带宽优势，也无法发挥其他Replica节点的CPU/Mem/IO。研发了基于Shared-Storage的并行执行引擎，能够SQL级别的弹性利用任意数目的CPU来加速分析查询。能够支持HTAP的混合负载场景。
​

详情请查阅 [architecture design](/doc/polardb/arch.md)
## Documentation

- [architecture design](/doc/polardb/arch.md)
- [roadmap](/doc/polardb/roadmap.md)
-  Features and their design in PolarDB for PG Version 1.0
   - [PolarVFS](/doc/polardb/polar_vfs.md)
   - [BufferManager](/doc/polardb/buffer_manager.md)
   - [Sync DDL](/doc/polardb/sync_ddl.md)
   - [LogIndex](/doc/polardb/LogIndex.md)
## Contributing
PolarDB is built on open-source projects and extends open-source PostgreSQL. Your contribution is welcome and appreciated. Please refer to [contributing](https://github.com/alibaba/PolarDB-for-PostgreSQL/blob/master/doc/polardb/contributing.md) for how to start coding and submit a PR.
## Licensing
PolarDB code is released under the Apache Version 2.0 License and the Licenses with PostgreSQL code.
The relevant licenses can be found in the comments at the top of each file.
Reference [License](https://github.com/alibaba/PolarDB-for-PostgreSQL/blob/master/LICENSE) and [NOTICE](https://github.com/alibaba/PolarDB-for-PostgreSQL/blob/master/NOTICE) for details.
## Acknowledgements
Some codes and design ideas were from other open-source projects, such as PG-XC/XL(pgxc_ctl), TBase (part of timestamp-based vacuum and MVCC), Greenplum, and Citus (pg_cron). Thanks for their contributions.
## Communications

- PolarDB for PostgreSQL at Slack [https://app.slack.com/client/T023NM10KGE/C023VEMKS02](https://app.slack.com/client/T023NM10KGE/C023VEMKS02)
- PolarDB Technial Promotion Group at DingDing [![](https://github.com/alibaba/PolarDB-for-PostgreSQL/raw/master/polardb_group.png#from=url&id=kdjhe&margin=%5Bobject%20Object%5D&originHeight=256&originWidth=256&originalType=binary&ratio=1&status=done&style=none)](https://github.com/alibaba/PolarDB-for-PostgreSQL/blob/master/polardb_group.png)

---

Copyright © Alibaba Group, Inc.
