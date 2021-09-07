PolarDB PostgreSQL将持续发布对用户有价值的功能。当前我们计划了5个阶段：
### PolarDB PostgreSQL 1.0版本
1.0版本基于Shared-Storage的存储计算分离架构，发布必备的最小功能集合，例如：PolarVFS、刷脏和Buffer管理、LogIndex、SyncDDL等。

- PolarVFS：数据库内核中抽象出了一层VFS层，使得内核可以对接任意的存储，包括bufferIO和directIO。
- 刷脏和Buffer管理：由原来的N份计算+N份存储，转变成了N份计算+1份存储，主节点在刷脏时需要做协调，避免只读节点读取到超前的“未来页面”。
- LogIndex: 由于只读节点不能刷脏，所需要的特定版本页面需要从Shared-Storage上读取一个老的版本页面，并通过在内存中回放来得到正确的版本。LogIndex结构记录了每个Page所对应的WAL日志Meta信息，在需要回放时直接查找LogIndex，从而加速回放过程。
- DDL同步: 在存储计算分离后，主节点在执行DDL时需要兼顾只读节点对Relation等对象的引用，相关的DDL动作需要同步地在只读节点上上锁。
- 数据库监控：支持主机和数据库的监控，同时为HA切换提供了判断依据。
### PolarDB PostgreSQL 2.0版本
除了在存储计算分离架构上改动之外，2.0版本将在优化器上进行深度的优化，例如：

- UniqueKey：和Plan节点的有序性类似，UniqueKey维护的是Plan节点数据的唯一性。数据的唯一性可以减少不必要的DISTINCT、Group By，增加Join结果有序性判断等。
### PolarDB PostgreSQL 3.0版本
3.0版本主要在存储计算分离后在可用性上进行重大优化，例如：

- 并行回放：存储计算分离之后，PolarDB通过LogIndex实现了Lazy的回放。实现原理为：仅标记一个Page应该回放哪些WAL日志，在读进程时再进行真正的回放过程。此时对读的性能是有影响的。在3.0版本中，我们在Lazy回放基础上实现了并行回放，从而加速Page的回放过程。
- OnlinePromote：在主节点崩溃后，切换到任意只读节点。该只读节点无需重启，继续并行回放完所有的WAL之后，Promote成为新的主节点，从而进一步降低了不可用时间。
### PolarDB PostgreSQL 4.0版本
为了满足日益增多的HTAP混合负载需求，4.0版本将发布基于Shared-Storage架构的分布式并行执行引擎，充分发挥多个只读节点的CPU/MEM/IO资源。  
经测试，在计算集群逐步扩展到256核时，性能仍然能够线性提升。
### PolarDB PostgreSQL 5.0版本
基于存储计算分离的一写多读架构中，读能力能够弹性的扩展，但是写入能力仍然只能在单个节点上执行。  
5.0版本将发布Share-Nothing On Share-Everything架构，结合PolarDB的分布式版本和PolarDB集中式版本的架构优势，使得多个节点都能够写入。
