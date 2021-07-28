PolarDB集中式版本开源项目持续的发布对用户有价值的功能。我们计划了5个阶段。
### PolarDB for PG version 1.0
基于Shared-Storage的存储计算分离后架构必备最小的功能集合，如：PolarVfs，刷脏和Buffer管理，LogIndex，SyncDDL:

- PolarVfs：数据库内核中抽象出了一层vfs层，使得内核可以对接任意的存储，包括bufferIO和directIO；
- 刷脏和Buffer管理：由原来的N份计算+N份存储，转变成了N份计算+1份存储，Master在刷脏时需要做协调，避免Replica节点读取到超前的“未来页面”；
- LogIndex: 由于Replica不能刷脏，所需要的特定版本页面需要从Shared-Storage上读取一个老的版本页面，并通过在内存中回放来得到正确的版本。LogIndex结构记录了每个Page所对应的WAL日志Meta信息，在需要回放时直接查找LogIndex，来加速回放过程；
- SyncDDL: 在存储计算分离后，Master在执行DDL时需要兼顾Replica节点对Relation等对象的引用，相关的DDL动作需要同步地在Replica上上锁；
### PolarDB for PG version 2.0
PolarDB除了在存储计算分离架构上改动之外，还在优化器上进行了深度的优化：

- UniqueKey：[@一挃(yizhi.fzh)](/yizhi.fzh)
### PolarDB for PG version 3.0
3.0版本主要在存储计算分离后在可用性上的重大优化：

- 并行回放：存储计算分离之后通过LogIndex实现了Lazy的回放，仅仅标记一个Page应该回放哪些WAL日志，在读进程时再进行真正的回放过程，此时对读的性能是有影响的。我们在Lazy回放基础上实现了并行回放，来加速Page的回放过程；
- OnlinePromote：在Master节点崩溃后，切换到任意一个Replica节点，该Replica节点无需重启，继续并行回放完所有的WAL之后就Promote称为新的Master，进一步降低了不可用时间；
### 
### PolarDB for PG version 4.0
为了满足日益增多的HTAP混合负载需求，在4.0版本PolarDB将发布基于Shared-Storage架构的分布式并行执行引擎，充分发布多个Replica节点的CPU/MEM/IO资源。经测试，在计算集群扩展到256核时性能仍然能够线性提升。
​

### PolarDB for PG version 5.0
基于存储计算分离的一写多读架构中，读能力能够弹性的扩展，但是写入能力仍然只能在单个节点上执行。PolarDB5.0版本会发布Share-Nothing On Share-Everything架构，结合PolarDB的分布式版本和PolarDB集中式版本的架构优势，多个节点都能够写入。
