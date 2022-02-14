
## 远程恢复保护数据

PostgreSQL引入了全页写（full page write）机制，以避免系统崩溃时产生不一致。但是，全页写大大增加了写操作，导致性能降低。远程恢复旨在在恢复期间从镜像节点（备节点或主节点）获取完整页面，以防止在没有全页写的情况下出现torn page。因此，可以关闭全页写以减少I/O并提高性能。

具体来说，自上一次检查点以来首次修改页面时，PolarDB PostgreSQL会在WAL中写入一个特殊位。在恢复过程中，当遇到一个特殊位，提示需要进行远程获取时，PolarDB PostgreSQL数据库实例将从其镜像实例中获取页面并在本地还原页面，然后再重放之后的修改。

远程恢复还可以利用并行WAL重做框架来获取页面以及并行中继WAL记录。

## 使用方法
1. 您可以在数据目录中添加一个remote_recovery.conf文件来支持远程恢复。remote_recovery.conf的格式与recovery.conf相同。您必须指定镜像节点地址，用于建立连接。例如，可以在remote_recovery.conf中添加"standby_conninfo = 'host = xx port = xx user = xx password = xx application_name = standby'"。
2. 为镜像节点配置的max_wal_senders参数值必须大于还原节点max_parallel_replay_workers参数的值。
3. 如果镜像节点是备份节点，则应开启hot_standby以接受来自还原节点的连接请求。同时应该打开checkpoint_sync_standby，保证一旦完成一个检查点，所有WAL修改都会同步到镜像节点上。

___

© 阿里巴巴集团控股有限公司 版权所有

