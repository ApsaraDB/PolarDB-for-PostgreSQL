
## 并行WAL重做

为了加快WAL重做进行数据恢复和复制，PolarDB PostgreSQL实现了并行WAL重做机制。PolarDB PostgreSQL支持表级和页级两种形式的并行，两者复用了相同的并行框架。具体来说，PolarDB PostgreSQL创建指定数量的Worker节点用于并行重放。主进程不断从WAL读取WAL记录并分派到并行Worker节点上重放。主进程通过共享内存队列（shm）与Worker节点进行通信，并使用批处理来减少通信开销。由于采用批处理，当前的并行重放不支持热备份复制。

## 使用方法
1. 要启用并行WAL重做，GUC参数max_parallel_replay_workers的值必须大于0。
2. 您可以将enable_parallel_recovery_bypage设置为true，以启用页面级并行重做。
3. 如果想要在热备份复制的情况下也可以使用并行重做，请设置allow_hot_standby_inconsistency参数。

___

© 阿里巴巴集团控股有限公司 版权所有

