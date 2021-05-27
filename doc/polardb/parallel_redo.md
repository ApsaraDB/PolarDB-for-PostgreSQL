
## Parallel WAL redo

In order to speed up WAL redo for recovery and replication, PolarDB for PG implements
a parallel WAL redo mechanism. PolarDB for PG supports two forms of parallelism: 
table-level and page-level, which however reuse the same parallel framework.
Specifically, PolarDB for PG creates a specified number of workers for parallel replaying.
The main process reads WAL records from WAL continously and dispaches them to parallel 
workers for replaying. The main process communicates with the workers through shared-memory
queues (shm) and uses batching to reduce the communication overhead. Our current parallel
replaying does not support hot-standby repliation due to the adoption of batching.

## How to use
1. You should set the guc parameter of max_parallel_replay_workers with a value greater than 
   0 to enable parallel WAL redo.
2. You can set enable_parallel_recovery_bypage to be true to enable page-level parallel redo.
3. You can use parallel redo even in the case of hot-standby replication if the parameter 
   allow_hot_standby_inconsistency is set.

___

Copyright © Alibaba Group, Inc.

