
## HLC, CTS and (Distributed) Snapshot Isolation

PolarDB for PG (or PolarDB for short) implements a multi-core scalable transaction processing system
and supports distributed transactions (upcoming) by using commit timestamp based MVCC.
The traditional PostgreSQL uses xid-based snapshot to provide transactional isolation
on top of MVCC which can introduce scaling bottleneck on many-core machines.
Specifically, each transaction snapshots the currently running transactions in the Proc-Array at its start 
by holding the Proc-Array lock. Meanwhile, the Proc-Array lock is acquired to clear a transaction at its end. 
The frequent use of the global Proc-Array lock in the critical path could lead to severe contention. 
For read-committed (RC) isolation, each snapshot is generated at the statement level which can further exacerbate the performance. 

To tackle this problem, PolarDB adopts the commit timestamp based MVCC for better
scalability. Specifically, each transaction assigns a start timestamp and commit timestamp
for each transaction when it starts and commits. The timestamps are monotonically increasing
to maintain transaction order for isolation. 

PolarDB implements a CTS (Commit Timestamp Store) which is carefully designed for scalability to store commit timestamps
and transaction status.
As being in critical path, PolarDB employs well-known concurrent data structures and lock-free algorithms for CTS to avoid scaling bottlenecks on multi-core machines.
Specifically, PolarDB avoids acquiring exclusive lock as far as possible when accessing the CTS buffers in memory.
Exclusive locking is needed only when a LRU replacement occurs. 
To parallelize LRU buffer replacement and flush, CTS adopts multi-lru structures for multi-core architectures.

As xid-based snapshots are removed for better performance, PolarDB implements CTS-based vacuum, hot-chain pruning and logical replication, etc. 
More interestingly, by using CTS PolarDB simplifies the snapshot building process of logical replication.
The original logical replication in PostgreSQL consists of four steps which is complex and is hard to understand and to reason its correctness. In contrast, the CTS based snapshot building undergoes only two steps and is significantly easier to understand
than the original one.

PolarDB designs a hybrid logical clock (HLC) to generate start and commit timestamps for transactions to maintain snapshot isolation (supporting RR and RC isolations). The adoption of HLC is to support decentralized distributed transaction management in our upcoming distributed shared-nothing PolarDB-PG. The HLC consists of a logical part (strictly increasing counter) and physical part. The logical part is used to track transaction order to ensure snapshot isolation while the physical part is used to generate fresshness snapshots across different machines. The physical clock on each node can be synchronized by using NTP (Network Time Protocol) or PTP (Precision Time Protocol).
The PTP within a local area network can keep the maximum clock skew between any two machines as small as several microseconds.
The adoption of advanced PTP in a single data center can enable PolarDB-PG to provide strong external consistency across different nodes like Google Spanner. However, our upcoming open-sourced distributed version assumes machines being synchronized by NTP and only aims to guarantee snapshot isolation and internal consistency across nodes. A 64 bit HLC timestamp consists of 16 lowest bit logical counter, 48 higher bit physical time and 2 reserved bits. 

To maintain distributed snapshot isolation, PolarDB adopts HLC to generate snapshot start timestmap for each transaction on the coordinator node. To commit a distributed transaction, PolarDB uses 2PC, collects prepared HLC timestamps from all the participating nodes during the prepare phase and determines its commit timestamp by choosing the maximum timestamp from all the prepared timestamps.
The hybrid logical clock on each node is updated using the arriving start and commit timestamps when a transaction accesses it, e.g., transaction begin and commit. PolarDB uses 2PC prepared wait mechanism to resolve causality ordering between transactions like
Google Percolator. The prepared status is maintained in CTS for fast access and is replaced with a commit timestamp when the prepared transaction commits. The HLC based distributed transaction will appear soon in our distributed shared-nothing version of PolarDB-PG.


___

Copyright © Alibaba Group, Inc.

