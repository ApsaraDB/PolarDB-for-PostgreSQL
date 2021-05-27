## Architecture and Main Functions

PolarDB for PG implements a share-nothing architecture using PostgreSQL as a main component. Without specific mension, we use PolarDB to represent PolarDB for PG. PolarDB is fully compatible to PostgreSQL and supports PostgreSQL's most SQL functions. As a distributed database system, PolarDB achieves same data consistency and ACID as a single-node database system. PolarDB implements Paxos based replication, which offers high availability, data redundance, and consistency across nodes, during node failure and cluster reconfiguration. Fine-grained sharding and application-transparent shard relocation allow PolarDB to efficiently utilize cloud-wise resource to adapt to varying computation and storage requirements. PolarDB's distributed SQL engine achieves fast processing of complex queires by combining the comprehensity of PostgreSQL optimizer and the efficiency of parallel execution among or inside nodes. 

Overall, PolarDB provides scalable SQL computation and fully-ACID relational data storage on commidity hardware or standard cloud resources, such as ECS and block storage service.  


### PolarDB's Architecture
<img src="sharding_plug_in.png" alt="Sharding and Plug-in" width="250"/>

PolarDB cluster is formed of three main components: database node (DN), cluster manager (CM), and 
transaction & time service (TM). They are different processes and can be deployed in multiple servers or ECS. DNs are main database engines, which receive SQL requests from clients or load balancer and process them. Each DN can act as a coordinator to distribute queries and coordinate transactions across multiple DNs. Each DN is responsible for part of data stored in a database. Any operations, including read and write, to those data, are processed by their cooresponding DN. The data in one DN is further partitioned into shards. Those shards can be relocated to other DNs when PolarDB scales out or re-balances workload. DNs have their replicas storing same data through Paxos based replication. DNs and their replicas form a replication group. In such group, a primary DN handles all write requests and propogate their results to replica DNs, or called follower DNs. Our Paxos replication also supports logger DNs, which only stores log records rather than data. 

TM is a centralized service to support globally consistent and in-order status or counters. Ascending timestamp and global transaction ID are two examples. 

CM monitors each DN's status and maintains the cluster configuration. CM also supports tools and commands to manage a PolarDB cluster, such as starting or stopping DNs, upgrading, taking backup, and balancing workload.  

The above figure shows those components and their interaction.

PolarDB is in evolution. The functions of the above three components are gradually opened. The roadmap can be found [here](roadmap.md). 


### PolarDB's Main Functions

* [ACID capable distributed OLTP](acid.md)

* [Full SQL Compatibility of PostgreSQL](pg_compatible.md)
* [Distributed SQL execution](dis_sql.md)
* [Fast upgrading with latest PostgreSQL versions](upgrade.md)

* [Paxos based replication for data consistency](ha_paxos.md)

* [Fine-grained Sharding](sharding.md)

* [Online shard relocation](shard_reloc.md)
* High performance
  * [Timestamp based MVCC](cts.md)
  * [Parallel Redo](parallel_redo.md)
  * [Full-page-write avoidance](no_fpw.md)
  * [Fast query shipment](query_fqs.md)
  * [one-phace commit](1pc.md)
  * ...
* Scalability & Elasticity
  * [Hybrid logic clock](hlc.md)
  * [Scalable coordination](scale_co.md)
  * [Single-node Tx optimization](single_node_tx.md)
  * [Store-procedure push-down](sp_pushdown.md)
  * ...
* High Availability
  * [Online cluster expansion](cluster_expansion.md) 
  * [Low RTO failover](failover.md)
  * [Zero-interruption & user-transparent switchover](switchover)
  * [Cross-AZ/DC zoer-RPO deployment](cross_az_deployment)
  * [On-the-fly repairment of page corruption](page_correction.md) 
  * ...

___

Copyright © Alibaba Group, Inc.

