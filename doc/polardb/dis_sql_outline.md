## FDW-based distributed plug-in design

### Background

* Postgres-XC is an optimizer and executor that features an advanced kernel on top of the kernel of PostgreSQL databases. Postgres-XC is an independent branch of the PostgresSQL community, and does not support massively parallel processing (MPP) architecture or online analytical processing (OLAP) queries. Postgres-XC provides Global Transaction Manager (GTM) to manage distributed transactions centrally, but node expansion is restricted by GTM. Postgres-XC supports global consistency. Postgres-XC has reached its End of Life (EOL).

* Postgres-XL is an advanced optimizer that uses new operators, adopts the MPP architecture, and supports Hybrid Transaction/Analytical Processing (HTAP). Postgres-XL is an independent branch of the PostgresSQL community, Postgres-XL provides GTM to manage distributed transactions centrally, but node expansion is restricted by GTM. Postgres-XL has reached its EOL.

* Tbase is built on top of Postgres-XL and is an independent branch of the PostgresSQL community. The kernel code, GTM, and node expansion capability of Tbase are significantly improved. Tbase supports Global Transaction Service (GTS) and HTAP. The version of Tbase lags behind PostgresSQL community.

* Citus is a distributed extension to PostgresSQL and is iterated rapidly in step with PostgresSQL versions. Citus does not support global consistency or the MPP architecture, and is not applicable to scenarios where a large amount of data needs to be transferred between worker nodes. Citus supports MX, a "masterless" mode for faster data loading.

### Summary

Developing an HTAP database that can be continuously iterated is our ultimate goal. However, independent development and kernel optimizations can easily cause our database products to diverge from the mainstream development trend, which may cause us to lose competitive advantages. To resolve this issue, the key lies in learning from the best practices of the open source community, focusing on the most important parts of distributed HTAP databases, and avoiding attempting to solve problems that have already been solved by the community.   Therefore, we leveraged the advantages of Postgres-XC, Postgres-XL, Tbase, and Citus to develop our FDW-based distributed plug-ins.


### Advantages of FDW-based distributed plug-ins

1. Distributed plug-ins based on the foreign data wrapper (FDW) framework leverage the advantages of native PostgreSQL optimizers, iterate in step with PostgreSQL versions, provide more flexible interfaces, and can extend optimizer capabilities. 38 FDW operations are supported to provide more fine-grained intervention in the generation and execution of query plans. These operations can be used together with the currently known hooks.
2. FDW-based distributed plug-ins can modularize distributed functions to facilitate continuous evolution, so that you can invest more energy on distributed design and development.
3. FDW-based distributed plug-ins can iterate quickly and support new features supported by the PostgreSQL community.
4. FDW-based distributed plug-ins support SQL statements and multi-mode computing, and allow databases to access various data sources.
5. FDW-based distributed plug-ins make it easier to extend the MPP architecture.

### Design overview

#### Architecture

Supported deployment of coordinators and datanodes:

<img src="dis_sql_cn_dn.png" alt="Coordinator node and data node in distributed SQL computation" width="400"/> 

#### Database Kernel architecture

<img src="dis_sql_kernel_arch.png" alt="DB Kernel Components for distributed SQL computation" width="500"/> 

#### Main features

* Support a variety of distributed compute engines.

A variety of compute engines can be designed for different FDW operations.  Currently, only the polarx_fdw compute engine is supported. If SQL statements cannot be processed by the Fast Query Shipping engine, the polarx_fdw compute engine can be used to generate a distributed execution plan.

* Support the pushdown of LEFT, RIGHT, FULL, and INNER JOIN queries.
* Support the direct pushdown of simple DML queries.
* Support the distributed pushdown of complex DML queries.
* Support the Fast Query Shipping engine that is deployed as a plug-in, improving the efficiency of processing transactions.
* Support the distributed processing engines for Data Definition Language (DDL), Datamax Programming Language (DPL), Data Control Language (DCL), and Transaction Control Language (TCL) that are deployed as plug-ins.
* Support utility statements that are executed as plug-ins to automatically create distributed tables on nodes.
* Support cost-based optimizers (CBOs).
* Support remote cost computation.
* Support local cost computation of Hash Joins and Nested Loop Joins.
* Support multiple distribution methods, such as hash, replication, modulo, and round-robin.
* Support all PostgreSQL syntax.
* Adopt a distributed plug-in structure and modular management.

___

Copyright © Alibaba Group, Inc.

