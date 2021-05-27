![PolarDB Banner](polardb.png)

## What is PolarDB for PostgreSQL?

PolarDB for PostgreSQL (PolarDB for short) is an open source database system based on PostgreSQL. It extends PostgreSQL to become a share-nothing distributed database, which supports **global data consistency** and **ACID across database nodes**, **distributed SQL processing**, and  **data redundancy** and **high availability** through Paxos based replication. PolarDB is designed to add values and new features to PostgreSQL in dimensions of high performance, scalability, high availability, and elasticity. At the same time, PolarDB remains SQL compatibility to single-node PostgreSQL with best effort.

PolarDB will evolve and offer its functions and features in two major parts: an extension and a patch to Postgres. The extension part includes components implemented outside PostgreSQL kernel, such as distributed transaction management, global or distributed time service, distributed SQL processing, additional metadata and internal functions, and tools to manage database clusters and conduct fault tolerance or recovery. Having most of its functions in a Postgres extension, PolarDB targets **easy upgrading**, **easy migration**, and **fast adoption**. The patch part includes the changes necessary to the kernel, such as distributed MVCC for different isolation levels. We expect functions and codes in the patch part is limited. As a result, PolarDB can be easily upgraded with newer PostgreSQL versions and maintain full compatible to PostgreSQL.

## Quick start with PolarDB

Three approaches are offered to quickly try out PolarDB: Alibaba Cloud service, deployment using Docker images, and deployment from source codes.

### Alibaba Cloud Service

TBD

### Deployment Using Docker Images
TBD

### Deployment from Source Code

We extend a tool named as pgxc_ctl from PG-XC/PG-XL open source project to support cluster management, such as configuration generation, configuration modification, cluster initialization, starting/stopping nodes, and switchover, etc. Its detail usage can be found [deployment](/doc/polardb/deployment.md).

* download source code
* install dependency packages (use Centos as an example)

```bash
         sudo yum install libzstd-devel libzstd zstd cmake openssl-devel protobuf-devel readline-devel libxml2-devel libxslt-devel zlib-devel bzip2-devel lz4-devel snappy-devel
```
* build and install binary

```bash
         ./configure --prefix=/home/postgres/polardb/polardbhome
         make
         make install
         cd contrib
         make
```

or you can just call build script to build.

        ./build.sh

* setup environment variables

```bash
         vi ~/.bash_profile
         export PGUSER=postgres
         export PGHOME=/home/postgres/polardb/polardbhome
         export PGXC_CTL_HOME=/home/postgres/polardb/polardbhome/bin/pgxc_ctl

         export LD_LIBRARY_PATH=$PGHOME/lib
         export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/lib:/usr/lib:/usr/local/lib
         export PATH=$PGHOME/bin:$PATH
```

* generate default configure file

```bash
        pgxc_ctl -c $HOME/polardb/polardb_paxos.conf prepare standalone

```

* deploy binary file

```bash
    pgxc_ctl -c $HOME/polardb/polardb_paxos.conf  deploy all
```

* clean residual installation and init cluster

```bash
         pgxc_ctl -c $HOME/polardb/polardb_paxos.conf  clean all

         pgxc_ctl -c $HOME/polardb/polardb_paxos.conf  init all

         pgxc_ctl -c $HOME/polardb/polardb_paxos.conf  monitor all
```

* install dependency packages for cluster management

```bash
	 pgxc_ctl -c $HOME/polardb/polardb_paxos.conf deploy cm
```

* start cluster or node

```bash
         pgxc_ctl -c $HOME/polardb/polardb_paxos.conf start all
```

* stop cluster or node

```bash
         pgxc_ctl -c $HOME/polardb/polardb_paxos.conf stop all
```

* failover datanode

datanode_1 is node name configured in polardb_paxos.conf.

```bash
         pgxc_ctl -c $HOME/polardb/polardb_paxos.conf failover datanode datanode_1
```

* cluster health check

 check cluster status and start failed node.

```bash
         pgxc_ctl -c $HOME/polardb/polardb_paxos.conf healthcheck all
```

* example for other command

```bash
        pgxc_ctl -c $HOME/polardb/polardb_paxos.conf kill all
        pgxc_ctl -c $HOME/polardb/polardb_paxos.conf log var datanodeNames
        pgxc_ctl -c $HOME/polardb/polardb_paxos.conf show configuration all
```

* check and test

```bash
         ps -ef | grep postgres
         psql -p 10001 -d postgres -c "create table t1(a int primary key, b int);"
         createdb test -p 10001
         psql -p 10001 -d test -c "select version();"
```

### One-Key Deployment 
onekey.sh can be used to build、configure、deploy and init a database cluster.

```bash
         ./onekey.sh all
```

reference [deployment](/doc/polardb/deployment.md) for detail instructions.

Regress and other test details can be found [here](/doc/polardb/regress.md). Some benchmarking example is [here](/doc/polardb/benchmark.md)

## Architecture & Roadmap

PolarDB uses a share-nothing architecture.  Each node stores data and also executes queries, and they coordinate with each other through message passing.  The architecture allows the database to be scaled by adding more nodes to the cluster.

PolarDB slices a table into shards by hashing its primary key. The number of shards is configurable. Shards are stored in PolarDB nodes. When a query accesses shards in multiple nodes, a distributed transaction and a transaction coordinator are used to maintain ACID across nodes. 

Each shard is replicated to three nodes with each replica stored on different node. In order to save costs, we can deploy two of the replicas to store complete data. The third replica only stores write ahead log (WAL), which participates in the election but cannot be chosen as the leader.

See [architecture design](/doc/polardb/arch.md) for more information

## Documentation

* [architecture design](/doc/polardb/arch.md)
* [roadmap](/doc/polardb/roadmap.md)
* Features and their design in PolarDB for PG Version 1.0
  * [Paxos replication](/doc/polardb/ha_paxos.md)
  * [cluster management](/doc/polardb/cluster.md)
  * [Parallel Redo](/doc/polardb/parallel_redo.md)
  * [Timestamp based MVCC](/doc/polardb/cts.md)


## Contributing

PolarDB is built on and of open source, and extends open source PostgreSQL. Your contributions are welcome. How to start developing PolarDB is summarized in [coding style](/doc/polardb/style.md). It also introduces our coding style and quality guidelines.

## Licensing
PolarDB code is released under the Apache Version 2.0 License and the Licenses with PostgreSQL code.

The relevant licenses can be found in the comments at the top of each file.

Reference [License](LICENSE) and [NOTICE](NOTICE) for details.

## Acknowledgements

Some codes and design ideas were from other open source projects, such as PG-XC/XL(pgxc_ctl), TBase (timestamp based vacuum and MVCC), and CitusDB (pg_cron). Thanks for their contributions.
___

Copyright © Alibaba Group, Inc.
