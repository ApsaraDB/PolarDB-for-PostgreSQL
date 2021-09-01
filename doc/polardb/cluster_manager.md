## Cluster Manager based on pgxc_ctl and pg_cron.

PolarDB for PostgreSQL HA Support Paxos 3 Nodes, Steaming HA, Logical replication based Active-Acitve HA models. Cluster version, and standalone version will base on above HA models to keep system High Availability.

pgxc_ctl is support by pgxc, main use for manage pgxc cluster, support cluster deploy, cluster configure, cluster init, cluster start, cluster stop, cluster clean etc. We have extend ability for Paxos 3 Nodes support, Logical replication based Active-Active support, standalone version support, cluster manager support For pgxc, pgxl customer will use PolarDB cluster and standalone version easily.

pg_cron is a time-based job scheduler support by Citus, and main use for cluster heartbeat. it will auto call healthcheck command to check cluster health, and auto active-standby switch if it's needed.


## How to use

### Deployment from Source Code

We extend a tool named as pgxc_ctl from PG-XC/PG-XL open source project to support cluster management, such as configuration generation, configuration modification, cluster initialization, starting/stopping nodes, and switchover, etc. Its detail usage can be found [deployment](/doc/polardb/deployment.md).

* download source code
* install dependent packages (use Centos as an example)

```bash
sudo yum install libzstd-devel libzstd zstd cmake openssl-devel protobuf-devel readline-devel libxml2-devel libxslt-devel zlib-devel bzip2-devel lz4-devel snappy-devel
```
* set up ssh login without password
Call ssh-copy-id command to configure ssh so that no password is needed when using pgxc_ctl.

```bash
ssh-copy-id username@IP
```

* build and install binary

```bash
./configure --prefix=/home/postgres/polardb/polardbhome
make
make install
cd contrib
make
```

or you can just call build.sh script.

```bash
./build.sh
```

* set up environment variables

```bash
vi ~/.bashrc
export PATH="$HOME/polardb/polardbhome/bin:$PATH"
export LD_LIBRARY_PATH="$HOME/polardb/polardbhome/lib:$LD_LIBRARY_PATH"ß
```

* generate default configure file

```bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf prepare standalone
```

* deploy binary file

```bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf deploy all
```

* clean residual installation and init cluster

```bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf clean all
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf init all
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf monitor all
```

* install dependent packages for cluster management

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

___

Copyright © Alibaba Group, Inc.
