## Deployment

We support two deployment approaches:
- One-Key Deployment: a script to create new default environment by only one command.
- Deployment from Source Code: start a cluster from scratch.

Both approaches need setup of dependencies with correct system environment, such as OS and software packages. 

- [OS and other dependencies](#os_dep)
- [Prerequisite](#prep)
- [One-Key Deployment](#one_key)
- [Deployment from Source Code](#from_source)


## <a name="os_dep"></a>OS and other dependencies

* operating systems
  * Alibaba Group Enterprise Linux Server, VERSION="7.2 (Paladin)", 3.10.0-327.ali2017.alios7.x86_64
  * Centos 7

* GCC versions
  * gcc 10.2.1
  * gcc 9.2.1
  * gcc 7.2.1
  * gcc 4.8.5

## <a name="prep"></a>Prerequisites

* download source code from https://github.com/alibaba/PolarDB-for-PostgreSQL

* install dependent packages (use Centos as an example)

```bash
sudo yum install bison flex libzstd-devel libzstd zstd cmake openssl-devel protobuf-devel readline-devel libxml2-devel libxslt-devel zlib-devel bzip2-devel lz4-devel snappy-devel python-devel
```
* set up authorized key for fast access

Call ssh-copy-id command to configure ssh so that no password is needed when using pgxc_ctl.

```bash
ssh-copy-id username@IP
```

* set up environment variables

```bash
vi ~/.bashrc
export PATH="$HOME/polardb/polardbhome/bin:$PATH"
export LD_LIBRARY_PATH="$HOME/polardb/polardbhome/lib:$LD_LIBRARY_PATH"
```

### <a name="one_key"></a>Fast Deployment(One-Key for all)
This script uses default configuration to compile PolarDB, to deploy binary, and to start a cluster of three nodes, including a leader and two followers.
before call this script, please check environment variables, dependent packages, and authorized key are set up correctly.

* run onekey.sh script

```bash
./onekey.sh all
```

* onekey.sh script introduce
```bash
sh onekey.sh [all|standalone|dispaxos|build|configure|deploy|setup|dependencies|cm|clean]
```
    * all: one key for full environment build, include build source code, deploy 
    a 2c2d(2 coordinator nodes & 2 datanodes) cluster.
    * dispaxos: one key for full environment build, include build source code, deploy 
    and setup 2c2d & each datanode having 3-node paxos environment.
    * standalone: one key for full environment build, include build source code, deploy 
    and setup 3 node paxos environment by default configure.
    * build: invoke *build.sh* script to compile and create a release version.
    * configure：generate default cluster configuration; the default configuration includes a leader and two followers.
    * deploy: deploy binary to all related machine.
    * setup: initialize and start database based on default configuration.
    * dependencies: install dependent packages (use Centos as an example).
    * cm: setup cluster manager component.
    * clean: clean environment.

* check running processes (1 leader, 2 follower), their replica roles and status (change port based on polardb_paxos.conf):

```bash
ps -ef|grep polardb
psql -p 10001 -d postgres -c "select * from pg_stat_replication;"
psql -p 10001 -d postgres -c "select * from polar_dma_cluster_status;"
```

* For other modes, we can use the following command to check status.

```bash
pgxc_ctl monitor -c $HOME/polardb/polardb_paxos.conf monitor all
```

## <a name="from_source"></a>Deployment from Source Code
We extend a tool named pgxc_ctl from PG-XC/PG-XL open-source project to support cluster management, such as configuration generation, configuration modification, cluster initialization, starting/stopping nodes, and switchover. Its detail usage can be found [deployment](/doc/polardb/deployment.md).

### Compile Source Code using *build.sh*

We can use a shell script *build.sh* offered with PolarDB source code
to create and install binary locally.

```bash
sh build.sh [deploy|verify|debug|repeat]
```

* deploy：release version, default
* verify：release version，assertion enabled
* debug ：debug version
* repeat：compile source code without calling configure

for example:
```bash
$ sh build.sh # release version
```

If you get linker errors about undefined references to symbols of protobuf. then it probably
indicates that the system-installed protobuf was compiled with an older version of GCC or older
AI version, please set -c option to ON in following section of build.sh.

```bash
# build polardb consensus dynamic library
cd $CODEHOME/src/backend/polar_dma/libconsensus/polar_wrapper
if [[ "$BLD_OPT" == "debug" ]]; then
sh ./build.sh -r -t debug -c ON
else
sh ./build.sh -r -t release -c ON
fi
cd $CODEHOME
```

## Cluster Installation

### Create Cluster Configuration

We use *pgxc_ctl prepare* to generate a default cluster configuration.

```bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf prepare standalone
```

for distributed mode (2c2d)

```bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf prepare distributed
```

for distributed mode with replicas (2c2d with replicas)

```bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf prepare dispaxos
```

### Cluster Configuration Format

```bash
#!/usr/bin/env bash
#
# polardb Configuration file for pgxc_ctl utility.

pgxcInstallDir=$HOME/pghome
#---- OVERALL -----------------------------------------------------------------------------
#
pgxcOwner=$USER         # owner of the Postgres-XC databaseo cluster.  Here, we use this
                                                # both as linus user and database user.  This must be
                                                # the super user of each coordinator and datanode.
pgxcUser=$pgxcOwner             # OS user of Postgres-XC owner

tmpDir=/tmp                                     # temporary dir used in XC servers
localTmpDir=$tmpDir                     # temporary dir used here locally

standAlone=y                    # cluster version still not open source for now

dataDirRoot=$HOME/DATA/polardb/nodes

#---- Datanodes -------------------------------------------------------------------------------------------------------

#---- Shortcuts --------------
datanodeMasterDir=$dataDirRoot/dn_master
datanodeSlaveDir=$dataDirRoot/dn_slave
datanodeLearnerDir=$dataDirRoot/dn_learner
datanodeArchLogDir=$dataDirRoot/datanode_archlog

#---- Overall ---------------
primaryDatanode=datanode_1                              # Primary Node.
datanodeNames=(datanode_1)
datanodePorts=(10001)   # Master and slave use the same port!
datanodePoolerPorts=(10011)     # Master and slave use the same port!
datanodePgHbaEntries=(::1/128)  # Assumes that all the coordinator (master/slave) accepts
#datanodePgHbaEntries=(127.0.0.1/32)    # Same as above but for IPv4 connections

#---- Master ----------------
datanodeMasterServers=(localhost)       # none means this master is not available.
datanodeMasterDirs=($datanodeMasterDir)
datanodeMaxWalSender=5                     # max_wal_senders: needed to configure slave. If zero value is
datanodeMaxWALSenders=($datanodeMaxWalSender)   # max_wal_senders configuration for each datanode


#---- Slave -----------------
datanodeSlave=y                 # Specify y if you configure at least one coordiantor slave.  Otherwise, the following
datanodeSlaveServers=(localhost)        # value none means this slave is not available
datanodeSlavePorts=(10101)      # Master and slave use the same port!
datanodeSlavePoolerPorts=(10111)        # Master and slave use the same port!
datanodeSlaveSync=y             # If datanode slave is connected in synchronized mode
datanodeSlaveDirs=($datanodeSlaveDir)
datanodeArchLogDirs=( $datanodeArchLogDir)
datanodeRepNum=2  #  no HA setting 0, streaming HA and active-active logcial replication setting 1 replication,  paxos HA setting 2 replication.
datanodeSlaveType=(3) # 1 is streaming HA, 2 is active-active logcial replication, 3 paxos HA.

#---- Learner -----------------
datanodeLearnerServers=(localhost)      # value none means this learner is not available
datanodeLearnerPorts=(11001)    # learner port!
#datanodeSlavePoolerPorts=(11011)       # learner pooler port!
datanodeLearnerSync=y           # If datanode learner is connected in synchronized mode
datanodeLearnerDirs=($datanodeLearnerDir)

# ---- Configuration files ---
# You may supply your bash script to setup extra config lines and extra pg_hba.conf entries here.
datanodeExtraConfig=datanodeExtraConfig
cat > $datanodeExtraConfig <<EOF
#================================================
# Added to all the datanode postgresql.conf
# Original: $datanodeExtraConfig
log_destination = 'stderr'
logging_collector = on
log_directory = 'pg_log'
listen_addresses = '*'
max_connections = 100
hot_standby = on
max_parallel_replay_workers = 0
max_worker_processes = 30
EOF
# Additional Configuration file for specific datanode master.
# You can define each setting by similar means as above.
datanodeSpecificExtraConfig=(none)
datanodeSpecificExtraPgHba=(none)
```

More exmples can be found under the directory *contrib/pgxc_ctl/*, such as pgxc_ctl_conf_dispaxos. 

### Deploy Binary using *pgxc_ctl*
We use *pgxc_ctl deploy* command to deploy PolarDB binary in a cluster，option -c for configuration file. PolarDB binary is installed in **pgxcInstallDir** of all nodes specified in the configuration file.

```bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf deploy all
```

### Initialize Database
* Initialize database nodes and start them based on the configuration file.

```bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf clean all
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf init all
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf monitor all
```

### check and test (change port based on polardb_paxos.conf)

```bash
ps -ef | grep postgres
psql -p 10001 -d postgres -c "create table t1(a int primary key, b int);"
createdb test -p 10001
psql -p 10001 -d test -c "select version();"


### Other command for cluster manager.

* install dependent packages for cluster management (only work for standalone mode)

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

* failover datanode (only work for standalone mode)

datanode_1 is node name configured in polardb_paxos.conf.

```bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf failover datanode datanode_1
```

* cluster health check (only work for standalone mode)

 check cluster status and start failed node.

```bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf healthcheck all
```

* examples of other command

```bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf kill all
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf log var datanodeNames
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf show configuration all
```

___

Copyright © Alibaba Group, Inc.


