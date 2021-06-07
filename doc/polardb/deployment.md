

## Deploy from Source Code

### Download code

git clone from one of our repositories

### OS and other dependencies

* operating systems
  * Alibaba Group Enterprise Linux Server, VERSION="7.2 (Paladin)", 3.10.0-327.ali2017.alios7.x86_64
  * Centos 7

* GCC versions 
  * gcc 10.2.1
  * gcc 9.2.1
  * gcc 7.2.1
  * gcc 4.8.5


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
ABI version, please set -c option to ON in following section of build.sh.

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
pgxc_ctl -c $HOME/DATA/polardb/polardb_paxos.conf prepare standalone
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

### Deploy Binary using *pgxc_ctl*
We use *pgxc_ctl deploy* command to deploy PolarDB binary in a cluster，option -c for configuration file. PolarDB binary is installed in **pgxcInstallDir** of all nodes specified in the configuration file. 

```bash
pgxc_ctl -c $HOME/DATA/polardb/polardb_paxos.conf deploy all
```

### Initialize Database
* Initialize database nodes and start them based on the configuration file. 

```bash
pgxc_ctl -c $HOME/DATA/polardb/polardb_paxos.conf clean all
pgxc_ctl -c $HOME/DATA/polardb/polardb_paxos.conf init all
pgxc_ctl -c $HOME/DATA/polardb/polardb_paxos.conf monitor all
```


```bash
psql -p 10001 -d postgres -c "CREATE EXTENSION pg_cron;"
psql -p 10001 -d postgres -c "CREATE EXTENSION plpythonu;"
psql -p 10001 -d postgres -f "/home/postgres/polardb/healthcheck.sql"
```

* Test database

```bash
ps -ef | grep postgre
psql -p 10001 -d postgres -c "create table t1(a int primary key, b int);"
createdb test -p 10001
psql -p 10001 -d test -c "select version();"
```

## Fast Deployment Script onekey.sh

This script uses default configuration to compile PolarDB, to deploy binary, and to start a cluster of three nodes, including a leader and two followers. 

```bash
sh onekey.sh [all|build|configure|deploy|setup]
```

* all：fulfill all deployment tasks, including compile source code，generate default cluster configuration, install cluster; after that, we can access the database cluster using *psql -p 10001 -d postgres*
* build：invoke *build.sh* script to compile and create a release version
* configure：generate default cluster configuration; the default configuration includes two nodes (leader and follower). 
* deploy：only deploy binary based on cluster configuration
* setup：initialize and start database based on default configuration

___

Copyright © Alibaba Group, Inc.


