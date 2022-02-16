# 部署概述

PolarDB PostgreSQL（以下简称PolarDB）支持两种部署方式：

- [一键部署](#一键部署)提供部署脚本。用户仅运行一个命令即可创建新的默认环境。
- [源码部署](#源码部署)详细介绍了部署命令及脚本的使用方法。

两种部署方式都有前提条件，详见[准备工作](#准备工作)。

- [准备工作](#准备工作)
- [一键部署](#一键部署)
- [源码部署](#源码部署)

# 准备工作

## 开始之前

* 下载源代码。下载地址为：<https://github.com/alibaba/PolarDB-for-PostgreSQL>。

* 安装依赖包（以CentOS为例）。

```bash
sudo yum install bison flex libzstd-devel libzstd zstd cmake openssl-devel protobuf-devel readline-devel libxml2-devel libxslt-devel zlib-devel bzip2-devel lz4-devel snappy-devel python-devel
```

* 设置授权密钥以便快速访问。

调用ssh-copy-id命令配置SSH，以便使用pgxc_ctl时不需要输入密码。

```bash
ssh-copy-id username@IP
```

* 设置环境变量。

```bash
vi ~/.bashrc
export PATH="$HOME/polardb/polardbhome/bin:$PATH"
export LD_LIBRARY_PATH="$HOME/polardb/polardbhome/lib:$LD_LIBRARY_PATH"
```

## 操作系统和其他依赖

* 操作系统
   * 阿里巴巴集团企业Linux服务器，版本为"7.2 (Paladin)", 3.10.0-327.ali2017.alios7.x86_64
   * CentOS 7

* GCC版本
   * GCC 10.2.1
   * GCC 9.2.1
   * GCC 7.2.1
   * GCC 4.8.5


## 一键部署

该脚本使用默认配置来编译PolarDB，采用二进制部署并启动一个集群。该集群由三个节点组成，包括一个Leader和两个Follower。在调用此脚本之前，请检查您的环境变量、依赖包和授权密钥。详情请参见[准备工作](#准备工作)中的[开始之前](#开始之前)。

* 运行onekey.sh脚本。

```bash
./onekey.sh all
```

* onekey.sh脚本介绍

```bash
sh onekey.sh [all|build|configure|deploy|setup|dependencies|cm|clean]
```

    * all：使用默认配置来编译PolarDB，采用二进制部署并启动一个集群。该集群由三个节点组成，包括一个Leader和两个Follower。
    * build：调用*build.sh*脚本编译PolarDB并创建发布版本。
    * configure：生成默认集群配置。默认配置包括一个Leader和两个Follower。
    * deploy：将二进制文件部署到所有相关的机器上。
    * setup：根据默认配置初始化和启动数据库。
    * dependencies：安装依赖包（以CentOS为例）。
    * cm：设置集群管理器（Cluster Manager）组件。
    * clean：清理环境。

* 检查正在运行的进程（1个Leader，2个Follower）、其副本角色以及状态。

```bash
ps -ef|grep polardb
psql -p 10001 -d postgres -c "select * from pg_stat_replication;"
psql -p 10001 -d postgres -c "select * from polar_dma_cluster_status;"
```


# 源码部署
我们基于PG-XC/PG-XL开源项目扩展了一个名为pgxc_ctl的工具，用来支持集群管理，如生成配置、修改配置、集群初始化、启动/停止节点和故障切换。关于该工具的详细使用方法，请参见[基于pgxc_ctl和pg_cron的集群管理器](cluster_manager.md)。

## 使用*build.sh*编译源代码

我们可以使用shell脚本*build.sh*在本地创建和安装二进制文件。该脚本提供了PolarDB源代码。

```bash
sh build.sh [deploy|verify|debug|repeat]
```

* deploy（默认值）：发布版本
* verify：发布版本，已启用断言
* debug：debug版本
* repeat：不调用configure，编译源代码

例如：
```bash
$ sh build.sh # 发布版本
```

如果碰到了关于protobuf符号未定义引用的链接器错误，很可能是因为系统安装的protobuf是使用较老版本的GCC或较老的AI版本编译的。请将-c选项设置为ON，并将以下内容添加到*build.sh*中。

```bash
# 构建PolarDB共识动态库
cd $CODEHOME/src/backend/polar_dma/libconsensus/polar_wrapper
if [[ "$BLD_OPT" == "debug" ]]; then
sh ./build.sh -r -t debug -c ON
else
sh ./build.sh -r -t release -c ON
fi
cd $CODEHOME
```

# 集群安装

## 创建集群配置

使用*pgxc_ctl prepare*生成默认的集群配置。

```bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf prepare standalone
```

## 集群配置格式

```bash
#!/usr/bin/env bash
#
# pgxc_ctl工具的PolarDB配置文件

pgxcInstallDir=$HOME/pghome
#---- 总体 -----------------------------------------------------------------------------
#
pgxcOwner=$USER         # Postgres-XC数据库集群的所有者。此处同时用作Linux用户和数据库用户。必须是每个协调器和数据节点的超级用户。
pgxcUser=$pgxcOwner             # Postgres-XC所有者的OS用户

tmpDir=/tmp                                     # XC服务器中使用的临时目录
localTmpDir=$tmpDir                     # 此处本地使用的临时目录

standAlone=y                    # 集群版本目前尚未开源

dataDirRoot=$HOME/DATA/polardb/nodes

#---- 数据节点 -------------------------------------------------------------------------------------------------------

#---- 快捷方式 --------------
datanodeMasterDir=$dataDirRoot/dn_master
datanodeSlaveDir=$dataDirRoot/dn_slave
datanodeLearnerDir=$dataDirRoot/dn_learner
datanodeArchLogDir=$dataDirRoot/datanode_archlog

#---- 总体 ---------------
primaryDatanode=datanode_1                              # 主节点
datanodeNames=(datanode_1)
datanodePorts=(10001)   # master和slave使用同一端口
datanodePoolerPorts=(10011)     # master和slave使用同一端口
datanodePgHbaEntries=(::1/128)  # 假定所有协调器（master/slave）都接受
#datanodePgHbaEntries=(127.0.0.1/32)    # 同上但用于IPv4连接

#---- Master ----------------
datanodeMasterServers=(localhost)       # 若无表示该master不可用
datanodeMasterDirs=($datanodeMasterDir)
datanodeMaxWalSender=5                     # max_wal_senders：需要配置slave。如果指定该参数为0，应由以下参数指定的外部文件显式提供该参数。如果不配置slave，请将该参数的取值保留为0。
datanodeMaxWALSenders=($datanodeMaxWalSender)   # 每个数据节点的max_wal_senders配置


#---- Slave -----------------
datanodeSlave=y                 # 如果配置至少一个slave协调器，请指定y。否则，下列配置参数将设为空值。
datanodeSlaveServers=(localhost)        # 若无表示该slave不可用
datanodeSlavePorts=(10101)      # master和slave使用同一端口
datanodeSlavePoolerPorts=(10111)        # master和slave使用同一端口
datanodeSlaveSync=y             # 如果slave数据节点以同步模式连接
datanodeSlaveDirs=($datanodeSlaveDir)
datanodeArchLogDirs=( $datanodeArchLogDir)
datanodeRepNum=2  #  无HA设为0，流式HA和双机热备逻辑复制设为1复制，Paxos HA设为2复制
datanodeSlaveType=(3) # 1表示流式HA，2表示双机热备逻辑复制，3表示Paxos HA

#---- Learner -----------------
datanodeLearnerServers=(localhost)      # 若无表示该learner不可用
datanodeLearnerPorts=(11001)    # learner端口
#datanodeSlavePoolerPorts=(11011)       # learner pooler端口
datanodeLearnerSync=y           # 如果learner数据节点以同步模式连接
datanodeLearnerDirs=($datanodeLearnerDir)

# ---- 配置文件 ---
# 您需要提供bash脚本来设置此处的配置行和pg_hba.conf条目。
datanodeExtraConfig=datanodeExtraConfig
cat > $datanodeExtraConfig <<EOF
#================================================
# 添加到所有数据节点的postgresql.conf中。
# 原始：$datanodeExtraConfig
log_destination = 'stderr'
logging_collector = on
log_directory = 'pg_log'
listen_addresses = '*'
max_connections = 100
hot_standby = on
max_parallel_replay_workers = 0
max_worker_processes = 30
EOF
# 指定master数据节点的附加配置文件。
# 您可以使用上述类似方法定义每个设置。
datanodeSpecificExtraConfig=(none)
datanodeSpecificExtraPgHba=(none)
```

## 使用*pgxc_ctl*部署二进制文件
运行*pgxc_ctl deploy*命令在集群中部署PolarDB二进制文件，通过-c选项指定配置文件。PolarDB二进制文件安装在配置文件中指定的所有节点的**pgxcInstallDir**下。

```bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf deploy all
```

## 初始化数据库
* 根据配置文件初始化并启动数据库节点。

```bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf clean all
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf init all
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf monitor all
```

## 检查和测试

```bash
ps -ef | grep postgres
psql -p 10001 -d postgres -c "create table t1(a int primary key, b int);"
createdb test -p 10001
psql -p 10001 -d test -c "select version();"
```

## 集群管理器的其他命令

* 安装依赖包进行集群管理。

``````bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf deploy cm
``````

* 启动集群或节点。

```bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf start all
```

* 停止集群或节点。

```bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf stop all
```

* 切换故障节点。

以下命令中，datanode_1表示polardb_paxos.conf文件中配置的节点名称。

```bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf failover datanode datanode_1
```

* 检查集群健康状况。

检查集群状态。

```bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf healthcheck all
```

* 其他命令的示例

```bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf kill all
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf log var datanodeNames
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf show configuration all
```

___

© 阿里巴巴集团控股有限公司 版权所有

