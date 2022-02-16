## 基于pgxc_ctl和pg_cron的集群管理器

PolarDB PostgreSQL（以下简称PolarDB）高可用版本支持基于双机热备高可用模型的Paxos三节点架构、流式计算高可用和逻辑复制。集群版和单节点版基于上述高可用模型，以保持系统的高可用性。

pgxc_ctl是一个基于pgxc的集群管理工具，主要用于管理pgxc集群，如部署集群、配置集群、初始化集群、启动集群、停止集群、清理集群等。PolarDB扩展支持Paxos三节点架构和基于双机热备的逻辑复制。同时，PolarDB提供单节点版和pgxc集群管理器。使用pgxl的客户能轻松使用PolarDB集群版和单节点版。

pg_cron是Citus提供的一个基于时间的作业调度程序，主要用于集群心跳管理。pg_cron可以自动调用healthcheck命令来检查集群的运行状况，并在需要时自动进行主备切换。


## 使用方法

### 使用源代码部署PolarDB数据库

您可以使用pgxc_ctl来管理集群，如配置集群、更新集群配置、初始化集群、启动或停止节点、容灾切换等。pgxc_ctl是基于Postgres-XC/Postgres-XL（PG-XC/PG-XL）开源项目的集群管理工具。pgxc_ctl的使用方法，请参见[部署](deployment.md)。

* 下载源代码。
* 安装依赖包。以下以CentOS系统为例。

```bash
sudo yum install libzstd-devel libzstd zstd cmake openssl-devel protobuf-devel readline-devel libxml2-devel libxslt-devel zlib-devel bzip2-devel lz4-devel snappy-devel
```
* 设置无密码SSH登录。
   调用ssh-copy-id命令配置SSH，以便在使用pgxc_ctl时，不需要输入密码。

```bash
ssh-copy-id username@IP
```

* 构建和安装二进制包。

```bash
./configure --prefix=/home/postgres/polardb/polardbhome
make
make install
cd contrib
make
```

您也可以使用build.sh脚本来构建安装包。

```bash
./build.sh
```

* 设置环境变量。

```bash
vi ~/.bashrc
export PATH="$HOME/polardb/polardbhome/bin:$PATH"
export LD_LIBRARY_PATH="$HOME/polardb/polardbhome/lib:$LD_LIBRARY_PATH"ß
```

* 生成默认配置文件。

```bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf prepare standalone
```

* 部署二进制文件。

```bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf deploy all
```

* 清除安装包残留并初始化集群。

```bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf clean all
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf init all
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf monitor all
```

* 安装集群管理依赖包。

```bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf deploy cm
```

* 启动集群或节点。

```bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf start all
```

* 停止集群或节点。

```bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf stop all
```

* 切换故障节点。

以下示例展示如何在polardb_paxos.conf文件中配置datanode_1为故障切换节点。

```bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf failover datanode datanode_1
```

* 检查集群健康状况。

检查集群状态。

```bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf healthcheck all
```

* 其他命令示例。

```bash
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf kill all
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf log var datanodeNames
pgxc_ctl -c $HOME/polardb/polardb_paxos.conf show configuration all
```

* 检查和测试。

```bash
ps -ef | grep postgres
psql -p 10001 -d postgres -c "create table t1(a int primary key, b int);"
createdb test -p 10001
psql -p 10001 -d test -c "select version();"
```

___

 © 阿里巴巴集团控股有限公司 版权所有
