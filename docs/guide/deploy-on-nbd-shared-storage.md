# Deploy on NBD Shared Storage

::: danger
Translation needed.
:::

Network Block Device (NBD) 是一种网络协议，可以在多个主机间共享块存储设备。它是 Client-Server 的架构，因此至少需要两台物理机来部署。

以两台物理机环境为例，本小节介绍基于 NBD 共享存储的实例构建方法大体如下：

- 首先，两台主机通过 NBD 共享一个块设备；
- 然后，两台主机上均部署 PolarDB-FileSystem (PFS) 来初始化并挂载到同一个块设备；
- 最后，在两台主机上分别部署 PolarDB-for-PostgreSQL 内核，构建主节点、只读节点以形成简单的一写多读实例。

::: warning
操作系统版本要求 CentOS 7.5 及以上。以下步骤在 CentOS 7.5 上通过测试。
:::

## 安装 NBD

### 操作系统支持 NBD 驱动

::: tip
操作系统内核需要支持 NBD 内核模块，如果操作系统当前不支持该内核模块，则需要自己通过对应内核版本进行编译和加载 NBD 内核模块。
:::

- 从[CentOS 官网](https://www.centos.org/)下载对应内核源码包并解压：
  ```bash
  rpm -ihv kernel-3.10.0-862.el7.src.rpm
  cd ~/rpmbuild/SOURCES
  tar Jxvf linux-3.10.0-862.el7.tar.xz -C /usr/src/kernels/
  cd /usr/src/kernels/linux-3.10.0-862.el7/
  ```
- NBD 驱动源码路径位于：`drivers/block/nbd.c`；
- 编译操作系统内核依赖和组件：
  ```bash
  cp ../$(uname -r)/Module.symvers ./
  make menuconfig # Device Driver -> Block devices -> Set 'M' On 'Network block device support'
  make prepare && make modules_prepare && make scripts
  make CONFIG_BLK_DEV_NBD=m M=drivers/block
  ```
- 检查是否正常生成驱动：
  ```bash
  modinfo drivers/block/nbd.ko
  ```
- 拷贝、生成依赖并安装驱动：
  ```bash
  cp drivers/block/nbd.ko /lib/modules/$(uname -r)/kernel/drivers/block
  depmod -a
  modprobe nbd # 或者 modprobe -f nbd 可以忽略模块版本检查
  ```
- 检查是否安装成功：
  ```bash
  # 检查已安装内核模块
  lsmod | grep nbd
  # 如果NBD驱动已经安装，则会生成/dev/nbd*设备（例如：/dev/nbd0、/dev/nbd1等）
  ls /dev/nbd*
  ```

### 安装 NBD 软件包

```bash
yum install nbd
```

## 使用 NBD 来共享块设备

### 服务端部署

拉起 NBD 服务端，按照同步方式（`sync/flush=true`）配置，在指定端口（例如 `1921`）上监听对指定块设备（例如 `/dev/vdb`）的访问。

```bash
nbd-server -C /root/nbd.conf
```

配置文件 `/root/nbd.conf` 的内容举例如下：

```bash
[generic]
    #user = nbd
    #group = nbd
    listenaddr = 0.0.0.0
    port = 1921
[export1]
    exportname = /dev/vdb
    readonly = false
    multifile = false
    copyonwrite = false
    flush = true
    fua = true
    sync = true
```

### 客户端部署

NBD 驱动安装成功后会看到 `/dev/nbd*` 设备, 根据服务端的配置把远程块设备映射为本地的某个 NBD 设备即可：

```bash
nbd-client x.x.x.x 1921 -N export1 /dev/nbd0
# x.x.x.x是NBD服务端主机的IP地址
```

## PolarDB FileSystem 安装部署

### PFS 编译安装

参见 PFS 的 [README](https://github.com/ApsaraDB/polardb-file-system/blob/master/Readme-CN.md)

### 块设备重命名

PFS 仅支持特定字符开头的块设备进行访问，建议所有块设备访问节点都通过软链接使用相同名字访问共享块设备。例如，在 NBD 服务端主机上执行：

```bash
ln -s /dev/vdb /dev/nvme0n1
```

在 NBD 客户端主机上执行：

```bash
ln -s /dev/nbd0 /dev/nvme0n1
```

如此，便可以在服务端和客户端 2 台主机，使用相同的路径 `/dev/nvme0n1` 来访问同一个块设备。

### 块设备初始化

在 NBD 服务端执行 PFS 操作来格式化共享块设备即可：

```bash
sudo pfs -C disk mkfs nvme0n1
```

### 块设备挂载

在 NBD 服务端和客户端上，分别启动 PFS，挂载共享盘：

```bash
sudo /usr/local/polarstore/pfsd/bin/start_pfsd.sh -p nvme0n1
```

## PolarDB for PG 内核编译部署

::: warning
请使用同一个用户进行以下步骤。请勿使用 root 用户搭建实例。
:::

### 主节点部署

#### 内核编译

```bash
./polardb_build.sh --with-pfsd
```

#### 节点初始化

```bash
$HOME/tmp_basedir_polardb_pg_1100_bld/bin/initdb -D primary
# 共享存储初始化
sudo pfs -C disk mkdir /nvme0n1/shared_data
sudo /home/[$USER]/tmp_basedir_polardb_pg_1100_bld/bin/polar-initdb.sh /home/[$USER]/primary/ /nvme0n1/shared_data/
```

#### 节点配置

打开 `postgresql.conf`，增加以下配置项：

```bash
port=5432
polar_hostid=1
polar_enable_shared_storage_mode=on
polar_disk_name='nvme0n1'
polar_datadir='/nvme0n1/shared_data/'
polar_vfs.localfs_mode=off
shared_preload_libraries='$libdir/polar_vfs,$libdir/polar_worker'
polar_storage_cluster_name='disk'
logging_collector=on
log_line_prefix='%p\t%r\t%u\t%m\t'
log_directory='pg_log'
listen_addresses='*'
max_connections=1000
synchronous_standby_names='replica1'
```

打开 `pg_hba.conf`，增加以下配置项：

```bash
host	replication	[$USER]	0.0.0.0/0	trust
```

#### 启动与检查

启动：

```bash
$HOME/tmp_basedir_polardb_pg_1100_bld/bin/pg_ctl start -D $HOME/primary
```

检查：

```bash
$HOME/tmp_basedir_polardb_pg_1100_bld/bin/psql -p 5432 -d postgres -c 'select version();'
```

#### 只读节点的流复制准备

创建相应的 replication slot，用于接下来创建的只读节点的物理流复制：

```bash
$HOME/tmp_basedir_polardb_pg_1100_bld/bin/psql -p 5432 -d postgres -c "select pg_create_physical_replication_slot('replica1');"
```

### 只读节点部署

#### 内核编译

```bash
./polardb_build.sh --with-pfsd
```

#### 节点初始化

```bash
$HOME/tmp_basedir_polardb_pg_1100_bld/bin/initdb -D replica1
```

#### 节点配置

打开 `postgresql.conf`，增加以下配置项：

```bash
port=5433
polar_hostid=2
polar_enable_shared_storage_mode=on
polar_disk_name='nvme0n1'
polar_datadir='/nvme0n1/shared_data/'
polar_vfs.localfs_mode=off
shared_preload_libraries='$libdir/polar_vfs,$libdir/polar_worker'
polar_storage_cluster_name='disk'
logging_collector=on
log_line_prefix='%p\t%r\t%u\t%m\t'
log_directory='pg_log'
listen_addresses='*'
max_connections=1000
```

创建 `recovery.conf`，增加以下配置项：

```bash
polar_replica='on'
recovery_target_timeline='latest'
primary_slot_name='replica1'
primary_conninfo='host=[主节点所在容器的IP] port=5432 user=[$USER] dbname=postgres application_name=replica1'
```

#### 启动与检查

启动：

```bash
$HOME/tmp_basedir_polardb_pg_1100_bld/bin/pg_ctl start -D $HOME/replica1
```

检查：

```bash
$HOME/tmp_basedir_polardb_pg_1100_bld/bin/psql -p 5433 -d postgres -c 'select version();'
```

### 实例检查和测试

部署完成后，需要进行实例检查和测试，确保主节点可正常写入数据、只读节点可以正常读取。

登录主节点，创建测试表并插入样例数据：

```bash
$HOME/tmp_basedir_polardb_pg_1100_bld/bin/psql -q -p 5432 -d postgres -c "create table t(t1 int primary key, t2 int);insert into t values (1, 1),(2, 3),(3, 3);"
```

登录只读节点，查询刚刚插入的样例数据：

```bash
$HOME/tmp_basedir_polardb_pg_1100_bld/bin/psql -q -p 5433 -d postgres -c "select * from t;"
```
