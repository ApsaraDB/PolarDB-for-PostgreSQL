---
author: 程义
date: 2022/11/02
minute: 15
---

# 基于 PFS for CurveBS 文件系统部署

<ArticleInfo :frontmatter=$frontmatter></ArticleInfo>

本文将指导您在分布式文件系统 PolarDB File System（PFS）上编译部署 PolarDB，适用于已经在 Curve 块存储上格式化并挂载 PFS 的计算节点。

我们在 DockerHub 上提供了一个 [PolarDB 开发镜像](https://hub.docker.com/r/polardb/polardb_pg_devel/tags)，里面已经包含编译运行 PolarDB for PostgreSQL 所需要的所有依赖。您可以直接使用这个开发镜像进行实例搭建。镜像目前支持 AMD64 和 ARM64 两种 CPU 架构。

## 源码下载

在前置文档中，我们已经从 DockerHub 上拉取了 PolarDB 开发镜像，并且进入到了容器中。进入容器后，从 [GitHub](https://github.com/ApsaraDB/PolarDB-for-PostgreSQL) 上下载 PolarDB for PostgreSQL 的源代码，稳定分支为 `POLARDB_11_STABLE`。如果因网络原因不能稳定访问 GitHub，则可以访问 [Gitee](https://gitee.com/mirrors/PolarDB-for-PostgreSQL)。

:::: code-group
::: code-group-item GitHub

```bash:no-line-numbers
git clone -b POLARDB_11_STABLE https://github.com/ApsaraDB/PolarDB-for-PostgreSQL.git
```

:::
::: code-group-item Gitee

```bash:no-line-numbers
git clone -b POLARDB_11_STABLE https://gitee.com/mirrors/PolarDB-for-PostgreSQL
```

:::
::::

代码克隆完毕后，进入源码目录：

```bash:no-line-numbers
cd PolarDB-for-PostgreSQL/
```

## 编译部署 PolarDB

### 读写节点部署

在读写节点上，使用 `--with-pfsd` 选项编译 PolarDB 内核。请参考 [编译测试选项说明](../development/dev-on-docker.md#编译测试选项说明) 查看更多编译选项的说明。

```bash:no-line-numbers
./polardb_build.sh --with-pfsd
```

::: warning
上述脚本在编译完成后，会自动部署一个基于 **单机文件系统** 的实例，运行于 `5432` 端口上。

手动键入以下命令停止这个实例，以便 **在 PFS 和共享存储上重新部署实例**：

```bash:no-line-numbers
$HOME/tmp_basedir_polardb_pg_1100_bld/bin/pg_ctl \
    -D $HOME/tmp_master_dir_polardb_pg_1100_bld/ \
    stop
```

:::

在节点本地初始化数据目录 `$HOME/primary/`：

```bash:no-line-numbers
$HOME/tmp_basedir_polardb_pg_1100_bld/bin/initdb -D $HOME/primary
```

在共享存储的 `/pool@@volume_my_/shared_data` 目录上初始化共享数据目录

```bash:no-line-numbers
# 使用 pfs 创建共享数据目录
sudo pfs -C curve mkdir /pool@@volume_my_/shared_data
# 初始化 db 的本地和共享数据目录
sudo $HOME/tmp_basedir_polardb_pg_1100_bld/bin/polar-initdb.sh \
    $HOME/primary/ /pool@@volume_my_/shared_data/ curve
```

编辑读写节点的配置。打开 `$HOME/primary/postgresql.conf`，增加配置项：

```ini
port=5432
polar_hostid=1
polar_enable_shared_storage_mode=on
polar_disk_name='pool@@volume_my_'
polar_datadir='/pool@@volume_my_/shared_data/'
polar_vfs.localfs_mode=off
shared_preload_libraries='$libdir/polar_vfs,$libdir/polar_worker'
polar_storage_cluster_name='curve'
logging_collector=on
log_line_prefix='%p\t%r\t%u\t%m\t'
log_directory='pg_log'
listen_addresses='*'
max_connections=1000
synchronous_standby_names='replica1'
```

打开 `$HOME/primary/pg_hba.conf`，增加以下配置项：

```ini:no-line-numbers
host	replication	postgres	0.0.0.0/0	trust
```

最后，启动读写节点：

```bash:no-line-numbers
$HOME/tmp_basedir_polardb_pg_1100_bld/bin/pg_ctl start -D $HOME/primary
```

检查读写节点能否正常运行：

```bash:no-line-numbers
$HOME/tmp_basedir_polardb_pg_1100_bld/bin/psql \
    -p 5432 \
    -d postgres \
    -c 'select version();'
# 下面为输出内容
            version
--------------------------------
 PostgreSQL 11.9 (POLARDB 11.9)
(1 row)
```

在读写节点上，为对应的只读节点创建相应的 replication slot，用于只读节点的物理流复制：

```bash:no-line-numbers
$HOME/tmp_basedir_polardb_pg_1100_bld/bin/psql \
    -p 5432 \
    -d postgres \
    -c "select pg_create_physical_replication_slot('replica1');"
# 下面为输出内容
 pg_create_physical_replication_slot
-------------------------------------
 (replica1,)
(1 row)
```

### 只读节点部署

在只读节点上，使用 `--with-pfsd` 选项编译 PolarDB 内核。

```bash:no-line-numbers
./polardb_build.sh --with-pfsd
```

::: warning
上述脚本在编译完成后，会自动部署一个基于 **单机文件系统** 的实例，运行于 `5432` 端口上。

手动键入以下命令停止这个实例，以便 **在 PFS 和共享存储上重新部署实例**：

```bash:no-line-numbers
$HOME/tmp_basedir_polardb_pg_1100_bld/bin/pg_ctl \
    -D $HOME/tmp_master_dir_polardb_pg_1100_bld/ \
    stop
```

:::

在节点本地初始化数据目录 `$HOME/replica1/`：

```bash:no-line-numbers
$HOME/tmp_basedir_polardb_pg_1100_bld/bin/initdb -D $HOME/replica1
```

编辑只读节点的配置。打开 `$HOME/replica1/postgresql.conf`，增加配置项：

```ini
port=5433
polar_hostid=2
polar_enable_shared_storage_mode=on
polar_disk_name='pool@@volume_my_'
polar_datadir='/pool@@volume_my_/shared_data/'
polar_vfs.localfs_mode=off
shared_preload_libraries='$libdir/polar_vfs,$libdir/polar_worker'
polar_storage_cluster_name='curve'
logging_collector=on
log_line_prefix='%p\t%r\t%u\t%m\t'
log_directory='pg_log'
listen_addresses='*'
max_connections=1000
```

创建 `$HOME/replica1/recovery.conf`，增加以下配置项：

::: warning
请在下面替换读写节点（容器）所在的 IP 地址。
:::

```ini
polar_replica='on'
recovery_target_timeline='latest'
primary_slot_name='replica1'
primary_conninfo='host=[读写节点所在IP] port=5432 user=postgres dbname=postgres application_name=replica1'
```

最后，启动只读节点：

```bash:no-line-numbers
$HOME/tmp_basedir_polardb_pg_1100_bld/bin/pg_ctl start -D $HOME/replica1
```

检查只读节点能否正常运行：

```bash:no-line-numbers
$HOME/tmp_basedir_polardb_pg_1100_bld/bin/psql \
    -p 5433 \
    -d postgres \
    -c 'select version();'
# 下面为输出内容
            version
--------------------------------
 PostgreSQL 11.9 (POLARDB 11.9)
(1 row)
```

### 集群检查和测试

部署完成后，需要进行实例检查和测试，确保读写节点可正常写入数据、只读节点可以正常读取。

登录 **读写节点**，创建测试表并插入样例数据：

```bash:no-line-numbers
$HOME/tmp_basedir_polardb_pg_1100_bld/bin/psql -q \
    -p 5432 \
    -d postgres \
    -c "create table t(t1 int primary key, t2 int);insert into t values (1, 1),(2, 3),(3, 3);"
```

登录 **只读节点**，查询刚刚插入的样例数据：

```bash:no-line-numbers
$HOME/tmp_basedir_polardb_pg_1100_bld/bin/psql -q \
    -p 5433 \
    -d postgres \
    -c "select * from t;"
# 下面为输出内容
 t1 | t2
----+----
  1 |  1
  2 |  3
  3 |  3
(3 rows)
```

在读写节点上插入的数据对只读节点可见。
