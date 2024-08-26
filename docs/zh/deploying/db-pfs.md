---
author: 棠羽
date: 2022/05/09
minute: 15
---

# 基于 PFS 文件系统部署

<ArticleInfo :frontmatter=$frontmatter></ArticleInfo>

本文将指导您在分布式文件系统 PolarDB File System（PFS）上编译部署 PolarDB-PG，适用于已经在共享存储上格式化并挂载 PFS 文件系统的计算节点。

[[toc]]

## 读写节点部署

初始化读写节点的本地数据目录 `~/primary/`：

```bash:no-line-numbers
$HOME/tmp_basedir_polardb_pg_1100_bld/bin/initdb -D $HOME/primary
```

在共享存储的 `/nvme1n1/shared_data/` 路径上创建共享数据目录，然后使用 `polar-initdb.sh` 脚本初始化共享数据目录：

```bash:no-line-numbers
# 使用 pfs 创建共享数据目录
sudo pfs -C disk mkdir /nvme1n1/shared_data
# 初始化 db 的本地和共享数据目录
sudo $HOME/tmp_basedir_polardb_pg_1100_bld/bin/polar-initdb.sh \
    $HOME/primary/ /nvme1n1/shared_data/
```

编辑读写节点的配置。打开 `~/primary/postgresql.conf`，增加配置项：

```ini
port=5432
polar_hostid=1
polar_enable_shared_storage_mode=on
polar_disk_name='nvme1n1'
polar_datadir='/nvme1n1/shared_data/'
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

编辑读写节点的客户端认证文件 `~/primary/pg_hba.conf`，增加以下配置项，允许只读节点进行物理复制：

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
    -c 'SELECT version();'
            version
--------------------------------
 PostgreSQL 11.9 (POLARDB 11.9)
(1 row)
```

在读写节点上，为对应的只读节点创建相应的复制槽，用于只读节点的物理复制：

```bash:no-line-numbers
$HOME/tmp_basedir_polardb_pg_1100_bld/bin/psql \
    -p 5432 \
    -d postgres \
    -c "SELECT pg_create_physical_replication_slot('replica1');"
 pg_create_physical_replication_slot
-------------------------------------
 (replica1,)
(1 row)
```

## 只读节点部署

在只读节点本地磁盘的 `~/replica1` 路径上创建一个空目录，然后通过 `polar-replica-initdb.sh` 脚本使用共享存储上的数据目录来初始化只读节点的本地目录。初始化后的本地目录中没有默认配置文件，所以还需要使用 `initdb` 创建一个临时的本地目录模板，然后将所有的默认配置文件拷贝到只读节点的本地目录下：

```shell:no-line-numbers
mkdir -m 0700 $HOME/replica1
sudo ~/tmp_basedir_polardb_pg_1100_bld/bin/polar-replica-initdb.sh \
    /nvme1n1/shared_data/ $HOME/replica1/

$HOME/tmp_basedir_polardb_pg_1100_bld/bin/initdb -D /tmp/replica1
cp /tmp/replica1/*.conf $HOME/replica1/
```

编辑只读节点的配置。打开 `~/replica1/postgresql.conf`，增加配置项：

```ini
port=5433
polar_hostid=2
polar_enable_shared_storage_mode=on
polar_disk_name='nvme1n1'
polar_datadir='/nvme1n1/shared_data/'
polar_vfs.localfs_mode=off
shared_preload_libraries='$libdir/polar_vfs,$libdir/polar_worker'
polar_storage_cluster_name='disk'
logging_collector=on
log_line_prefix='%p\t%r\t%u\t%m\t'
log_directory='pg_log'
listen_addresses='*'
max_connections=1000
```

创建只读节点的复制配置文件 `~/replica1/recovery.conf`，增加读写节点的连接信息，以及复制槽名称：

```ini:no-line-numbers
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
    -c 'SELECT version();'
            version
--------------------------------
 PostgreSQL 11.9 (POLARDB 11.9)
(1 row)
```

## 集群检查和测试

部署完成后，需要进行实例检查和测试，确保读写节点可正常写入数据、只读节点可以正常读取。

登录 **读写节点**，创建测试表并插入样例数据：

```bash:no-line-numbers
$HOME/tmp_basedir_polardb_pg_1100_bld/bin/psql -q \
    -p 5432 \
    -d postgres \
    -c "CREATE TABLE t (t1 INT PRIMARY KEY, t2 INT); INSERT INTO t VALUES (1, 1),(2, 3),(3, 3);"
```

登录 **只读节点**，查询刚刚插入的样例数据：

```bash:no-line-numbers
$HOME/tmp_basedir_polardb_pg_1100_bld/bin/psql -q \
    -p 5433 \
    -d postgres \
    -c "SELECT * FROM t;"
 t1 | t2
----+----
  1 |  1
  2 |  3
  3 |  3
(3 rows)
```

在读写节点上插入的数据对只读节点可见，这意味着基于共享存储的 PolarDB 计算节点集群搭建成功。

---

## 常见运维步骤

- [备份恢复](../operation/backup-and-restore.md)
- [共享存储在线扩容](../operation/grow-storage.md)
- [计算节点扩缩容](../operation/scale-out.md)
- [只读节点在线 Promote](../operation/ro-online-promote.md)
