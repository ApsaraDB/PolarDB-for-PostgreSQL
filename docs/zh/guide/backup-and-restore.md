# PolarDB PG 备份恢复

PolarDB 是基于共享存储的存算分离架构，因此 PolarDB 的备份恢复和 PostgreSQL 存在部分差异。本文将指导您如何对 PolarDB 做备份恢复，搭建只读节点，搭建 Standby 实例等：

1. PolarDB 备份恢复原理
2. PolarDB 的目录结构
3. polar_basebackup 备份工具
4. PolarDB 搭建 RO
5. PolarDB 搭建 Standby
6. PolarDB 按时间点恢复

前置条件是先准备一个 PolarDB 实例，可以参考文档 [搭建 PolarDB](./deploy.md)。

## 备份恢复原理

![theory](https://intranetproxy.alipay.com/skylark/lark/0/2022/png/135683/1656473213509-02fd756c-a796-47e0-af3b-cb7ce7e02751.png)

PolarDB 的备份恢复原理整体上和 PostgreSQL 几乎一致，总结为以下几步：

1. 执行 `pg_start_backup` 命令
2. 使用各种方式对数据库进行复制
3. 执行 `pg_stop_backup` 命令

进行备份的更简单方法是使用 `polar_basebackup` ，但它其实是在内部发出这些低级命令，并且支持使用网络将文件发送到远端。

- `pg_start_backup`：准备进行基本备份。恢复过程从 REDO 点开始，因此 `pg_start_backup` 必须执行检查点以在开始进行基本备份时显式创建 REDO 点。此外，其检查点的检查点位置必须保存在 `pg_control` 以外的文件中，因为在备份期间可能会多次执行常规检查点。因此 `pg_start_backup` 执行以下四个操作：

  1. 强制进入整页写模式。
  2. 切换到当前的 WAL 段文件。
  3. 做检查点。
  4. 创建一个 `backup_label` 文件——该文件在基础目录的顶层创建，包含关于基础备份本身的基本信息，例如该检查点的检查点位置。
     第三和第四个操作是这个命令的核心；执行第一和第二操作以更可靠地恢复数据库集群。

- `pg_stop_backup`：执行以下五个操作来完成备份。

  1. 如果已被 `pg_start_backup` 强制更改，则重置为非整页写入模式。
  2. 写一条备份端的 XLOG 记录。
  3. 切换 WAL 段文件。
  4. 创建备份历史文件——该文件包含 `backup_label` 文件的内容和 `pg_stop_backup` 已执行的时间戳。
  5. 删除 `backup_label` 文件 – 从基本备份恢复需要 `backup_label` 文件，一旦复制，在原始数据库集群中就不需要了。

## 目录结构

如上所述，PolarDB 备份过程总体可以概括为三步，其中第二步是使用各种方式对数据库进行复制：

- 手动 copy
- 使用网络工具传输
- 基于存储进行打快照。

因此，这里介绍一下 PolarDB 数据目录结构，以便于进一步理解备份恢复。

![storage](https://intranetproxy.alipay.com/skylark/lark/0/2022/png/135683/1656470771553-e0c32f21-2ca4-43a4-a0a8-44d8a15e7358.png)

如上图，PolarDB 是基于共享存储的，所以 PolarDB 在物理上有两个重要的数据目录，分别是 **本地存储目录** 和 **共享存储目录**。

### 本地存储目录

```
postgres=# show data_directory;
     data_directory
------------------------
 /home/postgres/primary
(1 row)
```

可以通过上述命令在数据库中获取本地存储目录的位置，可以看到它是类似于 PostgreSQL 的数据目录。

```
.
├── base
│   ├── 1
│   ├── 13938
│   ├── 13939
│   └── 13940
├── global
├── pg_commit_ts
├── pg_csnlog
├── pg_dynshmem
├── pg_log
├── pg_logical
│   ├── mappings
│   └── snapshots
├── pg_logindex
├── pg_multixact
│   ├── members
│   └── offsets
├── pg_notify
├── pg_replslot
├── pg_serial
├── pg_snapshots
├── pg_stat
├── pg_stat_tmp
├── pg_subtrans
├── pg_tblspc
├── pg_xact
├── polar_cache_trash
├── polar_fullpage
└── polar_rel_size_cache
```

本地存储目录中，大多都是通过 `initdb` 命令生成的文件或目录。随着数据库服务运行，这里会生成更多的本地文件，如临时文件、缓存文件、配置文件、日志文件。

由于本地存储目录中的文件不涉及核心数据，因此在做备份时本地存储目录是可选的。您可以仅备份共享存储上的数据目录，然后用 `initdb` 重新生成一份新的本地存储目录。但是需要记住之前的本地配置信息，如 `postgresql.conf`，`pg_hba.conf` 等。

::: tip
如果您不能记住历史配置，或者您需要保留历史日志，建议您将本地存储目录也进行备份。可以将这个目录完全复制后修改配置文件来搭建 RO 或者 Standby。
:::

### 共享存储目录

```sql
postgres=# show polar_datadir;
     polar_datadir
-----------------------
 /nvme0n1/shared_data/
(1 row)
```

```
.
├── base
│   ├── 1
│   ├── 16555
│   ├── 16556
│   ├── 16557
│   └── 16558
├── global
├── pg_commit_ts
├── pg_csnlog
├── pg_logindex
├── pg_multixact
│   ├── members
│   └── offsets
├── pg_replslot
├── pg_tblspc
├── pg_twophase
├── pg_wal
│   └── archive_status
├── pg_xact
├── polar_dma
│   ├── consensus_cc_log
│   └── consensus_log
├── polar_flog
├── polar_flog_index
├── polar_fraindex
│   ├── fbpoint
│   └── pg_xact
└── polar_fullpage
```

共享存储目录中存放 PolarDB 的核心数据文件，如表文件、索引文件、WAL 日志、DMA、LogIndex、Flashback 等。这些文件被一个 RW 节点和多个 RO 节点共享，因此是必须备份的。您可以使用 `copy` 命令、存储快照、网络传输等方式进行备份。如果您没有更好的选择，推荐使用 `polar_basebackup` 命令。

## polar_basebackup 备份工具

下面介绍一下 PolarDB 的备份工具 `polar_basebackup`，它由 [`pg_basebackup`](https://www.postgresql.org/docs/11/app-pgbasebackup.html) 改造而来，且完全兼容 `pg_baseabckup`，也就是说它同样可以用于对 PostgreSQL 做备份恢复。`polar_basebackup` 在 PolarDB 二进制安装目录下的 `bin/` 目录中，您可以配置 `export` 环境变量来直接使用它。

```shell
polar_basebackup takes a base backup of a running PostgreSQL server.

Usage:
  polar_basebackup [OPTION]...

Options controlling the output:
  -D, --pgdata=DIRECTORY receive base backup into directory
  -F, --format=p|t       output format (plain (default), tar)
  -r, --max-rate=RATE    maximum transfer rate to transfer data directory
                         (in kB/s, or use suffix "k" or "M")
  -R, --write-recovery-conf
                         write recovery.conf for replication
  -T, --tablespace-mapping=OLDDIR=NEWDIR
                         relocate tablespace in OLDDIR to NEWDIR
      --waldir=WALDIR    location for the write-ahead log directory
  -X, --wal-method=none|fetch|stream
                         include required WAL files with specified method
  -z, --gzip             compress tar output
  -Z, --compress=0-9     compress tar output with given compression level

General options:
  -c, --checkpoint=fast|spread
                         set fast or spread checkpointing
  -C, --create-slot      create replication slot
  -l, --label=LABEL      set backup label
  -n, --no-clean         do not clean up after errors
  -N, --no-sync          do not wait for changes to be written safely to disk
  -P, --progress         show progress information
  -S, --slot=SLOTNAME    replication slot to use
  -v, --verbose          output verbose messages
  -V, --version          output version information, then exit
      --no-slot          prevent creation of temporary replication slot
      --no-verify-checksums
                         do not verify checksums
  -?, --help             show this help, then exit

Connection options:
  -d, --dbname=CONNSTR   connection string
  -h, --host=HOSTNAME    database server host or socket directory
  -p, --port=PORT        database server port number
  -s, --status-interval=INTERVAL
                         time between status packets sent to server (in seconds)
  -U, --username=NAME    connect as specified database user
  -w, --no-password      never prompt for password
  -W, --password         force password prompt (should happen automatically)
      --polardata=datadir  receive polar data backup into directory
      --polar_disk_home=disk_home  polar_disk_home for polar data backup
      --polar_host_id=host_id  polar_host_id for polar data backup
      --polar_storage_cluster_name=cluster_name  polar_storage_cluster_name for polar data backup
```

可以看到 `polar_basebackup` 的大部分参数及用法都和 [`pg_basebackup`](https://www.postgresql.org/docs/11/app-pgbasebackup.html) 一致，只是多了以下几个参数，下面重点来介绍一下：

- `polardata`：
  如果您备份的实例是 PolarDB 共享存储架构，这个参数用于指定 PolarDB 共享存储目录的位置。如果您不指定，将会使用默认目录 `polar_shared_data`，并且放在本地存储目录（即 `-D` / `--pgdata` 所指定的参数）下面。如果您对 PostgreSQL 备份则无需关心他。
- `polar_disk_home`：
  如果您备份的实例是 PolarDB 共享存储架构，且您希望将共享存储目录通过 PFS 写入共享存储设备，则需要指定这个参数，它是 PFS 的使用参数。
- `polar_host_id`：
  如果您备份的实例是 PolarDB 共享存储架构，且您希望将共享存储目录通过 PFS 写入共享存储设备，则需要指定这个参数，它是 PFS 的使用参数。
- `polar_storage_cluster_name`：
  如果您备份的实例是 PolarDB 共享存储架构，且您希望将共享存储目录通过 PFS 写入共享存储设备，则需要指定这个参数，它是 PFS 的使用参数。

## 搭建 RO

您可以通过以下两种方式来搭建 RO node。

### 使用 initdb 来搭建 RO

主要步骤是使用 initdb 初始化 RO 的 Local Dir 目录，然后修改配置文件，启动实例。具体请参考 [只读节点部署](./db-pfs.md#只读节点部署)。

### 备份 RW 的本地存储目录来搭建 RO

这里使用备份 RW 的本地存储目录。下面通过 `polar_basebakcup` 来演示：

```bash:no-line-numbers
polar_basebackup --host=[主节点所在IP] --port=5432 -D /home/postgres/replica1 -X stream --progress --write-recovery-conf -v
```

![undefined](https://intranetproxy.alipay.com/skylark/lark/0/2022/png/135683/1656301721373-90677c11-9e82-4b5b-a60d-ea2e101cf81a.png)

完成 `polar_basebackup` 命令后，我们可以看到 `/home/postgres/replica1` 中存在一个 `polar_shared_data`, 搭建 RO 时不需要它，将它删除：

```bash:no-line-numbers
rm -rf /home/postgres/replica1/polar_shared_data
```

![undefined](https://intranetproxy.alipay.com/skylark/lark/0/2022/png/135683/1656301745881-219b7540-3190-4cfe-a717-4083d579cc3a.png)

打开 `/home/postgres/replica1/postgresql.conf`，修改如下配置项：

```ini
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
synchronous_standby_names=''
```

打开 `/home/postgres/replica1/recovery.conf`，使用以下配置项替换文件中的所有内容：

```ini
polar_replica='on'
recovery_target_timeline='latest'
primary_slot_name='replica1'
primary_conninfo='host=[主节点所在IP] port=5432 user=postgres dbname=postgres application_name=replica1'
```

最后，启动只读节点：

```shell
$HOME/tmp_basedir_polardb_pg_1100_bld/bin/pg_ctl start -D $HOME/replica1
```

检查只读节点能否正常运行：

```shell
$HOME/tmp_basedir_polardb_pg_1100_bld/bin/psql \
    -p 5433 \
    -d postgres \
    -c 'select version();'
            version
--------------------------------
 PostgreSQL 11.9 (POLARDB 11.9)
(1 row)
```

## 搭建 Standby

您可以使用全量备份集搭建 Standby，这里推荐使用 `polar_basebackup` 进行搭建，下面介绍搭建流程。

### 使用 `polar_basebakcup` 对实例作全量备份

```shell
polar_basebackup --host=[主节点所在IP] --port=5432 -D /home/postgres/standby --polardata=/nvme0n2/shared_data/  --polar_storage_cluster_name=disk --polar_disk_name=nvme0n2  --polar_host_id=3 -X stream --progress --write-recovery-conf -v
```

![undefined](https://intranetproxy.alipay.com/skylark/lark/0/2022/png/135683/1656315576998-2aaeff3e-4341-46df-bce1-eb211ea4c605.png)

::: tip
注意：这里是构建共享存储的 Standby，首先您需要找一台机器部署好 PolarDB 及其文件系统 PolarFS，且已经搭建好了共享存储`nvme0n2`, 具体操作请参考 [准备块设备与搭建文件系统](./deploy.md)
:::

备份完成后如下图所示：

![undefined](https://intranetproxy.alipay.com/skylark/lark/0/2022/png/135683/1656315627036-15320d4f-3711-4400-b094-b698edfa8caa.png)

::: tip
如果您没有共享存储设备，则不需要指定 `--polar_storage_cluster_name`，`--polar_disk_name`，`--polar_host_id` 参数。
:::

下面我们简单介绍下其他形态的 PolarDB 备份：

```shell
-- 单节点本地备份
polar_basebackup -D /polardb/data-standby -X stream  --progress --write-recovery-conf -v
--共享存储本地备份
polar_basebackup -D /polardb/data-standby --polardata=/polardb/data-local  -X stream --progress --write-recovery-conf -v
-- 共享存储写入pfs
polar_basebackup -D /polardb/data-standby --polardata=/nvme7n1/data  --polar_storage_cluster_name=disk --polar_disk_name=nvme7n1  --polar_host_id=3
```

### 检查备份是否正常

查看本地目录：

![undefined](https://intranetproxy.alipay.com/skylark/lark/0/2022/png/135683/1656315747873-16bf409d-d58f-4e65-b1e0-1ae0e523b863.png)

查看共享存储目录：

![undefined](https://intranetproxy.alipay.com/skylark/lark/0/2022/png/135683/1656315649979-c0731ab7-4516-4e5e-9626-93ccdd1abab4.png)

### 修改 postgresql.conf

将参数修改为如下所示：

```ini
polar_hostid = 3
polar_disk_name = 'nvme0n2'
polar_datadir = '/nvme0n2/shared_data'
polar_storage_cluster_name = 'disk'
synchronous_standby_names=''
```

### 在主库中创建复制槽

```shell
psql --host=[主节点所在IP]  --port=5432 -d postgres -c 'SELECT * FROM pg_create_physical_replication_slot('standby1');'
 slot_name | lsn
-----------+-----
 standby1  |
(1 row)
```

### 修改 Standby 本地目录配置

在 Standby 的 Local Dir 中 `recovery.conf` 文件中增加如下参数：

```ini
recovery_target_timeline = 'latest'
primary_slot_name = 'standby1'
```

### 启动 Standby

```shell
$HOME/tmp_basedir_polardb_pg_1100_bld/bin/pg_ctl start -D $HOME/standby
```

### 验证 Standby

```shell
psql --host=[master所在IP] --port=5432 -d postgres -c "create table t(t1 int primary key, t2 int);insert into t values (1, 1),(2, 3),(3, 3);"
CREATE TABLE
INSERT 0 3
```

```shell
psql --host=[standby所在IP] --port=5432 -d postgres -c ' select * from t;'
t1 | t2
----+----
 1 |  1
 2 |  3
 3 |  3
(3 rows)
```

## 按时间点恢复

可以参考 PostgreSQL [按时间点恢复 PITR](http://www.postgres.cn/docs/11/continuous-archiving.html)。其原理如图所示，使用备份集加上归档日志，可以恢复出任意历史时刻的 PolarDB 实例：

![undefined](https://intranetproxy.alipay.com/skylark/lark/0/2022/png/135683/1656473229849-8b7dbf39-2830-45e1-a076-adea16c06366.png)
