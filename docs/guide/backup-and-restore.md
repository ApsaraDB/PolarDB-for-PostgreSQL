本文将指导您如何对 PolarDB 做备份恢复，搭建只读节点，搭建 Standby 实例等。

首先我们先准备一个 PolarDB 实例，如何搭建 PolarDB 可以参考文档 [搭建 PolarDB](https://apsaradb.github.io/PolarDB-for-PostgreSQL/zh/guide/db-pfs.html)

注意：PolarDB 是基于共享存储的存算分离架构，因此 PolarDB 的备份恢复和 Postgres 有着许多不同。接下来我们将从以下几点来介绍 PolarDB 的备份恢复。

1. PolarDB 备份恢复原理
2. PolarDB 的目录结构
3. polar_basebackup 备份工具
4. PolarDB 搭建 RO
5. PolarDB 搭建 Standby
6. PolarDB 按时间点恢复

### PolarDB 备份恢复原理

![undefined](https://intranetproxy.alipay.com/skylark/lark/0/2022/png/135683/1656297442448-b129211a-a061-4e21-976b-1943814b0ece.png)
PolarDB 的备份恢复整体原理几乎和 Postgres 一致，总结为以下几步：
(1) 执行 pg_start_backup 命令
(2) 使用各种方式对数据库进行复制。
(3) 执行 pg_stop_backup 命令

进行备份的更简单方法是使用 polar_basebackup ，但它其实是在内部发出这些低级命令，并且支持使用网络将文件发送到远端。

- pg_start_backup
  pg_start_backup 准备进行基本备份。恢复过程从 REDO 点开始，因此 pg_start_backup 必须执行检查点以在开始进行基本备份时显式创建 REDO 点。此外，其检查点的检查点位置必须保存在 pg_control 以外的文件中，因为在备份期间可能会多次执行常规检查点。因此 pg_start_backup 执行以下四个操作：

1. 强制进入整页写模式。
2. 切换到当前的 WAL 段文件。
3. 做检查点。
4. 创建一个 backup_label 文件——该文件在基础目录的顶层创建，包含关于基础备份本身的基本信息，例如该检查点的检查点位置。
   第三和第四个操作是这个命令的核心；执行第一和第二操作以更可靠地恢复数据库集群。

- pg_stop_backup。
  pg_stop_backup 执行以下五个操作来完成备份。

1. 如果已被 pg_start_backup 强制更改，则重置为非整页写入模式。
1. 写一条备份端的 XLOG 记录。
1. 切换 WAL 段文件。
1. 创建备份历史文件——该文件包含 backup_label 文件的内容和 pg_stop_backup 已执行的时间戳。
1. 删除 backup_label 文件 – 从基本备份恢复需要 backup_label 文件，一旦复制，在原始数据库集群中就不需要了。

### PolarDB 的目录结构

如上所述，PolarDB 备份过程总体可以概括为三步，其中第二步是使用各种方式对数据库进行复制，也就是说，您可以在第二步时使用各种方式对 PolarDB 数据集簇进行复制，可以手动的 copy，也可以使用网络工具传输，也可以基于存储进行打快照。因此，这里介绍一下 PolarDB 数据目录结构，以便于进一步理解备份恢复。
![undefined](https://intranetproxy.alipay.com/skylark/lark/0/2022/png/135683/1656298647695-1dec3bcf-55d7-4389-8824-34aa4e5a2aeb.png)
如上图，PolarDB 是基于共享存储的，所以 PolarDB 在物理上有两个重要的数据目录，分别对应 Local Data （Local Dir），Shared Data（Shared Dir）。

1. Local Data （Local Dir）

```
postgres=# show data_directory;
     data_directory
------------------------
 /home/postgres/primary
(1 row)
```

可以通过上述命令在数据库中获取 Local Dir 的位置，可以看到它是类似于 Postgres 的 Data 目录。

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

LocaL Dir 目录中，大多都是通过 initdb 命令生成的文件或目录，随着数据库服务运行，这里会生成更多的本地文件，如临时文件，缓存文件，配置文件，日志文件。  
在做备份时，Local Dir 是可选的，由于 Local 文件不涉及核心的数据文件，因此您也可以不去备份这个目录，仅仅备份 Shared Dir，然后用 initdb 重新生成一份新的 Local Dir，但是需要记住之前的配置信息，如 postgresql.conf，pg_hba.conf 等。注意如果您不能记住历史配置，或者您需要保留历史日志，建议您将 Local Dir 也进行备份，同样的可以将这个目录完全 copy 后修改配置文件来搭建 RO 或者 Standby。

3. Shared Data（Shared Dir）

```
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

Shared Dir 目录中，是 PolarDB 的核心数据文件，如表文件，索引文件，wal 日志，dma，logindex，flashback 等等。
这些文件被一个 RW 和多个 RO 共享，它是必须备份的。
您可以使用 copy 命令，存储快照，网络传输方式进行备份，如果您没有更好的选择，推荐使用 polar_basebackup 命令。

### polar_basebackup 备份工具

首先我们需要介绍一下我们的备份工具 polar_basebackup，它由 [pg_basebackup](https://www.postgresql.org/docs/11/app-pgbasebackup.html) 改造而来，且完全兼容 pg_baseabckup，也就是说它同样可以用于对 Postgres 做备份恢复。
polar_basebackup 在 PolarDB 二进制安装目录下的 bin 目录中，您可以配置 export 环境变量来直接使用它。

```(英汉互译)
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

可以看到 polar_basebackup 的大多参数即用法都和 [pg_basebackup](https://www.postgresql.org/docs/11/app-pgbasebackup.html) 一致，只是多了以下几个参数，下面重点来介绍一下：

- polardata
  如果您备份的实例是 PolarDB 共享存储架构，这个参数用于指定 PolarDB 共享存储目录（Shared Dir）的位置。如果您不指定，将会使用默认目录 polar_shared_data，并且放在 Local Dir （即 -D, --pgdata 所指定的参数）下面。如果您对 Postgres 备份则无需关心他。
- polar_disk_home
  如果您备份的实例是 PolarDB 共享存储架构，且您希望将 Shared Dir 通过 pfs 写入共享存储设备，则需要指定这个参数，它是 pfs 的使用参数。
- polar_host_id
  如果您备份的实例是 PolarDB 共享存储架构，且您希望将 Shared Dir 通过 pfs 写入共享存储设备，则需要指定这个参数，它是 pfs 的使用参数。
- polar_storage_cluster_name
  如果您备份的实例是 PolarDB 共享存储架构，且您希望将 Shared Dir 通过 pfs 写入共享存储设备，则需要指定这个参数，它是 pfs 的使用参数。

### PolarDB 搭建 RO

您可以通过以下两种方式来搭建 RO node。

1. 使用 initdb 来搭建 RO
   主要步骤是使用 initdb 初始化 RO 的 Local Dir 目录，然后修改配置文件，启动实例。具体请参考[只读节点部署](https://apsaradb.github.io/PolarDB-for-PostgreSQL/zh/guide/db-pfs.html#%E5%8F%AA%E8%AF%BB%E8%8A%82%E7%82%B9%E9%83%A8%E7%BD%B2)
2. 备份 RW 的 Local Dir 来搭建 RO
   这里是将第一种方式的 initdb 初始化 RO 的 Local Dir 替换成备份 RW 的 Local Dir。
   我们这里使用 polar_basebakcup 来演示。
   `polar_basebackup --host=[主节点所在IP] --port=5432 -D /home/postgres/replica1 -X stream --progress --write-recovery-conf -v`
   ![undefined](https://intranetproxy.alipay.com/skylark/lark/0/2022/png/135683/1656301721373-90677c11-9e82-4b5b-a60d-ea2e101cf81a.png)
   完成 polar_basebackup 命令后，我们可以看到`/home/postgres/replica1` 中存在一个`polar_shared_data`, 搭建 RO 时不需要它，将它删除`rm -rf /home/postgres/replica1/polar_shared_data`
   ![undefined](https://intranetproxy.alipay.com/skylark/lark/0/2022/png/135683/1656301745881-219b7540-3190-4cfe-a717-4083d579cc3a.png)
   打开 /home/postgres/replica1/postgresql.conf，修改如下配置项：

```
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

/home/postgres/replica1/recovery.conf，使用以下配置项替换文件中的所有内容：

```
polar_replica='on'
recovery_target_timeline='latest'
primary_slot_name='replica1'
primary_conninfo='host=[主节点所在IP] port=5432 user=postgres dbname=postgres application_name=replica1'
```

最后，启动只读节点：

```
$HOME/tmp_basedir_polardb_pg_1100_bld/bin/pg_ctl start -D $HOME/replica1
```

检查只读节点能否正常运行：

```
$HOME/tmp_basedir_polardb_pg_1100_bld/bin/psql \
    -p 5433 \
    -d postgres \
    -c 'select version();'
            version
--------------------------------
 PostgreSQL 11.9 (POLARDB 11.9)
(1 row)
```

### PolarDB 搭建 Standby

您可以使用全量备份集搭建 Standby，这里推荐使用 polar_basebackup 进行搭建，下面介绍搭建流程。

1. 使用 polar_basebakcup 对实例作全量备份

```
polar_basebackup --host=[主节点所在IP] --port=5432 -D /home/postgres/standby --polardata=/nvme0n2/shared_data/  --polar_storage_cluster_name=disk --polar_disk_name=nvme0n2  --polar_host_id=3 -X stream --progress --write-recovery-conf -v
```

![undefined](https://intranetproxy.alipay.com/skylark/lark/0/2022/png/135683/1656315576998-2aaeff3e-4341-46df-bce1-eb211ea4c605.png)
注意：这里是构建共享存储的 Standby，首先您需要找一台机器部署好了 PolarDB 及其文件系统 PolarFS，且已经搭建好了共享存储`nvme0n2`, 具体操作请参考[准备块设备与搭建文件系统](https://apsaradb.github.io/PolarDB-for-PostgreSQL/zh/guide/deploy.html)
备份完成后如下：
![undefined](https://intranetproxy.alipay.com/skylark/lark/0/2022/png/135683/1656315627036-15320d4f-3711-4400-b094-b698edfa8caa.png)

注意：如果您没有共享存储设备，则您不需要指定--polar_storage_cluster_name， --polar_disk_name，polar_host_id 参数。下面我们简单介绍下其他形态的 PolarDB 备份：

```
-- 单节点本地备份
polar_basebackup -D /polardb/data-standby -X stream  --progress --write-recovery-conf -v
--共享存储本地备份
polar_basebackup -D /polardb/data-standby --polardata=/polardb/data-local  -X stream --progress --write-recovery-conf -v
-- 共享存储写入pfs
polar_basebackup -D /polardb/data-standby --polardata=/nvme7n1/data  --polar_storage_cluster_name=disk --polar_disk_name=nvme7n1  --polar_host_id=3
```

2. 检查备份是否正常查看 Local Dir 与 Shared Dir
   Local Dir
   ![undefined](https://intranetproxy.alipay.com/skylark/lark/0/2022/png/135683/1656315747873-16bf409d-d58f-4e65-b1e0-1ae0e523b863.png)
   Shared Dir
   ![undefined](https://intranetproxy.alipay.com/skylark/lark/0/2022/png/135683/1656315649979-c0731ab7-4516-4e5e-9626-93ccdd1abab4.png)
3. 修改 postgresql.conf 中的下列参数值如下所示：

```
polar_hostid = 3
polar_disk_name = 'nvme0n2'
polar_datadir = '/nvme0n2/shared_data'
polar_storage_cluster_name = 'disk'
synchronous_standby_names=''
```

4. 在主库中创建复制槽

```
psql --host=[主节点所在IP]  --port=5432 -d postgres -c 'SELECT * FROM pg_create_physical_replication_slot('standby1');'
 slot_name | lsn
-----------+-----
 standby1  |
(1 row)
```

5. 在 Standby 的 Local Dir 中 recovery.conf 文件中增加如下参数

```
recovery_target_timeline = 'latest'
primary_slot_name = 'standby1'
```

5. 启动 Standby

```
 $HOME/tmp_basedir_polardb_pg_1100_bld/bin/pg_ctl start -D $HOME/standby
```

6.  验证 Standby

```
psql --host=[master所在IP] --port=5432 -d postgres -c "create table t(t1 int primary key, t2 int);insert into t values (1, 1),(2, 3),(3, 3);"
CREATE TABLE
INSERT 0 3

psql --host=[standby所在IP] --port=5432 -d postgres -c ' select * from t;'
t1 | t2
----+----
 1 |  1
 2 |  3
 3 |  3
(3 rows)
```

### PolarDB 按时间点恢复

可以参考 Postgres 按时间点恢复[PITR](http://www.postgres.cn/docs/11/continuous-archiving.html)
![undefined](https://intranetproxy.alipay.com/skylark/lark/0/2022/png/135683/1656297698167-84bf8f58-c727-43a3-b018-f928aea6bca3.png)
