---
author: digoal
date: 2023/02/03
minute: 25
---

# PolarDB 读写分离连接池配置 - pgpool-II

<ArticleInfo :frontmatter=$frontmatter></ArticleInfo>

## 背景

PolarDB 的云原生存算分离架构, 具备低廉的数据存储、高效扩展弹性、高速多机并行计算能力、高速数据搜索和处理; PolarDB 与计算算法结合, 将实现双剑合璧, 推动业务数据的价值产出, 将数据变成生产力.

本文将介绍 PolarDB 开源版 使用 pgpool-II 实现透明读写分离.

- pgpool-II 是 PostgreSQL 读写分离中间件, 由于 PolarDB 是计算存储分离架构, 和 aws aurora 一样, 只需要配置 pgpool 的负载均衡, 不需要配置它 ha 功能.
- ha 功能建议采用 polardb 开源生态产品, 例如乘数科技的集群管理软件, 配置 pgpool 时使用 rw, ro 节点对应的 vip 即可(vip 由乘数的集群管理软件来管理).

测试环境为 macOS+docker, PolarDB 部署请参考下文:

- [《如何用 PolarDB 证明巴菲特的投资理念 - 包括 PolarDB 简单部署》](https://github.com/digoal/blog/blob/master/202209/20220908_02.md)

## 部署 pgpool-II

```
cd ~
wget https://www.pgpool.net/mediawiki/download.php?f=pgpool-II-4.4.1.tar.gz -O pgpool-II-4.4.1.tar.gz

tar -zxvf pgpool-II-4.4.1.tar.gz

cd pgpool-II-4.4.1

./configure --prefix=/usr/local/pgpool4.4.1 --with-openssl

make -j 8
sudo make install
```

配置动态库和默认路径

```
sudo vi /etc/ld.so.conf
# addd
/usr/local/pgpool4.4.1/lib

sudo ldconfig




vi ~/.bash_profile
# add
export PATH=/usr/local/pgpool4.4.1/bin:$PATH

. ~/.bash_profile
```

## 配置 pgpool-II

polardb 3 节点配置如下:

```
[postgres@1373488a35ab ~]$ netstat -anp|grep LISTEN
tcp        0      0 0.0.0.0:5434            0.0.0.0:*               LISTEN      72/postgres
tcp        0      0 0.0.0.0:5432            0.0.0.0:*               LISTEN      9/postgres
tcp        0      0 0.0.0.0:5433            0.0.0.0:*               LISTEN      33/postgres
tcp6       0      0 :::5434                 :::*                    LISTEN      72/postgres
tcp6       0      0 :::5432                 :::*                    LISTEN      9/postgres
tcp6       0      0 :::5433                 :::*                    LISTEN      33/postgres
unix  2      [ ACC ]     STREAM     LISTENING     22905    9/postgres           ./.s.PGSQL.5432
unix  2      [ ACC ]     STREAM     LISTENING     18212    33/postgres          ./.s.PGSQL.5433
unix  2      [ ACC ]     STREAM     LISTENING     24071    72/postgres          ./.s.PGSQL.5434


[postgres@1373488a35ab ~]$ psql -p 5432 -c "select pg_is_in_recovery();"
 pg_is_in_recovery
-------------------
 f
(1 row)

[postgres@1373488a35ab ~]$ psql -p 5433 -c "select pg_is_in_recovery();"
 pg_is_in_recovery
-------------------
 t
(1 row)

[postgres@1373488a35ab ~]$ psql -p 5434 -c "select pg_is_in_recovery();"
 pg_is_in_recovery
-------------------
 t
(1 row)
```

polardb 与 aurora 类似, 共享存储集群模式, 无需 pgpool 来管理 HA.

https://www.pgpool.net/docs/latest/en/html/example-aurora.html

配置 pgpool.conf

```
cd /usr/local/pgpool4.4.1/etc

sudo vi pgpool.conf

listen_addresses = '0.0.0.0'
port = 9999
unix_socket_directories = '/tmp'
pcp_listen_addresses = 'localhost'
pcp_port = 9898
pcp_socket_dir = '/tmp'
log_destination = 'stderr'
logging_collector = on
log_directory = '/tmp/pgpool_logs'
pid_file_name = '/var/run/pgpool/pgpool.pid'
logdir = '/tmp'

backend_clustering_mode = 'streaming_replication'
load_balance_mode = on
sr_check_period = 0
health_check_period = 0
failover_on_backend_shutdown=off
failover_on_backend_error=off
enable_pool_hba = on

backend_hostname0 = '127.0.0.1'
backend_port0 = '5432'
backend_weight0 = 1
backend_application_name0 = 'polardb_primray'
backend_flag0 = 'ALWAYS_PRIMARY|DISALLOW_TO_FAILOVER'

backend_hostname1 = '127.0.0.1'
backend_port1 = '5433'
backend_weight1 = 2
backend_application_name1 = 'polardb_reader1'
backend_flag1 = 'DISALLOW_TO_FAILOVER'

backend_hostname2 = '127.0.0.1'
backend_port2 = '5434'
backend_weight2 = 2
backend_application_name2 = 'polardb_reader2'
backend_flag2 = 'DISALLOW_TO_FAILOVER'
```

配置 pool_hba.conf

```
sudo vi pool_hba.conf
# add
host all all 0.0.0.0/0 md5
```

配置 pgpool 数据库用户密码文件 pool_passwd

```
[postgres@1373488a35ab etc]$ sudo pg_md5 --md5auth --username=digoal pwd123

[postgres@1373488a35ab etc]$ cat /usr/local/pgpool4.4.1/etc/pool_passwd
digoal:md531a770cec82aa37e217bb6e46c3f9d55



-- 实际上就是pwd+username的md5值
postgres=# select md5('pwd123digoal');
               md5
----------------------------------
 31a770cec82aa37e217bb6e46c3f9d55
(1 row)
```

在数据库中创建相应用户

```
postgres=# create user digoal superuser encrypted password 'pwd123' login;
CREATE ROLE
```

配置 pcp 管理用户密码文件 pcp.conf

```
postgres=# select md5('pwd123');
               md5
----------------------------------
 45cb41b32dcfb917ccd8614f1536d6da
(1 row)



cd /usr/local/pgpool4.4.1/etc
sudo vi pcp.conf
pcpadm:45cb41b32dcfb917ccd8614f1536d6da
```

准备 pgpool 运行时 pid 文件目录和日志目录

```
sudo mkdir /var/run/pgpool
sudo mkdir /tmp/pgpool_logs
```

启动 pgpool

```
sudo pgpool
```

查看 pgpool 监听

```
[postgres@1373488a35ab pgpool_logs]$ netstat -anp|grep LISTE
(Not all processes could be identified, non-owned process info
 will not be shown, you would have to be root to see it all.)
tcp        0      0 0.0.0.0:9999            0.0.0.0:*               LISTEN      -
tcp        0      0 0.0.0.0:5434            0.0.0.0:*               LISTEN      72/postgres
tcp        0      0 0.0.0.0:5432            0.0.0.0:*               LISTEN      9/postgres
tcp        0      0 0.0.0.0:5433            0.0.0.0:*               LISTEN      33/postgres
tcp        0      0 127.0.0.1:9898          0.0.0.0:*               LISTEN      -
tcp6       0      0 :::5434                 :::*                    LISTEN      72/postgres
tcp6       0      0 :::5432                 :::*                    LISTEN      9/postgres
tcp6       0      0 :::5433                 :::*                    LISTEN      33/postgres
unix  2      [ ACC ]     STREAM     LISTENING     22905    9/postgres           ./.s.PGSQL.5432
unix  2      [ ACC ]     STREAM     LISTENING     18212    33/postgres          ./.s.PGSQL.5433
unix  2      [ ACC ]     STREAM     LISTENING     24071    72/postgres          ./.s.PGSQL.5434
unix  2      [ ACC ]     STREAM     LISTENING     30964    -                    /tmp/.s.PGSQL.9999
unix  2      [ ACC ]     STREAM     LISTENING     30967    -                    /tmp/.s.PGSQL.9898
```

使用 pcp 管理命令查看 pgpool 中间件状态

```
pcp_node_info -U pcpadm -p 9898
Password:
127.0.0.1 5432 2 0.200000 up unknown primary unknown 0 none none 2023-01-02 03:44:20
127.0.0.1 5433 2 0.400000 up unknown standby unknown 0 none none 2023-01-02 03:44:20
127.0.0.1 5434 2 0.400000 up unknown standby unknown 0 none none 2023-01-02 03:44:20
```

```
[postgres@1373488a35ab etc]$ pcp_node_count -U pcpadm -p 9898
Password:
3
```

```
pcp_pool_status  -U pcpadm -h localhost -p 9898
Password:
...
name : backend_application_name1
value: polardb_reader1
desc : application_name for backend #1

name : backend_hostname2
value: 127.0.0.1
desc : backend #2 hostname

name : backend_port2
value: 5434
desc : backend #2 port number

name : backend_weight2
value: 0.400000
desc : weight of backend #2

name : backend_flag2
value: DISALLOW_TO_FAILOVER
desc : backend #2 flag
...
```

使用 pgpool 代理链接 polardb

```
export PGPASSWORD=pwd123
export PGDATABASE=postgres
psql -p 9999 -U digoal -c "select * from pg_stat_activity where pid=pg_backend_pid();"
```

## 测试 pgpool 读写分离

```
pgbench -i -s 1 -h 127.0.0.1 -p 9999 -U digoal postgres
```

```
pgbench -n -r -P 1 -c 8 -j 8 -T 10 -S -h 127.0.0.1 -p 9999 -U digoal postgres





[postgres@1373488a35ab ~]$ psql -p 5432 -c "select count(*) from pg_stat_activity where application_name='pgbench';"
 count
-------
     8
(1 row)

[postgres@1373488a35ab ~]$ psql -p 5433 -c "select count(*) from pg_stat_activity where application_name='pgbench';"
 count
-------
     8
(1 row)

[postgres@1373488a35ab ~]$ psql -p 5434 -c "select count(*) from pg_stat_activity where application_name='pgbench';"
 count
-------
     8
(1 row)
```

## 参考

https://www.pgpool.net/docs/latest/en/html/index.html
