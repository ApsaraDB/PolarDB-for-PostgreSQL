---
author: digoal
date: 2023/02/03
minute: 20
---

# PolarDB 读写分离连接池配置 - pgcat

<ArticleInfo :frontmatter=$frontmatter></ArticleInfo>

## 背景

PolarDB 开源数据库支持云原生存算分离分布式架构, 一份存储支持多个计算节点, 目前是一写多读的架构. 内核已经很强大了, 怎么实现业务透明的读写分离, 还缺一个连接池, pgcat 是不错的选择.

pgcat 支持连接池、sharding、读写负载均衡等, 更多功能请参考其官网 https://github.com/levkk/pgcat

## pgcat 部署例子

这个例子直接在 PolarDB 容器中部署 pgcat.

PolarDB 部署请参考:

- [《如何用 PolarDB 证明巴菲特的投资理念 - 包括 PolarDB 简单部署》](https://github.com/digoal/blog/blob/master/202209/20220908_02.md)

进入 PolarDB 环境

```
docker exec -it 67e1eed1b4b6 bash
```

安装 rust

```
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

source "$HOME/.cargo/env"
```

下载 pgcat

```
git clone --depth=1 https://github.com/levkk/pgcat
```

安装 pgcat

```
cd pgcat/


cargo build --release
```

配置 pgcat, 修改为 PolarDB 数据库连接串

```
vi pgcat.toml

pgcat.toml
#
# PgCat config example.
#

#
# General pooler settings
[general]
# What IP to run on, 0.0.0.0 means accessible from everywhere.
host = "0.0.0.0"

# Port to run on, same as PgBouncer used in this example.
port = 6432

# Whether to enable prometheus exporter or not.
enable_prometheus_exporter = true

# Port at which prometheus exporter listens on.
prometheus_exporter_port = 9930

# How long to wait before aborting a server connection (ms).
connect_timeout = 5000

# How much time to give the health check query to return with a result (ms).
healthcheck_timeout = 1000

# How long to keep connection available for immediate re-use, without running a healthcheck query on it
healthcheck_delay = 30000

# How much time to give clients during shutdown before forcibly killing client connections (ms).
shutdown_timeout = 60000

# For how long to ban a server if it fails a health check (seconds).
ban_time = 60 # seconds

# If we should log client connections
log_client_connections = false

# If we should log client disconnections
log_client_disconnections = false

# Reload config automatically if it changes.
autoreload = false

# TLS
# tls_certificate = "server.cert"
# tls_private_key = "server.key"

# Credentials to access the virtual administrative database (pgbouncer or pgcat)
# Connecting to that database allows running commands like `SHOW POOLS`, `SHOW DATABASES`, etc..
admin_username = "admin_user"
admin_password = "admin_pass"

# pool
# configs are structured as pool.<pool_name>
# the pool_name is what clients use as database name when connecting
# For the example below a client can connect using "postgres://sharding_user:sharding_user@pgcat_host:pgcat_port/sharded_db"
[pools.sharded_db]
# Pool mode (see PgBouncer docs for more).
# session: one server connection per connected client
# transaction: one server connection per client transaction
pool_mode = "transaction"

# If the client doesn't specify, route traffic to
# this role by default.
#
# any: round-robin between primary and replicas,
# replica: round-robin between replicas only without touching the primary,
# primary: all queries go to the primary unless otherwise specified.
default_role = "any"

# Query parser. If enabled, we'll attempt to parse
# every incoming query to determine if it's a read or a write.
# If it's a read query, we'll direct it to a replica. Otherwise, if it's a write,
# we'll direct it to the primary.
query_parser_enabled = true

# If the query parser is enabled and this setting is enabled, the primary will be part of the pool of databases used for
# load balancing of read queries. Otherwise, the primary will only be used for write
# queries. The primary can always be explicitly selected with our custom protocol.
primary_reads_enabled = true

# So what if you wanted to implement a different hashing function,
# or you've already built one and you want this pooler to use it?
#
# Current options:
#
# pg_bigint_hash: PARTITION BY HASH (Postgres hashing function)
# sha1: A hashing function based on SHA1
#
sharding_function = "pg_bigint_hash"

# Automatically parse this from queries and route queries to the right shard!
automatic_sharding_key = "id"

# Credentials for users that may connect to this cluster
[pools.sharded_db.users.0]
username = "sharding_user"
password = "sharding_user"
# Maximum number of server connections that can be established for this user
# The maximum number of connection from a single Pgcat process to any database in the cluster
# is the sum of pool_size across all users.
pool_size = 9

# Maximum query duration. Dangerous, but protects against DBs that died in a non-obvious way.
statement_timeout = 0

[pools.sharded_db.users.1]
username = "other_user"
password = "other_user"
pool_size = 21
statement_timeout = 15000

# Shard 0
[pools.sharded_db.shards.0]
# [ host, port, role ]
servers = [
    [ "127.0.0.1", 5432, "primary" ],
    [ "localhost", 5433, "replica" ],
    [ "localhost", 5434, "replica" ]
]
# Database name (e.g. "postgres")
database = "shard0"

[pools.sharded_db.shards.1]
servers = [
    [ "127.0.0.1", 5432, "primary" ],
    [ "localhost", 5433, "replica" ],
    [ "localhost", 5434, "replica" ]
]
database = "shard1"

[pools.sharded_db.shards.2]
servers = [
    [ "127.0.0.1", 5432, "primary" ],
    [ "localhost", 5433, "replica" ],
    [ "localhost", 5434, "replica" ]
]
database = "shard2"


[pools.simple_db]
pool_mode = "transaction"
default_role = "primary"
query_parser_enabled = true
primary_reads_enabled = true
sharding_function = "pg_bigint_hash"

[pools.simple_db.users.0]
username = "simple_user"
password = "simple_user"
pool_size = 5
statement_timeout = 0

[pools.simple_db.shards.0]
servers = [
    [ "127.0.0.1", 5432, "primary" ],
    [ "localhost", 5433, "replica" ],
    [ "localhost", 5434, "replica" ]
]
database = "some_db"
```

直接连接 polardb primary, 安装 pgcat 配置文件中设置的数据库、路由关系等.

```
psql -h 127.0.0.1 -f tests/sharding/query_routing_setup.sql
```

启动 pgcat

```
[postgres@67e1eed1b4b6 pgcat]$ RUST_LOG=info cargo run --release
    Finished release [optimized] target(s) in 0.12s
     Running `target/release/pgcat`
[2022-12-02T06:30:23.280411Z INFO  pgcat] Welcome to PgCat! Meow. (Version 0.6.0-alpha1)
[2022-12-02T06:30:23.284527Z INFO  pgcat] Running on 0.0.0.0:6432
[2022-12-02T06:30:23.284572Z INFO  pgcat::config] Ban time: 60s
[2022-12-02T06:30:23.284580Z INFO  pgcat::config] Healthcheck timeout: 1000ms
[2022-12-02T06:30:23.284585Z INFO  pgcat::config] Connection timeout: 5000ms
[2022-12-02T06:30:23.284590Z INFO  pgcat::config] Log client connections: false
[2022-12-02T06:30:23.284594Z INFO  pgcat::config] Log client disconnections: false
[2022-12-02T06:30:23.284599Z INFO  pgcat::config] Shutdown timeout: 60000ms
[2022-12-02T06:30:23.284603Z INFO  pgcat::config] Healthcheck delay: 30000ms
[2022-12-02T06:30:23.284608Z INFO  pgcat::config] TLS support is disabled
[2022-12-02T06:30:23.284613Z INFO  pgcat::config] [pool: sharded_db] Maximum user connections: 30
[2022-12-02T06:30:23.284618Z INFO  pgcat::config] [pool: sharded_db] Pool mode: Transaction
[2022-12-02T06:30:23.284624Z INFO  pgcat::config] [pool: sharded_db] Connection timeout: 5000ms
[2022-12-02T06:30:23.284629Z INFO  pgcat::config] [pool: sharded_db] Sharding function: pg_bigint_hash
[2022-12-02T06:30:23.284634Z INFO  pgcat::config] [pool: sharded_db] Primary reads: true
[2022-12-02T06:30:23.284804Z INFO  pgcat::config] [pool: sharded_db] Query router: true
[2022-12-02T06:30:23.284879Z INFO  pgcat::config] [pool: sharded_db] Number of shards: 3
[2022-12-02T06:30:23.285156Z INFO  pgcat::config] [pool: sharded_db] Number of users: 2
[2022-12-02T06:30:23.285171Z INFO  pgcat::config] [pool: sharded_db][user: sharding_user] Pool size: 9
[2022-12-02T06:30:23.285204Z INFO  pgcat::config] [pool: sharded_db][user: sharding_user] Statement timeout: 0
[2022-12-02T06:30:23.285345Z INFO  pgcat::config] [pool: sharded_db][user: other_user] Pool size: 21
[2022-12-02T06:30:23.285353Z INFO  pgcat::config] [pool: sharded_db][user: other_user] Statement timeout: 15000
[2022-12-02T06:30:23.285359Z INFO  pgcat::config] [pool: simple_db] Maximum user connections: 5
[2022-12-02T06:30:23.285364Z INFO  pgcat::config] [pool: simple_db] Pool mode: Transaction
[2022-12-02T06:30:23.285369Z INFO  pgcat::config] [pool: simple_db] Connection timeout: 5000ms
[2022-12-02T06:30:23.285374Z INFO  pgcat::config] [pool: simple_db] Sharding function: pg_bigint_hash
[2022-12-02T06:30:23.285379Z INFO  pgcat::config] [pool: simple_db] Primary reads: true
[2022-12-02T06:30:23.285383Z INFO  pgcat::config] [pool: simple_db] Query router: true
[2022-12-02T06:30:23.285388Z INFO  pgcat::config] [pool: simple_db] Number of shards: 1
[2022-12-02T06:30:23.285392Z INFO  pgcat::config] [pool: simple_db] Number of users: 1
[2022-12-02T06:30:23.285397Z INFO  pgcat::config] [pool: simple_db][user: simple_user] Pool size: 5
[2022-12-02T06:30:23.285402Z INFO  pgcat::config] [pool: simple_db][user: simple_user] Statement timeout: 0
[2022-12-02T06:30:23.284609Z INFO  pgcat::prometheus] Exposing prometheus metrics on http://0.0.0.0:9930/metrics.
[2022-12-02T06:30:23.285516Z INFO  pgcat::pool] [pool: sharded_db][user: sharding_user] creating new pool
[2022-12-02T06:30:23.285883Z INFO  pgcat::pool] Creating a new server connection Address { id: 0, host: "127.0.0.1", port: 5432, shard: 0, database: "shard0", role: Primary, replica_number: 0, address_index: 0, username: "sharding_user", pool_name: "sharded_db" }
[2022-12-02T06:30:23.292142Z INFO  pgcat::pool] Creating a new server connection Address { id: 1, host: "localhost", port: 5433, shard: 0, database: "shard0", role: Replica, replica_number: 0, address_index: 1, username: "sharding_user", pool_name: "sharded_db" }
[2022-12-02T06:30:23.316973Z INFO  pgcat::pool] Creating a new server connection Address { id: 2, host: "localhost", port: 5434, shard: 0, database: "shard0", role: Replica, replica_number: 1, address_index: 2, username: "sharding_user", pool_name: "sharded_db" }
[2022-12-02T06:30:23.337804Z INFO  pgcat::pool] Creating a new server connection Address { id: 3, host: "127.0.0.1", port: 5432, shard: 1, database: "shard1", role: Primary, replica_number: 0, address_index: 0, username: "sharding_user", pool_name: "sharded_db" }
[2022-12-02T06:30:23.342340Z INFO  pgcat::pool] Creating a new server connection Address { id: 4, host: "localhost", port: 5433, shard: 1, database: "shard1", role: Replica, replica_number: 0, address_index: 1, username: "sharding_user", pool_name: "sharded_db" }
[2022-12-02T06:30:23.357989Z INFO  pgcat::pool] Creating a new server connection Address { id: 5, host: "localhost", port: 5434, shard: 1, database: "shard1", role: Replica, replica_number: 1, address_index: 2, username: "sharding_user", pool_name: "sharded_db" }
[2022-12-02T06:30:23.369388Z INFO  pgcat::pool] Creating a new server connection Address { id: 6, host: "127.0.0.1", port: 5432, shard: 2, database: "shard2", role: Primary, replica_number: 0, address_index: 0, username: "sharding_user", pool_name: "sharded_db" }
[2022-12-02T06:30:23.374910Z INFO  pgcat::pool] Creating a new server connection Address { id: 7, host: "localhost", port: 5433, shard: 2, database: "shard2", role: Replica, replica_number: 0, address_index: 1, username: "sharding_user", pool_name: "sharded_db" }
[2022-12-02T06:30:23.394223Z INFO  pgcat::pool] Creating a new server connection Address { id: 8, host: "localhost", port: 5434, shard: 2, database: "shard2", role: Replica, replica_number: 1, address_index: 2, username: "sharding_user", pool_name: "sharded_db" }
[2022-12-02T06:30:23.405927Z INFO  pgcat::pool] [pool: sharded_db][user: other_user] creating new pool
[2022-12-02T06:30:23.406268Z INFO  pgcat::pool] Creating a new server connection Address { id: 9, host: "127.0.0.1", port: 5432, shard: 0, database: "shard0", role: Primary, replica_number: 0, address_index: 0, username: "other_user", pool_name: "sharded_db" }
[2022-12-02T06:30:23.412445Z INFO  pgcat::pool] Creating a new server connection Address { id: 10, host: "localhost", port: 5433, shard: 0, database: "shard0", role: Replica, replica_number: 0, address_index: 1, username: "other_user", pool_name: "sharded_db" }
[2022-12-02T06:30:23.417602Z INFO  pgcat::pool] Creating a new server connection Address { id: 11, host: "localhost", port: 5434, shard: 0, database: "shard0", role: Replica, replica_number: 1, address_index: 2, username: "other_user", pool_name: "sharded_db" }
[2022-12-02T06:30:23.422787Z INFO  pgcat::pool] Creating a new server connection Address { id: 12, host: "127.0.0.1", port: 5432, shard: 1, database: "shard1", role: Primary, replica_number: 0, address_index: 0, username: "other_user", pool_name: "sharded_db" }
[2022-12-02T06:30:23.429330Z INFO  pgcat::pool] Creating a new server connection Address { id: 13, host: "localhost", port: 5433, shard: 1, database: "shard1", role: Replica, replica_number: 0, address_index: 1, username: "other_user", pool_name: "sharded_db" }
[2022-12-02T06:30:23.438427Z INFO  pgcat::pool] Creating a new server connection Address { id: 14, host: "localhost", port: 5434, shard: 1, database: "shard1", role: Replica, replica_number: 1, address_index: 2, username: "other_user", pool_name: "sharded_db" }
[2022-12-02T06:30:23.445699Z INFO  pgcat::pool] Creating a new server connection Address { id: 15, host: "127.0.0.1", port: 5432, shard: 2, database: "shard2", role: Primary, replica_number: 0, address_index: 0, username: "other_user", pool_name: "sharded_db" }
[2022-12-02T06:30:23.452223Z INFO  pgcat::pool] Creating a new server connection Address { id: 16, host: "localhost", port: 5433, shard: 2, database: "shard2", role: Replica, replica_number: 0, address_index: 1, username: "other_user", pool_name: "sharded_db" }
[2022-12-02T06:30:23.458212Z INFO  pgcat::pool] Creating a new server connection Address { id: 17, host: "localhost", port: 5434, shard: 2, database: "shard2", role: Replica, replica_number: 1, address_index: 2, username: "other_user", pool_name: "sharded_db" }
[2022-12-02T06:30:23.464638Z INFO  pgcat::pool] [pool: simple_db][user: simple_user] creating new pool
[2022-12-02T06:30:23.464848Z INFO  pgcat::pool] Creating a new server connection Address { id: 18, host: "127.0.0.1", port: 5432, shard: 0, database: "some_db", role: Primary, replica_number: 0, address_index: 0, username: "simple_user", pool_name: "simple_db" }
[2022-12-02T06:30:23.472276Z INFO  pgcat::pool] Creating a new server connection Address { id: 19, host: "localhost", port: 5433, shard: 0, database: "some_db", role: Replica, replica_number: 0, address_index: 1, username: "simple_user", pool_name: "simple_db" }
[2022-12-02T06:30:23.484090Z INFO  pgcat::pool] Creating a new server connection Address { id: 20, host: "localhost", port: 5434, shard: 0, database: "some_db", role: Replica, replica_number: 1, address_index: 2, username: "simple_user", pool_name: "simple_db" }
[2022-12-02T06:30:23.496559Z INFO  pgcat] Config autoreloader: false
[2022-12-02T06:30:23.496816Z INFO  pgcat] Waiting for clients
[2022-12-02T06:30:23.496611Z INFO  pgcat::stats] Events reporter started
```

新开一个容器 bash, 测试 pgcat 的连接

```
docker exec -it 67e1eed1b4b6 bash

[postgres@67e1eed1b4b6 pgcat]$ export PGPASSWORD=simple_user
[postgres@67e1eed1b4b6 pgcat]$ psql -h 127.0.0.1 -p 6432 -U simple_user simple_db
psql (11.9)
Type "help" for help.

simple_db=> \q

pgbench -i -s 10  -h 127.0.0.1 -p 6432 -U simple_user simple_db

pgbench -M prepared -n -r -P 1 -c 4 -j 4 -T 120 -S -h 127.0.0.1 -p 6432 -U simple_user simple_db

pgbench -M extended -n -r -P 1 -c 12 -j 12 -T 120 -S -h 127.0.0.1 -p 6432 -U simple_user simple_db
progress: 1.0 s, 15276.8 tps, lat 0.782 ms stddev 0.388
progress: 2.0 s, 14739.9 tps, lat 0.814 ms stddev 0.417
progress: 3.0 s, 13652.3 tps, lat 0.879 ms stddev 0.483
progress: 4.0 s, 13377.7 tps, lat 0.897 ms stddev 0.483

pgbench -M extended -n -r -P 1 -c 120 -j 1200 -T 1200 -S -h 127.0.0.1 -p 6432 -U simple_user simple_db
progress: 1.0 s, 14764.8 tps, lat 7.635 ms stddev 4.248
progress: 2.0 s, 15562.4 tps, lat 7.704 ms stddev 4.211
progress: 3.0 s, 15672.1 tps, lat 7.646 ms stddev 5.242
progress: 4.0 s, 16104.7 tps, lat 7.448 ms stddev 5.672
progress: 5.0 s, 15078.6 tps, lat 7.965 ms stddev 6.022
```

读写分离测试:

```
psql -h 127.0.0.1 -p 6432 -U simple_user simple_db
psql (11.9)
Type "help" for help.

simple_db=> select pg_is_in_recovery();
 pg_is_in_recovery
-------------------
 t
(1 row)

simple_db=> select pg_is_in_recovery();
 pg_is_in_recovery
-------------------
 t
(1 row)

simple_db=> begin;
BEGIN
simple_db=> select pg_is_in_recovery();
 pg_is_in_recovery
-------------------
 f
(1 row)

simple_db=> select pg_is_in_recovery();
 pg_is_in_recovery
-------------------
 f
(1 row)
```

## 参考

[《如何用 PolarDB 证明巴菲特的投资理念 - 包括 PolarDB 简单部署》](https://github.com/digoal/blog/blob/master/202209/20220908_02.md)

https://github.com/levkk/pgcat

[《PostgreSQL sharding+pool+读写分离 中间件 - PGcat》](https://github.com/digoal/blog/blob/master/202211/20221116_01.md)

https://www.rust-lang.org/tools/install
