---
home: true
title: Documentation
heroImage: /images/polardb.png
actions:
  - text: Getting Started
    link: /guide/quick-start.html
    type: primary
  - text: Architecture Introduction
    link: /architecture/
    type: secondary
features:
  - title: Flexible Scalability
    details: Scale out compute/storage clusters on demand.
  - title: Millisecond-level Latency
    details: Lazy/parallel replay via shared-storage-based WAL and LogIndex.
  - title: HTAP
    details: Shared-storage-based massively parallel processing (MPP) framework.
footer: Apache 2.0 Licensed | Copyright © Alibaba Group, Inc.
---

### Use with Docker

Pull the [instance image](https://hub.docker.com/r/polardb/polardb_pg_local_instance/tags) of PolarDB for PostgreSQL based on local storage. Create, run and enter the container, and use PolarDB instance directly:

:::: code-group
::: code-group-item Single-Node

```bash:no-line-numbers
# pull the instance image from DockerHub
docker pull polardb/polardb_pg_local_instance:single
# create, run and enter the container
docker run -it --cap-add=SYS_PTRACE --privileged=true --name polardb_pg_single polardb/polardb_pg_local_instance:single bash
# check
psql -h 127.0.0.1 -c 'select version();'
            version
--------------------------------
 PostgreSQL 11.9 (POLARDB 11.9)
(1 row)
```

:::
::: code-group-item Multi-Node

```bash:no-line-numbers
# pull the instance image from DockerHub
docker pull polardb/polardb_pg_local_instance:withrep
# create, run and enter the container
docker run -it --cap-add=SYS_PTRACE --privileged=true --name polardb_pg_withrep polardb/polardb_pg_local_instance:withrep bash
# check
psql -h 127.0.0.1 -c 'select version();'
            version
--------------------------------
 PostgreSQL 11.9 (POLARDB 11.9)
(1 row)
```

:::

::: code-group-item HTAP

```bash:no-line-numbers
# pull the instance image from DockerHub
docker pull polardb/polardb_pg_local_instance:htap
# create, run and enter the container
docker run -it --cap-add=SYS_PTRACE --privileged=true --name polardb_pg_htap polardb/polardb_pg_local_instance:htap bash
# check
psql -h 127.0.0.1 -c 'select version();'
            version
--------------------------------
 PostgreSQL 11.9 (POLARDB 11.9)
(1 row)
```

:::
::::

### Develop with Docker

Pull the [development image](https://hub.docker.com/r/polardb/polardb_pg_devel/tags) of PolarDB for PostgreSQL from DockerHub. Create, run and enter the container:

```bash
# pull the development image of PolarDB
docker pull polardb/polardb_pg_devel
# create, run and enter the container
docker run -it --cap-add=SYS_PTRACE --privileged=true --name polardb_pg_devel polardb/polardb_pg_devel bash
```

After entering the container, clone the latest stable code from GitHub, build and deploy the simplest PolarDB instance and check:

```bash
# code fetching
git clone -b POLARDB_11_STABLE https://github.com/ApsaraDB/PolarDB-for-PostgreSQL.git
cd PolarDB-for-PostgreSQL
# build and deploy
./polardb_build.sh
# check
psql -h 127.0.0.1 -c 'select version();'
            version
--------------------------------
 PostgreSQL 11.9 (POLARDB 11.9)
(1 row)
```
