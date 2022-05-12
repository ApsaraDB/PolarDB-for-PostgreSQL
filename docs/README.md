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

### Try with Docker

Pull the [development image](https://hub.docker.com/r/polardb/polardb_pg_devel/tags) of PolarDB for PostgreSQL from DockerHub. Create, run and enter the container:

```bash
# pull the development image of PolarDB
docker pull polardb/polardb_pg_devel
# create, run and enter the container
docker run -it --cap-add=SYS_PTRACE --privileged=true --name polardb_pg polardb/polardb_pg_devel bash
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
