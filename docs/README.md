---
home: true
title: Documentation
heroImage: /images/polardb.png
footer: Apache 2.0 Licensed | Copyright © Alibaba Group, Inc.
---

---

### Quick Start with Docker

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

<div class="features">

  <div class="feature">
    <h3>Deployment</h3>
    <ul style="position: relative;z-index: 10;">
      <li><a href="./deploying/introduction.html">Architecture Introduction</a></li>
      <li><a href="./deploying/quick-start.html">Quick Deployment</a></li>
      <li><a href="./deploying/deploy.html">Advanced Deployment</a></li>
      <li><a href="./deploying/deploy-stack.html">More</a></li>
    </ul>
  </div>

  <div class="feature">
    <h3>Operation and Maintenance</h3>
    <ul style="position: relative;z-index: 10;">
      <li><a href="./operation/backup-and-restore.html">Backup and Recovery</a></li>
      <li><a href="./operation/tpcc-test.html">TPC-C Benchmarking</a></li>
    </ul>
  </div>

  <div class="feature">
    <h3>Feature Practice</h3>
    <ul style="position: relative;z-index: 10;">
      <li><a href="./features/tpch-on-px.html">Accelerate TPC-H with PolarDB HTAP</a></li>
    </ul>
  </div>

  <div class="feature">
    <h3>Theory</h3>
    <ul style="position: relative;z-index: 10;">
      <li><a href="./theory/arch-overview.html">Feature Overview</a></li>
      <li><a href="./theory/buffer-management.html">Buffer Management</a></li>
      <li><a href="./theory/ddl-synchronization.html">DDL Synchronization</a></li>
      <li><a href="./theory/logindex.html">LogIndex</a></li>
      <li><a href="./theory/analyze.html">Code Analysis of ANALYZE</a></li>
    </ul>
  </div>

  <div class="feature">
    <h3>Development</h3>
    <ul style="position: relative;z-index: 10;">
      <li><a href="./development/dev-on-docker.html">Developing on Docker</a></li>
      <li><a href="./development/customize-dev-env.html">Custom Development Environment</a></li>
    </ul>
  </div>

  <div class="feature">
    <h3>Contributing</h3>
    <ul style="position: relative;z-index: 10;">
      <li><a href="./contributing/code-of-conduct.html">Code of Conduct</a></li>
      <li><a href="./contributing/contributing-polardb-docs.html">Contributing Documentation</a></li>
      <li><a href="./contributing/contributing-polardb-kernel.html">Contributing Code</a></li>
      <li><a href="./contributing/coding-style">Coding Style</a></li>
    </ul>
  </div>

</div>
