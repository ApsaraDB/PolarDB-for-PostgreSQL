---
home: true
title: Documentation
heroImage: /images/polardb.png
footer: Apache 2.0 Licensed | Copyright © Alibaba Group, Inc.
---

---

### Quick Start with Docker

Pull the [local instance image](https://hub.docker.com/r/polardb/polardb_pg_local_instance/tags) of PolarDB for PostgreSQL based on local storage. Create and run the container, and try PolarDB-PG instance directly:

:::: code-group
::: code-group-item DockerHub

```bash:no-line-numbers
# pull the instance image and run the container
docker pull polardb/polardb_pg_local_instance:11
docker run -it --rm polardb/polardb_pg_local_instance:11 psql
# check
postgres=# SELECT version();
            version
--------------------------------
 PostgreSQL 11.9 (POLARDB 11.9)
(1 row)
```

:::
::: code-group-item Alibaba Cloud ACR

```bash:no-line-numbers
# pull the instance image and run the container
docker pull registry.cn-hangzhou.aliyuncs.com/polardb_pg/polardb_pg_local_instance:11
docker run -it --rm registry.cn-hangzhou.aliyuncs.com/polardb_pg/polardb_pg_local_instance:11 psql
# check
postgres=# SELECT version();
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
      <li><a href="./operation/tpch-test.html">TPC-H Benchmarking</a></li>
    </ul>
  </div>

  <div class="feature">
    <h3>Kernel Features</h3>
    <ul style="position: relative;z-index: 10;">
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
      <li><a href="./contributing/contributing-polardb-docs.html">Contributing Documentation</a></li>
      <li><a href="./contributing/contributing-polardb-kernel.html">Contributing Code</a></li>
      <li><a href="./contributing/coding-style">Coding Style</a></li>
    </ul>
  </div>

</div>
