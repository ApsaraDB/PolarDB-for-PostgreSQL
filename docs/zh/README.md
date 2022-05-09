---
home: true
title: 文档
heroImage: /images/polardb.png
actions:
  - text: 快速上手
    link: /zh/guide/quick-start.html
    type: primary
  - text: 架构解读
    link: /zh/architecture/
    type: secondary
features:
  - title: 极致弹性
    details: 存储与计算节点均可独立地横向扩展。
  - title: 毫秒级节点间延迟
    details: 基于 LogIndex 的延迟回放和并行回放。
  - title: HTAP 能力
    details: 基于共享存储的分布式并行执行框架。
footer: Apache 2.0 Licensed | Copyright © Alibaba Group, Inc.
---

### 通过 Docker 快速体验

从 DockerHub 上拉取 PolarDB for PostgreSQL 的 [开发镜像](https://hub.docker.com/r/polardb/polardb_pg_devel/tags)，创建、运行并进入容器：

```bash
# 拉取 PolarDB 开发镜像
docker pull polardb/polardb_pg_devel
# 创建、运行并进入容器
docker run -it --cap-add=SYS_PTRACE --privileged=true --name polardb_pg polardb/polardb_pg_devel bash
```

进入容器后，从 GitHub 拉取最新的稳定代码，快速编译部署最简单的 PolarDB 实例并进行验证：

```bash
# 代码拉取
git clone -b POLARDB_11_STABLE https://github.com/ApsaraDB/PolarDB-for-PostgreSQL.git
cd PolarDB-for-PostgreSQL
# 编译部署
./polardb_build.sh
# 验证
psql -h 127.0.0.1 -c 'select version();'
            version
--------------------------------
 PostgreSQL 11.9 (POLARDB 11.9)
(1 row)
```
