# PolarDB 编译部署：单机文件系统

本文将指导您在单机文件系统（如 ext4）上编译部署 PolarDB，适用于所有计算节点都可以访问相同本地磁盘存储的场景。

我们在 DockerHub 上提供了一个 [PolarDB 开发镜像](https://hub.docker.com/r/polardb/polardb_pg_devel/tags)，里面已经包含编译运行 PolarDB for PostgreSQL 所需要的所有依赖。您可以直接使用这个开发镜像进行实例搭建。镜像目前支持 AMD64 和 ARM64 两种 CPU 架构。

## 环境准备

拉取开发镜像，创建并进入容器：

```bash
docker pull polardb/polardb_pg_devel
docker run -it \
    --cap-add=SYS_PTRACE --privileged=true \
    --name polardb_pg \
    polardb/polardb_pg_devel bash
```

进入容器后，从 [GitHub](https://github.com/ApsaraDB/PolarDB-for-PostgreSQL) 上下载 PolarDB for PostgreSQL 的源代码，稳定分支为 `POLARDB_11_STABLE`。如果因网络原因不能稳定访问 GitHub，则可以访问 [Gitee 国内镜像](https://gitee.com/mirrors/PolarDB-for-PostgreSQL)。

:::: code-group
::: code-group-item GitHub

```bash:no-line-numbers
git clone -b POLARDB_11_STABLE https://github.com/ApsaraDB/PolarDB-for-PostgreSQL.git
```

:::
::: code-group-item Gitee 国内镜像

```bash:no-line-numbers
git clone -b POLARDB_11_STABLE https://gitee.com/mirrors/PolarDB-for-PostgreSQL
```

:::
::::

代码克隆完毕后，进入源码目录：

```bash
cd PolarDB-for-PostgreSQL/
```

## PolarDB 各集群形态编译部署

### 本地单节点实例

- 1 个主节点（运行于 `5432` 端口）

```bash:no-line-numbers
./polardb_build.sh
```

### 本地多节点实例

- 1 个主节点（运行于 `5432` 端口）
- 1 个只读节点（运行于 `5433` 端口）

```bash:no-line-numbers
./polardb_build.sh --withrep --repnum=1
```

### 本地多节点带备库实例

- 1 个主节点（运行于 `5432` 端口）
- 1 个只读节点（运行于 `5433` 端口）
- 1 个备库节点（运行于 `5434` 端口）

```bash:no-line-numbers
./polardb_build.sh --withrep --repnum=1 --withstandby
```

### 本地多节点 HTAP 实例

- 1 个主节点（运行于 `5432` 端口）
- 2 个只读节点（运行于 `5433` / `5434` 端口）

```bash:no-line-numbers
./polardb_build.sh --initpx
```

### 实例回归测试

普通实例回归测试：

```bash:no-line-numbers
./polardb_build.sh -r -e -r-external -r-contrib -r-pl --withrep --with-tde
```

HTAP 实例回归测试：

```bash:no-line-numbers
./polardb_build.sh -r-px -e -r-external -r-contrib -r-pl --with-tde
```

DMA 实例回归测试：

```bash:no-line-numbers
./polardb_build.sh -r -e -r-external -r-contrib -r-pl --with-tde --with-dma
```
