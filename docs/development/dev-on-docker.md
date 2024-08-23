# 基于 Docker 容器开发

::: danger
为简化使用，容器内的 `postgres` 用户没有设置密码，仅供体验。如果在生产环境等高安全性需求场合，请务必修改健壮的密码！
:::

## 在开发机器上下载源代码

从 [GitHub](https://github.com/ApsaraDB/PolarDB-for-PostgreSQL) 上下载 PolarDB for PostgreSQL 的源代码，稳定分支为 `POLARDB_11_STABLE`。如果因网络原因不能稳定访问 GitHub，则可以访问 [Gitee](https://gitee.com/mirrors/PolarDB-for-PostgreSQL)。

:::: code-group
::: code-group-item GitHub

```bash:no-line-numbers
git clone -b POLARDB_11_STABLE https://github.com/ApsaraDB/PolarDB-for-PostgreSQL.git
```

:::
::: code-group-item Gitee

```bash:no-line-numbers
git clone -b POLARDB_11_STABLE https://gitee.com/mirrors/PolarDB-for-PostgreSQL
```

:::
::::

代码克隆完毕后，进入源码目录：

```bash:no-line-numbers
cd PolarDB-for-PostgreSQL/
```

## 拉取开发镜像

拉取 PolarDB-PG 的 [开发镜像](https://hub.docker.com/r/polardb/polardb_pg_devel/tags)。

:::: code-group
::: code-group-item DockerHub

```bash:no-line-numbers
docker pull polardb/polardb_pg_devel:ubuntu24.04
```

:::
::: code-group-item 阿里云 ACR

```bash:no-line-numbers
docker pull registry.cn-hangzhou.aliyuncs.com/polardb_pg/polardb_pg_devel:ubuntu24.04
```

:::
::::

## 创建并运行容器

此时我们已经在开发机器的源码目录中。从开发镜像上创建一个容器，将当前目录作为一个 volume 挂载到容器中，这样可以：

- 在容器内的环境中编译源码
- 在容器外（开发机器上）使用编辑器来查看或修改代码

:::: code-group
::: code-group-item DockerHub

```bash:no-line-numbers
docker run -it \
    -v $PWD:/home/postgres/polardb_pg \
    --shm-size=512m --cap-add=SYS_PTRACE --privileged=true \
    --name polardb_pg_devel \
    polardb/polardb_pg_devel:ubuntu24.04 \
    bash
```

:::
::: code-group-item 阿里云 ACR

```bash:no-line-numbers
docker run -it \
    -v $PWD:/home/postgres/polardb_pg \
    --shm-size=512m --cap-add=SYS_PTRACE --privileged=true \
    --name polardb_pg_devel \
    registry.cn-hangzhou.aliyuncs.com/polardb_pg/polardb_pg_devel:ubuntu24.04 \
    bash
```

:::
::::

进入容器后，为容器内用户获取源码目录的权限，然后编译部署 PolarDB-PG 实例。

```bash:no-line-numbers
# 获取权限并编译部署
cd polardb_pg
sudo chmod -R a+wr ./
sudo chown -R postgres:postgres ./
./polardb_build.sh

# 验证
psql -h 127.0.0.1 -c 'select version();'
            version
--------------------------------
 PostgreSQL 11.9 (POLARDB 11.9)
(1 row)
```

## 编译测试选项说明

以下表格列出了编译、初始化或测试 PolarDB-PG 集群所可能使用到的选项及说明。更多选项及其说明详见源码目录下的 `polardb_build.sh` 脚本。

| 选项                         | 描述                                                                         | 默认值 |
| ---------------------------- | ---------------------------------------------------------------------------- | ------ |
| `--withrep`                  | 是否初始化只读节点                                                           | `NO`   |
| `--repnum`                   | 只读节点数量                                                                 | `1`    |
| `--withstandby`              | 是否初始化热备份节点                                                         | `NO`   |
| `--initpx`                   | 是否初始化为 HTAP 集群（1 个读写节点，2 个只读节点）                         | `NO`   |
| `--with-pfsd`                | 是否编译 PolarDB File System（PFS）相关功能                                  | `NO`   |
| `--with-tde`                 | 是否初始化 [透明数据加密（TDE）](https://zhuanlan.zhihu.com/p/84829027) 功能 | `NO`   |
| `--with-dma`                 | 是否初始化为 DMA（Data Max Availability）高可用三节点集群                    | `NO`   |
| `-r`/ `-t` / <br>`--regress` | 在编译安装完毕后运行内核回归测试                                             | `NO`   |
| `-r-px`                      | 运行 HTAP 实例的回归测试                                                     | `NO`   |
| `-e` /<br>`--extension`      | 运行扩展插件测试                                                             | `NO`   |
| `-r-external`                | 测试 `external/` 下的扩展插件                                                | `NO`   |
| `-r-contrib`                 | 测试 `contrib/` 下的扩展插件                                                 | `NO`   |
| `-r-pl`                      | 测试 `src/pl/` 下的扩展插件                                                  | `NO`   |

如无定制的需求，则可以按照下面给出的选项编译部署不同形态的 PolarDB-PG 集群并进行测试。

## PolarDB-PG 各形态编译部署

### 本地单节点实例

- 1 个读写节点（运行于 `5432` 端口）

```bash:no-line-numbers
./polardb_build.sh
```

### 本地多节点实例

- 1 个读写节点（运行于 `5432` 端口）
- 1 个只读节点（运行于 `5433` 端口）

```bash:no-line-numbers
./polardb_build.sh --withrep --repnum=1
```

### 本地多节点带备库实例

- 1 个读写节点（运行于 `5432` 端口）
- 1 个只读节点（运行于 `5433` 端口）
- 1 个备库节点（运行于 `5434` 端口）

```bash:no-line-numbers
./polardb_build.sh --withrep --repnum=1 --withstandby
```

### 本地多节点 HTAP 实例

- 1 个读写节点（运行于 `5432` 端口）
- 2 个只读节点（运行于 `5433` / `5434` 端口）

```bash:no-line-numbers
./polardb_build.sh --initpx
```

## 实例回归测试

普通实例回归测试：

```bash:no-line-numbers
./polardb_build.sh --withrep -r -e -r-external -r-contrib -r-pl --with-tde
```

HTAP 实例回归测试：

```bash:no-line-numbers
./polardb_build.sh -r-px -e -r-external -r-contrib -r-pl --with-tde
```

DMA 实例回归测试：

```bash:no-line-numbers
./polardb_build.sh -r -e -r-external -r-contrib -r-pl --with-tde --with-dma
```
