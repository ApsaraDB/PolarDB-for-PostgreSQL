# 基于 Docker 容器开发

::: danger
为简化使用，容器内的 `postgres` 用户没有设置密码，仅供体验。如果在生产环境等高安全性需求场合，请务必修改健壮的密码！
:::

## 在开发机器上下载源代码

从 [GitHub](https://github.com/ApsaraDB/PolarDB-for-PostgreSQL) 上下载 PolarDB for PostgreSQL 的源代码，稳定分支为 `POLARDB_11_STABLE`。如果因网络原因不能稳定访问 GitHub，则可以访问 [Gitee 国内镜像](https://gitee.com/mirrors/PolarDB-for-PostgreSQL)。

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

```bash:no-line-numbers
cd PolarDB-for-PostgreSQL/
```

## 拉取开发镜像

从 DockerHub 上拉取 PolarDB for PostgreSQL 的 [开发镜像](https://hub.docker.com/r/polardb/polardb_pg_devel/tags)。

```bash
# 拉取 PolarDB 开发镜像
docker pull polardb/polardb_pg_devel
```

## 创建并运行容器

此时我们已经在开发机器的源码目录中。从开发镜像上创建一个容器，将当前目录作为一个 volume 挂载到容器中，这样可以：

- 在容器内的环境中编译源码
- 在容器外（开发机器上）使用编辑器来查看或修改代码

```bash
docker run -it \
    -v $PWD:/home/postgres/polardb_pg \
    --shm-size=512m --cap-add=SYS_PTRACE --privileged=true \
    --name polardb_pg_devel \
    polardb/polardb_pg_devel \
    bash
```

进入容器后，为容器内用户获取源码目录的权限，然后编译部署 PolarDB 实例。

```bash
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
