---
author: 棠羽
date: 2022/08/31
minute: 20
---

# 格式化并挂载 PFS for CurveBS

<ArticleInfo :frontmatter=$frontmatter></ArticleInfo>

PolarDB File System，简称 PFS 或 PolarFS，是由阿里云自主研发的高性能类 POSIX 的用户态分布式文件系统，服务于阿里云数据库 PolarDB 产品。使用 PFS 对共享存储进行格式化并挂载后，能够保证一个计算节点对共享存储的写入能够立刻对另一个计算节点可见。

## PFS 编译安装

在 PolarDB 计算节点上准备好 PFS 相关工具。推荐使用 DockerHub 上的 PolarDB 开发镜像，其中已经包含了编译完毕的 PFS，无需再次编译安装。[Curve 开源社区](https://github.com/opencurve) 针对 PFS 对接 CurveBS 存储做了专门的优化。在用于部署 PolarDB 的计算节点上，使用下面的命令拉起带有 [PFS for CurveBS](https://github.com/opencurve/PolarDB-FileSystem) 的 PolarDB 开发镜像：

```shell
docker pull polardb/polardb_pg_devel:curvebs
docker run -it \
    --network=host \
    --cap-add=SYS_PTRACE --privileged=true \
    --name polardb_pg \
    polardb/polardb_pg_devel:curvebs bash
```

## 读写节点块设备映射与格式化

进入容器后需要修改 curve 相关的配置文件：

```shell
sudo vim /etc/curve/client.conf
#
################### mds一侧配置信息 ##################
#

# mds的地址信息，对于mds集群，地址以逗号隔开
mds.listen.addr=127.0.0.1:6666
... ...
```

注意，这里的 `mds.listen.addr` 请填写[部署 CurveBS 集群](./storage-curvebs.md#部署-curvebs-集群)中集群状态中输出的 `cluster mds addr`

容器内已经安装了 `curve` 工具，该工具可用于创建卷，用户需要使用该工具创建实际存储 PolarFS 数据的 curve 卷：

```shell
curve create --filename /volume --user my --length 10 --stripeUnit 16384 --stripeCount 64
```

用户可通过 curve create -h 命令查看创建卷的详细说明。上面的列子中，我们创建了一个拥有以下属性的卷：

- 卷名为 /volume
- 所属用户为 my
- 大小为 10GB
- 条带大小为 16KB
- 条带个数为 64

特别需要注意的是，在数据库场景下，我们强烈建议使用条带卷，只有这样才能充分发挥 Curve 的性能优势，而 16384 \* 64 的条带设置是目前最优的条带设置。

## 格式化 curve 卷

在使用 curve 卷之前需要使用 pfs 来格式化对应的 curve 卷：

```shell
sudo pfs -C curve mkfs pool@@volume_my_
```

与我们在本地挂载文件系统前要先在磁盘上格式化文件系统一样，我们也要把我们的 curve 卷格式化为 PolarFS 文件系统。

**_注意_**，由于 PolarFS 解析的特殊性，我们将以 `pool@${volume}_${user}_` 的形式指定我们的 curve 卷，此外还需要将卷名中的 / 替换成 @

## 启动 pfsd 守护进程

```shell
sudo /usr/local/polarstore/pfsd/bin/start_pfsd.sh -p pool@@volume_my_
```

如果 pfsd 启动成功，那么至此 curve 版 PolarFS 已全部部署完成，已经成功挂载 PFS 文件系统。
下面需要编译部署 PolarDB。

---

## 在 PFS 上编译部署 PolarDB for Curve

参阅 [PolarDB 编译部署：PFS 文件系统](./db-pfs-curve.md)。
