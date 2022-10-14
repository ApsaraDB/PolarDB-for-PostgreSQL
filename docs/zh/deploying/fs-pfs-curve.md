---
author: 棠羽
date: 2022/08/31
minute: 20
---

# 格式化并挂载 PFS for CurveBS

<ArticleInfo :frontmatter=$frontmatter></ArticleInfo>

PolarDB File System，简称 PFS 或 PolarFS，是由阿里云自主研发的高性能类 POSIX 的用户态分布式文件系统，服务于阿里云数据库 PolarDB 产品。使用 PFS 对共享存储进行格式化并挂载后，能够保证一个计算节点对共享存储的写入能够立刻对另一个计算节点可见。

## PFS 编译安装

在 PolarDB 计算节点上准备好 PFS 相关工具。推荐使用 DockerHub 上的 PolarDB 开发镜像，其中已经包含了编译完毕的 PFS，无需再次编译安装。在用于部署 PolarDB 的计算节点上，通过以下命令进入容器即可：

```shell
docker pull polardb/polardb_pg_devel
docker run -it \
    --network=host \
    --cap-add=SYS_PTRACE --privileged=true \
    --volume /dev:/dev \
    --name polardb_pg \
    polardb/polardb_pg_devel bash
```

Curve 开源社区针对 PFS 对接 CurveBS 存储做了专门的优化，开发了专门用于适配 CurveBS 的 PFS 版本。目前容器内已有标准版的 PFS。如果想要在 CurveBS 上追求最好的性能，可以在容器内覆盖安装 [PFS for CurveBS](https://github.com/opencurve/PolarDB-FileSystem)。

## 读写节点块设备映射与格式化

在 Curve 中控机上将 CurveBS 卷映射到 PolarDB 计算节点。这里创建了一个 `curve:/test` 的卷，共享到 `polardb-primary` 主机，也就是我们即将用于部署 PolarDB 读写节点的主机上。

```shell:no-line-numbers
curveadm map curve:/test \
    --host polardb-primary \
    -c client.yaml \
    --create --no-exclusive --size 100GB
```

此时，在 `polardb-primary` 节点的主机上，可以看到这个块设备 `nbd0` 了：

```shell:no-line-numbers
$ lsblk
NAME   MAJ:MIN RM  SIZE RO TYPE MOUNTPOINT
nbd0    43:0    0  100G  0 disk
vda    253:0    0   40G  0 disk
└─vda1 253:1    0   40G  0 part /
```

由于 PFS 仅支持访问 **以特定字符开头的块设备**，因此这里使用新的块设备名 `/dev/nvme1n1` 软链接到块设备的原有名称 `/dev/nbd0` 上：

```bash:no-line-numbers
sudo ln -s /dev/nbd0 /dev/nvme1n1
```

在 PolarDB 读写节点 `polardb-primary` 上格式化 PFS 分布式文件系统：

```bash:no-line-numbers
sudo pfs -C disk mkfs nvme1n1
```

## 只读节点块设备映射

在 Curve 中控机上将相同的 CurveBS 卷映射到另一个 PolarDB 计算节点 `polardb-replica` 主机，也就是我们即将用于部署 PolarDB 只读节点的主机上。

```shell:no-line-numbers
curveadm map curve:/test \
    --host polardb-replica \
    -c client.yaml \
    --create --no-exclusive --size 100GB
```

此时，在 `polardb-replica` 节点的主机上，也可以看到这个块设备 `nbd0` 了：

```shell:no-line-numbers
$ lsblk
NAME   MAJ:MIN RM  SIZE RO TYPE MOUNTPOINT
nbd0    43:0    0  100G  0 disk
vda    253:0    0   40G  0 disk
└─vda1 253:1    0   40G  0 part /
```

类似地，使用新的块设备名 `/dev/nvme1n1` 软链接到块设备的原有名称 `/dev/nbd0` 上：

```bash:no-line-numbers
sudo ln -s /dev/nbd0 /dev/nvme1n1
```

## PFS 文件系统挂载

在能够访问相同 Curve 卷的 **所有** PolarDB 计算节点上 **分别** 启动 PFS 守护进程，挂载 PFS 文件系统：

```bash:no-line-numbers
sudo /usr/local/polarstore/pfsd/bin/start_pfsd.sh -p nvme1n1
```

---

## 在 PFS 上编译部署 PolarDB

参阅 [PolarDB 编译部署：PFS 文件系统](./db-pfs.md)。
