# 格式化并挂载 PFS

PolarDB File System，简称 PFS 或 PolarFS，是由阿里云自主研发的高性能类 POSIX 的用户态分布式文件系统，服务于阿里云数据库 PolarDB 产品。使用 PFS 对共享存储进行格式化并挂载后，能够保证一个计算节点对共享存储的写入能够立刻对另一个计算节点可见。

## PFS 编译安装

推荐使用 DockerHub 上的 PolarDB 开发镜像，其中已经包含了编译完毕的 PFS，无需再次编译安装。通过以下命令进入容器即可：

```bash
docker pull polardb/polardb_pg_devel
docker run -it \
    --network=host \
    --cap-add=SYS_PTRACE --privileged=true \
    --name polardb_pg \
    polardb/polardb_pg_devel bash
```

PFS 的手动编译安装方式请参考 PFS 的 [README](https://github.com/ApsaraDB/polardb-file-system/blob/master/Readme-CN.md)，此处不再赘述。

## 块设备重命名（可选）

PFS 仅支持访问 **以特定字符开头的块设备**，因此我们建议在所有访问块设备的节点上，通过软链接使用相同的名字访问共享块设备，例如 `nvme1n1`。

例如，在 NBD 服务端主机上，使用新的块设备名 `/dev/nvme1n1` 软链到共享存储块设备的原有名称 `/dev/vdb` 上：

```bash:no-line-numbers
ln -s /dev/vdb /dev/nvme1n1
```

在 NBD 客户端主机上，使用同样的块设备名 `/dev/nvme1n1` 软链到共享存储块设备的原有名称 `/dev/nbd0` 上：

```bash:no-line-numbers
ln -s /dev/nbd0 /dev/nvme1n1
```

如此，便可以在服务端和客户端 2 台主机上使用相同的块设备名 `/dev/nvme1n1` 来访问同一个块设备。

## 块设备格式化

使用 **任意一台主机**，在共享存储块设备上格式化 PFS 分布式文件系统：

```bash:no-line-numbers
sudo pfs -C disk mkfs nvme1n1
```

## PFS 文件系统挂载

在能够访问共享存储的 **所有主机节点** 上，分别启动 PFS 守护进程并挂载 PFS 文件系统：

```bash:no-line-numbers
sudo /usr/local/polarstore/pfsd/bin/start_pfsd.sh -p nvme1n1
```

---

## 在 PFS 上编译部署 PolarDB

参阅 [PolarDB 编译部署：PFS 文件系统](./db-pfs.md)。
