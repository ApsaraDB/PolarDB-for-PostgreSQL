# NBD 共享存储

Network Block Device (NBD) 是一种网络协议，可以在多个主机间共享块存储设备。NBD 被设计为 Client-Server 的架构，因此至少需要两台物理机来部署。

以两台物理机环境为例，本小节介绍基于 NBD 共享存储的实例构建方法大体如下：

- 首先，两台主机通过 NBD 共享一个块设备；
- 然后，两台主机上均部署 PolarDB File System (PFS) 来初始化并挂载到同一个块设备；
- 最后，在两台主机上分别部署 PolarDB for PostgreSQL 内核，构建主节点、只读节点以形成简单的一写多读实例。

::: warning
以上步骤在 CentOS 7.5 上通过测试。
:::

## 安装 NBD

### 为操作系统下载安装 NBD 驱动

::: tip
操作系统内核需要支持 NBD 内核模块，如果操作系统当前不支持该内核模块，则需要自己通过对应内核版本进行编译和加载 NBD 内核模块。
:::

从 [CentOS 官网](https://www.centos.org/) 下载对应内核版本的驱动源码包并解压：

```bash
rpm -ihv kernel-3.10.0-862.el7.src.rpm
cd ~/rpmbuild/SOURCES
tar Jxvf linux-3.10.0-862.el7.tar.xz -C /usr/src/kernels/
cd /usr/src/kernels/linux-3.10.0-862.el7/
```

NBD 驱动源码路径位于：`drivers/block/nbd.c`。接下来编译操作系统内核依赖和组件：

```bash
cp ../$(uname -r)/Module.symvers ./
make menuconfig # Device Driver -> Block devices -> Set 'M' On 'Network block device support'
make prepare && make modules_prepare && make scripts
make CONFIG_BLK_DEV_NBD=m M=drivers/block
```

检查是否正常生成驱动：

```bash
modinfo drivers/block/nbd.ko
```

拷贝、生成依赖并安装驱动：

```bash
cp drivers/block/nbd.ko /lib/modules/$(uname -r)/kernel/drivers/block
depmod -a
modprobe nbd # 或者 modprobe -f nbd 可以忽略模块版本检查
```

检查是否安装成功：

```bash
# 检查已安装内核模块
lsmod | grep nbd
# 如果NBD驱动已经安装，则会生成/dev/nbd*设备（例如：/dev/nbd0、/dev/nbd1等）
ls /dev/nbd*
```

### 安装 NBD 软件包

```bash
yum install nbd
```

## 使用 NBD 来共享块设备

### 服务端部署

拉起 NBD 服务端，按照同步方式（`sync/flush=true`）配置，在指定端口（例如 `1921`）上监听对指定块设备（例如 `/dev/vdb`）的访问。

```bash
nbd-server -C /root/nbd.conf
```

配置文件 `/root/nbd.conf` 的内容举例如下：

```ini
[generic]
    #user = nbd
    #group = nbd
    listenaddr = 0.0.0.0
    port = 1921
[export1]
    exportname = /dev/vdb
    readonly = false
    multifile = false
    copyonwrite = false
    flush = true
    fua = true
    sync = true
```

### 客户端部署

NBD 驱动安装成功后会看到 `/dev/nbd*` 设备, 根据服务端的配置把远程块设备映射为本地的某个 NBD 设备即可：

```bash
nbd-client x.x.x.x 1921 -N export1 /dev/nbd0
# x.x.x.x是NBD服务端主机的IP地址
```

---

## 准备分布式文件系统

参阅 [格式化并挂载 PFS](./fs-pfs.md)。
