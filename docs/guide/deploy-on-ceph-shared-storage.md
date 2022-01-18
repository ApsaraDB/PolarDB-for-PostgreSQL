# Deploy on Ceph Shared Storage

::: danger
Translation needed.
:::

Ceph 是一个统一的分布式存储系统，由于它可以提供较好的性能、可靠性和可扩展性，被广泛的应用在存储领域。以下是在 ceph 分布式集群上部署 PolarDB-FileSystem 以及 PolarDB-for-PostgreSQL 的教程。

ceph 搭建需要 2 台及以上的物理机/虚拟机实现存储共享与数据备份，本教程以 3 台虚拟机机环境为例，介绍基于 ceph 共享存储的实例构建方法。大体如下：

1. 获取在同一网段的虚拟机三台，互相之间配置 ssh 免密登录，用作 ceph 密钥与配置信息的同步；
2. 在主节点启动 mon 进程，查看状态，并复制配置文件至其余各个节点，完成 mon 启动；
3. 在三个环境中启动 osd 进程配置存储盘，并在主节点环境启动 mgr 进程、rgw 进程；
4. 创建存储池与 rbd 块设备镜像，并对创建好的镜像在各个节点进行映射即可实现块设备的共享；
5. 对块设备进行 PolarFS 的格式化与 PolarDB 的部署。

::: warning
操作系统版本要求 CentOS 7.5 及以上。以下步骤在 CentOS 7.5 上通过测试。
:::

## 环境准备

    - 使用的虚拟机环境如下：

    ```
    IP                  hostname
    192.168.1.173       ceph001
    192.168.1.174       ceph002
    192.168.1.175       ceph003
    ```
    - 安装docker
        > 说明：本教程使用阿里云镜像站提供的docker包。
        - 安装docker依赖包:

            ```
            yum install -y yum-utils device-mapper-persistent-data lvm2
            ```
        - 安装并启动docker：

            ```
            yum-config-manager --add-repo http://mirrors.aliyun.com/docker-ce/linux/centos/docker-ce.repo
            yum makecache
            yum install -y docker-ce

            systemctl start docker
            systemctl enable docker
            ```
        - 检查是否安装成功：

            ```
            docker run hello-world
             ```
    - 配置ssh免密登录
        - 密钥的生成与拷贝：

            ```
            ssh-keygen
            ssh-copy-id -i /root/.ssh/id_rsa.pub    root@ceph001
            ssh-copy-id -i /root/.ssh/id_rsa.pub    root@ceph002
            ssh-copy-id -i /root/.ssh/id_rsa.pub    root@ceph003
            ```
        - 检查是否配置成功

            ```
            ssh root@ceph003
            ```
    - 下载ceph daemon

        ```bash
        docker pull ceph/daemon
        ```

## mon 部署

    - ceph001上mon进程启动

        ```
        docker run -d \
                   --net=host \
                   --privileged=true \
                   -v /etc/ceph:/etc/ceph \
                   -v /var/lib/ceph/:/var/lib/ceph/ \
                   -e MON_IP=192.168.1.173  \
                   -e CEPH_PUBLIC_NETWORK=192.168.1.0/24 \
                   --security-opt seccomp=unconfined \
                   --name=mon01 \
                   ceph/daemon mon
        ```
        **注意：根据实际网络环境修改IP、子网掩码位数。**
    - 查看容器状态

        ```
        docker exec mon01 ceph -s
        cluster:
          id:     937ccded-3483-4245-9f61-e6ef0dbd85ca
          health: HEALTH_OK

        services:
          mon: 1 daemons, quorum ceph001 (age 26m)
          mgr: no daemons active
          osd: 0 osds: 0 up, 0 in

        data:
          pools:   0 pools, 0 pgs
          objects: 0 objects, 0 B
          usage:   0 B used, 0 B / 0 B avail
          pgs:
        ```
        **注意：如果遇到mon is allowing insecure global_id reclaim的报错，使用以下命令解决。**

        ```
        docker exec mon01 ceph config set mon   auth_allow_insecure_global_id_reclaim false
        ```
    - 生成必须的keyring

        ```
        docker exec mon01  ceph auth get client.    bootstrap-osd -o /var/lib/ceph/bootstrap-osd/   ceph.keyring
        docker exec mon01 ceph auth get client. bootstrap-rgw -o /var/lib/ceph/bootstrap-rgw/    ceph.keyring
        ```
    - 配置文件同步

        ```
        ssh root@ceph002 mkdir -p /var/lib/ceph
        scp -r /etc/ceph root@ceph002:/etc
        scp -r /var/lib/ceph/bootstrap* root@ceph002:/  var/lib/ceph
        ssh root@ceph003 mkdir -p /var/lib/ceph
        scp -r /etc/ceph root@ceph003:/etc
        scp -r /var/lib/ceph/bootstrap* root@ceph003:/  var/lib/ceph
        ```
    - 在ceph002与ceph003中启动mon

        ```bash
         docker run -d \
            --net=host \
            --privileged=true \
            -v /etc/ceph:/etc/ceph \
            -v /var/lib/ceph/:/var/lib/ceph/ \
            -e MON_IP=192.168.1.174  \
            -e CEPH_PUBLIC_NETWORK=192.168.1.0/24 \
            --security-opt seccomp=unconfined \
            --name=mon02 \
            ceph/daemon mon

         docker run -d \
            --net=host \
            --privileged=true \
            -v /etc/ceph:/etc/ceph \
            -v /var/lib/ceph/:/var/lib/ceph/ \
            -e MON_IP=1192.168.1.175  \
            -e CEPH_PUBLIC_NETWORK=192.168.1.0/24 \
            --security-opt seccomp=unconfined \
            --name=mon03 \
            ceph/daemon mon
        ```
    - 查看当前集群状态

        ```
        docker exec mon01 ceph -s
        cluster:
          id:     937ccded-3483-4245-9f61-e6ef0dbd85ca
          health: HEALTH_OK

        services:
          mon: 3 daemons, quorum ceph001,ceph002,   ceph003 (age 35s)
          mgr: no daemons active
          osd: 0 osds: 0 up, 0 in

        data:
          pools:   0 pools, 0 pgs
          objects: 0 objects, 0 B
          usage:   0 B used, 0 B / 0 B avail
          pgs:
        ```
        **注意：从mon节点信息查看是否有添加在另外两个节点创建的mon添加进来。**

## osd 部署

    - osd准备阶段：
        >本环境的虚拟机只有一个/dev/vdb磁盘可用，因此为每个虚拟机只创建了一个osd节点。

        ```
        docker run --rm --privileged=true --net=host -  -ipc=host \
                        --security-opt seccomp=unconfined \
                        -v /run/lock/lvm:/run/lock/lvm:z \
                        -v /var/run/udev/:/var/run/udev/:z \
                        -v /dev:/dev -v /etc/ceph:/etc/ceph:z \
                        -v /run/lvm/:/run/lvm/ \
                        -v /var/lib/ceph/:/var/lib/ceph/:z \
                        -v /var/log/ceph/:/var/log/ceph/:z \
                        --entrypoint=ceph-volume \
                        docker.io/ceph/daemon \
                        --cluster ceph lvm prepare --bluestore --data /dev/vdb
        ```
        **注意：以上命令在三个节点都是一样的，只需要根据磁盘名称进行修改调整即可。**
    - osd激活阶段：

        ```
        docker run -d --privileged=true --net=host  --pid=host --ipc=host \
        				--security-opt  seccomp=unconfined \
                        -v /dev:/dev \
                        -v /etc/localtime:/etc/ localtime:ro \
                        -v /var/lib/ceph:/var/lib/  ceph:z \
                        -v /etc/ceph:/etc/ceph:z \
                        -v /var/run/ceph:/var/run/  ceph:z \
                        -v /var/run/udev/:/var/run/ udev/ \
                        -v /var/log/ceph:/var/log/  ceph:z \
                        -v /run/lvm/:/run/lvm/ \
                        -e CLUSTER=ceph \
                        -e  CEPH_DAEMON=OSD_CEPH_VOLUME_ACT  IVATE \
                        -e CONTAINER_IMAGE=docker.io/   ceph/daemon \
                        -e OSD_ID=0 \
                        --name=ceph-osd-0 \
                        docker.io/ceph/daemon
        ```
        **注意：各个节点需要修改OSD_ID与name属性，OSD_ID是从编号0递增的，其余节点为OSD_ID=1、OSD_ID=2。**
    - 查看集群状态

        ```
        docker exec mon01 ceph -s
        cluster:
          id:     e430d054-dda8-43f1-9cda-c0881b782e17
          health: HEALTH_WARN
                  no active mgr

        services:
          mon: 3 daemons, quorum ceph001,ceph002,   ceph003 (age 44m)
          mgr: no daemons active
          osd: 3 osds: 3 up (since 7m), 3 in (since     13m)

        data:
          pools:   0 pools, 0 pgs
          objects: 0 objects, 0 B
          usage:   0 B used, 0 B / 0 B avail
          pgs:
        ```

## mgr、mds、rgw 部署

    - 以下命令均在ceph001进行：

        ```
        docker run -d --net=host \
                      --privileged=true \
                      --security-opt seccomp=unconfined \
                      -v /etc/ceph:/etc/ceph \
                      -v /var/lib/ceph/:/var/lib/ceph/ \
                      --name=ceph-mgr-0 \
                      ceph/daemon mgr

        docker run -d --net=host \
                      --privileged=true \
                      --security-opt seccomp=unconfined \
                      -v /var/lib/ceph/:/var/lib/ceph/ \
                      -v /etc/ceph:/etc/ceph \
                      -e CEPHFS_CREATE=1 \
                      --name=ceph-mds-0 \
                      ceph/daemon mds

        docker run -d --net=host \
                      --privileged=true \
                      --security-opt seccomp=unconfined \
                      -v /var/lib/ceph/:/var/lib/ceph/ \
                      -v /etc/ceph:/etc/ceph \
                      --name=ceph-rgw-0 \
                      ceph/daemon rgw
        ```
    - 查看集群状态

        ```
        docker exec mon01 ceph -s
        cluster:
          id:     e430d054-dda8-43f1-9cda-c0881b782e17
          health: HEALTH_OK

        services:
          mon: 3 daemons, quorum ceph001,ceph002,   ceph003 (age 92m)
          mgr: ceph001(active, since 25m)
          mds: 1/1 daemons up
          osd: 3 osds: 3 up (since 54m), 3 in (since    60m)
          rgw: 1 daemon active (1 hosts, 1 zones)

        data:
          volumes: 1/1 healthy
          pools:   7 pools, 145 pgs
          objects: 243 objects, 7.2 KiB
          usage:   50 MiB used, 2.9 TiB / 2.9 TiB avail
          pgs:     145 active+clean
        ```

## rbd 块设备创建

    >以下命令均在容器mon01中进行。
    - 存储池的创建：

        ```
           docker exec -it mon01 bash
           ceph osd pool create rbd_polar
        ```
    - 创建镜像文件并查看信息

        ```
        rbd create --size 512000 rbd_polar/image02
        rbd info rbd_polar/image02

        rbd image 'image02':
        size 500 GiB in 128000 objects
        order 22 (4 MiB objects)
        snapshot_count: 0
        id: 13b97b252c5d
        block_name_prefix: rbd_data.13b97b252c5d
        format: 2
        features: layering, exclusive-lock,     object-map, fast-diff, deep-flatten
        op_features:
        flags:
        create_timestamp: Thu Oct 28 06:18:07 2021
        access_timestamp: Thu Oct 28 06:18:07 2021
        modify_timestamp: Thu Oct 28 06:18:07 2021
        ```
    - 映射镜像文件

        ```
        rbd map rbd_polar/image02

        rbd: sysfs write failed
        RBD image feature set mismatch. You can     disable features unsupported by the kernel  with "rbd feature    disable rbd_polar/image02   object-map fast-diff deep-flatten".
        In some cases useful info is found in syslog -  try "dmesg | tail".
        rbd: map failed: (6) No such device or address
        ```
        **注意：某些特性内核不支持，需要关闭才可以映射成功。如下进行：关闭rbd不支持特性，重新映射镜像，并查看映射列表。**

        ```
        rbd feature disable rbd_polar/image02   object-map fast-diff deep-flatten
        rbd map rbd_polar/image02
        rbd device list

        id  pool       namespace  image    snap  device
        0   rbd_polar             image01  -     /dev/  rbd0
        1   rbd_polar             image02  -     /dev/  rbd1
        ```
        >此处我已经先映射了一个image01，所以有两条信息。
    - 查看块设备
        >回到容器外，进行操作。查看系统中的块设备：

        ```
        lsblk

        NAME                                                               MAJ:MIN RM  SIZE RO TYPE  MOUNTPOINT
        vda                                                                253:0    0  500G  0 disk
        └─vda1                                                             253:1    0  500G  0 part /
        vdb                                                                253:16   0 1000G  0 disk
        └─ceph--7eefe77f--c618--4477--a1ed--b4f44520dfc 2-osd--block--bced3ff1--42b9--43e1--8f63--e853b  ce41435
                                                                           252:0    0 1000G  0 lvm
        rbd0                                                               251:0    0  100G  0 disk
        rbd1                                                               251:16   0  500G  0 disk
        ```
        **注意：块设备镜像需要在各个节点都进行映射才可以在本地环境中通过lsblk命令查看到，否则不显示。ceph002与ceph003上映射命令与上述一致。**

## PolarDB-FileSystem 安装部署

    >请参考 方式3：搭建基于NBD共享存储 — PolarDB-FileSystem安装部署

## PolarDB-for-PostgreSQL 内核安装部署

    >请参考 方式3：搭建基于NBD共享存储 — PolarDB-for-PostgreSQL内核安装部署
