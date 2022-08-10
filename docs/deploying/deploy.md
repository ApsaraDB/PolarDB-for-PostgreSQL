# 进阶部署

部署 PolarDB for PostgreSQL 需要在以下三个层面上做准备：

1. **块存储设备层**：用于提供存储介质。可以是单个物理块存储设备（本地存储），也可以是多个物理块设备构成的分布式块存储。
2. **文件系统层**：由于 PostgreSQL 将数据存储在文件中，因此需要在块存储设备上架设文件系统。根据底层块存储设备的不同，可以选用单机文件系统（如 ext4）或分布式文件系统 [PolarDB File System（PFS）](https://github.com/ApsaraDB/PolarDB-FileSystem)。
3. **数据库层**：PolarDB for PostgreSQL 的编译和部署环境。

以下表格给出了三个层次排列组合出的的不同实践方式，其中的步骤包含：

- 存储层：块存储设备的准备
- 文件系统：PolarDB File System 的编译、挂载
- 数据库层：PolarDB for PostgreSQL 各集群形态的编译部署

我们强烈推荐使用发布在 DockerHub 上的 [PolarDB 开发镜像](https://hub.docker.com/r/polardb/polardb_pg_devel/tags) 来完成实践！开发镜像中已经包含了文件系统层和数据库层所需要安装的所有依赖，无需手动安装。

|                                                                                                                                                               | 块存储                 | 文件系统                |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------- | ----------------------- |
| [实践 1（极简本地部署/开发）](./db-localfs.md)                                                                                                                | 本地 SSD               | 本地文件系统（如 ext4） |
| [实践 2（生产环境最佳实践）](./storage-aliyun-essd.md) <a href="https://developer.aliyun.com/live/249628"><Badge type="tip" text="Live" vertical="top" /></a> | 阿里云 ECS + ESSD 云盘 | PFS                     |
| [实践 3（生产环境最佳实践）](./storage-ceph.md)                                                                                                               | Ceph 共享存储          | PFS                     |
| [实践 4](./storage-nbd.md)                                                                                                                                    | NBD 共享存储           | PFS                     |
