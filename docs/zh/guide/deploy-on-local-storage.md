# 实例部署：基于单机本地存储

以下是基于单机本地存储编译 PolarDB 源码并启动一写多读实例的步骤。

我们推荐在 Docker 中编译源码并运行示例，从而尽可能减少配置环境的操作。我们提供了一个基于 [CentOS 7 官方 Docker 镜像](https://hub.docker.com/_/centos) `centos:centos7` 构建出的 [PolarDB 开发镜像](https://hub.docker.com/r/polardb/polardb_pg_devel/tags)，里面包含了编译运行 PolarDB 需要的所有依赖。您可以直接使用这个镜像进行实例搭建，也可以自行修改我们在下面提供的 Dockerfile 以满足您的定制需求。

当然，不使用 Docker 也完全没有问题。我们提供了基于纯净 CentOS 7 操作系统的依赖安装脚本，助您快速完成环境准备。🎉

## 安装 Docker

::: tip
如果不使用 Docker，可跳过本小节。
:::

请参阅 [Docker 官方文档](https://docs.docker.com/engine/install/) 完成不同平台上 Docker 的安装。

- Ubuntu：[在 Ubuntu 上安装 Docker Engine](https://docs.docker.com/engine/install/ubuntu/)
- Debian：[在 Debian 上安装 Docker Engine](https://docs.docker.com/engine/install/debian/)
- CentOS：[在 CentOS 上安装 Docker Engine](https://docs.docker.com/engine/install/centos/)
- RHEL：[在 RHEL 上安装 Docker Engine](https://docs.docker.com/engine/install/rhel/)
- Fedora：[在 Fedora 上安装 Docker Engine](https://docs.docker.com/engine/install/fedora/)
- macOS（支持 M1 芯片）：[在 Mac 上安装 Docker Desktop](https://docs.docker.com/desktop/mac/install/)，并建议将内存调整为 4GB

## 编译环境准备

以下两种方式任选一种即可：

- [基于 PolarDB Docker 开发镜像](./deploy-on-local-storage.md#基于-polardb-docker-开发镜像)：无需手动配置环境，较为简单
- [基于 CentOS 7 系统或容器](./deploy-on-local-storage.md#基于-centos-7-系统或容器)：适合对开发环境做更多定制

### 基于 PolarDB Docker 开发镜像

该方式使您可以在 Docker 容器中编译并部署示例。

#### Docker 镜像准备

我们在 DockerHub 上提供了构建完毕的镜像 [`polardb/polardb_pg_devel:centos7`](https://hub.docker.com/r/polardb/polardb_pg_devel/tags) 可供直接使用（支持 AMD64 和 ARM64 架构）😁。

另外，我们也提供了构建上述开发镜像的 Dockerfile，从 CentOS 7 官方镜像 `centos:centos7` 开始构建出一个安装完所有开发和运行时依赖的镜像。您可以根据自己的需要在 Dockerfile 中添加更多依赖。以下是手动构建镜像的 Dockerfile 及方法，如果您决定直接使用 DockerHub 上构建完毕的镜像，则跳过该步骤。

::: details

```dockerfile
FROM centos:centos7

CMD bash

# avoid missing locale problem
RUN sed -i 's/override_install_langs/# &/' /etc/yum.conf

# add EPEL and scl source
RUN rpmkeys --import file:///etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7 && \
    yum install -y epel-release centos-release-scl && \
    rpmkeys --import file:///etc/pki/rpm-gpg/RPM-GPG-KEY-EPEL-7 && \
    rpmkeys --import file:///etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-SIG-SCLo && \
    yum update -y

# GCC and LLVM
RUN yum install -y devtoolset-9-gcc devtoolset-9-gcc-c++ devtoolset-9-gdb \
                   devtoolset-9-libstdc++-devel devtoolset-9-make && \
    yum install -y llvm-toolset-7.0-llvm-devel llvm-toolset-7.0-clang-devel \
                   llvm-toolset-7.0-cmake

# dependencies
RUN yum install -y libicu-devel pam-devel readline-devel libxml2-devel \
                   libxslt-devel openldap-devel openldap-clients \
                   openldap-servers libuuid-devel xerces-c-devel \
                   bison flex gettext tcl-devel python-devel perl-IPC-Run \
                   perl-Expect perl-Test-Simple perl-DBD-Pg perl-ExtUtils-Embed \
                   perl-ExtUtils-MakeMaker zlib-devel krb5-devel \
                   krb5-workstation krb5-server protobuf-devel

RUN ln /usr/lib64/perl5/CORE/libperl.so /usr/lib64/libperl.so

# tools
RUN yum install -y git lcov psmisc sudo vim

# set to empty if GitHub is not barriered
ENV GITHUB_PROXY=https://ghproxy.com/

ENV OPENSSL_VERSION=OpenSSL_1_1_1k

# install dependencies from GitHub mirror
RUN yum install -y libaio-devel wget && \
    cd /usr/local && \
    # zlog for PFSD
    wget --no-verbose --no-check-certificate \
    "${GITHUB_PROXY}https://github.com/HardySimpson/zlog/archive/refs/tags/1.2.14.tar.gz" && \
    # PFSD
    wget --no-verbose --no-check-certificate \
    "${GITHUB_PROXY}https://github.com/ApsaraDB/PolarDB-FileSystem/archive/refs/tags/pfsd4pg-release-1.2.41-20211018.tar.gz" && \
    # OpenSSL 1.1.1
    wget --no-verbose --no-check-certificate \
    "${GITHUB_PROXY}https://github.com/openssl/openssl/archive/refs/tags/${OPENSSL_VERSION}.tar.gz" && \
    # enable build tools
    echo "source /opt/rh/devtoolset-9/enable" >> /etc/bashrc && \
    echo "source /opt/rh/llvm-toolset-7.0/enable" >> /etc/bashrc && \
    source /etc/bashrc && \
    # unzip and install zlog
    tar -zxf 1.2.14.tar.gz && \
    cd zlog-1.2.14 && \
    make && make install && \
    cd .. && \
    rm 1.2.14.tar.gz && \
    rm -rf zlog-1.2.14 && \
    # unzip and install PFSD
    tar -zxf pfsd4pg-release-1.2.41-20211018.tar.gz && \
    cd PolarDB-FileSystem-pfsd4pg-release-1.2.41-20211018 && \
    ./autobuild.sh && ./install.sh && \
    cd .. && \
    rm pfsd4pg-release-1.2.41-20211018.tar.gz && \
    rm -rf PolarDB-FileSystem-pfsd4pg-release-1.2.41-20211018 && \
    # unzip and install OpenSSL 1.1.1
    tar -zxf $OPENSSL_VERSION.tar.gz && \
    cd /usr/local/openssl-$OPENSSL_VERSION && \
    ./config --prefix=/usr/local/openssl && make -j64 && make install && \
    cp /usr/local/openssl/lib/libcrypto.so.1.1 /usr/lib64/ && \
    cp /usr/local/openssl/lib/libssl.so.1.1 /usr/lib64/ && \
    cp -r /usr/local/openssl/include/openssl /usr/include/ && \
    ln -sf /usr/lib64/libcrypto.so.1.1 /usr/lib64/libcrypto.so && \
    ln -sf /usr/lib64/libssl.so.1.1 /usr/lib64/libssl.so && \
    rm -f /usr/local/$OPENSSL_VERSION.tar.gz && \
    rm -rf /usr/local/openssl-$OPENSSL_VERSION

# create default user
ENV USER_NAME=postgres
RUN echo "create default user" && \
    groupadd -r $USER_NAME && useradd -g $USER_NAME $USER_NAME -p '' && \
    usermod -aG wheel $USER_NAME

WORKDIR /home/$USER_NAME

# modify conf
RUN echo "modify conf" && \
    mkdir -p /run/pfs && chown $USER_NAME /run/pfs && \
    mkdir -p /var/log/pfs && chown $USER_NAME /var/log/pfs && \
    echo "ulimit -c unlimited" >> /home/postgres/.bashrc && \
    echo "export PATH=/home/postgres/tmp_basedir_polardb_pg_1100_bld/bin:\$PATH" >> /home/postgres/.bashrc && \
    echo "alias pg='psql -h /home/postgres/tmp_master_dir_polardb_pg_1100_bld/'" >> /home/postgres/.bashrc && \
    rm /etc/localtime && \
    cp /usr/share/zoneinfo/UTC /etc/localtime && \
    sed -i 's/4096/unlimited/g' /etc/security/limits.d/20-nproc.conf && \
    sed -i 's/vim/vi/g' /root/.bashrc

USER $USER_NAME
```

:::

将上述内容复制到一个文件内（假设文件名为 `Dockerfile-PolarDB`）后，使用如下命令构建镜像：

::: tip
💡 请在下面的高亮行中按需替换 `<image_name>` 内的 Docker 镜像名称
:::

```bash:no-line-numbers{2}
docker build --network=host \
    -t <image_name> \
    -f Dockerfile-PolarDB .
```

#### 代码下载

PolarDB for PostgreSQL 的代码托管于 [GitHub](https://github.com/ApsaraDB/PolarDB-for-PostgreSQL) 上，稳定分支为 `POLARDB_11_STABLE`。如果因网络原因不能稳定访问 GitHub，则可以访问 [Gitee 国内镜像](https://gitee.com/mirrors/PolarDB-for-PostgreSQL)。

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

#### 创建并启动 Docker 容器

::: tip
💡 请在下面的高亮行中按需替换 `<>` 的部分：

1. PolarDB for PostgreSQL 的源码路径
2. 将要启动的 Docker 容器名称
3. 自行构建或 DockerHub 上的 PolarDB 开发镜像名称

:::

:::: code-group
::: code-group-item 本地镜像

```bash:no-line-numbers{3,5,6}
# 创建容器
docker create -it \
    -v <src_to_polardb>:/home/postgres/PolarDB-for-PostgreSQL \
    --cap-add=SYS_PTRACE --privileged=true \
    --name <container_name> \
    <image_name> bash
```

:::
::: code-group-item DockerHub 镜像

```bash:no-line-numbers{3,5}
# 创建容器
docker create -it \
    -v <src_to_polardb>:/home/postgres/PolarDB-for-PostgreSQL \
    --cap-add=SYS_PTRACE --privileged=true \
    --name <container_name> \
    polardb/polardb_pg_devel:centos7 bash
```

:::
::::

```bash:no-line-numbers{2}
# 启动容器
docker start <container_name>
```

镜像构建过程中已经创建了一个 `postgres:postgres` 用户，从该镜像运行的容器将直接使用这个用户。容器启动后，通过以下命令进入正在运行的容器中：

```bash:no-line-numbers{4}
docker exec -it \
    --env COLUMNS=`tput cols` \
    --env LINES=`tput lines` \
    <container_name> bash
```

通过 bash 进入容器后，进入源码目录，为用户 `postgres` 获取源代码目录权限，然后编译实例：

```bash
cd /home/postgres/PolarDB-for-PostgreSQL
sudo chown -R postgres:postgres ./
./polardb_build.sh
```

部署完成后，进行简单的实例检查，确保部署正确：

```bash
$HOME/tmp_basedir_polardb_pg_1100_bld/bin/psql \
    -p 5432 -h 127.0.0.1 -c 'select version();'
            version
--------------------------------
 PostgreSQL 11.9 (POLARDB 11.9)
(1 row)
```

### 基于 CentOS 7 系统或容器

该方式假设您从一台具有 root 权限的干净的 CentOS 7 操作系统上从零开始，可以是：

- 安装 CentOS 7 的物理机/虚拟机
- 从 CentOS 7 官方 Docker 镜像 `centos:centos7` 上启动的 Docker 容器

#### 建立非 root 用户

PolarDB for PostgreSQL 需要以非 root 用户运行。以下步骤能够帮助您创建一个名为 `postgres` 的用户组和一个名为 `postgres` 的用户。

::: tip
如果您已经有了一个非 root 用户，但名称不是 `postgres:postgres`，可以忽略该步骤；但请注意在后续示例步骤中将命令中用户相关的信息替换为您自己的用户组名与用户名。

:::

下面的命令能够创建用户组 `postgres` 和用户 `postgres`，并为该用户赋予 sudo 和工作目录的权限。需要以 root 用户执行这些命令。

```bash
# install sudo
yum install -y sudo

# create user and group
groupadd -r postgres
useradd -m -g postgres postgres -p ''
usermod -aG wheel postgres

# make postgres as sudoer
chmod u+w /etc/sudoers
echo 'postgres ALL=(ALL) NOPASSWD: ALL' >> /etc/sudoers
chmod u-w /etc/sudoers

# grant access to home directory
chown -R postgres:postgres /home/postgres/
echo 'source /etc/bashrc' >> /home/postgres/.bashrc
# for su postgres
sed -i 's/4096/unlimited/g' /etc/security/limits.d/20-nproc.conf
```

接下来，切换到 `postgres` 用户，就可以进行后续的步骤了：

```bash
su postgres
source /etc/bashrc
cd ~
```

#### 下载 PolarDB 源代码

PolarDB for PostgreSQL 的代码托管于 [GitHub](https://github.com/ApsaraDB/PolarDB-for-PostgreSQL) 上，稳定分支为 `POLARDB_11_STABLE`。如果因网络原因不能稳定访问 GitHub，则可以访问 [Gitee 国内镜像](https://gitee.com/mirrors/PolarDB-for-PostgreSQL)。

:::: code-group
::: code-group-item GitHub

```bash:no-line-numbers
sudo yum install -y git
git clone -b POLARDB_11_STABLE https://github.com/ApsaraDB/PolarDB-for-PostgreSQL.git
```

:::
::: code-group-item Gitee 国内镜像

```bash:no-line-numbers
sudo yum install -y git
git clone -b POLARDB_11_STABLE https://gitee.com/mirrors/PolarDB-for-PostgreSQL
```

:::
::::

#### 依赖安装

使用 `sudo` 执行源代码根目录下的依赖安装脚本 `install_dependencies.sh` 完成所有的依赖安装。

```bash
cd PolarDB-for-PostgreSQL
sudo ./install_dependencies.sh
```

#### 编译部署

依赖安装完毕后，刷新用户配置，开始编译部署：

```bash
source /etc/bashrc
./polardb_build.sh
```

部署完成后，进行简单的实例检查：

```bash
$HOME/tmp_basedir_polardb_pg_1100_bld/bin/psql \
    -p 5432 -h 127.0.0.1 -c 'select version();'
            version
--------------------------------
 PostgreSQL 11.9 (POLARDB 11.9)
(1 row)
```

## 编译实例类型

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

## 实例回归测试

普通实例回归测试：

```bash:no-line-numbers
./polardb_build.sh -r -e -r-external -r-contrib -r-pl --withrep --with-tde
```

HTAP 实例回归测试：

```bash:no-line-numbers
./polardb_build.sh -r-px -e -r-external -r-contrib -r-pl --with-tde
```

分布式实例回归测试：

```bash:no-line-numbers
./polardb_build.sh -r -e -r-external -r-contrib -r-pl --with-tde --with-dma
```
