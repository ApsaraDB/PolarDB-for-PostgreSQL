# 实例部署：基于单机本地存储

我们提供了快速部署步骤，用于编译 PolarDB 源码并在本地存储上启动一写多读实例。

推荐在 Docker 中编译源码。

## 1. 安装 Docker

如果已有 Docker 或标准 CentOS7，请跳过该步骤。不同操作系统的安装方式：

- macOS（支持 M1 芯片）：[在 Mac 上安装 Docker Desktop](https://docs.docker.com/desktop/mac/install/)，并建议将内存调整为 4GB
- Ubuntu：[在 Ubuntu 上安装 Docker Engine](https://docs.docker.com/engine/install/ubuntu/)
- Debian：[在 Debian 上安装 Docker Engine](https://docs.docker.com/engine/install/debian/)
- CentOS：[在 CentOS 上安装 Docker Engine](https://docs.docker.com/engine/install/centos/)
- RHEL：[在 RHEL 上安装 Docker Engine](https://docs.docker.com/engine/install/rhel/)
- Fedora：[在 Fedora 上安装 Docker Engine](https://docs.docker.com/engine/install/fedora/)

## 2. 下载 PolarDB 源代码

从 PolarDB for PostgreSQL [GitHub 仓库](https://github.com/ApsaraDB/PolarDB-for-PostgreSQL) 的稳定分支 `POLARDB_11_STABLE` 下载源码。

```bash:no-line-numbers
git clone -b POLARDB_11_STABLE git@github.com:ApsaraDB/PolarDB-for-PostgreSQL.git
```

## 3. 环境准备

我们提供两种方式助您完成开发环境的准备，只需选择其中一种即可：

- [基于 CentOS 7 的 Docker 开发镜像（推荐方式）](./deploy-on-local-storage.md#方式-1-基于-centos7-的-docker-开发镜像)：迅速完成单机 3 节点环境准备，适合快速尝鲜
- [基于 CentOS 7 标准系统（从零开始）](./deploy-on-local-storage.md#方式-2-基于-centos7-操作系统从零开始)：适合具有更多定制需求的开发人员或 DBA，适用于：
  - 干净的 CentOS 7 物理机/虚拟机
  - 干净的 `centos:centos7` 镜像容器

### 方式 1：基于 CentOS7 的 Docker 开发镜像

我们提供了下面的 Dockerfile，可以在安装 Docker 的主机上基于 CentOS 7 官方镜像 `centos:centos7` 直接构建出一个安装完所有开发和运行时依赖的镜像：

::: details

```dockerfile
FROM centos:centos7

LABEL maintainer="mrdrivingduck@gmail.com"

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
RUN yum install -y devtoolset-9-gcc devtoolset-9-gcc-c++ devtoolset-9-gdb devtoolset-9-libstdc++-devel devtoolset-9-make && \
    yum install -y llvm-toolset-7.0-llvm-devel llvm-toolset-7.0-clang-devel llvm-toolset-7.0-cmake

# dependencies
RUN yum install -y libicu-devel pam-devel readline-devel libxml2-devel libxslt-devel openldap-devel openldap-clients openldap-servers libuuid-devel xerces-c-devel bison flex gettext tcl-devel python-devel perl-IPC-Run perl-Expect perl-Test-Simple perl-DBD-Pg perl-ExtUtils-Embed perl-ExtUtils-MakeMaker zlib-devel krb5-devel krb5-workstation krb5-server protobuf-devel && \
    ln /usr/lib64/perl5/CORE/libperl.so /usr/lib64/libperl.so

# tools
RUN yum install -y git lcov psmisc sudo vim

# set to empty if GitHub is not barriered
ENV GITHUB_PROXY=https://ghproxy.com/

ENV OPENSSL_VERSION=OpenSSL_1_1_1k

# install dependencies from GitHub mirror
RUN yum install -y libaio-devel wget && \
    cd /usr/local && \
    # zlog for PFSD
    wget --no-verbose --no-check-certificate "${GITHUB_PROXY}https://github.com/HardySimpson/zlog/archive/refs/tags/1.2.14.tar.gz" && \
    # PFSD
    wget --no-verbose --no-check-certificate "${GITHUB_PROXY}https://github.com/ApsaraDB/PolarDB-FileSystem/archive/refs/tags/pfsd4pg-release-1.2.41-20211018.tar.gz" && \
    # OpenSSL 1.1.1
    wget --no-verbose --no-check-certificate "${GITHUB_PROXY}https://github.com/openssl/openssl/archive/refs/tags/${OPENSSL_VERSION}.tar.gz" && \
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

```bash:no-line-numbers
docker build --network=host \
    -t <your_name>:polardb_centos7 \
    -f Dockerfile-PolarDB .
```

镜像构建过程中已经创建了一个 `postgres:postgres` 用户。基于该镜像运行容器将直接使用这个用户：

```bash:no-line-numbers
docker run -it -v <src_to_polardb>:/home/postgres/PolarDB-for-PostgreSQL \
    --cap-add=SYS_PTRACE --privileged=true \
    --name <container_name> \
    <your_name>:polardb_centos7 bash
```

容器启动后，后续直接连接到正在运行的容器中：

```bash:no-line-numbers
docker exec -it --env COLUMNS=`tput cols` --env LINES=`tput lines` \
    <container_name> bash
```

在容器内运行以下命令为用户 `postgres` 获取源代码目录权限：

```bash:no-line-numbers
sudo chmod -R a+wr PolarDB-for-PostgreSQL
sudo chown -R postgres:postgres PolarDB-for-PostgreSQL
```

### 方式 2：基于 CentOS7 操作系统从零开始

该步骤的前提：需要在系统中以 `root` 用户创建一个 **具有 sudo 权限的普通用户 `postgres`**。通过以下步骤进行：

```bash
# install sudo
yum install -y sudo

# create user and group
groupadd -r postgres
useradd -m -g postgres postgres -p ''
usermod -aG wheel postgres

# make postgres as sudoer
sudo chmod u+w /etc/sudoers
echo 'postgres ALL=(ALL) NOPASSWD: ALL' >> /etc/sudoers
sudo chmod u-w /etc/sudoers

# grant access to home directory
chown -R postgres:postgres /home/postgres/
echo 'source /etc/bashrc' >> /home/postgres/.bashrc
```

接下来，切换到 `postgres` 用户，执行源代码根目录下的依赖安装脚本 `install_dependencies.sh` 完成所有的环境准备：

```bash
su postgres
cd <src_to_polardb>
sudo ./install_dependencies.sh
source /etc/bashrc
```

## 4. 编译并搭建实例

::: warning
以下操作均在容器内执行，且以 **普通用户 `postgres`** 完成实例的编译和搭建。
:::

### 本地单节点实例

- 1 个主节点（端口为 5432）

<CodeGroup>
  <CodeGroupItem title="x86_64">

```bash:no-line-numbers
./polardb_build.sh
```

  </CodeGroupItem>

  <CodeGroupItem title="Apple M1 / ARM">

```bash:no-line-numbers
./polardb_build.sh --witharm
```

  </CodeGroupItem>

</CodeGroup>

psql 连接数据库：

```bash:no-line-numbers
$HOME/tmp_basedir_polardb_pg_1100_bld/bin/psql -h127.0.0.1 -p5432
```

**至此，您已经成功编译源码并运行 PolarDB。**

### 本地多节点实例

- 1 个主节点（端口为 5432）
- 1 个只读节点（端口为 5433）

<CodeGroup>
  <CodeGroupItem title="x86_64">

```bash:no-line-numbers
./polardb_build.sh --withrep --repnum=1
```

  </CodeGroupItem>

  <CodeGroupItem title="Apple M1 / ARM">

```bash:no-line-numbers
./polardb_build.sh --witharm --withrep --repnum=1
```

  </CodeGroupItem>

</CodeGroup>

### 本地多节点带备库实例

- 1 个主节点（端口为 5432）
- 1 个只读节点（端口为 5433）
- 1 个备库节点（端口为 5434）

<CodeGroup>
  <CodeGroupItem title="x86_64">

```bash:no-line-numbers
./polardb_build.sh --withrep --repnum=1 --withstandby
```

  </CodeGroupItem>

  <CodeGroupItem title="Apple M1 / ARM">

```bash:no-line-numbers
./polardb_build.sh --witharm --withrep --repnum=1 --withstandby
```

  </CodeGroupItem>

</CodeGroup>

### 本地多节点 HTAP 实例

- 1 个主节点（端口为 5432）
- 2 个只读节点（端口为 5433、5434）

<CodeGroup>
  <CodeGroupItem title="x86_64">

```bash:no-line-numbers
./polardb_build.sh --initpx
```

  </CodeGroupItem>

  <CodeGroupItem title="Apple M1 / ARM">

```bash:no-line-numbers
./polardb_build.sh --witharm --initpx
```

  </CodeGroupItem>

</CodeGroup>

## 5. 检查和测试

部署完成后，需要进行实例检查和测试，确保部署正确。

### 实例检查

```bash
$HOME/tmp_basedir_polardb_pg_1100_bld/bin/psql \
    -p 5432 \
    -h $HOME/tmp_master_dir_polardb_pg_1100_bld/ \
    -c 'select version();'
$HOME/tmp_basedir_polardb_pg_1100_bld/bin/psql \
    -p 5432 \
    -h $HOME/tmp_master_dir_polardb_pg_1100_bld/ \
    -c 'select * from pg_replication_slots;'
```

### 一键执行全量回归测试

普通实例回归测试：

<CodeGroup>
  <CodeGroupItem title="x86_64">

```bash:no-line-numbers
./polardb_build.sh --withrep -r -e -r-external -r-contrib -r-pl --tde
```

  </CodeGroupItem>

  <CodeGroupItem title="Apple M1 / ARM">

```bash:no-line-numbers
./polardb_build.sh --witharm --withrep -r -e -r-external -r-contrib -r-pl --tde
```

  </CodeGroupItem>

</CodeGroup>

HTAP 实例回归测试：

<CodeGroup>
  <CodeGroupItem title="x86_64">

```bash:no-line-numbers
./polardb_build.sh -r-px -e -r-external -r-contrib -r-pl --tde
```

  </CodeGroupItem>

  <CodeGroupItem title="Apple M1 / ARM">

```bash:no-line-numbers
./polardb_build.sh --witharm -r-px -e -r-external -r-contrib -r-pl --tde
```

  </CodeGroupItem>

</CodeGroup>
