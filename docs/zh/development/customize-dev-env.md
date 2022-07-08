# 定制开发环境

## 自行构建开发镜像

我们在 DockerHub 上提供了构建完毕的镜像 [`polardb/polardb_pg_devel`](https://hub.docker.com/r/polardb/polardb_pg_devel/tags) 可供直接使用（支持 AMD64 和 ARM64 架构）😁。

另外，我们也提供了构建上述开发镜像的 Dockerfile，从 CentOS 7 官方镜像 `centos:centos7` 开始构建出一个安装完所有开发和运行时依赖的镜像。您可以根据自己的需要在 Dockerfile 中添加更多依赖。以下是手动构建镜像的 Dockerfile 及方法。

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
RUN yum install -y devtoolset-9-gcc devtoolset-9-gcc-c++ devtoolset-9-gdb devtoolset-9-libstdc++-devel devtoolset-9-make && \
    yum install -y llvm-toolset-7.0-llvm-devel llvm-toolset-7.0-clang-devel llvm-toolset-7.0-cmake

# dependencies
RUN yum install -y libicu-devel pam-devel readline-devel libxml2-devel libxslt-devel openldap-devel openldap-clients openldap-servers libuuid-devel xerces-c-devel bison flex gettext tcl-devel python-devel perl-IPC-Run perl-Expect perl-Test-Simple perl-DBD-Pg perl-ExtUtils-Embed perl-ExtUtils-MakeMaker zlib-devel krb5-devel krb5-workstation krb5-server protobuf-devel && \
    ln /usr/lib64/perl5/CORE/libperl.so /usr/lib64/libperl.so

# install basic tools
RUN echo "install basic tools" && \
    yum install -y \
        git lcov psmisc sudo vim \
        less  \
        net-tools  \
        python2-psycopg2 \
        python2-requests  \
        tar  \
        shadow-utils \
        which  \
        binutils\
        libtool \
        perf  \
        make sudo \
        util-linux

# set to empty if GitHub is not barriered
# ENV GITHUB_PROXY=https://ghproxy.com/
ENV GITHUB_PROXY=

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
    echo '/usr/local/lib' >> /etc/ld.so.conf && ldconfig && \
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

## 从干净的系统开始搭建开发环境

该方式假设您从一台具有 root 权限的干净的 CentOS 7 操作系统上从零开始，可以是：

- 安装 CentOS 7 的物理机/虚拟机
- 从 CentOS 7 官方 Docker 镜像 `centos:centos7` 上启动的 Docker 容器

### 建立非 root 用户

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

### 依赖安装

在 PolarDB for PostgreSQL 的源码库根目录下，有一个 `install_dependencies.sh` 脚本，包含了 PolarDB 和 PFS 需要运行的所有依赖。因此，首先需要克隆 PolarDB 的源码库。

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

源码下载完毕后，使用 `sudo` 执行源代码根目录下的依赖安装脚本 `install_dependencies.sh` 自动完成所有的依赖安装。如果有定制的开发需求，请自行修改 `install_dependencies.sh`。

```bash
cd PolarDB-for-PostgreSQL
sudo ./install_dependencies.sh
```
