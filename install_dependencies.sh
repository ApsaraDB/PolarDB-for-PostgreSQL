#!/bin/bash
# 
# The script is based on a clean Centos7 system.
# 
# Copyright (c) 2020, Alibaba Group Holding Limited
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# 

# Executed by root
if [[ $(/usr/bin/id -u) -ne 0 ]]; then
    echo "Not running as root"
    exit
fi

# avoid missing locale
sed -i 's/override_install_langs/# &/' /etc/yum.conf

# extra yum software source
# curl -o /etc/yum.repos.d/CentOS-Base.repo https://mirrors.aliyun.com/repo/Centos-7.repo
# curl -o /etc/yum.repos.d/epel.repo http://mirrors.aliyun.com/repo/epel-7.repo
rpmkeys --import file:///etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7
yum install -y epel-release centos-release-scl
rpmkeys --import file:///etc/pki/rpm-gpg/RPM-GPG-KEY-EPEL-7
rpmkeys --import file:///etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-SIG-SCLo
yum update -y

# compile tools
yum install -y devtoolset-9-gcc devtoolset-9-gcc-c++ devtoolset-9-gdb devtoolset-9-libstdc++-devel devtoolset-9-make llvm-toolset-7.0-llvm-devel llvm-toolset-7.0-clang-devel llvm-toolset-7.0-cmake

# dependencies
yum install -y libicu-devel pam-devel readline-devel libxml2-devel libxslt-devel openldap-devel openldap-clients openldap-servers libuuid-devel xerces-c-devel bison flex gettext tcl-devel python-devel perl-IPC-Run perl-Expect perl-Test-Simple perl-DBD-Pg perl-ExtUtils-Embed perl-ExtUtils-MakeMaker zlib-devel krb5-devel krb5-workstation krb5-server git lcov psmisc sudo vim libaio-devel wget protobuf-devel

ln /usr/lib64/perl5/CORE/libperl.so /usr/lib64/libperl.so

# enable GCC9 and LLVM7
echo "source /opt/rh/devtoolset-9/enable" >> /etc/bashrc && \
echo "source /opt/rh/llvm-toolset-7.0/enable" >> /etc/bashrc && \
source /etc/bashrc


OPENSSL_VERSION=OpenSSL_1_1_1k
GITHUB_PROXY=https://ghproxy.com

cd /usr/local

# download zlog for PFSD
wget --no-verbose --no-check-certificate "${GITHUB_PROXY}/https://github.com/HardySimpson/zlog/archive/refs/tags/1.2.14.tar.gz"
# download PFSD
wget --no-verbose --no-check-certificate "${GITHUB_PROXY}/https://github.com/ApsaraDB/PolarDB-FileSystem/archive/refs/tags/pfsd4pg-release-1.2.41-20211018.tar.gz"
# download OpenSSL 1.1.1
wget --no-verbose --no-check-certificate "${GITHUB_PROXY}/https://github.com/openssl/openssl/archive/refs/tags/${OPENSSL_VERSION}.tar.gz"

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
