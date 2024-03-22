---
author: digoal
date: 2023/02/03
minute: 10
---

# 部署 PostGIS 支撑时空轨迹|地理信息|路由等业务

<ArticleInfo :frontmatter=$frontmatter></ArticleInfo>

## 背景

PolarDB 的云原生存算分离架构, 具备低廉的数据存储、高效扩展弹性、高速多机并行计算能力、高速数据搜索和处理; PolarDB 与计算算法结合, 将实现双剑合璧, 推动业务数据的价值产出, 将数据变成生产力.

本文将介绍使用 PolarDB 开源版 部署 PostGIS 支撑时空轨迹|地理信息|路由等业务

测试环境为 macOS+docker, PolarDB 部署请参考下文:

- [《如何用 PolarDB 证明巴菲特的投资理念 - 包括 PolarDB 简单部署》](https://github.com/digoal/blog/blob/master/202209/20220908_02.md)

## PostGIS 的部署

依赖非常多, 请补充好体力.

1、GEOS

```
cd ~
wget https://download.osgeo.org/geos/geos-3.11.1.tar.bz2

tar -jxvf geos-3.11.1.tar.bz2
cd geos-3.11.1
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local ..
make -j 6
sudo make install
sudo vi /etc/ld.so.conf
# add
/usr/local/lib64
sudo ldconfig
```

2、SQLite3

```
cd ~
wget https://www.sqlite.org/2022/sqlite-autoconf-3400000.tar.gz
tar -zxvf sqlite-autoconf-3400000.tar.gz
cd sqlite-autoconf-3400000
./configure
make
sudo make install
sudo vi /etc/ld.so.conf
# add
/usr/local/lib
sudo ldconfig
```

3、

```
sudo yum install -y libtiff-devel
sudo yum install -y libcurl-devel
```

4、PROJ

```
cd ~
wget https://download.osgeo.org/proj/proj-9.1.1.tar.gz
tar -zxvf proj-9.1.1.tar.gz
cd proj-9.1.1
mkdir build
cd build
cmake ..
make -j 6
sudo make install
```

5、

```
cd ~
wget http://prdownloads.sourceforge.net/swig/swig-4.1.1.tar.gz
tar -zxvf swig-4.1.1.tar.gz
cd swig-4.1.1
wget https://github.com/PCRE2Project/pcre2/releases/download/pcre2-10.42/pcre2-10.42.tar.bz2
./Tools/pcre-build.sh
./configure
make -j 4
sudo make install
```

6、GDAL

```
cd ~
wget https://github.com/OSGeo/gdal/releases/download/v3.6.1/gdal-3.6.1.tar.gz
tar -zxvf gdal-3.6.1.tar.gz
cd gdal-3.6.1
mkdir build
cd build
cmake ..
make -j 6
sudo make install
```

7、

```
sudo yum install -y libxml2-devel
sudo yum install -y json-c-devel



rpm -qa|grep protobuf
sudo rpm -e protobuf-compiler-2.5.0-8.el7.x86_64 protobuf-2.5.0-8.el7.x86_64 protobuf-devel-2.5.0-8.el7.x86_64
```

8、protobuf and protobuf-c

```
cd ~
wget https://github.com/protocolbuffers/protobuf/releases/download/v3.11.4/protobuf-all-3.11.4.tar.gz
tar -zxvf protobuf-all-3.11.4.tar.gz
cd protobuf-3.11.4/
./configure --prefix=/usr/local/protobuf
make -j 6
sudo make install

sudo vi /etc/ld.so.conf
# add
/usr/local/protobuf/lib
sudo ldconfig




cd ~
wget https://github.com/protobuf-c/protobuf-c/releases/download/v1.3.3/protobuf-c-1.3.3.tar.gz
tar -zxvf protobuf-c-1.3.3.tar.gz
cd protobuf-c-1.3.3
export PATH=/usr/local/protobuf/bin:$PATH
export PKG_CONFIG_PATH=/usr/local/protobuf/lib/pkgconfig:$PKG_CONFIG_PATH
./configure --prefix=/usr/local/protobuf-c
make -j 6
sudo make install

sudo vi /etc/ld.so.conf
# add
/usr/local/protobuf-c/lib
sudo ldconfig
```

9、PostGIS

```
cd ~
wget https://download.osgeo.org/postgis/source/postgis-3.3.2.tar.gz
tar -zxvf postgis-3.3.2.tar.gz
cd postgis-3.3.2

export PKG_CONFIG_PATH=/usr/local/protobuf-c/lib/pkgconfig:/usr/local/protobuf/lib/pkgconfig:$PKG_CONFIG_PATH

export PATH=/usr/local/protobuf-c/bin:/usr/local/protobuf/bin:$PATH

./configure  --with-protobufdir=/usr/local/protobuf-c  --with-protobuf-inc=/usr/local/protobuf-c/include --with-protobuf-lib=/usr/local/protobuf-c/lib

make -j 8
sudo make install
```

10、可以在 PolarDB 数据库中安装 postgis 插件了

```
psql

postgres=# create extension postg
postgis
postgis_raster
postgis_tiger_geocoder
postgis_topology
```
