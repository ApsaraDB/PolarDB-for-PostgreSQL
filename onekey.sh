#!/bin/bash

echo "This script uses default configuration to compile PolarDB, to deploy binary, "
echo "and to start a cluster of three nodes including a leader, 2 follower. "
echo "   all:   one key for full environment build, include build source code, deploy"
echo "          a 2c2d(2 coordinator nodes & 2 datanodes) cluster."
echo "   dispaxos:  one key for full environment build, include build source code, deploy"
echo "          and setup 2c2d & each datanode having 3-node paxos environment."
echo "   standalone:   one key for full environment build, include build source code, deploy"
echo "          and setup 3 node paxos environment by default configure."
echo "   build:   configure the build with performance optimzation"
echo "             with assertion enabled, and then build"
echo "   configure:  create default configure."
echo "   deploy:  deploy binary to all related machine."
echo "   setup:   setup 3 node paxos environment by default configure"
echo "   cm: setup cluster manager component."
echo "   dependencies: install dependent packages (use Centos as an example)."
echo "   clean:   clean environment"
echo ""

set -e
pushd "$( dirname "${BASH_SOURCE[0]}" )"
CODEHOME=$PWD

if [ -z $PG_INSTALL ];
then
    PG_INSTALL=$HOME/polardb/polardbhome
fi
echo "PG installation dir: "$PG_INSTALL
PGBIN=$PG_INSTALL/bin

if [ $# -gt 0 ]; then
    BLD_OPT=$1
else
    BLD_OPT="all"
fi

CMD=()

PGXC_CTL=$PGBIN/pgxc_ctl

if [[ "$BLD_OPT" == "all" ]]; then
    sh ./build.sh
    mkdir -p $HOME/polardb
    touch $HOME/polardb/polardb_paxos.conf
    $PGXC_CTL -c $HOME/polardb/polardb_paxos.conf prepare distributed
    $PGXC_CTL -c $HOME/polardb/polardb_paxos.conf deploy all
    $PGXC_CTL -c $HOME/polardb/polardb_paxos.conf clean all
    $PGXC_CTL -c $HOME/polardb/polardb_paxos.conf init all
#    $PGXC_CTL -c $HOME/polardb/polardb_paxos.conf deploy cm
    $PGXC_CTL -c $HOME/polardb/polardb_paxos.conf monitor all
elif [[ "$BLD_OPT" == "dispaxos" ]]; then
    sh ./build.sh
    mkdir -p $HOME/polardb
    touch $HOME/polardb/polardb_paxos.conf
    $PGXC_CTL -c $HOME/polardb/polardb_paxos.conf prepare dispaxos
    $PGXC_CTL -c $HOME/polardb/polardb_paxos.conf deploy all
    $PGXC_CTL -c $HOME/polardb/polardb_paxos.conf clean all
    $PGXC_CTL -c $HOME/polardb/polardb_paxos.conf init all
#    $PGXC_CTL -c $HOME/polardb/polardb_paxos.conf deploy cm
    $PGXC_CTL -c $HOME/polardb/polardb_paxos.conf monitor all
elif [[ "$BLD_OPT" == "standalone" ]]; then
    sh ./build.sh
    mkdir -p $HOME/polardb
    touch $HOME/polardb/polardb_paxos.conf
    $PGXC_CTL -c $HOME/polardb/polardb_paxos.conf prepare standalone
    $PGXC_CTL -c $HOME/polardb/polardb_paxos.conf deploy all
    $PGXC_CTL -c $HOME/polardb/polardb_paxos.conf clean all
    $PGXC_CTL -c $HOME/polardb/polardb_paxos.conf init all
    $PGXC_CTL -c $HOME/polardb/polardb_paxos.conf deploy cm
    $PGXC_CTL -c $HOME/polardb/polardb_paxos.conf monitor all
elif [[ "$BLD_OPT" == "build" ]]; then
    sh ./build.sh
elif [[ "$BLD_OPT" == "configure" ]]; then
    mkdir -p $HOME/polardb
    touch $HOME/polardb/polardb_paxos.conf
    $PGXC_CTL -c $HOME/polardb/polardb_paxos.conf prepare standalone
elif [[ "$BLD_OPT" == "deploy" ]]; then
    $PGXC_CTL -c $HOME/polardb/polardb_paxos.conf deploy all
elif [[ "$BLD_OPT" == "setup" ]]; then
    $PGXC_CTL -c $HOME/polardb/polardb_paxos.conf clean all
    $PGXC_CTL -c $HOME/polardb/polardb_paxos.conf init all
    $PGXC_CTL -c $HOME/polardb/polardb_paxos.conf monitor all
elif [[ "$BLD_OPT" == "cm" ]]; then
    $PGXC_CTL -c $HOME/polardb/polardb_paxos.conf deploy cm
elif [[ "$BLD_OPT" == "dependencies" ]]; then
    sudo yum install bison flex libzstd-devel libzstd zstd cmake openssl-devel protobuf-devel readline-devel libxml2-devel libxslt-devel zlib-devel bzip2-devel lz4-devel snappy-devel python-devel
elif [[ "$BLD_OPT" == "clean" ]]; then
    $PGXC_CTL -c $HOME/polardb/polardb_paxos.conf clean all
else
    echo "Invalid Parameter! Usage: $0 [all|build|configure|deploy|setup|dependencies|cm|clean]"
    popd
    exit
fi

echo "============> Enjoy polardb! <==============="

popd
