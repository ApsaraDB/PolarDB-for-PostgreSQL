#!/bin/bash

echo "This script uses default configuration to compile PolarDB, to deploy binary, "
echo "and to start a cluster of three nodes including a leader, a follower, and a learner. "
echo "   all:   one key for full environment build, include build source code、deploy"
                 "and setup 3 node paxos environment by default configure."
echo "   build:   configure the build with performance optimzation"
echo "             with assertion enabled, and then build"
echo "   configure:  create default configure."
echo "   deploy:  deploy binary to all related machine."
echo "   setup:   setup 3 node paxos environment by default configure"
echo "   cm: setup cluster manager component."
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

if [[ "$BLD_OPT" == "all" ]]; then
    sh ./build.sh
    mkdir -p $HOME/polardb
    touch $HOME/polardb/polardb_paxos.conf
    pgxc_ctl -c $HOME/polardb/polardb_paxos.conf prepare standalone
    pgxc_ctl -c $HOME/polardb/polardb_paxos.conf deploy all
    pgxc_ctl -c $HOME/polardb/polardb_paxos.conf clean all
    pgxc_ctl -c $HOME/polardb/polardb_paxos.conf init all
    pgxc_ctl -c $HOME/polardb/polardb_paxos.conf deploy cm	
    pgxc_ctl -c $HOME/polardb/polardb_paxos.conf monitor all
elif [[ "$BLD_OPT" == "build" ]]; then
    sh ./build.sh
elif [[ "$BLD_OPT" == "configure" ]]; then
    mkdir -p $HOME/polardb
    touch $HOME/polardb/polardb_paxos.conf
    pgxc_ctl -c $HOME/polardb/polardb_paxos.conf prepare standalone
elif [[ "$BLD_OPT" == "deploy" ]]; then
    pgxc_ctl -c $HOME/polardb/polardb_paxos.conf deploy all
elif [[ "$BLD_OPT" == "setup" ]]; then
    pgxc_ctl -c $HOME/polardb/polardb_paxos.conf clean all
    pgxc_ctl -c $HOME/polardb/polardb_paxos.conf init all
    pgxc_ctl -c $HOME/polardb/polardb_paxos.conf monitor all
elif [[ "$BLD_OPT" == "cm" ]]; then
    pgxc_ctl -c $HOME/polardb/polardb_paxos.conf deploy cm
elif [[ "$BLD_OPT" == "clean" ]]; then
    pgxc_ctl -c $HOME/polardb/polardb_paxos.conf clean all	
else
    echo "Invalid Parameter! Usage: $0 [all|build|configure|deploy|setup|cm|clean]"
    popd
    exit
fi

echo "============> Enjoy polardb! <==============="

popd
