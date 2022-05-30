#!/bin/bash

echo "This script configure and build the code, It takes an optional"
echo "parameter that must be one of the following:"
echo "   deploy:   (default) configure the build with performance"
echo "             optimization options and build."
echo "   verify:   configure the build with performance optimization"
echo "             with assertion enabled, and then build"
echo "   debug:    configure the build with debug options and then"
echo "             build."
echo "   repeat:   skip configure, just build and install"
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
    BLD_OPT="deploy"
fi

CMD=()

CFLAGS="-fno-omit-frame-pointer -Wno-declaration-after-statement"
LDFLAGS="-L/usr/local/lib"
if [[ "$BLD_OPT" == "deploy" ]]; then
    CFLAGS="${CFLAGS} -g -O2"
    CMD+=(--with-python)
    CMD+=(--enable-regress)
elif [[ "$BLD_OPT" == "verify" ]]; then
    CFLAGS="${CFLAGS} -g -O2"
    CMD+=(--enable-cassert)
    CMD+=(--with-python)
#    CMD+=(--with-openssl)
elif [[ "$BLD_OPT" == "debug" ]]; then
    CFLAGS="${CFLAGS} -ggdb -Og -g3 "
#    CMD+=(--with-uuid=ossp --with-openssl)
    CMD+=(--enable-cassert)
    CMD+=(--with-python)
    CMD+=(--enable-debug)
    CMD+=(--enable-regress)
elif [[ "$BLD_OPT" != "repeat" ]]; then
    echo "Invalid Parameter! Usage: $0 [deploy|verify|debug|repeat]"
    popd
    exit
fi

if [[ "$BLD_OPT" != "repeat" ]]; then
    export CFLAGS
    export LDFLAGS
    ./configure --prefix=$PG_INSTALL ${CMD[@]}
fi

# build polardb consensus dynamic library
cd $CODEHOME/src/backend/polar_dma/libconsensus/polar_wrapper
if [[ "$BLD_OPT" == "debug" ]]; then
bash ./build.sh -r -t debug
else
bash ./build.sh -r -t release
fi
cd $CODEHOME

function error_retry {
    echo "Paxos library build failed, try again!"
    cd $CODEHOME/src/backend/polar_dma/libconsensus/polar_wrapper
    if [[ "$BLD_OPT" == "debug" ]]; then
        bash ./build.sh -r -t debug -c ON
    else
        bash ./build.sh -r -t release -c ON
    fi
    cd $CODEHOME
    make -sj 16
}

make -sj 16 || error_retry "build retry!"
make install

# extensions
export PATH=$PGBIN:$PATH
export PG_CONFIG=$PGBIN/pg_config
cd $CODEHOME/contrib && make -sj &&make install
#cd $CODEHOME/contrib/pg_cron && make &&  make install
cd $CODEHOME/src/pl/plpython && make install

echo "============> Enjoy coding! <==============="
echo "You could add $PGBIN to your PATH"

popd
