#!/bin/bash

set -e

usage()
{
cat <<EOF
Usage: $0 [-t debug|release] [-r] [-a] [-c ON|OFF]
       Or
       $0 [-h | --help]
  -t                      Select the build type.
  -r                      Rebuild all dependent libraries.
  -a                      Build with ASAN.
  -c                      Build with -D_GLIBCXX_USE_CXX11_ABI=0
  -h, --help              Show this help message.

EOF
}

get_key_value()
{
  echo "$1" | sed 's/^--[a-zA-Z_-]*=//'
}

parse_options()
{
  while test $# -gt 0
  do
    case "$1" in
    -t=*)
      build_type=`get_key_value "$1"`;;
    -t)
      shift
      build_type=`get_key_value "$1"`;;
    -c=*)
      without_cxx11_abi=`get_key_value "$1"`;;
    -c)
      shift
      without_cxx11_abi=`get_key_value "$1"`;;
    -a)
      with_asan=ON;;
    -r)
      rebuild_libraries=ON;;
    -h | --help)
      usage
      exit 0;;
    *)
      echo "Unknown option '$1'"
      exit 1;;
    esac
    shift
  done
}

build_type="debug"
without_cxx11_abi="OFF"
with_asan="OFF"
rebuild_libraries="OFF"
parse_options "$@"

if [ x"$rebuild_libraries" = x"ON" ]; then
rm -rf ../dependency/easy/src/bu
rm -rf ../consensus/bu
rm -rf ./lib
rm -rf ./include/easy
rm -rf ./include/aliconsensus
rm -rf bu
fi

export OPENSSL_ROOT_DIR=/usr/local/openssl

rm -rf bu && mkdir bu

CWD="`pwd`"
cd ../consensus
bash ./build.sh -t $build_type -a $with_asan -c $without_cxx11_abi -d $CWD/bu
cd "${CWD}" 

if [ x"$build_type" = x"debug" ]; then
  debug="ON"
elif [ x"$build_type" = x"release" ]; then
  debug="OFF"
else
  echo "Invalid build type, it must be \"debug\" or \"release\"."
  exit 1
fi

cd bu
cmake -D CMAKE_INSTALL_PREFIX=$CWD -D WITH_DEBUG=$debug -D WITHOUT_CXX11_ABI=$without_cxx11_abi -D WITH_ASAN=$with_asan ..
make -sj`getconf _NPROCESSORS_ONLN`
make install -j`getconf _NPROCESSORS_ONLN`
