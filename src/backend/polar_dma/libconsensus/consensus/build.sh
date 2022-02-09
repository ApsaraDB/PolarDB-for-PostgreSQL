#!/bin/bash
#
# Script for Dev's daily work.  It is a good idea to use the exact same
# build options as the released version.

set -e

get_key_value()
{
  echo "$1" | sed 's/^--[a-zA-Z_-]*=//'
}

usage()
{
cat <<EOF
Usage: $0 [-t debug|release] [-a ON|OFF] [-d install_prefix] [-c ON|OFF]
       Or
       $0 [-h | --help]
  -t                      Select the build type [debug|release].
  -a                      Build with ASAN.
  -d                      Install Prefix.
  -c                      Build with -D_GLIBCXX_USE_CXX11_ABI=0
  -h, --help              Show this help message.

Note: this script is intended for internal use by X-Paxos developers.
EOF
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
    -a=*)
      with_asan=`get_key_value "$1"`;;
    -a)
      shift
      with_asan=`get_key_value "$1"`;;
    -c=*)
      without_cxx11_abi=`get_key_value "$1"`;;
    -c)
      shift
      without_cxx11_abi=`get_key_value "$1"`;;
    -p=*)
      build_proto3=`get_key_value "$1"`;;
    -d=*)
      install_dir=`get_key_value "$1"`;;
    -d)
      shift
      install_dir=`get_key_value "$1"`;;
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

dump_options()
{
  echo "Dumping the options used by $0 ..."
  echo "build_type=$build_type"
  echo "with_asan=$with_asan"
  echo "install_dir=$dest_dir"
  echo "without_cxx11_abi=$without_cxx11_abi"
}

build_type="release"
install_dir="."
without_cxx11_abi="OFF"
with_asan="OFF"

parse_options "$@"
dump_options

if [ x"$build_type" = x"debug" ]; then
  debug="ON"
elif [ x"$build_type" = x"release" ]; then
  debug="OFF"
else
  echo "Invalid build type, it must be \"debug\" or \"release\"."
  exit 1
fi

CWD="`pwd`"
if [[ "${install_dir:0:1}" == "/" ]]; then
dest_dir=$install_dir
else
dest_dir=$CWD"/"$install_dir
fi

if [ ! -d bu ];then
mkdir bu
fi
cd bu

export OPENSSL_ROOT_DIR=/usr/local/openssl

# modify this cmake script for you own needs
cmake -D CMAKE_INSTALL_PREFIX=$dest_dir -D WITH_DEBUG=$debug -D WITHOUT_CXX11_ABI=$without_cxx11_abi -D WITH_TSAN=OFF -D WITH_ASAN=$with_asan ..
make libmyeasy -sj`getconf _NPROCESSORS_ONLN`
cd ../protocol
cd ../bu
make -sj`getconf _NPROCESSORS_ONLN`
make install -j`getconf _NPROCESSORS_ONLN`
