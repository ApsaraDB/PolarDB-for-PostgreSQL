#!/bin/bash
# Copyright (c) 2020, Alibaba Group Holding Limited
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

set -x
set -e
set -o pipefail

##################### Function Defination Start #####################
function usage() {
  cat <<EOF
This script is to be used to compile PG core source code (PG engine code without files within contrib and external)
It can be called with following options:
  --basedir=<temp dir for PG installation>, specifies which dir to install PG to, note this dir would be cleaned up before being used
  --datadir=<temp dir for databases>], specifies which dir to store database cluster, note this dir would be cleaned up before being used
  --user=<user to start PG>, specifies which user to run PG as
  --port=<port to run PG on>, specifies which port to run PG on
  --debug=[on|off], specifies whether to compile PG with debug mode (affecting gcc flags)
  -c,--coverage, specifies whether to build PG with coverage option
  --nc,--nocompile, prevents re-compilation, re-installation, and re-initialization
  --noinit, prevents creating primary, replica and standby instances
  -t,-r,--regress, runs regression test after compilation and installation
  --withrep init the database with a hot standby replica
  --withstandby init the database with a hot standby replica
  --with-pfsd, compile polar_vfs with PFSD support
  --polar_rep_port=<port to run PG rep on>, specifies which port to run PG replica on
  --polar_standby_port=<port to run PG standby on>, specifies which port to run PG standby on
  --repdir=<temp dir for databases>], specifies which dir to store replica data, note this dir would be cleaned up before being used
  --standbydir=<temp dir for databases>], specifies which dir to store standby data, note this dir would be cleaned up before being used
  -e,--extension, run extension test after compilation and installation
  --tap, configure with --enable-tap-tests option
  -r-external, runs external test after compilation and installation
  -r-contrib, runs contrib test after compilation and installation
  -r-pl, runs pl test after compilation and installation
  -r-quick|-t-quick, run test with quick mode
  --repnum=<number of replicas>, specifies how many replicas to be deployed 
  --repport=<port to run PG on>, specifies which port to run PG replica on. If there are multiple replicas, this is apply for the first replica
  --standbyport=<port to run PG on>, specifies which port to run PG standby on

Please lookup the following secion to find the default values for above options.

Typical command patterns to kick off this script:

1) To just cleanup, re-compile, re-install and get PG restart:
  polardb_build.sh
2) To run all steps included 1), as well as run the ALL regression test cases:
  polardb_build.sh -t
3) To cleanup and re-compile with code coverage option:
  polardb_build.sh -c
4) To run the tests besides 3).
  polardb_build.sh -c -t
5) To run with specific port, user
  polardb_build.sh --port=5501 --user=pg001
6) To run with a replica
  polardb_build.sh --withrep
7) To run with a standby
  polardb_build.sh --withstandby
8) To run all the tests (make check)(include src/test,src/pl,src/interfaces/ecpg,contrib,external)
  polardb_build.sh -r-check-all
9) To run all the tests (make installcheck)(include src/test,src/pl,src/interfaces/ecpg,contrib,external)
  polardb_build.sh -r-installcheck-all
EOF

  exit 0
}

function su_eval() {
  if [[ -z $su_str ]]; then
    eval "$1"
  else
    eval "$su_str \"$1\" "
  fi
}

function check_core() {
  polar_primary_dir_n=${polar_primary_dir}
  corefile=$(find ${polar_primary_dir_n} -name core.*)
  if [[ -n $corefile ]]; then
    echo "find core in primary" >>testlog
    exit
  fi

  if [[ $withrep == "yes" ]]; then
    for i in $(seq 1 $repnum); do
      polar_replica_dir_n=${polar_replica_dir}${i}
      corefile=$(find ${polar_replica_dir_n} -name core.*)
      if [[ -n $corefile ]]; then
        echo "find core in replica" >>testlog
        exit
      fi
    done
  fi

  if [[ $withstandby == "yes" ]]; then
    corefile=$(find ${polar_standby_dir} -name core.*)
    if [[ -n $corefile ]]; then
      echo "find core in standby" >>testlog
      exit
    fi
  fi
}

function check_failed_case() {
  set +e
  failcases=$(grep "\.\. FAILED" testlog | head -n 1)
  failcases=$(grep "\.\. Failed" testlog | head -n 1)$failcases
  failcases=$(grep "CMake Error at" testlog | head -n 1)$failcases
  failcases=$(grep "could not connect to server" testlog | head -n 1)$failcases
  failcases=$(grep "terminating connection due to administrator command" testlog | head -n 1)$failcases
  failcases=$(grep "FAIL" testlog | head -n 1)$failcases
  failcases=$(grep "Dubious, test returned 255" testlog | head -n 1)$failcases
  failcases=$(grep "FATAL" testlog | head -n 1)$failcases
  failcases=$(grep "Error 2" testlog | head -n 1)$failcases

  check_core

  if [[ -n $failcases ]]; then
    echo ""
    echo ""
    echo "============================"
    echo "Suspected failing records:"
    echo "============================"
    echo ""
    grep "\.\. FAILED" testlog
    grep "\.\. Failed" testlog
    grep "CMake Error at" testlog
    grep "could not connect to server" testlog
    grep "terminating connection due to administrator command" testlog
    grep "FATAL" testlog
    grep "Dubious, test returned 255" testlog
  else
    rm -f testlog
    echo ""
    echo ""
    echo "==========================="
    echo "Congratulations!! All cases" $1 "passed!"
    echo "==========================="
    echo ""
  fi
  set -e
}

function polar_stop_database() {
  su_eval "$polar_basedir/bin/pg_ctl -D $1 stop -m i || true"
}

function polar_reset_dir() {
  # cleanup dirs and PG process
  polar_stop_database $polar_primary_dir
  for replica_dir in `ls $polar_prefix | grep 'tmp_replica_dir_polardb_pg_1100_bld'`
  do
    polar_stop_database $polar_prefix/$replica_dir
  done
  polar_stop_database $polar_standby_dir

  # must clean before dir remove, some module depend on base_dir/bin/pg_config
  if [[ $noclean == "no" ]] && [[ -d $polar_basedir/bin ]]; then
    make distclean > /dev/null || true
  fi

  mkdir -p $polar_basedir
  mkdir -p $polar_primary_dir
  mkdir -p $polar_data_dir

  if [[ $normbasedir == "no" ]] && [[ -n $polar_basedir ]] && [[ -d $polar_basedir/bin ]]; then
    rm -fr $polar_basedir/*
  fi

  # cleanup datadir only if it is not specified explicitly by user (so a default trival one)
  if [[ $data_dir_specified == "no" ]] && [[ -n $polar_primary_dir ]]; then
    rm -fr $polar_primary_dir/*
    rm -fr $polar_data_dir/*
    need_initdb=yes
  fi
}

function polar_set_env() {
  gcc_opt_level_flag=

  if [[ $with_pfsd == "yes" ]]; then
    configure_flag="$configure_flag --with-pfsd"
  fi

  if [[ $debug_mode == "on" ]]; then
    gcc_opt_level_flag="-ggdb -O0 -g3 -fno-omit-frame-pointer "
    configure_flag="$configure_flag --enable-debug --enable-cassert --enable-tap-tests"
    export COPT="-Werror"
  else
    gcc_opt_level_flag=" -O3 "
  fi

  if [[ $coverage == "on" ]]; then
    configure_flag="$configure_flag --enable-coverage"
  fi

  if [[ $tap_tests == "on" ]]; then
    configure_flag="$configure_flag --enable-tap-tests"
  fi

  if [[ -z $conf_file ]]; then
    if [[ -f "./src/backend/utils/misc/postgresql.conf.regress" ]]; then
      conf_file="./src/backend/utils/misc/postgresql.conf.regress"
    else
      conf_file="./src/backend/utils/misc/postgresql.conf.sample"
    fi
  fi

  # setup env
  export PGPORT=$polar_port
  export PGUSER=postgres
  export PGHOST=localhost
  export PGDATABASE=postgres

  export CFLAGS=" $gcc_opt_level_flag -g -I/usr/include/et -DLINUX_OOM_SCORE_ADJ=0 -DLINUX_OOM_ADJ=0 -DMAP_HUGETLB=0x40000 -pipe -Wall -fexceptions -fstack-protector-strong --param=ssp-buffer-size=4 -grecord-gcc-switches -mtune=generic"
  export CXXFLAGS=" $gcc_opt_level_flag -g -I/usr/include/et  -pipe -Wall -fexceptions -fstack-protector-strong --param=ssp-buffer-size=4 -grecord-gcc-switches -mtune=generic"
  export LDFLAGS=" -Wl,-rpath,'\$\$ORIGIN/../lib' "

  # to get rid of postgis db creation error
  export LANG=en_US.UTF-8

  # need to set LD_LIBRARY_PATH for ecpg test cases to link correct libraries when compiled and run by its pg_regress
  export LD_LIBRARY_PATH=$polar_basedir/lib:$LD_LIBRARY_PATH

  # may be needed by some module Makefile to get the correct pg_config
  export PATH=$polar_basedir/bin:$PATH

  target_list="$target_list contrib"
}

function polar_compile_and_install() {
  if [[ $nocompile == "off" ]]; then
    ./configure --prefix=$polar_basedir --with-pgport=$polar_port $configure_flag
    for target in $target_list; do
      make -j$(getconf _NPROCESSORS_ONLN) -C $target >/dev/null
      make install -C $target >/dev/null
    done
  fi
}

function polar_test_non_polar() {
  if [[ $make_check_world == "on" ]]; then
    unset COPT
    su_eval "make check-world PG_TEST_EXTRA='kerberos ldap ssl' 2>&1" | tee -a testlog
    su_eval "make -C src/test/regress polar-check 2>&1" | tee -a testlog
    check_failed_case "check-world"
  fi

  if [[ $regress_test == "on" ]]; then
    su_eval "make -C src/test/regress polar-check 2>&1" | tee -a testlog
    if [ -e ./src/test/regress/regression.diffs ]; then
      cat ./src/test/regress/regression.diffs
      exit 1
    fi
  fi
}

common_configs="polar_enable_shared_storage_mode = on
polar_hostid = 1
max_connections = 1000
logging_collector = on
log_line_prefix='%p\t%r\t%u\t%m\t'
log_directory = 'pg_log'
shared_buffers = 2GB
synchronous_commit = off
full_page_writes = off
autovacuum_naptime = 10min
max_worker_processes = 32
oss_fdw.polar_enable_oss_endpoint_mapping = off
max_wal_size = 16GB
min_wal_size = 4GB
polar_vfs.localfs_mode = on
polar_enable_shared_storage_mode = on
polar_check_checkpoint_legal_interval = 1
listen_addresses = '*'
shared_preload_libraries = '\$libdir/polar_vfs,\$libdir/polar_worker'"

function polar_init_primary() {
  if [[ $need_initdb == "yes" ]]; then
    su_eval "$polar_basedir/bin/initdb -k -U $pg_db_user -D $polar_primary_dir"

    disk_name=$(echo $polar_data_dir | cut -d '/' -f2)

    # common configs
    echo "$common_configs" >> $polar_primary_dir/postgresql.conf
    echo "polar_disk_name = '${disk_name}'" >> $polar_primary_dir/postgresql.conf
    echo "polar_datadir = 'file-dio://${polar_data_dir}'" >> $polar_primary_dir/postgresql.conf

    su_eval "$polar_basedir/bin/polar-initdb.sh ${polar_primary_dir}/ ${polar_data_dir}/ localfs"
  fi

  echo "port = ${polar_port}" >>$polar_primary_dir/postgresql.conf
  echo "polar_hostid = 1" >>$polar_primary_dir/postgresql.conf
  echo "full_page_writes = off" >>$polar_primary_dir/postgresql.conf
}

function polar_init_replicas() {
  # init the config of the replica
  if [[ $withrep == "yes" ]]; then
    if [[ $rep_dir_specified == "no" ]]; then
      polar_replica_dir_source=$polar_primary_dir
    else
      polar_replica_dir_source=$polar_replica_dir
    fi

    # create replicas
    synchronous_standby_names=''
    if [ $repnum -ge 1 ]; then
      for i in $(seq 1 $repnum); do
        polar_replica_dir_n=${polar_replica_dir}${i}
        rm -fr $polar_replica_dir_n
        cp -frp $polar_replica_dir_source $polar_replica_dir_n
        polar_rep_port_n=$(($polar_rep_port + $i - 1))

        echo "port = $polar_rep_port_n" >>$polar_replica_dir_n/postgresql.conf
        echo "polar_hostid = $((1+$i))" >>$polar_replica_dir_n/postgresql.conf

        rm -f $polar_replica_dir_n/recovery.conf
        name="replica$i"
        echo "primary_conninfo = 'host=localhost port=$polar_port user=$pg_db_user dbname=postgres application_name=replica${i}'" >>$polar_replica_dir_n/recovery.conf
        echo "polar_replica = 'on'" >>$polar_replica_dir_n/recovery.conf
        echo "recovery_target_timeline = 'latest'" >>$polar_replica_dir_n/recovery.conf
        echo "primary_slot_name = '$name'" >>$polar_replica_dir_n/recovery.conf

        if [ -z $synchronous_standby_names ]; then
            synchronous_standby_names="$name"
        else
            synchronous_standby_names="$synchronous_standby_names,$name"
        fi
      done
    fi

    echo "synchronous_standby_names='$synchronous_standby_names'" >>$polar_primary_dir/postgresql.conf
  fi
}

function polar_init_standby() {
  if [[ $withstandby == "yes" ]]; then
    if [[ $standby_dir_specified == "no" ]] && [[ -n $polar_standby_dir ]]; then
      # init the dir of standby
      rm -fr $polar_standby_dir

      cp -frp $polar_primary_dir $polar_standby_dir

      standby_hostid=2
      if [[ $withrep == "yes" ]]; then
        standby_hostid=$(($standby_hostid + $repnum))
      fi
      echo "port = $polar_standby_port"     >>$polar_standby_dir/postgresql.conf
      echo "polar_hostid = $standby_hostid" >>$polar_standby_dir/postgresql.conf

      echo "primary_conninfo = 'host=localhost port=$polar_port user=$pg_db_user dbname=postgres application_name=standby1'" >>$polar_standby_dir/recovery.conf
      echo "standby_mode = 'on'" >>$polar_standby_dir/recovery.conf
      echo "recovery_target_timeline = 'latest'" >>$polar_standby_dir/recovery.conf
      echo "primary_slot_name = 'standby1'" >>$polar_standby_dir/recovery.conf
    fi

    # init the data dir of standby
    rm -fr $polar_standby_data_dir
    cp -frp $polar_data_dir $polar_standby_data_dir
    sed -i -E "s/${polar_data_dir//\//\\/}/${polar_standby_data_dir//\//\\/}/" $polar_standby_dir/postgresql.conf
  fi
}

function polar_init() {
  polar_init_primary

  polar_init_replicas

  polar_init_standby
}

function polar_start() {
  # start primary
  su_eval "$polar_basedir/bin/pg_ctl -D $polar_primary_dir start -w -c"

  # start replicas and create slots
  if [[ $withrep == "yes" ]]; then
    for i in $(seq 1 $repnum); do
      polar_replica_dir_n=${polar_replica_dir}${i}
      su_eval "env $polar_basedir/bin/psql -h 127.0.0.1 -d postgres -p $polar_port -U $pg_db_user -c \"SELECT * FROM pg_create_physical_replication_slot('replica${i}')\""
      su_eval "$polar_basedir/bin/pg_ctl -D $polar_replica_dir_n start -w -c"
    done
  fi

  # start standby and create slot
  if [[ $withstandby == "yes" ]]; then
    su_eval "env $polar_basedir/bin/psql -h 127.0.0.1 -d postgres -p $polar_port -U $pg_db_user -c \"SELECT * FROM pg_create_physical_replication_slot('standby1')\""
    su_eval "$polar_basedir/bin/pg_ctl -D $polar_standby_dir start -w -c -o '-p $polar_standby_port'"
  fi

  echo "Following command can be used to connect to PG:"
  echo ""
  echo $polar_basedir/bin/psql -h 127.0.0.1 -d postgres -U $pg_db_user -p $polar_port
  echo $polar_basedir/bin/psql -h 127.0.0.1 -d postgres -U $pg_db_user -p $polar_rep_port
  echo $polar_basedir/bin/psql -h 127.0.0.1 -d postgres -U $pg_db_user -p $polar_standby_port
  echo ""
}

function polar_test_regress() {
  if [[ $regress_test == "on" ]] && [[ $regress_test_quick == "off" ]]; then
    # some cases describe using "polar-ignore" when "make polar-installcheck", for polar model
    su_eval "make installcheck PG_TEST_EXTRA='kerberos ldap ssl' 2>&1" | tee -a testlog

    if [ -e ./src/test/regress/regression.diffs ]; then
      cat ./src/test/regress/regression.diffs
    fi

    su_eval "make -C src/test/regress polar-installcheck 2>&1" | tee -a testlog

    if [ -e ./src/test/regress/regression.diffs ]; then
      cat ./src/test/regress/regression.diffs
      exit 1
    fi
  fi
}

function polar_test_extension_contrib() {
  if [[ $contrib_test == "on" ]]; then
    # make installcheck the extensions
    echo "============================"
    echo "Check the contrib extensions:"
    echo "============================"
    su_eval "make -C contrib installcheck 2>&1 " | tee -a testlog
  fi
}

function polar_test_extension_pl() {
  if [[ $pl_test == "on" ]]; then
    echo "============================"
    echo "Check the pl extensions:"
    echo "============================"
    su_eval "make -C src/pl installcheck 2>&1 " | tee -a testlog
  fi

}

function polar_test_extension_external() {
  if [[ $external_test == "on" ]]; then
    echo "============================"
    echo "Check the external extensions:"
    echo "============================"
    su_eval "make -C external installcheck 2>&1 " | tee -a testlog
  fi
}

function polar_test_extension() {
  if [[ $extension_test == "on" ]]; then
    polar_test_extension_contrib

    polar_test_extension_pl

    polar_test_extension_external

    check_failed_case "extension_test"
  fi
}

function polar_test_installcheck() {
  if [[ $make_installcheck_world == "on" ]] && [[ $regress_test_quick == "off" ]]; then
    export PGHOST=/tmp
    su_eval "make installcheck-world PG_TEST_EXTRA='kerberos ldap ssl' 2>&1" | tee -a testlog
    su_eval "make -C src/test/regress polar-installcheck 2>&1" | tee -a testlog
    check_failed_case "installcheck-world"
    unset PGHOST
  fi
}

function polar_test() {
  polar_test_regress

  polar_test_extension

  polar_test_installcheck
}
###################### Function Defination End ######################

##################### Variable Defination Start #####################
data_dir_specified=no
rep_dir_specified=no
standby_dir_specified=no
withrep=no
withstandby=no
need_initdb=no
noclean=no
normbasedir=no
noinit=no
repnum=1
su_str=

target_list=". external"

polar_prefix=$HOME
polar_basedir=$polar_prefix/tmp_basedir_polardb_pg_1100_bld
polar_data_dir=$polar_prefix/tmp_datadir_polardb_pg_1100_bld
polar_primary_dir=$polar_prefix/tmp_primary_dir_polardb_pg_1100_bld
polar_replica_dir=$polar_prefix/tmp_replica_dir_polardb_pg_1100_bld
polar_standby_dir=$polar_prefix/tmp_standby_dir_polardb_pg_1100_bld
polar_standby_data_dir=$polar_prefix/tmp_standby_datadir_polardb_pg_1100_bld

polar_user=$(whoami)
polar_port=5432
polar_rep_port=5433
polar_standby_port=5434
polar_standby_port_specified=no
pg_db_user=postgres
nocompile=off
debug_mode=on
coverage=off
regress_test=off
regress_test_quick=off
make_installcheck_world=off
make_check_world=off
tap_tests=off
##################### Variable Defination End #######################

##################### PHASE 1: set up parameter #####################
for arg; do
  # the parameter after "=", or the whole $arg if no match
  val=$(echo "$arg" | sed -e 's;^--[^=]*=;;')

  #
  # Note we use eval for dir assignment to expand ~
  #
  case "$arg" in
  --basedir=*) eval polar_basedir="$val" ;;
  --datadir=*)
    eval polar_primary_dir="$val"
    data_dir_specified=yes
    ;;
  --repdir=*)
    eval polar_replica_dir="$val"
    rep_dir_specified=yes
    ;;
  --standbydir=*)
    eval polar_standby_dir="$val"
    standby_dir_specified=yes
    ;;
  --user=*) polar_user="$val" ;;
  --port=*) polar_port="$val" ;;
  -h | --help) usage ;;
  --nc | --nocompile) nocompile=on ;;
  --debug=*) debug_mode="$val" ;;
  --withrep*) withrep=yes ;;
  --withstandby*) withstandby=yes ;;
  --with-pfsd*) with_pfsd=yes ;;
  -c | --coverage)
    debug_mode=on
    coverage=on
    ;;
  -r | -t | --regress)
    regress_test=on
    ;;
  -r-check-all)
    make_check_world=on
    tap_tests=on
    ;;
  -r-installcheck-all)
    make_installcheck_world=on
    tap_tests=on
    ;;
  --noclean) noclean=yes ;;         #do not make distclean
  --normbasedir) normbasedir=yes ;; #do not remove data and base dirs
  --noinit) noinit=yes ;;         #do not build instances
  -e | --extension)
    extension_test=on
    ;;
  --tap) tap_tests=on ;;
  -r-external)
    external_test=on
    ;;
  -r-contrib)
    contrib_test=on
    ;;
  -r-pl)
    pl_test=on
    ;;
  -r-quick | -t-quick)
    regress_test_quick=on
    ;;
  --repnum*) repnum="$val" ;;
  --repport=*) polar_rep_port="$val" ;;
  --standbyport=*)
    polar_standby_port="$val"
    polar_standby_port_specified=yes
    ;;
  *)
    echo "wrong options : $arg"
    exit 1
    ;;
  esac
done

if [[ $withrep == "yes" ]] && [[ $polar_standby_port_specified == "no" ]]; then
  polar_standby_port=$(($polar_standby_port + $repnum - 1))
fi

# unset PG* envs
unset PGPORT
unset PGDATA
unset PGHOST
unset PGDATABASE
unset PGUSER

####### PHASE 2: cleanup dir and env, and set new dir and env #######
polar_reset_dir
polar_set_env

#################### PHASE 3: compile and install ###################
polar_compile_and_install

############# PHASE 4 Test: run test cases in non-polar #############
polar_test_non_polar

###################### PHASE 5: init and start ######################
if [[ $noinit == "no" ]]; then
  polar_init
  polar_start
fi

#################### PHASE 6 Test: test for polar ###################
polar_test

echo "polardb build done"

exit 0
