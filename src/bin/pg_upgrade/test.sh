#!/bin/sh

# src/bin/pg_upgrade/test.sh
#
# Test driver for pg_upgrade.  Initializes a new database cluster,
# runs the regression tests (to put in some data), runs pg_dumpall,
# runs pg_upgrade, runs pg_dumpall again, compares the dumps.
#
# Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California

set -e

: ${MAKE=make}

# Guard against parallel make issues (see comments in pg_regress.c)
unset MAKEFLAGS
unset MAKELEVEL

# Run a given "initdb" binary and overlay the regression testing
# authentication configuration.
standard_initdb() {
	# To increase coverage of non-standard segment size and group access
	# without increasing test runtime, run these tests with a custom setting.
	# Also, specify "-A trust" explicitly to suppress initdb's warning.
	"$1" -N --wal-segsize 1 -g -A trust
	if [ -n "$TEMP_CONFIG" -a -r "$TEMP_CONFIG" ]
	then
		cat "$TEMP_CONFIG" >> "$PGDATA/postgresql.conf"
	fi
	echo "polar_create_table_with_full_replica_identity=false" >> "$PGDATA/postgresql.conf"
	../../test/regress/pg_regress --config-auth "$PGDATA"
}

# What flavor of host are we on?
# Treat MINGW* (msys1) and MSYS* (msys2) the same.
testhost=`uname -s | sed 's/^MSYS/MINGW/'`

# Establish how the server will listen for connections
case $testhost in
	MINGW*)
		LISTEN_ADDRESSES="localhost"
		PGHOST=localhost
		;;
	*)
		LISTEN_ADDRESSES=""
		# Select a socket directory.  The algorithm is from the "configure"
		# script; the outcome mimics pg_regress.c:make_temp_sockdir().
		PGHOST=$PG_REGRESS_SOCK_DIR
		if [ "x$PGHOST" = x ]; then
			set +e
			dir=`(umask 077 &&
				  mktemp -d /tmp/pg_upgrade_check-XXXXXX) 2>/dev/null`
			if [ ! -d "$dir" ]; then
				dir=/tmp/pg_upgrade_check-$$-$RANDOM
				(umask 077 && mkdir "$dir")
				if [ ! -d "$dir" ]; then
					echo "could not create socket temporary directory in \"/tmp\""
					exit 1
				fi
			fi
			set -e
			PGHOST=$dir
			trap 'rm -rf "$PGHOST"' 0
			trap 'exit 3' 1 2 13 15
		fi
		;;
esac

POSTMASTER_OPTS="-F -c listen_addresses=\"$LISTEN_ADDRESSES\" -k \"$PGHOST\""
export PGHOST

# don't rely on $PWD here, as old shells don't set it
temp_root=`pwd`/tmp_check
rm -rf "$temp_root"
mkdir "$temp_root"

if [ "$1" = '--install' ]; then
	temp_install=$temp_root/install
	bindir=$temp_install/$bindir
	libdir=$temp_install/$libdir

	"$MAKE" -s -C ../.. install DESTDIR="$temp_install"

	# platform-specific magic to find the shared libraries; see pg_regress.c
	LD_LIBRARY_PATH=$libdir:$LD_LIBRARY_PATH
	export LD_LIBRARY_PATH
	DYLD_LIBRARY_PATH=$libdir:$DYLD_LIBRARY_PATH
	export DYLD_LIBRARY_PATH
	LIBPATH=$libdir:$LIBPATH
	export LIBPATH
	SHLIB_PATH=$libdir:$SHLIB_PATH
	export SHLIB_PATH
	PATH=$libdir:$PATH
fi

: ${oldbindir=$bindir}

: ${oldsrc=../../..}
oldsrc=`cd "$oldsrc" && pwd`
newsrc=`cd ../../.. && pwd`

# We need to make pg_regress use psql from the desired installation
# (likely a temporary one), because otherwise the installcheck run
# below would try to use psql from the proper installation directory
# of the target version, which might be outdated or not exist. But
# don't override anything else that's already in EXTRA_REGRESS_OPTS.
EXTRA_REGRESS_OPTS="$EXTRA_REGRESS_OPTS --bindir='$oldbindir'"
export EXTRA_REGRESS_OPTS

PATH=$bindir:$PATH
export PATH

BASE_PGDATA="$temp_root/data"
PGDATA="${BASE_PGDATA}.old"
export PGDATA

# Send installcheck outputs to a private directory.  This avoids conflict when
# check-world runs pg_upgrade check concurrently with src/test/regress check.
# To retrieve interesting files after a run, use pattern tmp_check/*/*.diffs.
outputdir="$temp_root/regress"
EXTRA_REGRESS_OPTS="$EXTRA_REGRESS_OPTS --outputdir=$outputdir"
export EXTRA_REGRESS_OPTS
mkdir "$outputdir"
mkdir "$outputdir"/sql
mkdir "$outputdir"/expected
mkdir "$outputdir"/testtablespace

logdir=`pwd`/log
rm -rf "$logdir"
mkdir "$logdir"

# Clear out any environment vars that might cause libpq to connect to
# the wrong postmaster (cf pg_regress.c)
#
# Some shells, such as NetBSD's, return non-zero from unset if the variable
# is already unset. Since we are operating under 'set -e', this causes the
# script to fail. To guard against this, set them all to an empty string first.
PGDATABASE="";        unset PGDATABASE
PGUSER="";            unset PGUSER
PGSERVICE="";         unset PGSERVICE
PGSSLMODE="";         unset PGSSLMODE
PGREQUIRESSL="";      unset PGREQUIRESSL
PGCONNECT_TIMEOUT=""; unset PGCONNECT_TIMEOUT
PGHOSTADDR="";        unset PGHOSTADDR

# Select a non-conflicting port number, similarly to pg_regress.c
PG_VERSION_NUM=`grep '#define PG_VERSION_NUM' "$newsrc"/src/include/pg_config.h | awk '{print $3}'`
PGPORT=`expr $PG_VERSION_NUM % 16384 + 49152`
export PGPORT

i=0
while psql -X postgres </dev/null 2>/dev/null
do
	i=`expr $i + 1`
	if [ $i -eq 16 ]
	then
		echo port $PGPORT apparently in use
		exit 1
	fi
	PGPORT=`expr $PGPORT + 1`
	export PGPORT
done

# buildfarm may try to override port via EXTRA_REGRESS_OPTS ...
EXTRA_REGRESS_OPTS="$EXTRA_REGRESS_OPTS --port=$PGPORT"
export EXTRA_REGRESS_OPTS

standard_initdb "$oldbindir"/initdb
"$oldbindir"/pg_ctl start -l "$logdir/postmaster1.log" -o "$POSTMASTER_OPTS" -w

# Create databases with names covering the ASCII bytes other than NUL, BEL,
# LF, or CR.  BEL would ring the terminal bell in the course of this test, and
# it is not otherwise a special case.  PostgreSQL doesn't support the rest.
dbname1=`awk 'BEGIN { for (i= 1; i < 46; i++)
	if (i != 7 && i != 10 && i != 13) printf "%c", i }' </dev/null`
# Exercise backslashes adjacent to double quotes, a Windows special case.
dbname1='\"\'$dbname1'\\"\\\'
dbname2=`awk 'BEGIN { for (i = 46; i <  91; i++) printf "%c", i }' </dev/null`
dbname3=`awk 'BEGIN { for (i = 91; i < 128; i++) printf "%c", i }' </dev/null`
createdb "$dbname1" || createdb_status=$?
createdb "$dbname2" || createdb_status=$?
createdb "$dbname3" || createdb_status=$?

if "$MAKE" -C "$oldsrc" installcheck; then
	oldpgversion=`psql -X -A -t -d regression -c "SHOW server_version_num"`

	# before dumping, get rid of objects not existing in later versions
	if [ "$newsrc" != "$oldsrc" ]; then
		fix_sql=""
		case $oldpgversion in
			804??)
				fix_sql="DROP FUNCTION public.myfunc(integer); DROP FUNCTION public.oldstyle_length(integer, text);"
				;;
			*)
				fix_sql="DROP FUNCTION public.oldstyle_length(integer, text);"
				;;
		esac
		psql -X -d regression -c "$fix_sql;" || psql_fix_sql_status=$?
	fi

	pg_dumpall --no-sync -f "$temp_root"/dump1.sql || pg_dumpall1_status=$?

	if [ "$newsrc" != "$oldsrc" ]; then
		# update references to old source tree's regress.so etc
		fix_sql=""
		case $oldpgversion in
			804??)
				fix_sql="UPDATE pg_proc SET probin = replace(probin::text, '$oldsrc', '$newsrc')::bytea WHERE probin LIKE '$oldsrc%';"
				;;
			*)
				fix_sql="UPDATE pg_proc SET probin = replace(probin, '$oldsrc', '$newsrc') WHERE probin LIKE '$oldsrc%';"
				;;
		esac
		psql -X -d regression -c "$fix_sql;" || psql_fix_sql_status=$?

		mv "$temp_root"/dump1.sql "$temp_root"/dump1.sql.orig
		sed "s;$oldsrc;$newsrc;g" "$temp_root"/dump1.sql.orig >"$temp_root"/dump1.sql
	fi
else
	make_installcheck_status=$?
fi
"$oldbindir"/pg_ctl -m fast stop
if [ -n "$createdb_status" ]; then
	exit 1
fi
if [ -n "$make_installcheck_status" ]; then
	exit 1
fi
if [ -n "$psql_fix_sql_status" ]; then
	exit 1
fi
if [ -n "$pg_dumpall1_status" ]; then
	echo "pg_dumpall of pre-upgrade database cluster failed"
	exit 1
fi

PGDATA="$BASE_PGDATA"

standard_initdb 'initdb'

pg_upgrade $PG_UPGRADE_OPTS -d "${PGDATA}.old" -D "$PGDATA" -b "$oldbindir" -B "$bindir" -p "$PGPORT" -P "$PGPORT"

# make sure all directories and files have group permissions, on Unix hosts
# Windows hosts don't support Unix-y permissions.
case $testhost in
	MINGW*) ;;
	*)	if [ `find "$PGDATA" -type f ! -perm 640 | wc -l` -ne 0 ]; then
			echo "files in PGDATA with permission != 640";
			exit 1;
		fi ;;
esac

case $testhost in
	MINGW*) ;;
	*)	if [ `find "$PGDATA" -type d ! -perm 750 | wc -l` -ne 0 ]; then
			echo "directories in PGDATA with permission != 750";
			exit 1;
		fi ;;
esac

pg_ctl start -l "$logdir/postmaster2.log" -o "$POSTMASTER_OPTS" -w

# In the commands below we inhibit msys2 from converting the "/c" switch
# in "cmd /c" to a file system path.

case $testhost in
	MINGW*)	MSYS2_ARG_CONV_EXCL=/c cmd /c analyze_new_cluster.bat ;;
	*)		sh ./analyze_new_cluster.sh ;;
esac

pg_dumpall --no-sync -f "$temp_root"/dump2.sql || pg_dumpall2_status=$?
pg_ctl -m fast stop

if [ -n "$pg_dumpall2_status" ]; then
	echo "pg_dumpall of post-upgrade database cluster failed"
	exit 1
fi

case $testhost in
	MINGW*)	MSYS2_ARG_CONV_EXCL=/c cmd /c delete_old_cluster.bat ;;
	*)	    sh ./delete_old_cluster.sh ;;
esac

if diff "$temp_root"/dump1.sql "$temp_root"/dump2.sql >/dev/null; then
	echo PASSED
	exit 0
else
	echo "Files $temp_root/dump1.sql and $temp_root/dump2.sql differ"
	echo "dumps were not identical"
	exit 1
fi
