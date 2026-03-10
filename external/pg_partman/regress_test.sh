#!/bin/sh
#
# regress test script for pg_partman
#
DBNAME=partman_db
createdb ${DBNAME}
psql ${DBNAME} -c "create schema partman; create extension pg_partman with schema partman;create extension pgtap with schema public;"
psql ${DBNAME} -c "alter database ${DBNAME} set max_locks_per_transaction = 64;"
if [ -f partman.log ] ;then 
    rm -f partman.log 
fi

for f in `find test -maxdepth 1 -name *.sql ` 
do
    # clear role partman_basic
    psql -d ${DBNAME} -c "drop role if exists partman_basic;" > /dev/null 2>&1
    psql -d ${DBNAME} -Xf $f >> partman.log 2>&1
    if [ ! $? -eq 0 ] ; then
        echo $f ... Failed
        exit $?
    fi 
    echo $f ... OK
done
dropdb ${DBNAME}
