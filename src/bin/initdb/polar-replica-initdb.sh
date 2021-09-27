#!/bin/bash

# arg is local_data arg2 is polar_data
# local_data and polar_data must be full path

pfs=/usr/local/bin/pfs

local_data=$2
polar_data=$1
filenum_polar_data=""
polar_object_type=""
polar_object_type_str=""
FileList=""
pFile=""
cluster_name=""
pfs_common_args=""

#check arg1 arg2
begin=`echo ${local_data:0:1}`
end=`echo ${local_data:0-1}`

if [ "$3" != "" ]; then
    echo "use pfs cluster $3"
    cluster_name="$3"
    pfs_common_args="${pfs_common_args} -C $3"
fi

if [ "$begin" != '/' ]; then
        echo "${local_data} must must be the full path"
        exit 1
fi

if [ "$end" != '/' ]; then
        echo "${local_data} must end with /"
        exit 1
fi

begin=`echo ${polar_data:0:1}`
end=`echo ${polar_data:0-1}`

if [ "$begin" != '/' ]; then
        echo "${polar_data} must be the full path"
        exit 1
fi

if [ "$end" != '/' ]; then
        echo "${polar_data} must end with /"
        exit 1
fi

# check pfs commamd line
if [ ! -f "${pfs}" ]; then
	echo "pfs does not exist"
	exit 1
fi

# check local_data
if [ ! -d "${local_data}" ]; then
        echo "local data :("${local_data}") dir does not exist"
        exit 1
else
	if [ -f "${local_data}/postgresql.conf" ]; then
        	echo "postgresql.conf in local exist in local dir"
        	exit 1
	fi

fi

#check polar_data
`${pfs} ${pfs_common_args} stat ${polar_data} 1>/dev/null 2>&1`
if [ $? -ne 0 ]; then
	echo "${polar_data} dir does not exist"
	exit 1
fi

polar_object_type_str=`${pfs} ${pfs_common_args} stat ${polar_data}`
polar_object_type=`expr substr "${polar_object_type_str}" 1 6`
if [ "$polar_object_type" != '   dir' ]; then
        echo "${polar_data} is not dir"
        exit 1
fi

# do mkdir dir in data
mkdir -p ${local_data}base
mkdir -p ${local_data}global
mkdir -p ${local_data}pg_dynshmem
mkdir -p ${local_data}pg_log
mkdir -p ${local_data}pg_logical/mappings
mkdir -p ${local_data}pg_logical/snapshots
mkdir -p ${local_data}pg_notify
mkdir -p ${local_data}pg_replslot
mkdir -p ${local_data}pg_serial
mkdir -p ${local_data}pg_snapshots
mkdir -p ${local_data}pg_stat
mkdir -p ${local_data}pg_stat_tmp
mkdir -p ${local_data}pg_subtrans
mkdir -p ${local_data}pg_tblspc
mkdir -p ${local_data}pg_twophase

echo "11" > ${local_data}PG_VERSION

if [ ! -d "${local_data}base" ]; then
        echo "local data :("${local_data}base") dir does not exist"
        exit 1
fi

if [ ! -d "${local_data}pg_twophase" ]; then
        echo "local data :("${local_data}pg_twophase") dir does not exist"
        exit 1
fi

if [ ! -f "${local_data}PG_VERSION" ]; then
        echo "local data :("${local_data}PG_VERSION") does not exist"
        exit 1
fi

# do mkdir dir in base
FileList=`${pfs} ${pfs_common_args} ls ${polar_data}/base/ | awk '{print $9}'`

#base dir
echo "list base dir file $FileList"
for pFile in $FileList
do
echo 　"mkdir ${local_data}base/${pFile}"  
mkdir -p ${local_data}base/${pFile}
if [ $? -ne 0 ]; then
        echo "mkdir ${local_data}base/${pFile} fail"
        exit 1
fi
done

echo "init polarDB replica mode dir success"

exit 0
