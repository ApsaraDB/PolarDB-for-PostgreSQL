#!/bin/bash

# arg1 is local_data, arg2 is polar_data, arg3 is cluster_name or flag "localfs" to use localfs mode
# local_data and polar_data must be full path

pfs=/usr/local/bin/pfs

local_data=$1
polar_data=$2
filenum_polar_data=""
polar_object_type=""
polar_object_type_str=""
FileList=""
pFile=""
cluster_name=""

function pfs_cp_dir()
{
    src_path=$1
    dst_path=$2
    cluster_name=$3

    if [ -z "${src_path}" ] || [ -z "${dst_path}" ]; then
        echo "cp: src_path and dst_path can not be empty"
        exit 1
    fi

    cp_args=""
    if [ ! -z "${cluster_name}" ]; then
        cp_args="${cp_args} -D ${cluster_name}"
    fi

    ${pfs} cp ${cp_args} -r ${src_path} ${dst_path}/
    if [ $? -ne 0 ]; then
        echo "copy ${src_path} to ${dst_path}/ fail"
        exit 1
    fi
}

function rm_dir()
{
    path=$1
    if [ -z "${path}" ]; then
        echo "path can not be empty"
        exit 1
    fi

    echo "delete ${path}"
    rm -rf ${path}
    if [ $? -ne 0 ]; then
            echo "delete ${path} fail"
            exit 1
    fi
}

if [ "$3" == "localfs" ]; then
	echo "use localfs mode"
	localfs_mode="on"
elif [ "$3" != "" ]; then
    echo "use pfs cluster $3"
    cluster_name="$3"
    pfs_common_args="${pfs_common_args} -C $3"
fi

#check arg1 arg2
begin=`echo ${local_data:0:1}`
end=`echo ${local_data:0-1}`

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

# check pfs commamd line in pfs mode
if [ ! -z "${localfs_mode}" ]; then
	pfs=""
elif [ ! -f "${pfs}" ]; then
	echo "pfs does not exist"
	exit 1
fi

# check local_data
if [ ! -d "${local_data}" ]; then
	echo "local data :("${local_data}") dir does not exist"
	exit 1
else
	if [ ! -f "${local_data}/postgresql.conf" ]; then
		echo "postgresql.conf in local data dir does not exist"
		exit 1
	fi

	if [ ! -d "${local_data}/pg_twophase" ]; then
		echo "pg_twophase in local data dir does not exist"
		exit 1
	fi

	if [ ! -d "${local_data}/pg_wal" ]; then
		echo "pg_wal in local data dir does not exist"
		exit 1
	fi

	if [ ! -d "${local_data}/pg_logindex" ]; then
		echo "pg_logindex in local data dir does not exist"
		exit 1
	fi

	if [ ! -f "${local_data}/global/pg_control" ]; then
		echo "pg_control in local data dir does not exist"
		exit 1
	fi

	if [ ! -d "${local_data}/pg_xact" ]; then
		echo "pg_xact in local data dir does not exist"
		exit 1
	fi

fi

#check polar_data in pfs mode
if [ -z "${localfs_mode}" ]; then
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

	filenum_polar_data=`${pfs} ${pfs_common_args} ls ${polar_data} | wc -l`
	if [[ ${filenum_polar_data} -ne 1 ]]; then
    	    echo "${polar_data} dir not empty"
        	exit 1
	fi
else
	`stat ${polar_data} 1>/dev/null 2>&1`
	if [ $? -ne 0 ]; then
		echo "${polar_data} dir does not exist"
		exit 1
	elif [ ! -d "${polar_data}" ]; then
		echo "${polar_data} is not dir"
		exit 1
	else
		filenum_polar_data=`ls ${polar_data} | wc -l`
		if [[ ${filenum_polar_data} -ne 0 ]]; then
	    	    echo "${polar_data} dir not empty"
    	    	exit 1
		fi
	fi
fi

# do copy work
pfs_cp_dir ${local_data}/base ${polar_data} ${cluster_name}
pfs_cp_dir ${local_data}/global ${polar_data} ${cluster_name}
pfs_cp_dir ${local_data}/pg_tblspc ${polar_data} ${cluster_name}
pfs_cp_dir ${local_data}/pg_wal ${polar_data} ${cluster_name}
pfs_cp_dir ${local_data}/pg_logindex ${polar_data} ${cluster_name}
pfs_cp_dir ${local_data}/pg_twophase ${polar_data} ${cluster_name}
pfs_cp_dir ${local_data}/pg_xact ${polar_data} ${cluster_name}
pfs_cp_dir ${local_data}/pg_commit_ts ${polar_data} ${cluster_name}
pfs_cp_dir ${local_data}/pg_multixact ${polar_data} ${cluster_name}
pfs_cp_dir ${local_data}/pg_csnlog ${polar_data} ${cluster_name}
pfs_cp_dir ${local_data}/polar_dma ${polar_data} ${cluster_name}

# do delete data
FileList=`ls ${local_data}/base/ 2>/dev/null`

echo "list base dir file $FileList"
for pFile in $FileList
do
    rm_dir ${local_data}base/${pFile}/*
done
rm_dir ${local_data}global/*
rm_dir ${local_data}pg_wal
rm_dir ${local_data}pg_logindex
rm_dir ${local_data}pg_twophase
rm_dir ${local_data}pg_xact
rm_dir ${local_data}pg_commit_ts
rm_dir ${local_data}pg_multixact
rm_dir ${local_data}pg_csnlog
rm_dir ${local_data}polar_dma

if [ ! -d "${local_data}pg_tblspc" ]; then
        echo "${polar_data}pg_tblspc not a folder"
        exit 1
fi

echo "init polarDB data dir success"

exit 0
