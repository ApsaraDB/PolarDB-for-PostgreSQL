/*-------------------------------------------------------------------------
 *
 * polar_io_stat.c
 *
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * IDENTIFICATION
 *	  external/polar_vfs/polar_io_stat.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xlog.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/polar_io_stat.h"
#include "storage/sinvaladt.h"
#include "tcop/utility.h"
#include "utils/builtins.h"


static void polar_io_stat_shmem_shutdown(int code, Datum arg);

static void POLARProcIOStatAdd(PolarProcIOStat x[][POLARIO_LOC_SIZE], PolarProcIOStat y[][POLARIO_LOC_SIZE]);
static void NumLatencyDistAdd(uint64 x[][LATENCY_INTERVAL_LEN], uint64 y[][LATENCY_INTERVAL_LEN]);
static void print_current_process_io_info(int io_type, const char *pro_name, const char *polar_dir_type_name);
static void polario_location_register(int kind, int loc);

/* Links to shared memory state */
POLAR_PROC_IO    *PolarIOStatArray = NULL;
int	polar_kind_to_location[POLAR_VFS_KIND_SIZE];

/*
 * shmem_startup hook: allocate or attach to shared memory,
 */
void 
polar_io_stat_shmem_startup(void)
{
    bool		found = false;
	Size		total_size = 0;

	/* The 0th index is used as a summary statistics */
	total_size =  mul_size(sizeof(POLAR_PROC_IO), PolarNumProcIOStatSlots);
	/*
	 * Create or attach to the shared memory state
	 */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	PolarIOStatArray = ShmemInitStruct("PolarIOStatArray",
						  total_size, &found);

	elog(LOG, "PolarIOStatArray share memory total size is %d", (int)total_size);

	if (!found)
	{
		int i;
		/* First time through ... */
		memset(PolarIOStatArray, 0, total_size);
		/* POLAR: polar io stat register all kinds of vfs interfaces. */
		for (i = 0; i < POLAR_VFS_KIND_SIZE; i ++)
			polar_kind_to_location[i] = -1;
		polario_location_register(POLAR_VFS_LOCAL_BIO, POLARIO_LOCAL);
#ifdef USE_PFSD
		polario_location_register(POLAR_VFS_PFS, POLARIO_SHARED);
#endif
		polario_location_register(POLAR_VFS_LOCAL_DIO, POLARIO_SHARED);
	}

	PolarGlobalIOReadStats = ShmemInitStruct("PolarGlobalIOReadStats", sizeof(POLAR_PROC_GLOBAL_IO_READ), &found);

	elog(LOG, "PolarGlobalIOReadStats share memory size is %ld", sizeof(POLAR_PROC_GLOBAL_IO_READ));

	if (!found)
	{
		memset(PolarGlobalIOReadStats, 0, sizeof(POLAR_PROC_GLOBAL_IO_READ) - sizeof(pg_atomic_uint32));
		pg_atomic_init_u32(&PolarGlobalIOReadStats->lock, 0);
	}

	LWLockRelease(AddinShmemInitLock);
}

void polar_io_shmem_exit_cleanup(void)
{
	on_shmem_exit(polar_io_stat_shmem_shutdown, (Datum) 0);
}

static void 
polar_io_stat_shmem_shutdown(int code, Datum arg)
{
	int my_proc_index;
   	/* Safety check ... shouldn't get here unless shmem is set up. */
	if (!PolarIOStatArray)
		return;

	if (MyBackendId == 0)
    {
		elog(LOG, "Excepted, backendid(0) exit.");
	}

	my_proc_index = polar_get_io_proc_index();

	if (AmStartupProcess())
	{
		print_current_process_io_info(POLARIO_TYPE_SIZE, "startup process", "Total");
		print_current_process_io_info(POLARIO_WAL, "startup process", "WAL");
		print_current_process_io_info(POLARIO_DATA, "startup process", "DATA");
	}

	if (my_proc_index > 0)
	{
		/* We think that 0 of backendid is not used by the process 
        * or by the master, so we will summarize the summary information 
        * of IO here so that we can display it in the view. 
        * NB: This process does not require locked because we don't need
        * to care about data consistency.
        */
		POLARProcIOStatAdd(PolarIOStatArray[0].polar_proc_io_stat_dist, PolarIOStatArray[my_proc_index].polar_proc_io_stat_dist);
		NumLatencyDistAdd(PolarIOStatArray[0].num_latency_dist, PolarIOStatArray[my_proc_index].num_latency_dist);
		MemSet(&PolarIOStatArray[my_proc_index], 0, sizeof(POLAR_PROC_IO));
	}
}

/* Add two PLOARPROCIOSTATs*/
static void 
POLARProcIOStatAdd(PolarProcIOStat x[][POLARIO_LOC_SIZE], PolarProcIOStat y[][POLARIO_LOC_SIZE])
{
	int 		i;
	int 		j;
	for (i = 0; i < POLARIO_TYPE_SIZE; i++)
		for (j = 0; j < POLARIO_LOC_SIZE; j++)
		{
			x[i][j].io_number_read += y[i][j].io_number_read; 
			x[i][j].io_number_write += y[i][j].io_number_write; 
			x[i][j].io_throughtput_read += y[i][j].io_throughtput_read; 
			x[i][j].io_throughtput_write += y[i][j].io_throughtput_write; 
			INSTR_TIME_ADD(x[i][j].io_latency_read, y[i][j].io_latency_read); 
			INSTR_TIME_ADD(x[i][j].io_latency_write, y[i][j].io_latency_write);
			x[i][j].io_open_num += y[i][j].io_open_num; 
			x[i][j].io_close_num += y[i][j].io_close_num; 
			INSTR_TIME_ADD(x[i][j].io_open_time, y[i][j].io_open_time); 
			INSTR_TIME_ADD(x[i][j].io_seek_time, y[i][j].io_seek_time);
			x[i][j].io_seek_count += y[i][j].io_seek_count; 
			INSTR_TIME_ADD(x[i][j].io_creat_time, y[i][j].io_creat_time); 
			INSTR_TIME_ADD(x[i][j].io_falloc_time, y[i][j].io_falloc_time); 
			INSTR_TIME_ADD(x[i][j].io_fsync_time, y[i][j].io_fsync_time); 
			x[i][j].io_creat_count += y[i][j].io_creat_count; 
			x[i][j].io_fsync_count += y[i][j].io_fsync_count; 
			x[i][j].io_falloc_count += y[i][j].io_falloc_count; 
		}
}

/* Add two PLOARPROCIOSTATs*/
static void 
NumLatencyDistAdd(uint64 x[][LATENCY_INTERVAL_LEN], uint64 y[][LATENCY_INTERVAL_LEN])
{
	int 		i;
	int 		j;
	for (i = 0; i < LATENCY_KIND_LEN; i++)
		for (j = 0; j < LATENCY_INTERVAL_LEN; j++)
		{
			x[i][j] += y[i][j];
		}
}

/* Calculate the I/O statistical index of the current process */
int
polar_get_io_proc_index(void)
{
	return MyBackendId == InvalidBackendId ?
		(MyAuxProcType ==  NotAnAuxProcess ? -1 : MaxBackends + MyAuxProcType + 1)
		: MyBackendId;
}

static void 
print_current_process_io_info(int io_type, const char *pro_name, const char *polar_dir_type_name)
{
	int 	backendid = polar_get_io_proc_index();
	bool 	once = true;
	int		index = io_type;
	instr_time vfs_open_time, vfs_write_time, vfs_read_time,
			vfs_seek_time, vfs_fsync_time;
	uint64 vfs_open_count, vfs_write_count, vfs_read_count,
		 	vfs_seek_count, vfs_fsync_count;
	PolarProcIOStat *tmp_stat;

	if (backendid == -1)
		return;

	/*no cover begin*/
	if (io_type > POLARIO_TYPE_SIZE || io_type < 0 )
		return;
	/*no cover end*/

	/* initial and clean up*/
	vfs_open_count = 0;
	vfs_write_count = 0;
	vfs_read_count = 0;
	vfs_seek_count = 0;
	vfs_fsync_count = 0;
	INSTR_TIME_SET_ZERO(vfs_open_time);
	INSTR_TIME_SET_ZERO(vfs_write_time);
	INSTR_TIME_SET_ZERO(vfs_read_time);
	INSTR_TIME_SET_ZERO(vfs_seek_time);
	INSTR_TIME_SET_ZERO(vfs_fsync_time);

	if (io_type == POLARIO_TYPE_SIZE)
	{
		once = false;
		index = 0;
	}

	do
	{
		int i;
		for (i = 0; i < POLARIO_LOC_SIZE; i ++)
		{
			tmp_stat = &PolarIOStatArray[backendid].polar_proc_io_stat_dist[index][i];
			INSTR_TIME_ADD(vfs_open_time, tmp_stat->io_open_time);
			INSTR_TIME_ADD(vfs_write_time, tmp_stat->io_latency_write);
			INSTR_TIME_ADD(vfs_read_time, tmp_stat->io_latency_read);
			INSTR_TIME_ADD(vfs_seek_time, tmp_stat->io_seek_time);
			INSTR_TIME_ADD(vfs_fsync_time, tmp_stat->io_fsync_time);
			vfs_open_count += tmp_stat->io_open_num;
			vfs_read_count += tmp_stat->io_number_read;
			vfs_write_count += tmp_stat->io_number_write;
			vfs_seek_count += tmp_stat->io_seek_count;
			vfs_fsync_count += tmp_stat->io_fsync_count;
		}
	} while (!once && (++index) < POLARIO_TYPE_SIZE);

	elog(LOG, "%s %s IO follows:"
			  "open %.6lf s, count %lu, "
			  "write %.6lf s, count %lu, "
			  "read %.6lf s, count %lu, "
			  "lseek %.6lf s, count %lu,"
			  "fsync %.6lf s, count %lu",
		 pro_name, polar_dir_type_name,
		 INSTR_TIME_GET_DOUBLE(vfs_open_time), vfs_open_count,
		 INSTR_TIME_GET_DOUBLE(vfs_write_time), vfs_write_count,
		 INSTR_TIME_GET_DOUBLE(vfs_read_time), vfs_read_count,
		 INSTR_TIME_GET_DOUBLE(vfs_seek_time), vfs_seek_count,
		 INSTR_TIME_GET_DOUBLE(vfs_fsync_time), vfs_fsync_count);
}

static void
polario_location_register(int kind, int loc)
{
	if ((kind >= 0 && kind < POLAR_VFS_KIND_SIZE) &&
		(loc >= 0 && loc < POLARIO_LOC_SIZE))
		polar_kind_to_location[kind] = loc;
	else
		elog(ERROR, "polario_location_register error at "
					"kind = %d, loc = %d, "
					"POLAR_VFS_KIND_SIZE = %d, "
					"POLARIO_LOC_SIZE = %d",
					kind, loc, POLAR_VFS_KIND_SIZE, POLARIO_LOC_SIZE);
}

int
polario_kind_to_location(int kind)
{
	int loc = -1;
	if (kind >= 0 && kind < POLAR_VFS_KIND_SIZE)
		loc = polar_kind_to_location[kind];
	return loc;
}


/*
 * collect the statistics of backends.
 *
 * We need to check the result, when call this funcition.
 * The value of result, -1 means fail, 0 means succeed.
 */
int
polar_collect_io_stat(PolarCollectIoStat *io_stat_info, int backendid)
{
	
	LocalPgBackendStatus *local_beentry;
	PgBackendStatus *beentry;
	int  			index = 0;

	uint64 			shared_read_ps = 0;
	uint64 			shared_write_ps = 0;
	uint64			shared_read_throughput = 0;
	uint64			shared_write_throughput = 0;
	instr_time		shared_read_latency ;
	instr_time		shared_write_latency ;
	int64			io_open_num = 0;
	int64			io_seek_count = 0;
	instr_time		io_open_time;
	instr_time		io_seek_time;


	INSTR_TIME_SET_ZERO(shared_read_latency);
	INSTR_TIME_SET_ZERO(shared_write_latency);
	INSTR_TIME_SET_ZERO(io_open_time);
	INSTR_TIME_SET_ZERO(io_seek_time);


	/* Get the next one in the list */
	local_beentry = pgstat_fetch_stat_local_beentry(backendid);
	if (!local_beentry)
	{
		/* Ignore if local_beentry type is empty */
		return -1;
	}

	beentry = &local_beentry->backendStatus;
	if (!beentry)
	{
		/* Ignore if beentry type is empty */
		return -1;
	}

	/* Each process accumulates it‘s file type by file location */
	if (PolarIOStatArray)
	{
		for (index = 0; index < POLARIO_TYPE_SIZE; index++)
		{

			shared_read_ps += PolarIOStatArray[backendid].polar_proc_io_stat_dist[index][POLARIO_SHARED].io_number_read;
			shared_write_ps += PolarIOStatArray[backendid].polar_proc_io_stat_dist[index][POLARIO_SHARED].io_number_write;
			shared_read_throughput += PolarIOStatArray[backendid].polar_proc_io_stat_dist[index][POLARIO_SHARED].io_throughtput_read;
			shared_write_throughput += PolarIOStatArray[backendid].polar_proc_io_stat_dist[index][POLARIO_SHARED].io_throughtput_write;
			INSTR_TIME_ADD(shared_read_latency, PolarIOStatArray[backendid].polar_proc_io_stat_dist[index][POLARIO_SHARED].io_latency_read);
			INSTR_TIME_ADD(shared_write_latency, PolarIOStatArray[backendid].polar_proc_io_stat_dist[index][POLARIO_SHARED].io_latency_write);

			io_open_num += PolarIOStatArray[backendid].polar_proc_io_stat_dist[index][POLARIO_SHARED].io_open_num;
			io_seek_count += PolarIOStatArray[backendid].polar_proc_io_stat_dist[index][POLARIO_SHARED].io_seek_count;

			INSTR_TIME_ADD(io_open_time, PolarIOStatArray[backendid].polar_proc_io_stat_dist[index][POLARIO_SHARED].io_open_time);
			INSTR_TIME_ADD(io_seek_time, PolarIOStatArray[backendid].polar_proc_io_stat_dist[index][POLARIO_SHARED].io_seek_time);

		}

		/* pfs iops */
		io_stat_info->shared_read_ps = shared_read_ps;
		io_stat_info->shared_write_ps = shared_write_ps;

		/* pfs io throughput */
		io_stat_info->shared_read_throughput = shared_read_throughput;
		io_stat_info->shared_write_throughput = shared_write_throughput;

		/* pfs io latency */
		io_stat_info->shared_read_latency = INSTR_TIME_GET_MILLISEC(shared_read_latency);
		io_stat_info->shared_write_latency = INSTR_TIME_GET_MILLISEC(shared_write_latency);

		/* open and seek count */
		io_stat_info->io_open_num = io_open_num;
		io_stat_info->io_seek_count = io_seek_count;

		/* open and seek time */
		io_stat_info->io_open_time = INSTR_TIME_GET_MILLISEC(io_open_time);
		io_stat_info->io_seek_time = INSTR_TIME_GET_MILLISEC(io_seek_time);

	}

	return 0;
}