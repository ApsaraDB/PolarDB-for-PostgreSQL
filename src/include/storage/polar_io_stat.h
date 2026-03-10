/*-------------------------------------------------------------------------
 *
 * polar_io_stat.h
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
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
 *	  src/include/storage/polar_io_stat.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef POLAR_IO_STAT_H
#define POLAR_IO_STAT_H

#include "postgres.h"

#include "port/atomics.h"
#include "portability/instr_time.h"
#include "storage/bufmgr.h"
#include "storage/proc.h"

/*
 * this is a struct that stat polar processes about io opreation.
 */
typedef struct polar_proc_io_stat
{
	uint64		io_number_read; /* Total count of read the file */
	uint64		io_number_write;	/* Total count of write the file */
	uint64		io_throughtput_read;	/* Total Bytes of read */
	uint64		io_throughtput_write;	/* Total Bytes of write */
	instr_time	io_latency_read;	/* Total time cost of read the file */
	instr_time	io_latency_write;	/* Total time cost of read the file */
	uint64		io_open_num;	/* Total number of open the file */
	uint64		io_close_num;	/* Total number of close the file */
	instr_time	io_open_time;	/* Total time cost of open file, but we don't
								 * use this */
	instr_time	io_seek_time;
	uint64		io_seek_count;
	instr_time	io_creat_time;
	uint64		io_creat_count;
	instr_time	io_fsync_time;
	uint64		io_fsync_count;
	instr_time	io_falloc_time;
	uint64		io_falloc_count;
}			polar_proc_io_stat;

enum polar_io_latency_interval
{
	LATENCY_200 = 0,
	LATENCY_400 = 1,
	LATENCY_600 = 2,
	LATENCY_800 = 3,
	LATENCY_1ms = 4,
	LATENCY_10ms = 5,
	LATENCY_100ms = 6,
	LATENCY_OUT = 7,

	LATENCY_INTERVAL_LEN = 8
};
enum polar_io_latency_kind
{
	LATENCY_read = 0,
	LATENCY_write = 1,
	LATENCY_open = 2,
	LATENCY_seek = 3,
	LATENCY_creat = 4,
	LATENCY_fsync = 5,
	LATENCY_falloc = 6,

	LATENCY_KIND_LEN = 7
};


/*
 * polar IO type
 * At present, we mainly count several secondary directories,
 * and the demand is like this.
 */
enum polar_io_type
{
	POLAR_IO_WAL = 0,
	POLAR_IO_DATA = 1,
	POLAR_IO_CLOG = 2,
	POLAR_IO_GLOBAL = 3,
	POLAR_IO_LOGINDEX = 4,
	POLAR_IO_MULTIXACT = 5,
	POLAR_IO_TWOPHASE = 6,
	POLAR_IO_REPLSOT = 7,
	POLAR_IO_SNAPSHOTS = 8,
	POLAR_IO_SUBTRANS = 9,
	POLAR_IO_OTHER = 10,
	/* NB: Define the size here for future maintenance */
	POLAR_IO_TYPE_SIZE = 11
};

/*
 * polar IO location
 */
enum polar_io_loc
{
	POLAR_IO_LOCAL = 0,
	POLAR_IO_SHARED = 1,
	/* NB: Define the size here for future maintenance */
	POLAR_IO_LOC_SIZE = 2
};

typedef struct polar_proc_io
{
	/*
	 * We do statistics IO for every process We do three dimensions of IO
	 * statistics so that we can make a more detailed and specific analysis.
	 * there are follows: processes ,file type(data,wal,clog),file
	 * location(local, polar file system) NB : we initialized this struct
	 * arrarys when create global memory， and memset this in child processes
	 * Init (ProcessInit or AuxiliaryInit) again. so ,we will save these value
	 * to ProcGobal when child processes is destroyed. we can make an accurate
	 * summary convenient.
	 */
	int			pid;
	polar_proc_io_stat polar_proc_io_stat_dist[POLAR_IO_TYPE_SIZE][POLAR_IO_LOC_SIZE];
	uint64		num_latency_dist[POLAR_IO_LOC_SIZE][LATENCY_KIND_LEN][LATENCY_INTERVAL_LEN];
}			polar_proc_io;

extern polar_proc_io * polar_io_stat_array;

/*
 * POLAR: use this struct to collect io read throughtput using for crash recovery
 * rto optimizer.
 */
#define POLAR_PROC_GLOBAL_IO_READ_LEN 10
#define POLAR_IO_READ_STATISTICS_TIMES 1000

typedef struct polar_proc_global_io_read
{
	int			count;			/* How many times for statistics collecting */
	uint64		delta_io_read_size; /* The delta io read size in a statistics
									 * period */
	instr_time	delta_io_read_time; /* The delta io read time in a statistics
									 * period */
	uint64		io_read_size_avg;	/* The average of io read size in a
									 * statistics period */
	double		io_read_time_avg;	/* The average of io read time in a
									 * statistics period */
	/* The top POLAR_PROC_GLOBAL_IO_READ_LEN of io read throughtput in history */
	uint64		io_read_throughtput[POLAR_PROC_GLOBAL_IO_READ_LEN];
	uint64		max_throughtput;	/* Max throughtput in io_read_throughtput */
	bool		enabled;		/* flags to judge if it is enabled */
	uint64		force_delay;	/* statistics infomation for polar_monitor */
	uint64		less_than_delay;	/* statistics infomation for polar_monitor */
	uint64		more_than_delay;	/* statistics infomation for polar_monitor */
	pg_atomic_uint32 lock;		/* flags to judge if it is in the statistics
								 * caculate */
}			polar_proc_global_io_read_t;

typedef struct polar_collect_io_stat
{
	int64		shared_read_ps;
	int64		shared_write_ps;
	int64		shared_read_throughput;
	int64		shared_write_throughput;
	double		shared_read_latency;
	double		shared_write_latency;
	int64		io_open_num;
	int64		io_seek_count;
	double		io_open_time;
	double		io_seek_time;
}			polar_collect_io_stat_t;

typedef struct PolarWriteCombineStat
{
	uint64		times[MAX_BUFFERS_TO_WRITE_BY];
	uint64		counts[MAX_BUFFERS_TO_WRITE_BY];
} PolarWriteCombineStat;

extern polar_proc_global_io_read_t * polar_global_io_read_stats;
extern PolarWriteCombineStat *polar_write_combine_stat;

extern void polar_io_stat_shmem_startup(void);
extern int	polar_io_kind_to_location(int kind);
extern int	polar_collect_io_stat(polar_collect_io_stat_t * io_stat_info, int backendid);
extern void polar_register_io_shmem_exit_cleanup(void);
extern Size polar_write_combine_stat_shmem_size(void);
extern void polar_write_combine_stat_shmem_init(void);
extern void polar_write_combine_stat_count_time(int nblocks, instr_time io_start);

#define POLAR_MAX_PROC_IO_STAT_SLOTS (MaxBackends + NUM_AUXILIARY_PROCS + 1)
#define CHECK_PROC_IO_BACKENDID_VALID(backendid) (backendid >= 1 && backendid < POLAR_MAX_PROC_IO_STAT_SLOTS)

/* 0-th slot cannot be used as a normal backend stat slot */
#define POLAR_PROC_IO_STAT_BACKEND_INDEX() \
    (CHECK_PROC_IO_BACKENDID_VALID(MyProcNumber + 1) ? MyProcNumber + 1 : -1)

#endif							/* POLAR_IO_STAT_H */
