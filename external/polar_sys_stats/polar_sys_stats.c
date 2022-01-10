/*-------------------------------------------------------------------------
 *
 * polar_sys_stats.c
 *	  Planner needs to know the hardware capability for a better plan.
 *	  Currently PG let user to set it, and this extension is designed to
 *	  set them automatically. We start with random_page_cost, which looks
 *	  have top-1 impacts on planner.
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
 *	  external/polar_sys_stats/polar_sys_stats.c
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "access/heapam.h"
#include "miscadmin.h"
#include "catalog/pg_class.h"
#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/polar_fd.h"
#include "storage/smgr.h"
#include "utils/guc.h"
#include "utils/rel.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;

Datum polar_random_page_cost(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(polar_random_page_cost);


static char *file_name_with_polardir(const char* filename);
static void polar_allocate_file(const char* filename,  long size, bool *file_created);
static void polar_invalidate_device_cache(long device_cache_size);
static double polar_bulk_read_lat(long file_size);
static double polar_random_rand_lat(long file_size);
static double polar_buffer_cache_lat(void);
static void polar_clean_sysstat_testfiles(int code, Datum arg);
static double get_real_lat(double cache_hit_ratio, double disk_lat, double cache_lat);

const char* test_data_file = "polar_sys_stat_test.dat";
const char* device_cache_invalid_file = "polar_sys_stat_dummy.dat";

bool test_file_created = false;
bool dummy_file_created = false;

void _PG_init(void);

void _PG_init(void)
{
	on_proc_exit(polar_clean_sysstat_testfiles, 0);
}


Datum
polar_random_page_cost(PG_FUNCTION_ARGS)
{
	long test_file_size = PG_GETARG_INT64(0);
	long device_cache = PG_GETARG_INT64(1);
	/*
	 * Since pfs doesn't have file system cache,  so we only need
	 * to take care of shared buffer hit ratio
	 */
	long cache_hit_ratio = PG_GETARG_FLOAT4(2);

	double bulk_read_lat, random_read_lat, shared_buf_lat;
	float8 random_page_cost;

	/*
	 * In case there are some remaining files in this backend, we remove it for now.
	 * like progrom raises ERROR between create file and clean files
	 */
	polar_clean_sysstat_testfiles(0, (Datum) NULL);

	polar_allocate_file(file_name_with_polardir(test_data_file),
						test_file_size,
						&test_file_created);
	polar_invalidate_device_cache(device_cache);
	bulk_read_lat = polar_bulk_read_lat(test_file_size);
	random_read_lat = polar_random_rand_lat(test_file_size);
	polar_clean_sysstat_testfiles(0, (Datum) NULL);
	shared_buf_lat = polar_buffer_cache_lat();
	random_page_cost = get_real_lat(cache_hit_ratio, random_read_lat, shared_buf_lat)
		/ get_real_lat(cache_hit_ratio, bulk_read_lat, shared_buf_lat);
	if (bulk_read_lat > random_read_lat)
	{
		elog(ERROR, "Average bulk read latency %f is greater than single random read latency %f",
			 bulk_read_lat,
			 random_read_lat);
	}
	/*
	 * This algorithm is still in testing stage, so no worth to provide
	 * very friendly interface for now.
	 */
	elog(INFO, "use SELECT ((1 - cache_hit_ratio) * %f + cache_hit_ratio * %f) / ((1 - cache_hit_ratio) * %f + cache_hit_ratio * %f\n to get random_page_cost with different cache hit ratio.)",
		 random_read_lat,
		 shared_buf_lat,
		 bulk_read_lat,
		 shared_buf_lat
		);
	return Float8GetDatum(random_page_cost);
}


static void
polar_allocate_file(const char* filename, long size, bool *file_created)
{
	int file;
	int blksz = 1024 * 1024;
	char *buf = palloc0(blksz);
	long current_size = 0;
	int ret;
	/* Race condition safe */
	file = polar_open(filename, O_WRONLY | O_CREAT | O_EXCL , 0600);
	if (file < 0)
		elog(ERROR, "open file %s failed: %s.", filename, strerror(errno));
	*file_created = true;
	do
	{
		ret = polar_write(file, buf, blksz);
		if (ret < 0)
		{
			polar_close(file);
			elog(ERROR, "write file %s failed: %s.", filename, strerror(errno));
		}

		current_size += ret;
	} while(current_size < size);
	polar_close(file);
}


static void
polar_invalidate_device_cache(long device_cache_size)
{
	if (device_cache_size > 0)
		polar_allocate_file(file_name_with_polardir(device_cache_invalid_file),
							device_cache_size, &dummy_file_created);
}


static long
polar_get_time_diff(TimestampTz start_tm,  TimestampTz end_tm)
{
	long secs;
	int microsecs;
	long res;
	TimestampDifference(start_tm, end_tm, &secs, &microsecs);
	res = secs * 1000000 + microsecs;
	return res;
}


static double
polar_bulk_read_lat(long file_size)
{
	long current_size = 0;
	long seq_buf_len = polar_bulk_read_size ?
		polar_bulk_read_size * BLCKSZ : BLCKSZ;
	char *seq_read_buf = palloc(seq_buf_len);
	TimestampTz start_tm, end_tm;
	int ret;

	int file = polar_open(test_data_file, O_RDONLY, 0600);
	if (file < 0)
		elog(ERROR, "Open test file %s failed.", test_data_file);

	start_tm = GetCurrentTimestamp();
	do
	{
		ret = polar_read(file, seq_read_buf, seq_buf_len);
		if (ret < 0)
		{
			polar_close(file);
			elog(ERROR, "READ test file %s failed.", test_data_file);
		}
		else if (ret == 0)
		{
			polar_close(file);
			elog(ERROR, "Reach EOF unexpectedly. Current size: %ld  Expected Size: %ld",
				 current_size,
				 file_size);
		}

		current_size += ret;
	} while (current_size < file_size);
	end_tm = GetCurrentTimestamp();

	polar_close(file);

	return polar_get_time_diff(start_tm, end_tm) / ((file_size / BLCKSZ) * 1.0);
}


static double
polar_random_rand_lat(long file_size)
{
	long current_size = 0;
	char *buf = palloc(BLCKSZ);
	TimestampTz start_tm, end_tm;
	int ret;
	off_t offset;

	int file = polar_open(test_data_file, O_RDONLY, 0600);
	if (file < 0)
		elog(ERROR, "Open test file %s failed.", test_data_file);

	start_tm = GetCurrentTimestamp();

	do
	{
			offset = rand() % (file_size / BLCKSZ) * BLCKSZ;
			if (polar_enable_pread)
			{
				ret = polar_pread(file, buf, BLCKSZ, offset);
			}
			else
			{
				if (polar_lseek(file, offset, SEEK_SET) != offset)
				{
					polar_close(file);
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not seek to offset %lu in file \"%s\": %m",
									offset, test_data_file)));
				}

				ret = polar_read(file, buf, BLCKSZ);
			}
			if (ret < 0)
			{
				polar_close(file);
				elog(ERROR, "random read failed. offset : %ld", offset);
			}
			else if (ret == 0)
			{
				polar_close(file);
				elog(ERROR, "reach to EOF unexpected. offset %ld, filesize: %ld",
					 offset, file_size);
			}
			current_size += ret;
	} while (current_size < file_size);
	end_tm = GetCurrentTimestamp();
	polar_close(file);
	return polar_get_time_diff(start_tm, end_tm) / ((file_size / BLCKSZ) * 1.0);
}

static void
polar_clean_sysstat_testfiles(int code, Datum arg)
{
	if (test_file_created)
	{
		polar_unlink(file_name_with_polardir(test_data_file));
		test_file_created = false;
	}
	if (dummy_file_created)
	{
		polar_unlink(file_name_with_polardir(device_cache_invalid_file));
		dummy_file_created = false;
	}
}

static void
polar_read_rel_1st_block(Relation reln)
{
	Buffer buf;
	RelationOpenSmgr(reln);
	buf = ReadBufferExtended(reln, MAIN_FORKNUM, 0, RBM_NORMAL, NULL);
	ReleaseBuffer(buf);
	RelationCloseSmgr(reln);
}


static double
polar_buffer_cache_lat()
{
	Relation pg_class_rel;
	int i = 0;
	int loops = 10000;
	TimestampTz start_tm, end_tm;

	pg_class_rel = heap_open(RelationRelationId, NoLock);

	polar_read_rel_1st_block(pg_class_rel);

	start_tm = GetCurrentTimestamp();
	for(i = 0; i < loops; i++)
	{
		polar_read_rel_1st_block(pg_class_rel);
	}
	end_tm = GetCurrentTimestamp();
	heap_close(pg_class_rel, NoLock);
	return polar_get_time_diff(start_tm, end_tm) / (loops * 1.0);
}


static double
get_real_lat(double cache_hit_ratio, double disk_lat, double cache_lat)
{
	return cache_hit_ratio * cache_lat + (1 - cache_hit_ratio) * disk_lat;
}

static char *
file_name_with_polardir(const char* filename)
{
	StringInfo s = makeStringInfo();
	appendStringInfo(s, "%s/%s",
					 POLAR_FILE_IN_SHARED_STORAGE() ? polar_datadir : DataDir,
					 filename);
	return s->data;
}
