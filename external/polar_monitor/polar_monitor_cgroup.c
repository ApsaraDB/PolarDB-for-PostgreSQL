/*-------------------------------------------------------------------------
 *
 * polar_monitor_cgroup.c
 *    views of polardb control group
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
 *    external/polar_monitor/polar_monitor_cgroup.c
 *-------------------------------------------------------------------------
 */

#include "polar_monitor.h"

#define MAX_LINE 1024

const int64 S_2_NS = 1000000000;
const int64 UNDEFINED_VALUE = -2;
const int64 NOT_RESTRICTION_VALUE = -1;

char rel_path[MAX_LINE];
char base_path[MAX_LINE];

/*
 * Declarations
 */
static void add_various_tuple(Tuplestorestate *state, TupleDesc tdesc, char* sqltype, char* cmdtype,
					 int64 count, Datum *values, bool *nulls, int array_size);
static int64 get_cpu_usage(void);
static int64 get_cpu_user(void);
static int64 get_cpu_system(void);
static int64 get_mem_usage(void);
static int64 get_huge_in_bytes(void);
static int64 get_io_serviced(void);
static int64 get_io_service_bytes(void);
static void add_mem_tuples(Tuplestorestate *state, TupleDesc tdesc, 
							Datum *values, bool *nulls, int array_size);
static int64 get_value_from_file(char* file);
static int64 get_value_from_last_line(char* file);
static int64 get_value_from_key(char* file, char* key);
static int64 get_value_from_blkio(char* file);
static int64 get_value_from_cpus(char* file);
static bool is_number(char c);
static int get_next_num(char* data, int* pos, int len);
static int64 get_memory_limit(void);
static int64 get_read_bps(void);
static int64 get_read_iops(void);
static int64 get_write_bps(void);
static int64 get_write_iops(void);
static int64 get_cfs_period(void);
static int64 get_cfs_quota(void);
static int64 get_rt_period(void);
static int64 get_rt_runtime(void);
static int64 get_cpus(void);
static char* get_relative_path(char* subsystem);
static char* get_base_path(void);
static void set_base_path(char* basepath);
static int64 get_ratio_user_hz_2_ns(void);

/*
 * Add various query type to tuple.
 */
static void
add_various_tuple(Tuplestorestate *state, TupleDesc tdesc, char* subtype, char* infotype,
					 int64 count, Datum *values, bool *nulls, int array_size)
{
	int col = 0;
	if (count == UNDEFINED_VALUE)
	{
		elog(WARNING, "Get invalid value from %s.%s, check whether the file exist", subtype, infotype);
		return;
	}
	MemSet(values, 0, sizeof(Datum) * array_size);
	MemSet(nulls, 0, sizeof(bool) * array_size);
	values[col++] = CStringGetTextDatum(subtype);
	values[col++] = CStringGetTextDatum(infotype);
	values[col++] = Int64GetDatumFast(count);
	tuplestore_putvalues(state, tdesc, values, nulls);
}

/*
 * Use this function when we want to obtain content with pattern: [%ld] in file
 */
static int64
get_value_from_file(char* file)
{
	FILE *fp;
	char buf[MAX_LINE]; 
	int64 value = UNDEFINED_VALUE;
	fp = fopen(file, "r");
	if (fp == NULL)
		return value;
	if (fgets(buf, MAX_LINE, fp) != NULL)
		sscanf(buf, "%ld", &value);
	fclose(fp);
    return value;
}

/*
 * Use this function when we want to obtain content with pattern: [%s %ld] in last line
 */
static int64
get_value_from_last_line(char* file)
{
	FILE *fp;
	char buf[MAX_LINE];
	char str[MAX_LINE];
	int64 value = UNDEFINED_VALUE;
	fp = fopen(file, "r");
	if (fp == NULL)
		return value;
	while (fgets(buf, MAX_LINE, fp) != NULL);
	sscanf(buf, "%s %ld", str, &value);
	fclose(fp);
    return value;
}

/*
 * Use this function when we want to obtain content with pattern: [%s %ld] in line
 */
static int64
get_value_from_key(char* file, char* key)
{
	FILE *fp;
	char buf[MAX_LINE];
	char str[MAX_LINE];
	int64 value = UNDEFINED_VALUE;
	fp = fopen(file, "r");
	if (fp == NULL)
		return value;
	while (fgets(buf, MAX_LINE, fp) != NULL)
	{
		sscanf(buf, "%s %ld", str, &value);
		if(strcmp(str, key) == 0)
			break;
	}
	fclose(fp);
    return value;
}

/*
 * Use this function when we want to obtain content with pattern: [%ld:%ld %ld] in line
 */
static int64
get_value_from_blkio(char* file)
{
	FILE *fp;
	char buf[MAX_LINE];
	int64 master = UNDEFINED_VALUE;
	int64 slave = UNDEFINED_VALUE;
	int64 quato = UNDEFINED_VALUE;
	fp = fopen(file, "r");
	if (fp == NULL)
		return quato;
	/* if the file is empty, there is no restriction */
	quato = NOT_RESTRICTION_VALUE;
	if (fgets(buf, MAX_LINE, fp) != NULL)
	{
		sscanf(buf, "%ld:%ld %ld", &master, &slave, &quato);
	}
	fclose(fp);
    return quato;
}

/*
 * A helper function to judge whether a char is bwtween '0' and '9'
 */
static bool
is_number(char c)
{
	return c >= '0' && c <= '9';
}

/*
 * A helper function to parse the next number in string.
 * After paring, pos move to next position.
 */
static int
get_next_num(char* data, int* pos, int len)
{
	int p = *pos;
	int sum = data[p] - '0';
	int cur_num;

	while (p + 1 < len && is_number(data[p + 1]))
	{
		++p;
		cur_num = data[p] - '0';
		sum = sum * 10 + cur_num;
	}
	++p;
	*pos = p;
	return sum;
}

/*
 * A simple parser to calculate the number that represent in file with pattern: [digit digit-digit],*[digit digit-digit]
 * eg: For "0-3,5,7-10", we should return 9.
 * Time Complexity: O(n)
 */
static int64
get_value_from_cpus(char* file)
{
	FILE *fp;
	char buf[MAX_LINE];
	int64 count = 0;
	fp = fopen(file, "r");
	if (fp == NULL)
		return count;
	if (fgets(buf, MAX_LINE, fp) != NULL)
	{
		int len = strlen(buf);
		int pos = 0;
		int eNum;
		while (pos < len)
		{
			int bNum = get_next_num(buf, &pos, len);
			if (pos >= len)
			{
				++count;
				break;
			}
			if (buf[pos] == ',')
			{
				++count;
				++pos;
			}else if (buf[pos] == '-')
			{
				++pos;
				eNum = get_next_num(buf, &pos, len);
				count += eNum - bNum + 1;
				++pos;
			}
		}
	}
	fclose(fp);
	return count;
}

static int64 
get_cpu_usage()
{
	char path[MAX_LINE];
	sprintf(path, "%s%scpu/cpuacct.usage", get_base_path(), get_relative_path("cpu"));
	return get_value_from_file(path);
}

static int64
get_ratio_user_hz_2_ns()
{
	int64 ticks = sysconf( _SC_CLK_TCK );
	return S_2_NS / ticks;
}

static int64
get_cpu_user()
{
	char path[MAX_LINE];
	int64 user_value;
	sprintf(path, "%s%scpu/cpuacct.stat", get_base_path(), get_relative_path("cpu"));
	user_value = get_value_from_key(path, "user");
	user_value *= get_ratio_user_hz_2_ns();
	return user_value;
}

static int64
get_cpu_system()
{
	char path[MAX_LINE];
	int64 sys_value;
	sprintf(path, "%s%scpu/cpuacct.stat", get_base_path(), get_relative_path("cpu"));
	sys_value = get_value_from_key(path, "system");
	sys_value *= get_ratio_user_hz_2_ns();
	return sys_value;
}

static int64
get_mem_usage()
{
	char path[MAX_LINE];
	sprintf(path, "%s%smemory/memory.usage_in_bytes", get_base_path(), get_relative_path("memory"));
	return get_value_from_file(path);
}

static int64
get_huge_in_bytes()
{
	char path[MAX_LINE];
	sprintf(path, "%s%shugetlb/hugetlb.2MB.usage_in_bytes", get_base_path(), get_relative_path("hugetlb"));
	return get_value_from_file(path);
}

static int64
get_io_serviced()
{
	char path[MAX_LINE];
	sprintf(path, "%s%sblkio/blkio.throttle.io_serviced", get_base_path(), get_relative_path("blkio"));
	return get_value_from_last_line(path);
}

static int64
get_io_service_bytes()
{
	char path[MAX_LINE];
	sprintf(path, "%s%sblkio/blkio.throttle.io_service_bytes", get_base_path(), get_relative_path("blkio"));
	return get_value_from_last_line(path);
}

static int64
get_memory_limit()
{
	char path[MAX_LINE];
	sprintf(path, "%s%smemory/memory.limit_in_bytes", get_base_path(), get_relative_path("memory"));
	return get_value_from_file(path);
}

static int64
get_read_bps()
{
	char path[MAX_LINE];
	sprintf(path, "%s%sblkio/blkio.throttle.read_bps_device", get_base_path(), get_relative_path("blkio"));
	return get_value_from_blkio(path);
}

static int64
get_read_iops()
{
	char path[MAX_LINE];
	sprintf(path, "%s%sblkio/blkio.throttle.read_iops_device", get_base_path(), get_relative_path("blkio"));
	return get_value_from_blkio(path);
}

static int64
get_write_bps()
{
	char path[MAX_LINE];
	sprintf(path, "%s%sblkio/blkio.throttle.write_bps_device", get_base_path(), get_relative_path("blkio"));
	return get_value_from_blkio(path);
}

static int64
get_write_iops()
{
	char path[MAX_LINE];
	sprintf(path, "%s%sblkio/blkio.throttle.write_iops_device", get_base_path(), get_relative_path("blkio"));
	return get_value_from_blkio(path);
}

static int64
get_cfs_period()
{
	char path[MAX_LINE];
	sprintf(path, "%s%scpu/cpu.cfs_period_us", get_base_path(), get_relative_path("cpu"));
	return get_value_from_file(path);
}

static int64
get_cfs_quota()
{
	char path[MAX_LINE];
	sprintf(path, "%s%scpu/cpu.cfs_quota_us", get_base_path(), get_relative_path("cpu"));
	return get_value_from_file(path);
}

static int64
get_rt_period()
{
	char path[MAX_LINE];
	sprintf(path, "%s%scpu/cpu.rt_period_us", get_base_path(), get_relative_path("cpu"));
	return get_value_from_file(path);
}

static int64
get_rt_runtime()
{
	char path[MAX_LINE];
	sprintf(path, "%s%scpu/cpu.rt_runtime_us", get_base_path(), get_relative_path("cpu"));
	return get_value_from_file(path);
}

static int64
get_cpus()
{
	char path[MAX_LINE];
	sprintf(path, "%s%scpu/cpuset.cpus", get_base_path(), get_relative_path("cpu"));
	return get_value_from_cpus(path);
}

static char*
get_base_path()
{
	return base_path;
}

static void
set_base_path(char* basepath)
{
	sprintf(base_path, "%s", basepath);
}

/*
 * Get relative path from /proc/self/cgroup.
 * Return default value '/' if not found the subsystem in file.
 * Time Complexity: O(n)
 */
static char*
get_relative_path(char* subsystem)
{
	char buf[MAX_LINE]; 
	int num;
	int p_start;
	int len;
	char subsystems[MAX_LINE];
	FILE *fp = fopen("/proc/self/cgroup", "r");
	if (fp == NULL)
		return "/";
	while (fgets(buf, MAX_LINE, fp) != NULL)
	{
		bool is_found = false;
		int i = 0;
		sscanf(buf, "%d:%[^:]:%s", &num, subsystems, rel_path);
		len = strlen(subsystems);
		subsystems[len] = ',';
		subsystems[len + 1] = '\0';
		p_start = 0;
		for (; i <= len; i++)
		{
			if (subsystems[i] == ',')
			{
				subsystems[i] = '\0';
				if (strcmp(subsystem, subsystems + p_start) == 0)
				{
					is_found = true;
					break;
				}
				p_start = i + 1;
			}
		}

		if (is_found)
		{
			len = strlen(rel_path);
			if (len > 1)
			{
				rel_path[len] = '/';
				rel_path[len + 1] = '\0';
			}
			fclose(fp);
			return rel_path;
		}
	}
	fclose(fp);
	return "/";
}

/*
 * Add all memory usage infos in file memroty.stat that with prefix 'total_'.
 */
static void
add_mem_tuples(Tuplestorestate *state, TupleDesc tdesc, Datum *values, 
	bool *nulls, int array_size)
{
	FILE *fp;
	char buf[MAX_LINE];
	char str[MAX_LINE];
	int64 value;
	char tmp;
	char* pattern = "total_";
	int end_pos = strlen(pattern);
	char path[MAX_LINE];
	sprintf(path, "%s%smemory/memory.stat", get_base_path(), get_relative_path("memory"));
	fp = fopen(path, "r");
	if (fp == NULL)
	{
		elog(WARNING, "Failed to open memory.stat");
		return;
	}	
	
	while (fgets(buf, MAX_LINE, fp) != NULL)
	{
		sscanf(buf, "%s %ld", str, &value);
		tmp = str[end_pos];
		str[end_pos] = '\0';
		if (strcmp(str, pattern) == 0)
		{
			str[end_pos] = tmp;
			add_various_tuple(state, tdesc, "memory", str,
					 value, values, nulls, array_size);
		}
	}
	fclose(fp);
}

PG_FUNCTION_INFO_V1(polar_stat_cgroup);
/*
 * Collect the cgroup info for creating the view.
 */
Datum
polar_stat_cgroup(PG_FUNCTION_ARGS)
{
	#define POLAR_STAT_CGROUP_COLS 3
	int  			index = 1;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;

    Datum		values[POLAR_STAT_CGROUP_COLS];
	bool		nulls[POLAR_STAT_CGROUP_COLS];

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	tupdesc = CreateTemplateTupleDesc(POLAR_STAT_CGROUP_COLS, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) index++, "sub_type",
						TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) index++, "info_type",
						TEXTOID, -1, 0);					
	TupleDescInitEntry(tupdesc, (AttrNumber) index++, "count",
						INT8OID, -1, 0);
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	set_base_path("/sys/fs/cgroup");

	/* cpu */
	add_various_tuple(tupstore, tupdesc, "cpu", "cpuacct.usage",
					 get_cpu_usage(), values, nulls, POLAR_STAT_CGROUP_COLS);
	add_various_tuple(tupstore, tupdesc, "cpu", "cpuacct.stat.user",
					 get_cpu_user(), values, nulls, POLAR_STAT_CGROUP_COLS);
	add_various_tuple(tupstore, tupdesc, "cpu", "cpuacct.stat.system",
					 get_cpu_system(), values, nulls, POLAR_STAT_CGROUP_COLS);
	/* memory */
	add_various_tuple(tupstore, tupdesc, "memory", "memory.usage_in_bytes",
					 get_mem_usage(), values, nulls, POLAR_STAT_CGROUP_COLS);
	add_mem_tuples(tupstore, tupdesc, values, nulls, POLAR_STAT_CGROUP_COLS);
	/* huge page */
	add_various_tuple(tupstore, tupdesc, "hugepage", "hugetlb.2MB.usage_in_bytes",
					 get_huge_in_bytes(), values, nulls, POLAR_STAT_CGROUP_COLS);
	/* io */
	add_various_tuple(tupstore, tupdesc, "io", "blkio.throttle.io_serviced",
					 get_io_serviced(), values, nulls, POLAR_STAT_CGROUP_COLS);
	add_various_tuple(tupstore, tupdesc, "io", "blkio.throttle.io_service_bytes",
					 get_io_service_bytes(), values, nulls, POLAR_STAT_CGROUP_COLS);

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);
	
	return (Datum) 0;
}

PG_FUNCTION_INFO_V1(polar_cgroup_quota);
/*
 * Collect the cgroup quota info for creating the view.
 */
Datum
polar_cgroup_quota(PG_FUNCTION_ARGS)
{
	#define POLAR_CGROUP_QUOTA_COLS 3
	int  			index = 1;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;

    Datum		values[POLAR_CGROUP_QUOTA_COLS];
	bool		nulls[POLAR_CGROUP_QUOTA_COLS];

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	tupdesc = CreateTemplateTupleDesc(POLAR_CGROUP_QUOTA_COLS, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) index++, "sub_type",
						TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) index++, "info_type",
						TEXTOID, -1, 0);					
	TupleDescInitEntry(tupdesc, (AttrNumber) index++, "count",
						INT8OID, -1, 0);
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	set_base_path("/sys/fs/cgroup");

	/* cpu */
	add_various_tuple(tupstore, tupdesc, "cpu", "cpu.cfs_period_us",
					 get_cfs_period(), values, nulls, POLAR_CGROUP_QUOTA_COLS);
	add_various_tuple(tupstore, tupdesc, "cpu", "cpu.cfs_quota_us",
					 get_cfs_quota(), values, nulls, POLAR_CGROUP_QUOTA_COLS);
	add_various_tuple(tupstore, tupdesc, "cpu", "cpu.rt_period_us",
					 get_rt_period(), values, nulls, POLAR_CGROUP_QUOTA_COLS);
	add_various_tuple(tupstore, tupdesc, "cpu", "cpu.rt_runtime_us",
					 get_rt_runtime(), values, nulls, POLAR_CGROUP_QUOTA_COLS);
	add_various_tuple(tupstore, tupdesc, "cpu", "cpuset.cpus",
					 get_cpus(), values, nulls, POLAR_CGROUP_QUOTA_COLS);
	/* memory */
	add_various_tuple(tupstore, tupdesc, "memory", "memory.limit_in_bytes",
					 get_memory_limit(), values, nulls, POLAR_CGROUP_QUOTA_COLS);
	/* io */
	add_various_tuple(tupstore, tupdesc, "blkio", "throttle.read_bps_device",
					 get_read_bps(), values, nulls, POLAR_CGROUP_QUOTA_COLS);
	add_various_tuple(tupstore, tupdesc, "blkio", "throttle.read_iops_device",
					 get_read_iops(), values, nulls, POLAR_CGROUP_QUOTA_COLS);
	add_various_tuple(tupstore, tupdesc, "blkio", "throttle.write_bps_device",
					 get_write_bps(), values, nulls, POLAR_CGROUP_QUOTA_COLS);
	add_various_tuple(tupstore, tupdesc, "blkio", "throttle.write_iops_device",
					 get_write_iops(), values, nulls, POLAR_CGROUP_QUOTA_COLS);

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);
	
	return (Datum) 0;
}