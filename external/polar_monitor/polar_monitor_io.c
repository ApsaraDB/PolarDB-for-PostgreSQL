/*-------------------------------------------------------------------------
 *
 * polar_io_view.c
 *    views of polardb io stat
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
 *    external/polar_monitor/polar_monitor_io.c
 *-------------------------------------------------------------------------
 */

#include "polar_monitor.h"

/* POLAR:  Static dictionary*/
typedef struct 
{
	enum IOLatencyInterval latencyInterval;
	char *latencyName;
}LatencyTuple;

static  LatencyTuple polar_latency_infos[LATENCY_INTERVAL_LEN] = {
		{LATENCY_200,"LessThan200us"},{LATENCY_400,"LessThan400us"},
		{LATENCY_600,"LessThan600us"},{LATENCY_800,"LessThan800us"},
		{LATENCY_1ms,"LessThan1ms"},{LATENCY_10ms,"LessThan10ms"},
		{LATENCY_100ms,"LessThan100ms"},{LATENCY_OUT,"MoreThan100ms"}
};
static 	char *polar_dir_type_names[] = {
		"WAL","DATA","CLOG","global","logindex","multixact",
		"twophase","replslot","snapshots","subtrans","others"
	};
static	char *polar_io_type_names[] = {
		"read","write","open","seek","creat",
		"fsync","falloc","others"
	};
/* POLAR end*/

/* POLAR : io stat for polar_monitor */
static void set_polar_proc_iostat(int backendid, Datum *values, bool *nulls);
/* POLAR End */

/*
 * POLAR: return the IO stat info ever backend and auxiliary  process
 */
Datum
polar_stat_process(PG_FUNCTION_ARGS)
{
#define PG_STAT_GET_POLAR_PROCESS_COLS	20
	int			num_backends = pgstat_fetch_stat_numbackends();
	int			curr_backend;
	int			cols = 1;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	
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

	/* Build a tuple descriptor for our result type */
	tupdesc = CreateTemplateTupleDesc(PG_STAT_GET_POLAR_PROCESS_COLS, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "pid",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "wait_object",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "wait_time(ms)",
						FLOAT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "cpu_user",
						INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "cpu_sys",
						INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "rss",
						INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "shared_read_ps",
						INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "shared_write_ps",
						INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "shared_read_throughput",
						INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "shared_write_throughput",
						INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "shared_read_latency(ms)",
						FLOAT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "shared_wirte_latency(ms)",
						FLOAT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "local_read_ps",
						INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "local_write_ps",
						INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "local_read_throughput",
						INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "local_write_throughput",
						INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "local_read_latency(ms)",
						FLOAT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "local_wirte_latency(ms)",
						FLOAT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "wait_type",
						TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "queryid",
						INT8OID, -1, 0);

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);
	/* 1-based index */
	for (curr_backend = 1; curr_backend <= num_backends; curr_backend++)
	{
		/* for each row */
		Datum		values[PG_STAT_GET_POLAR_PROCESS_COLS];
		bool		nulls[PG_STAT_GET_POLAR_PROCESS_COLS];
		volatile PGPROC	   	*proc;
		LocalPgBackendStatus *local_beentry;
		PgBackendStatus *beentry;
		polar_proc_stat procstat;
		int 	cur_wait_stack_index;
		int		cur_collect_type;
		int     cur_collect_object;
		instr_time 	cur_collect_time;
		instr_time 	wait_time;

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));
		memset(&procstat, 0, sizeof(procstat));
		INSTR_TIME_SET_ZERO(cur_collect_time);
		cur_wait_stack_index = -1;
		cur_collect_object = -1;
		cur_collect_type = -1;

		/* Get the next one in the list */
		local_beentry = pgstat_fetch_stat_local_beentry(curr_backend);
		if (!local_beentry)
		{
			int			i;

			/* Ignore missing entries if looking for specific PID */
			for (i = 0; i < lengthof(nulls); i++)
				nulls[i] = true;
			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
			continue;
		}
		beentry = &local_beentry->backendStatus;

		/* POLAR: Shared Server */
		/* TODO: not compat this monitor now @yanhua */
		if (POLAR_SHARED_SERVER_RUNNING() &&
			beentry->st_backendType == B_BACKEND &&
			beentry->session_local_id > 0)
			continue;

		values[0] = Int32GetDatum(beentry->st_procpid);
		values[19] = Int64GetDatum(beentry->queryid);

		proc = BackendPidGetProc(beentry->st_procpid);

		if (beentry->st_backendType != B_BACKEND)
		{
			/*
			* For an auxiliary process, retrieve process info from
			* AuxiliaryProcs stored in shared-memory.
			*/
			proc = AuxiliaryPidGetProc(beentry->st_procpid);
		}

		/* wait_object and wait_time */
		if (proc != NULL)
		{
			cur_wait_stack_index = proc->cur_wait_stack_index;
			if (cur_wait_stack_index < 0 || cur_wait_stack_index > 3)
			{
				nulls[1] = true;
				nulls[2] = true;
				nulls[18] = true;
			}
			else
			{
				
				cur_collect_object = proc->wait_object[proc->cur_wait_stack_index];
				cur_collect_type = proc->wait_type[proc->cur_wait_stack_index];
				INSTR_TIME_ADD(cur_collect_time, proc->wait_time[proc->cur_wait_stack_index]);
				if (!INSTR_TIME_IS_ZERO(cur_collect_time))
				{
					INSTR_TIME_SET_CURRENT(wait_time);
					INSTR_TIME_SUBTRACT(wait_time, cur_collect_time);
					values[1] = Int32GetDatum(cur_collect_object);
					values[2] = Float8GetDatum(INSTR_TIME_GET_MILLISEC(wait_time));
					switch (cur_collect_type)
					{
					case PGPROC_WAIT_PID:
						values[18] = CStringGetTextDatum("pid");
						break;
					case PGPROC_WAIT_FD:
						values[18] = CStringGetTextDatum("fd");
						break;
					default:
						values[18] = CStringGetTextDatum("unknow");
						break;
					}
				}
				else
				{
					nulls[1] = true;
					nulls[2] = true;
					nulls[18] = true;
				}
				
			}
		}
		else
		{
			nulls[1] = true;
			nulls[2] = true;
			nulls[18] = true;
		}

		/* HOW: 
		 * CPU info  
		 */
		if(!polar_get_proc_stat(beentry->st_procpid, &procstat))
		{
			values[3] = Int64GetDatum(procstat.utime);
			values[4] = Int64GetDatum(procstat.stime);
			values[5] = Int64GetDatum(procstat.rss - procstat.share);
		}
		else
		{
			/*no cover begin*/
			nulls[3] = true;
			nulls[4] = true;
			nulls[5] = true;
			/*no cover end*/
		}
		/*IO inof 6~11*/
		set_polar_proc_iostat(beentry->backendid, values, nulls);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}
	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

/*
 * POLAR: It seems stupid to write like this, but there is no other way.
 * Extract the information of PROC into Datum
 */
static void
set_polar_proc_iostat(int backendid, Datum *values, bool *nulls)
{
	int 			index;
	int				cols = 6;
	uint64 			shared_read_ps = 0;
	uint64 			shared_write_ps = 0;
	uint64			shared_read_throughput = 0;
	uint64			shared_write_throughput = 0;
	instr_time		shared_read_latency ;
	instr_time		shared_write_latency ;
	uint64 			local_read_ps = 0;
	uint64 			local_write_ps = 0;
	uint64			local_read_throughput = 0;
	uint64			local_write_throughput = 0;
	instr_time		local_read_latency ;
	instr_time		local_write_latency ;
	
	INSTR_TIME_SET_ZERO(shared_read_latency);
	INSTR_TIME_SET_ZERO(shared_write_latency);
	INSTR_TIME_SET_ZERO(local_read_latency);
	INSTR_TIME_SET_ZERO(local_write_latency);

	/* Each process accumulates it‘s file type by file location */
	if (PolarIOStatArray)
	{
		for (index = 0; index < POLARIO_TYPE_SIZE; index++)
		{
			local_read_ps += PolarIOStatArray[backendid].polar_proc_io_stat_dist[index][POLARIO_LOCAL].io_number_read;
			local_write_ps += PolarIOStatArray[backendid].polar_proc_io_stat_dist[index][POLARIO_LOCAL].io_number_write;
			local_read_throughput += PolarIOStatArray[backendid].polar_proc_io_stat_dist[index][POLARIO_LOCAL].io_throughtput_read;
			local_write_throughput += PolarIOStatArray[backendid].polar_proc_io_stat_dist[index][POLARIO_LOCAL].io_throughtput_write;
			INSTR_TIME_ADD(local_read_latency, PolarIOStatArray[backendid].polar_proc_io_stat_dist[index][POLARIO_LOCAL].io_latency_read);
			INSTR_TIME_ADD(local_write_latency, PolarIOStatArray[backendid].polar_proc_io_stat_dist[index][POLARIO_LOCAL].io_latency_write);

			shared_read_ps += PolarIOStatArray[backendid].polar_proc_io_stat_dist[index][POLARIO_SHARED].io_number_read;
			shared_write_ps += PolarIOStatArray[backendid].polar_proc_io_stat_dist[index][POLARIO_SHARED].io_number_write;
			shared_read_throughput += PolarIOStatArray[backendid].polar_proc_io_stat_dist[index][POLARIO_SHARED].io_throughtput_read;
			shared_write_throughput += PolarIOStatArray[backendid].polar_proc_io_stat_dist[index][POLARIO_SHARED].io_throughtput_write;
			INSTR_TIME_ADD(shared_read_latency, PolarIOStatArray[backendid].polar_proc_io_stat_dist[index][POLARIO_SHARED].io_latency_read);
			INSTR_TIME_ADD(shared_write_latency, PolarIOStatArray[backendid].polar_proc_io_stat_dist[index][POLARIO_SHARED].io_latency_write);
		}

		/* pfs iops */
		values[cols++] = Int64GetDatum(shared_read_ps);
		values[cols++] = Int64GetDatum(shared_write_ps);
		/* pfs io throughput */
		values[cols++] = Int64GetDatum(shared_read_throughput);
		values[cols++] = Int64GetDatum(shared_write_throughput);
		/* pfs io latency */
		values[cols++] = Float8GetDatum(INSTR_TIME_GET_MILLISEC(shared_read_latency));
		values[cols++] = Float8GetDatum(INSTR_TIME_GET_MILLISEC(shared_write_latency));
		/* local iops */
		values[cols++] = Int64GetDatum(local_read_ps);
		values[cols++] = Int64GetDatum(local_write_ps);
		/* local io throughput */
		values[cols++] = Int64GetDatum(local_read_throughput);
		values[cols++] = Int64GetDatum(local_write_throughput);
		/* local io latency */
		values[cols++] = Float8GetDatum(INSTR_TIME_GET_MILLISEC(local_read_latency));
		values[cols++] = Float8GetDatum(INSTR_TIME_GET_MILLISEC(local_write_latency));
	}
	else
	{
		/*no cover begin*/
		for (cols = 6; cols < 18; cols++)
			nulls[cols] = true;
		/*no cover end*/
	}
	
}

/*
 * POLAR: return the IO stat info ever flie type
 */
Datum
polar_stat_io_info(PG_FUNCTION_ARGS)
{
#define POLARIOSTATSIZE 20
	int         	curr_backend;
	int  			index = 0;
	int				cols = 1;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	PolarProcIOStat (*cur_polar_proc_io_stat)[POLARIO_LOC_SIZE];

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

	tupdesc = CreateTemplateTupleDesc(POLARIOSTATSIZE, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "pid",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "FileType",
						TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "FileLocation",
						TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "open_count",
						INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "open_latency",
						FLOAT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "close_count",
						INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "read_count",
						INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "write_count",
						INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "read_throughput",
						INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "write_throughput",
						INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "read_latency",
						FLOAT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "write_latency",
						FLOAT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "seek_count",
						INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "seek_latency",
						FLOAT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "creat_count",
						INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "creat_latency",
						FLOAT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "fsync_count",
						INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "fsync_latency",
						FLOAT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "falloc_count",
						INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "falloc_latency",
						FLOAT8OID, -1, 0);
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	if (!PolarIOStatArray)
	{
		tuplestore_donestoring(tupstore);
		elog(ERROR, "Io statistics is unavailable!");
	}

	/* 1-based index */
	for (curr_backend = 0; curr_backend < PolarNumProcIOStatSlots; curr_backend++)
	{
		int			cur_pid = 0;
		/* Get the next one in the list */
		cur_pid = curr_backend == 0 ? 0 : PolarIOStatArray[curr_backend].pid;
		if (curr_backend > 0 && cur_pid <= 0)
			continue;

		cur_polar_proc_io_stat = PolarIOStatArray[curr_backend].polar_proc_io_stat_dist;
		for (index = 0; index < POLARIO_TYPE_SIZE; index++)
		{
			/* for each row */
			Datum values[POLARIOSTATSIZE];
			bool nulls[POLARIOSTATSIZE];
			instr_time tmptime;

			if (cur_polar_proc_io_stat[index][POLARIO_SHARED].io_open_num > 0)
			{
				/*no cover begin*/
				MemSet(values, 0, sizeof(values));
				MemSet(nulls, 0, sizeof(nulls));
				MemSet(&tmptime, 0, sizeof(tmptime));
				cols = 0;
				/* pid*/
				values[cols++] = Int32GetDatum(cur_pid);
				/* FileType */
				values[cols++] = CStringGetTextDatum(polar_dir_type_names[index]);
				/* File Location */
				values[cols++] = CStringGetTextDatum("pfs");
				/* open_count */
				values[cols++] = Int64GetDatum(cur_polar_proc_io_stat[index][POLARIO_SHARED].io_open_num);
				/* open_latency */
				values[cols++] = Float8GetDatum(INSTR_TIME_GET_MICROSEC(cur_polar_proc_io_stat[index][POLARIO_SHARED].io_open_time));
				/* close_count */
				values[cols++] = Int64GetDatum(cur_polar_proc_io_stat[index][POLARIO_SHARED].io_close_num);
				/* read_count */
				values[cols++] = Int64GetDatum(cur_polar_proc_io_stat[index][POLARIO_SHARED].io_number_read);
				/* write_count */
				values[cols++] = Int64GetDatum(cur_polar_proc_io_stat[index][POLARIO_SHARED].io_number_write);
				/* read_throughput */
				values[cols++] = Int64GetDatum(cur_polar_proc_io_stat[index][POLARIO_SHARED].io_throughtput_read);
				/* wirte_throughput */
				values[cols++] = Int64GetDatum(cur_polar_proc_io_stat[index][POLARIO_SHARED].io_throughtput_write);
				/* read_latency */
				values[cols++] = Float8GetDatum(INSTR_TIME_GET_MICROSEC(cur_polar_proc_io_stat[index][POLARIO_SHARED].io_latency_read));
				/* write_latency */
				values[cols++] = Float8GetDatum(INSTR_TIME_GET_MICROSEC(cur_polar_proc_io_stat[index][POLARIO_SHARED].io_latency_write));
				/* seek_count */
				values[cols++] = Int64GetDatum(cur_polar_proc_io_stat[index][POLARIO_SHARED].io_seek_count);
				/* seek_latency */
				values[cols++] = Float8GetDatum(INSTR_TIME_GET_MICROSEC(cur_polar_proc_io_stat[index][POLARIO_SHARED].io_seek_time));
				/* creat_count */
				values[cols++] = Int64GetDatum(cur_polar_proc_io_stat[index][POLARIO_SHARED].io_creat_count);
				/* creat_latency */
				values[cols++] = Float8GetDatum(INSTR_TIME_GET_MICROSEC(cur_polar_proc_io_stat[index][POLARIO_SHARED].io_creat_time));
				/* fsync_count */
				values[cols++] = Int64GetDatum(cur_polar_proc_io_stat[index][POLARIO_SHARED].io_fsync_count);
				/* fsync_latency */
				values[cols++] = Float8GetDatum(INSTR_TIME_GET_MICROSEC(cur_polar_proc_io_stat[index][POLARIO_SHARED].io_fsync_time));
				/* falloc_count */
				values[cols++] = Int64GetDatum(cur_polar_proc_io_stat[index][POLARIO_SHARED].io_falloc_count);
				/* falloc_latency */
				values[cols++] = Float8GetDatum(INSTR_TIME_GET_MICROSEC(cur_polar_proc_io_stat[index][POLARIO_SHARED].io_falloc_time));
				tuplestore_putvalues(tupstore, tupdesc, values, nulls);
				/*no cover end*/
			}
			
			if (cur_polar_proc_io_stat[index][POLARIO_LOCAL].io_open_num > 0)
			{
				MemSet(values, 0, sizeof(values));
				MemSet(nulls, 0, sizeof(nulls));
				MemSet(&tmptime, 0, sizeof(tmptime));
				cols = 0;
				/* pid*/
				values[cols++] = Int32GetDatum(cur_pid);
				/* FileType */
				values[cols++] = CStringGetTextDatum(polar_dir_type_names[index]);
				/* File Location */
				values[cols++] = CStringGetTextDatum("local");
				/* open_count */
				values[cols++] = Int64GetDatum(cur_polar_proc_io_stat[index][POLARIO_LOCAL].io_open_num);
				/* open_latency */
				values[cols++] = Float8GetDatum(INSTR_TIME_GET_MICROSEC(cur_polar_proc_io_stat[index][POLARIO_LOCAL].io_open_time));
				/* close_count */
				values[cols++] = Int64GetDatum(cur_polar_proc_io_stat[index][POLARIO_LOCAL].io_close_num);
				/* read_count */
				values[cols++] = Int64GetDatum(cur_polar_proc_io_stat[index][POLARIO_LOCAL].io_number_read);
				/* write_count */
				values[cols++] = Int64GetDatum(cur_polar_proc_io_stat[index][POLARIO_LOCAL].io_number_write);
				/* read_throughput */
				values[cols++] = Int64GetDatum(cur_polar_proc_io_stat[index][POLARIO_LOCAL].io_throughtput_read);
				/* wirte_throughput */
				values[cols++] = Int64GetDatum(cur_polar_proc_io_stat[index][POLARIO_LOCAL].io_throughtput_write);
				/* read_latency */
				values[cols++] = Float8GetDatum(INSTR_TIME_GET_MICROSEC(cur_polar_proc_io_stat[index][POLARIO_LOCAL].io_latency_read));
				/* write_latency */
				values[cols++] = Float8GetDatum(INSTR_TIME_GET_MICROSEC(cur_polar_proc_io_stat[index][POLARIO_LOCAL].io_latency_write));
				/* seek_count */
				values[cols++] = Int64GetDatum(cur_polar_proc_io_stat[index][POLARIO_LOCAL].io_seek_count);
				/* seek_latency */
				values[cols++] = Float8GetDatum(INSTR_TIME_GET_MICROSEC(cur_polar_proc_io_stat[index][POLARIO_LOCAL].io_seek_time));
				/* creat_count */
				values[cols++] = Int64GetDatum(cur_polar_proc_io_stat[index][POLARIO_LOCAL].io_creat_count);
				/* creat_latency */
				values[cols++] = Float8GetDatum(INSTR_TIME_GET_MICROSEC(cur_polar_proc_io_stat[index][POLARIO_LOCAL].io_creat_time));
				/* fsync_count */
				values[cols++] = Int64GetDatum(cur_polar_proc_io_stat[index][POLARIO_LOCAL].io_fsync_count);
				/* fsync_latency */
				values[cols++] = Float8GetDatum(INSTR_TIME_GET_MICROSEC(cur_polar_proc_io_stat[index][POLARIO_LOCAL].io_fsync_time));
				/* falloc_count */
				values[cols++] = Int64GetDatum(cur_polar_proc_io_stat[index][POLARIO_LOCAL].io_falloc_count);
				/* falloc_latency */
				values[cols++] = Float8GetDatum(INSTR_TIME_GET_MICROSEC(cur_polar_proc_io_stat[index][POLARIO_LOCAL].io_falloc_time));
				tuplestore_putvalues(tupstore, tupdesc, values, nulls);
			}
		}
	}
	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

/*
 * return the IO stat info ever flie type
 */
Datum
polar_io_latency_info(PG_FUNCTION_ARGS)
{
	int         	curr_backend;
	int  			index = 0;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	uint64 (*cur_num_latency_dist)[LATENCY_INTERVAL_LEN];

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

	tupdesc = CreateTemplateTupleDesc(LATENCY_INTERVAL_LEN + 2, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "pid",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "IOKind",
						TEXTOID, -1, 0);
	for (index = 0;index < LATENCY_INTERVAL_LEN; index++)
	{
		TupleDescInitEntry(tupdesc, (AttrNumber) 3+index, polar_latency_infos[index].latencyName,
						INT8OID, -1, 0);
	}
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	if (!PolarIOStatArray)
	{
		tuplestore_donestoring(tupstore);
		elog(ERROR, "Io statistics is unavailable!");
	}

	/* 1-based index */
	for (curr_backend = 0; curr_backend <= PolarNumProcIOStatSlots; curr_backend++)
	{
		int 		cur_pid = 0;
		/* Get the next one in the list */
		cur_pid = curr_backend == 0 ? 0 : PolarIOStatArray[curr_backend].pid;
		if (curr_backend > 0 && cur_pid <= 0)
			continue;

		cur_num_latency_dist = PolarIOStatArray[curr_backend].num_latency_dist;
		for (index = 0; index < LATENCY_KIND_LEN; index++)
		{
			/* for each row */
			int			cur;
			Datum		values[LATENCY_INTERVAL_LEN + 2];
			bool		nulls[LATENCY_INTERVAL_LEN + 2];	
			MemSet(values, 0, sizeof(values));
			MemSet(nulls, 0, sizeof(nulls));
			values[0] = Int32GetDatum(cur_pid);
			values[1] = CStringGetTextDatum(polar_io_type_names[index]);
			for (cur = 0; cur < LATENCY_INTERVAL_LEN; cur ++)
			{
				values[cur + 2] = Int64GetDatum(cur_num_latency_dist[index][polar_latency_infos[cur].latencyInterval]);
			}
		
            tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		}
	}
	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

/* POLAR: io read info time */
Datum
polar_io_read_delta_info(PG_FUNCTION_ARGS)
{
#define IO_READ_DELTA_INFO_LEN 10
	int i, j;
	int cols = 1;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	Datum		values[IO_READ_DELTA_INFO_LEN];
	bool		nulls[IO_READ_DELTA_INFO_LEN];
	Datum top_throughtput[POLAR_PROC_GLOBAL_IO_READ_LEN];

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

	tupdesc = CreateTemplateTupleDesc(IO_READ_DELTA_INFO_LEN, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "count",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "force_delay_times",
						INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "less_than_delay_times",
						INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "more_than_delay_times",
						INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "read_size_avg",
						INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "read_time_avg",
						FLOAT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "current_throughtput",
						FLOAT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "max_throughtput",
						INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "user_set_throughtput",
						BOOLOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "top_throughtput",
						INT8ARRAYOID, -1, 0);

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	MemSet(values, 0, sizeof(Datum) * IO_READ_DELTA_INFO_LEN);
	MemSet(nulls, 0, sizeof(bool) * IO_READ_DELTA_INFO_LEN);
	MemSet(top_throughtput, 0, sizeof(Datum) * POLAR_PROC_GLOBAL_IO_READ_LEN);
	i = 0;
	values[i++] = Int32GetDatum(PolarGlobalIOReadStats->count);
	values[i++] = UInt64GetDatum(PolarGlobalIOReadStats->force_delay);
	values[i++] = UInt64GetDatum(PolarGlobalIOReadStats->less_than_delay);
	values[i++] = UInt64GetDatum(PolarGlobalIOReadStats->more_than_delay);
	values[i++] = UInt64GetDatum(PolarGlobalIOReadStats->io_read_size_avg);
	values[i++] = Float8GetDatum(PolarGlobalIOReadStats->io_read_time_avg);
	values[i++] = Float8GetDatum((double)PolarGlobalIOReadStats->io_read_size_avg / PolarGlobalIOReadStats->io_read_time_avg);
	values[i++] = UInt64GetDatum(PolarGlobalIOReadStats->max_throughtput);
	values[i++] = (polar_io_read_throughtput_userset > 0) ? true : false;

	for (j = 0; j < POLAR_PROC_GLOBAL_IO_READ_LEN; j++)
	{
		if (PolarGlobalIOReadStats->io_read_throughtput[j] == 0)
			break;
		top_throughtput[j] = UInt64GetDatum(PolarGlobalIOReadStats->io_read_throughtput[j]);
	}
	values[i++] = PointerGetDatum(construct_array(top_throughtput,
												  j,
												  INT8OID,
												  8, FLOAT8PASSBYVAL, 'd'));

	tuplestore_putvalues(tupstore, tupdesc, values, nulls);

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}
