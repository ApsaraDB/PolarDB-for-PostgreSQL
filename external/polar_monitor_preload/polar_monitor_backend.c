/*-------------------------------------------------------------------------
 *
 * polar_monitor_backend.c
 *    views of polardb monitor statistics by backend type.
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
 *    external/polar_monitor/polar_monitor_backend.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_type.h"
#include "funcapi.h"
#include "libpq/polar_network_stats.h"
#include "miscadmin.h"
#include "polar_monitor_backend.h"
#include "polar_procstat.h"
#include "postmaster/bgworker.h"
#include "storage/lwlock.h"
#include "storage/backendid.h"
#include "storage/ipc.h"
#include "storage/polar_lock_stats.h"
#include "storage/polar_io_stat.h"
#include "storage/sinvaladt.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "postmaster/polar_dispatcher.h"

#define BACKEND_TYPE_MAXLEN		96
#define BACKEND_MAX_NUMBER      (1024)

typedef enum STAT_COL_DEF
{
	COL_BACKEND_TYPE = 0,
	COL_CPU_USER,
	COL_CPU_SYS,
	COL_RSS,
	COL_SHARED_READ_PS,
	COL_SHARED_WRITE_PS,
	COL_SHARED_READ_THROUGHPUT,
	COL_SHARED_WRITE_THROUGHPUT,
	COL_SHARED_READ_LATENCY,
	COL_SHARED_WRITE_LATENCY,
	COL_LOCAL_READ_PS,
	COL_LOCAL_WRITE_PS,
	COL_LOCAL_READ_THROUGHPUT,
	COL_LOCAL_WRITE_THROUGHPUT,
	COL_LOCAL_READ_LATENCY,
	COL_LOCAL_WRITE_LATENCY,
	COL_SEND_COUNT,
	COL_SEND_BYTES,
	COL_RECV_COUNT,
	COL_RECV_BYTES,
	COL_POLAR_STAT_BACKEND
} STAT_COL_DEF;


typedef struct polarBackendHashKey
{
    char  backend_type[BACKEND_TYPE_MAXLEN];
}polarBackendHashKey;

typedef struct pgBackendCounters
{
	char  	backend_type[BACKEND_TYPE_MAXLEN];
	int64	cpu_user;
	int64	cpu_sys;
	int64 	rss;
	int64	shared_read_ps;
	int64	shared_write_ps;
	int64	shared_read_throughput;
	int64	shared_write_throughput;
	double	shared_read_latency;
	double 	shared_write_latency;
	int64	local_read_ps;
	int64	local_write_ps;
	int64	local_read_throughput;
	int64	local_write_throughput;
	double	local_read_latency;
	double 	local_write_latency;
	int64	send_count;	
	int64	send_bytes;
	int64	recv_count;
	int64	recv_bytes;
} pgBackendCounters;

typedef struct polarBackendEntry
{
	polarBackendHashKey key;	/* hash key of entry - MUST BE FIRST */
	pgBackendCounters	counters;		/* the statistics for this query */
	Size		query_offset;	/* query text offset in external file */
	int			query_len;		/* # of valid bytes in query string, or -1 */
	int			encoding;		/* query text encoding */
	slock_t		mutex;			/* protects the counters only */
} polarBackendEntry;

typedef struct
{
	LWLock	   *lock;					/* protects hashtable search/modification */
} polarBacktypeSharedState;


static polarBacktypeSharedState *polar_monitor_backtype_share_state = NULL;
static HTAB *polar_monitor_backend_hash = NULL;
int	polar_hash_entry_max = 0;

static void polar_monitor_backend_entry_store(HTAB *tmp_backend_htab, pgBackendCounters counters, LWLock *lock);
static int polar_backend_collect_stat(pgBackendCounters *counters, int backendid);
/*
 * POLAR: return the cpu stat info ever backend
 */
PG_FUNCTION_INFO_V1(polar_stat_backend);
Datum
polar_stat_backend(PG_FUNCTION_ARGS)
{
	ReturnSetInfo	*rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	MemoryContext	per_query_ctx;
	MemoryContext	oldcontext;
	TupleDesc		tupdesc;
	Tuplestorestate	*tupstore;
	HASH_SEQ_STATUS hash_seq;
	polarBackendEntry		*entry;
	int num_backends = pgstat_fetch_stat_numbackends();
	int	curr_backend = 1;
	HTAB	   *tmp_backend_htab;
	HASHCTL		tmp_backend_hash_ctl;
	HASH_SEQ_STATUS tmp_hash_seq;

	if (!polar_monitor_backend_hash)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("polar monitor must be loaded via shared_preload_libraries")));
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

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	memset(&tmp_backend_hash_ctl, 0, sizeof(tmp_backend_hash_ctl));
	tmp_backend_hash_ctl.keysize = sizeof(polarBackendHashKey);
	tmp_backend_hash_ctl.entrysize = sizeof(polarBackendEntry);
	tmp_backend_htab = hash_create("Temporary table of all backend info",
					   BACKEND_MAX_NUMBER,
					   &tmp_backend_hash_ctl,
					   HASH_ELEM | HASH_BLOBS );
	/* check create hash table */
	if(!tmp_backend_htab)
		elog(ERROR, "create hash table fail ");;

	/* copy the history backend statistics info the temp hash table */
	LWLockAcquire(polar_monitor_backtype_share_state->lock, LW_SHARED);
	hash_seq_init(&hash_seq, polar_monitor_backend_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		pgBackendCounters	tmp;
		volatile polarBackendEntry *e = (volatile polarBackendEntry *) entry;

		memset(&tmp, 0, sizeof(tmp));

		SpinLockAcquire(&e->mutex);
		tmp = e->counters;
		SpinLockRelease(&e->mutex);
		
		polar_monitor_backend_entry_store(tmp_backend_htab, tmp, NULL);
	
	}
	LWLockRelease(polar_monitor_backtype_share_state->lock);

	/* add active backend statistics to the temp hash table*/
	for (curr_backend = 1; curr_backend <= num_backends; curr_backend++)
	{
		pgBackendCounters	counters;
		memset(&counters, 0, sizeof(counters));

		/* collect backend statistics */
		if (0 != polar_backend_collect_stat(&counters, curr_backend))
			continue;
		
		polar_monitor_backend_entry_store(tmp_backend_htab, counters, NULL);
	}
	
	/* get all the backend statistics by search the temp hash table */
	hash_seq_init(&tmp_hash_seq, tmp_backend_htab);
	while ((entry = hash_seq_search(&tmp_hash_seq)) != NULL)
	{
		Datum			values[COL_POLAR_STAT_BACKEND];
		bool			nulls[COL_POLAR_STAT_BACKEND];
		pgBackendCounters	tmp;
		volatile polarBackendEntry *e = (volatile polarBackendEntry *) entry;

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		tmp = e->counters;

		values[COL_BACKEND_TYPE] = CStringGetTextDatum(entry->key.backend_type);

		values[COL_CPU_USER] = Int64GetDatumFast(tmp.cpu_user);
		values[COL_CPU_SYS] = Int64GetDatumFast(tmp.cpu_sys);
		values[COL_RSS] = Int64GetDatumFast(tmp.rss);

		values[COL_SHARED_READ_PS] = Int64GetDatumFast(tmp.shared_read_ps);
		values[COL_SHARED_WRITE_PS] = Int64GetDatumFast(tmp.shared_write_ps);
		values[COL_SHARED_READ_THROUGHPUT] = Int64GetDatumFast(tmp.shared_read_throughput);
		values[COL_SHARED_WRITE_THROUGHPUT] = Int64GetDatumFast(tmp.shared_write_throughput);
		values[COL_SHARED_READ_LATENCY] = Float8GetDatumFast(tmp.shared_read_latency);
		values[COL_SHARED_WRITE_LATENCY] = Float8GetDatumFast(tmp.shared_write_latency);

		values[COL_LOCAL_READ_PS] = Int64GetDatumFast(tmp.local_read_ps);
		values[COL_LOCAL_WRITE_PS] = Int64GetDatumFast(tmp.local_write_ps);
		values[COL_LOCAL_READ_THROUGHPUT] = Int64GetDatumFast(tmp.local_read_throughput);
		values[COL_LOCAL_WRITE_THROUGHPUT] = Int64GetDatumFast(tmp.local_write_throughput);
		values[COL_LOCAL_READ_LATENCY] = Float8GetDatumFast(tmp.local_read_latency);
		values[COL_LOCAL_WRITE_LATENCY] = Float8GetDatumFast(tmp.local_write_latency);

		values[COL_SEND_COUNT] = Int64GetDatumFast(tmp.send_count);
		values[COL_SEND_BYTES] = Int64GetDatumFast(tmp.send_bytes);

		values[COL_RECV_COUNT] = Int64GetDatumFast(tmp.recv_count);
		values[COL_RECV_BYTES] = Int64GetDatumFast(tmp.recv_bytes);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	hash_destroy(tmp_backend_htab);
	tmp_backend_htab = NULL;

	return (Datum) 0;
}

/*
 * Allocate a new hashtable entry.
 * caller must hold an exclusive lock on pgss->lock
 */
static polarBackendEntry *polar_monitor_entry_alloc(HTAB *tmp_backend_htab, polarBackendHashKey *key)
{
	polarBackendEntry  	*entry;
	bool				found;

	/* Find or create an entry with desired hash code */
	entry = (polarBackendEntry *) hash_search(tmp_backend_htab, key, HASH_ENTER, &found);

	if (!found)
	{
		/* New entry, initialize it */

		/* reset the statistics */
		memset(&entry->counters, 0, sizeof(pgBackendCounters));

		/* re-initialize the mutex each time ... we assume no one using it */
		SpinLockInit(&entry->mutex);
	}

	return entry;
}

static void
polar_monitor_backend_entry_store(HTAB *tmp_backend_htab, pgBackendCounters counters, LWLock *lock)
{
	volatile polarBackendEntry *e;

	polarBackendHashKey key;
	polarBackendEntry  *entry;

	memset(key.backend_type, 0, sizeof(key.backend_type));

	/* Safety check... */
	if (!tmp_backend_htab)
		return;

	/* Set up key for hashtable search */
	strcpy(key.backend_type, counters.backend_type);

	/* Lookup the hash table entry with shared lock. */
	if (lock)
	{
		LWLockAcquire(lock, LW_SHARED);
	}
	
	entry = (polarBackendEntry *) hash_search(tmp_backend_htab, &key, HASH_FIND, NULL);

	/* Create new entry, if not present */
	if (!entry)
	{
		/* Need exclusive lock to make a new hashtable entry - promote */
		if (lock)
		{
			LWLockRelease(lock);
			LWLockAcquire(lock, LW_EXCLUSIVE);
		}

		/* OK to create a new hashtable entry */
		entry = polar_monitor_entry_alloc(tmp_backend_htab, &key);
	}

	/*
	 * Grab the spinlock while updating the counters (see comment about
	 * locking rules at the head of the file)
	 */
	e = (volatile polarBackendEntry *) entry;

	SpinLockAcquire(&e->mutex);

	strcpy((char *)e->counters.backend_type, counters.backend_type);

	e->counters.cpu_user += counters.cpu_user;
	e->counters.cpu_sys += counters.cpu_sys;
	e->counters.rss += counters.rss;

	e->counters.shared_read_ps += counters.shared_read_ps;
	e->counters.shared_write_ps += counters.shared_write_ps;

	e->counters.shared_read_throughput += counters.shared_read_throughput;
	e->counters.shared_write_throughput += counters.shared_write_throughput;

	e->counters.shared_read_latency += counters.shared_read_latency;
	e->counters.shared_write_latency += counters.shared_write_latency;

	e->counters.local_read_ps += counters.local_read_ps;
	e->counters.local_write_ps += counters.local_write_ps;

	e->counters.local_read_throughput += counters.local_read_throughput;
	e->counters.local_write_throughput += counters.local_write_throughput;

	e->counters.local_read_latency += counters.local_read_latency;
	e->counters.local_write_latency += counters.local_write_latency;

	e->counters.send_count += counters.send_count;
	e->counters.send_bytes += counters.send_bytes;

	e->counters.recv_count += counters.recv_count;
	e->counters.recv_bytes += counters.recv_bytes;

	SpinLockRelease(&e->mutex);

	if (lock)
	{
		LWLockRelease(lock);
	}

}

/*
 * collect the statistics of backends.
 *
 * We need to check the result, when call this funcition.
 * The value of result, -1 means fail, 0 means succeed.
 */
static int
polar_backend_collect_stat(pgBackendCounters *counters, int backendid)
{
	
	LocalPgBackendStatus *local_beentry;
	PgBackendStatus *beentry;
	polar_proc_stat procstat;
	polar_net_stat 	*net_procstat;
	int  			index = 0;
	const char *backend_type;

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

	memset(&procstat, 0, sizeof(procstat));

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

	/* POLAR: Shared Server */
	/* TODO: not compat this monitor now @yanhua */
	if (POLAR_SHARED_SERVER_RUNNING() &&
		beentry->st_backendType == B_BACKEND &&
		beentry->session_local_id > 0)
		return -1;

	/* Add backend type */
	if (beentry->st_backendType == B_BG_WORKER)
	{
		backend_type = GetBackgroundWorkerTypeByPid(beentry->st_procpid);
	}
	else
	{
		backend_type = pgstat_get_backend_desc(beentry->st_backendType);
	}

	if (!backend_type)
	{
		/* Ignore if backend type is empty*/
		return -1;
	}

	strcpy(counters->backend_type, backend_type);

	if(!polar_get_proc_stat(beentry->st_procpid, &procstat))
	{
		counters->cpu_user = procstat.utime;
		counters->cpu_sys = procstat.stime;
		counters->rss = procstat.rss - procstat.share;
	}

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
		counters->shared_read_ps = shared_read_ps;
		counters->shared_write_ps = shared_write_ps;

		/* pfs io throughput */
		counters->shared_read_throughput = shared_read_throughput;
		counters->shared_write_throughput = shared_write_throughput;

		/* pfs io latency */
		counters->shared_read_latency = INSTR_TIME_GET_MILLISEC(shared_read_latency);
		counters->shared_write_latency = INSTR_TIME_GET_MILLISEC(shared_write_latency);

		/* local iops */
		counters->local_read_ps = local_read_ps;
		counters->local_write_ps = local_write_ps;

		/* local io throughput */
		counters->local_read_throughput = local_read_throughput;
		counters->local_write_throughput = local_write_throughput;

		/* local io latency */
		counters->local_read_latency = INSTR_TIME_GET_MILLISEC(local_read_latency);
		counters->local_write_latency = INSTR_TIME_GET_MILLISEC(local_write_latency);
	}


	net_procstat = &polar_network_stat_array[backendid - 1];

	counters->send_count = Int64GetDatumFast(net_procstat->opstat[POLAR_NETWORK_SEND_STAT].count);
	counters->send_bytes = Int64GetDatumFast(net_procstat->opstat[POLAR_NETWORK_SEND_STAT].bytes);

	counters->recv_count = Int64GetDatumFast(net_procstat->opstat[POLAR_NETWORK_RECV_STAT].count);
	counters->recv_bytes = Int64GetDatumFast(net_procstat->opstat[POLAR_NETWORK_RECV_STAT].bytes);

	return 0;
}

void 
polar_backend_stat_shmem_shutdown(int code, Datum arg)
{
	polar_net_stat 	*net_procstat;
	pgBackendCounters	counters;

	memset(&counters, 0, sizeof(counters));

	if (0 != polar_backend_collect_stat(&counters, MyBackendId))
		return;
	
	/* process exit, need to memset the network stat */
	net_procstat = &polar_network_stat_array[MyBackendId - 1];
	memset(net_procstat, 0, sizeof(polar_net_stat));

	polar_monitor_backend_entry_store(polar_monitor_backend_hash, counters, polar_monitor_backtype_share_state->lock);

}

Size backend_stat_memsize(void)
{
	Size	size;

	Assert(polar_hash_entry_max != 0);

	size = MAXALIGN(sizeof(polarBacktypeSharedState));
	size = add_size(size, hash_estimate_size(polar_hash_entry_max, sizeof(polarBackendEntry)));

	return size;
}

void
polar_backend_stat_shmem_startup(void)
{
	HASHCTL		info;
	bool 		found;
	
	/* Create or attach to the shared memory state */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	/* global access lock */
	polar_monitor_backtype_share_state = ShmemInitStruct("polar_monitor",
					sizeof(polarBacktypeSharedState),
					&found);

	if (!found)
	{
		/* First time through ... */
		polar_monitor_backtype_share_state->lock = &(GetNamedLWLockTranche("polar_stat_backend"))->lock;
	}

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(polarBackendHashKey);
	info.entrysize = sizeof(polarBackendEntry);

	/* allocate stats shared memory hash */
	polar_monitor_backend_hash = ShmemInitHash("polar_monitor cpu hash",
							  polar_hash_entry_max, polar_hash_entry_max,
							  &info,
							  HASH_ELEM | HASH_BLOBS);

	LWLockRelease(AddinShmemInitLock);

	return;

}

PG_FUNCTION_INFO_V1(polar_current_backend_pid);
Datum
polar_current_backend_pid(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(polar_pgstat_get_virtual_pid(MyProcPid, false));
}
