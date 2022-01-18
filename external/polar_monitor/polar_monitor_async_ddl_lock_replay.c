/*-------------------------------------------------------------------------
 *
 * polar_monitor_async_ddl_lock_replay.c
 *	  display some information of async ddl lock.
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
 *	  contrib/polar_monitor/polar_monitor_async_ddl_lock_replay.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/polar_async_ddl_lock_replay.h"
#include "access/polar_logindex.h"
#include "access/polar_logindex_internal.h"
#include "access/slru.h"
#include "access/xlog.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "libpq/libpq-be.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "procstat.h"
#include "replication/slot.h"
#include "replication/walreceiver.h"
#include "replication/walsender.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/pg_shmem.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/pg_lsn.h"
#include "utils/timestamp.h"

PG_FUNCTION_INFO_V1(polar_stat_async_ddl_lock_replay_worker);
Datum
polar_stat_async_ddl_lock_replay_worker(PG_FUNCTION_ARGS)
{
#define ASYNC_DDL_LOCK_REPLAY_COUNT 9
	int i;
	int cols = 1;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	polar_async_ddl_lock_replay_worker_t *workers;

	if (!polar_in_replica_mode())
		elog(ERROR, "It's not under replica mode");

	if (!polar_enable_async_ddl_lock_replay)
		elog(ERROR, "polar_enable_async_ddl_lock_replay is set to off");

	if (polar_async_ddl_lock_replay_worker_num == 0)
		elog(ERROR, "polar_async_ddl_lock_replay_worker_num is set to 0");

	/* Build a tuple descriptor for our result type */
	tupdesc = CreateTemplateTupleDesc(ASYNC_DDL_LOCK_REPLAY_COUNT, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "id",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "pid",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "xid",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "last end lsn",
						LSNOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "commit_state",
						TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "dbOid",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "relOid",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "rtime",
						TIMESTAMPTZOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "state",
						TEXTOID, -1, 0);

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	workers = polar_async_ddl_lock_replay_ctl->workers;
	LWLockAcquire(&polar_async_ddl_lock_replay_ctl->tx_tbl_lock, LW_SHARED);
	for (i = 0; i < polar_async_ddl_lock_replay_worker_num; ++i)
	{
		Datum		values[ASYNC_DDL_LOCK_REPLAY_COUNT];
		bool		nulls[ASYNC_DDL_LOCK_REPLAY_COUNT];
		bool		got = false;

		MemSet(nulls, 0, sizeof(bool) * ASYNC_DDL_LOCK_REPLAY_COUNT);

		values[0] = UInt32GetDatum(workers[i].id);
		values[1] = UInt32GetDatum(workers[i].pid);
		LWLockAcquire(&workers[i].lock, LW_SHARED);
		if (workers[i].cur_tx && (got = LWLockAcquire(&workers[i].cur_tx->lock, LW_SHARED)) && workers[i].cur_tx)
		{
			polar_pending_tx *tx = workers[i].cur_tx;
			polar_pending_lock *lock = tx->cur_lock;
			values[2] = ObjectIdGetDatum(lock->xid);
			values[3] = LSNGetDatum(lock->last_ptr);
			values[4] = CStringGetTextDatum(POLAR_PENDING_TX_RELEASE_STATE_STR(tx->commit_state));

			values[5] = ObjectIdGetDatum(lock->dbOid);
			values[6] = ObjectIdGetDatum(lock->relOid);
			values[7] = TimestampTzGetDatum(lock->rtime);
			values[8] = CStringGetTextDatum(POLAR_PENDING_LOCK_GET_STATE_STR(lock->state));
		}
		else
			nulls[2] = nulls[3] = nulls[4] = nulls[5] = nulls[6] = nulls[7] = nulls[8] = true;

		if (got)
			LWLockRelease(&workers[i].cur_tx->lock);

		LWLockRelease(&workers[i].lock);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}
	LWLockRelease(&polar_async_ddl_lock_replay_ctl->tx_tbl_lock);

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

PG_FUNCTION_INFO_V1(polar_stat_async_ddl_lock_replay_transaction);
Datum
polar_stat_async_ddl_lock_replay_transaction(PG_FUNCTION_ARGS)
{
#define ASYNC_DDL_LOCK_REPLAY_COUNT 9
	int cols = 1;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	HASH_SEQ_STATUS status;
	polar_pending_tx *tx;

	if (!polar_in_replica_mode())
		elog(ERROR, "It's not under replica mode");

	if (!polar_enable_async_ddl_lock_replay)
		elog(ERROR, "polar_enable_async_ddl_lock_replay is set to off");

	if (polar_async_ddl_lock_replay_worker_num == 0)
		elog(ERROR, "polar_async_ddl_lock_replay_worker_num is set to 0");

	/* Build a tuple descriptor for our result type */
	tupdesc = CreateTemplateTupleDesc(ASYNC_DDL_LOCK_REPLAY_COUNT, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "xid",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "last end lsn",
						LSNOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "commit_state",
						TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "dbOid",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "relOid",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "rtime",
						TIMESTAMPTZOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "state",
						TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "worker_id",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "worker_pid",
						INT4OID, -1, 0);

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);
	LWLockAcquire(&polar_async_ddl_lock_replay_ctl->tx_tbl_lock, LW_SHARED);
	hash_seq_init(&status, polar_async_ddl_lock_replay_ctl->entries);
	while ((tx = hash_seq_search(&status)))
	{
		Datum		values[ASYNC_DDL_LOCK_REPLAY_COUNT];
		bool		nulls[ASYNC_DDL_LOCK_REPLAY_COUNT];
		polar_pending_lock *lock;
		lock = tx->cur_lock;

		MemSet(nulls, 0, sizeof(bool) * ASYNC_DDL_LOCK_REPLAY_COUNT);

		LWLockAcquire(&tx->lock, LW_SHARED);
		values[0] = ObjectIdGetDatum(tx->xid);
		values[1] = LSNGetDatum(tx->last_ptr);
		values[2] = CStringGetTextDatum(POLAR_PENDING_TX_RELEASE_STATE_STR(tx->commit_state));

		values[3] = ObjectIdGetDatum(lock->dbOid);
		values[4] = ObjectIdGetDatum(lock->relOid);
		values[5] = TimestampTzGetDatum(lock->rtime);
		values[6] = CStringGetTextDatum(POLAR_PENDING_LOCK_GET_STATE_STR(lock->state));

		if (tx->worker)
		{
			values[7] = UInt32GetDatum(tx->worker->id);
			values[8] = UInt32GetDatum(tx->worker->pid);
		}
		else
			nulls[7] = nulls[8] = true;

		LWLockRelease(&tx->lock);
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}
	LWLockRelease(&polar_async_ddl_lock_replay_ctl->tx_tbl_lock);
	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

PG_FUNCTION_INFO_V1(polar_stat_async_ddl_lock_replay_lock);
Datum
polar_stat_async_ddl_lock_replay_lock(PG_FUNCTION_ARGS)
{
#define ASYNC_DDL_LOCK_REPLAY_COUNT 9
	int cols = 1;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	HASH_SEQ_STATUS status;
	polar_pending_tx *tx;

	if (!polar_in_replica_mode())
		elog(ERROR, "It's not under replica mode");

	if (!polar_enable_async_ddl_lock_replay)
		elog(ERROR, "polar_enable_async_ddl_lock_replay is set to off");

	if (polar_async_ddl_lock_replay_worker_num == 0)
		elog(ERROR, "polar_async_ddl_lock_replay_worker_num is set to 0");

	/* Build a tuple descriptor for our result type */
	tupdesc = CreateTemplateTupleDesc(ASYNC_DDL_LOCK_REPLAY_COUNT, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "xid",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "dbOid",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "relOid",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "last end lsn",
						LSNOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "rtime",
						TIMESTAMPTZOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "state",
						TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "commit_state",
						TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "worker_id",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "worker_pid",
						INT4OID, -1, 0);

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);
	LWLockAcquire(&polar_async_ddl_lock_replay_ctl->tx_tbl_lock, LW_SHARED);
	hash_seq_init(&status, polar_async_ddl_lock_replay_ctl->entries);
	while ((tx = hash_seq_search(&status)))
	{
		Datum		values[ASYNC_DDL_LOCK_REPLAY_COUNT];
		bool		nulls[ASYNC_DDL_LOCK_REPLAY_COUNT];
		polar_pending_lock *lock;

		LWLockAcquire(&tx->lock, LW_SHARED);
		for (lock = tx->head; lock != NULL; lock = lock->next)
		{
			MemSet(nulls, 0, sizeof(bool) * ASYNC_DDL_LOCK_REPLAY_COUNT);
			values[0] = ObjectIdGetDatum(lock->xid);
			values[1] = ObjectIdGetDatum(lock->dbOid);
			values[2] = ObjectIdGetDatum(lock->relOid);
			values[3] = LSNGetDatum(lock->last_ptr);
			values[4] = TimestampTzGetDatum(lock->rtime);

			values[5] = CStringGetTextDatum(POLAR_PENDING_LOCK_GET_STATE_STR(lock->state));
			values[6] = CStringGetTextDatum(POLAR_PENDING_TX_RELEASE_STATE_STR(tx->commit_state));

			if (tx->worker)
			{
				values[7] = UInt32GetDatum(tx->worker->id);
				values[8] = UInt32GetDatum(tx->worker->pid);
			}
			else
				nulls[7] = nulls[8] = true;
			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		}
		LWLockRelease(&tx->lock);
	}
	LWLockRelease(&polar_async_ddl_lock_replay_ctl->tx_tbl_lock);
	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}
