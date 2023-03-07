/*-------------------------------------------------------------------------
 *
 * polar_monitor.c
 *    display some information of polardb
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
 *    external/polar_monitor/polar_monitor.c
 *-------------------------------------------------------------------------
 */

#include "polar_monitor.h"

#define NUM_REPLICATION_SLOTS_ELEN 14
#define NUM_REPLICA_MULTI_VERSION_SNAPSHOT_ELEM 9


PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(polar_consistent_lsn);

Datum
polar_consistent_lsn(PG_FUNCTION_ARGS)
{
	XLogRecPtr cosistent_recptr;

	cosistent_recptr = polar_get_consistent_lsn();

	PG_RETURN_LSN(cosistent_recptr);
}

PG_FUNCTION_INFO_V1(polar_oldest_apply_lsn);

Datum
polar_oldest_apply_lsn(PG_FUNCTION_ARGS)
{
	if (RecoveryInProgress())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg(
					 "recovery is in progress"), errhint(
					 "WAL control functions cannot be executed during recovery.")));

	PG_RETURN_LSN(polar_get_oldest_applied_lsn());
}

PG_FUNCTION_INFO_V1(polar_oldest_lock_lsn);

Datum
polar_oldest_lock_lsn(PG_FUNCTION_ARGS)
{
	if (RecoveryInProgress())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg(
					 "recovery is in progress"), errhint(
					 "WAL control functions cannot be executed during recovery.")));

	PG_RETURN_LSN(polar_get_oldest_lock_lsn());
}

/*
 * useless function, keep them just for not influence performance collection tools
 */
PG_FUNCTION_INFO_V1(polar_xlog_buffer_full);		
Datum		
polar_xlog_buffer_full(PG_FUNCTION_ARGS)		
{		
	PG_RETURN_BOOL(false);		
}

PG_FUNCTION_INFO_V1(polar_page_hashtable_full);		
Datum		
polar_page_hashtable_full(PG_FUNCTION_ARGS)		
{		
	PG_RETURN_BOOL(false);		
}

PG_FUNCTION_INFO_V1(polar_page_hashtable_used_size);		
Datum		
polar_page_hashtable_used_size(PG_FUNCTION_ARGS)		
{		
	PG_RETURN_UINT32(0);		
}		

/*
 * Used in replica and calculate min LSN used by replica
 * backends or background process
 */
PG_FUNCTION_INFO_V1(polar_replica_min_used_lsn);
Datum
polar_replica_min_used_lsn(PG_FUNCTION_ARGS)
{
	XLogRecPtr min_lsn = InvalidXLogRecPtr;

	if (polar_in_replica_mode())
		min_lsn = polar_get_read_min_lsn(polar_get_primary_consist_ptr());

	PG_RETURN_LSN(min_lsn);
}

/*
 * Used in master and calculate min LSN used by cluster.
 * The WAL and logindex which LSN is less than min LSN
 * can be removed
 */
PG_FUNCTION_INFO_V1(polar_min_used_lsn);
Datum
polar_min_used_lsn(PG_FUNCTION_ARGS)
{
	XLogRecPtr min_lsn = polar_calc_min_used_lsn(true);

	PG_RETURN_LSN(min_lsn);
}

/* Get logindex mem table size */
PG_FUNCTION_INFO_V1(polar_get_logindex_mem_tbl_size);
Datum
polar_get_logindex_mem_tbl_size(PG_FUNCTION_ARGS)
{
	log_idx_table_id_t mem_tbl_size = 0;
	if (polar_logindex_redo_instance && polar_logindex_redo_instance->wal_logindex_snapshot)
		mem_tbl_size = polar_logindex_mem_tbl_size(polar_logindex_redo_instance->wal_logindex_snapshot);
	return (Datum)mem_tbl_size;
}

/* Get normal logindex snapshot used mem table size */
PG_FUNCTION_INFO_V1(polar_used_logindex_mem_tbl_size);
Datum
polar_used_logindex_mem_tbl_size(PG_FUNCTION_ARGS)
{
	log_idx_table_id_t mem_tbl_size = 0;
	if (polar_logindex_redo_instance && polar_logindex_redo_instance->wal_logindex_snapshot)
		mem_tbl_size = polar_logindex_used_mem_tbl_size(polar_logindex_redo_instance->wal_logindex_snapshot);

	return mem_tbl_size;	
}

/* Get fullpage logindex snapshot used mem table size */
PG_FUNCTION_INFO_V1(polar_used_logindex_fullpage_snapshot_mem_tbl_size);
Datum
polar_used_logindex_fullpage_snapshot_mem_tbl_size(PG_FUNCTION_ARGS)
{
	log_idx_table_id_t mem_tbl_size = 0;
	if (polar_logindex_redo_instance && polar_logindex_redo_instance->fullpage_logindex_snapshot)
		mem_tbl_size = polar_logindex_used_mem_tbl_size(polar_logindex_redo_instance->fullpage_logindex_snapshot);

	return mem_tbl_size;	
}


/* Used in replica. Check whether wal receiver get xlog from xlog queue */
PG_FUNCTION_INFO_V1(polar_replica_use_xlog_queue);
Datum
polar_replica_use_xlog_queue(PG_FUNCTION_ARGS)
{
	bool used = false;

	if (polar_in_replica_mode() && WalRcv)
	{
		SpinLockAcquire(&WalRcv->mutex);
		used = WalRcv->polar_use_xlog_queue;
		SpinLockRelease(&WalRcv->mutex);
	}

	return (Datum)used;
}

/* Used in replica.Get background process replayed lsn */
PG_FUNCTION_INFO_V1(polar_replica_bg_replay_lsn);
Datum
polar_replica_bg_replay_lsn(PG_FUNCTION_ARGS)
{
	XLogRecPtr bg_lsn = polar_bg_redo_get_replayed_lsn(polar_logindex_redo_instance);

	PG_RETURN_LSN(bg_lsn);
}

PG_FUNCTION_INFO_V1(polar_get_replication_slots);

Datum
polar_get_replication_slots(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc   tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	int         slotno;
	XLogRecPtr	currlsn;

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
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/*
	 * We don't require any special permission to see this function's data
	 * because nothing should be sensitive. The most critical being the slot
	 * name, which shouldn't contain anything particularly sensitive.
	 */

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	currlsn = GetXLogWriteRecPtr();

	LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);

	for (slotno = 0; slotno < max_replication_slots; slotno++)
	{
		ReplicationSlot *slot = &ReplicationSlotCtl->replication_slots[slotno];
		Datum       values[NUM_REPLICATION_SLOTS_ELEN];
		bool        nulls[NUM_REPLICATION_SLOTS_ELEN];
		WALAvailability walstate;

		ReplicationSlotPersistency persistency;
		TransactionId xmin;
		TransactionId catalog_xmin;
		XLogRecPtr  restart_lsn;
		XLogRecPtr  confirmed_flush_lsn;
		XLogRecPtr  polar_apply_lsn;
		pid_t       active_pid;
		Oid         database;
		NameData    slot_name;
		NameData    plugin;
		int         i;

		if (!slot->in_use)
			continue;

		SpinLockAcquire(&slot->mutex);

		xmin = slot->data.xmin;
		catalog_xmin = slot->data.catalog_xmin;
		database = slot->data.database;
		restart_lsn = slot->data.restart_lsn;
		confirmed_flush_lsn = slot->data.confirmed_flush;
		polar_apply_lsn = slot->data.polar_replica_apply_lsn;
		namecpy(&slot_name, &slot->data.name);
		namecpy(&plugin, &slot->data.plugin);
		active_pid = slot->active_pid;
		persistency = slot->data.persistency;

		SpinLockRelease(&slot->mutex);

		memset(nulls, 0, sizeof(nulls));

		i = 0;
		values[i++] = NameGetDatum(&slot_name);

		if (database == InvalidOid)
			nulls[i++] = true;
		else
			values[i++] = NameGetDatum(&plugin);

		if (database == InvalidOid)
			values[i++] = CStringGetTextDatum("physical");
		else
			values[i++] = CStringGetTextDatum("logical");

		if (database == InvalidOid)
			nulls[i++] = true;
		else
			values[i++] = database;

		values[i++] = BoolGetDatum(persistency == RS_TEMPORARY);
		values[i++] = BoolGetDatum(active_pid != 0);

		/* POLAR: return virtual pid if available */
		if (active_pid != 0)
			values[i++] = Int32GetDatum(polar_pgstat_get_virtual_pid(active_pid, false));
		else
			nulls[i++] = true;

		if (xmin != InvalidTransactionId)
			values[i++] = TransactionIdGetDatum(xmin);
		else
			nulls[i++] = true;

		if (catalog_xmin != InvalidTransactionId)
			values[i++] = TransactionIdGetDatum(catalog_xmin);
		else
			nulls[i++] = true;

		if (restart_lsn != InvalidXLogRecPtr)
			values[i++] = LSNGetDatum(restart_lsn);
		else
			nulls[i++] = true;

		if (confirmed_flush_lsn != InvalidXLogRecPtr)
			values[i++] = LSNGetDatum(confirmed_flush_lsn);
		else
			nulls[i++] = true;

		if (polar_apply_lsn != InvalidXLogRecPtr)
			values[i++] = LSNGetDatum(polar_apply_lsn);
		else
			nulls[i++] = true;

		if (polar_enable_max_slot_wal_keep_size)
		{
			/*
			* If restart_lsn is invalid, we know for certain that the slot has been invalidated.
			* Otherwise, test availability from restart_lsn.
			*/
			if (XLogRecPtrIsInvalid(slot->data.restart_lsn) &&
				!XLogRecPtrIsInvalid(slot->data.polar_invalidated_at))
				walstate = WALAVAIL_REMOVED;
			else
				walstate = GetWALAvailability(slot->data.restart_lsn);

			switch (walstate)
			{
				case WALAVAIL_INVALID_LSN:
					nulls[i++] = true;
					break;

				case WALAVAIL_RESERVED:
					values[i++] = CStringGetTextDatum("reserved");
					break;

				case WALAVAIL_EXTENDED:
					values[i++] = CStringGetTextDatum("extended");
					break;

				case WALAVAIL_UNRESERVED:
					values[i++] = CStringGetTextDatum("unreserved");
					break;

				case WALAVAIL_REMOVED:

					/*
					* If we read the restart_lsn long enough ago, maybe that file
					* has been removed by now.  However, the walsender could have
					* moved forward enough that it jumped to another file after
					* we looked.  If checkpointer signalled the process to
					* termination, then it's definitely lost; but if a process is
					* still alive, then "unreserved" seems more appropriate.
					*
					* If we do change it, save the state for safe_wal_size below.
					*/
					if (!XLogRecPtrIsInvalid(slot->data.restart_lsn))
					{
						int			pid;

						SpinLockAcquire(&slot->mutex);
						pid = slot->active_pid;
						slot->data.restart_lsn = slot->data.restart_lsn;
						SpinLockRelease(&slot->mutex);
						if (pid != 0)
						{
							values[i++] = CStringGetTextDatum("unreserved");
							walstate = WALAVAIL_UNRESERVED;
							break;
						}
					}
					values[i++] = CStringGetTextDatum("lost");
					break;
			}

			/*
			* safe_wal_size is only computed for slots that have not been lost,
			* and only if there's a configured maximum size.
			*/
			if (walstate == WALAVAIL_REMOVED || walstate == WALAVAIL_INVALID_LSN || max_slot_wal_keep_size_mb < 0)
				nulls[i++] = true;
			else
			{
				XLogSegNo   targetSeg;
				uint64   slotKeepSegs;
				XLogSegNo   failSeg;
				XLogRecPtr  failLSN;

				XLByteToSeg(slot->data.restart_lsn, targetSeg, wal_segment_size);

				/* determine how many segments slots can be kept by slots */
				slotKeepSegs = XLogMBVarToSegs(max_slot_wal_keep_size_mb, wal_segment_size);

				/* if currpos reaches failLSN, we lose our segment */
				failSeg = targetSeg + Max(slotKeepSegs, (uint64)wal_keep_segments) + 1;
				XLogSegNoOffsetToRecPtr(failSeg, 0, wal_segment_size, failLSN);

				values[i++] = Int64GetDatum(failLSN - currlsn);
			}
		}
		else
		{
			nulls[i++] = true;
			nulls[i++] = true;
		}

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	LWLockRelease(ReplicationSlotControlLock);

	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}


/* bulk read stats */
/* per table (or index) */
PG_FUNCTION_INFO_V1(polar_pg_stat_get_bulk_read_calls);
Datum
polar_pg_stat_get_bulk_read_calls(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	int64		result;
	PgStat_StatTabEntry *tabentry;

	if ((tabentry = pgstat_fetch_stat_tabentry(relid)) == NULL)
		result = 0;
	else
		result = (int64) (tabentry->polar_bulk_read_calls);

	PG_RETURN_INT64(result);
}

PG_FUNCTION_INFO_V1(polar_pg_stat_get_bulk_read_calls_IO);
Datum
polar_pg_stat_get_bulk_read_calls_IO(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	int64		result;
	PgStat_StatTabEntry *tabentry;

	if ((tabentry = pgstat_fetch_stat_tabentry(relid)) == NULL)
		result = 0;
	else
		result = (int64) (tabentry->polar_bulk_read_calls_IO);

	PG_RETURN_INT64(result);
}

PG_FUNCTION_INFO_V1(polar_pg_stat_get_bulk_read_blocks_IO);
Datum
polar_pg_stat_get_bulk_read_blocks_IO(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	int64		result;
	PgStat_StatTabEntry *tabentry;

	if ((tabentry = pgstat_fetch_stat_tabentry(relid)) == NULL)
		result = 0;
	else
		result = (int64) (tabentry->polar_bulk_read_blocks_IO);

	PG_RETURN_INT64(result);
}

/* data per database */
PG_FUNCTION_INFO_V1(polar_pg_stat_get_db_bulk_read_calls);
Datum
polar_pg_stat_get_db_bulk_read_calls(PG_FUNCTION_ARGS)
{
	Oid			dbid = PG_GETARG_OID(0);
	int64		result;
	PgStat_StatDBEntry *dbentry;

	if ((dbentry = pgstat_fetch_stat_dbentry(dbid)) == NULL)
		result = 0;
	else
		result = (int64) (dbentry->polar_n_bulk_read_calls);

	PG_RETURN_INT64(result);
}

PG_FUNCTION_INFO_V1(polar_pg_stat_get_db_bulk_read_calls_IO);
Datum
polar_pg_stat_get_db_bulk_read_calls_IO(PG_FUNCTION_ARGS)
{
	Oid			dbid = PG_GETARG_OID(0);
	int64		result;
	PgStat_StatDBEntry *dbentry;

	if ((dbentry = pgstat_fetch_stat_dbentry(dbid)) == NULL)
		result = 0;
	else
		result = (int64) (dbentry->polar_n_bulk_read_calls_IO);

	PG_RETURN_INT64(result);
}

PG_FUNCTION_INFO_V1(polar_pg_stat_get_db_bulk_read_blocks_IO);
Datum
polar_pg_stat_get_db_bulk_read_blocks_IO(PG_FUNCTION_ARGS)
{
	Oid			dbid = PG_GETARG_OID(0);
	int64		result;
	PgStat_StatDBEntry *dbentry;

	if ((dbentry = pgstat_fetch_stat_dbentry(dbid)) == NULL)
		result = 0;
	else
		result = (int64) (dbentry->polar_n_bulk_read_blocks_IO);

	PG_RETURN_INT64(result);
}

PG_FUNCTION_INFO_V1(polar_get_node_type);
Datum
polar_get_node_type(PG_FUNCTION_ARGS)
{
	char	*mode;
	PolarNodeType node_type = polar_node_type();

	switch (node_type)
	{
		case POLAR_MASTER:
			mode = "master";
			break;
		case POLAR_REPLICA:
			mode = "replica";
			break;
		case POLAR_STANDBY:
			mode = "standby";
			break;
		case POLAR_STANDALONE_DATAMAX:
			mode = "standalone_datamax";
			break;
		default:
			mode = "unknown";
	}

	PG_RETURN_TEXT_P(cstring_to_text(mode));
}
/* end: bulk read stats */

PG_FUNCTION_INFO_V1(polar_get_multi_version_snapshot_store_info);

Datum
polar_get_multi_version_snapshot_store_info(PG_FUNCTION_ARGS)
{
	TupleDesc   tupdesc;
	Datum       values[NUM_REPLICA_MULTI_VERSION_SNAPSHOT_ELEM];
	bool        nulls[NUM_REPLICA_MULTI_VERSION_SNAPSHOT_ELEM];
	HeapTuple	tuple;
	Datum		result;
	int         i = 0;

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	
	memset(nulls, 0, sizeof(nulls));

	values[i++] = UInt64GetDatum(polar_replica_multi_version_snapshot_store_shmem_size());
	values[i++] = Int32GetDatum(polar_replica_multi_version_snapshot_get_slot_num());
	values[i++] = Int32GetDatum(polar_replica_multi_version_snapshot_get_retry_times());
	values[i++] = UInt32GetDatum(polar_replica_multi_version_snapshot_get_curr_slot_no());
	values[i++] = Int32GetDatum(polar_replica_multi_version_snapshot_get_next_slot_no());
	values[i++] = UInt64GetDatum(polar_replica_multi_version_snapshot_get_read_retried_times());
	values[i++] = UInt64GetDatum(polar_replica_multi_version_snapshot_get_read_switched_times());
	values[i++] = UInt64GetDatum(polar_replica_multi_version_snapshot_get_write_retried_times());
	values[i++] = UInt64GetDatum(polar_replica_multi_version_snapshot_get_write_switched_times());

	tuple = heap_form_tuple(BlessTupleDesc(tupdesc), values, nulls);
	result = HeapTupleGetDatum(tuple);

	PG_RETURN_DATUM(result);
}
/* POLAR: get delay dml count form collector process */
PG_FUNCTION_INFO_V1(polar_pg_stat_get_delay_dml_count);
Datum
polar_pg_stat_get_delay_dml_count(PG_FUNCTION_ARGS)
{
	Oid			dbid = PG_GETARG_OID(0);
	int64		result;
	PgStat_StatDBEntry *dbentry;

	if ((dbentry = pgstat_fetch_stat_dbentry(dbid)) == NULL)
		result = 0;
	else
		result = (int64) (dbentry->polar_delay_dml_count);

	PG_RETURN_INT64(result);
}

PG_FUNCTION_INFO_V1(polar_stat_slru);
/*
 * POLAR: slru stat
 */
Datum
polar_stat_slru(PG_FUNCTION_ARGS)
{
#define SLRU_COUNT 10
#define SLRU_STAT_COLS 19
	int i = 0;
	int cols = 1;
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
	tupdesc = CreateTemplateTupleDesc(SLRU_STAT_COLS, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "slru_type",
						TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "slots_number",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "valid_pages",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "empty_pages",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "reading_pages",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "writing_pages",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "wait_readings",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "wait_writings",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "read_count",
						INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "read_only_count",
						INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "read_upgrade_count",
						INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "victim_count",
						INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "victim_write_count",
						INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "write_count",
						INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "zero_count",
						INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "flush_count",
						INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "truncate_count",
						INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "storage_read_count",
						INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "storage_write_count",
						INT8OID, -1, 0);

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	for (i = 0; i < n_polar_slru_stats; i++)
	{
		const polar_slru_stat *stat = polar_slru_stats[i];
		Datum		values[SLRU_STAT_COLS];
		bool		nulls[SLRU_STAT_COLS];
		int j = 0;

		MemSet(nulls, 0, sizeof(bool) * SLRU_STAT_COLS);

		values[j++] = CStringGetTextDatum(stat->name);
		values[j++] = UInt32GetDatum(stat->n_slots);
		values[j++] = UInt32GetDatum(stat->n_page_status_stat[SLRU_PAGE_VALID]);
		values[j++] = UInt32GetDatum(stat->n_page_status_stat[SLRU_PAGE_EMPTY]);
		values[j++] = UInt32GetDatum(stat->n_page_status_stat[SLRU_PAGE_READ_IN_PROGRESS]);
		values[j++] = UInt32GetDatum(stat->n_page_status_stat[SLRU_PAGE_WRITE_IN_PROGRESS]);
		values[j++] = UInt32GetDatum(stat->n_wait_reading_count);
		values[j++] = UInt32GetDatum(stat->n_wait_writing_count);
		values[j++] = UInt64GetDatum(stat->n_slru_read_count);
		values[j++] = UInt64GetDatum(stat->n_slru_read_only_count);
		values[j++] = UInt64GetDatum(stat->n_slru_read_upgrade_count);
		values[j++] = UInt64GetDatum(stat->n_victim_count);
		values[j++] = UInt64GetDatum(stat->n_victim_write_count);
		values[j++] = UInt64GetDatum(stat->n_slru_write_count);
		values[j++] = UInt64GetDatum(stat->n_slru_zero_count);
		values[j++] = UInt64GetDatum(stat->n_slru_flush_count);
		values[j++] = UInt64GetDatum(stat->n_slru_truncate_count);
		values[j++] = UInt64GetDatum(stat->n_storage_read_count);
		values[j++] = UInt64GetDatum(stat->n_storage_write_count);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}
	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

/*
 * POLAR: return the IO stat info ever backend and auxiliary  process
 */
PG_FUNCTION_INFO_V1(polar_stat_process);

/*
 * POLAR: return the IO stat info ever flie type
 */
PG_FUNCTION_INFO_V1(polar_stat_io_info);

PG_FUNCTION_INFO_V1(polar_io_latency_info);

PG_FUNCTION_INFO_V1(polar_io_read_delta_info);

extern polar_wal_pipeline_stats_t* polar_wal_pipeline_get_stats();
extern polar_wait_object_t* polar_wal_pipeline_get_worker_wait_obj(int thread_no);
extern void polar_wal_pipeline_stats_reset(void);

PG_FUNCTION_INFO_V1(polar_wal_pipeline_info);
Datum
polar_wal_pipeline_info(PG_FUNCTION_ARGS)
{
	TupleDesc   tupdesc;
	Datum       values[10];
	bool        nulls[10];
	HeapTuple	tuple;
	Datum		result;
	int         i = 0;
	int 		j;

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	
	memset(nulls, 0, sizeof(nulls));

	values[i++] = UInt64GetDatum(polar_wal_pipeline_get_current_insert_lsn());
	values[i++] = UInt64GetDatum(polar_wal_pipeline_get_continuous_insert_lsn());
	values[i++] = UInt64GetDatum(polar_wal_pipeline_get_write_lsn());
	values[i++] = UInt64GetDatum(polar_wal_pipeline_get_flush_lsn());
	values[i++] = UInt64GetDatum(polar_wal_pipeline_get_unflushed_xlog_add_slot_no());
	values[i++] = UInt64GetDatum(polar_wal_pipeline_get_unflushed_xlog_del_slot_no());
	for (j = 0; j < POLAR_WAL_PIPELINE_NOTIFY_WORKER_NUM_MAX; j++)
		values[i++] = UInt64GetDatum(polar_wal_pipeline_get_last_notify_lsn(j));
	
	tuple = heap_form_tuple(BlessTupleDesc(tupdesc), values, nulls);
	result = HeapTupleGetDatum(tuple);

	PG_RETURN_DATUM(result);
}

PG_FUNCTION_INFO_V1(polar_wal_pipeline_stats);
Datum
polar_wal_pipeline_stats(PG_FUNCTION_ARGS)
{
	TupleDesc   tupdesc;
	Datum       values[31];
	bool        nulls[31];
	HeapTuple	tuple;
	Datum		result;
	int         i = 0;
	int			j;
	polar_wal_pipeline_stats_t *stats = polar_wal_pipeline_get_stats();

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	
	memset(nulls, 0, sizeof(nulls));

	for (j = 0; j < POLAR_WAL_PIPELINE_MAX_THREAD_NUM; j++)
	{
		if (POLAR_WAL_PIPELINER_ENABLE())
		{
			polar_wait_object_t *wait_obj = polar_wal_pipeline_get_worker_wait_obj(j);

			values[i++] = UInt64GetDatum(pg_atomic_read_u64(&wait_obj->stats.timeout_waits));
			values[i++] = UInt64GetDatum(pg_atomic_read_u64(&wait_obj->stats.wakeup_waits));
		}
		else
		{
			values[i++] = UInt64GetDatum(0);
			values[i++] = UInt64GetDatum(0);
		}
	}

	values[i++] = UInt64GetDatum(pg_atomic_read_u64(&stats->total_user_group_commits));
	values[i++] = UInt64GetDatum(pg_atomic_read_u64(&stats->total_user_spin_commits));
	values[i++] = UInt64GetDatum(pg_atomic_read_u64(&stats->total_user_timeout_commits));
	values[i++] = UInt64GetDatum(pg_atomic_read_u64(&stats->total_user_wakeup_commits));
	values[i++] = UInt64GetDatum(pg_atomic_read_u64(&stats->total_user_miss_timeouts));
	values[i++] = UInt64GetDatum(pg_atomic_read_u64(&stats->total_user_miss_wakeups));
	values[i++] = UInt64GetDatum(pg_atomic_read_u64(&stats->total_advance_callups));
	values[i++] = UInt64GetDatum(pg_atomic_read_u64(&stats->total_advances));
	values[i++] = UInt64GetDatum(pg_atomic_read_u64(&stats->total_write_callups));
	values[i++] = UInt64GetDatum(pg_atomic_read_u64(&stats->total_writes));
	values[i++] = UInt64GetDatum(pg_atomic_read_u64(&stats->unflushed_xlog_slot_waits));
	values[i++] = UInt64GetDatum(pg_atomic_read_u64(&stats->total_flush_callups));
	values[i++] = UInt64GetDatum(pg_atomic_read_u64(&stats->total_flushes));
	values[i++] = UInt64GetDatum(pg_atomic_read_u64(&stats->total_flush_merges));
	values[i++] = UInt64GetDatum(pg_atomic_read_u64(&stats->total_notify_callups));
	values[i++] = UInt64GetDatum(pg_atomic_read_u64(&stats->total_notifies));
	values[i++] = UInt64GetDatum(pg_atomic_read_u64(&stats->total_notified_users));

	tuple = heap_form_tuple(BlessTupleDesc(tupdesc), values, nulls);
	result = HeapTupleGetDatum(tuple);

	PG_RETURN_DATUM(result);
}

PG_FUNCTION_INFO_V1(polar_wal_pipeline_reset_stats);

Datum
polar_wal_pipeline_reset_stats(PG_FUNCTION_ARGS)
{
	polar_wal_pipeline_stats_reset();

	PG_RETURN_DATUM(true);
}

/* POLAR: monitor smgr shared pool */
PG_FUNCTION_INFO_V1(polar_stat_smgr_shared_pool);
Datum
polar_stat_smgr_shared_pool(PG_FUNCTION_ARGS)
{
#define SMGR_SHARED_COLS 6
	int i = 0;
	int cols = 1;
	int total_number = 0;
	int locked_num = 0;
	int valid_num = 0;
	int dirty_num = 0;
	int syncing_num = 0;
	int just_dirtied_num = 0;
	uint32 flags;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	SMgrSharedRelation *sr;
	SMgrSharedRelationPool *sr_pool_copy = polar_get_smgr_shared_pool();

	Datum		values[SMGR_SHARED_COLS];
	bool		nulls[SMGR_SHARED_COLS];

	if (sr_pool_copy == NULL)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("smgr shared memory wasn't initialized yet")));

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
	tupdesc = CreateTemplateTupleDesc(SMGR_SHARED_COLS, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "total_num",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "locked_num",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "valid_num",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "dirty_num",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "syncing_num",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "just_dirtied_num",
						INT4OID, -1, 0);

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	total_number = smgr_shared_relations;
	for (i = 0; i < smgr_shared_relations; i++)
	{
		sr = &sr_pool_copy->objects[i];
		flags = pg_atomic_read_u32(&sr->flags);
		if (flags & SR_LOCKED)
			locked_num++;
		if (flags & SR_VALID)
			valid_num++;
		if (flags & SR_DIRTY_MASK)
			dirty_num++;
		if (flags & SR_SYNCING_MASK)
			syncing_num++;
		if (flags & SR_JUST_DIRTIED_MASK)
			just_dirtied_num++;
	}

	MemSet(values, 0, sizeof(Datum) * SMGR_SHARED_COLS);
	MemSet(nulls, 0, sizeof(bool) * SMGR_SHARED_COLS);
	i = 0;
	values[i++] = UInt32GetDatum(total_number);
	values[i++] = UInt32GetDatum(locked_num);
	values[i++] = UInt32GetDatum(valid_num);
	values[i++] = UInt32GetDatum(dirty_num);
	values[i++] = UInt32GetDatum(syncing_num);
	values[i++] = UInt32GetDatum(just_dirtied_num);

	tuplestore_putvalues(tupstore, tupdesc, values, nulls);

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

PG_FUNCTION_INFO_V1(polar_stat_get_smgr_shared_mem);
Datum
polar_stat_get_smgr_shared_mem(PG_FUNCTION_ARGS)
{
#define SMGR_GET_SHARED_COLS 11
	int i = 0;
	int j;
	int cols = 1;
	bool in_cache = false;
	uint32 hash;
	uint32 flags;
	LWLock *mapping_lock;
	bool get_mem_quiet = PG_GETARG_BOOL(0);
	RelFileNodeBackend rnode;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	SMgrSharedRelation *sr;
	SMgrSharedRelationPool *sr_pool_copy = polar_get_smgr_shared_pool();
	HTAB *sr_mapping_copy = polar_get_smgr_mapping_table();

	Datum		values[SMGR_GET_SHARED_COLS];
	bool		nulls[SMGR_GET_SHARED_COLS];

	if (sr_pool_copy == NULL)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("smgr shared memory wasn't initialized yet")));

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
	tupdesc = CreateTemplateTupleDesc(SMGR_GET_SHARED_COLS, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "spcNode",
						OIDOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "dbNode",
						OIDOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "relNode",
						OIDOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "backend",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "in_cache",
						BOOLOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "main_cache",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "fsm_cache",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "vm_cache",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "flags",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "generation",
						INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "usecount",
						INT8OID, -1, 0);

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	MemSet(values, 0, sizeof(Datum) * SMGR_GET_SHARED_COLS);
	for (i = 0; i < smgr_shared_relations; i++)
	{
		sr = &sr_pool_copy->objects[i];

		if (!get_mem_quiet)
			flags = smgr_lock_sr(sr);
		rnode = sr->rnode;

		if (!get_mem_quiet)
			smgr_unlock_sr(sr, flags);

		MemSet(nulls, 0, sizeof(bool) * SMGR_GET_SHARED_COLS);
		j = 0;

		hash = get_hash_value(sr_mapping_copy, &rnode);

		if (!get_mem_quiet)
		{
			mapping_lock = SR_PARTITION_LOCK(hash);
			LWLockAcquire(mapping_lock, LW_SHARED);
		}

		hash_search_with_hash_value(sr_mapping_copy,
										&rnode,
										hash,
										HASH_FIND,
										&in_cache);

		if (!get_mem_quiet)
			LWLockRelease(mapping_lock);


		values[j++] = rnode.node.spcNode;
		values[j++] = rnode.node.dbNode;
		values[j++] = rnode.node.relNode;
		values[j++] = rnode.backend;
		values[j++] = in_cache;
		values[j++] = sr->nblocks[MAIN_FORKNUM];
		values[j++] = sr->nblocks[FSM_FORKNUM];
		values[j++] = sr->nblocks[VISIBILITYMAP_FORKNUM];
		values[j++] = pg_atomic_read_u32(&sr->flags);
		values[j++] = pg_atomic_read_u64(&sr->generation);
		values[j++] = sr->usecount;

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}
	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

PG_FUNCTION_INFO_V1(polar_get_current_db_smgr_shared_mem);
Datum
polar_get_current_db_smgr_shared_mem(PG_FUNCTION_ARGS)
{
#define SMGR_GET_CURRENT_DB_COLS 7
	int i = 0;
	int j;
	int cols = 1;
	uint32 flags;
	Oid reloid;
	int main_real_blocks = 0;
	int fsm_real_blocks = 0;
	int vm_real_blocks = 0;
	int	polar_nblocks_cache_mode_save;
	bool get_mem_quiet = PG_GETARG_BOOL(0);
	Relation rel = NULL;
	RelFileNodeBackend rnode;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	SMgrSharedRelation *sr;
	SMgrSharedRelationPool *sr_pool_copy = polar_get_smgr_shared_pool();

	Datum		values[SMGR_GET_CURRENT_DB_COLS];
	bool		nulls[SMGR_GET_CURRENT_DB_COLS];

	if (sr_pool_copy == NULL)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("smgr shared memory wasn't initialized yet")));

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
	tupdesc = CreateTemplateTupleDesc(SMGR_GET_CURRENT_DB_COLS, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "spcNode",
						OIDOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "dbNode",
						OIDOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "relNode",
						OIDOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "backend",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "main_rea",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "fsm_real",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "vm_real",
						INT4OID, -1, 0);

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	MemSet(values, 0, sizeof(Datum) * SMGR_GET_CURRENT_DB_COLS);
	for (i = 0; i < smgr_shared_relations; i++)
	{
		sr = &sr_pool_copy->objects[i];

		if (!get_mem_quiet)
			flags = smgr_lock_sr(sr);
		rnode = sr->rnode;

		if (!get_mem_quiet)
			smgr_unlock_sr(sr, flags);

		MemSet(nulls, 0, sizeof(bool) * SMGR_GET_CURRENT_DB_COLS);
		j = 0;

		if (rnode.node.dbNode != MyDatabaseId)
			continue;

		if (rnode.node.spcNode != 0 && rnode.node.relNode != 0)
		{
			reloid = RelidByRelfilenode(rnode.node.spcNode, rnode.node.relNode);
			if (OidIsValid(reloid))
				rel = try_relation_open(reloid, AccessShareLock);

			if (rel->rd_refcnt <= 0)
				continue;

			if (rel)
			{
				polar_nblocks_cache_mode_save = polar_nblocks_cache_mode;
				polar_nblocks_cache_mode = POLAR_NBLOCKS_CACHE_OFF_MODE;

				RelationOpenSmgr(rel);

				if (smgrexists(rel->rd_smgr, MAIN_FORKNUM))
					main_real_blocks = RelationGetNumberOfBlocksInFork(rel, MAIN_FORKNUM);
				else
					main_real_blocks = InvalidBlockNumber;

				if (smgrexists(rel->rd_smgr, FSM_FORKNUM))
					fsm_real_blocks = RelationGetNumberOfBlocksInFork(rel, FSM_FORKNUM);
				else
					fsm_real_blocks = InvalidBlockNumber;

				if (smgrexists(rel->rd_smgr, VISIBILITYMAP_FORKNUM))
					vm_real_blocks = RelationGetNumberOfBlocksInFork(rel, VISIBILITYMAP_FORKNUM);
				else
					vm_real_blocks = InvalidBlockNumber;

				polar_nblocks_cache_mode = polar_nblocks_cache_mode_save;

				relation_close(rel, AccessShareLock);
			}
		}
		else
		{
			main_real_blocks = InvalidBlockNumber;
			fsm_real_blocks = InvalidBlockNumber;
			vm_real_blocks = InvalidBlockNumber;
		}


		values[j++] = rnode.node.spcNode;
		values[j++] = rnode.node.dbNode;
		values[j++] = rnode.node.relNode;
		values[j++] = rnode.backend;
		values[j++] = main_real_blocks;
		values[j++] = fsm_real_blocks;
		values[j++] = vm_real_blocks;

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}
	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

PG_FUNCTION_INFO_V1(polar_stat_get_smgr_shared_mem_ng);
Datum
polar_stat_get_smgr_shared_mem_ng(PG_FUNCTION_ARGS)
{
#define SMGR_STATS_NG 6
	int i = 0;
	int j;
	int cols = 1;
	uint32 flags;
	bool is_locked, is_valid, is_dirty, is_syncing, is_just_dirtied;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	SMgrSharedRelation *sr;
	SMgrSharedRelationPool *sr_pool_copy = polar_get_smgr_shared_pool();

	Datum		values[SMGR_STATS_NG];
	bool		nulls[SMGR_STATS_NG];

	if (sr_pool_copy == NULL)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("smgr shared memory wasn't initialized yet")));

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
	tupdesc = CreateTemplateTupleDesc(SMGR_STATS_NG, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "sr_offset",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "is_locked",
						BOOLOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "is_valid",
						BOOLOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "is_dirty",
						BOOLOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "is_syncing",
						BOOLOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "is_just_dirtied",
						BOOLOID, -1, 0);

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	for (i = 0; i < smgr_shared_relations; i++)
	{
		is_locked = false;
		is_valid = false;
		is_dirty = false;
		is_syncing = false;
		is_just_dirtied = false;

		sr = &sr_pool_copy->objects[i];
		flags = pg_atomic_read_u32(&sr->flags);
		if (flags & SR_LOCKED)
			is_locked = true;
		if (flags & SR_VALID)
			is_valid = true;
		if (flags & SR_DIRTY_MASK)
			is_dirty = true;
		if (flags & SR_SYNCING_MASK)
			is_syncing = true;
		if (flags & SR_JUST_DIRTIED_MASK)
			is_just_dirtied = true;

		MemSet(values, 0, sizeof(Datum) * SMGR_STATS_NG);
		MemSet(nulls, 0, sizeof(bool) * SMGR_STATS_NG);

		j = 0;
		values[j++] = i + 1;
		values[j++] = is_locked;
		values[j++] = is_valid;
		values[j++] = is_dirty;
		values[j++] = is_syncing;
		values[j++] = is_just_dirtied;

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}
	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

PG_FUNCTION_INFO_V1(polar_release_target_smgr_shared_mem);
Datum
polar_release_target_smgr_shared_mem(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	SMgrSharedRelationPool *sr_pool_copy = polar_get_smgr_shared_pool();
	SMgrSharedRelation *sr = NULL;
	int target_cache_index = PG_GETARG_INT32(0);

	if (sr_pool_copy == NULL)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("smgr shared memory wasn't initialized yet")));

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

	if (target_cache_index > smgr_shared_relations)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("targe cache index over smgr_shared_relations")));

	/* clean up the target cache flags */
	sr = &sr_pool_copy->objects[target_cache_index - 1];
	if (sr)
		smgr_unlock_sr(sr, 0);

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(polar_cluster_info);
Datum
polar_cluster_info(PG_FUNCTION_ARGS)
{
#define CLUSTER_INFO_COUNT 18
	int i, count, cols = 1;
	ReturnSetInfo	   *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc			tupdesc;
	Tuplestorestate    *tupstore;
	MemoryContext		per_query_ctx;
	MemoryContext		oldcontext;
	polar_cluster_info_item *items = polar_cluster_info_ctl->items;
	polar_cluster_info_item *item;

	/* Build a tuple descriptor for our result type */
	tupdesc = CreateTemplateTupleDesc(CLUSTER_INFO_COUNT, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "name",
						TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "host",
						TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "port",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "release_date",
						TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "version",
						TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "slot_name",
						TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "type",
						TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "state",
						TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "cpu",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "cpu_quota",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "memory",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "memory_quota",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "iops",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "iops_quota",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "connection",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "connection_quota",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "px_connection",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "px_connection_quota",
						INT4OID, -1, 0);

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);
	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;
	MemoryContextSwitchTo(oldcontext);

	LWLockAcquire(&polar_cluster_info_ctl->lock, LW_EXCLUSIVE);
	count = polar_cluster_info_ctl->count;
	for (i = 0; i < count; ++i)
	{
		Datum		values[CLUSTER_INFO_COUNT];
		bool		nulls[CLUSTER_INFO_COUNT];
		MemSet(nulls, 0, sizeof(bool) * CLUSTER_INFO_COUNT);

		item = &items[i];
		values[0] = CStringGetTextDatum(item->name);
		values[1] = CStringGetTextDatum(item->host);
		values[2] = Int32GetDatum(item->port);
		values[3] = CStringGetTextDatum(item->release_date);
		values[4] = CStringGetTextDatum(item->version);
		values[5] = CStringGetTextDatum(item->slot_name);
		values[6] = CStringGetTextDatum(polar_node_type_string(item->type, DEBUG5));
		values[7] = CStringGetTextDatum(polar_standby_state_string(item->state, DEBUG5));
		values[8] = Int32GetDatum(item->cpu);
		values[9] = Int32GetDatum(item->cpu_quota);
		values[10] = Int32GetDatum(item->memory);
		values[11] = Int32GetDatum(item->memory_quota);
		values[12] = Int32GetDatum(item->iops);
		values[13] = Int32GetDatum(item->iops_quota);
		values[14] = Int32GetDatum(item->connection);
		values[15] = Int32GetDatum(item->connection_quota);
		values[16] = Int32GetDatum(item->px_connection);
		values[17] = Int32GetDatum(item->px_connection_quota);
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}
	LWLockRelease(&polar_cluster_info_ctl->lock);

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

PG_FUNCTION_INFO_V1(polar_set_available);
Datum
polar_set_available(PG_FUNCTION_ARGS)
{
	bool available = PG_GETARG_BOOL(0);
	polar_set_available_state(available);
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(polar_is_available);
Datum
polar_is_available(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(polar_get_available_state());
}
