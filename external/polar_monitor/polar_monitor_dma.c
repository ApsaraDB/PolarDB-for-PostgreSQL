/*-------------------------------------------------------------------------
 *
 * polar_monitor_consensus.c
 *	  display some information of polardb consensus
 *
 * Portions Copyright (c) 2021, Alibaba Group Holding Limited
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  contrib/polar_monitor/polar_monitor_consensus.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "polar_dma/polar_dma.h"
#include "polar_dma/consensus_slru.h"
#include "polar_dma/consensus_log.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/pg_lsn.h"

PG_FUNCTION_INFO_V1(polar_dma_get_member_status);

Datum
polar_dma_get_member_status(PG_FUNCTION_ARGS)
{
	bool success;
	TupleDesc	tupdesc;
	Datum	   *values;
	bool	   *nulls;
	ConsensusMemberInfo member_info;

	if (!POLAR_ENABLE_DMA())
		PG_RETURN_NULL();

	success = ConsensusWaitForStatCollect(POLAR_DMA_STAT_MEMBER_INFO);
	if (!success)
	{
		ereport(WARNING, (errmsg("consensus get stat fail")));
		PG_RETURN_NULL();
	}
	ConsensusGetMemberInfo(&member_info);

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "consensus get stat return type must be a row type");

	values = palloc0(sizeof(Datum) * tupdesc->natts);
	nulls = palloc0(sizeof(bool) * tupdesc->natts);

	values[0] = UInt64GetDatum(member_info.serverId);
	values[1] = UInt64GetDatum(member_info.currentTerm);
	values[2] = UInt64GetDatum(member_info.currentLeader);
	values[3] = UInt64GetDatum(member_info.commitIndex);
	values[4] = UInt64GetDatum(member_info.lastLogTerm);
	values[5] = UInt64GetDatum(member_info.lastLogIndex);
	values[6] = Int32GetDatum(member_info.role);
	values[7] = UInt64GetDatum(member_info.votedFor);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

PG_FUNCTION_INFO_V1(polar_dma_get_cluster_status);

Datum
polar_dma_get_cluster_status(PG_FUNCTION_ARGS)
{
#define PG_GET_CLUSTER_INFO_COLS 10
	bool	success;
	ReturnSetInfo *clusters_info = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	int			cno;
	ConsensusClusterInfo	clusterInfo[POLAR_DMA_MAX_NODES_NUM];
	int			numCluster;

	if (!POLAR_ENABLE_DMA())
		PG_RETURN_NULL();

	success = ConsensusWaitForStatCollect(POLAR_DMA_STAT_CLUSTER_INFO);
	if (!success)
	{
		ereport(WARNING, (errmsg("consensus get stat fail")));
		PG_RETURN_NULL();
	}
	numCluster = ConsensusGetClusterInfo(clusterInfo);
	
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "consensus get stat return type must be a row type");

	per_query_ctx = clusters_info->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	clusters_info->returnMode = SFRM_Materialize;
	clusters_info->setResult = tupstore;
	clusters_info->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	for (cno = 0; cno < numCluster && cno < POLAR_DMA_MAX_NODES_NUM; cno++)
	{
		Datum		values[PG_GET_CLUSTER_INFO_COLS];
		bool		nulls[PG_GET_CLUSTER_INFO_COLS];
		int		 i;
		ConsensusClusterInfo cinfo = clusterInfo[cno];

		memset(nulls, 0, sizeof(nulls));

		i = 0;
		values[i++] = UInt64GetDatum(cinfo.serverId);
		values[i++] = CStringGetTextDatum(cinfo.ipPort);
		values[i++] = UInt64GetDatum(cinfo.matchIndex);
		values[i++] = UInt64GetDatum(cinfo.nextIndex);
		values[i++]	= Int32GetDatum(cinfo.role);
		values[i++] = BoolGetDatum(cinfo.hasVoted != 0 ? true : false);
		values[i++]	= BoolGetDatum(cinfo.forceSync);
		values[i++] = UInt32GetDatum(cinfo.electionWeight);
		values[i++]	= UInt64GetDatum(cinfo.learnerSource);
		values[i++] = BoolGetDatum(cinfo.pipelining);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

PG_FUNCTION_INFO_V1(polar_dma_get_msg_stats);

Datum
polar_dma_get_msg_stats(PG_FUNCTION_ARGS)
{
	bool success;
	TupleDesc	tupdesc;
	Datum	   *values;
	bool	   *nulls;
	ConsensusStatsInfo stats_info;
	int			i;

	if (!POLAR_ENABLE_DMA())
		PG_RETURN_NULL();

	success = ConsensusWaitForStatCollect(POLAR_DMA_STATS_INFO);
	if (!success)
	{
		ereport(WARNING, (errmsg("consensus get stat fail")));
		PG_RETURN_NULL();
	}
	ConsensusGetStatsInfo(&stats_info);

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "consensus get stat return type must be a row type");

	values = palloc0(sizeof(Datum) * tupdesc->natts);
	nulls = palloc0(sizeof(bool) * tupdesc->natts);

	i = 0;
	values[i++] = UInt64GetDatum(stats_info.serverId);
	values[i++] = UInt64GetDatum(stats_info.countMsgAppendLog);
	values[i++] = UInt64GetDatum(stats_info.countMsgRequestVote);
	values[i++] = UInt64GetDatum(stats_info.countHeartbeat);
	values[i++] = UInt64GetDatum(stats_info.countOnMsgAppendLog);
	values[i++] = UInt64GetDatum(stats_info.countOnMsgRequestVote);
	values[i++] = UInt64GetDatum(stats_info.countOnHeartbeat);
	values[i++] = UInt64GetDatum(stats_info.countReplicateLog);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

PG_FUNCTION_INFO_V1(polar_dma_get_consensus_status);

Datum
polar_dma_get_consensus_status(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum	   *values;
	bool	   *nulls;
	ConsensusStatus status_info;
	int			i;

	if (!POLAR_ENABLE_DMA())
		PG_RETURN_NULL();

	ConsensusGetStatus(&status_info);

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "consensus get stat return type must be a row type");

	values = palloc0(sizeof(Datum) * tupdesc->natts);
	nulls = palloc0(sizeof(bool) * tupdesc->natts);

	i = 0;
	values[i++] = UInt32GetDatum(status_info.current_state);
	values[i++] = UInt64GetDatum(status_info.term);
	values[i++] = UInt64GetDatum(status_info.log_term);
	values[i++] = UInt64GetDatum(status_info.xlog_term);
	values[i++] = UInt64GetDatum(status_info.leader_id);
	values[i++] = LSNGetDatum(status_info.sync_rqst_lsn);
	values[i++] = UInt32GetDatum(status_info.synced_tli);
	values[i++] = LSNGetDatum(status_info.synced_lsn);
	values[i++] = LSNGetDatum(status_info.purge_lsn);
	values[i++] = UInt64GetDatum(status_info.commit_index);
	values[i++] = LSNGetDatum(status_info.flushed_lsn);
	values[i++] = UInt32GetDatum(status_info.flushed_tli);
	values[i++] = LSNGetDatum(status_info.appended_lsn);
	values[i++] = UInt32GetDatum(status_info.commit_queue_length);
	values[i++] = UInt32GetDatum(status_info.stats_queue_length);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

PG_FUNCTION_INFO_V1(polar_dma_get_log_status);

Datum
polar_dma_get_log_status(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum	   *values;
	bool	   *nulls;
	ConsensusLogStatus status_info;
	int			i;

	if (!POLAR_ENABLE_DMA())
		PG_RETURN_NULL();

	ConsensusLogGetStatus(&status_info);

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "consensus get stat return type must be a row type");

	values = palloc0(sizeof(Datum) * tupdesc->natts);
	nulls = palloc0(sizeof(bool) * tupdesc->natts);

	i = 0;
	values[i++] = UInt64GetDatum(status_info.current_term);
	values[i++] = UInt64GetDatum(status_info.current_index);
	values[i++] = UInt64GetDatum(status_info.sync_index);
	values[i++] = LSNGetDatum(status_info.last_write_lsn);
	values[i++] = UInt64GetDatum(status_info.last_write_timeline);
	values[i++] = UInt64GetDatum(status_info.last_append_term);
	values[i++] = UInt64GetDatum(status_info.next_append_term);
	values[i++] = UInt64GetDatum(status_info.variable_length_log_next_offset);
	values[i++] = BoolGetDatum(status_info.disable_purge);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

PG_FUNCTION_INFO_V1(polar_dma_get_meta_status);

Datum
polar_dma_get_meta_status(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum	   *values;
	bool	   *nulls;
	ConsensusMetaStatus status_info;
	int	member_info_size, learner_info_size;
	char *member_info_ptr, *learner_info_ptr;
	char member_info_str[MEMBER_INFO_MAX_LENGTH], 
			 learner_info_str[MEMBER_INFO_MAX_LENGTH];
	int			i;

	if (!POLAR_ENABLE_DMA())
		PG_RETURN_NULL();

	ConsensusMetaGetStatus(&status_info);

	member_info_size = ConsensusMetaGetMemberInfoFromArray(&member_info_ptr);
	if (member_info_size > 0)
	{
		member_info_size  = member_info_size > MEMBER_INFO_MAX_LENGTH ? 
															MEMBER_INFO_MAX_LENGTH : member_info_size;
		strncpy(member_info_str, member_info_ptr, member_info_size-1);
		free(member_info_ptr);
		member_info_str[member_info_size-1] = '\0';
	}

	learner_info_size = ConsensusMetaGetLearnerInfoFromArray(&learner_info_ptr);
	if (learner_info_size > 0)
	{
		learner_info_size = learner_info_size > MEMBER_INFO_MAX_LENGTH ? 
															MEMBER_INFO_MAX_LENGTH : learner_info_size;
		strncpy(learner_info_str, learner_info_ptr, learner_info_size-1);
		free(learner_info_ptr);
		learner_info_str[learner_info_size-1] = '\0';
	}

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "consensus get stat return type must be a row type");

	values = palloc0(sizeof(Datum) * tupdesc->natts);
	nulls = palloc0(sizeof(bool) * tupdesc->natts);

	i = 0;
	values[i++] = UInt64GetDatum(status_info.current_term);
	values[i++] = UInt64GetDatum(status_info.vote_for);
	values[i++] = UInt64GetDatum(status_info.last_leader_term);
	values[i++] = UInt64GetDatum(status_info.last_leader_log_index);
	values[i++] = UInt64GetDatum(status_info.scan_index);
	values[i++] = UInt64GetDatum(status_info.cluster_id);
	values[i++] = UInt64GetDatum(status_info.commit_index);
	values[i++] = UInt64GetDatum(status_info.purge_index);

	if (member_info_size > 0)
		values[i++] = CStringGetTextDatum(member_info_str);
	else
		nulls[i++] = true;

	if (learner_info_size > 0)
		values[i++] = CStringGetTextDatum(learner_info_str);
	else
		nulls[i++] = true;

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

PG_FUNCTION_INFO_V1(polar_dma_get_consensus_stats);
	
Datum
polar_dma_get_consensus_stats(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum	   *values;
	bool	   *nulls;
	ConsensusStats stats_info;
	int			i;

	if (!POLAR_ENABLE_DMA())
		PG_RETURN_NULL();

	ConsensusGetStats(&stats_info);

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "consensus get stat return type must be a row type");

	values = palloc0(sizeof(Datum) * tupdesc->natts);
	nulls = palloc0(sizeof(bool) * tupdesc->natts);

	i = 0;
	values[i++] = UInt64GetDatum(stats_info.transit_waits);
	values[i++] = UInt64GetDatum(stats_info.xlog_transit_waits);
	values[i++] = UInt64GetDatum(stats_info.xlog_flush_waits);
	values[i++] = UInt64GetDatum(stats_info.consensus_appends);
	values[i++] = UInt64GetDatum(stats_info.consensus_wakeups);
	values[i++] = UInt64GetDatum(stats_info.consensus_backend_wakeups);
	values[i++] = UInt64GetDatum(stats_info.consensus_commit_waits);
	values[i++] = UInt64GetDatum(stats_info.consensus_commit_wait_time);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

PG_FUNCTION_INFO_V1(polar_dma_get_log_stats);

Datum
polar_dma_get_log_stats(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum	   *values;
	bool	   *nulls;
	ConsensusLogStats stats_info;
	int			i;

	if (!POLAR_ENABLE_DMA())
		PG_RETURN_NULL();

	ConsensusLogGetStats(&stats_info);

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "consensus get stat return type must be a row type");

	values = palloc0(sizeof(Datum) * tupdesc->natts);
	nulls = palloc0(sizeof(bool) * tupdesc->natts);

	i = 0;
	values[i++] = UInt64GetDatum(stats_info.log_reads);
	values[i++] = UInt64GetDatum(stats_info.variable_log_reads);
	values[i++] = UInt64GetDatum(stats_info.log_appends);
	values[i++] = UInt64GetDatum(stats_info.variable_log_appends);
	values[i++] = UInt64GetDatum(stats_info.log_flushes);
	values[i++] = UInt64GetDatum(stats_info.meta_flushes);
	values[i++] = UInt64GetDatum(stats_info.xlog_flush_tries);
	values[i++] = UInt64GetDatum(stats_info.xlog_flush_failed_tries);
	values[i++] = UInt64GetDatum(stats_info.xlog_transit_waits);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

PG_FUNCTION_INFO_V1(polar_dma_get_slru_stats);

Datum
polar_dma_get_slru_stats(PG_FUNCTION_ARGS)
{
#define SLRU_STAT_COLS 20
	int i = 0;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;

	if (!POLAR_ENABLE_DMA())
		PG_RETURN_NULL();

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "consensus get stat return type must be a row type");

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	for (i = 0; i < n_consensus_slru_stats; i++)
	{
		const consensus_slru_stat *stat = consensus_slru_stats[i];
		Datum		values[SLRU_STAT_COLS];
		bool		nulls[SLRU_STAT_COLS];
		int j = 0;
		
		if (stat == NULL)
			continue;

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
		values[j++] = UInt64GetDatum(stat->n_slru_truncate_forward_count);
		values[j++] = UInt64GetDatum(stat->n_slru_truncate_backward_count);
		values[j++] = UInt64GetDatum(stat->n_storage_read_count);
		values[j++] = UInt64GetDatum(stat->n_storage_write_count);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}
	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

PG_FUNCTION_INFO_V1(polar_dma_wal_committed_lsn);

/*
 * Report the current WAL consensus commit location 
 */
Datum
polar_dma_wal_committed_lsn(PG_FUNCTION_ARGS)
{
	XLogRecPtr	current_recptr;

	current_recptr = ConsensusGetSyncedLSN();

	PG_RETURN_LSN(current_recptr);
}
