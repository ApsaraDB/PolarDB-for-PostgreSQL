/*-------------------------------------------------------------------------
 *
 * polar_flashback_table.c
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, Alibaba Group Holding limited
 *
 * IDENTIFICATION
 *    src/backend/polar_flashback/polar_flashback_table.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/parallel.h"
#include "access/polar_log.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "commands/cluster.h"
#include "commands/tablecmds.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "polar_flashback/polar_fast_recovery_area.h"
#include "polar_flashback/polar_flashback_log.h"
#include "polar_flashback/polar_flashback_log_worker.h"
#include "polar_flashback/polar_flashback_rel_filenode.h"
#include "polar_flashback/polar_flashback_snapshot.h"
#include "polar_flashback/polar_flashback_table.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "tcop/tcopprot.h"

/*
 * DSM keys for parallel flashback.  Unlike other parallel execution code, since
 * we don't need to worry about DSM keys conflicting with plan_node_id we can
 * use small integers.
 */
#define FLASHBACK_KEY_STATE 1
#define FLASHBACK_KEY_SNAPSHOT 2
#define FLASHBACK_KEY_XIP 3
#define FLASHBACK_KEY_SUBXIP 4
#define FLASHBACK_KEY_BUFFER_USAGE 5
#define FLASHBACK_KEY_QUERY_TEXT 6

#define LOG_FLSHBAK_TBL_SHR_STATE(shared_state, target_block, elevel) \
	elog(elevel, "The flashback table %u to new table %u will replay xlog record from %X/%X to %X/%X, " \
		 "flashback log record from %X/%X to end, the current transaction id is %u, " \
		 "the max transaction in snapshot is %u, relation file node is (%u, %u, %u), " \
		 "total block number now is %u, the next block to flashback is %u, " \
		 "the next clog subdir number is %u", \
		 (shared_state)->old_relid, (shared_state)->new_relid, \
		 (uint32) ((shared_state)->wal_start_lsn >> 32), (uint32) ((shared_state)->wal_start_lsn), \
		 (uint32) ((shared_state)->wal_end_lsn >> 32), (uint32) ((shared_state)->wal_end_lsn), \
		 (uint32) ((shared_state)->flog_start_ptr >> 32), (uint32) ((shared_state)->flog_start_ptr), \
		 (shared_state)->curr_xid, (shared_state)->next_xid, (shared_state)->rel_filenode.spcNode, \
		 (shared_state)->rel_filenode.dbNode, (shared_state)->rel_filenode.relNode, \
		 (shared_state)->nblocks, target_block, (shared_state)->next_clog_subdir_no) \

int polar_workers_per_flashback_table;

static TimestampTz
get_flashback_target_time(Expr *flashback_time_expr)
{
	TimestampTz target_timestamptz;
	ExprState  *flashback_time_expr_state;
	EState     *estate = NULL;
	ExprContext *econtext;
	bool        is_null;
	Datum       target_time_dt = (Datum) 0;

	/* Get the flashback target_time. */
	estate = CreateExecutorState();
	flashback_time_expr_state = ExecPrepareExpr(flashback_time_expr, estate);
	econtext = GetPerTupleExprContext(estate);
	target_time_dt = ExecEvalExpr(flashback_time_expr_state, econtext, &is_null);
	target_timestamptz = DatumGetTimestampTz(target_time_dt);
	FreeExecutorState(estate);

	elog(DEBUG1, "The target timestamp of flashback table is %s",
		 timestamptz_to_str(target_timestamptz));
	return target_timestamptz;
}

static void
check_flashback_time(TimestampTz target_time)
{
	TimestampTz now = GetCurrentTimestamp();
	TimestampTz horizon;

	horizon = now -
			  (TimestampTz) polar_fast_recovery_area_rotation * SECS_PER_MINUTE * USECS_PER_SEC;

	if (timestamptz_cmp_internal(target_time, now) >= 0)
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						errmsg("The flashback table target time exceeds now!")));

	if (timestamptz_cmp_internal(target_time, horizon) < 0)
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						errmsg("The lag between now and flashback table target time exceeds the "
							   "polar_fast_recovery_area_rotation %d minutes", polar_fast_recovery_area_rotation)));
}

static inline void
log_flashback_table_state(flshbak_tbl_st_t state, int elevel)
{
	LOG_FLSHBAK_TBL_SHR_STATE(state->shared_state,
							  pg_atomic_read_u32(&(state->shared_state->next_blkno)), elevel);
	log_flashback_snapshot(state->snapshot, elevel);
}

void
polar_log_cannot_flashback_cause(Form_pg_class reltup, Oid relid, bool no_persistence_check)
{
	if (reltup->relkind != RELKIND_RELATION)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("can not support to flashback an irregular table \"%s\" now.", reltup->relname.data)));

	if (IsSystemClass(relid, reltup))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("can not support to flashback system catalog \"%s\".", reltup->relname.data)));

	if (reltup->relispartition)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("can not support to flashback an partition table \"%s\" now.", reltup->relname.data)));

	if (reltup->relhassubclass)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("can not support to flashback an parent table \"%s\" now.", reltup->relname.data)));

	/* Can not flashback temp or unlogged table */
	if (!no_persistence_check && reltup->relpersistence != RELPERSISTENCE_PERMANENT)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("can not support to flashback non-persistence table \"%s\".", reltup->relname.data)));

	/* Can not flashback toast table */
	if (OidIsValid(reltup->reltoastrelid))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("can not support to flashback table which has a toast table \"%s\".", reltup->relname.data)));
}

bool
polar_can_rel_flashback(Form_pg_class reltup, Oid relid, bool no_persistence_check)
{
	/*
	 * Now can't flashback:
	 * 1. index
	 * 2. toast table
	 * 3. materialized view
	 * 4. partitioned table
	 * 5. partition table
	 * 6. system table
	 * 7. foreign table
	 * 8. table has a toast table
	 * ...
	 */

	if (reltup->relkind == RELKIND_RELATION &&
			(no_persistence_check || reltup->relpersistence == RELPERSISTENCE_PERMANENT) &&
			!reltup->relispartition && !reltup->relhassubclass && !OidIsValid(reltup->reltoastrelid) &&
			!IsSystemClass(relid, reltup))
		return true;

	return false;
}

/*
 * Before acquiring a table lock, check whether we have sufficient rights
 * and all refrerence tables in the flashback objects.
 * NB: It doesn't support to flashback the partition table.
 */
static void
rangevar_callback_for_flashback(const RangeVar *relation, Oid relid, Oid old_relid, void *arg)
{
	char    relkind;
	HeapTuple   tp;
	Form_pg_class reltup;

	/* Nothing to do if the relation was not found. */
	if (!OidIsValid(relid))
		return;

	tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));

	if (!HeapTupleIsValid(tp))
		/*no cover line*/
		elog(ERROR, "cache lookup failed for relation %u", relid);

	reltup = (Form_pg_class) GETSTRUCT(tp);
	relkind = reltup->relkind;

	if (!polar_can_rel_flashback(reltup, relid, false))
		polar_log_cannot_flashback_cause(reltup, relid, false);

	ReleaseSysCache(tp);

	/* Check permissions */
	if (!pg_class_ownercheck(relid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, get_relkind_objtype(relkind), relation->relname);
}

/*
 * Parse the xlog record from start lsn to end_time
 * Return the end+1 xlog record need to apply.
 */
static XLogRecPtr
flashback_table_apply_wal(Snapshot snapshot, flshbak_tbl_shr_st_t shared_state, TimestampTz end_time,
						  const char *relname, XLogRecPtr *logindex_max_lsn)
{
	XLogRecord *record;
	XLogReaderState *xlogreader;
	char       *errormsg;
	XLogRecPtr  start_lsn = shared_state->wal_start_lsn;
	XLogRecPtr  end_lsn = start_lsn;
	XLogRecPtr  read_upto = GetFlushRecPtr();
	bool finish = false;
	flashback_rel_snapshot_t flshbak_rel_sn;

	/* To get a exactly snapshot, we read xlog from snapshot->lsn */
	start_lsn = Min(start_lsn, snapshot->lsn);

	if (start_lsn == read_upto)
		elog(ERROR, "There is no more wal record in disk while flashback table %s "
			 "from start lsn %X/%X, we think it is unchanged", relname,
			 (uint32)(start_lsn >> 32), (uint32) start_lsn);

	xlogreader = XLogReaderAllocate(wal_segment_size, &read_local_xlog_page, NULL);

	if (xlogreader == NULL)
		/*no cover line*/
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("Can not allocate the xlog reader memory for flashback table %s", relname)));

	/* Init the flashback relation snapshot */
	flshbak_rel_sn.snapshot = snapshot;
	flshbak_rel_sn.xip_size = 0;
	flshbak_rel_sn.removed_xid_pos = palloc0(REMOVED_XID_POS_SIZE);
	flshbak_rel_sn.removed_size = 0;
	flshbak_rel_sn.next_clog_subdir_no = shared_state->next_clog_subdir_no;
	flshbak_rel_sn.next_xid = shared_state->next_xid;

	/* Read the xlog record until the end time or xlog ptr read upto */
	do
	{
		record = XLogReadRecord(xlogreader, start_lsn, &errormsg);

		if (record != NULL)
		{
			/*
			 * We finish the apply while the xlog record xact_time newer than end_time.
			 *
			 * NB: The transaction time newer than end_time we don't apply. The end_lsn is
			 * always a xact commit or abort wal record end lsn or start_lsn while there is
			 * no xact commit or abort wal records.
			 */
			finish = polar_flashback_xact_redo(record, &flshbak_rel_sn, end_time, xlogreader);

			if (finish)
				break;

			/* We just care about wal relatived with data page */
			if (polar_xlog_remove_payload(record))
			{
				*logindex_max_lsn = xlogreader->currRecPtr;
				end_lsn = xlogreader->EndRecPtr;
			}
		}
		else
		{
			XLogRecPtr  errptr;

			/*no cover begin*/
			errptr = start_lsn ? start_lsn : xlogreader->EndRecPtr;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read xlog record at %X/%X in flashback table %s",
							(uint32)(errptr >> 32),
							(uint32) errptr, relname)));
			/*no cover end*/
		}

		start_lsn = InvalidXLogRecPtr; /* continue reading at next record */
	}
	while (xlogreader->EndRecPtr < read_upto);

	/* Compact the snapshot xip */
	polar_compact_xip(&flshbak_rel_sn);
	/* Update the shared_state next xid and next clog subdir no. */
	shared_state->next_xid = flshbak_rel_sn.next_xid;
	shared_state->next_clog_subdir_no = flshbak_rel_sn.next_clog_subdir_no;

	pfree(flshbak_rel_sn.removed_xid_pos);
	XLogReaderFree(xlogreader);
	elog(DEBUG1, "Flashback table %s to time %s parse the xlog record from lsn %X/%X "
		 "to lsn %X/%X", relname, timestamptz_to_str(end_time),
		 (uint32)(shared_state->wal_start_lsn >> 32), (uint32)(shared_state->wal_start_lsn),
		 (uint32)(end_lsn >> 32), (uint32)(end_lsn));
	return end_lsn;
}

static flshbak_tbl_st_t
construct_flashback_table_state(MemoryContext flashback_table_context, Relation rel, TimestampTz target_time)
{
	MemoryContext   old_context;
	fbpoint_rec_data_t fbpoint_rec;
	flshbak_tbl_shr_st_t shared_state = NULL;
	Snapshot snapshot;
	flshbak_tbl_st_t state = NULL;
	XLogRecPtr logindex_max_lsn_expected = InvalidXLogRecPtr;
	polar_flog_rec_ptr write_result;

	old_context = MemoryContextSwitchTo(flashback_table_context);
	shared_state = palloc(sizeof(flashback_table_shared_state_t));
	shared_state->old_relid = rel->rd_id;
	shared_state->nblocks = RelationGetNumberOfBlocks(rel);

	/* Get right flashback point */
	if (polar_get_right_fbpoint(fra_instance->point_ctl, fra_instance->dir,
								timestamptz_to_time_t(target_time), &fbpoint_rec, NULL))
	{
		elog(DEBUG2, "find the rigth flashback point before target time %s: "
			 "flashback log pointer is %08X/%08X, "
			 "WAL redo lsn is %08X/%08X, the time is %s, "
			 "the next clog sub directory number is %08X, "
			 "the snapshot position is %08X/%08X",
			 timestamptz_to_str(target_time),
			 (uint32)(fbpoint_rec.flog_ptr >> 32), (uint32) fbpoint_rec.flog_ptr,
			 (uint32)(fbpoint_rec.redo_lsn >> 32), (uint32) fbpoint_rec.redo_lsn,
			 timestamptz_to_str(time_t_to_timestamptz(fbpoint_rec.time)),
			 fbpoint_rec.next_clog_subdir_no,
			 fbpoint_rec.snapshot_pos.seg_no, fbpoint_rec.snapshot_pos.offset);
	}
	else
		/*no cover line*/
		elog(ERROR, "Can not find a right flashback point to flashback table %s.", RelationGetRelationName(rel));

	shared_state->flog_start_ptr = fbpoint_rec.flog_ptr;
	shared_state->rel_filenode = rel->rd_node;
	write_result = polar_get_flog_write_result(flog_instance->buf_ctl);

	/*
	 * Get origin relation file node.
	 *
	 * Sometime the flog_start_ptr is equal to the write_result,
	 * there is nothing to do.
	 */
	if (likely(shared_state->flog_start_ptr < write_result))
	{
		flog_reader_state *reader = NULL;

		/* Wait for flashback log to catch write result*/
		if (!polar_is_flog_index_inserted(flog_instance, &write_result))
		{
			elog(LOG, "Wait the flashback logindex to catch flashback log write result "
				 "to find origin relation file node of ([%u, %u, %u])",
				 shared_state->rel_filenode.spcNode, shared_state->rel_filenode.dbNode,
				 shared_state->rel_filenode.relNode);

			polar_wait_flog_bgworker(flog_instance, polar_is_flog_index_inserted,
									 &write_result);
		}

		FLOG_ALLOC_PAGE_READER(reader, flog_instance->buf_ctl, ERROR);

		while (polar_find_origin_filenode(flog_instance, &(shared_state->rel_filenode), target_time,
										  shared_state->flog_start_ptr, write_result, reader));

		/* Free the flashback log reader */
		polar_flog_reader_free(reader);
	}

	shared_state->wal_start_lsn = fbpoint_rec.redo_lsn;

	/* Get snapshot which xip is sorted */
	snapshot = polar_get_flashback_snapshot(fra_instance->dir, fbpoint_rec.snapshot_pos,
			&shared_state->next_clog_subdir_no, &shared_state->next_xid);

	/* Parse WAL and get end wal lsn and update the snapshot */
	shared_state->wal_end_lsn = flashback_table_apply_wal(snapshot, shared_state,
														  target_time, RelationGetRelationName(rel),
														  &logindex_max_lsn_expected);
	Assert(shared_state->wal_end_lsn >= shared_state->wal_start_lsn);

	/* Check the wal_logindex enough ? */
	if (ProcGlobal->walwriterLatch)
		SetLatch(ProcGlobal->walwriterLatch);

	while (logindex_max_lsn_expected &&
			logindex_max_lsn_expected > polar_get_logindex_snapshot_max_lsn(polar_logindex_redo_instance->wal_logindex_snapshot))
	{
		CHECK_FOR_INTERRUPTS();
		pg_usleep(1000);
	}

	shared_state->curr_xid = GetCurrentTransactionId();
	pg_atomic_init_u32(&(shared_state->next_blkno), 0);

	/* Set the pointer */
	state = palloc(sizeof(flashback_table_state_t));
	state->shared_state = shared_state;
	state->snapshot = snapshot;

	log_flashback_table_state(state, DEBUG2);
	MemoryContextSwitchTo(old_context);
	return state;
}

static inline bool
polar_is_buffer_unchanged(Buffer buffer, XLogRecPtr target_lsn)
{
	return BufferGetLSNAtomic(buffer) <= target_lsn;
}

static uint32
polar_flashback_rebuild_tuples(Buffer buffer, Snapshot snapshot, uint32 next_clog_subdir_no,
							   TransactionId max_xid, TransactionId xid, Oid relid, const char *fra_dir, bool use_tuple_infomask)
{
	OffsetNumber offnum,
				 maxoff;
	HeapTupleData tuple;
	Page page;
	BufferDesc *buf_desc;
	BlockNumber block;
	uint32      tuples = 0;

	buf_desc = GetBufferDescriptor(buffer - 1);
	block = buf_desc->tag.blockNum;
	page = BufferGetPage(buffer);

	/* Scan each block and reset the tuplehead. */
	maxoff = PageGetMaxOffsetNumber(page);

	for (offnum = FirstOffsetNumber;
			offnum <= maxoff;
			offnum = OffsetNumberNext(offnum))
	{
		ItemId      itemid;

		itemid = PageGetItemId(page, offnum);

		/* Unused items require no processing */
		if (!ItemIdIsUsed(itemid))
			continue;

		/* Redirect items mustn't be touched */
		if (ItemIdIsRedirected(itemid))
			continue;

		if (ItemIdIsDead(itemid))
			continue;

		ItemPointerSet(&(tuple.t_self), block, offnum);
		Assert(ItemIdIsNormal(itemid));
		tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemid);
		tuple.t_len = ItemIdGetLength(itemid);
		tuple.t_tableOid = relid;

		/* We can't use the info mask in the tuple, just clean it */
		if (!use_tuple_infomask)
		{
			tuple.t_data->t_infomask &= ~(HEAP_XMIN_COMMITTED | HEAP_XMIN_INVALID | HEAP_XMIN_FROZEN | HEAP_XMAX_COMMITTED | HEAP_XMAX_INVALID);
			tuple.t_data->t_infomask2 &= ~(HEAP2_XACT_MASK);
		}

		/*
		 * The criteria for counting a tuple as live in this block need to
		 * match what analyze.c's acquire_sample_rows() does, otherwise
		 * VACUUM and ANALYZE may produce wildly different reltuples
		 * values, e.g. when there are many recently-dead tuples.
		 *
		 * The logic here is a bit simpler than acquire_sample_rows(), as
		 * VACUUM can't run inside a transaction block, which makes some
		 * cases impossible (e.g. in-progress insert from the same
		 * transaction).
		 */
		switch (polar_tuple_satisfies_flashback(&tuple, buffer, snapshot, next_clog_subdir_no, max_xid, fra_dir))
		{
			case HEAPTUPLE_DEAD:
				ItemIdSetDead(itemid);
				break;

			case HEAPTUPLE_LIVE:
				HeapTupleHeaderSetXmin(tuple.t_data, xid);
				HeapTupleHeaderSetXmax(tuple.t_data, InvalidTransactionId);
				tuples++;
				break;

			default:
				/*no cover line*/
				elog(ERROR, "unexpected polar_tuple_satisfies_flashback result");
				break;
		}
	}

	return tuples;
}

/*
 * POLAR: Rebuild pages parallel.ƒ
 * Return the parallel worker number.
 */
static int
polar_launch_flashback_pages_workers(flshbak_tbl_st_t state)
{
	int         parallel_workers = 0;
	ParallelContext *pcxt;
	flshbak_tbl_shr_st_t flashback_shared_state;
	BufferUsage *buffer_usage;
	int i;
	int  querylen;
	char *sharedquery;
	Snapshot snapshot;
	TransactionId *xip;
	TransactionId *subxip;
	Size xip_size = 0;
	Size subxip_size = 0;

	parallel_workers = polar_workers_per_flashback_table;

	if (parallel_workers == 0)
		return 0;

	EnterParallelMode();
	pcxt = CreateParallelContext("postgres", "polar_flashback_pages_woker_main",
								 parallel_workers, true);
	Assert(pcxt->nworkers > 0);

	shm_toc_estimate_chunk(&pcxt->estimator,
						   sizeof(flashback_table_shared_state_t));
	shm_toc_estimate_keys(&pcxt->estimator, 1);

	shm_toc_estimate_chunk(&pcxt->estimator, sizeof(SnapshotData));
	shm_toc_estimate_keys(&pcxt->estimator, 1);

	if (state->snapshot->xcnt)
	{
		xip_size = mul_size(state->snapshot->xcnt, sizeof(TransactionId));
		shm_toc_estimate_chunk(&pcxt->estimator, xip_size);
		shm_toc_estimate_keys(&pcxt->estimator, 1);
	}

	if (state->snapshot->subxcnt)
	{
		subxip_size = mul_size(state->snapshot->subxcnt, sizeof(TransactionId));
		shm_toc_estimate_chunk(&pcxt->estimator, subxip_size);
		shm_toc_estimate_keys(&pcxt->estimator, 1);
	}

	shm_toc_estimate_chunk(&pcxt->estimator,
						   mul_size(sizeof(BufferUsage), pcxt->nworkers));
	shm_toc_estimate_keys(&pcxt->estimator, 1);

	querylen = strlen(debug_query_string);
	shm_toc_estimate_chunk(&pcxt->estimator, querylen + 1);
	shm_toc_estimate_keys(&pcxt->estimator, 1);

	InitializeParallelDSM(pcxt);

	flashback_shared_state =
		shm_toc_allocate(pcxt->toc, sizeof(flashback_table_shared_state_t));
	memcpy(flashback_shared_state, state->shared_state, sizeof(flashback_table_shared_state_t));
	pg_atomic_init_u32(&(flashback_shared_state->next_blkno), 0);
	shm_toc_insert(pcxt->toc, FLASHBACK_KEY_STATE, flashback_shared_state);

	snapshot = shm_toc_allocate(pcxt->toc, sizeof(SnapshotData));
	memcpy(snapshot, state->snapshot, sizeof(SnapshotData));
	shm_toc_insert(pcxt->toc, FLASHBACK_KEY_SNAPSHOT, snapshot);


	if (xip_size)
	{
		xip = shm_toc_allocate(pcxt->toc, xip_size);
		memcpy(xip, state->snapshot->xip, xip_size);
		shm_toc_insert(pcxt->toc, FLASHBACK_KEY_XIP, xip);
	}

	if (subxip_size)
	{
		subxip = shm_toc_allocate(pcxt->toc, subxip_size);
		memcpy(subxip, state->snapshot->subxip, subxip_size);
		shm_toc_insert(pcxt->toc, FLASHBACK_KEY_SUBXIP, subxip);
	}

	buffer_usage = shm_toc_allocate(pcxt->toc,
									mul_size(sizeof(BufferUsage), pcxt->nworkers));
	shm_toc_insert(pcxt->toc, FLASHBACK_KEY_BUFFER_USAGE, buffer_usage);

	sharedquery = (char *) shm_toc_allocate(pcxt->toc, querylen + 1);
	memcpy(sharedquery, debug_query_string, querylen);
	sharedquery[querylen] = '\0';
	shm_toc_insert(pcxt->toc, FLASHBACK_KEY_QUERY_TEXT, sharedquery);

	LaunchParallelWorkers(pcxt);
	parallel_workers = pcxt->nworkers_launched;
	WaitForParallelWorkersToFinish(pcxt);

	for (i = 0; i < pcxt->nworkers_launched; i++)
		InstrAccumParallelQuery(&buffer_usage[i]);

	DestroyParallelContext(pcxt);
	ExitParallelMode();
	return parallel_workers;
}

/*
 * POLAR: Flashback the relation page.
 *
 * There are some cases we can't find a origin page of the target tag:
 * 1. The origin page is in the disk. We will read the origin page from disk.
 * 2. The flashback log index has some flashback log not inserted. We will wait
 * them inserted.
 *
 * NB: Please make true the old buffer will not be flushed when process it in here.
 * We can lock it with EXCLUSIVE mode in the caller.
 */
static bool
polar_flashback_rel_page(flshbak_tbl_shr_st_t shared_state, BufferTag *tag,
						 BufferDesc *old_buf_desc, BufferDesc *new_buf_desc, flog_reader_state *reader)
{
	Buffer  new_buf;
	polar_flog_rec_ptr flog_end_ptr;
	polar_flog_rec_ptr logindex_max_ptr;
	bool    success = false;
	flshbak_buf_context_t context;

	Assert(old_buf_desc == NULL ||
		   LWLockHeldByMeInMode(BufferDescriptorGetContentLock(old_buf_desc), LW_EXCLUSIVE));

	new_buf = new_buf_desc->buf_id + 1;
	logindex_max_ptr = polar_get_logindex_max_parsed_lsn(flog_instance->logindex_snapshot);
	flog_end_ptr = polar_get_flog_write_result(flog_instance->buf_ctl);

	INIT_FLSHBAK_BUF_CONTEXT(context, shared_state->flog_start_ptr, flog_end_ptr,
							 shared_state->wal_start_lsn, shared_state->wal_end_lsn,
							 flog_instance->logindex_snapshot, reader, tag, new_buf, LOG, false);

	/* If no flashback log or can't flashback page with the flashback log and flashback logindex */
	if (shared_state->flog_start_ptr != flog_end_ptr)
		success = polar_flashback_buffer(&context);

	/*
	 * We can't find a valid origin page with the flashback log and
	 * flashback logindex. We will wait the flashback logindex and retry.
	 */
	if (!success && !polar_is_flog_index_inserted(flog_instance, &flog_end_ptr))
	{
		elog(LOG, "Wait the flashback logindex to find "
			 "origin page of " POLAR_LOG_BUFFER_TAG_FORMAT,
			 POLAR_LOG_BUFFER_TAG(tag));

		polar_wait_flog_bgworker(flog_instance, polar_is_flog_index_inserted,
								 &flog_end_ptr);

		/* Find the origin page with old logindex max pointer and new end pointer */
		context.start_ptr = logindex_max_ptr;
		success = polar_flashback_buffer(&context);
	}

	/*
	 * We can't find a valid origin page with the flashback log and
	 * flashback logindex, try to read the page in the disk as origin page.
	 *
	 * When the origin page can't be found in the flashback log and disk,
	 * it must be the last block or a flashback log lost.
	 */
	if (!success)
	{
		/* In some cases, the origin page is in the disk */
		if (new_buf_desc->tag.blockNum < shared_state->nblocks)
		{
			Assert(old_buf_desc);

			/* The origin page can't be in the disk with relfilenode changed */
			if (RelFileNodeEquals(tag->rnode, old_buf_desc->tag.rnode))
			{
				read_origin_page_from_file(tag, BufferGetPage(new_buf));

				if (likely(BufferGetLSN(new_buf_desc) <= shared_state->wal_end_lsn))
				{
					elog(LOG, "The origin page of " POLAR_LOG_BUFFER_TAG_FORMAT " is in the disk, "
						 "read and apply it", POLAR_LOG_BUFFER_TAG(tag));

					polar_logindex_apply_page(polar_logindex_redo_instance,
											  shared_state->wal_start_lsn,
											  shared_state->wal_end_lsn, tag, &new_buf);

					success = true;
				}
			}
		}
		/* We think it is last page */
		else
			return false;
	}

	if (!success)
		/*no cover line*/
		POLAR_LOG_FLOG_LOST(tag, ERROR);

	/* Check the page lsn */
	Assert(BufferGetLSN(new_buf_desc) <= shared_state->wal_end_lsn);
	return true;
}

static bool
polar_flashback_rebuild_page(flshbak_tbl_shr_st_t shared_state, Snapshot snapshot,
							 Buffer old_buffer, Buffer new_buffer, flog_reader_state *reader)
{
	Page new_page;
	Page old_page;
	XLogRecPtr new_lsn;
	BufferDesc  *old_buf_desc = NULL;
	BufferDesc  *new_buf_desc = NULL;
	RelFileNode new_rnode;
	BufferTag   old_tag;
	BlockNumber target_block;
	bool buf_changed = true;
	bool is_finished = false;

	new_buf_desc = GetBufferDescriptor(new_buffer - 1);
	/* Must hold the new buffer exclusive lock */
	Assert(LWLockHeldByMeInMode(BufferDescriptorGetContentLock(new_buf_desc), LW_EXCLUSIVE));
	new_page =  BufferGetPage(new_buffer);
	new_rnode = new_buf_desc->tag.rnode;
	target_block = new_buf_desc->tag.blockNum;
	INIT_BUFFERTAG(old_tag, shared_state->rel_filenode, MAIN_FORKNUM, target_block);

	if (BufferIsValid(old_buffer))
	{
		/* Check it is changed? */
		old_buf_desc = GetBufferDescriptor(old_buffer - 1);

		/* Hold the exclusive lock, so can't flush the buffer to disk */
		LockBuffer(old_buffer, BUFFER_LOCK_EXCLUSIVE);

		/* The block is unchanged from end_ptr to now, so just copy the buffer */
		if (polar_is_buffer_unchanged(old_buffer, shared_state->wal_end_lsn))
		{
			old_page = BufferGetPage(old_buffer);
			memcpy((char *)new_page, (char *) old_page, BLCKSZ);
			/* The update of info mask is without WAL, so we can't trust it */
			buf_changed = false;
		}
	}

	/*
	 * Get the origin page when buffer is changed.
	 * When the origin page can't find, it must be the last block.
	 */
	if (buf_changed && !polar_flashback_rel_page(shared_state, &old_tag,
												 old_buf_desc, new_buf_desc, reader))
		is_finished = true;

	if (!is_finished)
	{
		/* rebuild the tuples */
		if (!PageIsEmpty(new_page))
		{
			polar_flashback_rebuild_tuples(new_buffer, snapshot, shared_state->next_clog_subdir_no,
										   shared_state->next_xid, shared_state->curr_xid, shared_state->new_relid,
										   fra_instance->dir, buf_changed);
		}

		/* Log the new block */
		new_lsn = log_newpage(&new_rnode,
							  MAIN_FORKNUM,
							  target_block,
							  new_page,
							  true);
		PageSetLSN(new_page, new_lsn);
	}

	/*
	 * Release old and new buffer.
	 */
	if (BufferIsValid(old_buffer))
		UnlockReleaseBuffer(old_buffer);

	if (BufferIsValid(new_buffer))
	{
		/* Mark the new buffer dirty. */
		MarkBufferDirty(new_buffer);
		UnlockReleaseBuffer(new_buffer);
	}

	return is_finished;
}

static void
polar_flashback_rebuild_pages(flshbak_tbl_shr_st_t shared_state, Snapshot snapshot,
							  bool is_parallel)
{
	Relation     old_heap;
	Buffer       old_buffer = InvalidBuffer;
	/* New relation */
	Relation    new_heap;
	Buffer      new_buffer = InvalidBuffer;
	flog_reader_state *reader = NULL;

	old_heap = relation_open(shared_state->old_relid, AccessShareLock);
	new_heap = relation_open(shared_state->new_relid, AccessShareLock);

	FLOG_ALLOC_PAGE_READER(reader, flog_instance->buf_ctl, ERROR);

	for (;;)
	{
		BlockNumber target_block = InvalidBlockNumber;

		CHECK_FOR_INTERRUPTS();

		/* Init the buffer */
		old_buffer = InvalidBuffer;
		new_buffer = InvalidBuffer;

		target_block = pg_atomic_fetch_add_u32(&(shared_state->next_blkno), 1);
		LOG_FLSHBAK_TBL_SHR_STATE(shared_state, target_block, DEBUG2);

		/*
		 * Can't process the target block exceed than shared_state->nblocks
		 * in the parallel mode because we can't extend relation in the parallel
		 * mode.
		 */
		if (target_block < shared_state->nblocks)
			old_buffer = ReadBuffer(old_heap, target_block);
		else if (!is_parallel)
			target_block = P_NEW;
		else
			break;

		/* No need to read from disk, just zero and cleanup the buffer */
		new_buffer = ReadBufferExtended(new_heap, MAIN_FORKNUM, target_block, RBM_ZERO_AND_LOCK, NULL);

		if (polar_flashback_rebuild_page(shared_state, snapshot, old_buffer, new_buffer, reader))
			break;
	}

	/* Close the heap */
	heap_close(old_heap, AccessShareLock);
	heap_close(new_heap, AccessShareLock);

	/* Free the flashback log reader */
	polar_flog_reader_free(reader);
}

static void
bulk_extend_new_rel(Relation rel, BlockNumber nblocks)
{
#define MAX_EXTEND_NBLOCKS_ONCE (512)
	uint32 extend_nblks = 1;
	BlockNumber start_blk = 0;
	char *zero_blocks;

	/* 4 MB (512 blocks) once in shared storage mode */
	if (polar_enable_shared_storage_mode)
		zero_blocks = palloc0(BLCKSZ * MAX_EXTEND_NBLOCKS_ONCE);
	else
		zero_blocks = palloc0(BLCKSZ);

	RelationOpenSmgr(rel);

	/* 4 MB (512 blocks) once */
	while (nblocks > 0)
	{
		if (polar_enable_shared_storage_mode)
		{
			extend_nblks = Min(MAX_EXTEND_NBLOCKS_ONCE, nblocks);
			smgrextendbatch(rel->rd_smgr, MAIN_FORKNUM, start_blk, extend_nblks, zero_blocks, false);
		}
		else
			smgrextend(rel->rd_smgr, MAIN_FORKNUM, start_blk, zero_blocks, false);

		start_blk = start_blk + extend_nblks;
		nblocks = nblocks - extend_nblks;
	}

	pfree(zero_blocks);
}

static void
polar_flashback_rebuild_relation(flshbak_tbl_st_t state)
{
#define NEW_HEAP_NAME_PREFIX "polar_flashback"
	Oid           table_space;
	char    relpersistence;
	Oid           old_relid;
	Relation      old_rel;
	Oid           new_relid;
	Relation      new_rel;
	char          old_relname[NAMEDATALEN];
	char          new_relname[NAMEDATALEN];
	bool    can_parallel = true;
	BlockNumber nblocks = state->shared_state->nblocks;

	/* Get info for old relation */
	old_relid = state->shared_state->old_relid;
	old_rel = relation_open(old_relid, AccessShareLock);
	table_space = old_rel->rd_rel->reltablespace;
	relpersistence = old_rel->rd_rel->relpersistence;
	relation_close(old_rel, AccessShareLock);
	strncpy(old_relname, RelationGetRelationName(old_rel), NAMEDATALEN);

	/*
	 * Create the transient table that will receive the flashback data.
	 * perform_flashback_table has hold the AccessExclusiveLock,
	 * so we just hold NoLock.
	 */
	snprintf(new_relname, sizeof(new_relname), "%s_%u", NEW_HEAP_NAME_PREFIX, old_relid);
	new_relid = polar_make_new_heap(old_relid, table_space, relpersistence, NoLock, new_relname);
	state->shared_state->new_relid = new_relid;

	new_rel = relation_open(new_relid, AccessExclusiveLock);

	/*
	 * We can't process it in parallel mode when the relation size is zero.
	 */
	if (nblocks == 0)
		can_parallel = false;
	else
		bulk_extend_new_rel(new_rel, nblocks);

	/* Do the work in the parallel mode */
	if (can_parallel)
	{
		int     nworkers_launched = 0;

		nworkers_launched = polar_launch_flashback_pages_workers(state);

		/* The parallel work is done, update the next_blkno to nblocks */
		if (nworkers_launched)
			pg_atomic_write_u32(&(state->shared_state->next_blkno), state->shared_state->nblocks);
	}

	/* Rebuild the remain pages */
	polar_flashback_rebuild_pages(state->shared_state, state->snapshot, false);

	/* Make the update visible */
	CommandCounterIncrement();
	relation_close(new_rel, AccessExclusiveLock);

	elog(NOTICE, "Flashback the relation %s to new relation %s, "
		 "please check the data", old_relname, new_relname);
}

/* POLAR: Flashback a table */
static void
perform_flashback_table(const RangeVar *relation, TimestampTz target_time)
{
	Oid         table_oid;
	Relation    old_rel;
	flshbak_tbl_st_t state;
	/* lock mode is just AccessShareLock now */
	LOCKMODE lock_mode = AccessShareLock;
	MemoryContext flashback_table_context;

	/* Check for user-requested abort. */
	CHECK_FOR_INTERRUPTS();

	/*
	 * Create special memory context for cross-transaction storage.
	 *
	 * Since it is a child of PortalContext, it will go away even in case
	 * of error.
	 */
	flashback_table_context = AllocSetContextCreate(PortalContext,
													"Flashback table", ALLOCSET_DEFAULT_SIZES);

	/* Check for user-requested abort. */
	CHECK_FOR_INTERRUPTS();

	/* Find, lock, and check permissions and relation kind on the table. */
	table_oid = RangeVarGetRelidExtended(relation, lock_mode, 0, rangevar_callback_for_flashback, NULL);
	Assert(OidIsValid(table_oid));

	/*
	 * Now we just open the relation with AccessShareLock, but when we want to
	 * swap the relation file, must hold the AccessExclusiveLock and call CheckTableNotInUse
	 * and TransferPredicateLocksToHeapRelation.
	 */
	old_rel = relation_open(table_oid, lock_mode);

	/* Construct the flashback table state */
	state = construct_flashback_table_state(flashback_table_context, old_rel, target_time);

	/* polar_flashback_rebuild_relation does all the dirty work */
	polar_flashback_rebuild_relation(state);

	/* NB: polar_rebuild_relation does heap_close() on old_heap */
	relation_close(old_rel, lock_mode);

	/* Clean up working storage */
	MemoryContextDelete(flashback_table_context);
}

/* POLAR: flashback table statement main function. */
void
polar_exec_flashback_table_stmt(PolarFlashbackTableStmt *stmt)
{
	TimestampTz target_time;

	/* Check the GUCs */
	if (!fra_instance)
	{
		/*no cover line*/
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("Don't support to flashback table when"
							   "polar_enable_fast_recovery_area if off")));
	}

	/* Check the flashback log state */
	if (!polar_is_flog_ready(flog_instance))
	{
		/*no cover line*/
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("Don't support to flashback table when flashback log is not ready, "
							   "just wait a moment")));
	}

	/* Get the flashback target time and check */
	target_time = get_flashback_target_time((Expr *) stmt->time_expr);
	check_flashback_time(target_time);

	/* Rock and roll */
	perform_flashback_table(stmt->relation, target_time);
}

/*
 * POLAR: Perform work within a launched parallel process.
 *
 * Parallel flashback workers perform to flashback a block.
 */
void
polar_flashback_pages_woker_main(dsm_segment *seg, shm_toc *toc)
{
	char    *sharedquery;
	flshbak_tbl_shr_st_t shared_state;
	BufferUsage *buffer_usage;
	Snapshot snapshot;
	SnapshotData local_snapshot;

	shared_state = (flshbak_tbl_shr_st_t) shm_toc_lookup(toc, FLASHBACK_KEY_STATE, false);
	snapshot = (Snapshot) shm_toc_lookup(toc, FLASHBACK_KEY_SNAPSHOT, false);

	/* Copy snapshot to local */
	memcpy(&local_snapshot, snapshot, sizeof(SnapshotData));
	local_snapshot.xip = local_snapshot.subxip = NULL;

	if (snapshot->xcnt)
	{
		local_snapshot.xip = (TransactionId *) shm_toc_lookup(toc, FLASHBACK_KEY_XIP, false);
		Assert(local_snapshot.xip);
	}

	if (snapshot->subxcnt)
	{
		local_snapshot.subxip = (TransactionId *) shm_toc_lookup(toc, FLASHBACK_KEY_SUBXIP, false);
		Assert(local_snapshot.subxip);
	}

	log_flashback_snapshot(&local_snapshot, DEBUG2);

	/* Report the query string */
	sharedquery = (char *) shm_toc_lookup(toc, FLASHBACK_KEY_QUERY_TEXT, false);
	debug_query_string = sharedquery;
	pgstat_report_activity(STATE_RUNNING, debug_query_string);

	InstrStartParallelQuery();
	polar_flashback_rebuild_pages(shared_state, &local_snapshot, true);
	buffer_usage = (BufferUsage *) shm_toc_lookup(toc, FLASHBACK_KEY_BUFFER_USAGE, false);
	InstrEndParallelQuery(&buffer_usage[ParallelWorkerNumber]);
}
