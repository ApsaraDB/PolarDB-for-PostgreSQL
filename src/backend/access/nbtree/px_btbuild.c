/*-------------------------------------------------------------------------
 *
 * px_btbuild.c
 *		Build a btree from sorted using PolarDB Parallel Execution.
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
 *		src/backend/access/nbtree/px_btbuild.c
 *
 *-------------------------------------------------------------------------
 */
#include "access/reloptions.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "commands/defrem.h"
#include "nodes/makefuncs.h"
#include "px/px_util.h"
#include "utils/inval.h"
#include "utils/syscache.h"

static IndexTuple index_form_tuple_noalloc(TupleDesc tupleDescriptor,
				 Datum *values, bool *isnull,
				 char *start, Size free, Size *used);
static PxWorkerstate * _bt_start_pxworker(Relation heap, Relation index);
static void _bt_produce_pxworker(PxWorkerstate *px);
static void _bt_finish_pxworker(PxWorkerstate *px);
static void _bt_begin_pxworker(BTBuildState *buildstate, 
								Relation heap, Relation index, bool isconcurrent);
static void _bt_px_init_buffer(PxIndexTupleBuffer *buffer);
static void _bt_leafbuild_pxleader(PxLeaderstate *px);
static void _bt_finish_pxleader(PxLeaderstate *pxleader);
static IndexBuildResult *pxbuild(Relation heap, Relation index, IndexInfo *indexInfo);
static IndexTuple _bt_consume_pxleader(PxLeaderstate *pxleader);
static void _bt_px_alloc_page_buffer(PxIndexPageBuffer **px_index_buffer);
static void _bt_px_blwritepage(BTWriteState *wstate, Page page, BlockNumber blkno);
static void _bt_px_flush_blpage(BTWriteState *wstate);
static void _bt_px_bulkwrite(SMgrRelationData *rd_smgr, PxIndexPageBuffer *buffer, int start, int count);
static bool px_bulk_write_page_valid(PxIndexPageBuffer *buffer, int start, int count);

static IndexTuple
index_form_tuple_noalloc(TupleDesc tupleDescriptor,
				 Datum *values,
				 bool *isnull,
				 char *start,
				 Size free,
				 Size *used)
{
	char	   *tp;				/* tuple pointer */
	IndexTuple	tuple;			/* return tuple */
	Size		size,
				data_size,
				hoff;
	int			i;
	unsigned short infomask = 0;
	bool		hasnull = false;
	uint16		tupmask = 0;
	int			numberOfAttributes = tupleDescriptor->natts;

#ifdef TOAST_INDEX_HACK
	Datum		untoasted_values[INDEX_MAX_KEYS];
	bool		untoasted_free[INDEX_MAX_KEYS];
#endif

	if (numberOfAttributes > INDEX_MAX_KEYS)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_COLUMNS),
				 errmsg("number of index columns (%d) exceeds limit (%d)",
						numberOfAttributes, INDEX_MAX_KEYS)));

#ifdef TOAST_INDEX_HACK
	for (i = 0; i < numberOfAttributes; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupleDescriptor, i);

		untoasted_values[i] = values[i];
		untoasted_free[i] = false;

		/* Do nothing if value is NULL or not of varlena type */
		if (isnull[i] || att->attlen != -1)
			continue;

		/*
		 * If value is stored EXTERNAL, must fetch it so we are not depending
		 * on outside storage.  This should be improved someday.
		 */
		if (VARATT_IS_EXTERNAL(DatumGetPointer(values[i])))
		{
			untoasted_values[i] =
				PointerGetDatum(heap_tuple_fetch_attr((struct varlena *)
													  DatumGetPointer(values[i])));
			untoasted_free[i] = true;
		}

		/*
		 * If value is above size target, and is of a compressible datatype,
		 * try to compress it in-line.
		 */
		if (!VARATT_IS_EXTENDED(DatumGetPointer(untoasted_values[i])) &&
			VARSIZE(DatumGetPointer(untoasted_values[i])) > TOAST_INDEX_TARGET &&
			(att->attstorage == 'x' || att->attstorage == 'm'))
		{
			Datum		cvalue = toast_compress_datum(untoasted_values[i]);

			if (DatumGetPointer(cvalue) != NULL)
			{
				/* successful compression */
				if (untoasted_free[i])
					pfree(DatumGetPointer(untoasted_values[i]));
				untoasted_values[i] = cvalue;
				untoasted_free[i] = true;
			}
		}
	}
#endif

	for (i = 0; i < numberOfAttributes; i++)
	{
		if (isnull[i])
		{
			hasnull = true;
			break;
		}
	}

	if (hasnull)
		infomask |= INDEX_NULL_MASK;

	hoff = IndexInfoFindDataOffset(infomask);
#ifdef TOAST_INDEX_HACK
	data_size = heap_compute_data_size(tupleDescriptor,
									   untoasted_values, isnull);
#else
	data_size = heap_compute_data_size(tupleDescriptor,
									   values, isnull);
#endif
	size = hoff + data_size;
	size = MAXALIGN(size);		/* be conservative */

	*used = size;

	if (size > free)
		return NULL;

	tp = start;
	tuple = (IndexTuple) tp;

	heap_fill_tuple(tupleDescriptor,
#ifdef TOAST_INDEX_HACK
					untoasted_values,
#else
					values,
#endif
					isnull,
					(char *) tp + hoff,
					data_size,
					&tupmask,
					(hasnull ? (bits8 *) tp + sizeof(IndexTupleData) : NULL));

	/*
	 * We do this because heap_fill_tuple wants to initialize a "tupmask"
	 * which is used for HeapTuples, but we want an indextuple infomask. The
	 * only relevant info is the "has variable attributes" field. We have
	 * already set the hasnull bit above.
	 */
	if (tupmask & HEAP_HASVARWIDTH)
		infomask |= INDEX_VAR_MASK;

	/* Also assert we got rid of external attributes */
#ifdef TOAST_INDEX_HACK
	Assert((tupmask & HEAP_HASEXTERNAL) == 0);
#endif

	/*
	 * Here we make sure that the size will fit in the field reserved for it
	 * in t_info.
	 */
	if ((size & INDEX_SIZE_MASK) != size)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("index row requires %zu bytes, maximum size is %zu",
						size, (Size) INDEX_SIZE_MASK)));

	infomask |= size;

	/*
	 * initialize metadata
	 */
	tuple->t_info = infomask;
	return tuple;
}

IndexBuildResult *
pxbuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
	IndexBuildResult *result;
	BTBuildState buildstate;

	/*
	 * check if pxbuild can be used.
	 */
	if (indexInfo->ii_Expressions)
		elog(ERROR, "PX index build does not support expr index now");

	/*
	 * We expect to be called exactly once for any index relation. If that's
	 * not the case, big trouble's what we have.
	 */
	if (RelationGetNumberOfBlocks(index) != 0)
	{
		polar_check_nblocks_consistent(index);
		elog(ERROR, "index \"%s\" already contains data",
			 RelationGetRelationName(index));
	}

	buildstate.isunique = indexInfo->ii_Unique;
	buildstate.havedead = false;
	buildstate.heap = heap;
	buildstate.spool = NULL;
	buildstate.spool2 = NULL;
	buildstate.indtuples = 0;
	buildstate.btleader = NULL;
	buildstate.pxleader = NULL;

	if (px_info_debug)
		elog(INFO, "btbuild px with %d workers",  getPxWorkerCount());
	_bt_begin_pxworker(&buildstate, heap, index, indexInfo->ii_Concurrent);
	_bt_leafbuild_pxleader(buildstate.pxleader);
	_bt_finish_pxleader(buildstate.pxleader);
	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
	result->heap_tuples = buildstate.pxleader->processed;
	result->index_tuples = buildstate.indtuples;

	if (!indexInfo->ii_Concurrent)
		polar_px_btbuild_update_pg_class(heap, index);

	return result;
}

void
polar_px_bt_build_main(dsm_segment *seg, shm_toc *toc)
{
	char	   *sharedquery;
	PxShared   *shared;
	Relation	heapRel;
	Relation	indexRel;
	LOCKMODE	heapLockmode;
	LOCKMODE	indexLockmode;
	BufferUsage *bufferusage;
	PxWorkerstate *pxstate;

	/* Set debug_query_string for individual workers first */
	sharedquery = shm_toc_lookup(toc, PX_KEY_QUERY_TEXT, false);
	debug_query_string = sharedquery;

	/* Report the query string from leader */
	pgstat_report_activity(STATE_RUNNING, debug_query_string);

	/* Look up nbtree shared state */
	shared = shm_toc_lookup(toc, PX_KEY_BTREE_SHARED, false);

	/* Open relations using lock modes known to be obtained by index.c */
	if (!shared->isconcurrent)
	{
		heapLockmode = ShareLock;
		indexLockmode = AccessExclusiveLock;
	}
	else
	{
		heapLockmode = ShareUpdateExclusiveLock;
		indexLockmode = RowExclusiveLock;
	}

	/* Open relations within worker */
	heapRel = heap_open(shared->heaprelid, heapLockmode);
	indexRel = index_open(shared->indexrelid, indexLockmode);

	pxstate = _bt_start_pxworker(heapRel, indexRel);
	pxstate->shared = shared;

	_bt_produce_pxworker(pxstate);
	_bt_finish_pxworker(pxstate);
	/* Prepare to track buffer usage during parallel execution */
	InstrStartParallelQuery();

	/* Report buffer usage during parallel execution */
	bufferusage = shm_toc_lookup(toc, PX_KEY_BUFFER_USAGE, false);
	InstrEndParallelQuery(&bufferusage[ParallelWorkerNumber]);

#ifdef BTREE_BUILD_STATS
	if (log_btree_build_stats)
	{
		ShowUsage("BTREE BUILD (Worker Partial Spool) STATISTICS");
		ResetUsage();
	}
#endif							/* BTREE_BUILD_STATS */

	index_close(indexRel, indexLockmode);
	heap_close(heapRel, heapLockmode);
}

static PxWorkerstate *
_bt_start_pxworker(Relation heap, Relation index)
{
	int i;
	bool old_enable_px = polar_enable_px;
	bool old_px_plancache = px_enable_plan_cache;
	bool old_px_index = px_optimizer_enable_indexscan;
	bool old_px_indexonly = px_optimizer_enable_indexonlyscan;
	bool old_px_check_workers = px_enable_check_workers;
	bool old_px_tx = px_enable_transaction;
	bool old_px_replay_wait = px_enable_replay_wait;

	PxWorkerstate *px = palloc0(sizeof(PxWorkerstate));
	px->fetch_end = true;
	px->sql = makeStringInfo();

	/* generate sql */
	{
		StringInfo 	attrs = makeStringInfo();		/* attrs in SELECT clause */
		StringInfo	sortattrs = makeStringInfo();	/* attrs in ORDER BY clause */
		TupleDesc	tupdes = RelationGetDescr(index);
		int 		natts = tupdes->natts;
		ScanKey 	scankey = _bt_mkscankey_nodata(index);
		Assert(natts > 0);

		for (i = 0; i < natts; i++, scankey++)
		{
			Form_pg_attribute att = TupleDescAttr(tupdes, i);
			appendStringInfo(attrs, ", %s", NameStr(att->attname));

			appendStringInfo(sortattrs, "%s ", NameStr(att->attname));
			if ((scankey->sk_flags & SK_BT_DESC) != 0)
				appendStringInfo(sortattrs, "desc ");
			if ((scankey->sk_flags & SK_BT_NULLS_FIRST) != 0)
				appendStringInfo(sortattrs, "nulls first ");
			else
				appendStringInfo(sortattrs, "nulls last ");
			if (i != natts - 1)
				appendStringInfo(sortattrs, ", ");
		}
		appendStringInfo(px->sql, "select _root_ctid %s from %s order by %s",
			attrs->data, RelationGetRelationName(heap), sortattrs->data);

		elog(DEBUG3, "sql: %s", px->sql->data);
	}

	px->heap = heap;
	px->index = index;

	gettimeofday(&px->tv1, NULL);

	/* invoke SPI */
	{
		polar_enable_px = true;
		px_optimizer_enable_indexscan = false;
		px_optimizer_enable_indexonlyscan = false;
		px_enable_check_workers = false;
		px_enable_plan_cache = false;
		px_enable_transaction = true;
		px_enable_replay_wait = true;
	}

	SPI_connect();

	if ((px->plan = SPI_prepare_px(px->sql->data, 0, NULL)) == NULL)
		elog(ERROR, "SPI_prepare(\"%s\") failed", px->sql->data);

	if ((px->portal = SPI_cursor_open(NULL, px->plan, NULL, NULL, true)) == NULL)
		elog(ERROR, "SPI_cursor_open(\"%s\") failed", px->sql->data);

	{
		polar_enable_px = old_enable_px;
		px_optimizer_enable_indexscan = old_px_index;
		px_optimizer_enable_indexonlyscan = old_px_indexonly;
		px_enable_check_workers = old_px_check_workers;
		px_enable_plan_cache = old_px_plancache;
		px_enable_transaction = old_px_tx;
		px_enable_replay_wait = old_px_replay_wait;
	}
	return px;
}

static bool
polar_fetch_all_index_buffer(PxWorkerstate *pxworker, PxIndexTupleBuffer *buffer, uint64 begin)
{
	uint64 i;
	for (i = begin; i < SPI_processed; i++)
	{
		Datum	   values[INDEX_MAX_KEYS + 1];
		bool	   nulls[INDEX_MAX_KEYS + 1];
		IndexTuple  ituple;
		ItemPointer ip;
		Size		used;
		HeapTuple	tup = SPI_tuptable->vals[i];
		heap_deform_tuple(tup, SPI_tuptable->tupdesc, values, nulls);

		ip = (ItemPointer)values[0];
		ituple = index_form_tuple_noalloc(RelationGetDescr(pxworker->index),
										values + 1,
										nulls + 1,
										POLAR_GET_PX_ITUPLE_MEM(buffer) + buffer->ioffset,
										px_btbuild_mem_size - buffer->ioffset,
										&used);

		if (ituple == NULL)
		{
			pxworker->last_produce = i;
			pxworker->fetch_end = false;
			return false;
		}
		elog(DEBUG5, "btbuild px worker, put index tuple, buffer %d, offset %d, citd (%u, %u)", 
		pxworker->shared->bp, buffer->ioffset,
		ItemPointerGetBlockNumber(ip), ItemPointerGetOffsetNumber(ip));

		ituple->t_tid = *ip;

		buffer->addr[buffer->icount++] = buffer->ioffset;
		buffer->ioffset += used;
		pxworker->processed++;
	}
	return true;
}

/*
 * POLAR PX: create a parallel context for px btbuild.
 * PxShared is a varlen structure. It contains px_btbuild_queue_size PxIndexTupleBuffer and
 * each PxIndexTupleBuffer contains px_btbuild_mem_size size of mem.
 * So the structure of PxShared is defined as::
 * +--------------------------------------------------------------------------+
 * | heaprelid indexrelid isunique ... done                                   |
 * +--------------------------------------------------------------------------+
 * | PxIndexTupleBuffer1 PxIndexTupleBuffer1->ituple PxIndexTupleBuffer1->mem |
 * +--------------------------------------------------------------------------+
 * | PxIndexTupleBuffer2 PxIndexTupleBuffer2->ituple PxIndexTupleBuffer2->mem |
 * +--------------------------------------------------------------------------+
 * |			         ...                                                  |
 * +--------------------------------------------------------------------------+
 * | PxIndexTupleBuffern PxIndexTupleBuffern->ituple PxIndexTupleBuffern->mem |
 * +--------------------------------------------------------------------------+
 */
static void
_bt_begin_pxworker(BTBuildState *buildstate, Relation heap, Relation index, bool isconcurrent)
{
	ParallelContext *pcxt;
	Snapshot	snapshot;
	Size		pxbtshared;
	Size 		itbuffer;
	Size		batchsize;
	Size 		memsize;
	PxShared   *shared;
	BufferUsage *bufferusage;
	PxLeaderstate *pxleader;
	char	   *sharedquery;
	int			querylen;

	buildstate->pxleader = pxleader = (PxLeaderstate*)palloc0(sizeof(PxLeaderstate));
	gettimeofday(&pxleader->tv1, NULL);

	/*
	 * Enter parallel mode, and create context for px parallel build of btree
	 * index
	 */
	EnterParallelMode();
	pcxt = CreateParallelContext("postgres", "polar_px_bt_build_main",
								 1, true);
	/*
	 * Prepare for scan of the base relation.  In a normal index build, we use
	 * SnapshotAny because we must retrieve all tuples and do our own time
	 * qual checks (because we have to index RECENTLY_DEAD tuples).  In a
	 * concurrent build, we take a regular MVCC snapshot and index whatever's
	 * live according to that.
	 */
	if (!isconcurrent)
		snapshot = SnapshotAny;
	else
		snapshot = RegisterSnapshot(GetTransactionSnapshot());

	/*
	 * Estimate size for our own PARALLEL_KEY_BTREE_SHARED workspace, and
	 * PARALLEL_KEY_TUPLESORT tuplesort workspace
	 */
	pxbtshared = sizeof(PxShared);

	/* calculate the size of PxIndexTupleBuffer for PxShared */
	itbuffer = sizeof(PxIndexTupleBuffer) * px_btbuild_queue_size;

	/* calculate the size of addr for PxIndexTupleBuffer */
	batchsize = sizeof(Size) * px_btbuild_batch_size * px_btbuild_queue_size;

	/* calculate the size of mem for PxIndexTupleBuffer */
	memsize = sizeof(char) * px_btbuild_mem_size * px_btbuild_queue_size * (INDEX_MAX_KEYS + 1);

	pxbtshared += itbuffer;
	pxbtshared += batchsize;
	pxbtshared += memsize;
	shm_toc_estimate_chunk(&pcxt->estimator, pxbtshared);
	shm_toc_estimate_keys(&pcxt->estimator, 1);

	/* Finally, estimate PARALLEL_KEY_QUERY_TEXT space */
	querylen = strlen(debug_query_string);
	shm_toc_estimate_chunk(&pcxt->estimator, querylen + 1);
	shm_toc_estimate_keys(&pcxt->estimator, 1);

	/* Everyone's had a chance to ask for space, so now create the DSM */
	InitializeParallelDSM(pcxt);

	/* If no DSM segment was available, back out (do serial build) */
	if (pcxt->seg == NULL)
	{
		if (IsMVCCSnapshot(snapshot))
			UnregisterSnapshot(snapshot);
		DestroyParallelContext(pcxt);
		ExitParallelMode();
		elog(ERROR, "btbuild alloc px parallel worker dsm failed.");
		return;
	}

	/* Store shared build state, for which we reserved space */
	shared = (PxShared*) shm_toc_allocate(pcxt->toc, pxbtshared);
	/* Initialize immutable state */
	shared->heaprelid = RelationGetRelid(heap);
	shared->indexrelid = RelationGetRelid(index);
	shared->isunique = false;
	shared->isconcurrent = isconcurrent;

	ConditionVariableInit(&shared->cv);
	SpinLockInit(&shared->mutex);
	/* Initialize mutable state */
	shared->bfilled = 0;
	shared->bc = -1;
	shared->bp = -1;
	shared->done = false;

	shm_toc_insert(pcxt->toc, PX_KEY_BTREE_SHARED, shared);

	/* Store query string for workers */
	sharedquery = (char *) shm_toc_allocate(pcxt->toc, querylen + 1);
	memcpy(sharedquery, debug_query_string, querylen + 1);
	shm_toc_insert(pcxt->toc, PX_KEY_QUERY_TEXT, sharedquery);

	/* Allocate space for each worker's BufferUsage; no need to initialize */
	bufferusage = shm_toc_allocate(pcxt->toc,
								   mul_size(sizeof(BufferUsage), pcxt->nworkers));
	shm_toc_insert(pcxt->toc, PX_KEY_BUFFER_USAGE, bufferusage);

	/* Launch workers, saving status for leader/caller */
	LaunchParallelWorkers(pcxt);

	/* If no workers were successfully launched, back out (do serial build) */
	if (pcxt->nworkers_launched == 0)
	{
		elog(ERROR, "btbuild launch px parallel worker failed.");
		return;
	}

	/* Save leader state now that it's clear build will be parallel */
	pxleader->heap = heap;
	pxleader->index = index;
	pxleader->shared = shared;
	pxleader->buffer = NULL;
	pxleader->bufferidx = 0;
	pxleader->processed = 0;
	pxleader->pcxt = pcxt;
	pxleader->snapshot = snapshot;
	pxleader->bufferusage = bufferusage;
	pxleader->waittimes = 0;

	/*
	 * Caller needs to wait for all launched workers when we return.  Make
	 * sure that the failure-to-start case will not hang forever.
	 */
	WaitForParallelWorkersToAttach(pcxt);
}

static void
_bt_produce_pxworker(PxWorkerstate *pxworker)
{
	PxShared   *shared = pxworker->shared;
	PxIndexTupleBuffer *buffer = NULL;
	gettimeofday(&pxworker->tv2, NULL);

	for(;;)
	{
		/* get free buffer */
		{
			for (;;)
			{
				SpinLockAcquire(&shared->mutex);
				if (shared->bfilled < px_btbuild_queue_size)
				{
					int bpnext = PxQueueNext(shared->bp);
					shared->bp = bpnext;
					SpinLockRelease(&shared->mutex);
					buffer = POLAR_GET_PX_ITUPLE(shared, bpnext);
					elog(DEBUG3, "btbuild px worker get buffer ok %d", bpnext);
					break;
				}
				SpinLockRelease(&shared->mutex);
				pxworker->waittimes++;
				elog(DEBUG2, "btbuild px worker get buffer waitting %lu", pxworker->waittimes);
				ConditionVariableSleep(&shared->cv,
									WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN);
			}
		}

		Assert(buffer);
		_bt_px_init_buffer(buffer);

		/**
		 *  fetch tuple from qc, fill into buffer
		 * 	after fetch, these tuples must be fill into buffer
		 */
		{
			/* fetch tuples from the last produce location at location pxworker->last_produce */
			if (!pxworker->fetch_end)
			{
				if (polar_fetch_all_index_buffer(pxworker, buffer, pxworker->last_produce) == false)
					goto produce_buffer;
				else
					SPI_freetuptable(SPI_tuptable);
			}

			/*
			 * already fetched all the tuples from the last produce location,
			 * reset the location and fetch new tuples.
			 */
			SPI_cursor_fetch(pxworker->portal, true, px_btbuild_batch_size);

			if (SPI_processed == 0 && pxworker->fetch_end)
			{
				SpinLockAcquire(&shared->mutex);
				shared->done = true;
				SpinLockRelease(&shared->mutex);
				ConditionVariableSignal(&shared->cv);
				ConditionVariableCancelSleep();
				gettimeofday(&pxworker->tv4, NULL);
				return;
			}

			/* fetch from the new tuples at location 0 */
			pxworker->last_produce = 0;
			pxworker->fetch_end = true;
			if (polar_fetch_all_index_buffer(pxworker, buffer, 0) == false)
				goto produce_buffer;
			else
				SPI_freetuptable(SPI_tuptable);

produce_buffer:
			SpinLockAcquire(&shared->mutex);
			shared->bfilled ++;
			SpinLockRelease(&shared->mutex);
			ConditionVariableSignal(&shared->cv);

			if (unlikely(pxworker->processbuffers == 0))
				gettimeofday(&pxworker->tv3, NULL);

			pxworker->processbuffers++;
		}
	}
}

static void
_bt_finish_pxworker(PxWorkerstate *pxworker)
{
	if (px_info_debug)
		elog(INFO, "btbiuld px worker finish,  open cursor: %ldms, first fetch: %ldms, total fetch: %ldms, total processd: %lu, total buffers: %lu, total waittimes: %lu", 
			TvDiff(pxworker->tv2, pxworker->tv1),
			TvDiff(pxworker->tv3, pxworker->tv2),
			TvDiff(pxworker->tv4, pxworker->tv2),
			pxworker->processed, 
			pxworker->processbuffers,
			pxworker->waittimes);

	SPI_freetuptable(SPI_tuptable);
	SPI_cursor_close(pxworker->portal);
	SPI_freeplan(pxworker->plan);
	SPI_finish();
}

static IndexTuple
_bt_consume_pxleader(PxLeaderstate *pxleader)
{
	PxShared *shared = pxleader->shared;
	PxIndexTupleBuffer *b = pxleader->buffer;
	for(;;)
	{
		/* get on indextuple */
		if (b && b->idx < b->icount)
		{
			Size offset = b->addr[b->idx++];
			IndexTuple itup = (IndexTuple)(POLAR_GET_PX_ITUPLE_MEM(b) + offset);
			elog(DEBUG5, "btbuild px leader, get index tuple, buffer %d, offset %lu, citd (%u, %u)", 
					pxleader->bufferidx, offset, 
					ItemPointerGetBlockNumber(&itup->t_tid), ItemPointerGetOffsetNumber(&itup->t_tid));

			if(unlikely(pxleader->processed == 0))
				gettimeofday(&pxleader->tv2, NULL);

			pxleader->processed++;
			return itup;
		}

		/* notify producer */
		if (b)
		{
			SpinLockAcquire(&shared->mutex);
			shared->bfilled --;
			SpinLockRelease(&shared->mutex);
			ConditionVariableSignal(&shared->cv);
			pxleader->processbuffers++;
			elog(DEBUG3, "btbuild px leader, consumed one buffer notify px worker, idx:%d, icount:%d", b->idx, b->icount);
		}

		/* get another buffer */
		{
			for (;;)
			{
				SpinLockAcquire(&shared->mutex);

				if (shared->bfilled > 0)
				{
					int bcnext = PxQueueNext(shared->bc);
					shared->bc = bcnext;
					SpinLockRelease(&shared->mutex);
					elog(DEBUG3, "btbuild px leader get buffer %d", bcnext);
					pxleader->buffer = b = POLAR_GET_PX_ITUPLE(shared, bcnext);
					pxleader->bufferidx = bcnext;
					break;
				}

				if (shared->done)
				{
					SpinLockRelease(&shared->mutex);
					ConditionVariableCancelSleep();
					return NULL;
				}

				SpinLockRelease(&shared->mutex);
				pxleader->waittimes++;
				elog(DEBUG2, "btbuild px leader get buffer waitting %lu", pxleader->waittimes);
				ConditionVariableSleep(&shared->cv,
									WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN);
			}
		}
	}
}

static void
_bt_leafbuild_pxleader(PxLeaderstate *pxleader)
{
	BTWriteState wstate;
	wstate.pxleader = pxleader;
	wstate.heap = pxleader->heap;
	wstate.index = pxleader->index;

	/*
	 * We need to log index creation in WAL iff WAL archiving/streaming is
	 * enabled UNLESS the index isn't WAL-logged anyway.
	 */
	wstate.btws_use_wal = XLogIsNeeded() && RelationNeedsWAL(wstate.index);

	/* reserve the metapage */
	wstate.btws_pages_alloced = BTREE_METAPAGE + 1;
	wstate.btws_pages_written = 0;
	wstate.btws_zeropage = NULL;	/* until needed */

	/* POLAR: bulk extend index file */
	wstate.polar_index_create_bulk_extend_size_copy = polar_index_create_bulk_extend_size;
	wstate.px_index_buffer = NULL;

	_bt_load(&wstate, NULL, NULL);

	/*
	 * POLAR: Manually free memory of `wstate.btws_zeropage' beforehand,
	 * instead of automatically freeing memory via memcontext.
	 *
	 * `wstate' is a local variable of function _bt_leafbuild(),
	 * which would be destructed when _bt_leafbuild() finished.
	 * So it's safe to manually free memory of `wstate.btws_zeropage' here.
	 *
	 * Why this is needed? Avoid OOM.
	 * For example, polar_index_create_bulk_extend_size = 512, `wstate.btws_zeropage' will be 4MB.
	 * The OOM case is truncating a partitioning table with huge number of sub-partitioning.
	 * Huge number of indexes would be reindexed in one transaction.
	 * Every index has its own `wstate.btws_zeropage' memory in the only one transaction,
	 * If we don't manually free memory of `wstate.btws_zeropage' here,
	 * huge number of `wstate.btws_zeropage's would leed to OOM.
	 */
	if (wstate.btws_zeropage != NULL)
	{
		pfree(wstate.btws_zeropage);
		wstate.btws_zeropage = NULL;
	}
	/* POLAR: end */
}

static void
_bt_px_init_buffer(PxIndexTupleBuffer *buffer)
{
	buffer->icount = 0;
	buffer->ioffset = 0;
	buffer->idx = 0;
}

static void
_bt_finish_pxleader(PxLeaderstate *pxleader)
{
	int			i;
	if (pxleader->shared->bfilled !=0)
		elog(ERROR, "btbuild px leader, bfilled faild: %d", pxleader->shared->bfilled);

	gettimeofday(&pxleader->tv3, NULL);
	if (px_info_debug)
		elog(INFO, "btbuild px leader finish, first indextuple time: %ld, total build leaf time: %ld, total process: %lu, total buffers: %lu, total waittimes: %lu", 
			TvDiff(pxleader->tv2, pxleader->tv1),
			TvDiff(pxleader->tv3, pxleader->tv2),
			pxleader->processed, pxleader->processbuffers, pxleader->waittimes);
	/* Shutdown worker processes */
	WaitForParallelWorkersToFinish(pxleader->pcxt);

	/*
	 * Next, accumulate buffer usage.  (This must wait for the workers to
	 * finish, or we might get incomplete data.)
	 */
	for (i = 0; i < pxleader->pcxt->nworkers_launched; i++)
		InstrAccumParallelQuery(&pxleader->bufferusage[i]);

	/* Free last reference to MVCC snapshot, if one was used */
	if (IsMVCCSnapshot(pxleader->snapshot))
		UnregisterSnapshot(pxleader->snapshot);
	DestroyParallelContext(pxleader->pcxt);
	ExitParallelMode();
}

/*
 * The same as _bt_blwritepage. But in this function, we use px_index_buffer
 * for page bulk write.
 */
static void
_bt_px_blwritepage(BTWriteState *wstate, Page page, BlockNumber blkno)
{
	_bt_px_alloc_page_buffer(&wstate->px_index_buffer);

	/* Ensure rd_smgr is open (could have been closed by relcache flush!) */
	RelationOpenSmgr(wstate->index);

	/* XLOG stuff */
	if (wstate->btws_use_wal)
	{
		/* We use the heap NEWPAGE record type for this */
		log_newpage(&wstate->index->rd_node, MAIN_FORKNUM, blkno, page, true);
	}

	if (wstate->px_index_buffer->item_len == polar_bt_write_page_buffer_size)
		_bt_px_flush_blpage(wstate);

	PageEncryptInplace(page, MAIN_FORKNUM, blkno);
	PageSetChecksumInplace(page, blkno);

	memcpy(wstate->px_index_buffer->item[wstate->px_index_buffer->item_len].page, page, BLCKSZ);
	wstate->px_index_buffer->item[wstate->px_index_buffer->item_len].blkno = blkno;
	if (blkno > wstate->btws_pages_written)
		wstate->btws_pages_written = blkno;

	wstate->px_index_buffer->item_len++;
	wstate->px_index_buffer->current_written_blkno = wstate->btws_pages_written;

	pfree(page);
}

static int
px_index_buffer_compare(const void *a, const void *b)
{
	BlockNumber blkno_a = ((const PxIndexBufferItem *)a)->blkno;
	BlockNumber blkno_b = ((const PxIndexBufferItem *)b)->blkno;

	if (blkno_a == blkno_b)
		return 0;
	return (blkno_a > blkno_b) ? 1 : -1;
}

/* Check the write pages are valid. */
pg_attribute_unused()
static bool
px_bulk_write_page_valid(PxIndexPageBuffer *buffer, int start, int count)
{
	int i;
	char *page_write_start = (char *)buffer->item[start].page;
	for (i = 0; i < count; i++)
	{
		if ((char *)(buffer->item[start + i].page) != page_write_start)
			return false;
		page_write_start += BLCKSZ;
	}
	return true;
}

/*
 * POLAR: Use smgrbulkwrite to write buffer from start location to end location.
 * The caller should guarantee the buffer from start to end is continuous and without
 * empty pages.
 */
static void
_bt_px_bulkwrite(SMgrRelationData *rd_smgr, PxIndexPageBuffer *buffer, int start, int count)
{
	char *page_write = (char *)buffer->item[start].page;
	Assert(px_bulk_write_page_valid(buffer, start, count));
	polar_smgrbulkwrite(rd_smgr, MAIN_FORKNUM, buffer->item[start].blkno, count, page_write, true);
}

/*
 * POLAR: There are two locations when we use px flush btree leaf pages.
 * last_written_blkno: Last block number that we have written on the disk.
 * current_written_blkno: Current block number that we have written on the buffer_addr.
 * For all the buffer in buffer_addr, if the blkno is smaller than last_written_blkno, means
 * that this block has been written on the disk, might be a zero page. So we should just
 * overwrite it with the real content page. If the blkno is bigger than last_written_blkno,
 * means that this block has not been written on the disk, so we should just extend this page.
 * The differences between px index page overwrite and px index page extend is that, overwrite
 * page must be a valid page with real content. But extend page might be a hole, so we should
 * extend it as a zero page. This is the same process as _bt_blwritepage.
 */
static void
_bt_px_flush_blpage(BTWriteState *wstate)
{
	BlockNumber blkno_start, blkno_end;
	int max_blocknum_count, total_blocknum_count, count;
	int start = 0;
	int end = 0;
	PxIndexPageBuffer *buffer = wstate->px_index_buffer;
	SMgrRelationData *rd_smgr = wstate->index->rd_smgr;
	char *extend_buffer = NULL;
	if (buffer == NULL)
		return;
	Assert(buffer->item_len <= polar_bt_write_page_buffer_size);

	/* Sort it order by block number */
	qsort(buffer->item, buffer->item_len, sizeof(PxIndexBufferItem), px_index_buffer_compare);

	/*
	 * For block number smaller than last_writtten_blkno, we make continuous blocks bulk write together.
	 * It misses the final buffers bulk write.
	 */
	while (end + 1 < buffer->item_len && buffer->item[end + 1].blkno < buffer->last_written_blkno)
	{
		if (buffer->item[end + 1].blkno != buffer->item[end].blkno + 1)
		{
			count = end - start + 1;
			_bt_px_bulkwrite(rd_smgr, buffer, start, count);
			start = end + 1;
		}
		end++;
	}

	/*
	 * Deal with the final buffers bulk write.
	 * Two situations should be considered:
	 * 1. last_written_blkno has not been inited, so 'start' is the first block, it is the first
	 * time to call _bt_px_flush_blpage, all the buffers are bigger than last_written_blkno.
	 * 'start' is default set to 0, 'end' is set to buffer->item_len - 1, bulk write will be replaced by
	 * extend batch.
	 * 2. last_written_blkno has been inited
	 * 2.1 'start' is smaller than last_written_blkno, 'end' is smaller than last_written_blkno and
	 * 'end + 1' is bigger than last_written_blkno, we should deal with the final buffers bulk write
	 * from 'start' to 'end'
	 * 2.2 'start' is bigger than last_written_blkno, 'start' is default set to 0, 'end' is set to
	 * buffer->item_len - 1, bulk write will be replaced by extend batch.
	 */
	if (buffer->item[start].blkno < buffer->last_written_blkno)
	{
		count = end - start + 1;
		_bt_px_bulkwrite(rd_smgr, buffer, start, count);
		start += count;
	}
	end = buffer->item_len - 1;

	/*
	 * Record the start block number, if last_written_blkno has not been inited, it is 0, else
	 * it is last_written_blkno + 1
	 */
	blkno_start = (buffer->last_written_blkno == 0) ? 0 : (buffer->last_written_blkno + 1);
	blkno_end = buffer->item[end].blkno;
	total_blocknum_count = (int)(blkno_end - blkno_start + 1);
	while (total_blocknum_count > 0)
	{
		/* We must guarantee extend batch size is not over the max block number in this page */
		max_blocknum_count = Min(total_blocknum_count,
								(BlockNumber) RELSEG_SIZE - (blkno_start) % ((BlockNumber) RELSEG_SIZE));
		extend_buffer = (char *)palloc0(max_blocknum_count * BLCKSZ);
		while (start <= end && buffer->item[start].blkno < blkno_start + max_blocknum_count)
		{
			int offset = buffer->item[start].blkno - blkno_start;
			memcpy(extend_buffer + offset * BLCKSZ, buffer->item[start].page, BLCKSZ);
			start++;
		}
		smgrextendbatch(rd_smgr, MAIN_FORKNUM, blkno_start, max_blocknum_count, extend_buffer, true);
		pfree(extend_buffer);
		extend_buffer = NULL;
		total_blocknum_count -= max_blocknum_count;
		blkno_start += max_blocknum_count;
	}
	buffer->item_len = 0;
	buffer->last_written_blkno = buffer->current_written_blkno;
}

static void
_bt_px_alloc_page_buffer(PxIndexPageBuffer **px_index_buffer)
{
	int i;
	Size px_index_buffer_size;
	char *page_size_start = NULL;
	PxIndexPageBuffer *buffer = NULL;

	if (*px_index_buffer)
		return;

	px_index_buffer_size = sizeof(PxIndexPageBuffer) +
							polar_bt_write_page_buffer_size * sizeof(PxIndexBufferItem);
	px_index_buffer_size = PXBUILD_MAXALIGN(px_index_buffer_size);
	px_index_buffer_size += polar_bt_write_page_buffer_size * BLCKSZ;
	px_index_buffer_size = PXBUILD_MAXALIGN(px_index_buffer_size);

	if (posix_memalign((void **)px_index_buffer, PXBUILD_MAXIMUM_ALIGNOF, px_index_buffer_size) != 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
					errmsg("out of memory, posix_memalign alloc px_index_buffer failed")));
	}
	Assert(((uintptr_t)(*px_index_buffer) & (uintptr_t)(PXBUILD_MAXIMUM_ALIGNOF - 1)) == 0);

	buffer = (PxIndexPageBuffer *)(*px_index_buffer);
	memset(buffer, 0, px_index_buffer_size);
	page_size_start = (char *)(*px_index_buffer) +
						PXBUILD_MAXALIGN(sizeof(PxIndexPageBuffer) +
						polar_bt_write_page_buffer_size * sizeof(PxIndexBufferItem));

	for (i = 0; i < polar_bt_write_page_buffer_size; i++)
	{
		buffer->item[i].page = page_size_start;
		page_size_start += BLCKSZ;
	}
}

void
polar_px_enable_btbuild_precheck(bool isTopLevel, List *options)
{
	ListCell *cell = NULL;
	if (!px_enable_btbuild)
		return;

	foreach(cell, options)
	{
		DefElem *def = (DefElem *) lfirst(cell);
		if (strcmp(def->defname, "px_build") == 0 && strcmp(defGetString(def), "on") == 0)
		{
			if (IsInTransactionBlock(isTopLevel))
				elog(ERROR, "PX index build does not support in transaction block");
		}
	}
}
/* POLAR end */
