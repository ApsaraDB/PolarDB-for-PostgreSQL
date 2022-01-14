/*-------------------------------------------------------------------------
 *
 * nodeBitmapHeapscan.c
 *	  Routines to support bitmapped scans of relations
 *
 * NOTE: it is critical that this plan type only be used with MVCC-compliant
 * snapshots (ie, regular snapshots, not SnapshotAny or one of the other
 * special snapshots).  The reason is that since index and heap scans are
 * decoupled, there can be no assurance that the index tuple prompting a
 * visit to a particular heap TID still exists when the visit is made.
 * Therefore the tuple might not exist anymore either (which is OK because
 * heap_fetch will cope) --- but worse, the tuple slot could have been
 * re-used for a newer tuple.  With an MVCC snapshot the newer tuple is
 * certain to fail the time qual and so it will not be mistakenly returned,
 * but with anything else we might return a tuple that doesn't meet the
 * required index qual conditions.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeBitmapHeapscan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecBitmapHeapScan			scans a relation using bitmap info
 *		ExecBitmapHeapNext			workhorse for above
 *		ExecInitBitmapHeapScan		creates and initializes state info.
 *		ExecReScanBitmapHeapScan	prepares to rescan the plan.
 *		ExecEndBitmapHeapScan		releases all storage.
 */
#include "postgres.h"

#include <math.h>

#include "access/relscan.h"
#include "access/transam.h"
#include "access/visibilitymap.h"
#include "executor/execdebug.h"
#include "executor/nodeBitmapHeapscan.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/predicate.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/spccache.h"
#include "utils/snapmgr.h"
#include "utils/tqual.h"


static TupleTableSlot *BitmapHeapNext(BitmapHeapScanState *node);
static void bitgetpage(HeapScanDesc scan, TBMIterateResult *tbmres);
static inline void BitmapDoneInitializingSharedState(
								  ParallelBitmapHeapState *pstate);
static inline void BitmapAdjustPrefetchIterator(BitmapHeapScanState *node,
							 TBMIterateResult *tbmres);
static inline void BitmapAdjustPrefetchTarget(BitmapHeapScanState *node);
static inline void BitmapPrefetch(BitmapHeapScanState *node,
			   HeapScanDesc scan);
static bool BitmapShouldInitializeSharedState(
								  ParallelBitmapHeapState *pstate);

/* ----------------------------------------------------------------
 *		BitmapHeapNext
 *
 *		Retrieve next tuple from the BitmapHeapScan node's currentRelation
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
BitmapHeapNext(BitmapHeapScanState *node)
{
	ExprContext *econtext;
	HeapScanDesc scan;
	TIDBitmap  *tbm;
	TBMIterator *tbmiterator = NULL;
	TBMSharedIterator *shared_tbmiterator = NULL;
	TBMIterateResult *tbmres;
	OffsetNumber targoffset;
	TupleTableSlot *slot;
	ParallelBitmapHeapState *pstate = node->pstate;
	dsa_area   *dsa = node->ss.ps.state->es_query_dsa;

	/*
	 * extract necessary information from index scan node
	 */
	econtext = node->ss.ps.ps_ExprContext;
	slot = node->ss.ss_ScanTupleSlot;
	scan = node->ss.ss_currentScanDesc;
	tbm = node->tbm;
	if (pstate == NULL)
		tbmiterator = node->tbmiterator;
	else
		shared_tbmiterator = node->shared_tbmiterator;
	tbmres = node->tbmres;

	/*
	 * If we haven't yet performed the underlying index scan, do it, and begin
	 * the iteration over the bitmap.
	 *
	 * For prefetching, we use *two* iterators, one for the pages we are
	 * actually scanning and another that runs ahead of the first for
	 * prefetching.  node->prefetch_pages tracks exactly how many pages ahead
	 * the prefetch iterator is.  Also, node->prefetch_target tracks the
	 * desired prefetch distance, which starts small and increases up to the
	 * node->prefetch_maximum.  This is to avoid doing a lot of prefetching in
	 * a scan that stops after a few tuples because of a LIMIT.
	 */
	if (!node->initialized)
	{
		if (!pstate)
		{
			tbm = (TIDBitmap *) MultiExecProcNode(outerPlanState(node));

			if (!tbm || !IsA(tbm, TIDBitmap))
				elog(ERROR, "unrecognized result from subplan");

			node->tbm = tbm;
			node->tbmiterator = tbmiterator = tbm_begin_iterate(tbm);
			node->tbmres = tbmres = NULL;

#ifdef USE_PREFETCH
			if (node->prefetch_maximum > 0)
			{
				node->prefetch_iterator = tbm_begin_iterate(tbm);
				node->prefetch_pages = 0;
				node->prefetch_target = -1;
			}
#endif							/* USE_PREFETCH */
		}
		else
		{
			/*
			 * The leader will immediately come out of the function, but
			 * others will be blocked until leader populates the TBM and wakes
			 * them up.
			 */
			if (BitmapShouldInitializeSharedState(pstate))
			{
				tbm = (TIDBitmap *) MultiExecProcNode(outerPlanState(node));
				if (!tbm || !IsA(tbm, TIDBitmap))
					elog(ERROR, "unrecognized result from subplan");

				node->tbm = tbm;

				/*
				 * Prepare to iterate over the TBM. This will return the
				 * dsa_pointer of the iterator state which will be used by
				 * multiple processes to iterate jointly.
				 */
				pstate->tbmiterator = tbm_prepare_shared_iterate(tbm);
#ifdef USE_PREFETCH
				if (node->prefetch_maximum > 0)
				{
					pstate->prefetch_iterator =
						tbm_prepare_shared_iterate(tbm);

					/*
					 * We don't need the mutex here as we haven't yet woke up
					 * others.
					 */
					pstate->prefetch_pages = 0;
					pstate->prefetch_target = -1;
				}
#endif

				/* We have initialized the shared state so wake up others. */
				BitmapDoneInitializingSharedState(pstate);
			}

			/* Allocate a private iterator and attach the shared state to it */
			node->shared_tbmiterator = shared_tbmiterator =
				tbm_attach_shared_iterate(dsa, pstate->tbmiterator);
			node->tbmres = tbmres = NULL;

#ifdef USE_PREFETCH
			if (node->prefetch_maximum > 0)
			{
				node->shared_prefetch_iterator =
					tbm_attach_shared_iterate(dsa, pstate->prefetch_iterator);
			}
#endif							/* USE_PREFETCH */
		}
		node->initialized = true;
	}

	for (;;)
	{
		Page		dp;
		ItemId		lp;

		CHECK_FOR_INTERRUPTS();

		/*
		 * Get next page of results if needed
		 */
		if (tbmres == NULL)
		{
			if (!pstate)
				node->tbmres = tbmres = tbm_iterate(tbmiterator);
			else
				node->tbmres = tbmres = tbm_shared_iterate(shared_tbmiterator);
			if (tbmres == NULL)
			{
				/* no more entries in the bitmap */
				break;
			}

			BitmapAdjustPrefetchIterator(node, tbmres);

			/*
			 * Ignore any claimed entries past what we think is the end of the
			 * relation.  (This is probably not necessary given that we got at
			 * least AccessShareLock on the table before performing any of the
			 * indexscans, but let's be safe.)
			 */
			if (tbmres->blockno >= scan->rs_nblocks)
			{
				node->tbmres = tbmres = NULL;
				continue;
			}

			/*
			 * We can skip fetching the heap page if we don't need any fields
			 * from the heap, and the bitmap entries don't need rechecking,
			 * and all tuples on the page are visible to our transaction.
			 */
			node->skip_fetch = (node->can_skip_fetch &&
								!tbmres->recheck &&
								VM_ALL_VISIBLE(node->ss.ss_currentRelation,
											   tbmres->blockno,
											   &node->vmbuffer));
			/* POLAR px : do not scan other's page/tuples */
			if (NULL == scan->px_scan)
			{
				if (node->skip_fetch)
				{
					/*
					* The number of tuples on this page is put into
					* scan->rs_ntuples; note we don't fill scan->rs_vistuples.
					*/
					scan->rs_ntuples = tbmres->ntuples;
				}
				else
				{
					/*
				 	* Fetch the current heap page and identify candidate tuples.
				 	*/
					bitgetpage(scan, tbmres);
				}
			}
			else
			{
				BlockNumber unitno = PXSCAN_BlockNum2UnitNum(tbmres->blockno);
				if ((unitno % scan->px_scan->pxs_total_workers) != scan->px_scan->pxs_worker_id)
				{
					scan->rs_ntuples = 0;
				}
				else
				{
					if (node->skip_fetch)
					{
						scan->rs_ntuples = tbmres->ntuples;
					}
					else
					{
						bitgetpage(scan, tbmres);
					}	
				}
			}

			if (tbmres->ntuples >= 0)
				node->exact_pages++;
			else
				node->lossy_pages++;

			/*
			 * Set rs_cindex to first slot to examine
			 */
			scan->rs_cindex = 0;

			/* Adjust the prefetch target */
			BitmapAdjustPrefetchTarget(node);
		}
		else
		{
			/*
			 * Continuing in previously obtained page; advance rs_cindex
			 */
			scan->rs_cindex++;

#ifdef USE_PREFETCH

			/*
			 * Try to prefetch at least a few pages even before we get to the
			 * second page if we don't stop reading after the first tuple.
			 */
			if (!pstate)
			{
				if (node->prefetch_target < node->prefetch_maximum)
					node->prefetch_target++;
			}
			else if (pstate->prefetch_target < node->prefetch_maximum)
			{
				/* take spinlock while updating shared state */
				SpinLockAcquire(&pstate->mutex);
				if (pstate->prefetch_target < node->prefetch_maximum)
					pstate->prefetch_target++;
				SpinLockRelease(&pstate->mutex);
			}
#endif							/* USE_PREFETCH */
		}

		/*
		 * Out of range?  If so, nothing more to look at on this page
		 */
		if (scan->rs_cindex < 0 || scan->rs_cindex >= scan->rs_ntuples)
		{
			node->tbmres = tbmres = NULL;
			continue;
		}

		/*
		 * We issue prefetch requests *after* fetching the current page to try
		 * to avoid having prefetching interfere with the main I/O. Also, this
		 * should happen only when we have determined there is still something
		 * to do on the current page, else we may uselessly prefetch the same
		 * page we are just about to request for real.
		 */
		BitmapPrefetch(node, scan);

		if (node->skip_fetch)
		{
			/*
			 * If we don't have to fetch the tuple, just return nulls.
			 */
			ExecStoreAllNullTuple(slot);
		}
		else
		{
			/*
			 * Okay to fetch the tuple.
			 */
			targoffset = scan->rs_vistuples[scan->rs_cindex];
			dp = (Page) BufferGetPage(scan->rs_cbuf);
			lp = PageGetItemId(dp, targoffset);
			Assert(ItemIdIsNormal(lp));

			scan->rs_ctup.t_data = (HeapTupleHeader) PageGetItem((Page) dp, lp);
			scan->rs_ctup.t_len = ItemIdGetLength(lp);
			scan->rs_ctup.t_tableOid = scan->rs_rd->rd_id;
			ItemPointerSet(&scan->rs_ctup.t_self, tbmres->blockno, targoffset);

			pgstat_count_heap_fetch(scan->rs_rd);

			/*
			 * Set up the result slot to point to this tuple.  Note that the
			 * slot acquires a pin on the buffer.
			 */
			ExecStoreTuple(&scan->rs_ctup,
						   slot,
						   scan->rs_cbuf,
						   false);

			/*
			 * If we are using lossy info, we have to recheck the qual
			 * conditions at every tuple.
			 */
			if (tbmres->recheck)
			{
				econtext->ecxt_scantuple = slot;
				if (!ExecQualAndReset(node->bitmapqualorig, econtext))
				{
					/* Fails recheck, so drop it and loop back for another */
					InstrCountFiltered2(node, 1);
					ExecClearTuple(slot);
					continue;
				}
			}
		}

		/* OK to return this tuple */
		return slot;
	}

	/*
	 * if we get here it means we are at the end of the scan..
	 */
	return ExecClearTuple(slot);
}

/*
 * bitgetpage - subroutine for BitmapHeapNext()
 *
 * This routine reads and pins the specified page of the relation, then
 * builds an array indicating which tuples on the page are both potentially
 * interesting according to the bitmap, and visible according to the snapshot.
 */
static void
bitgetpage(HeapScanDesc scan, TBMIterateResult *tbmres)
{
	BlockNumber page = tbmres->blockno;
	Buffer		buffer;
	Snapshot	snapshot;
	int			ntup;

	/*
	 * Acquire pin on the target heap page, trading in any pin we held before.
	 */
	Assert(page < scan->rs_nblocks);

	scan->rs_cbuf = ReleaseAndReadBuffer(scan->rs_cbuf,
										 scan->rs_rd,
										 page);
	buffer = scan->rs_cbuf;
	snapshot = scan->rs_snapshot;

	ntup = 0;

	/*
	 * Prune and repair fragmentation for the whole page, if possible.
	 */
	heap_page_prune_opt(scan->rs_rd, buffer);

	/*
	 * We must hold share lock on the buffer content while examining tuple
	 * visibility.  Afterwards, however, the tuples we have found to be
	 * visible are guaranteed good as long as we hold the buffer pin.
	 */
	LockBuffer(buffer, BUFFER_LOCK_SHARE);

	/*
	 * We need two separate strategies for lossy and non-lossy cases.
	 */
	if (tbmres->ntuples >= 0)
	{
		/*
		 * Bitmap is non-lossy, so we just look through the offsets listed in
		 * tbmres; but we have to follow any HOT chain starting at each such
		 * offset.
		 */
		int			curslot;

		for (curslot = 0; curslot < tbmres->ntuples; curslot++)
		{
			OffsetNumber offnum = tbmres->offsets[curslot];
			ItemPointerData tid;
			HeapTupleData heapTuple;

			ItemPointerSet(&tid, page, offnum);
			if (heap_hot_search_buffer(&tid, scan->rs_rd, buffer, snapshot,
									   &heapTuple, NULL, true))
				scan->rs_vistuples[ntup++] = ItemPointerGetOffsetNumber(&tid);
		}
	}
	else
	{
		/*
		 * Bitmap is lossy, so we must examine each item pointer on the page.
		 * But we can ignore HOT chains, since we'll check each tuple anyway.
		 */
		Page		dp = (Page) BufferGetPage(buffer);
		OffsetNumber maxoff = PageGetMaxOffsetNumber(dp);
		OffsetNumber offnum;

		for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum = OffsetNumberNext(offnum))
		{
			ItemId		lp;
			HeapTupleData loctup;
			bool		valid;

			lp = PageGetItemId(dp, offnum);
			if (!ItemIdIsNormal(lp))
				continue;
			loctup.t_data = (HeapTupleHeader) PageGetItem((Page) dp, lp);
			loctup.t_len = ItemIdGetLength(lp);
			loctup.t_tableOid = scan->rs_rd->rd_id;
			ItemPointerSet(&loctup.t_self, page, offnum);
			valid = HeapTupleSatisfiesVisibility(&loctup, snapshot, buffer);
			if (valid)
			{
				scan->rs_vistuples[ntup++] = offnum;
				PredicateLockTuple(scan->rs_rd, &loctup, snapshot);
			}
			CheckForSerializableConflictOut(valid, scan->rs_rd, &loctup,
											buffer, snapshot);
		}
	}

	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

	Assert(ntup <= MaxHeapTuplesPerPage);
	scan->rs_ntuples = ntup;
}

/*
 *	BitmapDoneInitializingSharedState - Shared state is initialized
 *
 *	By this time the leader has already populated the TBM and initialized the
 *	shared state so wake up other processes.
 */
static inline void
BitmapDoneInitializingSharedState(ParallelBitmapHeapState *pstate)
{
	SpinLockAcquire(&pstate->mutex);
	pstate->state = BM_FINISHED;
	SpinLockRelease(&pstate->mutex);
	ConditionVariableBroadcast(&pstate->cv);
}

/*
 *	BitmapAdjustPrefetchIterator - Adjust the prefetch iterator
 */
static inline void
BitmapAdjustPrefetchIterator(BitmapHeapScanState *node,
							 TBMIterateResult *tbmres)
{
#ifdef USE_PREFETCH
	ParallelBitmapHeapState *pstate = node->pstate;

	if (pstate == NULL)
	{
		TBMIterator *prefetch_iterator = node->prefetch_iterator;

		if (node->prefetch_pages > 0)
		{
			/* The main iterator has closed the distance by one page */
			node->prefetch_pages--;
		}
		else if (prefetch_iterator)
		{
			/* Do not let the prefetch iterator get behind the main one */
			TBMIterateResult *tbmpre = tbm_iterate(prefetch_iterator);

			if (tbmpre == NULL || tbmpre->blockno != tbmres->blockno)
				elog(ERROR, "prefetch and main iterators are out of sync");
		}
		return;
	}

	if (node->prefetch_maximum > 0)
	{
		TBMSharedIterator *prefetch_iterator = node->shared_prefetch_iterator;

		SpinLockAcquire(&pstate->mutex);
		if (pstate->prefetch_pages > 0)
		{
			pstate->prefetch_pages--;
			SpinLockRelease(&pstate->mutex);
		}
		else
		{
			/* Release the mutex before iterating */
			SpinLockRelease(&pstate->mutex);

			/*
			 * In case of shared mode, we can not ensure that the current
			 * blockno of the main iterator and that of the prefetch iterator
			 * are same.  It's possible that whatever blockno we are
			 * prefetching will be processed by another process.  Therefore,
			 * we don't validate the blockno here as we do in non-parallel
			 * case.
			 */
			if (prefetch_iterator)
				tbm_shared_iterate(prefetch_iterator);
		}
	}
#endif							/* USE_PREFETCH */
}

/*
 * BitmapAdjustPrefetchTarget - Adjust the prefetch target
 *
 * Increase prefetch target if it's not yet at the max.  Note that
 * we will increase it to zero after fetching the very first
 * page/tuple, then to one after the second tuple is fetched, then
 * it doubles as later pages are fetched.
 */
static inline void
BitmapAdjustPrefetchTarget(BitmapHeapScanState *node)
{
#ifdef USE_PREFETCH
	ParallelBitmapHeapState *pstate = node->pstate;

	if (pstate == NULL)
	{
		if (node->prefetch_target >= node->prefetch_maximum)
			 /* don't increase any further */ ;
		else if (node->prefetch_target >= node->prefetch_maximum / 2)
			node->prefetch_target = node->prefetch_maximum;
		else if (node->prefetch_target > 0)
			node->prefetch_target *= 2;
		else
			node->prefetch_target++;
		return;
	}

	/* Do an unlocked check first to save spinlock acquisitions. */
	if (pstate->prefetch_target < node->prefetch_maximum)
	{
		SpinLockAcquire(&pstate->mutex);
		if (pstate->prefetch_target >= node->prefetch_maximum)
			 /* don't increase any further */ ;
		else if (pstate->prefetch_target >= node->prefetch_maximum / 2)
			pstate->prefetch_target = node->prefetch_maximum;
		else if (pstate->prefetch_target > 0)
			pstate->prefetch_target *= 2;
		else
			pstate->prefetch_target++;
		SpinLockRelease(&pstate->mutex);
	}
#endif							/* USE_PREFETCH */
}

/*
 * BitmapPrefetch - Prefetch, if prefetch_pages are behind prefetch_target
 */
static inline void
BitmapPrefetch(BitmapHeapScanState *node, HeapScanDesc scan)
{
#ifdef USE_PREFETCH
	ParallelBitmapHeapState *pstate = node->pstate;

	if (pstate == NULL)
	{
		TBMIterator *prefetch_iterator = node->prefetch_iterator;

		if (prefetch_iterator)
		{
			while (node->prefetch_pages < node->prefetch_target)
			{
				TBMIterateResult *tbmpre = tbm_iterate(prefetch_iterator);
				bool		skip_fetch;

				if (tbmpre == NULL)
				{
					/* No more pages to prefetch */
					tbm_end_iterate(prefetch_iterator);
					node->prefetch_iterator = NULL;
					break;
				}
				node->prefetch_pages++;

				/*
				 * If we expect not to have to actually read this heap page,
				 * skip this prefetch call, but continue to run the prefetch
				 * logic normally.  (Would it be better not to increment
				 * prefetch_pages?)
				 *
				 * This depends on the assumption that the index AM will
				 * report the same recheck flag for this future heap page as
				 * it did for the current heap page; which is not a certainty
				 * but is true in many cases.
				 */
				skip_fetch = (node->can_skip_fetch &&
							  (node->tbmres ? !node->tbmres->recheck : false) &&
							  VM_ALL_VISIBLE(node->ss.ss_currentRelation,
											 tbmpre->blockno,
											 &node->pvmbuffer));

				if (!skip_fetch)
					PrefetchBuffer(scan->rs_rd, MAIN_FORKNUM, tbmpre->blockno);
			}
		}

		return;
	}

	if (pstate->prefetch_pages < pstate->prefetch_target)
	{
		TBMSharedIterator *prefetch_iterator = node->shared_prefetch_iterator;

		if (prefetch_iterator)
		{
			while (1)
			{
				TBMIterateResult *tbmpre;
				bool		do_prefetch = false;
				bool		skip_fetch;

				/*
				 * Recheck under the mutex. If some other process has already
				 * done enough prefetching then we need not to do anything.
				 */
				SpinLockAcquire(&pstate->mutex);
				if (pstate->prefetch_pages < pstate->prefetch_target)
				{
					pstate->prefetch_pages++;
					do_prefetch = true;
				}
				SpinLockRelease(&pstate->mutex);

				if (!do_prefetch)
					return;

				tbmpre = tbm_shared_iterate(prefetch_iterator);
				if (tbmpre == NULL)
				{
					/* No more pages to prefetch */
					tbm_end_shared_iterate(prefetch_iterator);
					node->shared_prefetch_iterator = NULL;
					break;
				}

				/* As above, skip prefetch if we expect not to need page */
				skip_fetch = (node->can_skip_fetch &&
							  (node->tbmres ? !node->tbmres->recheck : false) &&
							  VM_ALL_VISIBLE(node->ss.ss_currentRelation,
											 tbmpre->blockno,
											 &node->pvmbuffer));

				if (!skip_fetch)
					PrefetchBuffer(scan->rs_rd, MAIN_FORKNUM, tbmpre->blockno);
			}
		}
	}
#endif							/* USE_PREFETCH */
}

/*
 * BitmapHeapRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool
BitmapHeapRecheck(BitmapHeapScanState *node, TupleTableSlot *slot)
{
	ExprContext *econtext;

	/*
	 * extract necessary information from index scan node
	 */
	econtext = node->ss.ps.ps_ExprContext;

	/* Does the tuple meet the original qual conditions? */
	econtext->ecxt_scantuple = slot;
	return ExecQualAndReset(node->bitmapqualorig, econtext);
}

/* ----------------------------------------------------------------
 *		ExecBitmapHeapScan(node)
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecBitmapHeapScan(PlanState *pstate)
{
	BitmapHeapScanState *node = castNode(BitmapHeapScanState, pstate);

	return ExecScan(&node->ss,
					(ExecScanAccessMtd) BitmapHeapNext,
					(ExecScanRecheckMtd) BitmapHeapRecheck);
}

/* ----------------------------------------------------------------
 *		ExecReScanBitmapHeapScan(node)
 * ----------------------------------------------------------------
 */
void
ExecReScanBitmapHeapScan(BitmapHeapScanState *node)
{
	PlanState  *outerPlan = outerPlanState(node);

	/* rescan to release any page pin */
	heap_rescan(node->ss.ss_currentScanDesc, NULL);

	/* release bitmaps and buffers if any */
	if (node->tbmiterator)
		tbm_end_iterate(node->tbmiterator);
	if (node->prefetch_iterator)
		tbm_end_iterate(node->prefetch_iterator);
	if (node->shared_tbmiterator)
		tbm_end_shared_iterate(node->shared_tbmiterator);
	if (node->shared_prefetch_iterator)
		tbm_end_shared_iterate(node->shared_prefetch_iterator);
	if (node->tbm)
		tbm_free(node->tbm);
	if (node->vmbuffer != InvalidBuffer)
		ReleaseBuffer(node->vmbuffer);
	if (node->pvmbuffer != InvalidBuffer)
		ReleaseBuffer(node->pvmbuffer);
	node->tbm = NULL;
	node->tbmiterator = NULL;
	node->tbmres = NULL;
	node->prefetch_iterator = NULL;
	node->initialized = false;
	node->shared_tbmiterator = NULL;
	node->shared_prefetch_iterator = NULL;
	node->vmbuffer = InvalidBuffer;
	node->pvmbuffer = InvalidBuffer;

	ExecScanReScan(&node->ss);

	/*
	 * if chgParam of subnode is not null then plan will be re-scanned by
	 * first ExecProcNode.
	 */
	if (outerPlan->chgParam == NULL)
		ExecReScan(outerPlan);
}

/* ----------------------------------------------------------------
 *		ExecEndBitmapHeapScan
 * ----------------------------------------------------------------
 */
void
ExecEndBitmapHeapScan(BitmapHeapScanState *node)
{
	Relation	relation;
	HeapScanDesc scanDesc;

	/*
	 * extract information from the node
	 */
	relation = node->ss.ss_currentRelation;
	scanDesc = node->ss.ss_currentScanDesc;

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->ss.ps);

	/*
	 * clear out tuple table slots
	 */
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	/*
	 * close down subplans
	 */
	ExecEndNode(outerPlanState(node));

	/*
	 * release bitmaps and buffers if any
	 */
	if (node->tbmiterator)
		tbm_end_iterate(node->tbmiterator);
	if (node->prefetch_iterator)
		tbm_end_iterate(node->prefetch_iterator);
	if (node->tbm)
		tbm_free(node->tbm);
	if (node->shared_tbmiterator)
		tbm_end_shared_iterate(node->shared_tbmiterator);
	if (node->shared_prefetch_iterator)
		tbm_end_shared_iterate(node->shared_prefetch_iterator);
	if (node->vmbuffer != InvalidBuffer)
		ReleaseBuffer(node->vmbuffer);
	if (node->pvmbuffer != InvalidBuffer)
		ReleaseBuffer(node->pvmbuffer);

	/*
	 * close heap scan
	 */
	heap_endscan(scanDesc);

	/*
	 * close the heap relation.
	 */
	ExecCloseScanRelation(relation);
}

/* ----------------------------------------------------------------
 *		ExecInitBitmapHeapScan
 *
 *		Initializes the scan's state information.
 * ----------------------------------------------------------------
 */
BitmapHeapScanState *
ExecInitBitmapHeapScan(BitmapHeapScan *node, EState *estate, int eflags)
{
	BitmapHeapScanState *scanstate;
	Relation	currentRelation;
	int			io_concurrency;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * Assert caller didn't ask for an unsafe snapshot --- see comments at
	 * head of file.
	 */
	Assert(IsMVCCSnapshot(estate->es_snapshot));

	/*
	 * create state structure
	 */
	scanstate = makeNode(BitmapHeapScanState);
	scanstate->ss.ps.plan = (Plan *) node;
	scanstate->ss.ps.state = estate;
	scanstate->ss.ps.ExecProcNode = ExecBitmapHeapScan;

	scanstate->tbm = NULL;
	scanstate->tbmiterator = NULL;
	scanstate->tbmres = NULL;
	scanstate->skip_fetch = false;
	scanstate->vmbuffer = InvalidBuffer;
	scanstate->pvmbuffer = InvalidBuffer;
	scanstate->exact_pages = 0;
	scanstate->lossy_pages = 0;
	scanstate->prefetch_iterator = NULL;
	scanstate->prefetch_pages = 0;
	scanstate->prefetch_target = 0;
	/* may be updated below */
	scanstate->prefetch_maximum = target_prefetch_pages;
	scanstate->pscan_len = 0;
	scanstate->initialized = false;
	scanstate->shared_tbmiterator = NULL;
	scanstate->shared_prefetch_iterator = NULL;
	scanstate->pstate = NULL;

	/*
	 * We can potentially skip fetching heap pages if we do not need any
	 * columns of the table, either for checking non-indexable quals or for
	 * returning data.  This test is a bit simplistic, as it checks the
	 * stronger condition that there's no qual or return tlist at all.  But in
	 * most cases it's probably not worth working harder than that.
	 */
	scanstate->can_skip_fetch = (node->scan.plan.qual == NIL &&
								 node->scan.plan.targetlist == NIL);

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &scanstate->ss.ps);

	/*
	 * open the base relation and acquire appropriate lock on it.
	 */
	currentRelation = ExecOpenScanRelation(estate, node->scan.scanrelid, eflags);

	/*
	 * initialize child nodes
	 *
	 * We do this after ExecOpenScanRelation because the child nodes will open
	 * indexscans on our relation's indexes, and we want to be sure we have
	 * acquired a lock on the relation first.
	 */
	outerPlanState(scanstate) = ExecInitNode(outerPlan(node), estate, eflags);

	/*
	 * get the scan type from the relation descriptor.
	 */
	ExecInitScanTupleSlot(estate, &scanstate->ss,
						  RelationGetDescr(currentRelation));


	/*
	 * Initialize result slot, type and projection.
	 */
	ExecInitResultTupleSlotTL(estate, &scanstate->ss.ps);
	ExecAssignScanProjectionInfo(&scanstate->ss);

	/*
	 * initialize child expressions
	 */
	scanstate->ss.ps.qual =
		ExecInitQual(node->scan.plan.qual, (PlanState *) scanstate);
	scanstate->bitmapqualorig =
		ExecInitQual(node->bitmapqualorig, (PlanState *) scanstate);

	/*
	 * Determine the maximum for prefetch_target.  If the tablespace has a
	 * specific IO concurrency set, use that to compute the corresponding
	 * maximum value; otherwise, we already initialized to the value computed
	 * by the GUC machinery.
	 */
	io_concurrency =
		get_tablespace_io_concurrency(currentRelation->rd_rel->reltablespace);
	if (io_concurrency != effective_io_concurrency)
	{
		double		maximum;

		if (ComputeIoConcurrency(io_concurrency, &maximum))
			scanstate->prefetch_maximum = rint(maximum);
	}

	scanstate->ss.ss_currentRelation = currentRelation;

	/*
	 * Even though we aren't going to do a conventional seqscan, it is useful
	 * to create a HeapScanDesc --- most of the fields in it are usable.
	 */
	/* POLAR px */
	if (px_role == PX_ROLE_PX &&
		px_is_executing &&
		scanstate->ss.ps.plan->px_scan_partial)
	{
		scanstate->ss.ss_currentScanDesc = heap_beginscan_bm_px(currentRelation,
														 estate->es_snapshot,
														 0,
														 NULL);
	}
	else
	{
		scanstate->ss.ss_currentScanDesc = heap_beginscan_bm(currentRelation,
														 estate->es_snapshot,
														 0,
														 NULL);
	}
	/* POLAR end */
	

	/*
	 * all done.
	 */
	return scanstate;
}

/*----------------
 *		BitmapShouldInitializeSharedState
 *
 *		The first process to come here and see the state to the BM_INITIAL
 *		will become the leader for the parallel bitmap scan and will be
 *		responsible for populating the TIDBitmap.  The other processes will
 *		be blocked by the condition variable until the leader wakes them up.
 * ---------------
 */
static bool
BitmapShouldInitializeSharedState(ParallelBitmapHeapState *pstate)
{
	SharedBitmapState state;

	while (1)
	{
		SpinLockAcquire(&pstate->mutex);
		state = pstate->state;
		if (pstate->state == BM_INITIAL)
			pstate->state = BM_INPROGRESS;
		SpinLockRelease(&pstate->mutex);

		/* Exit if bitmap is done, or if we're the leader. */
		if (state != BM_INPROGRESS)
			break;

		/* Wait for the leader to wake us up. */
		ConditionVariableSleep(&pstate->cv, WAIT_EVENT_PARALLEL_BITMAP_SCAN);
	}

	ConditionVariableCancelSleep();

	return (state == BM_INITIAL);
}

/* ----------------------------------------------------------------
 *		ExecBitmapHeapEstimate
 *
 *		Compute the amount of space we'll need in the parallel
 *		query DSM, and inform pcxt->estimator about our needs.
 * ----------------------------------------------------------------
 */
void
ExecBitmapHeapEstimate(BitmapHeapScanState *node,
					   ParallelContext *pcxt)
{
	EState	   *estate = node->ss.ps.state;

	node->pscan_len = add_size(offsetof(ParallelBitmapHeapState,
										phs_snapshot_data),
							   EstimateSnapshotSpace(estate->es_snapshot));

	shm_toc_estimate_chunk(&pcxt->estimator, node->pscan_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}

/* ----------------------------------------------------------------
 *		ExecBitmapHeapInitializeDSM
 *
 *		Set up a parallel bitmap heap scan descriptor.
 * ----------------------------------------------------------------
 */
void
ExecBitmapHeapInitializeDSM(BitmapHeapScanState *node,
							ParallelContext *pcxt)
{
	ParallelBitmapHeapState *pstate;
	EState	   *estate = node->ss.ps.state;
	dsa_area   *dsa = node->ss.ps.state->es_query_dsa;

	/* If there's no DSA, there are no workers; initialize nothing. */
	if (dsa == NULL)
		return;

	pstate = shm_toc_allocate(pcxt->toc, node->pscan_len);

	pstate->tbmiterator = 0;
	pstate->prefetch_iterator = 0;

	/* Initialize the mutex */
	SpinLockInit(&pstate->mutex);
	pstate->prefetch_pages = 0;
	pstate->prefetch_target = 0;
	pstate->state = BM_INITIAL;

	ConditionVariableInit(&pstate->cv);
	SerializeSnapshot(estate->es_snapshot, pstate->phs_snapshot_data);

	shm_toc_insert(pcxt->toc, node->ss.ps.plan->plan_node_id, pstate);
	node->pstate = pstate;
}

/* ----------------------------------------------------------------
 *		ExecBitmapHeapReInitializeDSM
 *
 *		Reset shared state before beginning a fresh scan.
 * ----------------------------------------------------------------
 */
void
ExecBitmapHeapReInitializeDSM(BitmapHeapScanState *node,
							  ParallelContext *pcxt)
{
	ParallelBitmapHeapState *pstate = node->pstate;
	dsa_area   *dsa = node->ss.ps.state->es_query_dsa;

	/* If there's no DSA, there are no workers; do nothing. */
	if (dsa == NULL)
		return;

	pstate->state = BM_INITIAL;

	if (DsaPointerIsValid(pstate->tbmiterator))
		tbm_free_shared_area(dsa, pstate->tbmiterator);

	if (DsaPointerIsValid(pstate->prefetch_iterator))
		tbm_free_shared_area(dsa, pstate->prefetch_iterator);

	pstate->tbmiterator = InvalidDsaPointer;
	pstate->prefetch_iterator = InvalidDsaPointer;
}

/* ----------------------------------------------------------------
 *		ExecBitmapHeapInitializeWorker
 *
 *		Copy relevant information from TOC into planstate.
 * ----------------------------------------------------------------
 */
void
ExecBitmapHeapInitializeWorker(BitmapHeapScanState *node,
							   ParallelWorkerContext *pwcxt)
{
	ParallelBitmapHeapState *pstate;
	Snapshot	snapshot;

	Assert(node->ss.ps.state->es_query_dsa != NULL);

	pstate = shm_toc_lookup(pwcxt->toc, node->ss.ps.plan->plan_node_id, false);
	node->pstate = pstate;

	snapshot = RestoreSnapshot(pstate->phs_snapshot_data);
	heap_update_snapshot(node->ss.ss_currentScanDesc, snapshot);
}
