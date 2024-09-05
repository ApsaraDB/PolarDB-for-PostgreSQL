/*-------------------------------------------------------------------------
 *
 * polar_heap2_idx.c
 *   Implementation of parse heap2 records.
 *
 * Portions Copyright (c) 2021, Alibaba Group Holding limited
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *   src/backend/access/logindex/polar_heap2_idx.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bufmask.h"
#include "access/heapam.h"
#include "access/heapam_xlog.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/polar_logindex_redo.h"
#include "access/visibilitymap.h"
#include "access/xlog.h"
#include "miscadmin.h"
#include "storage/buf_internals.h"
#include "storage/freespace.h"
#include "storage/standby.h"

static XLogRedoAction
polar_heap_clear_vm(XLogReaderState *record, RelFileNode *rnode,
					BlockNumber heapBlk, Buffer *buffer, uint8 flags)
{
	int			mapByte = HEAPBLK_TO_MAPBYTE(heapBlk);
	int			mapOffset = HEAPBLK_TO_OFFSET(heapBlk);
	uint8		mask = flags << mapOffset;
	char	   *map;
	Page		page;

	if (!BufferIsValid(*buffer))
	{
		Relation	reln = CreateFakeRelcacheEntry(*rnode);

		visibilitymap_pin(reln, heapBlk, buffer);
		FreeFakeRelcacheEntry(reln);
		LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);
	}

	page = BufferGetPage(*buffer);
	if (PageIsNew(page))
	{
		PageInit(page, BLCKSZ, 0);
		POLAR_LOG_REDO_INFO(page, record);
	}

	map = PageGetContents(page);

	if (map[mapByte] & mask)
		map[mapByte] &= ~mask;

	if (PageGetLSN(page) < record->EndRecPtr)
		PageSetLSN(page, record->EndRecPtr);

	return BLK_NEEDS_REDO;
}

static void
polar_heap_insert_save(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	xl_heap_insert *xlrec = (xl_heap_insert *) XLogRecGetData(record);

	if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
		polar_logindex_save_vm_block(instance, record, 0);

	polar_logindex_save_block(instance, record, 0);
}

static void
polar_heap_insert_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	xl_heap_insert *xlrec = (xl_heap_insert *) XLogRecGetData(record);

	if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
		polar_logindex_redo_vm_parse(instance, record, 0);

	polar_logindex_redo_parse(instance, record, 0);
}

static void
polar_heap_multi_insert_save(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	xl_heap_multi_insert *xlrec = (xl_heap_multi_insert *) XLogRecGetData(record);

	if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
		polar_logindex_save_vm_block(instance, record, 0);

	polar_logindex_save_block(instance, record, 0);
}

static void
polar_heap_prune_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	xl_heap_prune *xlrec = (xl_heap_prune *) XLogRecGetData(record);
	RelFileNode rnode;

	XLogRecGetBlockTag(record, 0, &rnode, NULL, NULL);

	/*
	 * We're about to remove tuples. In Hot Standby mode, ensure that there's
	 * no queries running for which the removed tuples are still visible.
	 */
	if (polar_enable_resolve_conflict && reachedConsistency && InHotStandby)
		ResolveRecoveryConflictWithSnapshot(xlrec->latestRemovedXid, rnode);

	polar_logindex_cleanup_parse(instance, record, 0);
}

static void
polar_heap_freeze_page_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	/*
	 * In Hot Standby mode, ensure that there's no queries running which still
	 * consider the frozen xids as running.
	 */
	if (polar_enable_resolve_conflict && reachedConsistency && InHotStandby)
	{
		xl_heap_freeze_page *xlrec = (xl_heap_freeze_page *) XLogRecGetData(record);
		RelFileNode rnode;
		TransactionId latestRemovedXid = xlrec->cutoff_xid;

		TransactionIdRetreat(latestRemovedXid);

		XLogRecGetBlockTag(record, 0, &rnode, NULL, NULL);
		ResolveRecoveryConflictWithSnapshot(latestRemovedXid, rnode);
	}

	polar_logindex_redo_parse(instance, record, 0);
}

static void
polar_heap_visible_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	/*
	 * If there are any Hot Standby transactions running that have an xmin
	 * horizon old enough that this page isn't all-visible for them, they
	 * might incorrectly decide that an index-only scan can skip a heap fetch.
	 *
	 * NB: It might be better to throw some kind of "soft" conflict here that
	 * forces any index-only scan that is in flight to perform heap fetches,
	 * rather than killing the transaction outright.
	 */
	if (polar_enable_resolve_conflict && reachedConsistency && InHotStandby)
	{
		xl_heap_visible *xlrec = (xl_heap_visible *) XLogRecGetData(record);
		RelFileNode rnode;

		XLogRecGetBlockTag(record, 1, &rnode, NULL, NULL);
		ResolveRecoveryConflictWithSnapshot(xlrec->cutoff_xid, rnode);
	}

	polar_logindex_redo_parse(instance, record, 1);
	polar_logindex_redo_parse(instance, record, 0);
}

static void
polar_heap_multi_insert_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	xl_heap_multi_insert *xlrec = (xl_heap_multi_insert *) XLogRecGetData(record);

	if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
		polar_logindex_redo_vm_parse(instance, record, 0);

	polar_logindex_redo_parse(instance, record, 0);
}

static void
polar_heap_delete_save(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	xl_heap_delete *xlrec = (xl_heap_delete *) XLogRecGetData(record);

	if (xlrec->flags & XLH_DELETE_ALL_VISIBLE_CLEARED)
		polar_logindex_save_vm_block(instance, record, 0);

	polar_logindex_save_block(instance, record, 0);
}

static void
polar_heap_delete_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	xl_heap_delete *xlrec = (xl_heap_delete *) XLogRecGetData(record);

	if (xlrec->flags & XLH_DELETE_ALL_VISIBLE_CLEARED)
		polar_logindex_redo_vm_parse(instance, record, 0);

	polar_logindex_redo_parse(instance, record, 0);
}

static void
polar_heap_lock_update_save(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	xl_heap_lock_updated *xlrec = (xl_heap_lock_updated *) XLogRecGetData(record);

	if (xlrec->flags & XLH_LOCK_ALL_FROZEN_CLEARED)
		polar_logindex_save_vm_block(instance, record, 0);

	polar_logindex_save_block(instance, record, 0);
}

static void
polar_heap_lock_update_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	xl_heap_lock_updated *xlrec = (xl_heap_lock_updated *) XLogRecGetData(record);

	if (xlrec->flags & XLH_LOCK_ALL_FROZEN_CLEARED)
		polar_logindex_redo_vm_parse(instance, record, 0);

	polar_logindex_redo_parse(instance, record, 0);
}

static void
polar_heap_lock_save(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	xl_heap_lock *xlrec = (xl_heap_lock *) XLogRecGetData(record);

	if (xlrec->flags & XLH_LOCK_ALL_FROZEN_CLEARED)
		polar_logindex_save_vm_block(instance, record, 0);

	polar_logindex_save_block(instance, record, 0);
}

static void
polar_heap_lock_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	xl_heap_lock *xlrec = (xl_heap_lock *) XLogRecGetData(record);

	if (xlrec->flags & XLH_LOCK_ALL_FROZEN_CLEARED)
		polar_logindex_redo_vm_parse(instance, record, 0);

	polar_logindex_redo_parse(instance, record, 0);
}

static void
polar_heap_xlog_update_save(polar_logindex_redo_ctl_t instance, XLogReaderState *record, bool hotupdate)
{
	BlockNumber oldblk,
				newblk;
	BlockNumber vm_saved_blk = InvalidBlockNumber;
	xl_heap_update *xlrec = (xl_heap_update *) (XLogRecGetData(record));

	XLogRecGetBlockTag(record, 0, NULL, NULL, &newblk);

	if (XLogRecGetBlockTagExtended(record, 1, NULL, NULL, &oldblk, NULL))
	{
		/* HOT updates are never done across pages */
		POLAR_ASSERT_PANIC(!hotupdate);
	}
	else
	{
		oldblk = newblk;
	}

	if (xlrec->flags & XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED)
	{
		polar_logindex_save_vm_block(instance, record, (oldblk == newblk) ? 0 : 1);
		vm_saved_blk = HEAPBLK_TO_MAPBLOCK(oldblk);
	}

	polar_logindex_save_block(instance, record, (oldblk == newblk) ? 0 : 1);

	if (oldblk != newblk)
	{
		polar_logindex_save_block(instance, record, 0);

		/* Avoid to add the same vm page to logindex twice with the same lsn */
		if (xlrec->flags & XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED &&
			vm_saved_blk != HEAPBLK_TO_MAPBLOCK(newblk))
			polar_logindex_save_vm_block(instance, record, 0);
	}
}

static void
polar_heap_xlog_update_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *record, bool hotupdate)
{
	BufferTag	tag0,
				tag1;
	BlockNumber oldblk,
				newblk;
	BlockNumber vm_parsed_blk = InvalidBlockNumber;
	polar_page_lock_t new_page_lock = POLAR_INVALID_PAGE_LOCK;
	polar_page_lock_t old_page_lock = POLAR_INVALID_PAGE_LOCK;
	Buffer		nbuffer = InvalidBuffer,
				obuffer = InvalidBuffer;
	xl_heap_update *xlrec = (xl_heap_update *) (XLogRecGetData(record));

	POLAR_GET_LOG_TAG(record, tag0, 0);
	newblk = tag0.blockNum;

	if (XLogRecHasBlockRef(record, 1))
	{
		POLAR_ASSERT_PANIC(!hotupdate);
		POLAR_GET_LOG_TAG(record, tag1, 1);
		oldblk = tag1.blockNum;
	}
	else
	{
		oldblk = newblk;
	}

	/*
	 * The visibility map may need to be fixed even if the heap page is
	 * already up-to-date.
	 */
	if (xlrec->flags & XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED)
	{
		polar_logindex_redo_vm_parse(instance, record, (oldblk == newblk) ? 0 : 1);
		vm_parsed_blk = HEAPBLK_TO_MAPBLOCK(oldblk);
	}

	old_page_lock = polar_logindex_mini_trans_lock(instance->mini_trans,
												   (oldblk == newblk) ? &tag0 : &tag1,
												   LW_EXCLUSIVE, NULL);
	obuffer = polar_logindex_parse(instance, record,
								   (oldblk == newblk) ? &tag0 : &tag1,
								   false, &old_page_lock);

	if (oldblk != newblk)
	{
		POLAR_MINI_TRANS_REDO_PARSE(instance, record, 0, tag0, new_page_lock, nbuffer);

		/*
		 * Avoid add the same vm page to logindex twice with the same lsn
		 * value
		 */
		if ((xlrec->flags & XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED) &&
			vm_parsed_blk != HEAPBLK_TO_MAPBLOCK(newblk))
			polar_logindex_redo_vm_parse(instance, record, 0);
	}

	if (BufferIsValid(nbuffer) && nbuffer != obuffer)
		UnlockReleaseBuffer(nbuffer);

	if (oldblk != newblk)
		polar_logindex_mini_trans_unlock(instance->mini_trans, new_page_lock);

	if (BufferIsValid(obuffer))
		UnlockReleaseBuffer(obuffer);

	polar_logindex_mini_trans_unlock(instance->mini_trans, old_page_lock);
}

/*
 * Handles HEAP2_PRUNE record type
 */
static XLogRedoAction
polar_heap_xlog_prune(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_heap_prune *xlrec = (xl_heap_prune *) XLogRecGetData(record);
	XLogRedoAction action = BLK_NOTFOUND;
	BufferTag	tag0;

	POLAR_GET_LOG_TAG(record, tag0, 0);

	if (!BUFFERTAGS_EQUAL(*tag, tag0))
		return action;

	/*
	 * If we have a full-page image, restore it (using a cleanup lock) and
	 * we're done.
	 */
	action = XLogReadBufferForRedoExtended(record, 0, POLAR_READ_MODE(*buffer), true, buffer);

	if (action == BLK_NEEDS_REDO)
	{
		Page		page = (Page) BufferGetPage(*buffer);
		OffsetNumber *end;
		OffsetNumber *redirected;
		OffsetNumber *nowdead;
		OffsetNumber *nowunused;
		int			nredirected;
		int			ndead;
		int			nunused;
		Size		datalen;

		redirected = (OffsetNumber *) XLogRecGetBlockData(record, 0, &datalen);

		nredirected = xlrec->nredirected;
		ndead = xlrec->ndead;
		end = (OffsetNumber *) ((char *) redirected + datalen);
		nowdead = redirected + (nredirected * 2);
		nowunused = nowdead + ndead;
		nunused = (end - nowunused);
		POLAR_ASSERT_PANIC(nunused >= 0);

		/* Update all item pointers per the record, and repair fragmentation */
		heap_page_prune_execute(*buffer,
								redirected, nredirected,
								nowdead, ndead,
								nowunused, nunused);

		/*
		 * Note: we don't worry about updating the page's prunability hints.
		 * At worst this will cause an extra prune cycle to occur soon.
		 */

		PageSetLSN(page, lsn);
	}

	return action;
}

/*
 * Replay XLOG_HEAP2_VISIBLE record.
 *
 * The critical integrity requirement here is that we must never end up with
 * a situation where the visibility map bit is set, and the page-level
 * PD_ALL_VISIBLE bit is clear.  If that were to occur, then a subsequent
 * page modification would fail to clear the visibility map bit.
 */
static XLogRedoAction
polar_heap_xlog_visible(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_heap_visible *xlrec = (xl_heap_visible *) XLogRecGetData(record);
	XLogRedoAction action = BLK_NOTFOUND;
	Page		page;
	BufferTag	tag0,
				tag1;

	POLAR_GET_LOG_TAG(record, tag1, 1);

	if (BUFFERTAGS_EQUAL(*tag, tag1))
	{
		/*
		 * Read the heap page, if it still exists. If the heap file has
		 * dropped or truncated later in recovery, we don't need to update the
		 * page, but we'd better still update the visibility map.
		 */
		action = POLAR_READ_BUFFER_FOR_REDO(record, 1, buffer);

		if (action == BLK_NEEDS_REDO)
		{
			/*
			 * We don't bump the LSN of the heap page when setting the
			 * visibility map bit (unless checksums or wal_hint_bits is
			 * enabled, in which case we must), because that would generate an
			 * unworkable volume of full-page writes.  This exposes us to torn
			 * page hazards, but since we're not inspecting the existing page
			 * contents in any way, we don't care.
			 *
			 * However, all operations that clear the visibility map bit *do*
			 * bump the LSN, and those operations will only be replayed if the
			 * XLOG LSN follows the page LSN.  Thus, if the page LSN has
			 * advanced past our XLOG record's LSN, we mustn't mark the page
			 * all-visible, because the subsequent update won't be replayed to
			 * clear the flag.
			 */
			page = BufferGetPage(*buffer);

			PageSetAllVisible(page);

			/*
			 * Update lsn otherwise we would not mark buffer dirty during
			 * online promote
			 */
			PageSetLSN(page, lsn);
		}
		else if (action == BLK_RESTORED)
		{
			/*
			 * If heap block was backed up, we already restored it and there's
			 * nothing more to do. (This can only happen with checksums or
			 * wal_log_hints enabled.)
			 */
		}
	}

	POLAR_GET_LOG_TAG(record, tag0, 0);

	if (BUFFERTAGS_EQUAL(*tag, tag0))
	{
		ReadBufferMode mode = BufferIsValid(*buffer) ? RBM_NORMAL_VALID : RBM_ZERO_ON_ERROR;

		/*
		 * Even if we skipped the heap page update due to the LSN interlock,
		 * it's still safe to update the visibility map.  Any WAL record that
		 * clears the visibility map bit does so before checking the page LSN,
		 * so any bits that need to be cleared will still be cleared.
		 */
		action = XLogReadBufferForRedoExtended(record, 0, mode, false, buffer);

		if (action == BLK_NEEDS_REDO)
		{
			Page		vmpage = BufferGetPage(*buffer);

			/* initialize the page if it was read as zeros */
			if (PageIsNew(vmpage))
				PageInit(vmpage, BLCKSZ, 0);

			/*
			 * Don't set the bit if replay has already passed this point.
			 *
			 * It might be safe to do this unconditionally; if replay has
			 * passed this point, we'll replay at least as far this time as we
			 * did before, and if this bit needs to be cleared, the record
			 * responsible for doing so should be again replayed, and clear
			 * it.  For right now, out of an abundance of conservatism, we use
			 * the same test here we did for the heap page.  If this results
			 * in a dropped bit, no real harm is done; and the next VACUUM
			 * will fix it.
			 */
			if (lsn > PageGetLSN(vmpage))
			{
				polar_visibilitymap_set(tag1.blockNum, *buffer, xlrec->flags);
				PageSetLSN(vmpage, lsn);
			}
		}
	}

	return action;
}

static XLogRedoAction
polar_heap_xlog_freeze_page(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_heap_freeze_page *xlrec = (xl_heap_freeze_page *) XLogRecGetData(record);
	XLogRedoAction action = BLK_NOTFOUND;
	int			ntup;
	BufferTag	tag0;

	POLAR_GET_LOG_TAG(record, tag0, 0);

	if (!BUFFERTAGS_EQUAL(*tag, tag0))
		return action;

	action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

	if (action == BLK_NEEDS_REDO)
	{
		Page		page = BufferGetPage(*buffer);
		xl_heap_freeze_tuple *tuples;

		tuples = (xl_heap_freeze_tuple *) XLogRecGetBlockData(record, 0, NULL);

		/* now execute freeze plan for each frozen tuple */
		for (ntup = 0; ntup < xlrec->ntuples; ntup++)
		{
			xl_heap_freeze_tuple *xlrec_tp;
			ItemId		lp;
			HeapTupleHeader tuple;

			xlrec_tp = &tuples[ntup];
			lp = PageGetItemId(page, xlrec_tp->offset); /* offsets are one-based */
			tuple = (HeapTupleHeader) PageGetItem(page, lp);

			heap_execute_freeze_tuple(tuple, xlrec_tp);
		}

		PageSetLSN(page, lsn);
	}

	return action;
}

static XLogRedoAction
polar_heap_xlog_vacuum(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_heap_vacuum *xlrec = (xl_heap_vacuum *) XLogRecGetData(record);
	XLogRedoAction action = BLK_NOTFOUND;
	BufferTag	tag0;

	POLAR_GET_LOG_TAG(record, tag0, 0);

	if (!BUFFERTAGS_EQUAL(*tag, tag0))
		return action;

	/*
	 * If we have a full-page image, restore it	(without using a cleanup lock)
	 * and we're done.
	 */
	action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

	if (action == BLK_NEEDS_REDO)
	{
		Page		page = (Page) BufferGetPage(*buffer);
		OffsetNumber *nowunused;
		Size		datalen;
		OffsetNumber *offnum;

		nowunused = (OffsetNumber *) XLogRecGetBlockData(record, 0, &datalen);

		/* Shouldn't be a record unless there's something to do */
		POLAR_ASSERT_PANIC(xlrec->nunused > 0);

		/* Update all now-unused line pointers */
		offnum = nowunused;
		for (int i = 0; i < xlrec->nunused; i++)
		{
			OffsetNumber off = *offnum++;
			ItemId		lp = PageGetItemId(page, off);

			POLAR_ASSERT_PANIC(ItemIdIsDead(lp) && !ItemIdHasStorage(lp));
			ItemIdSetUnused(lp);
		}

		/* Attempt to truncate line pointer array now */
		PageTruncateLinePointerArray(page);

		PageSetLSN(page, lsn);
	}

	return action;
}

/*
 * Handles MULTI_INSERT record type.
 */
static XLogRedoAction
polar_heap_xlog_multi_insert(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	XLogRedoAction action = BLK_NOTFOUND;
	xl_heap_multi_insert *xlrec;
	Page		page;
	union
	{
		HeapTupleHeaderData hdr;
		char		data[MaxHeapTupleSize];
	}			tbuf;
	HeapTupleHeader htup;
	uint32		newlen;
	int			i;
	bool		isinit = (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE) != 0;
	BufferTag	heap_tag,
				vm_tag;

	POLAR_GET_LOG_TAG(record, heap_tag, 0);

	/*
	 * Insertion doesn't overwrite MVCC data, so no conflict processing is
	 * required.
	 */
	xlrec = (xl_heap_multi_insert *) XLogRecGetData(record);

	INIT_BUFFERTAG(vm_tag, heap_tag.rnode, VISIBILITYMAP_FORKNUM, HEAPBLK_TO_MAPBLOCK(heap_tag.blockNum));

	if (BUFFERTAGS_EQUAL(vm_tag, *tag))
	{
		/*
		 * The visibility map may need to be fixed even if the heap page is
		 * already up-to-date.
		 */

		if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
			action = polar_heap_clear_vm(record, &heap_tag.rnode, heap_tag.blockNum, buffer,
										 VISIBILITYMAP_VALID_BITS);
		return action;
	}

	if (BUFFERTAGS_EQUAL(heap_tag, *tag))
	{
		if (isinit)
		{
			POLAR_INIT_BUFFER_FOR_REDO(record, 0, buffer);

			page = BufferGetPage(*buffer);
			PageInit(page, BufferGetPageSize(*buffer), 0);
			action = BLK_NEEDS_REDO;
		}
		else
			action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

		if (action == BLK_NEEDS_REDO)
		{
			char	   *tupdata;
			char	   *endptr;
			Size		len;

			/* Tuples are stored as block data */
			tupdata = XLogRecGetBlockData(record, 0, &len);
			endptr = tupdata + len;

			page = (Page) BufferGetPage(*buffer);

			for (i = 0; i < xlrec->ntuples; i++)
			{
				OffsetNumber offnum;
				xl_multi_insert_tuple *xlhdr;

				/*
				 * If we're reinitializing the page, the tuples are stored in
				 * order from FirstOffsetNumber. Otherwise there's an array of
				 * offsets in the WAL record, and the tuples come after that.
				 */
				if (isinit)
					offnum = FirstOffsetNumber + i;
				else
					offnum = xlrec->offsets[i];

				if (PageGetMaxOffsetNumber(page) + 1 < offnum)
				{
					POLAR_LOG_REDO_INFO(page, record);
					elog(PANIC, "invalid max offset number, page_max_off=%ld, offnum=%d",
						 PageGetMaxOffsetNumber(page), offnum);
				}

				xlhdr = (xl_multi_insert_tuple *) SHORTALIGN(tupdata);
				tupdata = ((char *) xlhdr) + SizeOfMultiInsertTuple;

				newlen = xlhdr->datalen;
				POLAR_ASSERT_PANIC(newlen <= MaxHeapTupleSize);
				htup = &tbuf.hdr;
				MemSet((char *) htup, 0, SizeofHeapTupleHeader);
				/* PG73FORMAT: get bitmap [+ padding] [+ oid] + data */
				memcpy((char *) htup + SizeofHeapTupleHeader,
					   (char *) tupdata,
					   newlen);
				tupdata += newlen;

				newlen += SizeofHeapTupleHeader;
				htup->t_infomask2 = xlhdr->t_infomask2;
				htup->t_infomask = xlhdr->t_infomask;
				htup->t_hoff = xlhdr->t_hoff;
				HeapTupleHeaderSetXmin(htup, XLogRecGetXid(record));
				HeapTupleHeaderSetCmin(htup, FirstCommandId);
				ItemPointerSetBlockNumber(&htup->t_ctid, heap_tag.blockNum);
				ItemPointerSetOffsetNumber(&htup->t_ctid, offnum);

				offnum = PageAddItem(page, (Item) htup, newlen, offnum, true, true);

				if (offnum == InvalidOffsetNumber)
				{
					POLAR_LOG_REDO_INFO(page, record);
					elog(PANIC, "failed to add tuple");
				}
			}

			if (tupdata != endptr)
			{
				POLAR_LOG_REDO_INFO(page, record);
				elog(PANIC, "total tuple length mismatch, gap=%ld", endptr - tupdata);
			}

			PageSetLSN(page, lsn);

			if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
				PageClearAllVisible(page);

			/* XLH_INSERT_ALL_FROZEN_SET implies that all tuples are visible */
			if (xlrec->flags & XLH_INSERT_ALL_FROZEN_SET)
				PageSetAllVisible(page);
		}

		return action;
	}

	return action;
}

static XLogRedoAction
polar_heap_xlog_lock_updated(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	XLogRedoAction action = BLK_NOTFOUND;
	xl_heap_lock_updated *xlrec;
	Page		page;
	OffsetNumber offnum;
	ItemId		lp = NULL;
	HeapTupleHeader htup;
	BufferTag	heap_tag,
				vm_tag;

	POLAR_GET_LOG_TAG(record, heap_tag, 0);
	INIT_BUFFERTAG(vm_tag, heap_tag.rnode, VISIBILITYMAP_FORKNUM, HEAPBLK_TO_MAPBLOCK(heap_tag.blockNum));

	xlrec = (xl_heap_lock_updated *) XLogRecGetData(record);

	if (BUFFERTAGS_EQUAL(*tag, vm_tag))
	{
		/*
		 * The visibility map may need to be fixed even if the heap page is
		 * already up-to-date.
		 */
		if (xlrec->flags & XLH_LOCK_ALL_FROZEN_CLEARED)
			action = polar_heap_clear_vm(record, &heap_tag.rnode, heap_tag.blockNum, buffer,
										 VISIBILITYMAP_ALL_FROZEN);
		return action;
	}

	if (BUFFERTAGS_EQUAL(*tag, heap_tag))
	{
		action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

		if (action == BLK_NEEDS_REDO)
		{
			page = BufferGetPage(*buffer);

			offnum = xlrec->offnum;

			if (PageGetMaxOffsetNumber(page) >= offnum)
				lp = PageGetItemId(page, offnum);

			if (PageGetMaxOffsetNumber(page) < offnum || !ItemIdIsNormal(lp))
			{
				POLAR_LOG_REDO_INFO(page, record);
				elog(PANIC, "invalid lp: page_max_off=%ld, offnum=%d, lp=%d",
					 PageGetMaxOffsetNumber(page), offnum, (lp ? lp->lp_flags : 0));
			}

			htup = (HeapTupleHeader) PageGetItem(page, lp);

			htup->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
			htup->t_infomask2 &= ~HEAP_KEYS_UPDATED;
			fix_infomask_from_infobits(xlrec->infobits_set, &htup->t_infomask,
									   &htup->t_infomask2);
			HeapTupleHeaderSetXmax(htup, xlrec->xmax);

			PageSetLSN(page, lsn);
		}

		return action;
	}

	return action;
}

static XLogRedoAction
polar_heap_xlog_insert(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_heap_insert *xlrec = (xl_heap_insert *) XLogRecGetData(record);
	Page		page;
	union
	{
		HeapTupleHeaderData hdr;
		char		data[MaxHeapTupleSize];
	}			tbuf;
	HeapTupleHeader htup;
	xl_heap_header xlhdr;
	uint32		newlen;
	ItemPointerData target_tid;
	BufferTag	heap_tag,
				vm_tag;
	XLogRedoAction action = BLK_NOTFOUND;

	POLAR_GET_LOG_TAG(record, heap_tag, 0);
	INIT_BUFFERTAG(vm_tag, heap_tag.rnode, VISIBILITYMAP_FORKNUM, HEAPBLK_TO_MAPBLOCK(heap_tag.blockNum));

	if (BUFFERTAGS_EQUAL(*tag, vm_tag))
	{
		/*
		 * The visibility map may need to be fixed even if the heap page is
		 * already up-to-date.
		 */
		if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
			action = polar_heap_clear_vm(record, &heap_tag.rnode, heap_tag.blockNum, buffer,
										 VISIBILITYMAP_VALID_BITS);
		return action;
	}

	if (BUFFERTAGS_EQUAL(*tag, heap_tag))
	{
		ItemPointerSetBlockNumber(&target_tid, heap_tag.blockNum);
		ItemPointerSetOffsetNumber(&target_tid, xlrec->offnum);

		/*
		 * If we inserted the first and only tuple on the page, re-initialize
		 * the page from scratch.
		 */
		if (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE)
		{
			POLAR_INIT_BUFFER_FOR_REDO(record, 0, buffer);

			page = BufferGetPage(*buffer);
			PageInit(page, BufferGetPageSize(*buffer), 0);
			action = BLK_NEEDS_REDO;
		}
		else
			action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

		if (action == BLK_NEEDS_REDO)
		{
			Size		datalen;
			char	   *data;

			page = BufferGetPage(*buffer);

			if (PageGetMaxOffsetNumber(page) + 1 < xlrec->offnum)
			{
				POLAR_LOG_REDO_INFO(page, record);
				elog(PANIC, "invalid max offset number: page_max_off=%ld, offnum=%d",
					 PageGetMaxOffsetNumber(page), xlrec->offnum);
			}

			data = XLogRecGetBlockData(record, 0, &datalen);

			newlen = datalen - SizeOfHeapHeader;
			POLAR_ASSERT_PANIC(datalen > SizeOfHeapHeader && newlen <= MaxHeapTupleSize);
			memcpy((char *) &xlhdr, data, SizeOfHeapHeader);
			data += SizeOfHeapHeader;

			htup = &tbuf.hdr;
			MemSet((char *) htup, 0, SizeofHeapTupleHeader);
			/* PG73FORMAT: get bitmap [+ padding] [+ oid] + data */
			memcpy((char *) htup + SizeofHeapTupleHeader,
				   data,
				   newlen);
			newlen += SizeofHeapTupleHeader;
			htup->t_infomask2 = xlhdr.t_infomask2;
			htup->t_infomask = xlhdr.t_infomask;
			htup->t_hoff = xlhdr.t_hoff;
			HeapTupleHeaderSetXmin(htup, XLogRecGetXid(record));
			HeapTupleHeaderSetCmin(htup, FirstCommandId);
			htup->t_ctid = target_tid;

			if (PageAddItem(page, (Item) htup, newlen, xlrec->offnum,
							true, true) == InvalidOffsetNumber)
			{
				POLAR_LOG_REDO_INFO(page, record);
				elog(PANIC, "failed to add tuple");
			}

			PageSetLSN(page, lsn);

			if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
				PageClearAllVisible(page);

			/* XLH_INSERT_ALL_FROZEN_SET implies that all tuples are visible */
			if (xlrec->flags & XLH_INSERT_ALL_FROZEN_SET)
				PageSetAllVisible(page);
		}

		return action;
	}

	return action;
}

static XLogRedoAction
polar_heap_xlog_delete(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_heap_delete *xlrec = (xl_heap_delete *) XLogRecGetData(record);
	Page		page;
	ItemId		lp = NULL;
	HeapTupleHeader htup;
	ItemPointerData target_tid;
	BufferTag	heap_tag,
				vm_tag;

	XLogRedoAction action = BLK_NOTFOUND;

	POLAR_GET_LOG_TAG(record, heap_tag, 0);
	INIT_BUFFERTAG(vm_tag, heap_tag.rnode, VISIBILITYMAP_FORKNUM, HEAPBLK_TO_MAPBLOCK(heap_tag.blockNum));

	if (BUFFERTAGS_EQUAL(*tag, vm_tag))
	{
		/*
		 * The visibility map may need to be fixed even if the heap page is
		 * already up-to-date.
		 */
		if (xlrec->flags & XLH_DELETE_ALL_VISIBLE_CLEARED)
			action = polar_heap_clear_vm(record, &heap_tag.rnode, heap_tag.blockNum, buffer,
										 VISIBILITYMAP_VALID_BITS);
		return action;
	}

	if (BUFFERTAGS_EQUAL(*tag, heap_tag))
	{
		ItemPointerSetBlockNumber(&target_tid, heap_tag.blockNum);
		ItemPointerSetOffsetNumber(&target_tid, xlrec->offnum);

		action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

		if (action == BLK_NEEDS_REDO)
		{
			page = BufferGetPage(*buffer);

			if (PageGetMaxOffsetNumber(page) >= xlrec->offnum)
				lp = PageGetItemId(page, xlrec->offnum);

			if (PageGetMaxOffsetNumber(page) < xlrec->offnum || !ItemIdIsNormal(lp))
			{
				POLAR_LOG_REDO_INFO(page, record);
				elog(PANIC, "invalid lp: page_max_off=%ld, offnum=%d, lp=%d",
					 PageGetMaxOffsetNumber(page), xlrec->offnum, (lp ? lp->lp_flags : 0));
			}

			htup = (HeapTupleHeader) PageGetItem(page, lp);

			htup->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
			htup->t_infomask2 &= ~HEAP_KEYS_UPDATED;
			HeapTupleHeaderClearHotUpdated(htup);
			fix_infomask_from_infobits(xlrec->infobits_set,
									   &htup->t_infomask, &htup->t_infomask2);

			if (!(xlrec->flags & XLH_DELETE_IS_SUPER))
				HeapTupleHeaderSetXmax(htup, xlrec->xmax);
			else
				HeapTupleHeaderSetXmin(htup, InvalidTransactionId);

			HeapTupleHeaderSetCmax(htup, FirstCommandId, false);

			/* Mark the page as a candidate for pruning */
			PageSetPrunable(page, XLogRecGetXid(record));

			if (xlrec->flags & XLH_DELETE_ALL_VISIBLE_CLEARED)
				PageClearAllVisible(page);

			/* Make sure t_ctid is set correctly */
			if (xlrec->flags & XLH_DELETE_IS_PARTITION_MOVE)
				HeapTupleHeaderSetMovedPartitions(htup);
			else
				htup->t_ctid = target_tid;

			PageSetLSN(page, lsn);
		}

		return action;
	}

	return action;
}

static XLogRedoAction
polar_heap_xlog_update(XLogReaderState *record, BufferTag *tag, Buffer *buffer, bool hotupdate)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	XLogRecPtr	orig_old_lsn = InvalidXLogRecPtr;
	xl_heap_update *xlrec = (xl_heap_update *) XLogRecGetData(record);
	BlockNumber oldblk;
	BlockNumber newblk;
	ItemPointerData newtid;
	Page		page;
	OffsetNumber offnum;
	ItemId		lp = NULL;
	HeapTupleData oldtup;
	HeapTupleHeader htup;
	uint16		prefixlen = 0,
				suffixlen = 0;
	char	   *newp;
	union
	{
		HeapTupleHeaderData hdr;
		char		data[MaxHeapTupleSize];
	}			tbuf;
	xl_heap_header xlhdr;
	uint32		newlen;
	XLogRedoAction action = BLK_NOTFOUND;
	BufferTag	tag0,
				tag1,
				tag2,
				tag3;
	BufferTag  *old_tag = NULL,
			   *new_tag = NULL,
			   *old_vm_tag = NULL,
			   *new_vm_tag = NULL;

	/* initialize to keep the compiler quiet */
	oldtup.t_data = NULL;
	oldtup.t_len = 0;

	POLAR_GET_LOG_TAG(record, tag0, 0);
	newblk = tag0.blockNum;
	new_tag = &tag0;

	CLEAR_BUFFERTAG(tag1);

	if (XLogRecHasBlockRef(record, 1))
	{
		/* HOT updates are never done across pages */
		POLAR_ASSERT_PANIC(!hotupdate);
		POLAR_GET_LOG_TAG(record, tag1, 1);
		oldblk = tag1.blockNum;
		old_tag = &tag1;
	}
	else
	{
		oldblk = newblk;
		old_tag = &tag0;
	}

	if (oldblk != newblk)
	{
		if (xlrec->flags & XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED)
		{
			INIT_BUFFERTAG(tag3, tag1.rnode, VISIBILITYMAP_FORKNUM,
						   HEAPBLK_TO_MAPBLOCK(tag1.blockNum));

			old_vm_tag = &tag3;
		}

		if (xlrec->flags & XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED)
		{
			INIT_BUFFERTAG(tag2, tag0.rnode, VISIBILITYMAP_FORKNUM,
						   HEAPBLK_TO_MAPBLOCK(tag0.blockNum));
			new_vm_tag = &tag2;
		}
	}
	else
	{
		if (xlrec->flags & XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED)
		{
			INIT_BUFFERTAG(tag2, tag0.rnode, VISIBILITYMAP_FORKNUM,
						   HEAPBLK_TO_MAPBLOCK(tag0.blockNum));
			old_vm_tag = &tag2;
		}
	}

	ItemPointerSet(&newtid, newblk, xlrec->new_offnum);

	if (old_vm_tag != NULL && BUFFERTAGS_EQUAL(*old_vm_tag, *tag))
	{
		/*
		 * The visibility map may need to be fixed even if the heap page is
		 * already up-to-date.
		 */
		if ((xlrec->flags & XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED))
		{
			action = polar_heap_clear_vm(record, &old_tag->rnode, old_tag->blockNum,
										 buffer, VISIBILITYMAP_VALID_BITS);
		}
	}

	if (new_vm_tag != NULL && BUFFERTAGS_EQUAL(*new_vm_tag, *tag))
	{
		/*
		 * The visibility map may need to be fixed even if the heap page is
		 * already up-to-date.
		 */
		if (xlrec->flags & XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED)
		{
			action = polar_heap_clear_vm(record, &new_tag->rnode, new_tag->blockNum,
										 buffer, VISIBILITYMAP_VALID_BITS);
		}
	}

	if (BUFFERTAGS_EQUAL(*tag, *old_tag))
	{
		/*
		 * In normal operation, it is important to lock the two pages in
		 * page-number order, to avoid possible deadlocks against other update
		 * operations going the other way.  However, during WAL replay there
		 * can be no other update happening, so we don't need to worry about
		 * that. But we *do* need to worry that we don't expose an
		 * inconsistent state to Hot Standby queries --- so the original page
		 * can't be unlocked before we've added the new tuple to the new page.
		 */

		/* Deal with old tuple version */
		action = POLAR_READ_BUFFER_FOR_REDO(record, (oldblk == newblk) ? 0 : 1, buffer);

		if (action == BLK_NEEDS_REDO)
		{
			page = BufferGetPage(*buffer);
			offnum = xlrec->old_offnum;
			orig_old_lsn = PageGetLSN(page);

			if (PageGetMaxOffsetNumber(page) >= offnum)
				lp = PageGetItemId(page, offnum);

			if (PageGetMaxOffsetNumber(page) < offnum || !ItemIdIsNormal(lp))
			{
				POLAR_LOG_REDO_INFO(page, record);
				elog(PANIC, "invalid lp: page_max_off=%ld, offnum=%d, lp=%d",
					 PageGetMaxOffsetNumber(page), offnum, (lp ? lp->lp_flags : 0));
			}

			htup = (HeapTupleHeader) PageGetItem(page, lp);

			oldtup.t_data = htup;
			oldtup.t_len = ItemIdGetLength(lp);

			htup->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
			htup->t_infomask2 &= ~HEAP_KEYS_UPDATED;

			if (hotupdate)
				HeapTupleHeaderSetHotUpdated(htup);
			else
				HeapTupleHeaderClearHotUpdated(htup);

			fix_infomask_from_infobits(xlrec->old_infobits_set, &htup->t_infomask,
									   &htup->t_infomask2);
			HeapTupleHeaderSetXmax(htup, xlrec->old_xmax);
			HeapTupleHeaderSetCmax(htup, FirstCommandId, false);
			/* Set forward chain link in t_ctid */
			htup->t_ctid = newtid;

			/* Mark the page as a candidate for pruning */
			PageSetPrunable(page, XLogRecGetXid(record));

			if (xlrec->flags & XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED)
				PageClearAllVisible(page);

			PageSetLSN(page, lsn);
		}
	}

	/* Maybe old and new tuple all in the same page  */
	if (BUFFERTAGS_EQUAL(*tag, *new_tag))
	{
		/*
		 * Read the page the new tuple goes into, if different from old.
		 */
		if (oldblk != newblk)
		{
			if (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE)
			{
				POLAR_INIT_BUFFER_FOR_REDO(record, 0, buffer);

				page = (Page) BufferGetPage(*buffer);
				PageInit(page, BufferGetPageSize(*buffer), 0);
				action = BLK_NEEDS_REDO;
			}
			else
				action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);
		}

		/* Deal with new tuple */
		if (action == BLK_NEEDS_REDO)
		{
			char	   *recdata;
			char	   *recdata_end;
			Size		datalen;
			Size		tuplen;

			recdata = XLogRecGetBlockData(record, 0, &datalen);
			recdata_end = recdata + datalen;

			page = BufferGetPage(*buffer);

			offnum = xlrec->new_offnum;

			if (PageGetMaxOffsetNumber(page) + 1 < offnum)
			{
				elog(LOG, "Original old page lsn is %lX", orig_old_lsn);

				POLAR_LOG_REDO_INFO(page, record);
				elog(PANIC, "invalid max offset number: page_max_off=%ld, offnum=%d",
					 PageGetMaxOffsetNumber(page), offnum);
			}

			if (xlrec->flags & XLH_UPDATE_PREFIX_FROM_OLD)
			{
				/*
				 * pageTag->blockNum must equal oldblk, see function
				 * log_heap_update
				 */
				POLAR_ASSERT_PANIC(newblk == oldblk);
				memcpy(&prefixlen, recdata, sizeof(uint16));
				recdata += sizeof(uint16);
			}

			if (xlrec->flags & XLH_UPDATE_SUFFIX_FROM_OLD)
			{
				/*
				 * pageTag->blockNum must equal oldblk, see function
				 * log_heap_update
				 */
				POLAR_ASSERT_PANIC(newblk == oldblk);
				memcpy(&suffixlen, recdata, sizeof(uint16));
				recdata += sizeof(uint16);
			}

			memcpy((char *) &xlhdr, recdata, SizeOfHeapHeader);
			recdata += SizeOfHeapHeader;

			tuplen = recdata_end - recdata;
			POLAR_ASSERT_PANIC(tuplen <= MaxHeapTupleSize);

			htup = &tbuf.hdr;
			MemSet((char *) htup, 0, SizeofHeapTupleHeader);

			/*
			 * Reconstruct the new tuple using the prefix and/or suffix from
			 * the old tuple, and the data stored in the WAL record.
			 */
			newp = (char *) htup + SizeofHeapTupleHeader;

			if (prefixlen > 0)
			{
				int			len;

				/* copy bitmap [+ padding] [+ oid] from WAL record */
				len = xlhdr.t_hoff - SizeofHeapTupleHeader;
				memcpy(newp, recdata, len);
				recdata += len;
				newp += len;

				if (!oldtup.t_data)
				{
					elog(LOG, "Original old page lsn is %lX", orig_old_lsn);
					POLAR_LOG_REDO_INFO(page, record);
					elog(PANIC, "Old tuple data is null");
				}

				/* copy prefix from old tuple */
				memcpy(newp, (char *) oldtup.t_data + oldtup.t_data->t_hoff, prefixlen);
				newp += prefixlen;

				/* copy new tuple data from WAL record */
				len = tuplen - (xlhdr.t_hoff - SizeofHeapTupleHeader);
				memcpy(newp, recdata, len);
				recdata += len;
				newp += len;
			}
			else
			{
				/*
				 * copy bitmap [+ padding] [+ oid] + data from record, all in
				 * one go
				 */
				memcpy(newp, recdata, tuplen);
				recdata += tuplen;
				newp += tuplen;
			}

			if (recdata != recdata_end)
			{
				elog(LOG, "Original old page lsn is %lX", orig_old_lsn);
				POLAR_LOG_REDO_INFO(page, record);
				elog(PANIC, "Failed to decode tuple");
			}

			/* copy suffix from old tuple */
			if (suffixlen > 0)
			{
				if (!oldtup.t_data)
				{
					elog(LOG, "Original old page lsn is %lX", orig_old_lsn);
					POLAR_LOG_REDO_INFO(page, record);
					elog(PANIC, "Old tuple data is null");
				}

				memcpy(newp, (char *) oldtup.t_data + oldtup.t_len - suffixlen, suffixlen);
			}

			newlen = SizeofHeapTupleHeader + tuplen + prefixlen + suffixlen;
			htup->t_infomask2 = xlhdr.t_infomask2;
			htup->t_infomask = xlhdr.t_infomask;
			htup->t_hoff = xlhdr.t_hoff;

			HeapTupleHeaderSetXmin(htup, XLogRecGetXid(record));
			HeapTupleHeaderSetCmin(htup, FirstCommandId);
			HeapTupleHeaderSetXmax(htup, xlrec->new_xmax);
			/* Make sure there is no forward chain link in t_ctid */
			htup->t_ctid = newtid;

			offnum = PageAddItem(page, (Item) htup, newlen, offnum, true, true);

			if (offnum == InvalidOffsetNumber)
			{
				elog(LOG, "Original old page lsn is %lX", orig_old_lsn);
				POLAR_LOG_REDO_INFO(page, record);
				elog(PANIC, "failed to add tuple");
			}

			if (xlrec->flags & XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED)
				PageClearAllVisible(page);

			PageSetLSN(page, lsn);
		}
	}

	return action;
}

static XLogRedoAction
polar_heap_xlog_confirm(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_heap_confirm *xlrec = (xl_heap_confirm *) XLogRecGetData(record);
	XLogRedoAction action = BLK_NOTFOUND;
	Page		page;
	OffsetNumber offnum;
	ItemId		lp = NULL;
	HeapTupleHeader htup;
	BufferTag	tag0;

	POLAR_GET_LOG_TAG(record, tag0, 0);

	if (!BUFFERTAGS_EQUAL(*tag, tag0))
		return action;

	action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

	if (action == BLK_NEEDS_REDO)
	{
		page = BufferGetPage(*buffer);

		offnum = xlrec->offnum;

		if (PageGetMaxOffsetNumber(page) >= offnum)
			lp = PageGetItemId(page, offnum);

		if (PageGetMaxOffsetNumber(page) < offnum || !ItemIdIsNormal(lp))
		{
			POLAR_LOG_REDO_INFO(page, record);
			elog(PANIC, "invalid lp: page_max_off=%ld, offnum=%d, lp=%d",
				 PageGetMaxOffsetNumber(page), offnum, (lp ? lp->lp_flags : 0));
		}

		htup = (HeapTupleHeader) PageGetItem(page, lp);

		/*
		 * Confirm tuple as actually inserted
		 */
		ItemPointerSet(&htup->t_ctid, BufferGetBlockNumber(*buffer), offnum);

		PageSetLSN(page, lsn);
	}

	return action;
}

static XLogRedoAction
polar_heap_xlog_lock(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_heap_lock *xlrec = (xl_heap_lock *) XLogRecGetData(record);
	XLogRedoAction action = BLK_NOTFOUND;
	Page		page;
	OffsetNumber offnum;
	ItemId		lp = NULL;
	HeapTupleHeader htup;
	BufferTag	heap_tag,
				vm_tag;

	POLAR_GET_LOG_TAG(record, heap_tag, 0);
	INIT_BUFFERTAG(vm_tag, heap_tag.rnode, VISIBILITYMAP_FORKNUM, HEAPBLK_TO_MAPBLOCK(heap_tag.blockNum));

	if (BUFFERTAGS_EQUAL(*tag, vm_tag))
	{
		if (xlrec->flags & XLH_LOCK_ALL_FROZEN_CLEARED)
		{
			action = polar_heap_clear_vm(record, &heap_tag.rnode, heap_tag.blockNum, buffer,
										 VISIBILITYMAP_ALL_FROZEN);
		}
	}

	if (BUFFERTAGS_EQUAL(*tag, heap_tag))
	{
		action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

		if (action == BLK_NEEDS_REDO)
		{
			page = (Page) BufferGetPage(*buffer);

			offnum = xlrec->offnum;

			if (PageGetMaxOffsetNumber(page) >= offnum)
				lp = PageGetItemId(page, offnum);

			if (PageGetMaxOffsetNumber(page) < offnum || !ItemIdIsNormal(lp))
			{
				POLAR_LOG_REDO_INFO(page, record);
				elog(PANIC, "invalid lp: page_max_off=%ld, offnum=%d, lp=%d",
					 PageGetMaxOffsetNumber(page), offnum, (lp ? lp->lp_flags : 0));
			}

			htup = (HeapTupleHeader) PageGetItem(page, lp);

			htup->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
			htup->t_infomask2 &= ~HEAP_KEYS_UPDATED;
			fix_infomask_from_infobits(xlrec->infobits_set, &htup->t_infomask,
									   &htup->t_infomask2);

			/*
			 * Clear relevant update flags, but only if the modified infomask
			 * says there's no update.
			 */
			if (HEAP_XMAX_IS_LOCKED_ONLY(htup->t_infomask))
			{
				HeapTupleHeaderClearHotUpdated(htup);
				/* Make sure there is no forward chain link in t_ctid */
				ItemPointerSet(&htup->t_ctid,
							   BufferGetBlockNumber(*buffer),
							   offnum);
			}

			HeapTupleHeaderSetXmax(htup, xlrec->locking_xid);
			HeapTupleHeaderSetCmax(htup, FirstCommandId, false);
			PageSetLSN(page, lsn);
		}
	}

	return action;
}

static XLogRedoAction
polar_heap_xlog_inplace(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_heap_inplace *xlrec = (xl_heap_inplace *) XLogRecGetData(record);
	XLogRedoAction action = BLK_NOTFOUND;
	Page		page;
	OffsetNumber offnum;
	ItemId		lp = NULL;
	HeapTupleHeader htup;
	uint32		oldlen;
	Size		newlen;
	BufferTag	tag0;

	POLAR_GET_LOG_TAG(record, tag0, 0);

	if (!BUFFERTAGS_EQUAL(*tag, tag0))
		return action;

	action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

	if (action == BLK_NEEDS_REDO)
	{
		char	   *newtup = XLogRecGetBlockData(record, 0, &newlen);

		page = BufferGetPage(*buffer);

		offnum = xlrec->offnum;

		if (PageGetMaxOffsetNumber(page) >= offnum)
			lp = PageGetItemId(page, offnum);

		if (PageGetMaxOffsetNumber(page) < offnum || !ItemIdIsNormal(lp))
		{
			POLAR_LOG_REDO_INFO(page, record);
			elog(PANIC, "invalid lp: page_max_off=%ld, offnum=%d, lp=%d",
				 PageGetMaxOffsetNumber(page), offnum, (lp ? lp->lp_flags : 0));
		}

		htup = (HeapTupleHeader) PageGetItem(page, lp);

		oldlen = ItemIdGetLength(lp) - htup->t_hoff;

		if (oldlen != newlen)
		{
			POLAR_LOG_REDO_INFO(page, record);
			elog(PANIC, "wrong tuple length: oldlen=%u, newlen=%ld", oldlen, newlen);
		}

		memcpy((char *) htup + htup->t_hoff, newtup, newlen);

		PageSetLSN(page, lsn);
	}

	return action;
}

void
polar_heap2_idx_save(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info & XLOG_HEAP_OPMASK)
	{
		case XLOG_HEAP2_PRUNE:
		case XLOG_HEAP2_FREEZE_PAGE:
			polar_logindex_save_block(instance, record, 0);
			break;

		case XLOG_HEAP2_VACUUM:
			polar_logindex_save_block(instance, record, 0);
			break;

		case XLOG_HEAP2_VISIBLE:
			polar_logindex_save_block(instance, record, 1);
			polar_logindex_save_block(instance, record, 0);
			break;

		case XLOG_HEAP2_MULTI_INSERT:
			polar_heap_multi_insert_save(instance, record);
			break;

		case XLOG_HEAP2_LOCK_UPDATED:
			polar_heap_lock_update_save(instance, record);
			break;

		case XLOG_HEAP2_NEW_CID:
			break;

		case XLOG_HEAP2_REWRITE:
			break;

		default:
			elog(PANIC, "polar_heap2_idx_save: unknown op code %u", info);
			break;
	}
}

bool
polar_heap2_idx_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info & XLOG_HEAP_OPMASK)
	{
		case XLOG_HEAP2_PRUNE:
			polar_heap_prune_parse(instance, record);
			break;

		case XLOG_HEAP2_FREEZE_PAGE:
			polar_heap_freeze_page_parse(instance, record);
			break;

		case XLOG_HEAP2_VACUUM:
			polar_logindex_redo_parse(instance, record, 0);
			break;

		case XLOG_HEAP2_VISIBLE:
			polar_heap_visible_parse(instance, record);
			break;

		case XLOG_HEAP2_MULTI_INSERT:
			polar_heap_multi_insert_parse(instance, record);
			break;

		case XLOG_HEAP2_LOCK_UPDATED:
			polar_heap_lock_update_parse(instance, record);
			break;

		case XLOG_HEAP2_NEW_CID:
			break;

		case XLOG_HEAP2_REWRITE:
			heap_xlog_logical_rewrite(record);
			break;

		default:
			elog(PANIC, "polar_heap2_idx_parse: unknown op code %u", info);
			break;
	}

	return true;
}

XLogRedoAction
polar_heap2_idx_redo(polar_logindex_redo_ctl_t instance, XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info & XLOG_HEAP_OPMASK)
	{
		case XLOG_HEAP2_PRUNE:
			return polar_heap_xlog_prune(record, tag, buffer);

		case XLOG_HEAP2_FREEZE_PAGE:
			return polar_heap_xlog_freeze_page(record, tag, buffer);

		case XLOG_HEAP2_VACUUM:
			polar_heap_xlog_vacuum(record, tag, buffer);
			break;

		case XLOG_HEAP2_VISIBLE:
			return polar_heap_xlog_visible(record, tag, buffer);

		case XLOG_HEAP2_MULTI_INSERT:
			return polar_heap_xlog_multi_insert(record, tag, buffer);

		case XLOG_HEAP2_LOCK_UPDATED:
			return polar_heap_xlog_lock_updated(record, tag, buffer);

		default:
			elog(PANIC, "polar_heap2_idx_redo: unknown op code %u", info);
			break;
	}

	return BLK_NOTFOUND;
}

void
polar_heap_idx_save(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info & XLOG_HEAP_OPMASK)
	{
		case XLOG_HEAP_INSERT:
			polar_heap_insert_save(instance, record);
			break;

		case XLOG_HEAP_DELETE:
			polar_heap_delete_save(instance, record);
			break;

		case XLOG_HEAP_UPDATE:
			polar_heap_xlog_update_save(instance, record, false);
			break;

		case XLOG_HEAP_TRUNCATE:
			break;

		case XLOG_HEAP_HOT_UPDATE:
			polar_heap_xlog_update_save(instance, record, true);
			break;

		case XLOG_HEAP_CONFIRM:
			polar_logindex_save_block(instance, record, 0);
			break;

		case XLOG_HEAP_LOCK:
			polar_heap_lock_save(instance, record);
			break;

		case XLOG_HEAP_INPLACE:
			polar_logindex_save_block(instance, record, 0);
			break;

		default:
			elog(PANIC, "polar_heap_idx_save: unknown op code %u", info);
			break;
	}

}

bool
polar_heap_idx_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info & XLOG_HEAP_OPMASK)
	{
		case XLOG_HEAP_INSERT:
			polar_heap_insert_parse(instance, record);
			break;

		case XLOG_HEAP_DELETE:
			polar_heap_delete_parse(instance, record);
			break;

		case XLOG_HEAP_UPDATE:
			polar_heap_xlog_update_parse(instance, record, false);
			break;

		case XLOG_HEAP_TRUNCATE:

			/*
			 * TRUNCATE is a no-op because the actions are already logged as
			 * SMGR WAL records.  TRUNCATE WAL record only exists for logical
			 * decoding.
			 */
			break;

		case XLOG_HEAP_HOT_UPDATE:
			polar_heap_xlog_update_parse(instance, record, true);
			break;

		case XLOG_HEAP_CONFIRM:
			polar_logindex_redo_parse(instance, record, 0);
			break;

		case XLOG_HEAP_LOCK:
			polar_heap_lock_parse(instance, record);
			break;

		case XLOG_HEAP_INPLACE:
			polar_logindex_redo_parse(instance, record, 0);
			break;

		default:
			elog(PANIC, "polar_heap_idx_parse: unknown op code %u", info);
			break;
	}

	return true;
}

XLogRedoAction
polar_heap_idx_redo(polar_logindex_redo_ctl_t instance, XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info & XLOG_HEAP_OPMASK)
	{
		case XLOG_HEAP_INSERT:
			return polar_heap_xlog_insert(record, tag, buffer);

		case XLOG_HEAP_DELETE:
			return polar_heap_xlog_delete(record, tag, buffer);

		case XLOG_HEAP_UPDATE:
			return polar_heap_xlog_update(record, tag, buffer, false);

		case XLOG_HEAP_TRUNCATE:

			/*
			 * TRUNCATE is a no-op because the actions are already logged as
			 * SMGR WAL records.  TRUNCATE WAL record only exists for logical
			 * decoding.
			 */
			break;

		case XLOG_HEAP_HOT_UPDATE:
			return polar_heap_xlog_update(record, tag, buffer, true);

		case XLOG_HEAP_CONFIRM:
			return polar_heap_xlog_confirm(record, tag, buffer);

		case XLOG_HEAP_LOCK:
			return polar_heap_xlog_lock(record, tag, buffer);

		case XLOG_HEAP_INPLACE:
			return polar_heap_xlog_inplace(record, tag, buffer);

		default:
			elog(PANIC, "polar_heap_idx_redo: unknown op code %u", info);
			break;
	}

	return BLK_NOTFOUND;
}
