/*-------------------------------------------------------------------------
 *
 * hio.c
 *	  POSTGRES heap access method input/output code.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/hio.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/hio.h"
#include "access/htup_details.h"
#include "access/visibilitymap.h"
#include "storage/bufmgr.h"
#include "storage/freespace.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"

/* POLAR */
#include "polar_flashback/polar_flashback.h"
#include "utils/guc.h"

static Buffer polar_relation_add_extra_blocks_and_return_last_buffer(Relation relation, BulkInsertState bistate);
/* POLAR end */

/*
 * RelationPutHeapTuple - place tuple at specified page
 *
 * !!! EREPORT(ERROR) IS DISALLOWED HERE !!!  Must PANIC on failure!!!
 *
 * Note - caller must hold BUFFER_LOCK_EXCLUSIVE on the buffer.
 */
void
RelationPutHeapTuple(Relation relation,
					 Buffer buffer,
					 HeapTuple tuple,
					 bool token)
{
	Page		pageHeader;
	OffsetNumber offnum;

	/*
	 * A tuple that's being inserted speculatively should already have its
	 * token set.
	 */
	Assert(!token || HeapTupleHeaderIsSpeculative(tuple->t_data));

	/* Add the tuple to the page */
	pageHeader = BufferGetPage(buffer);

	offnum = PageAddItem(pageHeader, (Item) tuple->t_data,
						 tuple->t_len, InvalidOffsetNumber, false, true);

	if (offnum == InvalidOffsetNumber)
		elog(PANIC, "failed to add tuple to page");

	/* Update tuple->t_self to the actual position where it was stored */
	ItemPointerSet(&(tuple->t_self), BufferGetBlockNumber(buffer), offnum);

	/*
	 * Insert the correct position into CTID of the stored tuple, too (unless
	 * this is a speculative insertion, in which case the token is held in
	 * CTID field instead)
	 */
	if (!token)
	{
		ItemId		itemId = PageGetItemId(pageHeader, offnum);
		HeapTupleHeader item = (HeapTupleHeader) PageGetItem(pageHeader, itemId);

		item->t_ctid = tuple->t_self;
	}
}

/*
 * Read in a buffer, using bulk-insert strategy if bistate isn't NULL.
 */
static Buffer
ReadBufferBI(Relation relation, BlockNumber targetBlock,
			 BulkInsertState bistate)
{
	Buffer		buffer;

	/* If not bulk-insert, exactly like ReadBuffer */
	if (!bistate)
		return ReadBuffer(relation, targetBlock);

	/* If we have the desired block already pinned, re-pin and return it */
	if (bistate->current_buf != InvalidBuffer)
	{
		if (BufferGetBlockNumber(bistate->current_buf) == targetBlock)
		{
			IncrBufferRefCount(bistate->current_buf);
			return bistate->current_buf;
		}
		/* ... else drop the old buffer */
		ReleaseBuffer(bistate->current_buf);
		bistate->current_buf = InvalidBuffer;
	}

	/* Perform a read using the buffer strategy */
	buffer = ReadBufferExtended(relation, MAIN_FORKNUM, targetBlock,
								RBM_NORMAL, bistate->strategy);

	/* Save the selected block as target for future inserts */
	IncrBufferRefCount(buffer);
	bistate->current_buf = buffer;

	return buffer;
}

/*
 * For each heap page which is all-visible, acquire a pin on the appropriate
 * visibility map page, if we haven't already got one.
 *
 * buffer2 may be InvalidBuffer, if only one buffer is involved.  buffer1
 * must not be InvalidBuffer.  If both buffers are specified, block1 must
 * be less than block2.
 */
static void
GetVisibilityMapPins(Relation relation, Buffer buffer1, Buffer buffer2,
					 BlockNumber block1, BlockNumber block2,
					 Buffer *vmbuffer1, Buffer *vmbuffer2)
{
	bool		need_to_pin_buffer1;
	bool		need_to_pin_buffer2;

	Assert(BufferIsValid(buffer1));
	Assert(buffer2 == InvalidBuffer || block1 <= block2);

	while (1)
	{
		/* Figure out which pins we need but don't have. */
		need_to_pin_buffer1 = PageIsAllVisible(BufferGetPage(buffer1))
			&& !visibilitymap_pin_ok(block1, *vmbuffer1);
		need_to_pin_buffer2 = buffer2 != InvalidBuffer
			&& PageIsAllVisible(BufferGetPage(buffer2))
			&& !visibilitymap_pin_ok(block2, *vmbuffer2);
		if (!need_to_pin_buffer1 && !need_to_pin_buffer2)
			return;

		/* We must unlock both buffers before doing any I/O. */
		LockBuffer(buffer1, BUFFER_LOCK_UNLOCK);
		if (buffer2 != InvalidBuffer && buffer2 != buffer1)
			LockBuffer(buffer2, BUFFER_LOCK_UNLOCK);

		/* Get pins. */
		if (need_to_pin_buffer1)
			visibilitymap_pin(relation, block1, vmbuffer1);
		if (need_to_pin_buffer2)
			visibilitymap_pin(relation, block2, vmbuffer2);

		/* Relock buffers. */
		LockBuffer(buffer1, BUFFER_LOCK_EXCLUSIVE);
		if (buffer2 != InvalidBuffer && buffer2 != buffer1)
			LockBuffer(buffer2, BUFFER_LOCK_EXCLUSIVE);

		/*
		 * If there are two buffers involved and we pinned just one of them,
		 * it's possible that the second one became all-visible while we were
		 * busy pinning the first one.  If it looks like that's a possible
		 * scenario, we'll need to make a second pass through this loop.
		 */
		if (buffer2 == InvalidBuffer || buffer1 == buffer2
			|| (need_to_pin_buffer1 && need_to_pin_buffer2))
			break;
	}
}

/*
 * Extend a relation by multiple blocks to avoid future contention on the
 * relation extension lock.  Our goal is to pre-extend the relation by an
 * amount which ramps up as the degree of contention ramps up, but limiting
 * the result to some sane overall value.
 */
static void
RelationAddExtraBlocks(Relation relation, BulkInsertState bistate)
{
	BlockNumber blockNum,
				firstBlock = InvalidBlockNumber;
	int			extraBlocks;
	int			lockWaiters;

	/* Use the length of the lock wait queue to judge how much to extend. */
	lockWaiters = RelationExtensionLockWaiterCount(relation);
	if (lockWaiters <= 0)
		return;

	/*
	 * It might seem like multiplying the number of lock waiters by as much as
	 * 20 is too aggressive, but benchmarking revealed that smaller numbers
	 * were insufficient.  512 is just an arbitrary cap to prevent
	 * pathological results.
	 */
	extraBlocks = Min(512, lockWaiters * 20);

	do
	{
		Buffer		buffer;
		Page		page;
		Size		freespace;

		/*
		 * Extend by one page.  This should generally match the main-line
		 * extension code in RelationGetBufferForTuple, except that we hold
		 * the relation extension lock throughout.
		 */
		buffer = ReadBufferBI(relation, P_NEW, bistate);

		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buffer);

		if (!PageIsNew(page))
			elog(ERROR, "page %u of relation \"%s\" should be empty but is not",
				 BufferGetBlockNumber(buffer),
				 RelationGetRelationName(relation));

		PageInit(page, BufferGetPageSize(buffer), 0);

		/*
		 * We mark all the new buffers dirty, but do nothing to write them
		 * out; they'll probably get used soon, and even if they are not, a
		 * crash will leave an okay all-zeroes page on disk.
		 */
		MarkBufferDirty(buffer);

		/* we'll need this info below */
		blockNum = BufferGetBlockNumber(buffer);
		freespace = PageGetHeapFreeSpace(page);

		UnlockReleaseBuffer(buffer);

		/* Remember first block number thus added. */
		if (firstBlock == InvalidBlockNumber)
			firstBlock = blockNum;

		/*
		 * Immediately update the bottom level of the FSM.  This has a good
		 * chance of making this page visible to other concurrently inserting
		 * backends, and we want that to happen without delay.
		 */
		RecordPageWithFreeSpace(relation, blockNum, freespace);
	}
	while (--extraBlocks > 0);

	/*
	 * Updating the upper levels of the free space map is too expensive to do
	 * for every block, but it's worth doing once at the end to make sure that
	 * subsequent insertion activity sees all of those nifty free pages we
	 * just inserted.
	 */
	FreeSpaceMapVacuumRange(relation, firstBlock, blockNum + 1);
}

/*
 * RelationGetBufferForTuple
 *
 *	Returns pinned and exclusive-locked buffer of a page in given relation
 *	with free space >= given len.
 *
 *	If otherBuffer is not InvalidBuffer, then it references a previously
 *	pinned buffer of another page in the same relation; on return, this
 *	buffer will also be exclusive-locked.  (This case is used by heap_update;
 *	the otherBuffer contains the tuple being updated.)
 *
 *	The reason for passing otherBuffer is that if two backends are doing
 *	concurrent heap_update operations, a deadlock could occur if they try
 *	to lock the same two buffers in opposite orders.  To ensure that this
 *	can't happen, we impose the rule that buffers of a relation must be
 *	locked in increasing page number order.  This is most conveniently done
 *	by having RelationGetBufferForTuple lock them both, with suitable care
 *	for ordering.
 *
 *	NOTE: it is unlikely, but not quite impossible, for otherBuffer to be the
 *	same buffer we select for insertion of the new tuple (this could only
 *	happen if space is freed in that page after heap_update finds there's not
 *	enough there).  In that case, the page will be pinned and locked only once.
 *
 *	For the vmbuffer and vmbuffer_other arguments, we avoid deadlock by
 *	locking them only after locking the corresponding heap page, and taking
 *	no further lwlocks while they are locked.
 *
 *	We normally use FSM to help us find free space.  However,
 *	if HEAP_INSERT_SKIP_FSM is specified, we just append a new empty page to
 *	the end of the relation if the tuple won't fit on the current target page.
 *	This can save some cycles when we know the relation is new and doesn't
 *	contain useful amounts of free space.
 *
 *	HEAP_INSERT_SKIP_FSM is also useful for non-WAL-logged additions to a
 *	relation, if the caller holds exclusive lock and is careful to invalidate
 *	relation's smgr_targblock before the first insertion --- that ensures that
 *	all insertions will occur into newly added pages and not be intermixed
 *	with tuples from other transactions.  That way, a crash can't risk losing
 *	any committed data of other transactions.  (See heap_insert's comments
 *	for additional constraints needed for safe usage of this behavior.)
 *
 *	The caller can also provide a BulkInsertState object to optimize many
 *	insertions into the same relation.  This keeps a pin on the current
 *	insertion target page (to save pin/unpin cycles) and also passes a
 *	BULKWRITE buffer selection strategy object to the buffer manager.
 *	Passing NULL for bistate selects the default behavior.
 *
 *	We always try to avoid filling existing pages further than the fillfactor.
 *	This is OK since this routine is not consulted when updating a tuple and
 *	keeping it on the same page, which is the scenario fillfactor is meant
 *	to reserve space for.
 *
 *	ereport(ERROR) is allowed here, so this routine *must* be called
 *	before any (unlogged) changes are made in buffer pool.
 */
Buffer
RelationGetBufferForTuple(Relation relation, Size len,
						  Buffer otherBuffer, int options,
						  BulkInsertState bistate,
						  Buffer *vmbuffer, Buffer *vmbuffer_other)
{
	bool		use_fsm = !(options & HEAP_INSERT_SKIP_FSM);
	Buffer		buffer = InvalidBuffer;
	Page		page;
	Size		pageFreeSpace = 0,
				saveFreeSpace = 0;
	BlockNumber targetBlock,
				otherBlock;
	bool		needLock;
	int			bulk_extend_size = polar_bulk_extend_size;

	len = MAXALIGN(len);		/* be conservative */

	/* Bulk insert is not supported for updates, only inserts. */
	Assert(otherBuffer == InvalidBuffer || !bistate);

	/*
	 * POLAR: when enable preallocate_file,
	 * use fsm record blocks => if preallocate_file is enabled,
	 * use fsm record blocks
	 */
	if (polar_enable_shared_storage_mode && bulk_extend_size > 0)
		use_fsm = true;

	/*
	 * If we're gonna fail for oversize tuple, do it right away
	 */
	if (len > MaxHeapTupleSize)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("row is too big: size %zu, maximum size %zu",
						len, MaxHeapTupleSize)));

	/* Compute desired extra freespace due to fillfactor option */
	saveFreeSpace = RelationGetTargetPageFreeSpace(relation,
												   HEAP_DEFAULT_FILLFACTOR);

	if (otherBuffer != InvalidBuffer)
		otherBlock = BufferGetBlockNumber(otherBuffer);
	else
		otherBlock = InvalidBlockNumber;	/* just to keep compiler quiet */

	/*
	 * We first try to put the tuple on the same page we last inserted a tuple
	 * on, as cached in the BulkInsertState or relcache entry.  If that
	 * doesn't work, we ask the Free Space Map to locate a suitable page.
	 * Since the FSM's info might be out of date, we have to be prepared to
	 * loop around and retry multiple times. (To insure this isn't an infinite
	 * loop, we must update the FSM with the correct amount of free space on
	 * each page that proves not to be suitable.)  If the FSM has no record of
	 * a page with enough free space, we give up and extend the relation.
	 *
	 * When use_fsm is false, we either put the tuple onto the existing target
	 * page or extend the relation.
	 */
	if (len + saveFreeSpace > MaxHeapTupleSize)
	{
		/* can't fit, don't bother asking FSM */
		targetBlock = InvalidBlockNumber;
		use_fsm = false;
	}
	else if (bistate && bistate->current_buf != InvalidBuffer)
		targetBlock = BufferGetBlockNumber(bistate->current_buf);
	else
		targetBlock = RelationGetTargetBlock(relation);

	if (targetBlock == InvalidBlockNumber && use_fsm)
	{
		/*
		 * We have no cached target page, so ask the FSM for an initial
		 * target.
		 */
		targetBlock = GetPageWithFreeSpace(relation, len + saveFreeSpace);

		/*
		 * If the FSM knows nothing of the rel, try the last page before we
		 * give up and extend.  This avoids one-tuple-per-page syndrome during
		 * bootstrapping or in a recently-started system.
		 */
		if (targetBlock == InvalidBlockNumber)
		{
			BlockNumber nblocks = RelationGetNumberOfBlocks(relation);

			if (nblocks > 0)
				targetBlock = nblocks - 1;
		}
	}

loop:
	while (targetBlock != InvalidBlockNumber)
	{
		/*
		 * Read and exclusive-lock the target block, as well as the other
		 * block if one was given, taking suitable care with lock ordering and
		 * the possibility they are the same block.
		 *
		 * If the page-level all-visible flag is set, caller will need to
		 * clear both that and the corresponding visibility map bit.  However,
		 * by the time we return, we'll have x-locked the buffer, and we don't
		 * want to do any I/O while in that state.  So we check the bit here
		 * before taking the lock, and pin the page if it appears necessary.
		 * Checking without the lock creates a risk of getting the wrong
		 * answer, so we'll have to recheck after acquiring the lock.
		 */
		if (otherBuffer == InvalidBuffer)
		{
			/* easy case */
			buffer = ReadBufferBI(relation, targetBlock, bistate);
			if (PageIsAllVisible(BufferGetPage(buffer)))
				visibilitymap_pin(relation, targetBlock, vmbuffer);
			LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
		}
		else if (otherBlock == targetBlock)
		{
			/* also easy case */
			buffer = otherBuffer;
			if (PageIsAllVisible(BufferGetPage(buffer)))
				visibilitymap_pin(relation, targetBlock, vmbuffer);
			LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
		}
		else if (otherBlock < targetBlock)
		{
			/* lock other buffer first */
			buffer = ReadBuffer(relation, targetBlock);
			if (PageIsAllVisible(BufferGetPage(buffer)))
				visibilitymap_pin(relation, targetBlock, vmbuffer);
			LockBuffer(otherBuffer, BUFFER_LOCK_EXCLUSIVE);
			LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
		}
		else
		{
			/* lock target buffer first */
			buffer = ReadBuffer(relation, targetBlock);
			if (PageIsAllVisible(BufferGetPage(buffer)))
				visibilitymap_pin(relation, targetBlock, vmbuffer);
			LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
			LockBuffer(otherBuffer, BUFFER_LOCK_EXCLUSIVE);
		}

		/*
		 * We now have the target page (and the other buffer, if any) pinned
		 * and locked.  However, since our initial PageIsAllVisible checks
		 * were performed before acquiring the lock, the results might now be
		 * out of date, either for the selected victim buffer, or for the
		 * other buffer passed by the caller.  In that case, we'll need to
		 * give up our locks, go get the pin(s) we failed to get earlier, and
		 * re-lock.  That's pretty painful, but hopefully shouldn't happen
		 * often.
		 *
		 * Note that there's a small possibility that we didn't pin the page
		 * above but still have the correct page pinned anyway, either because
		 * we've already made a previous pass through this loop, or because
		 * caller passed us the right page anyway.
		 *
		 * Note also that it's possible that by the time we get the pin and
		 * retake the buffer locks, the visibility map bit will have been
		 * cleared by some other backend anyway.  In that case, we'll have
		 * done a bit of extra work for no gain, but there's no real harm
		 * done.
		 */
		if (otherBuffer == InvalidBuffer || targetBlock <= otherBlock)
			GetVisibilityMapPins(relation, buffer, otherBuffer,
								 targetBlock, otherBlock, vmbuffer,
								 vmbuffer_other);
		else
			GetVisibilityMapPins(relation, otherBuffer, buffer,
								 otherBlock, targetBlock, vmbuffer_other,
								 vmbuffer);

		/*
		 * Now we can check to see if there's enough free space here. If so,
		 * we're done.
		 */
		page = BufferGetPage(buffer);
		pageFreeSpace = PageGetHeapFreeSpace(page);
		if (len + saveFreeSpace <= pageFreeSpace)
		{
			/* use this page as future insert target, too */
			RelationSetTargetBlock(relation, targetBlock);
			return buffer;
		}

		/*
		 * Not enough space, so we must give up our page locks and pin (if
		 * any) and prepare to look elsewhere.  We don't care which order we
		 * unlock the two buffers in, so this can be slightly simpler than the
		 * code above.
		 */
		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		if (otherBuffer == InvalidBuffer)
			ReleaseBuffer(buffer);
		else if (otherBlock != targetBlock)
		{
			LockBuffer(otherBuffer, BUFFER_LOCK_UNLOCK);
			ReleaseBuffer(buffer);
		}

		/* Without FSM, always fall out of the loop and extend */
		if (!use_fsm)
			break;

		/*
		 * Update FSM as to condition of this page, and ask for another page
		 * to try.
		 */
		targetBlock = RecordAndGetPageWithFreeSpace(relation,
													targetBlock,
													pageFreeSpace,
													len + saveFreeSpace);
	}

	/*
	 * Have to extend the relation.
	 *
	 * We have to use a lock to ensure no one else is extending the rel at the
	 * same time, else we will both try to initialize the same new page.  We
	 * can skip locking for new or temp relations, however, since no one else
	 * could be accessing them.
	 */
	needLock = !RELATION_IS_LOCAL(relation);

	/*
	 * If we need the lock but are not able to acquire it immediately, we'll
	 * consider extending the relation by multiple blocks at a time to manage
	 * contention on the relation extension lock.  However, this only makes
	 * sense if we're using the FSM; otherwise, there's no point.
	 */
	if (needLock)
	{
		if (!use_fsm)
			LockRelationForExtension(relation, ExclusiveLock);
		else if (!ConditionalLockRelationForExtension(relation, ExclusiveLock))
		{
			/* Couldn't get the lock immediately; wait for it. */
			LockRelationForExtension(relation, ExclusiveLock);

			/*
			 * Check if some other backend has extended a block for us while
			 * we were waiting on the lock.
			 */
			targetBlock = GetPageWithFreeSpace(relation, len + saveFreeSpace);

			/*
			 * If some other waiter has already extended the relation, we
			 * don't need to do so; just use the existing freespace.
			 */
			if (targetBlock != InvalidBlockNumber)
			{
				UnlockRelationForExtension(relation, ExclusiveLock);
				goto loop;
			}

			/* POLAR: If we don't need file prealloc, use origin normal method */
			if (!(polar_enable_shared_storage_mode && bulk_extend_size > 0))
			{
				/* Time to bulk-extend. */
				RelationAddExtraBlocks(relation, bistate);
			}
		}
	}

	/*
	 * In addition to whatever extension we performed above, we always add at
	 * least one block to satisfy our own request.
	 *
	 * XXX This does an lseek - rather expensive - but at the moment it is the
	 * only way to accurately determine how many blocks are in a relation.  Is
	 * it worth keeping an accurate file length in shared memory someplace,
	 * rather than relying on the kernel to do it for us?
	 */
	/* POLAR: preallocate multiple block and only use one block */
	if (polar_enable_shared_storage_mode && bulk_extend_size > 0)
		buffer = polar_relation_add_extra_blocks_and_return_last_buffer(relation, bistate);
	else
		buffer = ReadBufferBI(relation, P_NEW, bistate);

	/*
	 * We can be certain that locking the otherBuffer first is OK, since it
	 * must have a lower page number.
	 */
	if (otherBuffer != InvalidBuffer)
		LockBuffer(otherBuffer, BUFFER_LOCK_EXCLUSIVE);

	/*
	 * Now acquire lock on the new page.
	 */
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

	/*
	 * Release the file-extension lock; it's now OK for someone else to extend
	 * the relation some more.  Note that we cannot release this lock before
	 * we have buffer lock on the new page, or we risk a race condition
	 * against vacuumlazy.c --- see comments therein.
	 */
	if (needLock)
		UnlockRelationForExtension(relation, ExclusiveLock);

	/*
	 * We need to initialize the empty new page.  Double-check that it really
	 * is empty (this should never happen, but if it does we don't want to
	 * risk wiping out valid data).
	 */
	page = BufferGetPage(buffer);

	if (!PageIsNew(page))
		elog(ERROR, "page %u of relation \"%s\" should be empty but is not",
			 BufferGetBlockNumber(buffer),
			 RelationGetRelationName(relation));

	PageInit(page, BufferGetPageSize(buffer), 0);

	if (len > PageGetHeapFreeSpace(page))
	{
		/* We should not get here given the test at the top */
		elog(PANIC, "tuple is too big: size %zu", len);
	}

	/*
	 * Remember the new page as our target for future insertions.
	 *
	 * XXX should we enter the new page into the free space map immediately,
	 * or just keep it for this backend's exclusive use in the short run
	 * (until VACUUM sees it)?	Seems to depend on whether you expect the
	 * current backend to make more insertions or not, which is probably a
	 * good bet most of the time.  So for now, don't add it to FSM yet.
	 */
	RelationSetTargetBlock(relation, BufferGetBlockNumber(buffer));

	return buffer;
}

/*
 * POLAR:
 * polar_relation_add_extra_blocks_and_return_last_buffer - Extend a relation by multiple blocks
 * to avoid future contention on the relation extension lock and expensive pfs extend operation.
 *
 * If bistate isn't NULL, bistate->current_buf is assigned to last buffer alloced.
 *
 * Returns: the last buffer alloced is returned, which is zero-page and has been pinned.
 *          The other alloced buffer is initialized、marked dirty、added to fsm for later use.
 *          Does not return on error --- elog's instead.
 *
 * Assume when this function is called, that reln has been opened already and
 * relation extension lock has been acquired if relation is not local.
 */
static Buffer
polar_relation_add_extra_blocks_and_return_last_buffer(Relation relation, BulkInsertState bistate)
{
	BlockNumber first_block_num_extended = InvalidBlockNumber;
	int			block_count = 0;
	Buffer	    last_buffer = InvalidBuffer;
	Buffer      *buffers = NULL;
	int         index = 0;
	char        *bulk_buf_block = NULL;
	BufferAccessStrategy strategy = NULL;
	bool		need_flog;

	need_flog = polar_enable_fra(fra_instance);

	if (bistate != NULL)
	{
		strategy = bistate->strategy;
		if (bistate->current_buf != InvalidBuffer)
		{
			/* because this function just extend rel, drop the old buffer */
			ReleaseBuffer(bistate->current_buf);
			bistate->current_buf = InvalidBuffer;
		}
	}

	/* Open it at the smgr level if not already done */
	RelationOpenSmgr(relation);

	PG_TRY();
	{
		/* init bulk extend backend-local-variable */
		polar_smgr_init_bulk_extend(relation->rd_smgr, MAIN_FORKNUM);

		first_block_num_extended = relation->rd_smgr->polar_nblocks_faked_for_bulk_extend[MAIN_FORKNUM];
		block_count = Min(polar_bulk_extend_size, (BlockNumber) RELSEG_SIZE - (first_block_num_extended % ((BlockNumber) RELSEG_SIZE)));
		if (block_count < 1)
			block_count = 1;

		/* avoid small table bloat */
		if (first_block_num_extended < polar_min_bulk_extend_table_size)
			block_count = 1;

		bulk_buf_block = (char*)palloc0(block_count * BLCKSZ);
		buffers = (Buffer*)palloc(block_count * sizeof(Buffer));

		for (index = 0; index < block_count; index++)
		{
			/*
			 * Extend by one page.  This should generally match the main-line
			 * extension code in RelationGetBufferForTuple, except that we hold
			 * the relation extension lock throughout.
			 */
			buffers[index] = ReadBufferExtended(relation, MAIN_FORKNUM, P_NEW, RBM_NORMAL, strategy);
		}
	}
	PG_CATCH();
	{
		/* error recovery, very important, reset bulk extend backend-local-variable */
		if (relation->rd_smgr != NULL)
		{
			polar_smgr_clear_bulk_extend(relation->rd_smgr, MAIN_FORKNUM);
		}
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* reset bulk extend backend-local-variable */
	polar_smgr_clear_bulk_extend(relation->rd_smgr, MAIN_FORKNUM);

	/* bulk extend polar_store */
	smgrextendbatch(relation->rd_smgr, MAIN_FORKNUM, first_block_num_extended, block_count, bulk_buf_block, false);
	pfree(bulk_buf_block);

	/*
	 * Until here, all alloced buffer are zero page(BM_VALID, non-BM_DIRTY). It is safe.
	 * 1. They are not initialized, still zero-page.
	 *      a. Only the caller of P_NEW and Vacuum have obligation and the right
	 *         to initialize a page.
	 *      b. Other backend would't initialize them.
	 *      c. Before relation extension lock are freed, Vacuum can't initialize them.
	 *         When Vacuum initializes a zero-page, it will acquire relation extension lock,
	 *         and then recheck if-zero-page.
	 * 2. They could't be inserted tuple to.
	 *      a. Because PageGetFreeSpace(page) of zero-page is 0.
	 * 3. Full Table Scan is OK.
	 *      a. Because PageGetMaxOffsetNumber(page) of zero-page is 0.
	 * 4. They are pinned, can't be evicted.
	 */

	/* process left (block_count-1) blocks, skip last block*/
	block_count--;
	for(index = 0; index < block_count; index++)
	{
		Buffer		buffer;
		Page		page;
		Size		freespace;

		buffer = buffers[index];
		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buffer);
		if (!PageIsNew(page))
			elog(ERROR, "page %u of relation \"%s\" should be empty but is not",
				 BufferGetBlockNumber(buffer),
				 RelationGetRelationName(relation));

		PageInit(page, BufferGetPageSize(buffer), 0);

		/* Insert the flashback log record for relation bulk extend */
		if (need_flog)
			polar_flog_rel_bulk_extend(flog_instance, buffer);

		/*
		 * We mark all the new buffers dirty, but do nothing to write them
		 * out; they'll probably get used soon, and even if they are not, a
		 * crash will leave an okay all-zeroes page on disk.
		 */
		MarkBufferDirty(buffer);

		/* we'll need this info below */
		Assert((first_block_num_extended + index) == BufferGetBlockNumber(buffer));
		freespace = PageGetHeapFreeSpace(page);

		UnlockReleaseBuffer(buffer);

		/*
		 * Immediately update the bottom level of the FSM.  This has a good
		 * chance of making this page visible to other concurrently inserting
		 * backends, and we want that to happen without delay.
		 */
		RecordPageWithFreeSpace(relation, first_block_num_extended + index, freespace);
	}
	/*
	 * Updating the upper levels of the free space map is too expensive to do
	 * for every block, but it's worth doing once at the end to make sure that
	 * subsequent insertion activity sees all of those nifty free pages we
	 * just inserted. skip last block.
	 */
	if (block_count > 0)
		FreeSpaceMapVacuumRange(relation, first_block_num_extended, first_block_num_extended + block_count);

	/* last block */
	last_buffer = buffers[block_count];
	/* Save the selected block as target for future inserts */
	if (bistate != NULL)
	{
		IncrBufferRefCount(last_buffer);
		bistate->current_buf = last_buffer;
	}
	pfree(buffers);
	return last_buffer;
}
