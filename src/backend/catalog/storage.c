/*-------------------------------------------------------------------------
 *
 * storage.c
 *	  code to create and destroy physical storage for relations
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/catalog/storage.c
 *
 * NOTES
 *	  Some of this code used to be in storage/smgr/smgr.c, and the
 *	  function names still reflect that.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/parallel.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "miscadmin.h"
#include "replication/syncrep.h"
#include "storage/bulk_write.h"
#include "storage/freespace.h"
#include "storage/smgr.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/* GUC variables */
int			wal_skip_threshold = 2048;	/* in kilobytes */
bool		polar_hold_truncate_interrupt = false;

/*
 * We keep a list of all relations (represented as RelFileNode values)
 * that have been created or deleted in the current transaction.  When
 * a relation is created, we create the physical file immediately, but
 * remember it so that we can delete the file again if the current
 * transaction is aborted.  Conversely, a deletion request is NOT
 * executed immediately, but is just entered in the list.  When and if
 * the transaction commits, we can delete the physical file.
 *
 * To handle subtransactions, every entry is marked with its transaction
 * nesting level.  At subtransaction commit, we reassign the subtransaction's
 * entries to the parent nesting level.  At subtransaction abort, we can
 * immediately execute the abort-time actions for all entries of the current
 * nesting level.
 *
 * NOTE: the list is kept in TopMemoryContext to be sure it won't disappear
 * unbetimes.  It'd probably be OK to keep it in TopTransactionContext,
 * but I'm being paranoid.
 */

typedef struct PendingRelDelete
{
	RelFileNode relnode;		/* relation that may need to be deleted */
	BackendId	backend;		/* InvalidBackendId if not a temp rel */
	bool		atCommit;		/* T=delete at commit; F=delete at abort */
	int			nestLevel;		/* xact nesting level of request */
	struct PendingRelDelete *next;	/* linked-list link */
} PendingRelDelete;

typedef struct PendingRelSync
{
	RelFileNode rnode;
	bool		is_truncated;	/* Has the file experienced truncation? */
} PendingRelSync;

static PendingRelDelete *pendingDeletes = NULL; /* head of linked list */
static HTAB *pendingSyncHash = NULL;


/*
 * AddPendingSync
 *		Queue an at-commit fsync.
 */
static void
AddPendingSync(const RelFileNode *rnode)
{
	PendingRelSync *pending;
	bool		found;

	/* create the hash if not yet */
	if (!pendingSyncHash)
	{
		HASHCTL		ctl;

		ctl.keysize = sizeof(RelFileNode);
		ctl.entrysize = sizeof(PendingRelSync);
		ctl.hcxt = TopTransactionContext;
		pendingSyncHash = hash_create("pending sync hash", 16, &ctl,
									  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	pending = hash_search(pendingSyncHash, rnode, HASH_ENTER, &found);
	Assert(!found);
	pending->is_truncated = false;
}

/*
 * RelationCreateStorage
 *		Create physical storage for a relation.
 *
 * Create the underlying disk file storage for the relation. This only
 * creates the main fork; additional forks are created lazily by the
 * modules that need them.
 *
 * This function is transactional. The creation is WAL-logged, and if the
 * transaction aborts later on, the storage will be destroyed.  A caller
 * that does not want the storage to be destroyed in case of an abort may
 * pass register_delete = false.
 */
SMgrRelation
RelationCreateStorage(RelFileNode rnode, char relpersistence,
					  bool register_delete)
{
	SMgrRelation srel;
	BackendId	backend;
	bool		needs_wal;

	Assert(!IsInParallelMode());	/* couldn't update pendingSyncHash */

	switch (relpersistence)
	{
		case RELPERSISTENCE_TEMP:
			backend = BackendIdForTempRelations();
			needs_wal = false;
			break;
		case RELPERSISTENCE_UNLOGGED:
			backend = InvalidBackendId;
			needs_wal = false;
			break;
		case RELPERSISTENCE_PERMANENT:
			backend = InvalidBackendId;
			needs_wal = true;
			break;
		default:
			elog(ERROR, "invalid relpersistence: %c", relpersistence);
			return NULL;		/* placate compiler */
	}

	srel = smgropen(rnode, backend);
	smgrcreate(srel, MAIN_FORKNUM, false);

	if (needs_wal)
		log_smgrcreate(&srel->smgr_rnode.node, MAIN_FORKNUM);

	/*
	 * Add the relation to the list of stuff to delete at abort, if we are
	 * asked to do so.
	 */
	if (register_delete)
	{
		PendingRelDelete *pending;

		pending = (PendingRelDelete *)
			MemoryContextAlloc(TopMemoryContext, sizeof(PendingRelDelete));
		pending->relnode = rnode;
		pending->backend = backend;
		pending->atCommit = false;	/* delete if abort */
		pending->nestLevel = GetCurrentTransactionNestLevel();
		pending->next = pendingDeletes;
		pendingDeletes = pending;
	}

	if (relpersistence == RELPERSISTENCE_PERMANENT && !XLogIsNeeded())
	{
		Assert(backend == InvalidBackendId);
		AddPendingSync(&rnode);
	}

	return srel;
}

/*
 * Perform XLogInsert of an XLOG_SMGR_CREATE record to WAL.
 */
void
log_smgrcreate(const RelFileNode *rnode, ForkNumber forkNum)
{
	xl_smgr_create xlrec;

	/*
	 * Make an XLOG entry reporting the file creation.
	 */
	xlrec.rnode = *rnode;
	xlrec.forkNum = forkNum;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, sizeof(xlrec));
	XLogInsert(RM_SMGR_ID, XLOG_SMGR_CREATE | XLR_SPECIAL_REL_UPDATE);
}

/*
 * RelationDropStorage
 *		Schedule unlinking of physical storage at transaction commit.
 */
void
RelationDropStorage(Relation rel)
{
	PendingRelDelete *pending;

	/* Add the relation to the list of stuff to delete at commit */
	pending = (PendingRelDelete *)
		MemoryContextAlloc(TopMemoryContext, sizeof(PendingRelDelete));
	pending->relnode = rel->rd_node;
	pending->backend = rel->rd_backend;
	pending->atCommit = true;	/* delete if commit */
	pending->nestLevel = GetCurrentTransactionNestLevel();
	pending->next = pendingDeletes;
	pendingDeletes = pending;

	/*
	 * NOTE: if the relation was created in this transaction, it will now be
	 * present in the pending-delete list twice, once with atCommit true and
	 * once with atCommit false.  Hence, it will be physically deleted at end
	 * of xact in either case (and the other entry will be ignored by
	 * smgrDoPendingDeletes, so no error will occur).  We could instead remove
	 * the existing list entry and delete the physical file immediately, but
	 * for now I'll keep the logic simple.
	 */

	RelationCloseSmgr(rel);
}

/*
 * RelationPreserveStorage
 *		Mark a relation as not to be deleted after all.
 *
 * We need this function because relation mapping changes are committed
 * separately from commit of the whole transaction, so it's still possible
 * for the transaction to abort after the mapping update is done.
 * When a new physical relation is installed in the map, it would be
 * scheduled for delete-on-abort, so we'd delete it, and be in trouble.
 * The relation mapper fixes this by telling us to not delete such relations
 * after all as part of its commit.
 *
 * We also use this to reuse an old build of an index during ALTER TABLE, this
 * time removing the delete-at-commit entry.
 *
 * No-op if the relation is not among those scheduled for deletion.
 */
void
RelationPreserveStorage(RelFileNode rnode, bool atCommit)
{
	PendingRelDelete *pending;
	PendingRelDelete *prev;
	PendingRelDelete *next;

	prev = NULL;
	for (pending = pendingDeletes; pending != NULL; pending = next)
	{
		next = pending->next;
		if (RelFileNodeEquals(rnode, pending->relnode)
			&& pending->atCommit == atCommit)
		{
			/* unlink and delete list entry */
			if (prev)
				prev->next = next;
			else
				pendingDeletes = next;
			pfree(pending);
			/* prev does not change */
		}
		else
		{
			/* unrelated entry, don't touch it */
			prev = pending;
		}
	}
}

/*
 * RelationTruncate
 *		Physically truncate a relation to the specified number of blocks.
 *
 * This includes getting rid of any buffers for the blocks that are to be
 * dropped.
 */
void
RelationTruncate(Relation rel, BlockNumber nblocks)
{
	bool		fsm;
	bool		vm;
	bool		need_fsm_vacuum = false;
	ForkNumber	forks[MAX_FORKNUM];
	BlockNumber old_blocks[MAX_FORKNUM];
	BlockNumber blocks[MAX_FORKNUM];
	int			nforks = 0;
	SMgrRelation reln;

	/* POLAR: If this flag is true,we will disable cancel signal */
	bool		disable_cancel = false;

	/*
	 * Make sure smgr_targblock etc aren't pointing somewhere past new end.
	 * (Note: don't rely on this reln pointer below this loop.)
	 */
	reln = RelationGetSmgr(rel);
	reln->smgr_targblock = InvalidBlockNumber;
	for (int i = 0; i <= MAX_FORKNUM; ++i)
		reln->smgr_cached_nblocks[i] = InvalidBlockNumber;

	/* Prepare for truncation of MAIN fork of the relation */
	forks[nforks] = MAIN_FORKNUM;
	old_blocks[nforks] = smgrnblocks_ensure_opened(reln, MAIN_FORKNUM);
	blocks[nforks] = nblocks;
	nforks++;

	/* Prepare for truncation of the FSM if it exists */
	fsm = smgrexists(RelationGetSmgr(rel), FSM_FORKNUM);
	if (fsm)
	{
		blocks[nforks] = FreeSpaceMapPrepareTruncateRel(rel, nblocks);
		if (BlockNumberIsValid(blocks[nforks]))
		{
			forks[nforks] = FSM_FORKNUM;
			old_blocks[nforks] = smgrnblocks_ensure_opened(reln, FSM_FORKNUM);
			nforks++;
			need_fsm_vacuum = true;
		}
	}

	/* Prepare for truncation of the visibility map too if it exists */
	vm = smgrexists(RelationGetSmgr(rel), VISIBILITYMAP_FORKNUM);
	if (vm)
	{
		blocks[nforks] = visibilitymap_prepare_truncate(rel, nblocks);
		if (BlockNumberIsValid(blocks[nforks]))
		{
			forks[nforks] = VISIBILITYMAP_FORKNUM;
			old_blocks[nforks] = smgrnblocks_ensure_opened(reln, VISIBILITYMAP_FORKNUM);
			nforks++;
		}
	}

	if (polar_enable_sync_ddl && reln->smgr_rnode.backend == InvalidBackendId)
		polar_wait_ddl_lock();

	RelationPreTruncate(rel);

	/*
	 * The code which follows can interact with concurrent checkpoints in two
	 * separate ways.
	 *
	 * First, the truncation operation might drop buffers that the checkpoint
	 * otherwise would have flushed. If it does, then it's essential that the
	 * files actually get truncated on disk before the checkpoint record is
	 * written. Otherwise, if reply begins from that checkpoint, the
	 * to-be-truncated blocks might still exist on disk but have older
	 * contents than expected, which can cause replay to fail. It's OK for the
	 * blocks to not exist on disk at all, but not for them to have the wrong
	 * contents. For this reason, we need to set DELAY_CHKPT_COMPLETE while
	 * this code executes.
	 *
	 * Second, the call to smgrtruncate() below will in turn call
	 * RegisterSyncRequest(). We need the sync request created by that call to
	 * be processed before the checkpoint completes. CheckPointGuts() will
	 * call ProcessSyncRequests(), but if we register our sync request after
	 * that happens, then the WAL record for the truncation could end up
	 * preceding the checkpoint record, while the actual sync doesn't happen
	 * until the next checkpoint. To prevent that, we need to set
	 * DELAY_CHKPT_START here. That way, if the XLOG_SMGR_TRUNCATE precedes
	 * the redo pointer of a concurrent checkpoint, we're guaranteed that the
	 * corresponding sync request will be processed before the checkpoint
	 * completes.
	 */
	Assert((MyProc->delayChkptFlags & (DELAY_CHKPT_START | DELAY_CHKPT_COMPLETE)) == 0);
	MyProc->delayChkptFlags |= DELAY_CHKPT_START | DELAY_CHKPT_COMPLETE;

	/*
	 * We WAL-log the truncation first and then truncate in a critical
	 * section. Truncation drops buffers, even if dirty, and then truncates
	 * disk files. All of that work needs to complete before the lock is
	 * released, or else old versions of pages on disk that are missing recent
	 * changes would become accessible again.  We'll try the whole operation
	 * again in crash recovery if we panic, but even then we can't give up
	 * because we don't want standbys' relation sizes to diverge and break
	 * replay or visibility invariants downstream.  The critical section also
	 * suppresses interrupts.
	 *
	 * (See also pg_visibilitymap.c if changing this code.)
	 */
	START_CRIT_SECTION();

	if (RelationNeedsWAL(rel))
	{
		/*
		 * Make an XLOG entry reporting the file truncation.
		 */
		XLogRecPtr	lsn;
		xl_smgr_truncate xlrec;

		/*
		 * POLAR: Disable cancel signal during writing truacte XLOG and
		 * truncating. If standby receive truncate log but master failed to
		 * trucate file, standby will crash when master write to these blocks
		 * which truncated in standby node.
		 */
		if (polar_hold_truncate_interrupt)
		{
			disable_cancel = true;
			HOLD_INTERRUPTS();
		}

		xlrec.blkno = nblocks;
		xlrec.rnode = rel->rd_node;
		xlrec.flags = SMGR_TRUNCATE_ALL;

		XLogBeginInsert();
		XLogRegisterData((char *) &xlrec, sizeof(xlrec));

		lsn = XLogInsert(RM_SMGR_ID,
						 XLOG_SMGR_TRUNCATE | XLR_SPECIAL_REL_UPDATE);

		/*
		 * Flush, because otherwise the truncation of the main relation might
		 * hit the disk before the WAL record, and the truncation of the FSM
		 * or visibility map. If we crashed during that window, we'd be left
		 * with a truncated heap, but the FSM or visibility map would still
		 * contain entries for the non-existent heap pages, and standbys would
		 * also never replay the truncation.
		 */
		XLogFlush(lsn);
	}

	/*
	 * This will first remove any buffers from the buffer pool that should no
	 * longer exist after truncation is complete, and then truncate the
	 * corresponding files on disk.
	 */
	smgrtruncate2(RelationGetSmgr(rel), forks, nforks, old_blocks, blocks);

	END_CRIT_SECTION();

	/* We've done all the critical work, so checkpoints are OK now. */
	MyProc->delayChkptFlags &= ~(DELAY_CHKPT_START | DELAY_CHKPT_COMPLETE);

	/* POLAR: Resume to enable cancel signal */
	if (disable_cancel)
	{
		RESUME_INTERRUPTS();
		CHECK_FOR_INTERRUPTS();
	}

	/*
	 * Update upper-level FSM pages to account for the truncation. This is
	 * important because the just-truncated pages were likely marked as
	 * all-free, and would be preferentially selected.
	 *
	 * NB: There's no point in delaying checkpoints until this is done.
	 * Because the FSM is not WAL-logged, we have to be prepared for the
	 * possibility of corruption after a crash anyway.
	 */
	if (need_fsm_vacuum)
		FreeSpaceMapVacuumRange(rel, nblocks, InvalidBlockNumber);
}

/*
 * RelationPreTruncate
 *		Perform AM-independent work before a physical truncation.
 *
 * If an access method's relation_nontransactional_truncate does not call
 * RelationTruncate(), it must call this before decreasing the table size.
 */
void
RelationPreTruncate(Relation rel)
{
	PendingRelSync *pending;

	if (!pendingSyncHash)
		return;

	pending = hash_search(pendingSyncHash,
						  &(RelationGetSmgr(rel)->smgr_rnode.node),
						  HASH_FIND, NULL);
	if (pending)
		pending->is_truncated = true;
}

/*
 * Copy a fork's data, block by block.
 *
 * Note that this requires that there is no dirty data in shared buffers. If
 * it's possible that there are, callers need to flush those using
 * e.g. FlushRelationBuffers(rel).
 *
 * Also note that this is frequently called via locutions such as
 *		RelationCopyStorage(RelationGetSmgr(rel), ...);
 * That's safe only because we perform only smgr and WAL operations here.
 * If we invoked anything else, a relcache flush could cause our SMgrRelation
 * argument to become a dangling pointer.
 */
void
RelationCopyStorage(SMgrRelation src, SMgrRelation dst,
					ForkNumber forkNum, char relpersistence)
{
	bool		use_wal;
	bool		copying_initfork;
	BlockNumber nblocks;
	BlockNumber blkno;
	BulkWriteState *bulkstate;
	void	   *buffer;
	int			block_count;
	int			max_block_count;

	/*
	 * The init fork for an unlogged relation in many respects has to be
	 * treated the same as normal relation, changes need to be WAL logged and
	 * it needs to be synced to disk.
	 */
	copying_initfork = relpersistence == RELPERSISTENCE_UNLOGGED &&
		forkNum == INIT_FORKNUM;

	/*
	 * We need to log the copied data in WAL iff WAL archiving/streaming is
	 * enabled AND it's a permanent relation.  This gives the same answer as
	 * "RelationNeedsWAL(rel) || copying_initfork", because we know the
	 * current operation created a new relfilenode.
	 */
	use_wal = XLogIsNeeded() &&
		(relpersistence == RELPERSISTENCE_PERMANENT || copying_initfork);

	max_block_count = Max(1, polar_bulk_read_size);
	buffer = palloc_aligned(max_block_count * BLCKSZ, PG_IO_ALIGN_SIZE, 0);

	bulkstate = smgr_bulk_start_smgr(dst, forkNum, use_wal, relpersistence);

	nblocks = smgrnblocks(src, forkNum);

	for (blkno = 0; blkno < nblocks; blkno += block_count)
	{
		BulkWriteBuffer buf;

		/* If we got a cancel signal during the copy of the data, quit */
		CHECK_FOR_INTERRUPTS();

		block_count = Min(max_block_count, nblocks - blkno);

		polar_smgrbulkread(src, forkNum, blkno, block_count, buffer);

		for (int i = 0; i < block_count; i++)
		{
			BlockNumber cur_blkno = blkno + i;
			Page		page = (Page) ((char *) buffer + i * BLCKSZ);

			if (!PageIsVerifiedExtended(page, cur_blkno,
										PIV_LOG_WARNING | PIV_REPORT_STAT))
			{
				/*
				 * For paranoia's sake, capture the file path before invoking
				 * the ereport machinery.  This guards against the possibility
				 * of a relcache flush caused by, e.g., an errcontext
				 * callback. (errcontext callbacks shouldn't be risking any
				 * such thing, but people have been known to forget that
				 * rule.)
				 */
				char	   *relpath = relpathbackend(src->smgr_rnode.node,
													 src->smgr_rnode.backend,
													 forkNum);

				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("invalid page in block %u of relation %s",
								cur_blkno, relpath)));
			}

			buf = smgr_bulk_get_buf(bulkstate);

			memcpy(buf, page, BLCKSZ);

			/*
			 * Queue the page for WAL-logging and writing out.  Unfortunately
			 * we don't know what kind of a page this is, so we have to log
			 * the full page including any unused space.
			 */
			smgr_bulk_write(bulkstate, cur_blkno, buf, false);
		}
	}
	Assert(blkno == nblocks);

	smgr_bulk_finish(bulkstate);

	pfree(buffer);
}

/*
 * RelFileNodeSkippingWAL
 *		Check if a BM_PERMANENT relfilenode is using WAL.
 *
 * Changes of certain relfilenodes must not write WAL; see "Skipping WAL for
 * New RelFileNode" in src/backend/access/transam/README.  Though it is known
 * from Relation efficiently, this function is intended for the code paths not
 * having access to Relation.
 */
bool
RelFileNodeSkippingWAL(RelFileNode rnode)
{
	if (!pendingSyncHash ||
		hash_search(pendingSyncHash, &rnode, HASH_FIND, NULL) == NULL)
		return false;

	return true;
}

/*
 * EstimatePendingSyncsSpace
 *		Estimate space needed to pass syncs to parallel workers.
 */
Size
EstimatePendingSyncsSpace(void)
{
	long		entries;

	entries = pendingSyncHash ? hash_get_num_entries(pendingSyncHash) : 0;
	return mul_size(1 + entries, sizeof(RelFileNode));
}

/*
 * SerializePendingSyncs
 *		Serialize syncs for parallel workers.
 */
void
SerializePendingSyncs(Size maxSize, char *startAddress)
{
	HTAB	   *tmphash;
	HASHCTL		ctl;
	HASH_SEQ_STATUS scan;
	PendingRelSync *sync;
	PendingRelDelete *delete;
	RelFileNode *src;
	RelFileNode *dest = (RelFileNode *) startAddress;

	if (!pendingSyncHash)
		goto terminate;

	/* Create temporary hash to collect active relfilenodes */
	ctl.keysize = sizeof(RelFileNode);
	ctl.entrysize = sizeof(RelFileNode);
	ctl.hcxt = CurrentMemoryContext;
	tmphash = hash_create("tmp relfilenodes",
						  hash_get_num_entries(pendingSyncHash), &ctl,
						  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	/* collect all rnodes from pending syncs */
	hash_seq_init(&scan, pendingSyncHash);
	while ((sync = (PendingRelSync *) hash_seq_search(&scan)))
		(void) hash_search(tmphash, &sync->rnode, HASH_ENTER, NULL);

	/* remove deleted rnodes */
	for (delete = pendingDeletes; delete != NULL; delete = delete->next)
		if (delete->atCommit)
			(void) hash_search(tmphash, (void *) &delete->relnode,
							   HASH_REMOVE, NULL);

	hash_seq_init(&scan, tmphash);
	while ((src = (RelFileNode *) hash_seq_search(&scan)))
		*dest++ = *src;

	hash_destroy(tmphash);

terminate:
	MemSet(dest, 0, sizeof(RelFileNode));
}

/*
 * RestorePendingSyncs
 *		Restore syncs within a parallel worker.
 *
 * RelationNeedsWAL() and RelFileNodeSkippingWAL() must offer the correct
 * answer to parallel workers.  Only smgrDoPendingSyncs() reads the
 * is_truncated field, at end of transaction.  Hence, don't restore it.
 */
void
RestorePendingSyncs(char *startAddress)
{
	RelFileNode *rnode;

	Assert(pendingSyncHash == NULL);
	for (rnode = (RelFileNode *) startAddress; rnode->relNode != 0; rnode++)
		AddPendingSync(rnode);
}

/*
 *	smgrDoPendingDeletes() -- Take care of relation deletes at end of xact.
 *
 * This also runs when aborting a subxact; we want to clean up a failed
 * subxact immediately.
 *
 * Note: It's possible that we're being asked to remove a relation that has
 * no physical storage in any fork. In particular, it's possible that we're
 * cleaning up an old temporary relation for which RemovePgTempFiles has
 * already recovered the physical storage.
 */
void
smgrDoPendingDeletes(bool isCommit)
{
	int			nestLevel = GetCurrentTransactionNestLevel();
	PendingRelDelete *pending;
	PendingRelDelete *prev;
	PendingRelDelete *next;
	int			nrels = 0,
				maxrels = 0;
	SMgrRelation *srels = NULL;

	prev = NULL;
	for (pending = pendingDeletes; pending != NULL; pending = next)
	{
		next = pending->next;
		if (pending->nestLevel < nestLevel)
		{
			/* outer-level entries should not be processed yet */
			prev = pending;
		}
		else
		{
			/* unlink list entry first, so we don't retry on failure */
			if (prev)
				prev->next = next;
			else
				pendingDeletes = next;
			/* do deletion if called for */
			if (pending->atCommit == isCommit)
			{
				SMgrRelation srel;

				srel = smgropen(pending->relnode, pending->backend);

				/* allocate the initial array, or extend it, if needed */
				if (maxrels == 0)
				{
					maxrels = 8;
					srels = palloc(sizeof(SMgrRelation) * maxrels);
				}
				else if (maxrels <= nrels)
				{
					maxrels *= 2;
					srels = repalloc(srels, sizeof(SMgrRelation) * maxrels);
				}

				srels[nrels++] = srel;
			}
			/* must explicitly free the list entry */
			pfree(pending);
			/* prev does not change */
		}
	}

	if (nrels > 0)
	{
		smgrdounlinkall(srels, nrels, false);

		for (int i = 0; i < nrels; i++)
			smgrclose(srels[i]);

		pfree(srels);
	}
}

/*
 *	smgrDoPendingSyncs() -- Take care of relation syncs at end of xact.
 */
void
smgrDoPendingSyncs(bool isCommit, bool isParallelWorker)
{
	PendingRelDelete *pending;
	int			nrels = 0,
				maxrels = 0;
	SMgrRelation *srels = NULL;
	HASH_SEQ_STATUS scan;
	PendingRelSync *pendingsync;

	Assert(GetCurrentTransactionNestLevel() == 1);

	if (!pendingSyncHash)
		return;					/* no relation needs sync */

	/* Abort -- just throw away all pending syncs */
	if (!isCommit)
	{
		pendingSyncHash = NULL;
		return;
	}

	AssertPendingSyncs_RelationCache();

	/* Parallel worker -- just throw away all pending syncs */
	if (isParallelWorker)
	{
		pendingSyncHash = NULL;
		return;
	}

	/* Skip syncing nodes that smgrDoPendingDeletes() will delete. */
	for (pending = pendingDeletes; pending != NULL; pending = pending->next)
		if (pending->atCommit)
			(void) hash_search(pendingSyncHash, (void *) &pending->relnode,
							   HASH_REMOVE, NULL);

	hash_seq_init(&scan, pendingSyncHash);
	while ((pendingsync = (PendingRelSync *) hash_seq_search(&scan)))
	{
		ForkNumber	fork;
		BlockNumber nblocks[MAX_FORKNUM + 1];
		uint64		total_blocks = 0;
		SMgrRelation srel;

		srel = smgropen(pendingsync->rnode, InvalidBackendId);

		/*
		 * We emit newpage WAL records for smaller relations.
		 *
		 * Small WAL records have a chance to be emitted along with other
		 * backends' WAL records.  We emit WAL records instead of syncing for
		 * files that are smaller than a certain threshold, expecting faster
		 * commit.  The threshold is defined by the GUC wal_skip_threshold.
		 */
		if (!pendingsync->is_truncated)
		{
			for (fork = 0; fork <= MAX_FORKNUM; fork++)
			{
				if (smgrexists(srel, fork))
				{
					BlockNumber n = smgrnblocks(srel, fork);

					/* we shouldn't come here for unlogged relations */
					Assert(fork != INIT_FORKNUM);
					nblocks[fork] = n;
					total_blocks += n;
				}
				else
					nblocks[fork] = InvalidBlockNumber;
			}
		}

		/*
		 * Sync file or emit WAL records for its contents.
		 *
		 * Although we emit WAL record if the file is small enough, do file
		 * sync regardless of the size if the file has experienced a
		 * truncation. It is because the file would be followed by trailing
		 * garbage blocks after a crash recovery if, while a past longer file
		 * had been flushed out, we omitted syncing-out of the file and
		 * emitted WAL instead.  You might think that we could choose WAL if
		 * the current main fork is longer than ever, but there's a case where
		 * main fork is longer than ever but FSM fork gets shorter.
		 */
		if (pendingsync->is_truncated ||
			total_blocks >= wal_skip_threshold * (uint64) 1024 / BLCKSZ)
		{
			/* allocate the initial array, or extend it, if needed */
			if (maxrels == 0)
			{
				maxrels = 8;
				srels = palloc(sizeof(SMgrRelation) * maxrels);
			}
			else if (maxrels <= nrels)
			{
				maxrels *= 2;
				srels = repalloc(srels, sizeof(SMgrRelation) * maxrels);
			}

			srels[nrels++] = srel;
		}
		else
		{
			/* Emit WAL records for all blocks.  The file is small enough. */
			for (fork = 0; fork <= MAX_FORKNUM; fork++)
			{
				int			n = nblocks[fork];
				Relation	rel;

				if (!BlockNumberIsValid(n))
					continue;

				/*
				 * Emit WAL for the whole file.  Unfortunately we don't know
				 * what kind of a page this is, so we have to log the full
				 * page including any unused space.  ReadBufferExtended()
				 * counts some pgstat events; unfortunately, we discard them.
				 */
				rel = CreateFakeRelcacheEntry(srel->smgr_rnode.node);
				log_newpage_range(rel, fork, 0, n, false);
				FreeFakeRelcacheEntry(rel);
			}
		}
	}

	pendingSyncHash = NULL;

	if (nrels > 0)
	{
		smgrdosyncall(srels, nrels);
		pfree(srels);
	}
}

/*
 * smgrGetPendingDeletes() -- Get a list of non-temp relations to be deleted.
 *
 * The return value is the number of relations scheduled for termination.
 * *ptr is set to point to a freshly-palloc'd array of RelFileNodes.
 * If there are no relations to be deleted, *ptr is set to NULL.
 *
 * Only non-temporary relations are included in the returned list.  This is OK
 * because the list is used only in contexts where temporary relations don't
 * matter: we're either writing to the two-phase state file (and transactions
 * that have touched temp tables can't be prepared) or we're writing to xlog
 * (and all temporary files will be zapped if we restart anyway, so no need
 * for redo to do it also).
 *
 * Note that the list does not include anything scheduled for termination
 * by upper-level transactions.
 */
int
smgrGetPendingDeletes(bool forCommit, RelFileNode **ptr)
{
	int			nestLevel = GetCurrentTransactionNestLevel();
	int			nrels;
	RelFileNode *rptr;
	PendingRelDelete *pending;

	nrels = 0;
	for (pending = pendingDeletes; pending != NULL; pending = pending->next)
	{
		if (pending->nestLevel >= nestLevel && pending->atCommit == forCommit
			&& pending->backend == InvalidBackendId)
			nrels++;
	}
	if (nrels == 0)
	{
		*ptr = NULL;
		return 0;
	}
	rptr = (RelFileNode *) palloc(nrels * sizeof(RelFileNode));
	*ptr = rptr;
	for (pending = pendingDeletes; pending != NULL; pending = pending->next)
	{
		if (pending->nestLevel >= nestLevel && pending->atCommit == forCommit
			&& pending->backend == InvalidBackendId)
		{
			*rptr = pending->relnode;
			rptr++;
		}
	}
	return nrels;
}

/*
 *	PostPrepare_smgr -- Clean up after a successful PREPARE
 *
 * What we have to do here is throw away the in-memory state about pending
 * relation deletes.  It's all been recorded in the 2PC state file and
 * it's no longer smgr's job to worry about it.
 */
void
PostPrepare_smgr(void)
{
	PendingRelDelete *pending;
	PendingRelDelete *next;

	for (pending = pendingDeletes; pending != NULL; pending = next)
	{
		next = pending->next;
		pendingDeletes = next;
		/* must explicitly free the list entry */
		pfree(pending);
	}
}


/*
 * AtSubCommit_smgr() --- Take care of subtransaction commit.
 *
 * Reassign all items in the pending-deletes list to the parent transaction.
 */
void
AtSubCommit_smgr(void)
{
	int			nestLevel = GetCurrentTransactionNestLevel();
	PendingRelDelete *pending;

	for (pending = pendingDeletes; pending != NULL; pending = pending->next)
	{
		if (pending->nestLevel >= nestLevel)
			pending->nestLevel = nestLevel - 1;
	}
}

/*
 * AtSubAbort_smgr() --- Take care of subtransaction abort.
 *
 * Delete created relations and forget about deleted relations.
 * We can execute these operations immediately because we know this
 * subtransaction will not commit.
 */
void
AtSubAbort_smgr(void)
{
	smgrDoPendingDeletes(false);
}

void
smgr_redo(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	/* Backup blocks are not used in smgr records */
	Assert(!XLogRecHasAnyBlockRefs(record));

	/* replica do not create or truncate files */
	if ((info == XLOG_SMGR_CREATE || info == XLOG_SMGR_TRUNCATE) &&
		polar_is_replica())
		return;

	if (info == XLOG_SMGR_CREATE)
	{
		xl_smgr_create *xlrec = (xl_smgr_create *) XLogRecGetData(record);
		SMgrRelation reln;

		reln = smgropen(xlrec->rnode, InvalidBackendId);
		smgrcreate(reln, xlrec->forkNum, true);
	}
	else if (info == XLOG_SMGR_TRUNCATE)
	{
		xl_smgr_truncate *xlrec = (xl_smgr_truncate *) XLogRecGetData(record);
		SMgrRelation reln;
		Relation	rel;
		ForkNumber	forks[MAX_FORKNUM];
		BlockNumber blocks[MAX_FORKNUM];
		BlockNumber old_blocks[MAX_FORKNUM];
		int			nforks = 0;
		bool		need_fsm_vacuum = false;

		reln = smgropen(xlrec->rnode, InvalidBackendId);

		/*
		 * Forcibly create relation if it doesn't exist (which suggests that
		 * it was dropped somewhere later in the WAL sequence).  As in
		 * XLogReadBufferForRedo, we prefer to recreate the rel and replay the
		 * log as best we can until the drop is seen.
		 */
		smgrcreate(reln, MAIN_FORKNUM, true);

		/*
		 * Before we perform the truncation, update minimum recovery point to
		 * cover this WAL record. Once the relation is truncated, there's no
		 * going back. The buffer manager enforces the WAL-first rule for
		 * normal updates to relation files, so that the minimum recovery
		 * point is always updated before the corresponding change in the data
		 * file is flushed to disk. We have to do the same manually here.
		 *
		 * Doing this before the truncation means that if the truncation fails
		 * for some reason, you cannot start up the system even after restart,
		 * until you fix the underlying situation so that the truncation will
		 * succeed. Alternatively, we could update the minimum recovery point
		 * after truncation, but that would leave a small window where the
		 * WAL-first rule could be violated.
		 */
		XLogFlush(lsn);

		/* Prepare for truncation of MAIN fork */
		if ((xlrec->flags & SMGR_TRUNCATE_HEAP) != 0)
		{
			forks[nforks] = MAIN_FORKNUM;
			old_blocks[nforks] = smgrnblocks_ensure_opened(reln, MAIN_FORKNUM);
			blocks[nforks] = xlrec->blkno;
			nforks++;

			/* Also tell xlogutils.c about it */
			XLogTruncateRelation(xlrec->rnode, MAIN_FORKNUM, xlrec->blkno);
		}

		/* Prepare for truncation of FSM and VM too */
		rel = CreateFakeRelcacheEntry(xlrec->rnode);

		if ((xlrec->flags & SMGR_TRUNCATE_FSM) != 0 &&
			smgrexists(reln, FSM_FORKNUM))
		{
			blocks[nforks] = FreeSpaceMapPrepareTruncateRel(rel, xlrec->blkno);
			if (BlockNumberIsValid(blocks[nforks]))
			{
				forks[nforks] = FSM_FORKNUM;
				old_blocks[nforks] = smgrnblocks_ensure_opened(reln, FSM_FORKNUM);
				nforks++;
				need_fsm_vacuum = true;
			}
		}
		if ((xlrec->flags & SMGR_TRUNCATE_VM) != 0 &&
			smgrexists(reln, VISIBILITYMAP_FORKNUM))
		{
			blocks[nforks] = visibilitymap_prepare_truncate(rel, xlrec->blkno);
			if (BlockNumberIsValid(blocks[nforks]))
			{
				forks[nforks] = VISIBILITYMAP_FORKNUM;
				old_blocks[nforks] = smgrnblocks_ensure_opened(reln, VISIBILITYMAP_FORKNUM);
				nforks++;
			}
		}

		/* Do the real work to truncate relation forks */
		if (nforks > 0)
		{
			START_CRIT_SECTION();
			smgrtruncate2(reln, forks, nforks, old_blocks, blocks);
			END_CRIT_SECTION();
		}

		/*
		 * Update upper-level FSM pages to account for the truncation. This is
		 * important because the just-truncated pages were likely marked as
		 * all-free, and would be preferentially selected.
		 */
		if (need_fsm_vacuum)
			FreeSpaceMapVacuumRange(rel, xlrec->blkno,
									InvalidBlockNumber);

		FreeFakeRelcacheEntry(rel);
	}
	else
		elog(PANIC, "smgr_redo: unknown op code %u", info);
}
