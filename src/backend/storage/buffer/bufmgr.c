/*-------------------------------------------------------------------------
 *
 * bufmgr.c
 *	  buffer manager interface routines
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/bufmgr.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * Principal entry points:
 *
 * ReadBuffer() -- find or create a buffer holding the requested page,
 *		and pin it so that no one can destroy it while this process
 *		is using it.
 *
 * ReleaseBuffer() -- unpin a buffer
 *
 * MarkBufferDirty() -- mark a pinned buffer's contents as "dirty".
 *		The disk write is delayed until buffer replacement or checkpoint.
 *
 * See also these files:
 *		freelist.c -- chooses victim for buffer replacement
 *		buf_table.c -- manages the buffer lookup table
 */
#include "postgres.h"

#include <sys/file.h>
#include <unistd.h>

#include "access/xlog.h"
#include "catalog/catalog.h"
#include "catalog/storage.h"
#include "executor/instrument.h"
#include "lib/binaryheap.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "pgstat.h"
#include "postmaster/bgwriter.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/smgr.h"
#include "storage/standby.h"
#include "utils/rel.h"
#include "utils/resowner_private.h"
#include "utils/timestamp.h"

/* POLAR */
#include "access/polar_logindex_redo.h"
#include "polar_flashback/polar_flashback_log.h"
#include "replication/walreceiver.h"
#include "storage/checksum.h"
#include "storage/polar_fd.h"
#include "storage/polar_bufmgr.h"
#include "storage/polar_copybuf.h"
#include "storage/polar_flushlist.h"
#include "storage/polar_pbp.h"
#include "utils/guc.h"

/* Note: this macro only works on local buffers, not shared ones! */
#define LocalBufHdrGetBlock(bufHdr) \
	LocalBufferBlockPointers[-((bufHdr)->buf_id + 2)]

/* Bits in SyncOneBuffer's return value */
#define BUF_WRITTEN				0x01
#define BUF_REUSABLE			0x02

#define DROP_RELS_BSEARCH_THRESHOLD		20

typedef enum polar_redo_action
{
	POLAR_REDO_NO_ACTION,
	POLAR_REDO_REPLAY_XLOG,
	POLAR_REDO_MARK_OUTDATE
} polar_redo_action;

typedef enum polar_checksum_err_action
{
	POLAR_CHECKSUM_ERR_NO_ACTION,
	POLAR_CHECKSUM_ERR_REPEAT_READ,		/* Repeat read this block */
	POLAR_CHECKSUM_ERR_CHECKPOINT_REDO, /* Iterate logindex for this page from checkpoint and find the first FPI xlog to replay */
} polar_checksum_err_action;

typedef struct PrivateRefCountEntry
{
	Buffer		buffer;
	int32		refcount;
} PrivateRefCountEntry;

/* 64 bytes, about the size of a cache line on common systems */
#define REFCOUNT_ARRAY_ENTRIES 8

/*
 * Status of buffers to checkpoint for a particular tablespace, used
 * internally in BufferSync.
 */
typedef struct CkptTsStatus
{
	/* oid of the tablespace */
	Oid			tsId;

	/*
	 * Checkpoint progress for this tablespace. To make progress comparable
	 * between tablespaces the progress is, for each tablespace, measured as a
	 * number between 0 and the total number of to-be-checkpointed pages. Each
	 * page checkpointed in this tablespace increments this space's progress
	 * by progress_slice.
	 */
	float8		progress;
	float8		progress_slice;

	/* number of to-be checkpointed pages in this tablespace */
	int			num_to_scan;
	/* already processed pages in this tablespace */
	int			num_scanned;

	/* current offset in CkptBufferIds for this tablespace */
	int			index;
} CkptTsStatus;

/* GUC variables */
bool		zero_damaged_pages = false;
int			bgwriter_lru_maxpages = 100;
double		bgwriter_lru_multiplier = 2.0;
bool		track_io_timing = false;
int			effective_io_concurrency = 0;

/*
 * GUC variables about triggering kernel writeback for buffers written; OS
 * dependent defaults are set via the GUC mechanism.
 */
int			checkpoint_flush_after = 0;
int			bgwriter_flush_after = 0;
int			backend_flush_after = 0;

/*
 * How many buffers PrefetchBuffer callers should try to stay ahead of their
 * ReadBuffer calls by.  This is maintained by the assign hook for
 * effective_io_concurrency.  Zero means "never prefetch".  This value is
 * only used for buffers not belonging to tablespaces that have their
 * effective_io_concurrency parameter set.
 */
int			target_prefetch_pages = 0;

/* local state for StartBufferIO and related functions */
static BufferDesc *InProgressBuf = NULL;
static bool IsForInput;

/*
 * POLAR: bulk io local state for StartBufferIO/TerminateBufferIO/AbortBufferIO and related functions.
 *
 * notice: bulk read io may be mixed with temporary write io, for flushing dirty evicted page.
 *	       So polar_bulk_io_is_for_input[] is required for error recovery.
 */
bool polar_bulk_io_is_in_progress = false;
int polar_bulk_io_in_progress_count = 0;
BufferDesc **polar_bulk_io_in_progress_buf = NULL;
static bool *polar_bulk_io_is_for_input = NULL;
/* POLAR end */

/* local state for LockBufferForCleanup */
static BufferDesc *PinCountWaitBuf = NULL;

/*
 * Backend-Private refcount management:
 *
 * Each buffer also has a private refcount that keeps track of the number of
 * times the buffer is pinned in the current process.  This is so that the
 * shared refcount needs to be modified only once if a buffer is pinned more
 * than once by an individual backend.  It's also used to check that no buffers
 * are still pinned at the end of transactions and when exiting.
 *
 *
 * To avoid - as we used to - requiring an array with NBuffers entries to keep
 * track of local buffers, we use a small sequentially searched array
 * (PrivateRefCountArray) and an overflow hash table (PrivateRefCountHash) to
 * keep track of backend local pins.
 *
 * Until no more than REFCOUNT_ARRAY_ENTRIES buffers are pinned at once, all
 * refcounts are kept track of in the array; after that, new array entries
 * displace old ones into the hash table. That way a frequently used entry
 * can't get "stuck" in the hashtable while infrequent ones clog the array.
 *
 * Note that in most scenarios the number of pinned buffers will not exceed
 * REFCOUNT_ARRAY_ENTRIES.
 *
 *
 * To enter a buffer into the refcount tracking mechanism first reserve a free
 * entry using ReservePrivateRefCountEntry() and then later, if necessary,
 * fill it with NewPrivateRefCountEntry(). That split lets us avoid doing
 * memory allocations in NewPrivateRefCountEntry() which can be important
 * because in some scenarios it's called with a spinlock held...
 */
static struct PrivateRefCountEntry PrivateRefCountArray[REFCOUNT_ARRAY_ENTRIES];
static HTAB *PrivateRefCountHash = NULL;
static int32 PrivateRefCountOverflowed = 0;
static uint32 PrivateRefCountClock = 0;
static PrivateRefCountEntry *ReservedRefCountEntry = NULL;

static void ReservePrivateRefCountEntry(void);
static PrivateRefCountEntry *NewPrivateRefCountEntry(Buffer buffer);
static PrivateRefCountEntry *GetPrivateRefCountEntry(Buffer buffer, bool do_move);
static inline int32 GetPrivateRefCount(Buffer buffer);
static void ForgetPrivateRefCountEntry(PrivateRefCountEntry *ref);

static bool polar_apply_io_locked_page(BufferDesc *bufHdr, XLogRecPtr replay_from, XLogRecPtr checkpoint_lsn);
static polar_redo_action polar_require_backend_redo(bool local_buf, ReadBufferMode mode, ForkNumber fork_num, XLogRecPtr *replay_from);
static polar_checksum_err_action polar_handle_read_error_block(Block bufBlock, SMgrRelation smgr, ForkNumber forkNum, BlockNumber blockNum, ReadBufferMode mode, polar_redo_action redo_action, int *repeat_read_times, BufferDesc *buf_desc);

/*
 * Ensure that the PrivateRefCountArray has sufficient space to store one more
 * entry. This has to be called before using NewPrivateRefCountEntry() to fill
 * a new entry - but it's perfectly fine to not use a reserved entry.
 */
static void
ReservePrivateRefCountEntry(void)
{
	/* Already reserved (or freed), nothing to do */
	if (ReservedRefCountEntry != NULL)
		return;

	/*
	 * First search for a free entry the array, that'll be sufficient in the
	 * majority of cases.
	 */
	{
		int			i;

		for (i = 0; i < REFCOUNT_ARRAY_ENTRIES; i++)
		{
			PrivateRefCountEntry *res;

			res = &PrivateRefCountArray[i];

			if (res->buffer == InvalidBuffer)
			{
				ReservedRefCountEntry = res;
				return;
			}
		}
	}

	/*
	 * No luck. All array entries are full. Move one array entry into the hash
	 * table.
	 */
	{
		/*
		 * Move entry from the current clock position in the array into the
		 * hashtable. Use that slot.
		 */
		PrivateRefCountEntry *hashent;
		bool		found;

		/* select victim slot */
		ReservedRefCountEntry =
			&PrivateRefCountArray[PrivateRefCountClock++ % REFCOUNT_ARRAY_ENTRIES];

		/* Better be used, otherwise we shouldn't get here. */
		Assert(ReservedRefCountEntry->buffer != InvalidBuffer);

		/* enter victim array entry into hashtable */
		hashent = hash_search(PrivateRefCountHash,
							  (void *) &(ReservedRefCountEntry->buffer),
							  HASH_ENTER,
							  &found);
		Assert(!found);
		hashent->refcount = ReservedRefCountEntry->refcount;

		/* clear the now free array slot */
		ReservedRefCountEntry->buffer = InvalidBuffer;
		ReservedRefCountEntry->refcount = 0;

		PrivateRefCountOverflowed++;
	}
}

/*
 * Fill a previously reserved refcount entry.
 */
static PrivateRefCountEntry *
NewPrivateRefCountEntry(Buffer buffer)
{
	PrivateRefCountEntry *res;

	/* only allowed to be called when a reservation has been made */
	Assert(ReservedRefCountEntry != NULL);

	/* use up the reserved entry */
	res = ReservedRefCountEntry;
	ReservedRefCountEntry = NULL;

	/* and fill it */
	res->buffer = buffer;
	res->refcount = 0;

	return res;
}

/*
 * Return the PrivateRefCount entry for the passed buffer.
 *
 * Returns NULL if a buffer doesn't have a refcount entry. Otherwise, if
 * do_move is true, and the entry resides in the hashtable the entry is
 * optimized for frequent access by moving it to the array.
 */
static PrivateRefCountEntry *
GetPrivateRefCountEntry(Buffer buffer, bool do_move)
{
	PrivateRefCountEntry *res;
	int			i;

	Assert(BufferIsValid(buffer));
	Assert(!BufferIsLocal(buffer));

	/*
	 * First search for references in the array, that'll be sufficient in the
	 * majority of cases.
	 */
	for (i = 0; i < REFCOUNT_ARRAY_ENTRIES; i++)
	{
		res = &PrivateRefCountArray[i];

		if (res->buffer == buffer)
			return res;
	}

	/*
	 * By here we know that the buffer, if already pinned, isn't residing in
	 * the array.
	 *
	 * Only look up the buffer in the hashtable if we've previously overflowed
	 * into it.
	 */
	if (PrivateRefCountOverflowed == 0)
		return NULL;

	res = hash_search(PrivateRefCountHash,
					  (void *) &buffer,
					  HASH_FIND,
					  NULL);

	if (res == NULL)
		return NULL;
	else if (!do_move)
	{
		/* caller doesn't want us to move the hash entry into the array */
		return res;
	}
	else
	{
		/* move buffer from hashtable into the free array slot */
		bool		found;
		PrivateRefCountEntry *free;

		/* Ensure there's a free array slot */
		ReservePrivateRefCountEntry();

		/* Use up the reserved slot */
		Assert(ReservedRefCountEntry != NULL);
		free = ReservedRefCountEntry;
		ReservedRefCountEntry = NULL;
		Assert(free->buffer == InvalidBuffer);

		/* and fill it */
		free->buffer = buffer;
		free->refcount = res->refcount;

		/* delete from hashtable */
		hash_search(PrivateRefCountHash,
					(void *) &buffer,
					HASH_REMOVE,
					&found);
		Assert(found);
		Assert(PrivateRefCountOverflowed > 0);
		PrivateRefCountOverflowed--;

		return free;
	}
}

/*
 * Returns how many times the passed buffer is pinned by this backend.
 *
 * Only works for shared memory buffers!
 */
static inline int32
GetPrivateRefCount(Buffer buffer)
{
	PrivateRefCountEntry *ref;

	Assert(BufferIsValid(buffer));
	Assert(!BufferIsLocal(buffer));

	/*
	 * Not moving the entry - that's ok for the current users, but we might
	 * want to change this one day.
	 */
	ref = GetPrivateRefCountEntry(buffer, false);

	if (ref == NULL)
		return 0;
	return ref->refcount;
}

/*
 * Release resources used to track the reference count of a buffer which we no
 * longer have pinned and don't want to pin again immediately.
 */
static void
ForgetPrivateRefCountEntry(PrivateRefCountEntry *ref)
{
	Assert(ref->refcount == 0);

	if (ref >= &PrivateRefCountArray[0] &&
		ref < &PrivateRefCountArray[REFCOUNT_ARRAY_ENTRIES])
	{
		ref->buffer = InvalidBuffer;

		/*
		 * Mark the just used entry as reserved - in many scenarios that
		 * allows us to avoid ever having to search the array/hash for free
		 * entries.
		 */
		ReservedRefCountEntry = ref;
	}
	else
	{
		bool		found;
		Buffer		buffer = ref->buffer;

		hash_search(PrivateRefCountHash,
					(void *) &buffer,
					HASH_REMOVE,
					&found);
		Assert(found);
		Assert(PrivateRefCountOverflowed > 0);
		PrivateRefCountOverflowed--;
	}
}

/*
 * BufferIsPinned
 *		True iff the buffer is pinned (also checks for valid buffer number).
 *
 *		NOTE: what we check here is that *this* backend holds a pin on
 *		the buffer.  We do not care whether some other backend does.
 */
#define BufferIsPinned(bufnum) \
( \
	!BufferIsValid(bufnum) ? \
		false \
	: \
		BufferIsLocal(bufnum) ? \
			(LocalRefCount[-(bufnum) - 1] > 0) \
		: \
	(GetPrivateRefCount(bufnum) > 0) \
)


static Buffer ReadBuffer_common(SMgrRelation reln, char relpersistence,
				  ForkNumber forkNum, BlockNumber blockNum,
				  ReadBufferMode mode, BufferAccessStrategy strategy,
				  bool *hit);
static bool PinBuffer(BufferDesc *buf, BufferAccessStrategy strategy);
static void PinBuffer_Locked(BufferDesc *buf);
static void UnpinBuffer(BufferDesc *buf, bool fixOwner);
static void BufferSync(int flags);
static uint32 WaitBufHdrUnlocked(BufferDesc *buf);
static int	SyncOneBuffer(int buf_id, bool skip_recently_used,
							WritebackContext *flush_context, int flags);
static void WaitIO(BufferDesc *buf);
static void shared_buffer_write_error_callback(void *arg);
static void local_buffer_write_error_callback(void *arg);
static BufferDesc *BufferAlloc(SMgrRelation smgr,
			char relpersistence,
			ForkNumber forkNum,
			BlockNumber blockNum,
			BufferAccessStrategy strategy,
			bool *foundPtr);
static void FlushBuffer(BufferDesc *buf, SMgrRelation reln, XLogRecPtr polar_oldest_apply_lsn, bool polar_skip_fullpage);
static void AtProcExit_Buffers(int code, Datum arg);
static void CheckForBufferLeaks(void);
static int	rnode_comparator(const void *p1, const void *p2);
static int	buffertag_comparator(const void *p1, const void *p2);
static int	ckpt_buforder_comparator(const void *pa, const void *pb);
static int	ts_ckpt_progress_comparator(Datum a, Datum b, void *arg);


/*
 * ComputeIoConcurrency -- get the number of pages to prefetch for a given
 *		number of spindles.
 */
bool
ComputeIoConcurrency(int io_concurrency, double *target)
{
	double		new_prefetch_pages = 0.0;
	int			i;

	/*
	 * Make sure the io_concurrency value is within valid range; it may have
	 * been forced with a manual pg_tablespace update.
	 */
	io_concurrency = Min(Max(io_concurrency, 0), MAX_IO_CONCURRENCY);

	/*----------
	 * The user-visible GUC parameter is the number of drives (spindles),
	 * which we need to translate to a number-of-pages-to-prefetch target.
	 * The target value is stashed in *extra and then assigned to the actual
	 * variable by assign_effective_io_concurrency.
	 *
	 * The expected number of prefetch pages needed to keep N drives busy is:
	 *
	 * drives |   I/O requests
	 * -------+----------------
	 *		1 |   1
	 *		2 |   2/1 + 2/2 = 3
	 *		3 |   3/1 + 3/2 + 3/3 = 5 1/2
	 *		4 |   4/1 + 4/2 + 4/3 + 4/4 = 8 1/3
	 *		n |   n * H(n)
	 *
	 * This is called the "coupon collector problem" and H(n) is called the
	 * harmonic series.  This could be approximated by n * ln(n), but for
	 * reasonable numbers of drives we might as well just compute the series.
	 *
	 * Alternatively we could set the target to the number of pages necessary
	 * so that the expected number of active spindles is some arbitrary
	 * percentage of the total.  This sounds the same but is actually slightly
	 * different.  The result ends up being ln(1-P)/ln((n-1)/n) where P is
	 * that desired fraction.
	 *
	 * Experimental results show that both of these formulas aren't aggressive
	 * enough, but we don't really have any better proposals.
	 *
	 * Note that if io_concurrency = 0 (disabled), we must set target = 0.
	 *----------
	 */

	for (i = 1; i <= io_concurrency; i++)
		new_prefetch_pages += (double) io_concurrency / (double) i;

	*target = new_prefetch_pages;

	/* This range check shouldn't fail, but let's be paranoid */
	return (new_prefetch_pages >= 0.0 && new_prefetch_pages < (double) INT_MAX);
}

/*
 * PrefetchBuffer -- initiate asynchronous read of a block of a relation
 *
 * This is named by analogy to ReadBuffer but doesn't actually allocate a
 * buffer.  Instead it tries to ensure that a future ReadBuffer for the given
 * block will not be delayed by the I/O.  Prefetching is optional.
 * No-op if prefetching isn't compiled in.
 */
void
PrefetchBuffer(Relation reln, ForkNumber forkNum, BlockNumber blockNum)
{
#ifdef USE_PREFETCH
	Assert(RelationIsValid(reln));
	Assert(BlockNumberIsValid(blockNum));

	/* Open it at the smgr level if not already done */
	RelationOpenSmgr(reln);

	if (RelationUsesLocalBuffers(reln))
	{
		/* see comments in ReadBufferExtended */
		if (RELATION_IS_OTHER_TEMP(reln))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot access temporary tables of other sessions")));

		/* pass it off to localbuf.c */
		LocalPrefetchBuffer(reln->rd_smgr, forkNum, blockNum);
	}
	else
	{
		BufferTag	newTag;		/* identity of requested block */
		uint32		newHash;	/* hash value for newTag */
		LWLock	   *newPartitionLock;	/* buffer partition lock for it */
		int			buf_id;

		/* create a tag so we can lookup the buffer */
		INIT_BUFFERTAG(newTag, reln->rd_smgr->smgr_rnode.node,
					   forkNum, blockNum);

		/* determine its hash code and partition lock ID */
		newHash = BufTableHashCode(&newTag);
		newPartitionLock = BufMappingPartitionLock(newHash);

		/* see if the block is in the buffer pool already */
		LWLockAcquire(newPartitionLock, LW_SHARED);
		buf_id = BufTableLookup(&newTag, newHash);
		LWLockRelease(newPartitionLock);

		/* If not in buffers, initiate prefetch */
		if (buf_id < 0)
			smgrprefetch(reln->rd_smgr, forkNum, blockNum);

		/*
		 * If the block *is* in buffers, we do nothing.  This is not really
		 * ideal: the block might be just about to be evicted, which would be
		 * stupid since we know we are going to need it soon.  But the only
		 * easy answer is to bump the usage_count, which does not seem like a
		 * great solution: when the caller does ultimately touch the block,
		 * usage_count would get bumped again, resulting in too much
		 * favoritism for blocks that are involved in a prefetch sequence. A
		 * real fix would involve some additional per-buffer state, and it's
		 * not clear that there's enough of a problem to justify that.
		 */
	}
#endif							/* USE_PREFETCH */
}


/*
 * ReadBuffer -- a shorthand for ReadBufferExtended, for reading from main
 *		fork with RBM_NORMAL mode and default strategy.
 */
Buffer
ReadBuffer(Relation reln, BlockNumber blockNum)
{
	return ReadBufferExtended(reln, MAIN_FORKNUM, blockNum, RBM_NORMAL, NULL);
}

/*
 * ReadBufferExtended -- returns a buffer containing the requested
 *		block of the requested relation.  If the blknum
 *		requested is P_NEW, extend the relation file and
 *		allocate a new block.  (Caller is responsible for
 *		ensuring that only one backend tries to extend a
 *		relation at the same time!)
 *
 * Returns: the buffer number for the buffer containing
 *		the block read.  The returned buffer has been pinned.
 *		Does not return on error --- elog's instead.
 *
 * Assume when this function is called, that reln has been opened already.
 *
 * In RBM_NORMAL mode, the page is read from disk, and the page header is
 * validated.  An error is thrown if the page header is not valid.  (But
 * note that an all-zero page is considered "valid"; see PageIsVerified().)
 *
 * RBM_ZERO_ON_ERROR is like the normal mode, but if the page header is not
 * valid, the page is zeroed instead of throwing an error. This is intended
 * for non-critical data, where the caller is prepared to repair errors.
 *
 * In RBM_ZERO_AND_LOCK mode, if the page isn't in buffer cache already, it's
 * filled with zeros instead of reading it from disk.  Useful when the caller
 * is going to fill the page from scratch, since this saves I/O and avoids
 * unnecessary failure if the page-on-disk has corrupt page headers.
 * The page is returned locked to ensure that the caller has a chance to
 * initialize the page before it's made visible to others.
 * Caution: do not use this mode to read a page that is beyond the relation's
 * current physical EOF; that is likely to cause problems in md.c when
 * the page is modified and written out. P_NEW is OK, though.
 *
 * RBM_ZERO_AND_CLEANUP_LOCK is the same as RBM_ZERO_AND_LOCK, but acquires
 * a cleanup-strength lock on the page.
 *
 * RBM_NORMAL_NO_LOG mode is treated the same as RBM_NORMAL here.
 *
 * If strategy is not NULL, a nondefault buffer access strategy is used.
 * See buffer/README for details.
 */
Buffer
ReadBufferExtended(Relation reln, ForkNumber forkNum, BlockNumber blockNum,
				   ReadBufferMode mode, BufferAccessStrategy strategy)
{
	bool		hit;
	Buffer		buf;

	/* Open it at the smgr level if not already done */
	RelationOpenSmgr(reln);

	/*
	 * Reject attempts to read non-local temporary relations; we would be
	 * likely to get wrong data since we have no visibility into the owning
	 * session's local buffers.
	 */
	if (RELATION_IS_OTHER_TEMP(reln))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot access temporary tables of other sessions")));

	/*
	 * Read the buffer, and update pgstat counters to reflect a cache hit or
	 * miss.
	 */
	pgstat_count_buffer_read(reln);
	buf = ReadBuffer_common(reln->rd_smgr, reln->rd_rel->relpersistence,
							forkNum, blockNum, mode, strategy, &hit);
	if (hit)
		pgstat_count_buffer_hit(reln);
	return buf;
}

/*
 * ReadBufferWithoutRelcache -- like ReadBufferExtended, but doesn't require
 *		a relcache entry for the relation.
 *
 * NB: At present, this function may only be used on permanent relations, which
 * is OK, because we only use it during XLOG replay.  If in the future we
 * want to use it on temporary or unlogged relations, we could pass additional
 * parameters.
 */
Buffer
ReadBufferWithoutRelcache(RelFileNode rnode, ForkNumber forkNum,
						  BlockNumber blockNum, ReadBufferMode mode,
						  BufferAccessStrategy strategy)
{
	bool		hit;

	SMgrRelation smgr = smgropen(rnode, InvalidBackendId);

	Assert(RecoveryInProgress() || POLAR_IN_LOGINDEX_PARALLEL_REPLAY());

	return ReadBuffer_common(smgr, RELPERSISTENCE_PERMANENT, forkNum, blockNum,
							 mode, strategy, &hit);
}


/*
 * ReadBuffer_common -- common logic for all ReadBuffer variants
 *
 * *hit is set to true if the request was satisfied from shared buffer cache.
 */
static Buffer
ReadBuffer_common(SMgrRelation smgr, char relpersistence, ForkNumber forkNum,
				  BlockNumber blockNum, ReadBufferMode mode,
				  BufferAccessStrategy strategy, bool *hit)
{
	BufferDesc *bufHdr;
	Block		bufBlock;
	bool		found;
	bool		isExtend;
	bool		isLocalBuf = SmgrIsTemp(smgr);
	/* Polar: start lsn to do replay */
	XLogRecPtr  replay_from = InvalidXLogRecPtr;
	XLogRecPtr  checkpoint_redo_lsn = InvalidXLogRecPtr;
	polar_redo_action        redo_action;
	int 		repeat_read_times = 0;

	/* POLAR: make sure that buffer pool has been inited */
	if (unlikely(!polar_buffer_pool_is_inited))
		elog(PANIC, "buffer pool is not inited");
	/* POLAR end */

	*hit = false;

	/* Make sure we will have room to remember the buffer pin */
	ResourceOwnerEnlargeBuffers(CurrentResourceOwner);

	isExtend = (blockNum == P_NEW);

	TRACE_POSTGRESQL_BUFFER_READ_START(forkNum, blockNum,
									   smgr->smgr_rnode.node.spcNode,
									   smgr->smgr_rnode.node.dbNode,
									   smgr->smgr_rnode.node.relNode,
									   smgr->smgr_rnode.backend,
									   isExtend);

	/* Substitute proper block number if caller asked for P_NEW */
	if (isExtend)
		blockNum = smgrnblocks(smgr, forkNum);

	if (isLocalBuf)
	{
		bufHdr = LocalBufferAlloc(smgr, forkNum, blockNum, &found);
		if (found)
			pgBufferUsage.local_blks_hit++;
		else
			pgBufferUsage.local_blks_read++;
	}
	else
	{
		/*
		 * lookup the buffer.  IO_IN_PROGRESS is set if the requested block is
		 * not currently in memory.
		 */
		bufHdr = BufferAlloc(smgr, relpersistence, forkNum, blockNum,
							 strategy, &found);
		if (found)
			pgBufferUsage.shared_blks_hit++;
		else
			pgBufferUsage.shared_blks_read++;
	}

	/* At this point we do NOT hold any locks. */

	/* if it was already in the buffer pool, we're done */
	if (found)
	{
		if (!isExtend)
		{
			/* Just need to update stats before we exit */
			*hit = true;
			VacuumPageHit++;

			if (VacuumCostActive)
				VacuumCostBalance += VacuumCostPageHit;

			TRACE_POSTGRESQL_BUFFER_READ_DONE(forkNum, blockNum,
											  smgr->smgr_rnode.node.spcNode,
											  smgr->smgr_rnode.node.dbNode,
											  smgr->smgr_rnode.node.relNode,
											  smgr->smgr_rnode.backend,
											  isExtend,
											  found);

			/*
			 * In RBM_ZERO_AND_LOCK mode the caller expects the page to be
			 * locked on return.
			 */
			if (!isLocalBuf)
			{
				if (mode == RBM_ZERO_AND_LOCK)
					LWLockAcquire(BufferDescriptorGetContentLock(bufHdr),
								  LW_EXCLUSIVE);
				else if (mode == RBM_ZERO_AND_CLEANUP_LOCK)
					LockBufferForCleanup(BufferDescriptorGetBuffer(bufHdr));
			}

			return BufferDescriptorGetBuffer(bufHdr);
		}

		/*
		 * We get here only in the corner case where we are trying to extend
		 * the relation but we found a pre-existing buffer marked BM_VALID.
		 * This can happen because mdread doesn't complain about reads beyond
		 * EOF (when zero_damaged_pages is ON) and so a previous attempt to
		 * read a block beyond EOF could have left a "valid" zero-filled
		 * buffer.  Unfortunately, we have also seen this case occurring
		 * because of buggy Linux kernels that sometimes return an
		 * lseek(SEEK_END) result that doesn't account for a recent write. In
		 * that situation, the pre-existing buffer would contain valid data
		 * that we don't want to overwrite.  Since the legitimate case should
		 * always have left a zero-filled buffer, complain if not PageIsNew.
		 */
		bufBlock = isLocalBuf ? LocalBufHdrGetBlock(bufHdr) : BufHdrGetBlock(bufHdr);
		if (!PageIsNew((Page) bufBlock))
			ereport(ERROR,
					(errmsg("unexpected data beyond EOF in block %u of relation %s",
							blockNum, relpath(smgr->smgr_rnode, forkNum)),
					 errhint("This has been seen to occur with buggy kernels; consider updating your system.")));

		/*
		 * We *must* do smgrextend before succeeding, else the page will not
		 * be reserved by the kernel, and the next P_NEW call will decide to
		 * return the same page.  Clear the BM_VALID bit, do the StartBufferIO
		 * call that BufferAlloc didn't, and proceed.
		 */
		if (isLocalBuf)
		{
			/* Only need to adjust flags */
			uint32		buf_state = pg_atomic_read_u32(&bufHdr->state);

			Assert(buf_state & BM_VALID);
			buf_state &= ~BM_VALID;
			pg_atomic_unlocked_write_u32(&bufHdr->state, buf_state);
		}
		else
		{
			/*
			 * Loop to handle the very small possibility that someone re-sets
			 * BM_VALID between our clearing it and StartBufferIO inspecting
			 * it.
			 */
			do
			{
				uint32		buf_state = LockBufHdr(bufHdr);

				Assert(buf_state & BM_VALID);
				buf_state &= ~BM_VALID;
				UnlockBufHdr(bufHdr, buf_state);
			} while (!StartBufferIO(bufHdr, true));
		}
	}

repeat_read:
	/*
	 * POLAR: page-replay.
	 *
	 * Get consistent lsn which used by replay.
	 *
	 * Note: All modifications about replay-page must be
	 *       applied to both polar_bulk_read_buffer_common() and ReadBuffer_common().
	 */
	redo_action = polar_require_backend_redo(isLocalBuf, mode, forkNum, &replay_from);

	if (redo_action != POLAR_REDO_NO_ACTION)
	{
		checkpoint_redo_lsn = GetRedoRecPtr();
		Assert(!XLogRecPtrIsInvalid(checkpoint_redo_lsn));
	}
	/* POLAR end */

	/*
	 * if we have gotten to this point, we have allocated a buffer for the
	 * page but its contents are not yet valid.  IO_IN_PROGRESS is set for it,
	 * if it's a shared buffer.
	 *
	 * Note: if smgrextend fails, we will end up with a buffer that is
	 * allocated but not marked BM_VALID.  P_NEW will still select the same
	 * block number (because the relation didn't get any longer on disk) and
	 * so future attempts to extend the relation will find the same buffer (if
	 * it's not been recycled) but come right back here to try smgrextend
	 * again.
	 */
	Assert(!(pg_atomic_read_u32(&bufHdr->state) & BM_VALID));	/* spinlock not needed */

	bufBlock = isLocalBuf ? LocalBufHdrGetBlock(bufHdr) : BufHdrGetBlock(bufHdr);

	if (isExtend)
	{
		/* new buffers are zero-filled */
		MemSet((char *) bufBlock, 0, BLCKSZ);
		/* don't set checksum for all-zero page */
		smgrextend(smgr, forkNum, blockNum, (char *) bufBlock, false);

		/*
		 * NB: we're *not* doing a ScheduleBufferTagForWriteback here;
		 * although we're essentially performing a write. At least on linux
		 * doing so defeats the 'delayed allocation' mechanism, leading to
		 * increased file fragmentation.
		 */
	}
	else
	{
		/*
		 * Read in the page, unless the caller intends to overwrite it and
		 * just wants us to allocate a buffer.
		 */
		if (mode == RBM_ZERO_AND_LOCK || mode == RBM_ZERO_AND_CLEANUP_LOCK)
			MemSet((char *) bufBlock, 0, BLCKSZ);
		else
		{
			instr_time	io_start,
						io_time;

			if (track_io_timing)
				INSTR_TIME_SET_CURRENT(io_start);

			smgrread(smgr, forkNum, blockNum, (char *) bufBlock);

			if (track_io_timing)
			{
				INSTR_TIME_SET_CURRENT(io_time);
				INSTR_TIME_SUBTRACT(io_time, io_start);
				pgstat_count_buffer_read_time(INSTR_TIME_GET_MICROSEC(io_time));
				INSTR_TIME_ADD(pgBufferUsage.blk_read_time, io_time);
			}

			/* check for garbage data */
			if (!PageIsVerified((Page) bufBlock, forkNum, blockNum, smgr))
			{
				polar_checksum_err_action err_act = polar_handle_read_error_block(bufBlock, smgr, forkNum, blockNum,
						mode, redo_action, &repeat_read_times, bufHdr);

				if (err_act == POLAR_CHECKSUM_ERR_REPEAT_READ)
					POLAR_REPEAT_READ();
				else if (err_act == POLAR_CHECKSUM_ERR_CHECKPOINT_REDO)
				{
					replay_from = checkpoint_redo_lsn;
					ereport(WARNING,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("Replay from lastcheckpoint which is %lX", checkpoint_redo_lsn)));

				}
			}
		}
	}

	/*
	 * In RBM_ZERO_AND_LOCK mode, grab the buffer content lock before marking
	 * the page as valid, to make sure that no other backend sees the zeroed
	 * page before the caller has had a chance to initialize it.
	 *
	 * Since no-one else can be looking at the page contents yet, there is no
	 * difference between an exclusive lock and a cleanup-strength lock. (Note
	 * that we cannot use LockBuffer() or LockBufferForCleanup() here, because
	 * they assert that the buffer is already valid.)
	 */
	if ((mode == RBM_ZERO_AND_LOCK || mode == RBM_ZERO_AND_CLEANUP_LOCK) &&
		!isLocalBuf)
	{
		LWLockAcquire(BufferDescriptorGetContentLock(bufHdr), LW_EXCLUSIVE);
	}

	if (isLocalBuf)
	{
		/* Only need to adjust flags */
		uint32		buf_state = pg_atomic_read_u32(&bufHdr->state);

		buf_state |= BM_VALID;
		pg_atomic_unlocked_write_u32(&bufHdr->state, buf_state);
	}
	else
	{
		/*
		 * POLAR: page-replay.
		 *
		 * apply logs to this old page when read from disk.
		 *
		 * Note: All modifications about replay-page must be
		 *       applied to both polar_bulk_read_buffer_common() and ReadBuffer_common().
		 */
		if (redo_action == POLAR_REDO_REPLAY_XLOG)
		{
			/*
			 * POLAR: we want to do record replay on page only in
			 * non-Startup process and consistency reached Startup process.
			 * This judge is *ONLY* valid in replica mode, so we set
			 * replica check above with polar_in_replica_mode().
			 */
			polar_apply_io_locked_page(bufHdr, replay_from, checkpoint_redo_lsn);

			POLAR_RESET_BACKEND_READ_MIN_LSN();
		}
		else if (redo_action == POLAR_REDO_MARK_OUTDATE)
		{
			uint32 redo_state = polar_lock_redo_state(bufHdr);
			redo_state |= POLAR_REDO_OUTDATE;
			polar_unlock_redo_state(bufHdr, redo_state);

			POLAR_RESET_BACKEND_READ_MIN_LSN();
		}
		/* POLAR end */

		/* Set BM_VALID, terminate IO, and wake up any waiters */
		TerminateBufferIO(bufHdr, false, BM_VALID);
	}

	VacuumPageMiss++;
	if (VacuumCostActive)
		VacuumCostBalance += VacuumCostPageMiss;

	TRACE_POSTGRESQL_BUFFER_READ_DONE(forkNum, blockNum,
									  smgr->smgr_rnode.node.spcNode,
									  smgr->smgr_rnode.node.dbNode,
									  smgr->smgr_rnode.node.relNode,
									  smgr->smgr_rnode.backend,
									  isExtend,
									  found);

	return BufferDescriptorGetBuffer(bufHdr);
}

/*
 * BufferAlloc -- subroutine for ReadBuffer.  Handles lookup of a shared
 *		buffer.  If no buffer exists already, selects a replacement
 *		victim and evicts the old page, but does NOT read in new page.
 *
 * "strategy" can be a buffer replacement strategy object, or NULL for
 * the default strategy.  The selected buffer's usage_count is advanced when
 * using the default strategy, but otherwise possibly not (see PinBuffer).
 *
 * The returned buffer is pinned and is already marked as holding the
 * desired page.  If it already did have the desired page, *foundPtr is
 * set true.  Otherwise, *foundPtr is set false and the buffer is marked
 * as IO_IN_PROGRESS; ReadBuffer will now need to do I/O to fill it.
 *
 * *foundPtr is actually redundant with the buffer's BM_VALID flag, but
 * we keep it for simplicity in ReadBuffer.
 *
 * No locks are held either at entry or exit.
 *
 * POLAR: if reading bulk non-frist page and most buffers are pinned, return NULL
 * instead of log(error).
 */
static BufferDesc *
BufferAlloc(SMgrRelation smgr, char relpersistence, ForkNumber forkNum,
			BlockNumber blockNum,
			BufferAccessStrategy strategy,
			bool *foundPtr)
{
	BufferTag	newTag;			/* identity of requested block */
	uint32		newHash;		/* hash value for newTag */
	LWLock	   *newPartitionLock;	/* buffer partition lock for it */
	BufferTag	oldTag;			/* previous identity of selected buffer */
	uint32		oldHash;		/* hash value for oldTag */
	LWLock	   *oldPartitionLock;	/* buffer partition lock for it */
	uint32		oldFlags;
	int			buf_id;
	BufferDesc *buf;
	bool		valid;
	uint32		buf_state;

	/* POLAR */
	XLogRecPtr	oldest_apply_lsn = InvalidXLogRecPtr;
	uint32		redo_state;

	/* create a tag so we can lookup the buffer */
	INIT_BUFFERTAG(newTag, smgr->smgr_rnode.node, forkNum, blockNum);

	/* determine its hash code and partition lock ID */
	newHash = BufTableHashCode(&newTag);
	newPartitionLock = BufMappingPartitionLock(newHash);

	/* see if the block is in the buffer pool already */
	LWLockAcquire(newPartitionLock, LW_SHARED);
	buf_id = BufTableLookup(&newTag, newHash);
	if (buf_id >= 0)
	{
		/*
		 * Found it.  Now, pin the buffer so no one can steal it from the
		 * buffer pool, and check to see if the correct data has been loaded
		 * into the buffer.
		 */
		buf = GetBufferDescriptor(buf_id);

		valid = PinBuffer(buf, strategy);

		/* Can release the mapping lock as soon as we've pinned it */
		LWLockRelease(newPartitionLock);

		*foundPtr = true;

		if (!valid)
		{
			/*
			 * We can only get here if (a) someone else is still reading in
			 * the page, or (b) a previous read attempt failed.  We have to
			 * wait for any active read attempt to finish, and then set up our
			 * own read attempt if the page is still not BM_VALID.
			 * StartBufferIO does it all.
			 */
			if (StartBufferIO(buf, true))
			{
				/*
				 * If we get here, previous attempts to read the buffer must
				 * have failed ... but we shall bravely try again.
				 */
				*foundPtr = false;
			}
		}

		return buf;
	}

	/*
	 * Didn't find it in the buffer pool.  We'll have to initialize a new
	 * buffer.  Remember to unlock the mapping lock while doing the work.
	 */
	LWLockRelease(newPartitionLock);

	/* Loop here in case we have to try another victim buffer */
	for (;;)
	{
		/*
		 * Ensure, while the spinlock's not yet held, that there's a free
		 * refcount entry.
		 */
		ReservePrivateRefCountEntry();

		/*
		 * Select a victim buffer.  The buffer is returned with its header
		 * spinlock still held!
		 */
		buf = StrategyGetBuffer(strategy, &buf_state);

		/*
		 * POLAR: bulk read
		 * Only in bulk read, StrategyGetBuffer can return NULL.
		 * If reading bulk non-frist page and most buffers are pinned,
		 * return NULL instead of log(error).
		 */
		if (NULL == buf)
		{
			*foundPtr = false;
			return NULL;
		}
		/* POLAR end */

		Assert(BUF_STATE_GET_REFCOUNT(buf_state) == 0);

		/* Must copy buffer flags while we still hold the spinlock */
		oldFlags = buf_state & BUF_FLAG_MASK;

		/* Pin the buffer and then release the buffer spinlock */
		PinBuffer_Locked(buf);

		/*
		 * If the buffer was dirty, try to write it out.  There is a race
		 * condition here, in that someone might dirty it after we released it
		 * above, or even while we are writing it out (since our share-lock
		 * won't prevent hint-bit updates).  We will recheck the dirty bit
		 * after re-locking the buffer header.
		 */
		if (oldFlags & BM_DIRTY)
		{
			/*
			 * We need a share-lock on the buffer contents to write it out
			 * (else we might write invalid data, eg because someone else is
			 * compacting the page contents while we write).  We must use a
			 * conditional lock acquisition here to avoid deadlock.  Even
			 * though the buffer was not pinned (and therefore surely not
			 * locked) when StrategyGetBuffer returned it, someone else could
			 * have pinned and exclusive-locked it by the time we get here. If
			 * we try to get the lock unconditionally, we'd block waiting for
			 * them; if they later block waiting for us, deadlock ensues.
			 * (This has been observed to happen when two backends are both
			 * trying to split btree index pages, and the second one just
			 * happens to be trying to split the page the first one got from
			 * StrategyGetBuffer.)
			 */
			if (LWLockConditionalAcquire(BufferDescriptorGetContentLock(buf),
										 LW_SHARED))
			{
				/*
				 * If using a nondefault strategy, and writing the buffer
				 * would require a WAL flush, let the strategy decide whether
				 * to go ahead and write/reuse the buffer or to choose another
				 * victim.  We need lock to inspect the page LSN, so this
				 * can't be done inside StrategyGetBuffer.
				 */
				if (strategy != NULL)
				{
					XLogRecPtr	lsn;

					/* Read the LSN while holding buffer header lock */
					buf_state = LockBufHdr(buf);
					lsn = BufferGetLSN(buf);
					UnlockBufHdr(buf, buf_state);

					if (XLogNeedsFlush(lsn) &&
						StrategyRejectBuffer(strategy, buf))
					{
						/* Drop lock/pin and loop around for another buffer */
						LWLockRelease(BufferDescriptorGetContentLock(buf));
						UnpinBuffer(buf, true);
						continue;
					}
				}

				/* POLAR */
				oldest_apply_lsn = polar_get_oldest_applied_lsn();
				if (!polar_buffer_can_be_flushed(buf, oldest_apply_lsn, false))
				{
					/* Drop lock/pin and loop around for another buffer */
					LWLockRelease(BufferDescriptorGetContentLock(buf));
					UnpinBuffer(buf, true);

					/*
					 * POLAR: Check for interrupts if we can not flush buffer , like buffer pool is full.
					 * Otherwise user can not terminate this transaction
					 */
					CHECK_FOR_INTERRUPTS();

					continue;
				}

				/* OK, do the I/O */
				TRACE_POSTGRESQL_BUFFER_WRITE_DIRTY_START(forkNum, blockNum,
														  smgr->smgr_rnode.node.spcNode,
														  smgr->smgr_rnode.node.dbNode,
														  smgr->smgr_rnode.node.relNode);

				FlushBuffer(buf, NULL, oldest_apply_lsn, false);
				LWLockRelease(BufferDescriptorGetContentLock(buf));

				if (!polar_enable_shared_storage_mode)
					ScheduleBufferTagForWriteback(&BackendWritebackContext,
												  &buf->tag);

				TRACE_POSTGRESQL_BUFFER_WRITE_DIRTY_DONE(forkNum, blockNum,
														 smgr->smgr_rnode.node.spcNode,
														 smgr->smgr_rnode.node.dbNode,
														 smgr->smgr_rnode.node.relNode);
			}
			else
			{
				/*
				 * Someone else has locked the buffer, so give it up and loop
				 * back to get another one.
				 */
				UnpinBuffer(buf, true);
				continue;
			}
		}

		/*
		 * To change the association of a valid buffer, we'll need to have
		 * exclusive lock on both the old and new mapping partitions.
		 */
		if (oldFlags & BM_TAG_VALID)
		{
			/*
			 * Need to compute the old tag's hashcode and partition lock ID.
			 * XXX is it worth storing the hashcode in BufferDesc so we need
			 * not recompute it here?  Probably not.
			 */
			oldTag = buf->tag;
			oldHash = BufTableHashCode(&oldTag);
			oldPartitionLock = BufMappingPartitionLock(oldHash);

			/*
			 * Must lock the lower-numbered partition first to avoid
			 * deadlocks.
			 */
			if (oldPartitionLock < newPartitionLock)
			{
				LWLockAcquire(oldPartitionLock, LW_EXCLUSIVE);
				LWLockAcquire(newPartitionLock, LW_EXCLUSIVE);
			}
			else if (oldPartitionLock > newPartitionLock)
			{
				LWLockAcquire(newPartitionLock, LW_EXCLUSIVE);
				LWLockAcquire(oldPartitionLock, LW_EXCLUSIVE);
			}
			else
			{
				/* only one partition, only one lock */
				LWLockAcquire(newPartitionLock, LW_EXCLUSIVE);
			}
		}
		else
		{
			/* if it wasn't valid, we need only the new partition */
			LWLockAcquire(newPartitionLock, LW_EXCLUSIVE);
			/* remember we have no old-partition lock or tag */
			oldPartitionLock = NULL;
			/* this just keeps the compiler quiet about uninit variables */
			oldHash = 0;
		}

		/*
		 * Try to make a hashtable entry for the buffer under its new tag.
		 * This could fail because while we were writing someone else
		 * allocated another buffer for the same block we want to read in.
		 * Note that we have not yet removed the hashtable entry for the old
		 * tag.
		 */
		buf_id = BufTableInsert(&newTag, newHash, buf->buf_id);

		if (buf_id >= 0)
		{
			/*
			 * Got a collision. Someone has already done what we were about to
			 * do. We'll just handle this as if it were found in the buffer
			 * pool in the first place.  First, give up the buffer we were
			 * planning to use.
			 */
			UnpinBuffer(buf, true);

			/* Can give up that buffer's mapping partition lock now */
			if (oldPartitionLock != NULL &&
				oldPartitionLock != newPartitionLock)
				LWLockRelease(oldPartitionLock);

			/* remaining code should match code at top of routine */

			buf = GetBufferDescriptor(buf_id);

			valid = PinBuffer(buf, strategy);

			/* Can release the mapping lock as soon as we've pinned it */
			LWLockRelease(newPartitionLock);

			*foundPtr = true;

			if (!valid)
			{
				/*
				 * We can only get here if (a) someone else is still reading
				 * in the page, or (b) a previous read attempt failed.  We
				 * have to wait for any active read attempt to finish, and
				 * then set up our own read attempt if the page is still not
				 * BM_VALID.  StartBufferIO does it all.
				 */
				if (StartBufferIO(buf, true))
				{
					/*
					 * If we get here, previous attempts to read the buffer
					 * must have failed ... but we shall bravely try again.
					 */
					*foundPtr = false;
				}
			}

			return buf;
		}

		/*
		 * Need to lock the buffer header too in order to change its tag.
		 */
		buf_state = LockBufHdr(buf);

		/*
		 * Somebody could have pinned or re-dirtied the buffer while we were
		 * doing the I/O and making the new hashtable entry.  If so, we can't
		 * recycle this buffer; we must undo everything we've done and start
		 * over with a new victim buffer.
		 */
		oldFlags = buf_state & BUF_FLAG_MASK;
		if (BUF_STATE_GET_REFCOUNT(buf_state) == 1 && !(oldFlags & BM_DIRTY))
			break;

		UnlockBufHdr(buf, buf_state);
		BufTableDelete(&newTag, newHash);
		if (oldPartitionLock != NULL &&
			oldPartitionLock != newPartitionLock)
			LWLockRelease(oldPartitionLock);
		LWLockRelease(newPartitionLock);
		UnpinBuffer(buf, true);
	}

	/*
	 * Okay, it's finally safe to rename the buffer.
	 *
	 * Clearing BM_VALID here is necessary, clearing the dirtybits is just
	 * paranoia.  We also reset the usage_count since any recency of use of
	 * the old content is no longer relevant.  (The usage_count starts out at
	 * 1 so that the buffer can survive one clock-sweep pass.)
	 *
	 * Make sure BM_PERMANENT is set for buffers that must be written at every
	 * checkpoint.  Unlogged buffers only need to be written at shutdown
	 * checkpoints, except for their "init" forks, which need to be treated
	 * just like permanent relations.
	 */
	buf->tag = newTag;
	buf_state &= ~(BM_VALID | BM_DIRTY | BM_JUST_DIRTIED |
				   BM_CHECKPOINT_NEEDED | BM_IO_ERROR | BM_PERMANENT |
				   BUF_USAGECOUNT_MASK);
	if (relpersistence == RELPERSISTENCE_PERMANENT || forkNum == INIT_FORKNUM)
		buf_state |= BM_TAG_VALID | BM_PERMANENT | BUF_USAGECOUNT_ONE;
	else
		buf_state |= BM_TAG_VALID | BUF_USAGECOUNT_ONE;

	/* POLAR; clear polar_flags when buffer header is reused */
	buf->polar_flags = 0;
	/* POLAR end */

	UnlockBufHdr(buf, buf_state);

	/*
	 * POLAR: Make true there are no flashback record of the buffer.
	 */
	polar_make_true_no_flog(flog_instance, buf);

	/*
	 * POLAR: reset redo state for the new buffer
	 */
	redo_state = polar_lock_redo_state(buf);
	redo_state &= ~(POLAR_BUF_REDO_FLAG_MASK | POLAR_BUF_FLASHBACK_FLAG_MASK);
	polar_unlock_redo_state(buf, redo_state);

	if (oldPartitionLock != NULL)
	{
		BufTableDelete(&oldTag, oldHash);
		if (oldPartitionLock != newPartitionLock)
			LWLockRelease(oldPartitionLock);
	}

	LWLockRelease(newPartitionLock);

	/*
	 * Buffer contents are currently invalid.  Try to get the io_in_progress
	 * lock.  If StartBufferIO returns false, then someone else managed to
	 * read it before we did, so there's nothing left for BufferAlloc() to do.
	 */
	if (StartBufferIO(buf, true))
		*foundPtr = false;
	else
		*foundPtr = true;

	return buf;
}

/*
 * InvalidateBuffer -- mark a shared buffer invalid and return it to the
 * freelist.
 *
 * The buffer header spinlock must be held at entry.  We drop it before
 * returning.  (This is sane because the caller must have locked the
 * buffer in order to be sure it should be dropped.)
 *
 * This is used only in contexts such as dropping a relation.  We assume
 * that no other backend could possibly be interested in using the page,
 * so the only reason the buffer might be pinned is if someone else is
 * trying to write it out.  We have to let them finish before we can
 * reclaim the buffer.
 *
 * The buffer could get reclaimed by someone else while we are waiting
 * to acquire the necessary locks; if so, don't mess it up.
 */
static void
InvalidateBuffer(BufferDesc *buf)
{
	BufferTag	oldTag;
	uint32		oldHash;		/* hash value for oldTag */
	LWLock	   *oldPartitionLock;	/* buffer partition lock for it */
	uint32		oldFlags;
	uint32		buf_state;
	uint32 		redo_state;

	/* Save the original buffer tag before dropping the spinlock */
	oldTag = buf->tag;

	buf_state = pg_atomic_read_u32(&buf->state);
	Assert(buf_state & BM_LOCKED);
	UnlockBufHdr(buf, buf_state);

	/*
	 * Need to compute the old tag's hashcode and partition lock ID. XXX is it
	 * worth storing the hashcode in BufferDesc so we need not recompute it
	 * here?  Probably not.
	 */
	oldHash = BufTableHashCode(&oldTag);
	oldPartitionLock = BufMappingPartitionLock(oldHash);

	/* POLAR: Mark this buffer is invalidating and make background process not to replay this buffer */
	redo_state = polar_lock_redo_state(buf);
	redo_state |= POLAR_REDO_INVALIDATE;
	polar_unlock_redo_state(buf, redo_state);

retry:

	/*
	 * Acquire exclusive mapping lock in preparation for changing the buffer's
	 * association.
	 */
	LWLockAcquire(oldPartitionLock, LW_EXCLUSIVE);

	/* Re-lock the buffer header */
	buf_state = LockBufHdr(buf);

	/* If it's changed while we were waiting for lock, do nothing */
	if (!BUFFERTAGS_EQUAL(buf->tag, oldTag))
	{
		UnlockBufHdr(buf, buf_state);
		LWLockRelease(oldPartitionLock);
		return;
	}

	/*
	 * We assume the only reason for it to be pinned is that someone else is
	 * flushing the page out.  Wait for them to finish.  (This could be an
	 * infinite loop if the refcount is messed up... it would be nice to time
	 * out after awhile, but there seems no way to be sure how many loops may
	 * be needed.  Note that if the other guy has pinned the buffer but not
	 * yet done StartBufferIO, WaitIO will fall through and we'll effectively
	 * be busy-looping here.)
	 */
	if (BUF_STATE_GET_REFCOUNT(buf_state) != 0)
	{
		UnlockBufHdr(buf, buf_state);
		LWLockRelease(oldPartitionLock);
		/* safety check: should definitely not be our *own* pin */
		if (GetPrivateRefCount(BufferDescriptorGetBuffer(buf)) > 0)
			elog(ERROR, "buffer is pinned in InvalidateBuffer");
		WaitIO(buf);
		goto retry;
	}

	/* POLAR: Flush the buffer flashback log record */
	if (buf_state & BM_PERMANENT)
		polar_flush_buf_flog_rec(buf, flog_instance, true);

	/*
	 * Clear out the buffer's tag and flags.  We must do this to ensure that
	 * linear scans of the buffer array don't think the buffer is valid.
	 */
	oldFlags = buf_state & BUF_FLAG_MASK;
	CLEAR_BUFFERTAG(buf->tag);
	buf_state &= ~(BUF_FLAG_MASK | BUF_USAGECOUNT_MASK);
	UnlockBufHdr(buf, buf_state);

	/*
	 * POLAR:Clear all redo state
	 */

	/*
	 * Remove the buffer from the lookup hashtable, if it was in there.
	 */
	if (oldFlags & BM_TAG_VALID)
		BufTableDelete(&oldTag, oldHash);

	/*
	 * POLAR: Make true there are no flashback record of the buffer.
	 */
	polar_make_true_no_flog(flog_instance, buf);

	/* POLAR: Clear all polar redo state flag */
	redo_state = polar_lock_redo_state(buf);
	redo_state &= ~(POLAR_BUF_REDO_FLAG_MASK | POLAR_BUF_FLASHBACK_FLAG_MASK);
	polar_unlock_redo_state(buf, redo_state);

	/* POLAR: free its copy buffer and remove it from flushlist */
	if (polar_enable_shared_storage_mode)
	{
		polar_free_copy_buffer(buf);
		polar_reset_buffer_oldest_lsn(buf);
	}

	/*
	 * Done with mapping lock.
	 */
	LWLockRelease(oldPartitionLock);

	/*
	 * Insert the buffer at the head of the list of free buffers.
	 */
	StrategyFreeBuffer(buf);
}

/*
 * MarkBufferDirty
 *
 *		Marks buffer contents as dirty (actual write happens later).
 *
 * Buffer must be pinned and exclusive-locked.  (If caller does not hold
 * exclusive lock, then somebody could be in process of writing the buffer,
 * leading to risk of bad data written to disk.)
 */
void
MarkBufferDirty(Buffer buffer)
{
	BufferDesc *bufHdr;
	uint32		buf_state;
	uint32		old_buf_state;

	if (!BufferIsValid(buffer))
		elog(ERROR, "bad buffer ID: %d", buffer);

	if (BufferIsLocal(buffer))
	{
		MarkLocalBufferDirty(buffer);
		return;
	}

	bufHdr = GetBufferDescriptor(buffer - 1);

	Assert(BufferIsPinned(buffer));

	/* POLAR: If ro promoted to be rw, then we will mark buffer dirty when read buffer from storage and replay xlog */
	Assert(LWLockHeldByMeInMode(BufferDescriptorGetContentLock(bufHdr),
								LW_EXCLUSIVE)
			|| LWLockHeldByMeInMode(BufferDescriptorGetIOLock(bufHdr), LW_EXCLUSIVE));

	old_buf_state = pg_atomic_read_u32(&bufHdr->state);
	for (;;)
	{
		if (old_buf_state & BM_LOCKED)
			old_buf_state = WaitBufHdrUnlocked(bufHdr);

		buf_state = old_buf_state;

		Assert(BUF_STATE_GET_REFCOUNT(buf_state) > 0);
		buf_state |= BM_DIRTY | BM_JUST_DIRTIED;

		if (pg_atomic_compare_exchange_u32(&bufHdr->state, &old_buf_state,
										   buf_state))
			break;
	}

	/* POLAR: set a fake oldest lsn */
	polar_set_buffer_fake_oldest_lsn(bufHdr);

	/*
	 * If the buffer was not dirty already, do vacuum accounting.
	 */
	if (!(old_buf_state & BM_DIRTY))
	{
		VacuumPageDirty++;
		pgBufferUsage.shared_blks_dirtied++;
		if (VacuumCostActive)
			VacuumCostBalance += VacuumCostPageDirty;
	}
}

/*
 * ReleaseAndReadBuffer -- combine ReleaseBuffer() and ReadBuffer()
 *
 * Formerly, this saved one cycle of acquiring/releasing the BufMgrLock
 * compared to calling the two routines separately.  Now it's mainly just
 * a convenience function.  However, if the passed buffer is valid and
 * already contains the desired block, we just return it as-is; and that
 * does save considerable work compared to a full release and reacquire.
 *
 * Note: it is OK to pass buffer == InvalidBuffer, indicating that no old
 * buffer actually needs to be released.  This case is the same as ReadBuffer,
 * but can save some tests in the caller.
 */
Buffer
ReleaseAndReadBuffer(Buffer buffer,
					 Relation relation,
					 BlockNumber blockNum)
{
	ForkNumber	forkNum = MAIN_FORKNUM;
	BufferDesc *bufHdr;

	if (BufferIsValid(buffer))
	{
		Assert(BufferIsPinned(buffer));
		if (BufferIsLocal(buffer))
		{
			bufHdr = GetLocalBufferDescriptor(-buffer - 1);
			if (bufHdr->tag.blockNum == blockNum &&
				RelFileNodeEquals(bufHdr->tag.rnode, relation->rd_node) &&
				bufHdr->tag.forkNum == forkNum)
				return buffer;
			ResourceOwnerForgetBuffer(CurrentResourceOwner, buffer);
			LocalRefCount[-buffer - 1]--;
		}
		else
		{
			bufHdr = GetBufferDescriptor(buffer - 1);
			/* we have pin, so it's ok to examine tag without spinlock */
			if (bufHdr->tag.blockNum == blockNum &&
				RelFileNodeEquals(bufHdr->tag.rnode, relation->rd_node) &&
				bufHdr->tag.forkNum == forkNum)
				return buffer;
			UnpinBuffer(bufHdr, true);
		}
	}

	return ReadBuffer(relation, blockNum);
}

/*
 * PinBuffer -- make buffer unavailable for replacement.
 *
 * For the default access strategy, the buffer's usage_count is incremented
 * when we first pin it; for other strategies we just make sure the usage_count
 * isn't zero.  (The idea of the latter is that we don't want synchronized
 * heap scans to inflate the count, but we need it to not be zero to discourage
 * other backends from stealing buffers from our ring.  As long as we cycle
 * through the ring faster than the global clock-sweep cycles, buffers in
 * our ring won't be chosen as victims for replacement by other backends.)
 *
 * This should be applied only to shared buffers, never local ones.
 *
 * Since buffers are pinned/unpinned very frequently, pin buffers without
 * taking the buffer header lock; instead update the state variable in loop of
 * CAS operations. Hopefully it's just a single CAS.
 *
 * Note that ResourceOwnerEnlargeBuffers must have been done already.
 *
 * Returns true if buffer is BM_VALID, else false.  This provision allows
 * some callers to avoid an extra spinlock cycle.
 */
static bool
PinBuffer(BufferDesc *buf, BufferAccessStrategy strategy)
{
	Buffer		b = BufferDescriptorGetBuffer(buf);
	bool		result;
	PrivateRefCountEntry *ref;

	ref = GetPrivateRefCountEntry(b, true);

	if (ref == NULL)
	{
		uint32		buf_state;
		uint32		old_buf_state;

		ReservePrivateRefCountEntry();
		ref = NewPrivateRefCountEntry(b);

		old_buf_state = pg_atomic_read_u32(&buf->state);
		for (;;)
		{
			if (old_buf_state & BM_LOCKED)
				old_buf_state = WaitBufHdrUnlocked(buf);

			buf_state = old_buf_state;

			/* increase refcount */
			buf_state += BUF_REFCOUNT_ONE;

			if (strategy == NULL)
			{
				/* Default case: increase usagecount unless already max. */
				if (BUF_STATE_GET_USAGECOUNT(buf_state) < BM_MAX_USAGE_COUNT)
					buf_state += BUF_USAGECOUNT_ONE;
			}
			else
			{
				/*
				 * Ring buffers shouldn't evict others from pool.  Thus we
				 * don't make usagecount more than 1.
				 */
				if (BUF_STATE_GET_USAGECOUNT(buf_state) == 0)
					buf_state += BUF_USAGECOUNT_ONE;
			}

			if (pg_atomic_compare_exchange_u32(&buf->state, &old_buf_state,
											   buf_state))
			{
				result = (buf_state & BM_VALID) != 0;
				break;
			}
		}
	}
	else
	{
		/* If we previously pinned the buffer, it must surely be valid */
		result = true;
	}

	ref->refcount++;
	Assert(ref->refcount > 0);
	ResourceOwnerRememberBuffer(CurrentResourceOwner, b);
	return result;
}

/*
 * PinBuffer_Locked -- as above, but caller already locked the buffer header.
 * The spinlock is released before return.
 *
 * As this function is called with the spinlock held, the caller has to
 * previously call ReservePrivateRefCountEntry().
 *
 * Currently, no callers of this function want to modify the buffer's
 * usage_count at all, so there's no need for a strategy parameter.
 * Also we don't bother with a BM_VALID test (the caller could check that for
 * itself).
 *
 * Also all callers only ever use this function when it's known that the
 * buffer can't have a preexisting pin by this backend. That allows us to skip
 * searching the private refcount array & hash, which is a boon, because the
 * spinlock is still held.
 *
 * Note: use of this routine is frequently mandatory, not just an optimization
 * to save a spin lock/unlock cycle, because we need to pin a buffer before
 * its state can change under us.
 */
static void
PinBuffer_Locked(BufferDesc *buf)
{
	Buffer		b;
	PrivateRefCountEntry *ref;
	uint32		buf_state;

	/*
	 * As explained, We don't expect any preexisting pins. That allows us to
	 * manipulate the PrivateRefCount after releasing the spinlock
	 */
	Assert(GetPrivateRefCountEntry(BufferDescriptorGetBuffer(buf), false) == NULL);

	/*
	 * Since we hold the buffer spinlock, we can update the buffer state and
	 * release the lock in one operation.
	 */
	buf_state = pg_atomic_read_u32(&buf->state);
	Assert(buf_state & BM_LOCKED);
	buf_state += BUF_REFCOUNT_ONE;
	UnlockBufHdr(buf, buf_state);

	b = BufferDescriptorGetBuffer(buf);

	ref = NewPrivateRefCountEntry(b);
	ref->refcount++;

	ResourceOwnerRememberBuffer(CurrentResourceOwner, b);
}

/*
 * UnpinBuffer -- make buffer available for replacement.
 *
 * This should be applied only to shared buffers, never local ones.
 *
 * Most but not all callers want CurrentResourceOwner to be adjusted.
 * Those that don't should pass fixOwner = false.
 */
static void
UnpinBuffer(BufferDesc *buf, bool fixOwner)
{
	PrivateRefCountEntry *ref;
	Buffer		b = BufferDescriptorGetBuffer(buf);

	/* not moving as we're likely deleting it soon anyway */
	ref = GetPrivateRefCountEntry(b, false);
	Assert(ref != NULL);

	if (fixOwner)
		ResourceOwnerForgetBuffer(CurrentResourceOwner, b);

	Assert(ref->refcount > 0);
	ref->refcount--;
	if (ref->refcount == 0)
	{
		uint32		buf_state;
		uint32		old_buf_state;

		/* I'd better not still hold any locks on the buffer */
		Assert(!LWLockHeldByMe(BufferDescriptorGetContentLock(buf)));
		Assert(!LWLockHeldByMe(BufferDescriptorGetIOLock(buf)));

		/*
		 * Decrement the shared reference count.
		 *
		 * Since buffer spinlock holder can update status using just write,
		 * it's not safe to use atomic decrement here; thus use a CAS loop.
		 */
		old_buf_state = pg_atomic_read_u32(&buf->state);
		for (;;)
		{
			if (old_buf_state & BM_LOCKED)
				old_buf_state = WaitBufHdrUnlocked(buf);

			buf_state = old_buf_state;

			buf_state -= BUF_REFCOUNT_ONE;

			if (pg_atomic_compare_exchange_u32(&buf->state, &old_buf_state,
											   buf_state))
				break;
		}

		/* Support LockBufferForCleanup() */
		if (buf_state & BM_PIN_COUNT_WAITER)
		{
			/*
			 * Acquire the buffer header lock, re-check that there's a waiter.
			 * Another backend could have unpinned this buffer, and already
			 * woken up the waiter.  There's no danger of the buffer being
			 * replaced after we unpinned it above, as it's pinned by the
			 * waiter.
			 */
			buf_state = LockBufHdr(buf);

			if ((buf_state & BM_PIN_COUNT_WAITER) &&
				BUF_STATE_GET_REFCOUNT(buf_state) == 1)
			{
				/* we just released the last pin other than the waiter's */
				int			wait_backend_pid = buf->wait_backend_pid;

				buf_state &= ~BM_PIN_COUNT_WAITER;
				UnlockBufHdr(buf, buf_state);
				ProcSendSignal(wait_backend_pid);
			}
			else
				UnlockBufHdr(buf, buf_state);
		}
		ForgetPrivateRefCountEntry(ref);
	}
}

/*
 * BufferSync -- Write out all dirty buffers in the pool.
 *
 * This is called at checkpoint time to write out all dirty shared buffers.
 * The checkpoint request flags should be passed in.  If CHECKPOINT_IMMEDIATE
 * is set, we disable delays between writes; if CHECKPOINT_IS_SHUTDOWN,
 * CHECKPOINT_END_OF_RECOVERY or CHECKPOINT_FLUSH_ALL is set, we write even
 * unlogged buffers, which are otherwise skipped.  The remaining flags
 * currently have no effect here.
 */
static void
BufferSync(int flags)
{
	uint32		buf_state;
	int			buf_id;
	int			num_to_scan;
	int			num_spaces;
	int			num_processed;
	int			num_written;
	CkptTsStatus *per_ts_stat = NULL;
	Oid			last_tsid;
	binaryheap *ts_heap;
	int			i;
	int			mask = BM_DIRTY;
	WritebackContext wb_context;

	/* POLAR */
	XLogRecPtr	oldest_apply_lsn = polar_get_oldest_applied_lsn();

	/* Make sure we can handle the pin inside SyncOneBuffer */
	ResourceOwnerEnlargeBuffers(CurrentResourceOwner);

	/*
	 * Unless this is a shutdown checkpoint or we have been explicitly told,
	 * we write only permanent, dirty buffers.  But at shutdown or end of
	 * recovery, we write all dirty buffers.
	 */
	if (!((flags & (CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_END_OF_RECOVERY |
					CHECKPOINT_FLUSH_ALL))))
		mask |= BM_PERMANENT;

	/*
	 * Loop over all buffers, and mark the ones that need to be written with
	 * BM_CHECKPOINT_NEEDED.  Count them as we go (num_to_scan), so that we
	 * can estimate how much work needs to be done.
	 *
	 * This allows us to write only those pages that were dirty when the
	 * checkpoint began, and not those that get dirtied while it proceeds.
	 * Whenever a page with BM_CHECKPOINT_NEEDED is written out, either by us
	 * later in this function, or by normal backends or the bgwriter cleaning
	 * scan, the flag is cleared.  Any buffer dirtied after this point won't
	 * have the flag set.
	 *
	 * Note that if we fail to write some buffer, we may leave buffers with
	 * BM_CHECKPOINT_NEEDED still set.  This is OK since any such buffer would
	 * certainly need to be written for the next checkpoint attempt, too.
	 */
	num_to_scan = 0;
	for (buf_id = 0; buf_id < NBuffers; buf_id++)
	{
		BufferDesc *bufHdr = GetBufferDescriptor(buf_id);

		/*
		 * Header spinlock is enough to examine BM_DIRTY, see comment in
		 * SyncOneBuffer.
		 */
		buf_state = LockBufHdr(bufHdr);

		/*
		 * POLAR: the lsn check here will reduce the list of buffer to be qsort.
		 * Actually, there is another throttle against the lsn later in
		 * SyncOneBuffer().
		 */
		if (((buf_state & mask) == mask) &&
			polar_buffer_can_be_flushed_by_checkpoint(bufHdr, oldest_apply_lsn, flags))
		{
			CkptSortItem *item;

			buf_state |= BM_CHECKPOINT_NEEDED;

			item = &CkptBufferIds[num_to_scan++];
			item->buf_id = buf_id;
			item->tsId = bufHdr->tag.rnode.spcNode;
			item->relNode = bufHdr->tag.rnode.relNode;
			item->forkNum = bufHdr->tag.forkNum;
			item->blockNum = bufHdr->tag.blockNum;
		}

		UnlockBufHdr(bufHdr, buf_state);
	}

	if (num_to_scan == 0)
		return;					/* nothing to do */

	WritebackContextInit(&wb_context, &checkpoint_flush_after);

	TRACE_POSTGRESQL_BUFFER_SYNC_START(NBuffers, num_to_scan);

	/*
	 * Sort buffers that need to be written to reduce the likelihood of random
	 * IO. The sorting is also important for the implementation of balancing
	 * writes between tablespaces. Without balancing writes we'd potentially
	 * end up writing to the tablespaces one-by-one; possibly overloading the
	 * underlying system.
	 */
	qsort(CkptBufferIds, num_to_scan, sizeof(CkptSortItem),
		  ckpt_buforder_comparator);

	num_spaces = 0;

	/*
	 * Allocate progress status for each tablespace with buffers that need to
	 * be flushed. This requires the to-be-flushed array to be sorted.
	 */
	last_tsid = InvalidOid;
	for (i = 0; i < num_to_scan; i++)
	{
		CkptTsStatus *s;
		Oid			cur_tsid;

		cur_tsid = CkptBufferIds[i].tsId;

		/*
		 * Grow array of per-tablespace status structs, every time a new
		 * tablespace is found.
		 */
		if (last_tsid == InvalidOid || last_tsid != cur_tsid)
		{
			Size		sz;

			num_spaces++;

			/*
			 * Not worth adding grow-by-power-of-2 logic here - even with a
			 * few hundred tablespaces this should be fine.
			 */
			sz = sizeof(CkptTsStatus) * num_spaces;

			if (per_ts_stat == NULL)
				per_ts_stat = (CkptTsStatus *) palloc(sz);
			else
				per_ts_stat = (CkptTsStatus *) repalloc(per_ts_stat, sz);

			s = &per_ts_stat[num_spaces - 1];
			memset(s, 0, sizeof(*s));
			s->tsId = cur_tsid;

			/*
			 * The first buffer in this tablespace. As CkptBufferIds is sorted
			 * by tablespace all (s->num_to_scan) buffers in this tablespace
			 * will follow afterwards.
			 */
			s->index = i;

			/*
			 * progress_slice will be determined once we know how many buffers
			 * are in each tablespace, i.e. after this loop.
			 */

			last_tsid = cur_tsid;
		}
		else
		{
			s = &per_ts_stat[num_spaces - 1];
		}

		s->num_to_scan++;
	}

	Assert(num_spaces > 0);

	/*
	 * Build a min-heap over the write-progress in the individual tablespaces,
	 * and compute how large a portion of the total progress a single
	 * processed buffer is.
	 */
	ts_heap = binaryheap_allocate(num_spaces,
								  ts_ckpt_progress_comparator,
								  NULL);

	for (i = 0; i < num_spaces; i++)
	{
		CkptTsStatus *ts_stat = &per_ts_stat[i];

		ts_stat->progress_slice = (float8) num_to_scan / ts_stat->num_to_scan;

		binaryheap_add_unordered(ts_heap, PointerGetDatum(ts_stat));
	}

	binaryheap_build(ts_heap);

	/*
	 * Iterate through to-be-checkpointed buffers and write the ones (still)
	 * marked with BM_CHECKPOINT_NEEDED. The writes are balanced between
	 * tablespaces; otherwise the sorting would lead to only one tablespace
	 * receiving writes at a time, making inefficient use of the hardware.
	 */
	num_processed = 0;
	num_written = 0;
	while (!binaryheap_empty(ts_heap))
	{
		BufferDesc *bufHdr = NULL;
		CkptTsStatus *ts_stat = (CkptTsStatus *)
		DatumGetPointer(binaryheap_first(ts_heap));

		buf_id = CkptBufferIds[ts_stat->index].buf_id;
		Assert(buf_id != -1);

		bufHdr = GetBufferDescriptor(buf_id);

		num_processed++;

		/*
		 * We don't need to acquire the lock here, because we're only looking
		 * at a single bit. It's possible that someone else writes the buffer
		 * and clears the flag right after we check, but that doesn't matter
		 * since SyncOneBuffer will then do nothing.  However, there is a
		 * further race condition: it's conceivable that between the time we
		 * examine the bit here and the time SyncOneBuffer acquires the lock,
		 * someone else not only wrote the buffer but replaced it with another
		 * page and dirtied it.  In that improbable case, SyncOneBuffer will
		 * write the buffer though we didn't need to.  It doesn't seem worth
		 * guarding against this, though.
		 */
		if (pg_atomic_read_u32(&bufHdr->state) & BM_CHECKPOINT_NEEDED)
		{
			if (SyncOneBuffer(buf_id,
							  false,
							  &wb_context, flags) & BUF_WRITTEN)
			{
				TRACE_POSTGRESQL_BUFFER_SYNC_WRITTEN(buf_id);
				BgWriterStats.m_buf_written_checkpoints++;
				num_written++;
			}
		}

		/*
		 * Measure progress independent of actually having to flush the buffer
		 * - otherwise writing become unbalanced.
		 */
		ts_stat->progress += ts_stat->progress_slice;
		ts_stat->num_scanned++;
		ts_stat->index++;

		/* Have all the buffers from the tablespace been processed? */
		if (ts_stat->num_scanned == ts_stat->num_to_scan)
		{
			binaryheap_remove_first(ts_heap);
		}
		else
		{
			/* update heap with the new progress */
			binaryheap_replace_first(ts_heap, PointerGetDatum(ts_stat));
		}

		/*
		 * Sleep to throttle our I/O rate.
		 */
		CheckpointWriteDelay(flags, (double) num_processed / num_to_scan);
	}

	/* issue all pending flushes */
	IssuePendingWritebacks(&wb_context);

	pfree(per_ts_stat);
	per_ts_stat = NULL;
	binaryheap_free(ts_heap);

	/*
	 * Update checkpoint statistics. As noted above, this doesn't include
	 * buffers written by other backends or bgwriter scan.
	 */
	CheckpointStats.ckpt_bufs_written += num_written;

	TRACE_POSTGRESQL_BUFFER_SYNC_DONE(NBuffers, num_written, num_to_scan);
}

/*
 * BgBufferSync -- Write out some dirty buffers in the pool.
 *
 * This is called periodically by the background writer process.
 *
 * Returns true if it's appropriate for the bgwriter process to go into
 * low-power hibernation mode.  (This happens if the strategy clock sweep
 * has been "lapped" and no buffer allocations have occurred recently,
 * or if the bgwriter has been effectively disabled by setting
 * bgwriter_lru_maxpages to 0.)
 */
bool
BgBufferSync(WritebackContext *wb_context, int flags)
{
	/* info obtained from freelist.c */
	int			strategy_buf_id;
	uint32		strategy_passes;
	uint32		recent_alloc;

	/*
	 * Information saved between calls so we can determine the strategy
	 * point's advance rate and avoid scanning already-cleaned buffers.
	 */
	static bool saved_info_valid = false;
	static int	prev_strategy_buf_id;
	static uint32 prev_strategy_passes;
	static int	next_to_clean;
	static uint32 next_passes;

	/* Moving averages of allocation rate and clean-buffer density */
	static float smoothed_alloc = 0;
	static float smoothed_density = 10.0;

	/* Potentially these could be tunables, but for now, not */
	float		smoothing_samples = 16;
	float		scan_whole_pool_milliseconds = 120000.0;

	/* Used to compute how far we scan ahead */
	long		strategy_delta;
	int			bufs_to_lap;
	int			bufs_ahead;
	float		scans_per_alloc;
	int			reusable_buffers_est;
	int			upcoming_alloc_est;
	int			min_scan_buffers;

	/* Variables for the scanning loop proper */
	int			num_to_scan;
	int			num_written;
	int			reusable_buffers;

	/* Variables for final smoothed_density update */
	long		new_strategy_delta;
	uint32		new_recent_alloc;

	/*
	 * Find out where the freelist clock sweep currently is, and how many
	 * buffer allocations have happened since our last call.
	 */
	strategy_buf_id = StrategySyncStart(&strategy_passes, &recent_alloc);

	/* Report buffer alloc counts to pgstat */
	BgWriterStats.m_buf_alloc += recent_alloc;

	/*
	 * If we're not running the LRU scan, just stop after doing the stats
	 * stuff.  We mark the saved state invalid so that we can recover sanely
	 * if LRU scan is turned back on later.
	 */
	if (bgwriter_lru_maxpages <= 0)
	{
		saved_info_valid = false;
		return true;
	}

	/*
	 * Compute strategy_delta = how many buffers have been scanned by the
	 * clock sweep since last time.  If first time through, assume none. Then
	 * see if we are still ahead of the clock sweep, and if so, how many
	 * buffers we could scan before we'd catch up with it and "lap" it. Note:
	 * weird-looking coding of xxx_passes comparisons are to avoid bogus
	 * behavior when the passes counts wrap around.
	 */
	if (saved_info_valid)
	{
		int32		passes_delta = strategy_passes - prev_strategy_passes;

		strategy_delta = strategy_buf_id - prev_strategy_buf_id;
		strategy_delta += (long) passes_delta * NBuffers;

		Assert(strategy_delta >= 0);

		if ((int32) (next_passes - strategy_passes) > 0)
		{
			/* we're one pass ahead of the strategy point */
			bufs_to_lap = strategy_buf_id - next_to_clean;
#ifdef BGW_DEBUG
			elog(DEBUG2, "bgwriter ahead: bgw %u-%u strategy %u-%u delta=%ld lap=%d",
				 next_passes, next_to_clean,
				 strategy_passes, strategy_buf_id,
				 strategy_delta, bufs_to_lap);
#endif
		}
		else if (next_passes == strategy_passes &&
				 next_to_clean >= strategy_buf_id)
		{
			/* on same pass, but ahead or at least not behind */
			bufs_to_lap = NBuffers - (next_to_clean - strategy_buf_id);
#ifdef BGW_DEBUG
			elog(DEBUG2, "bgwriter ahead: bgw %u-%u strategy %u-%u delta=%ld lap=%d",
				 next_passes, next_to_clean,
				 strategy_passes, strategy_buf_id,
				 strategy_delta, bufs_to_lap);
#endif
		}
		else
		{
			/*
			 * We're behind, so skip forward to the strategy point and start
			 * cleaning from there.
			 */
#ifdef BGW_DEBUG
			elog(DEBUG2, "bgwriter behind: bgw %u-%u strategy %u-%u delta=%ld",
				 next_passes, next_to_clean,
				 strategy_passes, strategy_buf_id,
				 strategy_delta);
#endif
			next_to_clean = strategy_buf_id;
			next_passes = strategy_passes;
			bufs_to_lap = NBuffers;
		}
	}
	else
	{
		/*
		 * Initializing at startup or after LRU scanning had been off. Always
		 * start at the strategy point.
		 */
#ifdef BGW_DEBUG
		elog(DEBUG2, "bgwriter initializing: strategy %u-%u",
			 strategy_passes, strategy_buf_id);
#endif
		strategy_delta = 0;
		next_to_clean = strategy_buf_id;
		next_passes = strategy_passes;
		bufs_to_lap = NBuffers;
	}

	/* Update saved info for next time */
	prev_strategy_buf_id = strategy_buf_id;
	prev_strategy_passes = strategy_passes;
	saved_info_valid = true;

	/*
	 * Compute how many buffers had to be scanned for each new allocation, ie,
	 * 1/density of reusable buffers, and track a moving average of that.
	 *
	 * If the strategy point didn't move, we don't update the density estimate
	 */
	if (strategy_delta > 0 && recent_alloc > 0)
	{
		scans_per_alloc = (float) strategy_delta / (float) recent_alloc;
		smoothed_density += (scans_per_alloc - smoothed_density) /
			smoothing_samples;
	}

	/*
	 * Estimate how many reusable buffers there are between the current
	 * strategy point and where we've scanned ahead to, based on the smoothed
	 * density estimate.
	 */
	bufs_ahead = NBuffers - bufs_to_lap;
	reusable_buffers_est = (float) bufs_ahead / smoothed_density;

	/*
	 * Track a moving average of recent buffer allocations.  Here, rather than
	 * a true average we want a fast-attack, slow-decline behavior: we
	 * immediately follow any increase.
	 */
	if (smoothed_alloc <= (float) recent_alloc)
		smoothed_alloc = recent_alloc;
	else
		smoothed_alloc += ((float) recent_alloc - smoothed_alloc) /
			smoothing_samples;

	/* Scale the estimate by a GUC to allow more aggressive tuning. */
	upcoming_alloc_est = (int) (smoothed_alloc * bgwriter_lru_multiplier);

	/*
	 * If recent_alloc remains at zero for many cycles, smoothed_alloc will
	 * eventually underflow to zero, and the underflows produce annoying
	 * kernel warnings on some platforms.  Once upcoming_alloc_est has gone to
	 * zero, there's no point in tracking smaller and smaller values of
	 * smoothed_alloc, so just reset it to exactly zero to avoid this
	 * syndrome.  It will pop back up as soon as recent_alloc increases.
	 */
	if (upcoming_alloc_est == 0)
		smoothed_alloc = 0;

	/*
	 * Even in cases where there's been little or no buffer allocation
	 * activity, we want to make a small amount of progress through the buffer
	 * cache so that as many reusable buffers as possible are clean after an
	 * idle period.
	 *
	 * (scan_whole_pool_milliseconds / BgWriterDelay) computes how many times
	 * the BGW will be called during the scan_whole_pool time; slice the
	 * buffer pool into that many sections.
	 */
	min_scan_buffers = (int) (NBuffers / (scan_whole_pool_milliseconds / BgWriterDelay));

	if (upcoming_alloc_est < (min_scan_buffers + reusable_buffers_est))
	{
#ifdef BGW_DEBUG
		elog(DEBUG2, "bgwriter: alloc_est=%d too small, using min=%d + reusable_est=%d",
			 upcoming_alloc_est, min_scan_buffers, reusable_buffers_est);
#endif
		upcoming_alloc_est = min_scan_buffers + reusable_buffers_est;
	}

	/*
	 * Now write out dirty reusable buffers, working forward from the
	 * next_to_clean point, until we have lapped the strategy scan, or cleaned
	 * enough buffers to match our estimate of the next cycle's allocation
	 * requirements, or hit the bgwriter_lru_maxpages limit.
	 */

	/* Make sure we can handle the pin inside SyncOneBuffer */
	ResourceOwnerEnlargeBuffers(CurrentResourceOwner);

	num_to_scan = bufs_to_lap;
	num_written = 0;
	reusable_buffers = reusable_buffers_est;

	/* Execute the LRU scan */
	while (num_to_scan > 0 && reusable_buffers < upcoming_alloc_est)
	{
		int			sync_state = 0;

		sync_state = SyncOneBuffer(next_to_clean,
								   true,
								   wb_context,
								   flags);

		if (++next_to_clean >= NBuffers)
		{
			next_to_clean = 0;
			next_passes++;
		}
		num_to_scan--;

		if (sync_state & BUF_WRITTEN)
		{
			reusable_buffers++;
			if (++num_written >= bgwriter_lru_maxpages)
			{
				BgWriterStats.m_maxwritten_clean++;
				break;
			}
		}
		else if (sync_state & BUF_REUSABLE)
			reusable_buffers++;
	}

	BgWriterStats.m_buf_written_clean += num_written;

#ifdef BGW_DEBUG
	elog(DEBUG1, "bgwriter: recent_alloc=%u smoothed=%.2f delta=%ld ahead=%d density=%.2f reusable_est=%d upcoming_est=%d scanned=%d wrote=%d reusable=%d",
		 recent_alloc, smoothed_alloc, strategy_delta, bufs_ahead,
		 smoothed_density, reusable_buffers_est, upcoming_alloc_est,
		 bufs_to_lap - num_to_scan,
		 num_written,
		 reusable_buffers - reusable_buffers_est);
#endif

	/*
	 * Consider the above scan as being like a new allocation scan.
	 * Characterize its density and update the smoothed one based on it. This
	 * effectively halves the moving average period in cases where both the
	 * strategy and the background writer are doing some useful scanning,
	 * which is helpful because a long memory isn't as desirable on the
	 * density estimates.
	 */
	new_strategy_delta = bufs_to_lap - num_to_scan;
	new_recent_alloc = reusable_buffers - reusable_buffers_est;
	if (new_strategy_delta > 0 && new_recent_alloc > 0)
	{
		scans_per_alloc = (float) new_strategy_delta / (float) new_recent_alloc;
		smoothed_density += (scans_per_alloc - smoothed_density) /
			smoothing_samples;

#ifdef BGW_DEBUG
		elog(DEBUG2, "bgwriter: cleaner density alloc=%u scan=%ld density=%.2f new smoothed=%.2f",
			 new_recent_alloc, new_strategy_delta,
			 scans_per_alloc, smoothed_density);
#endif
	}

	/* Return true if OK to hibernate */
	return (bufs_to_lap == 0 && recent_alloc == 0);
}

/*
 * SyncOneBuffer -- process a single buffer during syncing.
 *
 * If skip_recently_used is true, we don't write currently-pinned buffers, nor
 * buffers marked recently used, as these are not replacement candidates.
 *
 * Returns a bitmask containing the following flag bits:
 *	BUF_WRITTEN: we wrote the buffer.
 *	BUF_REUSABLE: buffer is available for replacement, ie, it has
 *		pin count 0 and usage count 0.
 *
 * (BUF_WRITTEN could be set in error if FlushBuffers finds the buffer clean
 * after locking it, but we don't care all that much.)
 *
 * Note: caller must have done ResourceOwnerEnlargeBuffers.
 */
static int
SyncOneBuffer(int buf_id,
			  bool skip_recently_used,
			  WritebackContext *wb_context,
			  int flags)
{
	BufferDesc *bufHdr = GetBufferDescriptor(buf_id);
	int			result = 0;
	uint32		buf_state;
	BufferTag	tag;

	/* POLAR */
	XLogRecPtr	polar_oldest_apply_lsn;

	ReservePrivateRefCountEntry();

	/*
	 * Check whether buffer needs writing.
	 *
	 * We can make this check without taking the buffer content lock so long
	 * as we mark pages dirty in access methods *before* logging changes with
	 * XLogInsert(): if someone marks the buffer dirty just after our check we
	 * don't worry because our checkpoint.redo points before log record for
	 * upcoming changes and so we are not required to write such dirty buffer.
	 */
	buf_state = LockBufHdr(bufHdr);

	if (BUF_STATE_GET_REFCOUNT(buf_state) == 0 &&
		BUF_STATE_GET_USAGECOUNT(buf_state) == 0)
	{
		result |= BUF_REUSABLE;
	}
	else if (skip_recently_used)
	{
		/* Caller told us not to write recently-used buffers */
		UnlockBufHdr(bufHdr, buf_state);
		return result;
	}

	if (!(buf_state & BM_VALID) || !(buf_state & BM_DIRTY))
	{
		/* It's clean, so nothing to do */
		UnlockBufHdr(bufHdr, buf_state);
		return result;
	}

	/* POLAR */
	polar_oldest_apply_lsn = polar_get_oldest_applied_lsn();

	/*
	 * Pin it, share-lock it, write it.  (FlushBuffer will do nothing if the
	 * buffer is clean by the time we've locked it.)
	 */
	PinBuffer_Locked(bufHdr);
	LWLockAcquire(BufferDescriptorGetContentLock(bufHdr), LW_SHARED);

	/* POLAR: if the buffer can be flushed, flush it. */
	if (polar_buffer_can_be_flushed(bufHdr, polar_oldest_apply_lsn, false))
	{
		/* Flush buffer directly, if it has copy buffer, that will be freed */
		FlushBuffer(bufHdr, NULL, polar_oldest_apply_lsn, false);
		LWLockRelease(BufferDescriptorGetContentLock(bufHdr));

		tag = bufHdr->tag;

		UnpinBuffer(bufHdr, true);

		if (!polar_enable_shared_storage_mode)
			ScheduleBufferTagForWriteback(wb_context, &tag);

		return result | BUF_WRITTEN;
	}
	/*
	 * POLAR: try to write fullpage wal
	 * NOTE: shutdown checkpoint don't allow write wal record
	 */
	else if (!(flags & CHECKPOINT_IS_SHUTDOWN) &&
			polar_buffer_need_fullpage_snapshot(bufHdr, polar_oldest_apply_lsn))
	{
		FlushBuffer(bufHdr, NULL, polar_oldest_apply_lsn, false);
		LWLockRelease(BufferDescriptorGetContentLock(bufHdr));
		UnpinBuffer(bufHdr, true);
		return result | BUF_WRITTEN;
	}
	/*
	 * POLAR: if buffer has a copy buffer and the copy buffer can be flushed,
	 * flush the copy buffer.
	 */
	else if (polar_buffer_can_be_flushed(bufHdr, polar_oldest_apply_lsn, true))
	{
		/* Flush copy buffer */
		polar_flush_copy_buffer(bufHdr, NULL);
		LWLockRelease(BufferDescriptorGetContentLock(bufHdr));
		UnpinBuffer(bufHdr, true);
		return result;
	}
	/* POLAR: if buffer satisfy the copy condition, create a copy buffer for it. */
	else
	{
		/* Can not flush, can we copy the buffer? */
		if (polar_buffer_copy_is_satisfied(bufHdr, polar_oldest_apply_lsn, true))
		{
			polar_buffer_copy_if_needed(bufHdr, polar_oldest_apply_lsn);
		}
		else if (unlikely(polar_enable_debug))
		{
			elog(DEBUG1,
			     "Buffer ([%u,%u,%u], %u, %d) can not be copied, oldest apply %X/%X, oldest lsn %X/%X, latest lsn %X/%X, copied: %d",
			     bufHdr->tag.rnode.spcNode,
			     bufHdr->tag.rnode.dbNode,
			     bufHdr->tag.rnode.relNode,
			     bufHdr->tag.blockNum,
			     bufHdr->tag.forkNum,
			     (uint32) (polar_oldest_apply_lsn >> 32),
			     (uint32) polar_oldest_apply_lsn,
			     (uint32) (polar_buffer_get_oldest_lsn(bufHdr) >> 32),
			     (uint32) polar_buffer_get_oldest_lsn(bufHdr),
			     (uint32) (BufferGetLSN(bufHdr) >> 32),
			     (uint32) BufferGetLSN(bufHdr),
			     bufHdr->copy_buffer != NULL);
		}

		LWLockRelease(BufferDescriptorGetContentLock(bufHdr));
		UnpinBuffer(bufHdr, true);
		return result;
	}
}

/*
 *		AtEOXact_Buffers - clean up at end of transaction.
 *
 *		As of PostgreSQL 8.0, buffer pins should get released by the
 *		ResourceOwner mechanism.  This routine is just a debugging
 *		cross-check that no pins remain.
 */
void
AtEOXact_Buffers(bool isCommit)
{
	CheckForBufferLeaks();

	AtEOXact_LocalBuffers(isCommit);

	Assert(PrivateRefCountOverflowed == 0);
}

/*
 * Initialize access to shared buffer pool
 *
 * This is called during backend startup (whether standalone or under the
 * postmaster).  It sets up for this backend's access to the already-existing
 * buffer pool.
 *
 * NB: this is called before InitProcess(), so we do not have a PGPROC and
 * cannot do LWLockAcquire; hence we can't actually access stuff in
 * shared memory yet.  We are only initializing local data here.
 * (See also InitBufferPoolBackend)
 */
void
InitBufferPoolAccess(void)
{
	HASHCTL		hash_ctl;

	memset(&PrivateRefCountArray, 0, sizeof(PrivateRefCountArray));

	MemSet(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(int32);
	hash_ctl.entrysize = sizeof(PrivateRefCountEntry);

	PrivateRefCountHash = hash_create("PrivateRefCount", 100, &hash_ctl,
									  HASH_ELEM | HASH_BLOBS);
}

/*
 * InitBufferPoolBackend --- second-stage initialization of a new backend
 *
 * This is called after we have acquired a PGPROC and so can safely get
 * LWLocks.  We don't currently need to do anything at this stage ...
 * except register a shmem-exit callback.  AtProcExit_Buffers needs LWLock
 * access, and thereby has to be called at the corresponding phase of
 * backend shutdown.
 */
void
InitBufferPoolBackend(void)
{
	on_shmem_exit(AtProcExit_Buffers, 0);
}

/*
 * During backend exit, ensure that we released all shared-buffer locks and
 * assert that we have no remaining pins.
 */
static void
AtProcExit_Buffers(int code, Datum arg)
{
	AbortBufferIO();
	UnlockBuffers();

	CheckForBufferLeaks();

	/* localbuf.c needs a chance too */
	AtProcExit_LocalBuffers();
}

/*
 *		CheckForBufferLeaks - ensure this backend holds no buffer pins
 *
 *		As of PostgreSQL 8.0, buffer pins should get released by the
 *		ResourceOwner mechanism.  This routine is just a debugging
 *		cross-check that no pins remain.
 */
static void
CheckForBufferLeaks(void)
{
#ifdef USE_ASSERT_CHECKING
	int			RefCountErrors = 0;
	PrivateRefCountEntry *res;
	int			i;

	/* check the array */
	for (i = 0; i < REFCOUNT_ARRAY_ENTRIES; i++)
	{
		res = &PrivateRefCountArray[i];

		if (res->buffer != InvalidBuffer)
		{
			PrintBufferLeakWarning(res->buffer);
			RefCountErrors++;
		}
	}

	/* if necessary search the hash */
	if (PrivateRefCountOverflowed)
	{
		HASH_SEQ_STATUS hstat;

		hash_seq_init(&hstat, PrivateRefCountHash);
		while ((res = (PrivateRefCountEntry *) hash_seq_search(&hstat)) != NULL)
		{
			PrintBufferLeakWarning(res->buffer);
			RefCountErrors++;
		}

	}

	Assert(RefCountErrors == 0);
#endif
}

/*
 * Helper routine to issue warnings when a buffer is unexpectedly pinned
 */
void
PrintBufferLeakWarning(Buffer buffer)
{
	BufferDesc *buf;
	int32		loccount;
	char	   *path;
	BackendId	backend;
	uint32		buf_state;

	Assert(BufferIsValid(buffer));
	if (BufferIsLocal(buffer))
	{
		buf = GetLocalBufferDescriptor(-buffer - 1);
		loccount = LocalRefCount[-buffer - 1];
		backend = MyBackendId;
	}
	else
	{
		buf = GetBufferDescriptor(buffer - 1);
		loccount = GetPrivateRefCount(buffer);
		backend = InvalidBackendId;
	}

	/* theoretically we should lock the bufhdr here */
	path = relpathbackend(buf->tag.rnode, backend, buf->tag.forkNum);
	buf_state = pg_atomic_read_u32(&buf->state);
	elog(WARNING,
		 "buffer refcount leak: [%03d] "
		 "(rel=%s, blockNum=%u, flags=0x%x, refcount=%u %d)",
		 buffer, path,
		 buf->tag.blockNum, buf_state & BUF_FLAG_MASK,
		 BUF_STATE_GET_REFCOUNT(buf_state), loccount);
	pfree(path);
}

/*
 * CheckPointBuffers
 *
 * Flush all dirty blocks in buffer pool to disk at checkpoint time.
 *
 * Note: temporary relations do not participate in checkpoints, so they don't
 * need to be flushed.
 */
void
CheckPointBuffers(int flags)
{
	TRACE_POSTGRESQL_BUFFER_CHECKPOINT_START(flags);
	CheckpointStats.ckpt_write_t = GetCurrentTimestamp();
	BufferSync(flags);
	CheckpointStats.ckpt_sync_t = GetCurrentTimestamp();
	TRACE_POSTGRESQL_BUFFER_CHECKPOINT_SYNC_START();
	smgrsync();
	CheckpointStats.ckpt_sync_end_t = GetCurrentTimestamp();
	TRACE_POSTGRESQL_BUFFER_CHECKPOINT_DONE();
}


/*
 * Do whatever is needed to prepare for commit at the bufmgr and smgr levels
 */
void
BufmgrCommit(void)
{
	/* Nothing to do in bufmgr anymore... */
}

/*
 * BufferGetBlockNumber
 *		Returns the block number associated with a buffer.
 *
 * Note:
 *		Assumes that the buffer is valid and pinned, else the
 *		value may be obsolete immediately...
 */
BlockNumber
BufferGetBlockNumber(Buffer buffer)
{
	BufferDesc *bufHdr;

	Assert(BufferIsPinned(buffer));

	if (BufferIsLocal(buffer))
		bufHdr = GetLocalBufferDescriptor(-buffer - 1);
	else
		bufHdr = GetBufferDescriptor(buffer - 1);

	/* pinned, so OK to read tag without spinlock */
	return bufHdr->tag.blockNum;
}

/*
 * BufferGetTag
 *		Returns the relfilenode, fork number and block number associated with
 *		a buffer.
 */
void
BufferGetTag(Buffer buffer, RelFileNode *rnode, ForkNumber *forknum,
			 BlockNumber *blknum)
{
	BufferDesc *bufHdr;

	/* Do the same checks as BufferGetBlockNumber. */
	Assert(BufferIsPinned(buffer));

	if (BufferIsLocal(buffer))
		bufHdr = GetLocalBufferDescriptor(-buffer - 1);
	else
		bufHdr = GetBufferDescriptor(buffer - 1);

	/* pinned, so OK to read tag without spinlock */
	*rnode = bufHdr->tag.rnode;
	*forknum = bufHdr->tag.forkNum;
	*blknum = bufHdr->tag.blockNum;
}

/*
 * FlushBuffer
 *		Physically write out a shared buffer.
 *
 * NOTE: this actually just passes the buffer contents to the kernel; the
 * real write to disk won't happen until the kernel feels like it.  This
 * is okay from our point of view since we can redo the changes from WAL.
 * However, we will need to force the changes to disk via fsync before
 * we can checkpoint WAL.
 *
 * The caller must hold a pin on the buffer and have share-locked the
 * buffer contents.  (Note: a share-lock does not prevent updates of
 * hint bits in the buffer, so the page could change while the write
 * is in progress, but we assume that that will not invalidate the data
 * written.)
 *
 * If the caller has an smgr reference for the buffer's relation, pass it
 * as the second parameter.  If not, pass NULL.
 *
 * POLAR: If this buffer has copy buffer, that will be freed.
 */
static void
FlushBuffer(BufferDesc *buf, SMgrRelation reln, XLogRecPtr polar_oldest_apply_lsn, bool polar_skip_fullpage)
{
	XLogRecPtr	recptr;
	ErrorContextCallback errcallback;
	instr_time	io_start,
				io_time;
	Block		bufBlock;
	char	   *bufToWrite;
	uint32		buf_state;
	bool		polar_replica = polar_in_replica_mode();

	/*
	 * Acquire the buffer's io_in_progress lock.  If StartBufferIO returns
	 * false, then someone else flushed the buffer before we could, so we need
	 * not do anything.
	 */
	if (!StartBufferIO(buf, false))
		return;

	/* Setup error traceback support for ereport() */
	errcallback.callback = shared_buffer_write_error_callback;
	errcallback.arg = (void *) buf;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	/* Find smgr relation for buffer */
	/* POLAR: replica can not flush data */
	if (!polar_replica && reln == NULL)
		reln = smgropen(buf->tag.rnode, InvalidBackendId);

	TRACE_POSTGRESQL_BUFFER_FLUSH_START(buf->tag.forkNum,
										buf->tag.blockNum,
										reln->smgr_rnode.node.spcNode,
										reln->smgr_rnode.node.dbNode,
										reln->smgr_rnode.node.relNode);

	buf_state = LockBufHdr(buf);

	/*
	 * Run PageGetLSN while holding header lock, since we don't have the
	 * buffer locked exclusively in all cases.
	 */
	recptr = BufferGetLSN(buf);

	/* To check if block content changes while flushing. - vadim 01/17/97 */
	buf_state &= ~BM_JUST_DIRTIED;
	UnlockBufHdr(buf, buf_state);

	if (unlikely(polar_enable_debug))
	{
		elog(LOG,
			 "Flush buffer %d page ([%u,%u,%u], %u), oldest lsn %X/%X, latest lsn %X/%X, oldest apply lsn %X/%X ",
			 BufferDescriptorGetBuffer(buf),
			 buf->tag.rnode.spcNode,
			 buf->tag.rnode.dbNode,
			 buf->tag.rnode.relNode,
			 buf->tag.blockNum,
			 (uint32) (buf->oldest_lsn >> 32),
			 (uint32) buf->oldest_lsn,
			 (uint32) (recptr >> 32),
			 (uint32) recptr,
			 (uint32) (polar_oldest_apply_lsn >> 32),
			 (uint32) polar_oldest_apply_lsn);
	}

	/*
	 * Force XLOG flush up to buffer's LSN.  This implements the basic WAL
	 * rule that log updates must hit disk before any of the data-file changes
	 * they describe do.
	 *
	 * However, this rule does not apply to unlogged relations, which will be
	 * lost after a crash anyway.  Most unlogged relation pages do not bear
	 * LSNs since we never emit WAL records for them, and therefore flushing
	 * up through the buffer LSN would be useless, but harmless.  However,
	 * GiST indexes use LSNs internally to track page-splits, and therefore
	 * unlogged GiST pages bear "fake" LSNs generated by
	 * GetFakeLSNForUnloggedRel.  It is unlikely but possible that the fake
	 * LSN counter could advance past the WAL insertion point; and if it did
	 * happen, attempting to flush WAL through that location would fail, with
	 * disastrous system-wide consequences.  To make sure that can't happen,
	 * skip the flush if the buffer isn't permanent.
	 */
	if (buf_state & BM_PERMANENT)
	{
		XLogFlush(recptr);
		polar_flush_buf_flog_rec(buf, flog_instance, false);
	}

	/*
	 * Now it's safe to write buffer to disk. Note that no one else should
	 * have been able to write it while we were busy with log flushing because
	 * we have the io_in_progress lock.
	 */
	bufBlock = BufHdrGetBlock(buf);

	bufToWrite = PageEncryptCopy((Page) bufBlock, buf->tag.forkNum,
								 buf->tag.blockNum);

	/*
	 * Update page checksum if desired.  Since we have only shared lock on the
	 * buffer, other processes might be updating hint bits in it, so we must
	 * copy the page to private storage if we do checksumming.
	 */
	bufToWrite = PageSetChecksumCopy((Page) bufToWrite, buf->tag.blockNum);

	if (track_io_timing)
		INSTR_TIME_SET_CURRENT(io_start);

	/* POLAR: replica never writes to filesystem */
	if (!polar_replica)
	{
		/* POLAR: write fullpage wal when flush a future page */
		if (POLAR_LOGINDEX_ENABLE_FULLPAGE() &&
			!polar_skip_fullpage && !PageIsNew(bufBlock) &&
			!XLogRecPtrIsInvalid(polar_oldest_apply_lsn) &&
			BufferGetLSN(buf) > polar_oldest_apply_lsn &&
			buf->tag.forkNum == MAIN_FORKNUM)
		{
			polar_log_fullpage_snapshot_image(polar_logindex_redo_instance->fullpage_ctl,
					BufferDescriptorGetBuffer(buf), polar_oldest_apply_lsn);
		}

		/*
		 * bufToWrite is either the shared buffer or a copy, as appropriate.
		 */
		smgrwrite(reln,
				  buf->tag.forkNum,
				  buf->tag.blockNum,
				  bufToWrite,
				  false);
	}
	/* POLAR end */

	if (track_io_timing)
	{
		INSTR_TIME_SET_CURRENT(io_time);
		INSTR_TIME_SUBTRACT(io_time, io_start);
		pgstat_count_buffer_write_time(INSTR_TIME_GET_MICROSEC(io_time));
		INSTR_TIME_ADD(pgBufferUsage.blk_write_time, io_time);
	}

	pgBufferUsage.shared_blks_written++;

	/*
	 * The oldest_lsn should be cleared once it is flushed. It will be set
	 * again when it is dirty.
	 */
	if (polar_enable_shared_storage_mode)
	{
		/* Free copy buffer if exists */
		polar_free_copy_buffer(buf);
		polar_reset_buffer_oldest_lsn(buf);
	}

	/*
	 * Mark the buffer as clean (unless BM_JUST_DIRTIED has become set) and
	 * end the io_in_progress state.
	 */
	TerminateBufferIO(buf, true, 0);

	TRACE_POSTGRESQL_BUFFER_FLUSH_DONE(buf->tag.forkNum,
									   buf->tag.blockNum,
									   reln->smgr_rnode.node.spcNode,
									   reln->smgr_rnode.node.dbNode,
									   reln->smgr_rnode.node.relNode);

	/* Pop the error context stack */
	error_context_stack = errcallback.previous;
}

/*
 * RelationGetNumberOfBlocksInFork
 *		Determines the current number of pages in the specified relation fork.
 */
BlockNumber
RelationGetNumberOfBlocksInFork(Relation relation, ForkNumber forkNum)
{
	/* Open it at the smgr level if not already done */
	RelationOpenSmgr(relation);

	return smgrnblocks(relation->rd_smgr, forkNum);
}

/*
 * POLAR: A same function of RelationGetNumberOfBlocksInFork. But use polar_smgrnblocks_cache
 * to return the real relation blocks.
 */
BlockNumber
polar_relation_get_number_of_cache_blocks_in_fork(Relation relation, ForkNumber forkNum)
{
	/* Open it at the smgr level if not already done */
	RelationOpenSmgr(relation);

	return polar_smgrnblocks_cache(relation->rd_smgr, forkNum);
}

/*
 * BufferIsPermanent
 *		Determines whether a buffer will potentially still be around after
 *		a crash.  Caller must hold a buffer pin.
 */
bool
BufferIsPermanent(Buffer buffer)
{
	BufferDesc *bufHdr;

	/* Local buffers are used only for temp relations. */
	if (BufferIsLocal(buffer))
		return false;

	/* Make sure we've got a real buffer, and that we hold a pin on it. */
	Assert(BufferIsValid(buffer));
	Assert(BufferIsPinned(buffer));

	/*
	 * BM_PERMANENT can't be changed while we hold a pin on the buffer, so we
	 * need not bother with the buffer header spinlock.  Even if someone else
	 * changes the buffer header state while we're doing this, the state is
	 * changed atomically, so we'll read the old value or the new value, but
	 * not random garbage.
	 */
	bufHdr = GetBufferDescriptor(buffer - 1);
	return (pg_atomic_read_u32(&bufHdr->state) & BM_PERMANENT) != 0;
}

/*
 * BufferGetLSNAtomic
 *		Retrieves the LSN of the buffer atomically using a buffer header lock.
 *		This is necessary for some callers who may not have an exclusive lock
 *		on the buffer.
 */
XLogRecPtr
BufferGetLSNAtomic(Buffer buffer)
{
	BufferDesc *bufHdr = GetBufferDescriptor(buffer - 1);
	char	   *page = BufferGetPage(buffer);
	XLogRecPtr	lsn;
	uint32		buf_state;

	/*
	 * If we don't need locking for correctness, fastpath out.
	 */
	if (!XLogHintBitIsNeeded() || BufferIsLocal(buffer))
		return PageGetLSN(page);

	/* Make sure we've got a real buffer, and that we hold a pin on it. */
	Assert(BufferIsValid(buffer));
	Assert(BufferIsPinned(buffer));

	buf_state = LockBufHdr(bufHdr);
	lsn = PageGetLSN(page);
	UnlockBufHdr(bufHdr, buf_state);

	return lsn;
}

/* ---------------------------------------------------------------------
 *		DropRelFileNodeBuffers
 *
 *		This function removes from the buffer pool all the pages of the
 *		specified relation fork that have block numbers >= firstDelBlock.
 *		(In particular, with firstDelBlock = 0, all pages are removed.)
 *		Dirty pages are simply dropped, without bothering to write them
 *		out first.  Therefore, this is NOT rollback-able, and so should be
 *		used only with extreme caution!
 *
 *		Currently, this is called only from smgr.c when the underlying file
 *		is about to be deleted or truncated (firstDelBlock is needed for
 *		the truncation case).  The data in the affected pages would therefore
 *		be deleted momentarily anyway, and there is no point in writing it.
 *		It is the responsibility of higher-level code to ensure that the
 *		deletion or truncation does not lose any data that could be needed
 *		later.  It is also the responsibility of higher-level code to ensure
 *		that no other process could be trying to load more pages of the
 *		relation into buffers.
 *
 *		XXX currently it sequentially searches the buffer pool, should be
 *		changed to more clever ways of searching.  However, this routine
 *		is used only in code paths that aren't very performance-critical,
 *		and we shouldn't slow down the hot paths to make it faster ...
 * --------------------------------------------------------------------
 */
void
DropRelFileNodeBuffers(RelFileNodeBackend rnode, ForkNumber forkNum,
					   BlockNumber firstDelBlock)
{
	int			i;

	/* If it's a local relation, it's localbuf.c's problem. */
	if (RelFileNodeBackendIsTemp(rnode))
	{
		if (rnode.backend == MyBackendId)
			DropRelFileNodeLocalBuffers(rnode.node, forkNum, firstDelBlock);
		return;
	}

	for (i = 0; i < NBuffers; i++)
	{
		BufferDesc *bufHdr = GetBufferDescriptor(i);
		uint32		buf_state;

		/*
		 * We can make this a tad faster by prechecking the buffer tag before
		 * we attempt to lock the buffer; this saves a lot of lock
		 * acquisitions in typical cases.  It should be safe because the
		 * caller must have AccessExclusiveLock on the relation, or some other
		 * reason to be certain that no one is loading new pages of the rel
		 * into the buffer pool.  (Otherwise we might well miss such pages
		 * entirely.)  Therefore, while the tag might be changing while we
		 * look at it, it can't be changing *to* a value we care about, only
		 * *away* from such a value.  So false negatives are impossible, and
		 * false positives are safe because we'll recheck after getting the
		 * buffer lock.
		 *
		 * We could check forkNum and blockNum as well as the rnode, but the
		 * incremental win from doing so seems small.
		 */
		if (!RelFileNodeEquals(bufHdr->tag.rnode, rnode.node))
			continue;

		buf_state = LockBufHdr(bufHdr);
		if (RelFileNodeEquals(bufHdr->tag.rnode, rnode.node) &&
			bufHdr->tag.forkNum == forkNum &&
			bufHdr->tag.blockNum >= firstDelBlock)
			InvalidateBuffer(bufHdr);	/* releases spinlock */
		else
			UnlockBufHdr(bufHdr, buf_state);
	}
}

/* ---------------------------------------------------------------------
 *		DropRelFileNodesAllBuffers
 *
 *		This function removes from the buffer pool all the pages of all
 *		forks of the specified relations.  It's equivalent to calling
 *		DropRelFileNodeBuffers once per fork per relation with
 *		firstDelBlock = 0.
 * --------------------------------------------------------------------
 */
void
DropRelFileNodesAllBuffers(RelFileNodeBackend *rnodes, int nnodes)
{
	int			i,
				n = 0;
	RelFileNode *nodes;
	bool		use_bsearch;

	if (nnodes == 0)
		return;

	nodes = palloc(sizeof(RelFileNode) * nnodes);	/* non-local relations */

	/* If it's a local relation, it's localbuf.c's problem. */
	for (i = 0; i < nnodes; i++)
	{
		if (RelFileNodeBackendIsTemp(rnodes[i]))
		{
			if (rnodes[i].backend == MyBackendId)
				DropRelFileNodeAllLocalBuffers(rnodes[i].node);
		}
		else
			nodes[n++] = rnodes[i].node;
	}

	/*
	 * If there are no non-local relations, then we're done. Release the
	 * memory and return.
	 */
	if (n == 0)
	{
		pfree(nodes);
		return;
	}

	/*
	 * For low number of relations to drop just use a simple walk through, to
	 * save the bsearch overhead. The threshold to use is rather a guess than
	 * an exactly determined value, as it depends on many factors (CPU and RAM
	 * speeds, amount of shared buffers etc.).
	 */
	use_bsearch = n > DROP_RELS_BSEARCH_THRESHOLD;

	/* sort the list of rnodes if necessary */
	if (use_bsearch)
		pg_qsort(nodes, n, sizeof(RelFileNode), rnode_comparator);

	for (i = 0; i < NBuffers; i++)
	{
		RelFileNode *rnode = NULL;
		BufferDesc *bufHdr = GetBufferDescriptor(i);
		uint32		buf_state;

		/*
		 * As in DropRelFileNodeBuffers, an unlocked precheck should be safe
		 * and saves some cycles.
		 */

		if (!use_bsearch)
		{
			int			j;

			for (j = 0; j < n; j++)
			{
				if (RelFileNodeEquals(bufHdr->tag.rnode, nodes[j]))
				{
					rnode = &nodes[j];
					break;
				}
			}
		}
		else
		{
			rnode = bsearch((const void *) &(bufHdr->tag.rnode),
							nodes, n, sizeof(RelFileNode),
							rnode_comparator);
		}

		/* buffer doesn't belong to any of the given relfilenodes; skip it */
		if (rnode == NULL)
			continue;

		buf_state = LockBufHdr(bufHdr);
		if (RelFileNodeEquals(bufHdr->tag.rnode, (*rnode)))
			InvalidateBuffer(bufHdr);	/* releases spinlock */
		else
			UnlockBufHdr(bufHdr, buf_state);
	}

	pfree(nodes);
}

/* ---------------------------------------------------------------------
 *		DropDatabaseBuffers
 *
 *		This function removes all the buffers in the buffer cache for a
 *		particular database.  Dirty pages are simply dropped, without
 *		bothering to write them out first.  This is used when we destroy a
 *		database, to avoid trying to flush data to disk when the directory
 *		tree no longer exists.  Implementation is pretty similar to
 *		DropRelFileNodeBuffers() which is for destroying just one relation.
 * --------------------------------------------------------------------
 */
void
DropDatabaseBuffers(Oid dbid)
{
	int			i;

	/*
	 * We needn't consider local buffers, since by assumption the target
	 * database isn't our own.
	 */

	for (i = 0; i < NBuffers; i++)
	{
		BufferDesc *bufHdr = GetBufferDescriptor(i);
		uint32		buf_state;

		/*
		 * As in DropRelFileNodeBuffers, an unlocked precheck should be safe
		 * and saves some cycles.
		 */
		if (bufHdr->tag.rnode.dbNode != dbid)
			continue;

		buf_state = LockBufHdr(bufHdr);
		if (bufHdr->tag.rnode.dbNode == dbid)
			InvalidateBuffer(bufHdr);	/* releases spinlock */
		else
			UnlockBufHdr(bufHdr, buf_state);
	}
}

/* -----------------------------------------------------------------
 *		PrintBufferDescs
 *
 *		this function prints all the buffer descriptors, for debugging
 *		use only.
 * -----------------------------------------------------------------
 */
#ifdef NOT_USED
void
PrintBufferDescs(void)
{
	int			i;

	for (i = 0; i < NBuffers; ++i)
	{
		BufferDesc *buf = GetBufferDescriptor(i);
		Buffer		b = BufferDescriptorGetBuffer(buf);

		/* theoretically we should lock the bufhdr here */
		elog(LOG,
			 "[%02d] (freeNext=%d, rel=%s, "
			 "blockNum=%u, flags=0x%x, refcount=%u %d)",
			 i, buf->freeNext,
			 relpathbackend(buf->tag.rnode, InvalidBackendId, buf->tag.forkNum),
			 buf->tag.blockNum, buf->flags,
			 buf->refcount, GetPrivateRefCount(b));
	}
}
#endif

#ifdef NOT_USED
void
PrintPinnedBufs(void)
{
	int			i;

	for (i = 0; i < NBuffers; ++i)
	{
		BufferDesc *buf = GetBufferDescriptor(i);
		Buffer		b = BufferDescriptorGetBuffer(buf);

		if (GetPrivateRefCount(b) > 0)
		{
			/* theoretically we should lock the bufhdr here */
			elog(LOG,
				 "[%02d] (freeNext=%d, rel=%s, "
				 "blockNum=%u, flags=0x%x, refcount=%u %d)",
				 i, buf->freeNext,
				 relpathperm(buf->tag.rnode, buf->tag.forkNum),
				 buf->tag.blockNum, buf->flags,
				 buf->refcount, GetPrivateRefCount(b));
		}
	}
}
#endif

/* ---------------------------------------------------------------------
 *		FlushRelationBuffers
 *
 *		This function writes all dirty pages of a relation out to disk
 *		(or more accurately, out to kernel disk buffers), ensuring that the
 *		kernel has an up-to-date view of the relation.
 *
 *		Generally, the caller should be holding AccessExclusiveLock on the
 *		target relation to ensure that no other backend is busy dirtying
 *		more blocks of the relation; the effects can't be expected to last
 *		after the lock is released.
 *
 *		XXX currently it sequentially searches the buffer pool, should be
 *		changed to more clever ways of searching.  This routine is not
 *		used in any performance-critical code paths, so it's not worth
 *		adding additional overhead to normal paths to make it go faster;
 *		but see also DropRelFileNodeBuffers.
 * --------------------------------------------------------------------
 */
void
FlushRelationBuffers(Relation rel)
{
	int			i;
	BufferDesc *bufHdr;

	/* POLAR */
	XLogRecPtr	oldest_apply_lsn = polar_get_oldest_applied_lsn();

	/* Open rel at the smgr level if not already done */
	RelationOpenSmgr(rel);

	if (RelationUsesLocalBuffers(rel))
	{
		for (i = 0; i < NLocBuffer; i++)
		{
			uint32		buf_state;

			bufHdr = GetLocalBufferDescriptor(i);
			if (RelFileNodeEquals(bufHdr->tag.rnode, rel->rd_node) &&
				((buf_state = pg_atomic_read_u32(&bufHdr->state)) &
				 (BM_VALID | BM_DIRTY)) == (BM_VALID | BM_DIRTY))
			{
				ErrorContextCallback errcallback;
				Page		localpage;

				localpage = (char *) LocalBufHdrGetBlock(bufHdr);

				/* Setup error traceback support for ereport() */
				errcallback.callback = local_buffer_write_error_callback;
				errcallback.arg = (void *) bufHdr;
				errcallback.previous = error_context_stack;
				error_context_stack = &errcallback;

				PageEncryptInplace(localpage, bufHdr->tag.forkNum, bufHdr->tag.blockNum);
				PageSetChecksumInplace(localpage, bufHdr->tag.blockNum);

				smgrwrite(rel->rd_smgr,
						  bufHdr->tag.forkNum,
						  bufHdr->tag.blockNum,
						  localpage,
						  false);

				buf_state &= ~(BM_DIRTY | BM_JUST_DIRTIED);
				pg_atomic_unlocked_write_u32(&bufHdr->state, buf_state);

				/* Pop the error context stack */
				error_context_stack = errcallback.previous;
			}
		}

		return;
	}

	/* Make sure we can handle the pin inside the loop */
	ResourceOwnerEnlargeBuffers(CurrentResourceOwner);

	for (i = 0; i < NBuffers; i++)
	{
		uint32		buf_state;

		bufHdr = GetBufferDescriptor(i);

		/*
		 * As in DropRelFileNodeBuffers, an unlocked precheck should be safe
		 * and saves some cycles.
		 */
		if (!RelFileNodeEquals(bufHdr->tag.rnode, rel->rd_node))
			continue;

		ReservePrivateRefCountEntry();

		buf_state = LockBufHdr(bufHdr);
		if (RelFileNodeEquals(bufHdr->tag.rnode, rel->rd_node) &&
			(buf_state & (BM_VALID | BM_DIRTY)) == (BM_VALID | BM_DIRTY))
		{
			PinBuffer_Locked(bufHdr);
			LWLockAcquire(BufferDescriptorGetContentLock(bufHdr), LW_SHARED);

			/*
			 * POLAR: it's no need to do buffer flush control for
			 * vacuum full/rewrite table
			 */
			FlushBuffer(bufHdr, rel->rd_smgr, oldest_apply_lsn, true);
			LWLockRelease(BufferDescriptorGetContentLock(bufHdr));
			UnpinBuffer(bufHdr, true);
		}
		else
			UnlockBufHdr(bufHdr, buf_state);
	}
}

/* ---------------------------------------------------------------------
 *		FlushDatabaseBuffers
 *
 *		This function writes all dirty pages of a database out to disk
 *		(or more accurately, out to kernel disk buffers), ensuring that the
 *		kernel has an up-to-date view of the database.
 *
 *		Generally, the caller should be holding an appropriate lock to ensure
 *		no other backend is active in the target database; otherwise more
 *		pages could get dirtied.
 *
 *		Note we don't worry about flushing any pages of temporary relations.
 *		It's assumed these wouldn't be interesting.
 * --------------------------------------------------------------------
 */
void
FlushDatabaseBuffers(Oid dbid)
{
	int			i;
	BufferDesc *bufHdr;

	/* POLAR */
	XLogRecPtr	oldest_apply_lsn = polar_get_oldest_applied_lsn();

	/* Make sure we can handle the pin inside the loop */
	ResourceOwnerEnlargeBuffers(CurrentResourceOwner);

	for (i = 0; i < NBuffers; i++)
	{
		uint32		buf_state;

		bufHdr = GetBufferDescriptor(i);

		/*
		 * As in DropRelFileNodeBuffers, an unlocked precheck should be safe
		 * and saves some cycles.
		 */
		if (bufHdr->tag.rnode.dbNode != dbid)
			continue;

		ReservePrivateRefCountEntry();

		buf_state = LockBufHdr(bufHdr);
		if (bufHdr->tag.rnode.dbNode == dbid &&
			(buf_state & (BM_VALID | BM_DIRTY)) == (BM_VALID | BM_DIRTY))
		{
			PinBuffer_Locked(bufHdr);
			LWLockAcquire(BufferDescriptorGetContentLock(bufHdr), LW_SHARED);

			/*
			 * POLAR: it's no need to do buffer flush control for
			 * vacuum full/rewrite table
			 */
			FlushBuffer(bufHdr, NULL, oldest_apply_lsn, true);
			LWLockRelease(BufferDescriptorGetContentLock(bufHdr));
			UnpinBuffer(bufHdr, true);
		}
		else
			UnlockBufHdr(bufHdr, buf_state);
	}
}

/*
 * Flush a previously, shared or exclusively, locked and pinned buffer to the
 * OS.
 */
void
FlushOneBuffer(Buffer buffer)
{
	BufferDesc *bufHdr;

	/* currently not needed, but no fundamental reason not to support */
	Assert(!BufferIsLocal(buffer));

	Assert(BufferIsPinned(buffer));

	bufHdr = GetBufferDescriptor(buffer - 1);

	Assert(LWLockHeldByMe(BufferDescriptorGetContentLock(bufHdr)));

	/* POLAR: FlushOneBuffer currently only used for hash xlog, do not care about it */
	FlushBuffer(bufHdr, NULL, InvalidXLogRecPtr, false);
}

/*
 * ReleaseBuffer -- release the pin on a buffer
 */
void
ReleaseBuffer(Buffer buffer)
{
	if (!BufferIsValid(buffer))
		elog(ERROR, "bad buffer ID: %d", buffer);

	if (BufferIsLocal(buffer))
	{
		ResourceOwnerForgetBuffer(CurrentResourceOwner, buffer);

		Assert(LocalRefCount[-buffer - 1] > 0);
		LocalRefCount[-buffer - 1]--;
		return;
	}

	UnpinBuffer(GetBufferDescriptor(buffer - 1), true);
}

/*
 * UnlockReleaseBuffer -- release the content lock and pin on a buffer
 *
 * This is just a shorthand for a common combination.
 */
void
UnlockReleaseBuffer(Buffer buffer)
{
	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	ReleaseBuffer(buffer);
}

/*
 * IncrBufferRefCount
 *		Increment the pin count on a buffer that we have *already* pinned
 *		at least once.
 *
 *		This function cannot be used on a buffer we do not have pinned,
 *		because it doesn't change the shared buffer state.
 */
void
IncrBufferRefCount(Buffer buffer)
{
	Assert(BufferIsPinned(buffer));
	ResourceOwnerEnlargeBuffers(CurrentResourceOwner);
	if (BufferIsLocal(buffer))
		LocalRefCount[-buffer - 1]++;
	else
	{
		PrivateRefCountEntry *ref;

		ref = GetPrivateRefCountEntry(buffer, true);
		Assert(ref != NULL);
		ref->refcount++;
	}
	ResourceOwnerRememberBuffer(CurrentResourceOwner, buffer);
}

/*
 * MarkBufferDirtyHint
 *
 *	Mark a buffer dirty for non-critical changes.
 *
 * This is essentially the same as MarkBufferDirty, except:
 *
 * 1. The caller does not write WAL; so if checksums are enabled, we may need
 *	  to write an XLOG_FPI WAL record to protect against torn pages.
 * 2. The caller might have only share-lock instead of exclusive-lock on the
 *	  buffer's content lock.
 * 3. This function does not guarantee that the buffer is always marked dirty
 *	  (due to a race condition), so it cannot be used for important changes.
 */
void
MarkBufferDirtyHint(Buffer buffer, bool buffer_std)
{
	BufferDesc *bufHdr;
	Page		page = BufferGetPage(buffer);

	if (!BufferIsValid(buffer))
		elog(ERROR, "bad buffer ID: %d", buffer);

	if (BufferIsLocal(buffer))
	{
		MarkLocalBufferDirty(buffer);
		return;
	}

	/* POLAR: It does not make sense to set buffer dirty in ro node */
	if (polar_in_replica_mode())
		return;

	bufHdr = GetBufferDescriptor(buffer - 1);

	Assert(GetPrivateRefCount(buffer) > 0);
	/* here, either share or exclusive lock is OK */
	Assert(LWLockHeldByMe(BufferDescriptorGetContentLock(bufHdr)));

	/*
	 * This routine might get called many times on the same page, if we are
	 * making the first scan after commit of an xact that added/deleted many
	 * tuples. So, be as quick as we can if the buffer is already dirty.  We
	 * do this by not acquiring spinlock if it looks like the status bits are
	 * already set.  Since we make this test unlocked, there's a chance we
	 * might fail to notice that the flags have just been cleared, and failed
	 * to reset them, due to memory-ordering issues.  But since this function
	 * is only intended to be used in cases where failing to write out the
	 * data would be harmless anyway, it doesn't really matter.
	 */
	if ((pg_atomic_read_u32(&bufHdr->state) & (BM_DIRTY | BM_JUST_DIRTIED)) !=
		(BM_DIRTY | BM_JUST_DIRTIED))
	{
		XLogRecPtr	lsn = InvalidXLogRecPtr;
		bool		dirtied = false;
		bool		delayChkpt = false;
		uint32		buf_state;

		/*
		 * If we need to protect hint bit updates from torn writes, WAL-log a
		 * full page image of the page. This full page image is only necessary
		 * if the hint bit update is the first change to the page since the
		 * last checkpoint.
		 *
		 * We don't check full_page_writes here because that logic is included
		 * when we call XLogInsert() since the value changes dynamically.
		 */
		if (XLogHintBitIsNeeded() &&
			(pg_atomic_read_u32(&bufHdr->state) & BM_PERMANENT)
			&& polar_has_partial_write)
		{
			/*
			 * If we're in recovery we cannot dirty a page because of a hint.
			 * We can set the hint, just not dirty the page as a result so the
			 * hint is lost when we evict the page or shutdown.
			 *
			 * See src/backend/storage/page/README for longer discussion.
			 */
			if (RecoveryInProgress())
				return;

			/*
			 * If the block is already dirty because we either made a change
			 * or set a hint already, then we don't need to write a full page
			 * image.  Note that aggressive cleaning of blocks dirtied by hint
			 * bit setting would increase the call rate. Bulk setting of hint
			 * bits would reduce the call rate...
			 *
			 * We must issue the WAL record before we mark the buffer dirty.
			 * Otherwise we might write the page before we write the WAL. That
			 * causes a race condition, since a checkpoint might occur between
			 * writing the WAL record and marking the buffer dirty. We solve
			 * that with a kluge, but one that is already in use during
			 * transaction commit to prevent race conditions. Basically, we
			 * simply prevent the checkpoint WAL record from being written
			 * until we have marked the buffer dirty. We don't start the
			 * checkpoint flush until we have marked dirty, so our checkpoint
			 * must flush the change to disk successfully or the checkpoint
			 * never gets written, so crash recovery will fix.
			 *
			 * It's possible we may enter here without an xid, so it is
			 * essential that CreateCheckpoint waits for virtual transactions
			 * rather than full transactionids.
			 */
			MyPgXact->delayChkpt = delayChkpt = true;
			lsn = XLogSaveBufferForHint(buffer, buffer_std);
		}

		buf_state = LockBufHdr(bufHdr);

		Assert(BUF_STATE_GET_REFCOUNT(buf_state) > 0);

		if (!(buf_state & BM_DIRTY))
		{
			dirtied = true;		/* Means "will be dirtied by this action" */

			/*
			 * Set the page LSN if we wrote a backup block. We aren't supposed
			 * to set this when only holding a share lock but as long as we
			 * serialise it somehow we're OK. We choose to set LSN while
			 * holding the buffer header lock, which causes any reader of an
			 * LSN who holds only a share lock to also obtain a buffer header
			 * lock before using PageGetLSN(), which is enforced in
			 * BufferGetLSNAtomic().
			 *
			 * If checksums are enabled, you might think we should reset the
			 * checksum here. That will happen when the page is written
			 * sometime later in this checkpoint cycle.
			 */
			if (!XLogRecPtrIsInvalid(lsn))
				PageSetLSN(page, lsn);
		}

		buf_state |= BM_DIRTY | BM_JUST_DIRTIED;
		UnlockBufHdr(bufHdr, buf_state);

		if (delayChkpt)
			MyPgXact->delayChkpt = false;

		if (dirtied)
		{
			VacuumPageDirty++;
			pgBufferUsage.shared_blks_dirtied++;
			if (VacuumCostActive)
				VacuumCostBalance += VacuumCostPageDirty;
		}
	}
}

/*
 * Release buffer content locks for shared buffers.
 *
 * Used to clean up after errors.
 *
 * Currently, we can expect that lwlock.c's LWLockReleaseAll() took care
 * of releasing buffer content locks per se; the only thing we need to deal
 * with here is clearing any PIN_COUNT request that was in progress.
 */
void
UnlockBuffers(void)
{
	BufferDesc *buf = PinCountWaitBuf;

	if (buf)
	{
		uint32		buf_state;

		buf_state = LockBufHdr(buf);

		/*
		 * Don't complain if flag bit not set; it could have been reset but we
		 * got a cancel/die interrupt before getting the signal.
		 */
		if ((buf_state & BM_PIN_COUNT_WAITER) != 0 &&
			buf->wait_backend_pid == MyProcPid)
			buf_state &= ~BM_PIN_COUNT_WAITER;

		UnlockBufHdr(buf, buf_state);

		PinCountWaitBuf = NULL;
	}
}

/*
 * Acquire or release the content_lock for the buffer.
 */
void
LockBuffer(Buffer buffer, int mode)
{
	/* POLAR: Call extended version when page outdate enabled */
	polar_lock_buffer_ext(buffer, mode, POLAR_LOGINDEX_ENABLE_PAGE_OUTDATE());
	/* POLAR end */
}

/*
 * Acquire the content_lock for the buffer, but only if we don't have to wait.
 *
 * This assumes the caller wants BUFFER_LOCK_EXCLUSIVE mode.
 */
bool
ConditionalLockBuffer(Buffer buffer)
{
	/* POLAR: Call extended version when page outdate enabled */
	return polar_conditional_lock_buffer_ext(buffer, POLAR_LOGINDEX_ENABLE_PAGE_OUTDATE());
	/* POLAR end */
}


/*
 * LockBufferForCleanup - lock a buffer in preparation for deleting items
 *
 * Items may be deleted from a disk page only when the caller (a) holds an
 * exclusive lock on the buffer and (b) has observed that no other backend
 * holds a pin on the buffer.  If there is a pin, then the other backend
 * might have a pointer into the buffer (for example, a heapscan reference
 * to an item --- see README for more details).  It's OK if a pin is added
 * after the cleanup starts, however; the newly-arrived backend will be
 * unable to look at the page until we release the exclusive lock.
 *
 * To implement this protocol, a would-be deleter must pin the buffer and
 * then call LockBufferForCleanup().  LockBufferForCleanup() is similar to
 * LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE), except that it loops until
 * it has successfully observed pin count = 1.
 */
void
LockBufferForCleanup(Buffer buffer)
{
	/* POLAR: Call extended version when page outdate enabled */
	polar_lock_buffer_for_cleanup_ext(buffer, POLAR_LOGINDEX_ENABLE_PAGE_OUTDATE());
	/* POLAR end */
}

void
polar_lock_buffer_for_cleanup_ext(Buffer buffer, bool fresh_check)
{
	BufferDesc *bufHdr;

	Assert(BufferIsValid(buffer));
	Assert(PinCountWaitBuf == NULL);

	if (BufferIsLocal(buffer))
	{
		/* There should be exactly one pin */
		if (LocalRefCount[-buffer - 1] != 1)
			elog(ERROR, "incorrect local pin count: %d",
				 LocalRefCount[-buffer - 1]);
		/* Nobody else to wait for */
		return;
	}

	/* There should be exactly one local pin */
	if (GetPrivateRefCount(buffer) != 1)
		elog(ERROR, "incorrect local pin count: %d",
			 GetPrivateRefCount(buffer));

	bufHdr = GetBufferDescriptor(buffer - 1);

	for (;;)
	{
		uint32		buf_state;

		/* Try to acquire lock */
		polar_lock_buffer_ext(buffer, BUFFER_LOCK_EXCLUSIVE, false);
		buf_state = LockBufHdr(bufHdr);

		Assert(BUF_STATE_GET_REFCOUNT(buf_state) > 0);
		if (BUF_STATE_GET_REFCOUNT(buf_state) == 1)
		{
			/* Successfully acquired exclusive lock with pincount 1 */
			if (fresh_check)
				polar_logindex_lock_apply_buffer(polar_logindex_redo_instance, &buffer);

			UnlockBufHdr(bufHdr, buf_state);
			return;
		}
		/* Failed, so mark myself as waiting for pincount 1 */
		if (buf_state & BM_PIN_COUNT_WAITER)
		{
			UnlockBufHdr(bufHdr, buf_state);
			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
			elog(ERROR, "multiple backends attempting to wait for pincount 1");
		}
		bufHdr->wait_backend_pid = MyProcPid;
		PinCountWaitBuf = bufHdr;
		buf_state |= BM_PIN_COUNT_WAITER;
		UnlockBufHdr(bufHdr, buf_state);
		polar_lock_buffer_ext(buffer, BUFFER_LOCK_UNLOCK, fresh_check);

		/* Wait to be signaled by UnpinBuffer() */
		if (InHotStandby)
		{
			/* Publish the bufid that Startup process waits on */
			SetStartupBufferPinWaitBufId(buffer - 1);
			/* Set alarm and then wait to be signaled by UnpinBuffer() */
			ResolveRecoveryConflictWithBufferPin();
			/* Reset the published bufid */
			SetStartupBufferPinWaitBufId(-1);
		}
		else
			ProcWaitForSignal(PG_WAIT_BUFFER_PIN);

		/*
		 * Remove flag marking us as waiter. Normally this will not be set
		 * anymore, but ProcWaitForSignal() can return for other signals as
		 * well.  We take care to only reset the flag if we're the waiter, as
		 * theoretically another backend could have started waiting. That's
		 * impossible with the current usages due to table level locking, but
		 * better be safe.
		 */
		buf_state = LockBufHdr(bufHdr);
		if ((buf_state & BM_PIN_COUNT_WAITER) != 0 &&
			bufHdr->wait_backend_pid == MyProcPid)
			buf_state &= ~BM_PIN_COUNT_WAITER;
		UnlockBufHdr(bufHdr, buf_state);

		PinCountWaitBuf = NULL;
		/* Loop back and try again */
	}
}

/*
 * Check called from RecoveryConflictInterrupt handler when Startup
 * process requests cancellation of all pin holders that are blocking it.
 */
bool
HoldingBufferPinThatDelaysRecovery(void)
{
	int			bufid = GetStartupBufferPinWaitBufId();

	/*
	 * If we get woken slowly then it's possible that the Startup process was
	 * already woken by other backends before we got here. Also possible that
	 * we get here by multiple interrupts or interrupts at inappropriate
	 * times, so make sure we do nothing if the bufid is not set.
	 */
	if (bufid < 0)
		return false;

	if (GetPrivateRefCount(bufid + 1) > 0)
		return true;

	return false;
}

/*
 * ConditionalLockBufferForCleanup - as above, but don't wait to get the lock
 *
 * We won't loop, but just check once to see if the pin count is OK.  If
 * not, return false with no lock held.
 */
bool
ConditionalLockBufferForCleanup(Buffer buffer)
{
	BufferDesc *bufHdr;
	uint32		buf_state,
				refcount;

	Assert(BufferIsValid(buffer));

	if (BufferIsLocal(buffer))
	{
		refcount = LocalRefCount[-buffer - 1];
		/* There should be exactly one pin */
		Assert(refcount > 0);
		if (refcount != 1)
			return false;
		/* Nobody else to wait for */
		return true;
	}

	/* There should be exactly one local pin */
	refcount = GetPrivateRefCount(buffer);
	Assert(refcount);
	if (refcount != 1)
		return false;

	/* Try to acquire lock */
	if (!ConditionalLockBuffer(buffer))
		return false;

	bufHdr = GetBufferDescriptor(buffer - 1);
	buf_state = LockBufHdr(bufHdr);
	refcount = BUF_STATE_GET_REFCOUNT(buf_state);

	Assert(refcount > 0);
	if (refcount == 1)
	{
		/* Successfully acquired exclusive lock with pincount 1 */
		UnlockBufHdr(bufHdr, buf_state);
		return true;
	}

	/* Failed, so release the lock */
	UnlockBufHdr(bufHdr, buf_state);
	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	return false;
}

/*
 * IsBufferCleanupOK - as above, but we already have the lock
 *
 * Check whether it's OK to perform cleanup on a buffer we've already
 * locked.  If we observe that the pin count is 1, our exclusive lock
 * happens to be a cleanup lock, and we can proceed with anything that
 * would have been allowable had we sought a cleanup lock originally.
 */
bool
IsBufferCleanupOK(Buffer buffer)
{
	BufferDesc *bufHdr;
	uint32		buf_state;

	Assert(BufferIsValid(buffer));

	if (BufferIsLocal(buffer))
	{
		/* There should be exactly one pin */
		if (LocalRefCount[-buffer - 1] != 1)
			return false;
		/* Nobody else to wait for */
		return true;
	}

	/* There should be exactly one local pin */
	if (GetPrivateRefCount(buffer) != 1)
		return false;

	bufHdr = GetBufferDescriptor(buffer - 1);

	/* caller must hold exclusive lock on buffer */
	Assert(LWLockHeldByMeInMode(BufferDescriptorGetContentLock(bufHdr),LW_EXCLUSIVE));

	buf_state = LockBufHdr(bufHdr);

	Assert(BUF_STATE_GET_REFCOUNT(buf_state) > 0);
	if (BUF_STATE_GET_REFCOUNT(buf_state) == 1)
	{
		/* pincount is OK. */
		UnlockBufHdr(bufHdr, buf_state);
		return true;
	}

	UnlockBufHdr(bufHdr, buf_state);
	return false;
}


/*
 *	Functions for buffer I/O handling
 *
 *	Note: We assume that nested buffer I/O never occurs.
 *	i.e at most one io_in_progress lock is held per proc.
 *
 *	Also note that these are used only for shared buffers, not local ones.
 */

/*
 * WaitIO -- Block until the IO_IN_PROGRESS flag on 'buf' is cleared.
 */
static void
WaitIO(BufferDesc *buf)
{
	/*
	 * Changed to wait until there's no IO - Inoue 01/13/2000
	 *
	 * Note this is *necessary* because an error abort in the process doing
	 * I/O could release the io_in_progress_lock prematurely. See
	 * AbortBufferIO.
	 */
	for (;;)
	{
		uint32		buf_state;

		/*
		 * It may not be necessary to acquire the spinlock to check the flag
		 * here, but since this test is essential for correctness, we'd better
		 * play it safe.
		 */
		buf_state = LockBufHdr(buf);
		UnlockBufHdr(buf, buf_state);

		if (!(buf_state & BM_IO_IN_PROGRESS))
			break;
		LWLockAcquire(BufferDescriptorGetIOLock(buf), LW_SHARED);
		LWLockRelease(BufferDescriptorGetIOLock(buf));
	}
}

/*
 * StartBufferIO: begin I/O on this buffer
 *	(Assumptions)
 *	My process is executing no IO
 *	The buffer is Pinned
 *
 * In some scenarios there are race conditions in which multiple backends
 * could attempt the same I/O operation concurrently.  If someone else
 * has already started I/O on this buffer then we will block on the
 * io_in_progress lock until he's done.
 *
 * Input operations are only attempted on buffers that are not BM_VALID,
 * and output operations only on buffers that are BM_VALID and BM_DIRTY,
 * so we can always tell if the work is already done.
 *
 * Returns true if we successfully marked the buffer as I/O busy,
 * false if someone else already did the work.
 */
bool
StartBufferIO(BufferDesc *buf, bool forInput)
{
	return polar_start_buffer_io_extend(buf, forInput, false);
}

/*
 * POLAR: base on the original StartBufferIO, it will be used by the copy buffer.
 */
bool
polar_start_buffer_io_extend(BufferDesc *buf,
							 bool forInput,
							 bool polar_copy_buf)
{
	uint32		buf_state;

	/* POLAR: bulk io*/
	if (!polar_bulk_io_is_in_progress)
	{
		/* single io */
		Assert(!InProgressBuf);
	}
	/* POLAR end */

	for (;;)
	{
		/*
		 * Grab the io_in_progress lock so that other processes can wait for
		 * me to finish the I/O.
		 */
		LWLockAcquire(BufferDescriptorGetIOLock(buf), LW_EXCLUSIVE);

		buf_state = LockBufHdr(buf);

		if (!(buf_state & BM_IO_IN_PROGRESS))
			break;

		/*
		 * The only way BM_IO_IN_PROGRESS could be set when the io_in_progress
		 * lock isn't held is if the process doing the I/O is recovering from
		 * an error (see AbortBufferIO).  If that's the case, we must wait for
		 * him to get unwedged.
		 */
		UnlockBufHdr(buf, buf_state);
		LWLockRelease(BufferDescriptorGetIOLock(buf));
		WaitIO(buf);
	}

	/* POLAR: For copy buffer, maybe someone else already copy or flush it. */
	if (polar_copy_buf)
	{
		if (!(buf_state & BM_VALID) ||
			!(buf_state & BM_DIRTY) ||
			(forInput && buf->copy_buffer != NULL) || /* Buffer has been copied */
			(!forInput && buf->copy_buffer == NULL))  /* Copy buffer has been flushed */
		{
			/* someone else already did the I/O */
			UnlockBufHdr(buf, buf_state);
			LWLockRelease(BufferDescriptorGetIOLock(buf));
			return false;
		}
	}
	/* Once we get here, there is definitely no I/O active on this buffer */

	else if (forInput ? (buf_state & BM_VALID) : !(buf_state & BM_DIRTY))
	{
		/* someone else already did the I/O */
		UnlockBufHdr(buf, buf_state);
		LWLockRelease(BufferDescriptorGetIOLock(buf));
		return false;
	}

	buf_state |= BM_IO_IN_PROGRESS;
	UnlockBufHdr(buf, buf_state);

	/* POLAR: bulk io */
	if (!polar_bulk_io_is_in_progress)
	{
		/* single io */
		InProgressBuf = buf;
		IsForInput = forInput;
	}
	else
	{
		/* bulk io */
		polar_bulk_io_in_progress_buf[polar_bulk_io_in_progress_count] = buf;
		/*
		 * bulk read io may be mixed with temporary write io, for flushing dirty evicted page.
		 * So polar_bulk_io_is_for_input[] is required for error recovery.
		 */
		polar_bulk_io_is_for_input[polar_bulk_io_in_progress_count] = forInput;
		polar_bulk_io_in_progress_count++;
	}
	/* POLAR end */

	return true;
}

/*
 * TerminateBufferIO: release a buffer we were doing I/O on
 *	(Assumptions)
 *	My process is executing IO for the buffer
 *	BM_IO_IN_PROGRESS bit is set for the buffer
 *	We hold the buffer's io_in_progress lock
 *	The buffer is Pinned
 *
 * If clear_dirty is true and BM_JUST_DIRTIED is not set, we clear the
 * buffer's BM_DIRTY flag.  This is appropriate when terminating a
 * successful write.  The check on BM_JUST_DIRTIED is necessary to avoid
 * marking the buffer clean if it was re-dirtied while we were writing.
 *
 * set_flag_bits gets ORed into the buffer's flags.  It must include
 * BM_IO_ERROR in a failure case.  For successful completion it could
 * be 0, or BM_VALID if we just finished reading in the page.
 */
void
TerminateBufferIO(BufferDesc *buf, bool clear_dirty, uint32 set_flag_bits)
{
	uint32		buf_state;

	/* POLAR：bulk io */
	/*
	 * single io:
	 * if (!polar_bulk_io_is_in_progress)
	   {  Assert(buf == InProgressBuf); }
	 */
	Assert(polar_bulk_io_is_in_progress || buf == InProgressBuf);
	/*
	 * bulk io:
	 * if (polar_bulk_io_is_in_progress)
	 * {
	 *   Assert(polar_bulk_io_in_progress_count > 0);
	 *   Assert(buf == polar_bulk_io_in_progress_buf[polar_bulk_io_in_progress_count - 1]);
	 * }
	 */
	Assert(!polar_bulk_io_is_in_progress || polar_bulk_io_in_progress_count > 0);
	Assert(!polar_bulk_io_is_in_progress || buf == polar_bulk_io_in_progress_buf[polar_bulk_io_in_progress_count - 1]);
	/* POLAR end */

	buf_state = LockBufHdr(buf);

	Assert(buf_state & BM_IO_IN_PROGRESS);

	buf_state &= ~(BM_IO_IN_PROGRESS | BM_IO_ERROR);
	if (clear_dirty && !(buf_state & BM_JUST_DIRTIED))
		buf_state &= ~(BM_DIRTY | BM_CHECKPOINT_NEEDED);

	buf_state |= set_flag_bits;
	UnlockBufHdr(buf, buf_state);

	/* POLAR: bulk io */
	if (!polar_bulk_io_is_in_progress)
	{
		/* single io */
		InProgressBuf = NULL;
	}
	else
	{
		/* bulk io */
		polar_bulk_io_in_progress_count--;
	}
	/* POLAR end */

	LWLockRelease(BufferDescriptorGetIOLock(buf));
}

/*
 * AbortBufferIO: Clean up any active buffer I/O after an error.
 *
 *	All LWLocks we might have held have been released,
 *	but we haven't yet released buffer pins, so the buffer is still pinned.
 *
 *	If I/O was in progress, we always set BM_IO_ERROR, even though it's
 *	possible the error condition wasn't related to the I/O.
 */
void
AbortBufferIO(void)
{
	BufferDesc *buf = InProgressBuf;

polar_bulk_read:
	/*
	 * POLAR: deal with local buffer(buf->buf_id < 0)
	 * local buffer doesn't need to release source, just decrease
	 * io_in_progress_count
	 */
	if (buf && buf->buf_id < 0)
		polar_bulk_io_in_progress_count--;
	else if (buf)
	{
		uint32		buf_state;

		/*
		 * Since LWLockReleaseAll has already been called, we're not holding
		 * the buffer's io_in_progress_lock. We have to re-acquire it so that
		 * we can use TerminateBufferIO. Anyone who's executing WaitIO on the
		 * buffer will be in a busy spin until we succeed in doing this.
		 */
		LWLockAcquire(BufferDescriptorGetIOLock(buf), LW_EXCLUSIVE);

		buf_state = LockBufHdr(buf);
		Assert(buf_state & BM_IO_IN_PROGRESS);
		if (IsForInput)
		{
			Assert(!(buf_state & BM_DIRTY));

			/* We'd better not think buffer is valid yet */
			Assert(!(buf_state & BM_VALID));
			UnlockBufHdr(buf, buf_state);
		}
		else
		{
			Assert(buf_state & BM_DIRTY);
			UnlockBufHdr(buf, buf_state);
			/* Issue notice if this is not the first failure... */
			if (buf_state & BM_IO_ERROR)
			{
				/* Buffer is pinned, so we can read tag without spinlock */
				char	   *path;

				path = relpathperm(buf->tag.rnode, buf->tag.forkNum);
				ereport(WARNING,
						(errcode(ERRCODE_IO_ERROR),
						 errmsg("could not write block %u of %s",
								buf->tag.blockNum, path),
						 errdetail("Multiple failures --- write error might be permanent.")));
				pfree(path);
			}
		}
		TerminateBufferIO(buf, false, BM_IO_ERROR);
	}

	/* POLAR: bulk io recovery */
	if (polar_bulk_io_is_in_progress)
	{
		if (polar_bulk_io_in_progress_count > 0)
		{
			buf = polar_bulk_io_in_progress_buf[polar_bulk_io_in_progress_count - 1];
			IsForInput = polar_bulk_io_is_for_input[polar_bulk_io_in_progress_count - 1];
			/*
			 * In TerminateBufferIO(), polar_bulk_io_in_progress_count was reduced by 1.
			 */
			goto polar_bulk_read;
		}
		polar_bulk_io_is_in_progress = false;
	}


	/*
	 * POLAR: we must reset read_min_lsn where ERROR, otherwise bgwriter cannot
	 * clean hashtable or logindex anymore
	 */
	POLAR_RESET_BACKEND_READ_MIN_LSN();
}

/*
 * Error context callback for errors occurring during shared buffer writes.
 */
static void
shared_buffer_write_error_callback(void *arg)
{
	BufferDesc *bufHdr = (BufferDesc *) arg;

	/* Buffer is pinned, so we can read the tag without locking the spinlock */
	if (bufHdr != NULL)
	{
		char	   *path = relpathperm(bufHdr->tag.rnode, bufHdr->tag.forkNum);

		errcontext("writing block %u of relation %s",
				   bufHdr->tag.blockNum, path);
		pfree(path);
	}
}

/*
 * Error context callback for errors occurring during local buffer writes.
 */
static void
local_buffer_write_error_callback(void *arg)
{
	BufferDesc *bufHdr = (BufferDesc *) arg;

	if (bufHdr != NULL)
	{
		char	   *path = relpathbackend(bufHdr->tag.rnode, MyBackendId,
										  bufHdr->tag.forkNum);

		errcontext("writing block %u of relation %s",
				   bufHdr->tag.blockNum, path);
		pfree(path);
	}
}

/*
 * RelFileNode qsort/bsearch comparator; see RelFileNodeEquals.
 */
static int
rnode_comparator(const void *p1, const void *p2)
{
	RelFileNode n1 = *(const RelFileNode *) p1;
	RelFileNode n2 = *(const RelFileNode *) p2;

	if (n1.relNode < n2.relNode)
		return -1;
	else if (n1.relNode > n2.relNode)
		return 1;

	if (n1.dbNode < n2.dbNode)
		return -1;
	else if (n1.dbNode > n2.dbNode)
		return 1;

	if (n1.spcNode < n2.spcNode)
		return -1;
	else if (n1.spcNode > n2.spcNode)
		return 1;
	else
		return 0;
}

/*
 * Lock buffer header - set BM_LOCKED in buffer state.
 */
uint32
LockBufHdr(BufferDesc *desc)
{
	SpinDelayStatus delayStatus;
	uint32		old_buf_state;

	init_local_spin_delay(&delayStatus);

	while (true)
	{
		/* set BM_LOCKED flag */
		old_buf_state = pg_atomic_fetch_or_u32(&desc->state, BM_LOCKED);
		/* if it wasn't set before we're OK */
		if (!(old_buf_state & BM_LOCKED))
			break;
		perform_spin_delay(&delayStatus);
	}
	finish_spin_delay(&delayStatus);
	return old_buf_state | BM_LOCKED;
}

/*
 * Wait until the BM_LOCKED flag isn't set anymore and return the buffer's
 * state at that point.
 *
 * Obviously the buffer could be locked by the time the value is returned, so
 * this is primarily useful in CAS style loops.
 */
static uint32
WaitBufHdrUnlocked(BufferDesc *buf)
{
	SpinDelayStatus delayStatus;
	uint32		buf_state;

	init_local_spin_delay(&delayStatus);

	buf_state = pg_atomic_read_u32(&buf->state);

	while (buf_state & BM_LOCKED)
	{
		perform_spin_delay(&delayStatus);
		buf_state = pg_atomic_read_u32(&buf->state);
	}

	finish_spin_delay(&delayStatus);

	return buf_state;
}

/*
 * BufferTag comparator.
 */
static int
buffertag_comparator(const void *a, const void *b)
{
	const BufferTag *ba = (const BufferTag *) a;
	const BufferTag *bb = (const BufferTag *) b;
	int			ret;

	ret = rnode_comparator(&ba->rnode, &bb->rnode);

	if (ret != 0)
		return ret;

	if (ba->forkNum < bb->forkNum)
		return -1;
	if (ba->forkNum > bb->forkNum)
		return 1;

	if (ba->blockNum < bb->blockNum)
		return -1;
	if (ba->blockNum > bb->blockNum)
		return 1;

	return 0;
}

/*
 * Comparator determining the writeout order in a checkpoint.
 *
 * It is important that tablespaces are compared first, the logic balancing
 * writes between tablespaces relies on it.
 */
static int
ckpt_buforder_comparator(const void *pa, const void *pb)
{
	const CkptSortItem *a = (const CkptSortItem *) pa;
	const CkptSortItem *b = (const CkptSortItem *) pb;

	/* compare tablespace */
	if (a->tsId < b->tsId)
		return -1;
	else if (a->tsId > b->tsId)
		return 1;
	/* compare relation */
	if (a->relNode < b->relNode)
		return -1;
	else if (a->relNode > b->relNode)
		return 1;
	/* compare fork */
	else if (a->forkNum < b->forkNum)
		return -1;
	else if (a->forkNum > b->forkNum)
		return 1;
	/* compare block number */
	else if (a->blockNum < b->blockNum)
		return -1;
	else if (a->blockNum > b->blockNum)
		return 1;
	/* equal page IDs are unlikely, but not impossible */
	return 0;
}

/*
 * Comparator for a Min-Heap over the per-tablespace checkpoint completion
 * progress.
 */
static int
ts_ckpt_progress_comparator(Datum a, Datum b, void *arg)
{
	CkptTsStatus *sa = (CkptTsStatus *) a;
	CkptTsStatus *sb = (CkptTsStatus *) b;

	/* we want a min-heap, so return 1 for the a < b */
	if (sa->progress < sb->progress)
		return 1;
	else if (sa->progress == sb->progress)
		return 0;
	else
		return -1;
}

/*
 * Initialize a writeback context, discarding potential previous state.
 *
 * *max_pending is a pointer instead of an immediate value, so the coalesce
 * limits can easily changed by the GUC mechanism, and so calling code does
 * not have to check the current configuration. A value is 0 means that no
 * writeback control will be performed.
 */
void
WritebackContextInit(WritebackContext *context, int *max_pending)
{
	Assert(*max_pending <= WRITEBACK_MAX_PENDING_FLUSHES);

	context->max_pending = max_pending;
	context->nr_pending = 0;
}

/*
 * Add buffer to list of pending writeback requests.
 */
void
ScheduleBufferTagForWriteback(WritebackContext *context, BufferTag *tag)
{
	PendingWriteback *pending;

	/*
	 * Add buffer to the pending writeback array, unless writeback control is
	 * disabled.
	 */
	if (*context->max_pending > 0)
	{
		Assert(*context->max_pending <= WRITEBACK_MAX_PENDING_FLUSHES);

		pending = &context->pending_writebacks[context->nr_pending++];

		pending->tag = *tag;
	}

	/*
	 * Perform pending flushes if the writeback limit is exceeded. This
	 * includes the case where previously an item has been added, but control
	 * is now disabled.
	 */
	if (context->nr_pending >= *context->max_pending)
		IssuePendingWritebacks(context);
}

/*
 * Issue all pending writeback requests, previously scheduled with
 * ScheduleBufferTagForWriteback, to the OS.
 *
 * Because this is only used to improve the OSs IO scheduling we try to never
 * error out - it's just a hint.
 */
void
IssuePendingWritebacks(WritebackContext *context)
{
	int			i;

	if (context->nr_pending == 0)
		return;

	/*
	 * Executing the writes in-order can make them a lot faster, and allows to
	 * merge writeback requests to consecutive blocks into larger writebacks.
	 */
	qsort(&context->pending_writebacks, context->nr_pending,
		  sizeof(PendingWriteback), buffertag_comparator);

	/*
	 * Coalesce neighbouring writes, but nothing else. For that we iterate
	 * through the, now sorted, array of pending flushes, and look forward to
	 * find all neighbouring (or identical) writes.
	 */
	for (i = 0; i < context->nr_pending; i++)
	{
		PendingWriteback *cur;
		PendingWriteback *next;
		SMgrRelation reln;
		int			ahead;
		BufferTag	tag;
		Size		nblocks = 1;

		cur = &context->pending_writebacks[i];
		tag = cur->tag;

		/*
		 * Peek ahead, into following writeback requests, to see if they can
		 * be combined with the current one.
		 */
		for (ahead = 0; i + ahead + 1 < context->nr_pending; ahead++)
		{
			next = &context->pending_writebacks[i + ahead + 1];

			/* different file, stop */
			if (!RelFileNodeEquals(cur->tag.rnode, next->tag.rnode) ||
				cur->tag.forkNum != next->tag.forkNum)
				break;

			/* ok, block queued twice, skip */
			if (cur->tag.blockNum == next->tag.blockNum)
				continue;

			/* only merge consecutive writes */
			if (cur->tag.blockNum + 1 != next->tag.blockNum)
				break;

			nblocks++;
			cur = next;
		}

		i += ahead;

		/* and finally tell the kernel to write the data to storage */
		reln = smgropen(tag.rnode, InvalidBackendId);
		smgrwriteback(reln, tag.forkNum, tag.blockNum, nblocks);
	}

	context->nr_pending = 0;
}


/*
 * Implement slower/larger portions of TestForOldSnapshot
 *
 * Smaller/faster portions are put inline, but the entire set of logic is too
 * big for that.
 */
void
TestForOldSnapshot_impl(Snapshot snapshot, Relation relation)
{
	if (RelationAllowsEarlyPruning(relation)
		&& (snapshot)->whenTaken < GetOldSnapshotThresholdTimestamp())
		ereport(ERROR,
				(errcode(ERRCODE_SNAPSHOT_TOO_OLD),
				 errmsg("snapshot too old")));
}

/* POLAR: Replay buffer with io lock. Return true if page lsn changed after replay */
static bool
polar_apply_io_locked_page(BufferDesc *bufHdr, XLogRecPtr replay_from, XLogRecPtr checkpoint_redo_lsn)
{
	/*
	 * POLAR: It's better to use AmStartupProcess() than InRecovery to avoid
	 * ambiguity.
	 */
	if (polar_logindex_redo_instance &&
		((!InRecovery) || reachedConsistency))
	{
		/*
		 * Note: All modifications about replay-page must be
		 *       applied to both polar_bulk_read_buffer_common() and ReadBuffer_common().
		 */

		return polar_logindex_io_lock_apply(polar_logindex_redo_instance, bufHdr, replay_from, checkpoint_redo_lsn);
	}

	return false;
}

static polar_redo_action 
polar_require_backend_redo(bool local_buf, ReadBufferMode mode, ForkNumber fork_num, XLogRecPtr *replay_from)
{
	bool redo_required = polar_logindex_require_backend_redo(polar_logindex_redo_instance, fork_num, replay_from);
	polar_redo_action action = POLAR_REDO_NO_ACTION;

	if (!local_buf)
	{
		if (redo_required)
		{
			if (POLAR_IN_LOGINDEX_PARALLEL_REPLAY() &&
					!(mode == RBM_ZERO_AND_LOCK || mode == RBM_ZERO_AND_CLEANUP_LOCK))
			{
				action = POLAR_REDO_MARK_OUTDATE;
			}
			else
				action = POLAR_REDO_REPLAY_XLOG;
		}
	}

	return action;
}

static polar_checksum_err_action
polar_handle_read_error_block(Block bufBlock, SMgrRelation smgr, ForkNumber forkNum, BlockNumber blockNum,
		ReadBufferMode mode, polar_redo_action redo_action, int *repeat_read_times, BufferDesc *buf_desc)
{
	bool has_redo_action = (redo_action != POLAR_REDO_NO_ACTION);

	if (mode == RBM_ZERO_ON_ERROR || zero_damaged_pages)
	{
		char *relpath = relpath(smgr->smgr_rnode, forkNum);
		ereport(WARNING,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid page in block %u of relation %s; zeroing out page",
						blockNum, relpath)));
		MemSet((char *) bufBlock, 0, BLCKSZ);
		pfree(relpath);
		return POLAR_CHECKSUM_ERR_NO_ACTION;
	}
	else if (polar_has_partial_write && polar_in_replica_mode())
	{
		/*
		 * POLAR: We enable full_page_writes if filesystem doesn't support atomic wirte. rw could write part of the page while ro read this page and failed to verify checksum.
		 * So in replica mode we will delay some time and read this page again
		 */
		(*repeat_read_times)++;
		if (*repeat_read_times % POLAR_WARNING_REPEAT_TIMES == 0)
		{
			char *relpath = relpath(smgr->smgr_rnode, forkNum);
			ereport(WARNING, (errmsg("%d times to repeat read block %u of relation %s",
							*repeat_read_times, blockNum, relpath)));
			pfree(relpath);
			CHECK_FOR_INTERRUPTS();
		}

		POLAR_DELAY_REPEAT_READ();
		return POLAR_CHECKSUM_ERR_REPEAT_READ;
	}
	else if (fullPageWrites && has_redo_action)
	{
		/*
		 * POLAR: It's doing online promote if redo_required but is not in replica mode.
		 * We will replay this page from last checkpoint, so we can find the first FPI log to replay
		 */
		char *relpath = relpath(smgr->smgr_rnode, forkNum);
		ereport(WARNING,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid page in block %u of relation %s; will replay from last checkpoint",
						blockNum, relpath)));
		MemSet((char *) bufBlock, 0, BLCKSZ);
		pfree(relpath);

		return POLAR_CHECKSUM_ERR_CHECKPOINT_REDO;
	}
	else if (polar_can_flog_repair(flog_instance, buf_desc, has_redo_action))
	{
		/*
		 * POLAR: When it is in recovery but not a ro node
		 * and flashback log is enable, we can repair the page by flashback log.
		 */
		char *relpath = relpath(smgr->smgr_rnode, forkNum);

		/* Just WARNING */
		ereport(WARNING, (errcode(ERRCODE_DATA_CORRUPTED),
						  errmsg("invalid page in block %u of relation %s; "
								 "get origin page in flashback log to repair it",
								 blockNum, relpath)));
		pfree(relpath);
		polar_repair_partial_write(flog_instance, buf_desc);
		if (has_redo_action)
			return POLAR_CHECKSUM_ERR_CHECKPOINT_REDO;
		else
			return POLAR_CHECKSUM_ERR_NO_ACTION;
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid page in block %u of relation %s",
						blockNum,
						relpath(smgr->smgr_rnode, forkNum))));

	return POLAR_CHECKSUM_ERR_NO_ACTION;
}

/*
 * POLAR: In some cases, we check for interrupt between pin buffer and
 * unpin buffer, so we must unpin the buffer before exit.
 */
void
polar_unpin_buffer_proc_exit(Buffer buf)
{
	if (GetPrivateRefCount(buf))
	{
		BufferDesc *buf_desc;

		buf_desc = GetBufferDescriptor(buf - 1);
		UnpinBuffer(buf_desc, false);
	}
}

#ifndef POLAR_BUFMGR_C
#define POLAR_BUFMGR_C

#include "polar_bufmgr.c"

#endif
