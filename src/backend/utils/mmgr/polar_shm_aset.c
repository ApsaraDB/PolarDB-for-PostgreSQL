/*-------------------------------------------------------------------------
 *
 * polar_shm_aset.c
 *    Implement ShmAllocSet that is AllocSet based on shared memory
 *    that managed by dsa.
 *
 * This file is copied from aset.c, we replace malloc/free with dsa to
 * manage the shared memory. ShmAllocSet can be used by global plan cache
 * or other global cache.
 *
 * For more information please refer to src/backend/utils/mmgr/aset.c.
 *
 * Portions Copyright (c) 2022, Alibaba Group Holding Limited
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/mmgr/polar_shm_aset.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/memdebug.h"
#include "utils/memutils.h"

/* POLAR */
#include "storage/spin.h"
#include "storage/polar_memutils.h"
#include "portability/instr_time.h"
#include "utils/dsa.h"

/* Define this to detail debug alloc information */
/* #define HAVE_ALLOCINFO */

/*--------------------
 * Chunk freelist k holds chunks of size 1 << (k + ALLOC_MINBITS),
 * for k = 0 .. ALLOCSET_NUM_FREELISTS-1.
 *
 * Note that all chunks in the freelists have power-of-2 sizes.  This
 * improves recyclability: we may waste some space, but the wasted space
 * should stay pretty constant as requests are made and released.
 *
 * A request too large for the last freelist is handled by allocating a
 * dedicated block from malloc().  The block still has a block header and
 * chunk header, but when the chunk is freed we'll return the whole block
 * to malloc(), not put it on our freelists.
 *
 * CAUTION: ALLOC_MINBITS must be large enough so that
 * 1<<ALLOC_MINBITS is at least MAXALIGN,
 * or we may fail to align the smallest chunks adequately.
 * 8-byte alignment is enough on all currently known machines.
 *
 * With the current parameters, request sizes up to 8K are treated as chunks,
 * larger requests go into dedicated blocks.  Change ALLOCSET_NUM_FREELISTS
 * to adjust the boundary point; and adjust ALLOCSET_SEPARATE_THRESHOLD in
 * memutils.h to agree.  (Note: in contexts with small maxBlockSize, we may
 * set the allocChunkLimit to less than 8K, so as to avoid space wastage.)
 *--------------------
 */

#define ALLOC_MINBITS		3	/* smallest chunk size is 8 bytes */
#define ALLOCSET_NUM_FREELISTS	11
#define ALLOC_CHUNK_LIMIT	(1 << (ALLOCSET_NUM_FREELISTS-1+ALLOC_MINBITS))
/* Size of largest chunk that we use a fixed size for */
#define ALLOC_CHUNK_FRACTION	4
/* We allow chunks to be at most 1/4 of maxBlockSize (less overhead) */

/*--------------------
 * The first block allocated for an allocset has size initBlockSize.
 * Each time we have to allocate another block, we double the block size
 * (if possible, and without exceeding maxBlockSize), so as to reduce
 * the bookkeeping load on malloc().
 *
 * Blocks allocated to hold oversize chunks do not follow this rule, however;
 * they are just however big they need to be to hold that single chunk.
 *
 * Also, if a minContextSize is specified, the first block has that size,
 * and then initBlockSize is used for the next one.
 *--------------------
 */

#define ALLOC_BLOCKHDRSZ	MAXALIGN(sizeof(AllocBlockData))
#define ALLOC_CHUNKHDRSZ	sizeof(struct AllocChunkData)

typedef struct AllocBlockData *AllocBlock;	/* forward reference */
typedef struct AllocChunkData *AllocChunk;

/*
 * AllocPointer
 *		Aligned pointer which may be a member of an allocation set.
 */
typedef void *AllocPointer;

/*
 * Like AllocSetContext, this is our standard implementation of MemoryContext
 * based on shared memory.
 *
 * Note: header.isReset means there is nothing for ShmAllocSetReset to do.
 * This is different from the aset being physically empty (empty blocks list)
 * because we will still have a keeper block.  It's also different from the set
 * being logically empty, because we don't attempt to detect pfree'ing the
 * last active chunk.
 */
typedef struct ShmAllocSetContext
{
	MemoryContextData header;	/* Standard memory-context fields */
	/* Info about storage allocated in this context: */
	AllocBlock	blocks;			/* head of list of blocks in this set */
	AllocChunk	freelist[ALLOCSET_NUM_FREELISTS];	/* free chunk lists */
	/* Allocation parameters for this context: */
	Size		initBlockSize;	/* initial block size */
	Size		maxBlockSize;	/* maximum block size */
	Size		nextBlockSize;	/* next block size to allocate */
	Size		allocChunkLimit;	/* effective chunk size limit */
	AllocBlock	keeper;			/* keep this block over resets */
	/* freelist this context could be put in, or -1 if not a candidate: */
	int			freeListIndex;	/* index in context_freelists[], or -1 */

	/*
	 * PX: Memory accounting fields
	 *
	 * accountingParent: Each MemoryContext has a designated MemoryContext
	 * that will act as its account for tracking all memory allocations and
	 * de-allocations performed within the MemoryContext, captured in
	 * localAllocated, currentAllocated and peakAllocated. The account for
	 * a MemoryContext must be the context itself, or one of its ancestors
	 * in the MemoryContext tree. For MemoryContexts that are accounts, their
	 * accountingParent field will point to itself.
	 *
	 * localAllocated: This is the memory allocated (in bytes) for this memory
	 * context alone (i.e. it does not consider the memory allocated for any
	 * members of the context's subtree).
	 * GPDB_13_MERGE_FIXME: PostgreSQL v13 added a field like this in
	 * MemoryContextData.mem_allocated. We should probably switch to using that
	 * once we catch up.
	 *
	 * currentAllocated: This field is only applicable to a MemoryContext
	 * designated as an account. It tracks the current bytes allocated in all
	 * of the subtree MemoryContexts it is responsible for.
	 *ls -
	 * peakAllocated: Maximum 'currentAllocated' value ever held in the
	 * lifetime of this context.
	 */
	struct ShmAllocSetContext *accountingParent;
	Size		localAllocated;

	Size		currentAllocated;
	Size		peakAllocated;

	/* POLAR: sharad memory info */
	ShmAsetCtl	*ctl;
} ShmAllocSetContext;

typedef ShmAllocSetContext *ShmAllocSet;

 /* the memorycontexts are accounted when accountingParent point to itself */
static inline bool
polar_is_memory_account(ShmAllocSet set)
{
	return (set->accountingParent == set);
}

/* account the new memory */
static inline void
polar_memory_account_inc_allocated(ShmAllocSet set, Size newbytes)
{
	ShmAllocSet	parent = set->accountingParent;

	Assert(parent != NULL);
	set->localAllocated += newbytes;

	parent->currentAllocated += newbytes;
	parent->peakAllocated = Max(parent->peakAllocated,
							  parent->currentAllocated);

	/* Make sure these values are not overflow */
	Assert(set->localAllocated >= newbytes);
	// Assert(parent->currentAllocated >= set->localAllocated);
}

/* delete the allocated value for resetting account */
static inline void
polar_memory_account_dec_allocated(ShmAllocSet set, Size newbytes)
{
	ShmAllocSet	parent = set->accountingParent;

	Assert(parent != NULL);
	Assert(set->localAllocated >= newbytes);
	// Assert(parent->currentAllocated >= set->localAllocated);

	set->localAllocated -= newbytes;
	parent->currentAllocated -= newbytes;

}

/*
 * AllocBlock
 *		An AllocBlock is the unit of memory that is obtained by aset.c
 *		from malloc().  It contains one or more AllocChunks, which are
 *		the units requested by palloc() and freed by pfree().  AllocChunks
 *		cannot be returned to malloc() individually, instead they are put
 *		on freelists by pfree() and re-used by the next palloc() that has
 *		a matching request size.
 *
 *		AllocBlockData is the header data for a block --- the usable space
 *		within the block begins at the next alignment boundary.
 */
typedef struct AllocBlockData
{
	ShmAllocSet	aset;			/* aset that owns this block */
	AllocBlock	prev;			/* prev block in aset's blocks list, if any */
	AllocBlock	next;			/* next block in aset's blocks list, if any */
	char	   *freeptr;		/* start of free space in this block */
	char	   *endptr;			/* end of space in this block */
}			AllocBlockData;

/*
 * AllocChunk
 *		The prefix of each piece of memory in an AllocBlock
 *
 * Note: to meet the memory context APIs, the payload area of the chunk must
 * be maxaligned, and the "aset" link must be immediately adjacent to the
 * payload area (cf. GetMemoryChunkContext).  We simplify matters for this
 * module by requiring sizeof(AllocChunkData) to be maxaligned, and then
 * we can ensure things work by adding any required alignment padding before
 * the "aset" field.  There is a static assertion below that the alignment
 * is done correctly.
 */
typedef struct AllocChunkData
{
	/* size is always the size of the usable space in the chunk */
	Size		size;
	/* when debugging memory usage, also store actual requested size */
	/* this is zero in a free chunk */
	Size		requested_size;/* POLAR: Shared Server */

#define ALLOCCHUNK_RAWSIZE  (SIZEOF_SIZE_T * 2 + SIZEOF_VOID_P)

	/* ensure proper alignment by adding padding if needed */
#if (ALLOCCHUNK_RAWSIZE % MAXIMUM_ALIGNOF) != 0
	char		padding[MAXIMUM_ALIGNOF - ALLOCCHUNK_RAWSIZE % MAXIMUM_ALIGNOF];
#endif

	/* aset is the owning aset if allocated, or the freelist link if free */
	void	   *aset;
	/* there must not be any padding to reach a MAXALIGN boundary here! */
}			AllocChunkData;

/*
 * Only the "aset" field should be accessed outside this module.
 * We keep the rest of an allocated chunk's header marked NOACCESS when using
 * valgrind.  But note that chunk headers that are in a freelist are kept
 * accessible, for simplicity.
 */
#define ALLOCCHUNK_PRIVATE_LEN	offsetof(AllocChunkData, aset)

/*
 * AllocPointerIsValid
 *		True iff pointer is valid allocation pointer.
 */
#define AllocPointerIsValid(pointer) PointerIsValid(pointer)

/*
 * ShmAllocSetIsValid
 *		True iff set is valid allocation set.
 */
#define ShmAllocSetIsValid(set) PointerIsValid(set)

#define AllocPointerGetChunk(ptr)	\
					((AllocChunk)(((char *)(ptr)) - ALLOC_CHUNKHDRSZ))
#define AllocChunkGetPointer(chk)	\
					((AllocPointer)(((char *)(chk)) + ALLOC_CHUNKHDRSZ))

/*
 * Rather than repeatedly creating and deleting memory contexts, we keep some
 * freed contexts in freelists so that we can hand them out again with little
 * work.  Before putting a context in a freelist, we reset it so that it has
 * only its initial malloc chunk and no others.  To be a candidate for a
 * freelist, a context must have the same minContextSize/initBlockSize as
 * other contexts in the list; but its maxBlockSize is irrelevant since that
 * doesn't affect the size of the initial chunk.
 *
 * We currently provide one freelist for ALLOCSET_DEFAULT_SIZES contexts
 * and one for ALLOCSET_SMALL_SIZES contexts; the latter works for
 * ALLOCSET_START_SMALL_SIZES too, since only the maxBlockSize differs.
 *
 * Ordinarily, we re-use freelist contexts in last-in-first-out order, in
 * hopes of improving locality of reference.  But if there get to be too
 * many contexts in the list, we'd prefer to drop the most-recently-created
 * contexts in hopes of keeping the process memory map compact.
 * We approximate that by simply deleting all existing entries when the list
 * overflows, on the assumption that queries that allocate a lot of contexts
 * will probably free them in more or less reverse order of allocation.
 *
 * Contexts in a freelist are chained via their nextchild pointers.
 */
#define MAX_FREE_CONTEXTS 10	/* arbitrary limit on freelist length */

/*
 * These functions implement the MemoryContext API for ShmAllocSet contexts.
 */
static void *ShmAllocSetAlloc(MemoryContext context, Size size);
static void ShmAllocSetFree(MemoryContext context, void *pointer);
static void *ShmAllocSetRealloc(MemoryContext context, void *pointer, Size size);
static void ShmAllocSetReset(MemoryContext context);
static void ShmAllocSetDelete(MemoryContext context, MemoryContext parent);
static Size ShmAllocSetGetChunkSpace(MemoryContext context, void *pointer);
static bool ShmAllocSetIsEmpty(MemoryContext context);
static void ShmAllocSetStats(MemoryContext context,
			  MemoryStatsPrintFunc printfunc, void *passthru,
			  MemoryContextCounters *totals);

/* POLAR px */
static void polar_AllocSetDeclareAccountingRoot(MemoryContext context);
static Size polar_AllocSetGetPeakUsage(MemoryContext context);
/*POLAR end */

static Size	polar_ShmAllocUsableSize(MemoryContext context, void *pointer);

#ifdef MEMORY_CONTEXT_CHECKING
static void ShmAllocSetCheck(MemoryContext context);
#endif

/*
 * This is the virtual function table for ShmAllocSet contexts.
 */
static const MemoryContextMethods AllocSetMethods = {
	ShmAllocSetAlloc,
	ShmAllocSetFree,
	ShmAllocSetRealloc,
	ShmAllocSetReset,
	ShmAllocSetDelete,
	ShmAllocSetGetChunkSpace,
	ShmAllocSetIsEmpty,
	ShmAllocSetStats
#ifdef MEMORY_CONTEXT_CHECKING
	,ShmAllocSetCheck
#endif
	/* POLAR px */
	,polar_AllocSetDeclareAccountingRoot
	,polar_AllocSetGetPeakUsage
	/* POLAR end */
	,polar_ShmAllocUsableSize
};

/*
 * Table for ShmAllocSetFreeIndex
 */
#define LT16(n) n, n, n, n, n, n, n, n, n, n, n, n, n, n, n, n

static const unsigned char LogTable256[256] =
{
	0, 1, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4,
	LT16(5), LT16(6), LT16(6), LT16(7), LT16(7), LT16(7), LT16(7),
	LT16(8), LT16(8), LT16(8), LT16(8), LT16(8), LT16(8), LT16(8), LT16(8)
};

/* ----------
 * Debug macros
 * ----------
 */
#ifdef HAVE_ALLOCINFO
#define AllocFreeInfo(_cxt, _chunk) \
			fprintf(stderr, "AllocFree: %s: %p, %zu\n", \
				(_cxt)->header.name, (_chunk), (_chunk)->size)
#define AllocAllocInfo(_cxt, _chunk) \
			fprintf(stderr, "AllocAlloc: %s: %p, %zu\n", \
				(_cxt)->header.name, (_chunk), (_chunk)->size)
#else
#define AllocFreeInfo(_cxt, _chunk)
#define AllocAllocInfo(_cxt, _chunk)
#endif

/* POLAR */
#define shmaset_dsa_free(ctl, ptr) \
	(dsa_free(polar_shm_aset_get_area((ctl)->type), rawptr_to_dsaptr((ptr), ctl->base)))

static void *shmaset_dsa_alloc(ShmAsetCtl *ctl, size_t size, const char *name);
/* POLAR end */


#define FREELIST_LOCK_STAT_INIT \
	DSALockStat *area_lock_stat = dsa_get_lock_stat(polar_shm_aset_get_area(ctl->type));\
	instr_time lock_start;\
	instr_time lock_duration;\
	uint64 lock_time_us;\

#define FREELIST_LOCK_STAT_BEGIN \
	do { \
		INSTR_TIME_SET_CURRENT(lock_start);\
		area_lock_stat->lock_freelist_count++;\
	} while (0)

#define FREELIST_LOCK_STAT_END \
	do { \
		INSTR_TIME_SET_CURRENT(lock_duration);\
		INSTR_TIME_SUBTRACT(lock_duration, lock_start);\
		lock_time_us = INSTR_TIME_GET_MICROSEC(lock_duration);\
		area_lock_stat->lock_freelist_time_us += lock_time_us;\
		if (lock_time_us > area_lock_stat->lock_freelist_max_time_us)\
			area_lock_stat->lock_freelist_max_time_us = lock_time_us;\
	} while (0)


/* ----------
 * ShmAllocSetFreeIndex - like AllocSetFreeIndex
 *
 *		Depending on the size of an allocation compute which freechunk
 *		list of the alloc set it belongs to.  Caller must have verified
 *		that size <= ALLOC_CHUNK_LIMIT.
 * ----------
 */
static inline int
ShmAllocSetFreeIndex(Size size)
{
	int			idx;
	unsigned int t,
				tsize;

	if (size > (1 << ALLOC_MINBITS))
	{
		tsize = (size - 1) >> ALLOC_MINBITS;

		/*
		 * At this point we need to obtain log2(tsize)+1, ie, the number of
		 * not-all-zero bits at the right.  We used to do this with a
		 * shift-and-count loop, but this function is enough of a hotspot to
		 * justify micro-optimization effort.  The best approach seems to be
		 * to use a lookup table.  Note that this code assumes that
		 * ALLOCSET_NUM_FREELISTS <= 17, since we only cope with two bytes of
		 * the tsize value.
		 */
		t = tsize >> 8;
		idx = t ? LogTable256[t] + 8 : LogTable256[tsize];

		Assert(idx < ALLOCSET_NUM_FREELISTS);
	}
	else
		idx = 0;

	return idx;
}


/*
 * Public routines
 */

/*
 * ShmAllocSetReset
 *		Frees all memory which is allocated in the given set.
 *
 * Actually, this routine has some discretion about what to do.
 * It should mark all allocated chunks freed, but it need not necessarily
 * give back all the resources the set owns.  Our actual implementation is
 * that we give back all but the "keeper" block (which we must keep, since
 * it shares a malloc chunk with the context header).  In this way, we don't
 * thrash malloc() when a context is repeatedly reset after small allocations,
 * which is typical behavior for per-tuple contexts.
 */
static void
ShmAllocSetReset(MemoryContext context)
{
	ShmAllocSet	set = (ShmAllocSet) context;
	AllocBlock	block;

	AssertArg(ShmAllocSetIsValid(set));

#ifdef MEMORY_CONTEXT_CHECKING
	/* Check for corruption and leaks before freeing */
	ShmAllocSetCheck(context);
#endif

    /*
	 * Make sure all children have been deleted,
	 * or the accounting data is incorrect.
	 */
	Assert(context->firstchild == NULL);
	polar_memory_account_dec_allocated(set, set->localAllocated);

	/* Clear chunk freelists */
	MemSetAligned(set->freelist, 0, sizeof(set->freelist));

	block = set->blocks;

	/* New blocks list will be just the keeper block */
	set->blocks = set->keeper;

	while (block != NULL)
	{
		AllocBlock	next = block->next;

		if (block == set->keeper)
		{
			/* Reset the block, but don't return it to malloc */
			char	   *datastart = ((char *) block) + ALLOC_BLOCKHDRSZ;

#ifdef CLOBBER_FREED_MEMORY
			wipe_mem(datastart, block->freeptr - datastart);
#else
			/* wipe_mem() would have done this */
			MEMDEBUG_MAKE_MEM_NOACCESS(datastart, block->freeptr - datastart);
#endif
			block->freeptr = datastart;
			block->prev = NULL;
			block->next = NULL;
		}
		else
		{
			/* Normal case, release the block */
			context->mem_allocated -= block->endptr - ((char *) block);

#ifdef CLOBBER_FREED_MEMORY
			wipe_mem(block, block->freeptr - ((char *) block));
#endif
			shmaset_dsa_free(set->ctl, block);
		}
		block = next;
	}

	/* Reset block size allocation sequence, too */
	set->nextBlockSize = set->initBlockSize;
}

/*
 * ShmAllocSetDelete
 *		Frees all memory which is allocated in the given set,
 *		in preparation for deletion of the set.
 *
 * Unlike ShmAllocSetReset, this *must* free all resources of the set.
 */
static void
ShmAllocSetDelete(MemoryContext context, MemoryContext parent)
{
	ShmAllocSet	set = (ShmAllocSet) context;
	AllocBlock	block = set->blocks;

	/* POLAR */
	ShmAsetCtl	*ctl;

	AssertArg(ShmAllocSetIsValid(set));

	/* POLAR */
	ctl = set->ctl;
	Assert(ctl != NULL);

#ifdef MEMORY_CONTEXT_CHECKING
	/* Check for corruption and leaks before freeing */
	ShmAllocSetCheck(context);
#endif

	/* POLAR px :Make sure all children have been deleted */
	Assert(context->firstchild == NULL);
	polar_memory_account_dec_allocated(set, set->localAllocated);
	if (polar_is_memory_account(set) && parent)
	{
		/* Roll up our peak value to the parent, before this context goes away. */
		ShmAllocSet	parentset = (ShmAllocSet) parent;

		parentset->accountingParent->peakAllocated =
			Max(set->peakAllocated,
				parentset->accountingParent->peakAllocated);
	}
	/* POLAR end */

	/*
	 * If the context is a candidate for a freelist, put it into that freelist
	 * instead of destroying it.
	 */
	if (set->freeListIndex >= 0)
	{
		ShmAsetFreeList *freelist = &ctl->context_freelists[set->freeListIndex];
		FREELIST_LOCK_STAT_INIT;

		/*
		 * Reset the context, if it needs it, so that we aren't hanging on to
		 * more than the initial malloc chunk.
		 */
		if (!context->isReset)
			MemoryContextResetOnly(context);

		FREELIST_LOCK_STAT_BEGIN;
		SpinLockAcquire(&freelist->lock);
		if (freelist->num_free > MAX_FREE_CONTEXTS)
		{
			while (freelist->first_free != NULL)
			{
				ShmAllocSetContext *oldset = freelist->first_free;

				freelist->first_free = (ShmAllocSetContext *) oldset->header.nextchild;
				freelist->num_free--;

				/* All that remains is to free the header/initial block */
				shmaset_dsa_free(ctl, oldset);
			}
			Assert(freelist->num_free == 0);
		}

		/* Now add the just-deleted context to the freelist. */
		set->header.nextchild = (MemoryContext) freelist->first_free;
		freelist->first_free = set;
		freelist->num_free++;

		SpinLockRelease(&freelist->lock);
		FREELIST_LOCK_STAT_END;
		return;
	}

	/* Free all blocks, except the keeper which is part of context header */
	while (block != NULL)
	{
		AllocBlock	next = block->next;

		if (block != set->keeper)
			context->mem_allocated -= block->endptr - ((char *) block);

#ifdef CLOBBER_FREED_MEMORY
		wipe_mem(block, block->freeptr - ((char *) block));
#endif

		if (block != set->keeper)
			shmaset_dsa_free(ctl, block);

		block = next;
	}

	/* Finally, free the context header, including the keeper block */
	shmaset_dsa_free(ctl, set);
}

/*
 * ShmAllocSetAlloc
 *		Returns pointer to allocated memory of given size or NULL if
 *		request could not be completed; memory is added to the set.
 *
 * No request may exceed:
 *		MAXALIGN_DOWN(SIZE_MAX) - ALLOC_BLOCKHDRSZ - ALLOC_CHUNKHDRSZ
 * All callers use a much-lower limit.
 *
 * Note: when using valgrind, it doesn't matter how the returned allocation
 * is marked, as mcxt.c will set it to UNDEFINED.  In some paths we will
 * return space that is marked NOACCESS - ShmAllocSetRealloc has to beware!
 */
static void *
ShmAllocSetAlloc(MemoryContext context, Size size)
{
	ShmAllocSet	set = (ShmAllocSet) context;
	AllocBlock	block;
	AllocChunk	chunk;
	int			fidx;
	Size		chunk_size;
	Size		blksize;

	/* POLAR */
	ShmAsetCtl	*ctl = set->ctl;

	AssertArg(ShmAllocSetIsValid(set));

	/*
	 * If requested size exceeds maximum for chunks, allocate an entire block
	 * for this request.
	 */
	if (size > set->allocChunkLimit)
	{
		chunk_size = MAXALIGN(size);
		blksize = chunk_size + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ;

		block = (AllocBlock) shmaset_dsa_alloc(ctl, blksize, set->header.name);
		if (block == NULL)
			return NULL;

		context->mem_allocated += blksize;

		block->aset = set;
		block->freeptr = block->endptr = ((char *) block) + blksize;

		chunk = (AllocChunk) (((char *) block) + ALLOC_BLOCKHDRSZ);
		chunk->aset = set;
		chunk->size = chunk_size;
		chunk->requested_size = size;
#ifdef MEMORY_CONTEXT_CHECKING
		/* set mark to catch clobber of "unused" space */
		if (size < chunk_size)
			set_sentinel(AllocChunkGetPointer(chunk), size);
#endif
#ifdef RANDOMIZE_ALLOCATED_MEMORY
		/* fill the allocated space with junk */
		randomize_mem((char *) AllocChunkGetPointer(chunk), size);
#endif

		/*
		 * Stick the new block underneath the active allocation block, if any,
		 * so that we don't lose the use of the space remaining therein.
		 */
		if (set->blocks != NULL)
		{
			block->prev = set->blocks;
			block->next = set->blocks->next;
			if (block->next)
				block->next->prev = block;
			set->blocks->next = block;
		}
		else
		{
			block->prev = NULL;
			block->next = NULL;
			set->blocks = block;
		}

		AllocAllocInfo(set, chunk);

		polar_memory_account_inc_allocated(set, chunk->size);

		/* Ensure any padding bytes are marked NOACCESS. */
		MEMDEBUG_MAKE_MEM_NOACCESS((char *) AllocChunkGetPointer(chunk) + size,
								   chunk_size - size);

		/* Disallow external access to private part of chunk header. */
		MEMDEBUG_MAKE_MEM_NOACCESS(chunk, ALLOCCHUNK_PRIVATE_LEN);

		return AllocChunkGetPointer(chunk);
	}

	/*
	 * Request is small enough to be treated as a chunk.  Look in the
	 * corresponding free list to see if there is a free chunk we could reuse.
	 * If one is found, remove it from the free list, make it again a member
	 * of the alloc set and return its data address.
	 */
	fidx = ShmAllocSetFreeIndex(size);
	chunk = set->freelist[fidx];
	if (chunk != NULL)
	{
		Assert(chunk->size >= size);

		set->freelist[fidx] = (AllocChunk) chunk->aset;

		chunk->aset = (void *) set;
		chunk->requested_size = size;

#ifdef MEMORY_CONTEXT_CHECKING
		/* set mark to catch clobber of "unused" space */
		if (size < chunk->size)
			set_sentinel(AllocChunkGetPointer(chunk), size);
#endif
#ifdef RANDOMIZE_ALLOCATED_MEMORY
		/* fill the allocated space with junk */
		randomize_mem((char *) AllocChunkGetPointer(chunk), size);
#endif

		AllocAllocInfo(set, chunk);

		polar_memory_account_inc_allocated(set, chunk->size);

		/* Ensure any padding bytes are marked NOACCESS. */
		MEMDEBUG_MAKE_MEM_NOACCESS((char *) AllocChunkGetPointer(chunk) + size,
								   chunk->size - size);

		/* Disallow external access to private part of chunk header. */
		MEMDEBUG_MAKE_MEM_NOACCESS(chunk, ALLOCCHUNK_PRIVATE_LEN);

		return AllocChunkGetPointer(chunk);
	}

	/*
	 * Choose the actual chunk size to allocate.
	 */
	chunk_size = (1 << ALLOC_MINBITS) << fidx;
	Assert(chunk_size >= size);

	/*
	 * If there is enough room in the active allocation block, we will put the
	 * chunk into that block.  Else must start a new one.
	 */
	if ((block = set->blocks) != NULL)
	{
		Size		availspace = block->endptr - block->freeptr;

		if (availspace < (chunk_size + ALLOC_CHUNKHDRSZ))
		{
			/*
			 * The existing active (top) block does not have enough room for
			 * the requested allocation, but it might still have a useful
			 * amount of space in it.  Once we push it down in the block list,
			 * we'll never try to allocate more space from it. So, before we
			 * do that, carve up its free space into chunks that we can put on
			 * the set's freelists.
			 *
			 * Because we can only get here when there's less than
			 * ALLOC_CHUNK_LIMIT left in the block, this loop cannot iterate
			 * more than ALLOCSET_NUM_FREELISTS-1 times.
			 */
			while (availspace >= ((1 << ALLOC_MINBITS) + ALLOC_CHUNKHDRSZ))
			{
				Size		availchunk = availspace - ALLOC_CHUNKHDRSZ;
				int			a_fidx = ShmAllocSetFreeIndex(availchunk);

				/*
				 * In most cases, we'll get back the index of the next larger
				 * freelist than the one we need to put this chunk on.  The
				 * exception is when availchunk is exactly a power of 2.
				 */
				if (availchunk != ((Size) 1 << (a_fidx + ALLOC_MINBITS)))
				{
					a_fidx--;
					Assert(a_fidx >= 0);
					availchunk = ((Size) 1 << (a_fidx + ALLOC_MINBITS));
				}

				chunk = (AllocChunk) (block->freeptr);

				/* Prepare to initialize the chunk header. */
				MEMDEBUG_MAKE_MEM_UNDEFINED(chunk, ALLOC_CHUNKHDRSZ);

				block->freeptr += (availchunk + ALLOC_CHUNKHDRSZ);
				availspace -= (availchunk + ALLOC_CHUNKHDRSZ);

				chunk->size = availchunk;
				chunk->requested_size = 0;	/* mark it free */
				chunk->aset = (void *) set->freelist[a_fidx];
				set->freelist[a_fidx] = chunk;
			}

			/* Mark that we need to create a new block */
			block = NULL;
		}
	}

	/*
	 * Time to create a new regular (multi-chunk) block?
	 */
	if (block == NULL)
	{
		Size		required_size;

		/*
		 * The first such block has size initBlockSize, and we double the
		 * space in each succeeding block, but not more than maxBlockSize.
		 */
		blksize = set->nextBlockSize;
		set->nextBlockSize <<= 1;
		if (set->nextBlockSize > set->maxBlockSize)
			set->nextBlockSize = set->maxBlockSize;

		/*
		 * If initBlockSize is less than ALLOC_CHUNK_LIMIT, we could need more
		 * space... but try to keep it a power of 2.
		 */
		required_size = chunk_size + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ;
		while (blksize < required_size)
			blksize <<= 1;

		/* Try to allocate it */
		block = (AllocBlock) shmaset_dsa_alloc(ctl, blksize, set->header.name);

		/*
		 * We could be asking for pretty big blocks here, so cope if malloc
		 * fails.  But give up if there's less than a meg or so available...
		 */
		while (block == NULL && blksize > 1024 * 1024)
		{
			blksize >>= 1;
			if (blksize < required_size)
				break;

			block = (AllocBlock) shmaset_dsa_alloc(ctl,
												   blksize,
												   set->header.name);
		}

		if (block == NULL)
			return NULL;

		context->mem_allocated += blksize;

		block->aset = set;
		block->freeptr = ((char *) block) + ALLOC_BLOCKHDRSZ;
		block->endptr = ((char *) block) + blksize;

		/* Mark unallocated space NOACCESS. */
		MEMDEBUG_MAKE_MEM_NOACCESS(block->freeptr,
								   blksize - ALLOC_BLOCKHDRSZ);

		block->prev = NULL;
		block->next = set->blocks;
		if (block->next)
			block->next->prev = block;
		set->blocks = block;
	}

	/*
	 * OK, do the allocation
	 */
	chunk = (AllocChunk) (block->freeptr);

	/* Prepare to initialize the chunk header. */
	MEMDEBUG_MAKE_MEM_UNDEFINED(chunk, ALLOC_CHUNKHDRSZ);

	block->freeptr += (chunk_size + ALLOC_CHUNKHDRSZ);
	Assert(block->freeptr <= block->endptr);

	chunk->aset = (void *) set;
	chunk->size = chunk_size;
	chunk->requested_size = size;
#ifdef MEMORY_CONTEXT_CHECKING
	/* set mark to catch clobber of "unused" space */
	if (size < chunk->size)
		set_sentinel(AllocChunkGetPointer(chunk), size);
#endif
#ifdef RANDOMIZE_ALLOCATED_MEMORY
	/* fill the allocated space with junk */
	randomize_mem((char *) AllocChunkGetPointer(chunk), size);
#endif

	AllocAllocInfo(set, chunk);

	polar_memory_account_inc_allocated(set, chunk->size);

	/* Ensure any padding bytes are marked NOACCESS. */
	MEMDEBUG_MAKE_MEM_NOACCESS((char *) AllocChunkGetPointer(chunk) + size,
							   chunk_size - size);

	/* Disallow external access to private part of chunk header. */
	MEMDEBUG_MAKE_MEM_NOACCESS(chunk, ALLOCCHUNK_PRIVATE_LEN);

	return AllocChunkGetPointer(chunk);
}

/*
 * ShmAllocSetFree
 *		Frees allocated memory; memory is removed from the set.
 */
static void
ShmAllocSetFree(MemoryContext context, void *pointer)
{
	ShmAllocSet	set = (ShmAllocSet) context;
	AllocChunk	chunk = AllocPointerGetChunk(pointer);

	/* Allow access to private part of chunk header. */
	MEMDEBUG_MAKE_MEM_DEFINED(chunk, ALLOCCHUNK_PRIVATE_LEN);

	AllocFreeInfo(set, chunk);

	polar_memory_account_dec_allocated(set, chunk->size);

#ifdef MEMORY_CONTEXT_CHECKING
	/* Test for someone scribbling on unused space in chunk */
	if (chunk->requested_size < chunk->size)
		if (!sentinel_ok(pointer, chunk->requested_size))
			elog(WARNING, "detected write past chunk end in %s %p",
				 set->header.name, chunk);
#endif

	if (chunk->size > set->allocChunkLimit)
	{
		/*
		 * Big chunks are certain to have been allocated as single-chunk
		 * blocks.  Just unlink that block and return it to malloc().
		 */
		AllocBlock	block = (AllocBlock) (((char *) chunk) - ALLOC_BLOCKHDRSZ);

		/*
		 * Try to verify that we have a sane block pointer: it should
		 * reference the correct aset, and freeptr and endptr should point
		 * just past the chunk.
		 */
		if (block->aset != set ||
			block->freeptr != block->endptr ||
			block->freeptr != ((char *) block) +
			(chunk->size + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ))
			elog(ERROR, "could not find block containing chunk %p", chunk);

		/* OK, remove block from aset's list and free it */
		if (block->prev)
			block->prev->next = block->next;
		else
			set->blocks = block->next;
		if (block->next)
			block->next->prev = block->prev;
		shmaset_dsa_free(set->ctl, block);
	}
	else
	{
		/* Normal case, put the chunk into appropriate freelist */
		int			fidx = ShmAllocSetFreeIndex(chunk->size);

		chunk->aset = (void *) set->freelist[fidx];

#ifdef CLOBBER_FREED_MEMORY
		wipe_mem(pointer, chunk->size);
#endif

#if defined(__SANITIZE_ADDRESS__)
		/* POLAR: Mark freed memory noaccess explicitly. */
		MEMDEBUG_MAKE_MEM_NOACCESS(pointer, chunk->size);
#endif

		/* Reset requested_size to 0 in chunks that are on freelist */
		chunk->requested_size = 0;
		set->freelist[fidx] = chunk;
	}
}

/*
 * ShmAllocSetRealloc
 *		Returns new pointer to allocated memory of given size or NULL if
 *		request could not be completed; this memory is added to the set.
 *		Memory associated with given pointer is copied into the new memory,
 *		and the old memory is freed.
 *
 * Without MEMORY_CONTEXT_CHECKING, we don't know the old request size.  This
 * makes our Valgrind client requests less-precise, hazarding false negatives.
 * (In principle, we could use VALGRIND_GET_VBITS() to rediscover the old
 * request size.)
 */
static void *
ShmAllocSetRealloc(MemoryContext context, void *pointer, Size size)
{
	ShmAllocSet	set = (ShmAllocSet) context;
	AllocChunk	chunk = AllocPointerGetChunk(pointer);
	Size		oldsize;

	/* Allow access to private part of chunk header. */
	MEMDEBUG_MAKE_MEM_DEFINED(chunk, ALLOCCHUNK_PRIVATE_LEN);

	oldsize = chunk->size;

#ifdef MEMORY_CONTEXT_CHECKING
	/* Test for someone scribbling on unused space in chunk */
	if (chunk->requested_size < oldsize)
		if (!sentinel_ok(pointer, chunk->requested_size))
			elog(WARNING, "detected write past chunk end in %s %p",
				 set->header.name, chunk);
#endif

	/*
	 * Chunk sizes are aligned to power of 2 in ShmAllocSetAlloc().  Maybe the
	 * allocated area already is >= the new size.  (In particular, we will
	 * fall out here if the requested size is a decrease.)
	 */
	if (oldsize >= size)
	{
#ifdef MEMORY_CONTEXT_CHECKING
		Size		oldrequest = chunk->requested_size;

#ifdef RANDOMIZE_ALLOCATED_MEMORY
		/* We can only fill the extra space if we know the prior request */
		if (size > oldrequest)
			randomize_mem((char *) pointer + oldrequest,
						  size - oldrequest);
#endif

		chunk->requested_size = size;

		/*
		 * If this is an increase, mark any newly-available part UNDEFINED.
		 * Otherwise, mark the obsolete part NOACCESS.
		 */
		if (size > oldrequest)
			MEMDEBUG_MAKE_MEM_UNDEFINED((char *) pointer + oldrequest,
										size - oldrequest);
		else
			MEMDEBUG_MAKE_MEM_NOACCESS((char *) pointer + size,
									   oldsize - size);

		/* set mark to catch clobber of "unused" space */
		if (size < oldsize)
			set_sentinel(pointer, size);
#else							/* !MEMORY_CONTEXT_CHECKING */

		chunk->requested_size = size;
		/*
		 * We don't have the information to determine whether we're growing
		 * the old request or shrinking it, so we conservatively mark the
		 * entire new allocation DEFINED.
		 */
		MEMDEBUG_MAKE_MEM_NOACCESS(pointer, oldsize);
		MEMDEBUG_MAKE_MEM_DEFINED(pointer, size);
#endif

		/* Disallow external access to private part of chunk header. */
		MEMDEBUG_MAKE_MEM_NOACCESS(chunk, ALLOCCHUNK_PRIVATE_LEN);

		return pointer;
	}
	else
	{
		/*
		 * Enlarge-a-small-chunk case.  We just do this by brute force, ie,
		 * allocate a new chunk and copy the data.  Since we know the existing
		 * data isn't huge, this won't involve any great memcpy expense, so
		 * it's not worth being smarter.  (At one time we tried to avoid
		 * memcpy when it was possible to enlarge the chunk in-place, but that
		 * turns out to misbehave unpleasantly for repeated cycles of
		 * palloc/repalloc/pfree: the eventually freed chunks go into the
		 * wrong freelist for the next initial palloc request, and so we leak
		 * memory indefinitely.  See pgsql-hackers archives for 2007-08-11.)
		 */
		AllocPointer newPointer;

		/* allocate new chunk */
		newPointer = ShmAllocSetAlloc((MemoryContext) set, size);

		/* leave immediately if request was not completed */
		if (newPointer == NULL)
		{
			/* Disallow external access to private part of chunk header. */
			MEMDEBUG_MAKE_MEM_NOACCESS(chunk, ALLOCCHUNK_PRIVATE_LEN);
			return NULL;
		}

		/*
		 * ShmAllocSetAlloc() may have returned a region that is still NOACCESS.
		 * Change it to UNDEFINED for the moment; memcpy() will then transfer
		 * definedness from the old allocation to the new.  If we know the old
		 * allocation, copy just that much.  Otherwise, make the entire old
		 * chunk defined to avoid errors as we copy the currently-NOACCESS
		 * trailing bytes.
		 */
		MEMDEBUG_MAKE_MEM_UNDEFINED(newPointer, size);
		oldsize = chunk->requested_size;
#ifndef MEMORY_CONTEXT_CHECKING
		MEMDEBUG_MAKE_MEM_DEFINED(pointer, oldsize);
#endif

		/* transfer existing data (certain to fit) */
		memcpy(newPointer, pointer, oldsize);

		/* free old chunk */
		ShmAllocSetFree((MemoryContext) set, pointer);

		return newPointer;
	}
}

/*
 * ShmAllocSetGetChunkSpace
 *		Given a currently-allocated chunk, determine the total space
 *		it occupies (including all memory-allocation overhead).
 */
static Size
ShmAllocSetGetChunkSpace(MemoryContext context, void *pointer)
{
	AllocChunk	chunk = AllocPointerGetChunk(pointer);
	Size		result;

	MEMDEBUG_MAKE_MEM_DEFINED(chunk, ALLOCCHUNK_PRIVATE_LEN);
	result = chunk->size + ALLOC_CHUNKHDRSZ;
	MEMDEBUG_MAKE_MEM_NOACCESS(chunk, ALLOCCHUNK_PRIVATE_LEN);
	return result;
}

static Size
polar_ShmAllocUsableSize(MemoryContext context, void *pointer)
{
	Size ret;
	ShmAllocSet	set = (ShmAllocSet) context;
	AllocChunk	chunk = AllocPointerGetChunk(pointer);

	MEMDEBUG_MAKE_MEM_DEFINED(chunk, ALLOCCHUNK_PRIVATE_LEN);

#ifdef MEMORY_CONTEXT_CHECKING
	/* Test for someone scribbling on unused space in chunk */
	if (chunk->requested_size < chunk->size)
		if (!sentinel_ok(pointer, chunk->requested_size))
			elog(ERROR, "detected write past chunk end in %s %p",
				 set->header.name, chunk);
#endif

	ret = chunk->requested_size;

	MEMDEBUG_MAKE_MEM_NOACCESS(chunk, ALLOCCHUNK_PRIVATE_LEN);

	return ret;
}

/*
 * ShmAllocSetIsEmpty
 *		Is an ShmAllocSet empty of any allocated space?
 */
static bool
ShmAllocSetIsEmpty(MemoryContext context)
{
	/*
	 * For now, we say "empty" only if the context is new or just reset. We
	 * could examine the freelists to determine if all space has been freed,
	 * but it's not really worth the trouble for present uses of this
	 * functionality.
	 */
	if (context->isReset)
		return true;
	return false;
}

/*
 * ShmAllocSetStats
 *		Compute stats about memory consumption of an ShmAllocSet.
 *
 * printfunc: if not NULL, pass a human-readable stats string to this.
 * passthru: pass this pointer through to printfunc.
 * totals: if not NULL, add stats about this context into *totals.
 */
static void
ShmAllocSetStats(MemoryContext context,
			  MemoryStatsPrintFunc printfunc, void *passthru,
			  MemoryContextCounters *totals)
{
	ShmAllocSet	set = (ShmAllocSet) context;
	Size		nblocks = 0;
	Size		freechunks = 0;
	Size		totalspace;
	Size		freespace = 0;
	AllocBlock	block;
	int			fidx;

	/* Include context header in totalspace */
	totalspace = MAXALIGN(sizeof(ShmAllocSetContext));

	for (block = set->blocks; block != NULL; block = block->next)
	{
		nblocks++;
		totalspace += block->endptr - ((char *) block);
		freespace += block->endptr - block->freeptr;
	}
	for (fidx = 0; fidx < ALLOCSET_NUM_FREELISTS; fidx++)
	{
		AllocChunk	chunk;

		for (chunk = set->freelist[fidx]; chunk != NULL;
			 chunk = (AllocChunk) chunk->aset)
		{
			freechunks++;
			freespace += chunk->size + ALLOC_CHUNKHDRSZ;
		}
	}

	if (printfunc)
	{
		char		stats_string[200];

		snprintf(stats_string, sizeof(stats_string),
				 "%zu total in %zd blocks; %zu free (%zd chunks); %zu used",
				 totalspace, nblocks, freespace, freechunks,
				 totalspace - freespace);
		printfunc(context, passthru, stats_string);
	}

	if (totals)
	{
		totals->nblocks += nblocks;
		totals->freechunks += freechunks;
		totals->totalspace += totalspace;
		totals->freespace += freespace;

		/* POLAR: Shared Server */
		dsa_lock_stat_add(&totals->dsa_lock_stat, dsa_get_lock_stat(polar_shm_aset_get_area(set->ctl->type)));
	}

}


#ifdef MEMORY_CONTEXT_CHECKING

/*
 * ShmAllocSetCheck
 *		Walk through chunks and check consistency of memory.
 *
 * NOTE: report errors as WARNING, *not* ERROR or FATAL.  Otherwise you'll
 * find yourself in an infinite loop when trouble occurs, because this
 * routine will be entered again when elog cleanup tries to release memory!
 */
static void
ShmAllocSetCheck(MemoryContext context)
{
	ShmAllocSet	set = (ShmAllocSet) context;
	const char *name = set->header.name;
	AllocBlock	prevblock;
	AllocBlock	block;

	for (prevblock = NULL, block = set->blocks;
		 block != NULL;
		 prevblock = block, block = block->next)
	{
		char	   *bpoz = ((char *) block) + ALLOC_BLOCKHDRSZ;
		long		blk_used = block->freeptr - bpoz;
		long		blk_data = 0;
		long		nchunks = 0;

		/*
		 * Empty block - empty can be keeper-block only
		 */
		if (!blk_used)
		{
			if (set->keeper != block)
				elog(WARNING, "problem in alloc set %s: empty block %p",
					 name, block);
		}

		/*
		 * Check block header fields
		 */
		if (block->aset != set ||
			block->prev != prevblock ||
			block->freeptr < bpoz ||
			block->freeptr > block->endptr)
			elog(WARNING, "problem in alloc set %s: corrupt header in block %p",
				 name, block);

		/*
		 * Chunk walker
		 */
		while (bpoz < block->freeptr)
		{
			AllocChunk	chunk = (AllocChunk) bpoz;
			Size		chsize,
						dsize;

			/* Allow access to private part of chunk header. */
			MEMDEBUG_MAKE_MEM_DEFINED(chunk, ALLOCCHUNK_PRIVATE_LEN);

			chsize = chunk->size;	/* aligned chunk size */
			dsize = chunk->requested_size;	/* real data */

			/*
			 * Check chunk size
			 */
			if (dsize > chsize)
				elog(WARNING, "problem in alloc set %s: req size > alloc size for chunk %p in block %p",
					 name, chunk, block);
			if (chsize < (1 << ALLOC_MINBITS))
				elog(WARNING, "problem in alloc set %s: bad size %zu for chunk %p in block %p",
					 name, chsize, chunk, block);

			/* single-chunk block? */
			if (chsize > set->allocChunkLimit &&
				chsize + ALLOC_CHUNKHDRSZ != blk_used)
				elog(WARNING, "problem in alloc set %s: bad single-chunk %p in block %p",
					 name, chunk, block);

			/*
			 * If chunk is allocated, check for correct aset pointer. (If it's
			 * free, the aset is the freelist pointer, which we can't check as
			 * easily...)  Note this is an incomplete test, since palloc(0)
			 * produces an allocated chunk with requested_size == 0.
			 */
			if (dsize > 0 && chunk->aset != (void *) set)
				elog(WARNING, "problem in alloc set %s: bogus aset link in block %p, chunk %p",
					 name, block, chunk);

			/*
			 * Check for overwrite of padding space in an allocated chunk.
			 */
			if (chunk->aset == (void *) set && dsize < chsize &&
				!sentinel_ok(chunk, ALLOC_CHUNKHDRSZ + dsize))
				elog(WARNING, "problem in alloc set %s: detected write past chunk end in block %p, chunk %p",
					 name, block, chunk);

			/*
			 * If chunk is allocated, disallow external access to private part
			 * of chunk header.
			 */
			if (chunk->aset == (void *) set)
				MEMDEBUG_MAKE_MEM_NOACCESS(chunk, ALLOCCHUNK_PRIVATE_LEN);

			blk_data += chsize;
			nchunks++;

			bpoz += ALLOC_CHUNKHDRSZ + chsize;
		}

		if ((blk_data + (nchunks * ALLOC_CHUNKHDRSZ)) != blk_used)
			elog(WARNING, "problem in alloc set %s: found inconsistent memory block %p",
				 name, block);
	}
}

#endif							/* MEMORY_CONTEXT_CHECKING */

/* POLAR: implement ShmAllocSet. */
#include "miscadmin.h"
#include "storage/shmem.h"
#include "storage/spin.h"

ShmAsetCtl	*shm_aset_ctl = NULL;
dsa_area	*local_area[SHM_ASET_TYPE_NUM];

Size
polar_shm_aset_ctl_size(void)
{
	Size	size = 0;

	if (!polar_shm_aset_enabled())
		return size;

	size = add_size(size, SHM_ASET_TYPE_NUM * sizeof(ShmAsetCtl));
	return size;
}

void
polar_shm_aset_ctl_init(void)
{
	bool	found;

	if (!polar_shm_aset_enabled())
		return;

	shm_aset_ctl = (ShmAsetCtl *) ShmemInitStruct("shm_aset_ctl",
												  polar_shm_aset_ctl_size(),
												  &found);
	if (!found)
	{
		int i;

		for (i = 0; i < SHM_ASET_TYPE_NUM; i++)
		{
			ShmAsetCtl *ctl = &shm_aset_ctl[i];
			int        j;

			ctl->magic = POLAR_SHM_ASET_MAGIC;
			ctl->type = i;
			memset(ctl->context_freelists, 0, FREELIST_COUNT * sizeof(ShmAsetFreeList));

			for (j = 0; j < FREELIST_COUNT; j++)
				SpinLockInit(&ctl->context_freelists[j].lock);
		}
	}
	else
		Assert(!IsUnderPostmaster);
}

/*
 * ShmAllocSetContextCreate
 * 		like AllocSetContextCreateExtended, create a new ShmAllocSet.
 *
 * parent: parent context, or NULL if top-level context
 * name: name of context (must be statically allocated)
 * minContextSize: minimum context size
 * initBlockSize: initial allocation block size
 * maxBlockSize: maximum allocation block size
 * ctl: shared memory and freelist into
 *
 * Most callers should abstract the context size parameters using a macro
 * such as ALLOCSET_DEFAULT_SIZES.
 */
MemoryContext
ShmAllocSetContextCreate(MemoryContext parent,
						 const char *name,
						 Size minContextSize,
						 Size initBlockSize,
						 Size maxBlockSize,
						 ShmAsetCtl *ctl)
{
	int			freeListIndex;
	Size		firstBlockSize;
	ShmAllocSet	set;
	AllocBlock	block;

	Assert(polar_shm_aset_enabled());

	/* Assert we padded AllocChunkData properly */
	StaticAssertStmt(ALLOC_CHUNKHDRSZ == MAXALIGN(ALLOC_CHUNKHDRSZ),
					 "sizeof(AllocChunkData) is not maxaligned");
	StaticAssertStmt(offsetof(AllocChunkData, aset) + sizeof(MemoryContext) ==
					 ALLOC_CHUNKHDRSZ,
					 "padding calculation in AllocChunkData is wrong");

	/*
	 * First, validate allocation parameters.  Once these were regular runtime
	 * test and elog's, but in practice Asserts seem sufficient because nobody
	 * varies their parameters at runtime.  We somewhat arbitrarily enforce a
	 * minimum 1K block size.
	 */
	Assert(initBlockSize == MAXALIGN(initBlockSize) &&
		   initBlockSize >= 1024);
	Assert(maxBlockSize == MAXALIGN(maxBlockSize) &&
		   maxBlockSize >= initBlockSize &&
		   AllocHugeSizeIsValid(maxBlockSize)); /* must be safe to double */
	Assert(minContextSize == 0 ||
		   (minContextSize == MAXALIGN(minContextSize) &&
			minContextSize >= 1024 &&
			minContextSize <= maxBlockSize));

	/*
	 * Check whether the parameters match either available freelist.  We do
	 * not need to demand a match of maxBlockSize.
	 */
	if (minContextSize == ALLOCSET_DEFAULT_MINSIZE &&
		initBlockSize == ALLOCSET_DEFAULT_INITSIZE)
		freeListIndex = 0;
	else if (minContextSize == ALLOCSET_SMALL_MINSIZE &&
			 initBlockSize == ALLOCSET_SMALL_INITSIZE)
		freeListIndex = 1;
	else
		freeListIndex = -1;

	/*
	 * If a suitable freelist entry exists, just recycle that context.
	 */
	if (freeListIndex >= 0)
	{
		ShmAsetFreeList *freelist = &ctl->context_freelists[freeListIndex];
		FREELIST_LOCK_STAT_INIT;

		FREELIST_LOCK_STAT_BEGIN;
		SpinLockAcquire(&freelist->lock);
		if (freelist->first_free != NULL)
		{
			/* Remove entry from freelist */
			set = freelist->first_free;
			freelist->first_free = (ShmAllocSet) set->header.nextchild;
			freelist->num_free--;
			SpinLockRelease(&freelist->lock);
			FREELIST_LOCK_STAT_END;

			/* Update its maxBlockSize; everything else should be OK */
			set->maxBlockSize = maxBlockSize;

			/* Reinitialize its header, installing correct name and parent */
			MemoryContextCreate((MemoryContext) set,
								T_ShmAllocSetContext,
								&AllocSetMethods,
								parent,
								name);

			((MemoryContext) set)->mem_allocated =
				set->keeper->endptr - ((char *) set);

			if (parent)
				set->accountingParent = ((ShmAllocSet) parent)->accountingParent;
			else
				set->accountingParent = set;
			
			set->localAllocated = 0;
			set->currentAllocated = 0;
			set->peakAllocated = 0;

			return (MemoryContext) set;
		}
		SpinLockRelease(&freelist->lock);
		FREELIST_LOCK_STAT_END;
	}

	/* Determine size of initial block */
	firstBlockSize = MAXALIGN(sizeof(ShmAllocSetContext)) +
		ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ;
	if (minContextSize != 0)
		firstBlockSize = Max(firstBlockSize, minContextSize);
	else
		firstBlockSize = Max(firstBlockSize, initBlockSize);

	/*
	 * Allocate the initial block.  Unlike other aset.c blocks, it starts with
	 * the context header and its block header follows that.
	 */
	set = (ShmAllocSet) shmaset_dsa_alloc(ctl, firstBlockSize, name);
	if (set == NULL)
		return NULL;

	/*
	 * Avoid writing code that can fail between here and MemoryContextCreate;
	 * we'd leak the header/initial block if we ereport in this stretch.
	 */

	/* Fill in the initial block's block header */
	block = (AllocBlock) (((char *) set) + MAXALIGN(sizeof(ShmAllocSetContext)));
	block->aset = set;
	block->freeptr = ((char *) block) + ALLOC_BLOCKHDRSZ;
	block->endptr = ((char *) set) + firstBlockSize;
	block->prev = NULL;
	block->next = NULL;

	/* Mark unallocated space NOACCESS; leave the block header alone. */
	MEMDEBUG_MAKE_MEM_NOACCESS(block->freeptr, block->endptr - block->freeptr);

	/* Remember block as part of block list */
	set->blocks = block;
	/* Mark block as not to be released at reset time */
	set->keeper = block;

	/* Finish filling in aset-specific parts of the context header */
	MemSetAligned(set->freelist, 0, sizeof(set->freelist));

	set->initBlockSize = initBlockSize;
	set->maxBlockSize = maxBlockSize;
	set->nextBlockSize = initBlockSize;
	set->freeListIndex = freeListIndex;

	/* POLAR */
	set->ctl = ctl;

	/*
	 * Compute the allocation chunk size limit for this context.  It can't be
	 * more than ALLOC_CHUNK_LIMIT because of the fixed number of freelists.
	 * If maxBlockSize is small then requests exceeding the maxBlockSize, or
	 * even a significant fraction of it, should be treated as large chunks
	 * too.  For the typical case of maxBlockSize a power of 2, the chunk size
	 * limit will be at most 1/8th maxBlockSize, so that given a stream of
	 * requests that are all the maximum chunk size we will waste at most
	 * 1/8th of the allocated space.
	 *
	 * We have to have allocChunkLimit a power of two, because the requested
	 * and actually-allocated sizes of any chunk must be on the same side of
	 * the limit, else we get confused about whether the chunk is "big".
	 *
	 * Also, allocChunkLimit must not exceed ALLOCSET_SEPARATE_THRESHOLD.
	 */
	StaticAssertStmt(ALLOC_CHUNK_LIMIT == ALLOCSET_SEPARATE_THRESHOLD,
					 "ALLOC_CHUNK_LIMIT != ALLOCSET_SEPARATE_THRESHOLD");

	set->allocChunkLimit = ALLOC_CHUNK_LIMIT;
	while ((Size) (set->allocChunkLimit + ALLOC_CHUNKHDRSZ) >
		   (Size) ((maxBlockSize - ALLOC_BLOCKHDRSZ) / ALLOC_CHUNK_FRACTION))
		set->allocChunkLimit >>= 1;

	/* Finally, do the type-independent part of context creation */
	MemoryContextCreate((MemoryContext) set,
						T_ShmAllocSetContext,
						&AllocSetMethods,
						parent,
						name);

	/* POLAR px */
	if (parent)
		set->accountingParent = ((ShmAllocSet) parent)->accountingParent;
	else
		set->accountingParent = set;

	set->localAllocated = 0;
	set->currentAllocated = 0;
	set->peakAllocated = 0;
	/* POLAR end */

	((MemoryContext) set)->mem_allocated = firstBlockSize;

	if (parent)
		set->accountingParent = ((ShmAllocSet) parent)->accountingParent;
	else
		set->accountingParent = set;
	
	set->localAllocated = 0;
	set->currentAllocated = 0;
	set->peakAllocated = 0;

	return (MemoryContext) set;
}

/* POLAR px */
static void
polar_AllocSetDeclareAccountingRoot(MemoryContext context)
{
	ShmAllocSet	set = (ShmAllocSet) context;

	Assert(set->localAllocated == 0);

	set->accountingParent = set;
}

static Size
polar_AllocSetgetPeakUsageRecurse(MemoryContext parent, MemoryContext context)
{
	MemoryContext child;
	Size		total;

	total = 0;
	for (child = context->firstchild;
		 child != NULL;
		 child = child->nextchild)
	{
		ShmAllocSet	childset = (ShmAllocSet) child;

		if (childset->accountingParent == (ShmAllocSet) parent)
			polar_AllocSetgetPeakUsageRecurse(parent, child);
		else
			total += polar_AllocSetGetPeakUsage(child);
	}

	return total;
}

static Size
polar_AllocSetGetPeakUsage(MemoryContext context)
{
	ShmAllocSet	set = (ShmAllocSet) context;
	Size		total;

	total = set->peakAllocated;

	total += polar_AllocSetgetPeakUsageRecurse(context, context);

	return total;
}
/* POLAR end */

static void *
shmaset_dsa_alloc(ShmAsetCtl *ctl, size_t size, const char *name)
{
	dsa_pointer dsa_p = dsa_allocate_extended(polar_shm_aset_get_area(ctl->type), size, DSA_ALLOC_NO_OOM);
	if (!DsaPointerIsValid(dsa_p))
	{
		ereport(LOG,
				(errcode(ERRCODE_OUT_OF_MEMORY),
					errmsg("out of memory"),
					errdetail("Failed while creating ShmAllocSet \"%s\", %lu.", name, size)));
		return NULL;
	}

	return dsaptr_to_rawptr(dsa_p, ctl->base);
}
