/*-------------------------------------------------------------------------
 *
 * indexfsm.c
 *	  POSTGRES free space map for quickly finding free pages in relations
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/freespace/indexfsm.c
 *
 *
 * NOTES:
 *
 *	This is similar to the FSM used for heap, in freespace.c, but instead
 *	of tracking the amount of free space on pages, we only track whether
 *	pages are completely free or in-use. We use the same FSM implementation
 *	as for heaps, using BLCKSZ - 1 to denote used pages, and 0 for unused.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/freespace.h"
#include "storage/indexfsm.h"

/* POLAR */
#include "utils/rel.h"
#include "utils/guc.h"
#include "storage/bufmgr.h"

/*
 * Exported routines
 */

/*
 * GetFreeIndexPage - return a free page from the FSM
 *
 * As a side effect, the page is marked as used in the FSM.
 */
BlockNumber
GetFreeIndexPage(Relation rel)
{
	BlockNumber blkno = GetPageWithFreeSpace(rel, BLCKSZ / 2);

	if (blkno != InvalidBlockNumber)
		RecordUsedIndexPage(rel, blkno);

	return blkno;
}

/*
 * RecordFreeIndexPage - mark a page as free in the FSM
 */
void
RecordFreeIndexPage(Relation rel, BlockNumber freeBlock)
{
	RecordPageWithFreeSpace(rel, freeBlock, BLCKSZ - 1);
}


/*
 * RecordUsedIndexPage - mark a page as used in the FSM
 */
void
RecordUsedIndexPage(Relation rel, BlockNumber usedBlock)
{
	RecordPageWithFreeSpace(rel, usedBlock, 0);
}

/*
 * IndexFreeSpaceMapVacuum - scan and fix any inconsistencies in the FSM
 */
void
IndexFreeSpaceMapVacuum(Relation rel)
{
	FreeSpaceMapVacuum(rel);
}

/*
 * POLAR: index insert bulk extend. If we can not find free pages in index relation while
 * doing index insert, we will do index bulk extend. The free blocks will be registered
 * in FSM.
 */
Buffer
polar_index_add_extra_blocks_and_return_last_buffer(Relation reln, BlockNumber blockNum)
{
	BlockNumber first_block_num_extended = InvalidBlockNumber;
	int block_count = 0;
	Buffer last_buffer = InvalidBuffer;
	Buffer *buffers = NULL;
	int index = 0;
	char *bulk_buf_block = NULL;

	/* Open it at the smgr level if not already done */
	RelationOpenSmgr(reln);

	PG_TRY();
	{
		/* init bulk extend backend-local-variable */
		polar_smgr_init_bulk_extend(reln->rd_smgr, MAIN_FORKNUM);

		first_block_num_extended = reln->rd_smgr->polar_nblocks_faked_for_bulk_extend[MAIN_FORKNUM];
		block_count = Min(polar_index_bulk_extend_size, (BlockNumber) RELSEG_SIZE - (first_block_num_extended % ((BlockNumber) RELSEG_SIZE)));
		if (block_count < 1)
		{
			/*no cover line*/
			block_count = 1;
		}

		/* avoid small table bloat */
		if (first_block_num_extended < polar_min_bulk_extend_table_size)
			block_count = 1;

		bulk_buf_block = (char *)palloc0(block_count * BLCKSZ);
		buffers = (Buffer *)palloc0(block_count * sizeof(Buffer));

		for (index = 0; index < block_count; index++)
		{
			/*
			 * Extend by one page.  This should generally match the main-line
			 * extension code in RelationGetBufferForTuple, except that we hold
			 * the relation extension lock throughout.
			 */
			buffers[index] = ReadBufferExtended(reln, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
		}
	}
	/*no cover begin*/
	PG_CATCH();
	{
		/* error recovery, very important, reset bulk extend backend-local-variable */
		if (reln->rd_smgr != NULL)
			polar_smgr_clear_bulk_extend(reln->rd_smgr, MAIN_FORKNUM);
		PG_RE_THROW();
	}
	PG_END_TRY();
	/*no cover end*/

	/* reset bulk extend backend-local-variable */
	polar_smgr_clear_bulk_extend(reln->rd_smgr, MAIN_FORKNUM);

	/* bulk extend polar store */
	smgrextendbatch(reln->rd_smgr, MAIN_FORKNUM, first_block_num_extended, block_count, bulk_buf_block, false);
	pfree(bulk_buf_block);

	/* process left (block_count-1) blocks, skip last block*/
	block_count--;
	for (index = 0; index < block_count; index++)
	{
		Buffer buffer;
		Page page;

		buffer = buffers[index];
		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buffer);
		if (!PageIsNew(page))
		{
			/*no cover line*/
			elog(ERROR, "index bulk extend page %u of relation \"%s\" should be empty but is not",
				BufferGetBlockNumber(buffer),
				RelationGetRelationName(reln));
		}
		Assert((first_block_num_extended + index) == BufferGetBlockNumber(buffer));
		UnlockReleaseBuffer(buffer);

		/* We just register the free pages into FSM, no need to mark all the new buffers dirty */
		RecordFreeIndexPage(reln, first_block_num_extended + index);
	}

	/*
	 * Finally, vacuum the FSM. Update the upper-level FSM pages to ensure that
	 * searchers can find them.
	 */
	if (block_count > 0)
		IndexFreeSpaceMapVacuum(reln);

	/* last block */
	last_buffer = buffers[block_count];
	pfree(buffers);

	return last_buffer;
}
