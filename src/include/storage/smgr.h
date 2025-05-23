/*-------------------------------------------------------------------------
 *
 * smgr.h
 *	  storage manager switch public interface declarations.
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/smgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SMGR_H
#define SMGR_H

#include "lib/ilist.h"
#include "storage/block.h"
#include "storage/relfilenode.h"

/* POLAR */
#include "storage/polar_rsc.h"

/*
 * smgr.c maintains a table of SMgrRelation objects, which are essentially
 * cached file handles.  An SMgrRelation is created (if not already present)
 * by smgropen(), and destroyed by smgrdestroy().  Note that neither of these
 * operations imply I/O, they just create or destroy a hashtable entry.  (But
 * smgrdestroy() may release associated resources, such as OS-level file
 * descriptors.)
 *
 * An SMgrRelation may be "pinned", to prevent it from being destroyed while
 * it's in use.  We use this to prevent pointers relcache to smgr from being
 * invalidated.  SMgrRelations that are not pinned are deleted at end of
 * transaction.
 */
typedef struct SMgrRelationData
{
	/* rnode is the hashtable lookup key, so it must be first! */
	RelFileNodeBackend smgr_rnode;	/* relation physical identifier */

	/* pointer to shared object, valid if non-NULL and generation matches */
	struct polar_rsc_shared_relation_t *rsc_ref;
	uint64		rsc_generation;

	/*
	 * The following fields are reset to InvalidBlockNumber upon a cache flush
	 * event, and hold the last known size for each fork.  This information is
	 * currently only reliable during recovery, since there is no cache
	 * invalidation for fork extension.
	 */
	BlockNumber smgr_targblock; /* current insertion target block */
	BlockNumber smgr_cached_nblocks[MAX_FORKNUM + 1];	/* last known size */

	/* additional public fields may someday exist here */

	/*
	 * Fields below here are intended to be private to smgr.c and its
	 * submodules.  Do not touch them from elsewhere.
	 */
	int			smgr_which;		/* storage manager selector */

	/*
	 * for md.c; per-fork arrays of the number of open segments
	 * (md_num_open_segs) and the segments themselves (md_seg_fds).
	 */
	int			md_num_open_segs[MAX_FORKNUM + 1];
	struct _MdfdVec *md_seg_fds[MAX_FORKNUM + 1];

	/*
	 * Pinning support.  If unpinned (ie. pincount == 0), 'node' is a list
	 * link in list of all unpinned SMgrRelations.
	 */
	int			pincount;
	dlist_node	node;

	/* POLAR: bulk extend */
	bool		polar_flag_for_bulk_extend[MAX_FORKNUM + 1];
	BlockNumber polar_nblocks_faked_for_bulk_extend[MAX_FORKNUM + 1];
	/* POLAR end */
} SMgrRelationData;

typedef SMgrRelationData *SMgrRelation;

#define SmgrIsTemp(smgr) \
	RelFileNodeBackendIsTemp((smgr)->smgr_rnode)

extern void smgrinit(void);
extern SMgrRelation smgropen(RelFileNode rnode, BackendId backend);
extern bool smgrexists(SMgrRelation reln, ForkNumber forknum);
extern void smgrpin(SMgrRelation reln);
extern void smgrunpin(SMgrRelation reln);
extern void smgrclose(SMgrRelation reln);
extern void smgrdestroyall(void);
extern void smgrrelease(SMgrRelation reln);
extern void smgrreleaseall(void);
extern void smgrreleaserelnode(RelFileNodeBackend rnode);
extern void smgrcreate(SMgrRelation reln, ForkNumber forknum, bool isRedo);
extern void smgrdosyncall(SMgrRelation *rels, int nrels);
extern void smgrdounlinkall(SMgrRelation *rels, int nrels, bool isRedo);
extern void smgrextend(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber blocknum, const void *buffer, bool skipFsync);
extern void smgrzeroextend(SMgrRelation reln, ForkNumber forknum,
						   BlockNumber blocknum, int nblocks, bool skipFsync);
extern bool smgrprefetch(SMgrRelation reln, ForkNumber forknum,
						 BlockNumber blocknum);
extern void smgrread(SMgrRelation reln, ForkNumber forknum,
					 BlockNumber blocknum, void *buffer);
extern void smgrwrite(SMgrRelation reln, ForkNumber forknum,
					  BlockNumber blocknum, const void *buffer, bool skipFsync);
extern void smgrwriteback(SMgrRelation reln, ForkNumber forknum,
						  BlockNumber blocknum, BlockNumber nblocks);
extern BlockNumber smgrnblocks(SMgrRelation reln, ForkNumber forknum);
extern BlockNumber smgrnblocks_cached(SMgrRelation reln, ForkNumber forknum);
extern BlockNumber smgrnblocks_ensure_opened(SMgrRelation reln, ForkNumber forknum);
extern void smgrtruncate(SMgrRelation reln, ForkNumber *forknum, int nforks,
						 BlockNumber *nblocks);
extern void smgrtruncate2(SMgrRelation reln, ForkNumber *forknum, int nforks,
						  BlockNumber *old_nblocks,
						  BlockNumber *nblocks);
extern void smgrimmedsync(SMgrRelation reln, ForkNumber forknum);
extern void smgrregistersync(SMgrRelation reln, ForkNumber forknum);
extern void AtEOXact_SMgr(void);
extern bool ProcessBarrierSmgrRelease(void);

/* POLAR */
#define POLAR_ZERO_EXTEND_NONE		0
#define POLAR_ZERO_EXTEND_BULKWRITE	1
#define POLAR_ZERO_EXTEND_FALLOCATE	2

extern PGDLLIMPORT int polar_zero_extend_method;

extern void polar_smgrbulkread(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
							   int nblocks, void *buffer);
extern void polar_smgrbulkwrite(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
								int nblocks, const void *buffer, bool skipFsync);
extern void polar_smgrbulkextend(SMgrRelation reln, ForkNumber forknum,
								 BlockNumber blocknum, int nblocks, const void *buffer, bool skipFsync);
extern void polar_smgr_init_bulk_extend(SMgrRelation reln, ForkNumber forknum);
extern void polar_smgr_clear_bulk_extend(SMgrRelation reln, ForkNumber forknum);

/* POLAR end */

#endif							/* SMGR_H */
