/*-------------------------------------------------------------------------
 *
 * md.c
 *	  This code manages relations that reside on magnetic disk.
 *
 * Or at least, that was what the Berkeley folk had in mind when they named
 * this file.  In reality, what this code provides is an interface from
 * the smgr API to Unix-like filesystem APIs, so it will work with any type
 * of device for which the operating system provides filesystem support.
 * It doesn't matter whether the bits are on spinning rust or some other
 * storage technology.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/smgr/md.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/file.h>

#include "miscadmin.h"
#include "access/xlogutils.h"
#include "access/xlog.h"
#include "pgstat.h"
#include "portability/instr_time.h"
#include "postmaster/bgwriter.h"
#include "storage/fd.h"
#include "storage/bufmgr.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "pg_trace.h"

/* POLAR */
#include "access/polar_logindex_redo.h"
#include "storage/polar_fd.h"
#include "utils/faultinjector.h"
#include "utils/guc.h"

/* intervals for calling AbsorbFsyncRequests in mdsync and mdpostckpt */
#define FSYNCS_PER_ABSORB		10
#define UNLINKS_PER_ABSORB		10

/*
 * Special values for the segno arg to RememberFsyncRequest.
 *
 * Note that CompactCheckpointerRequestQueue assumes that it's OK to remove an
 * fsync request from the queue if an identical, subsequent request is found.
 * See comments there before making changes here.
 */
#define FORGET_RELATION_FSYNC	(InvalidBlockNumber)
#define FORGET_DATABASE_FSYNC	(InvalidBlockNumber-1)
#define UNLINK_RELATION_REQUEST (InvalidBlockNumber-2)

/*
 * On Windows, we have to interpret EACCES as possibly meaning the same as
 * ENOENT, because if a file is unlinked-but-not-yet-gone on that platform,
 * that's what you get.  Ugh.  This code is designed so that we don't
 * actually believe these cases are okay without further evidence (namely,
 * a pending fsync request getting canceled ... see mdsync).
 */
#ifndef WIN32
#define FILE_POSSIBLY_DELETED(err)	((err) == ENOENT)
#else
#define FILE_POSSIBLY_DELETED(err)	((err) == ENOENT || (err) == EACCES)
#endif

/*
 *	The magnetic disk storage manager keeps track of open file
 *	descriptors in its own descriptor pool.  This is done to make it
 *	easier to support relations that are larger than the operating
 *	system's file size limit (often 2GBytes).  In order to do that,
 *	we break relations up into "segment" files that are each shorter than
 *	the OS file size limit.  The segment size is set by the RELSEG_SIZE
 *	configuration constant in pg_config.h.
 *
 *	On disk, a relation must consist of consecutively numbered segment
 *	files in the pattern
 *		-- Zero or more full segments of exactly RELSEG_SIZE blocks each
 *		-- Exactly one partial segment of size 0 <= size < RELSEG_SIZE blocks
 *		-- Optionally, any number of inactive segments of size 0 blocks.
 *	The full and partial segments are collectively the "active" segments.
 *	Inactive segments are those that once contained data but are currently
 *	not needed because of an mdtruncate() operation.  The reason for leaving
 *	them present at size zero, rather than unlinking them, is that other
 *	backends and/or the checkpointer might be holding open file references to
 *	such segments.  If the relation expands again after mdtruncate(), such
 *	that a deactivated segment becomes active again, it is important that
 *	such file references still be valid --- else data might get written
 *	out to an unlinked old copy of a segment file that will eventually
 *	disappear.
 *
 *	File descriptors are stored in the per-fork md_seg_fds arrays inside
 *	SMgrRelation. The length of these arrays is stored in md_num_open_segs.
 *	Note that a fork's md_num_open_segs having a specific value does not
 *	necessarily mean the relation doesn't have additional segments; we may
 *	just not have opened the next segment yet.  (We could not have "all
 *	segments are in the array" as an invariant anyway, since another backend
 *	could extend the relation while we aren't looking.)  We do not have
 *	entries for inactive segments, however; as soon as we find a partial
 *	segment, we assume that any subsequent segments are inactive.
 *
 *	The entire MdfdVec array is palloc'd in the MdCxt memory context.
 */

typedef struct _MdfdVec
{
	File		mdfd_vfd;		/* fd number in fd.c's pool */
	BlockNumber mdfd_segno;		/* segment number, from 0 */
} MdfdVec;

static MemoryContext MdCxt;		/* context for all MdfdVec objects */


/*
 * In some contexts (currently, standalone backends and the checkpointer)
 * we keep track of pending fsync operations: we need to remember all relation
 * segments that have been written since the last checkpoint, so that we can
 * fsync them down to disk before completing the next checkpoint.  This hash
 * table remembers the pending operations.  We use a hash table mostly as
 * a convenient way of merging duplicate requests.
 *
 * We use a similar mechanism to remember no-longer-needed files that can
 * be deleted after the next checkpoint, but we use a linked list instead of
 * a hash table, because we don't expect there to be any duplicate requests.
 *
 * These mechanisms are only used for non-temp relations; we never fsync
 * temp rels, nor do we need to postpone their deletion (see comments in
 * mdunlink).
 *
 * (Regular backends do not track pending operations locally, but forward
 * them to the checkpointer.)
 */
typedef uint16 CycleCtr;		/* can be any convenient integer size */

typedef struct
{
	RelFileNode rnode;			/* hash table key (must be first!) */
	CycleCtr	cycle_ctr;		/* mdsync_cycle_ctr of oldest request */
	/* requests[f] has bit n set if we need to fsync segment n of fork f */
	Bitmapset  *requests[MAX_FORKNUM + 1];
	/* canceled[f] is true if we canceled fsyncs for fork "recently" */
	bool		canceled[MAX_FORKNUM + 1];
} PendingOperationEntry;

typedef struct
{
	RelFileNode rnode;			/* the dead relation to delete */
	CycleCtr	cycle_ctr;		/* mdckpt_cycle_ctr when request was made */
} PendingUnlinkEntry;

static HTAB *pendingOpsTable = NULL;
static List *pendingUnlinks = NIL;
static MemoryContext pendingOpsCxt; /* context for the above  */

static CycleCtr mdsync_cycle_ctr = 0;
static CycleCtr mdckpt_cycle_ctr = 0;


/*** behavior for mdopen & _mdfd_getseg ***/
/* ereport if segment not present */
#define EXTENSION_FAIL				(1 << 0)
/* return NULL if segment not present */
#define EXTENSION_RETURN_NULL		(1 << 1)
/* create new segments as needed */
#define EXTENSION_CREATE			(1 << 2)
/* create new segments if needed during recovery */
#define EXTENSION_CREATE_RECOVERY	(1 << 3)
/*
 * Allow opening segments which are preceded by segments smaller than
 * RELSEG_SIZE, e.g. inactive segments (see above). Note that this is breaks
 * mdnblocks() and related functionality henceforth - which currently is ok,
 * because this is only required in the checkpointer which never uses
 * mdnblocks().
 */
#define EXTENSION_DONT_CHECK_SIZE	(1 << 4)


/* local routines */
static void mdunlinkfork(RelFileNodeBackend rnode, ForkNumber forkNum,
			 bool isRedo);
static MdfdVec *mdopen(SMgrRelation reln, ForkNumber forknum, int behavior);
static void register_dirty_segment(SMgrRelation reln, ForkNumber forknum,
					   MdfdVec *seg);
static void register_unlink(RelFileNodeBackend rnode);
static void _fdvec_resize(SMgrRelation reln,
			  ForkNumber forknum,
			  int nseg);
static char *_mdfd_segpath(SMgrRelation reln, ForkNumber forknum,
			  BlockNumber segno);
static MdfdVec *_mdfd_openseg(SMgrRelation reln, ForkNumber forkno,
			  BlockNumber segno, int oflags);
static MdfdVec *_mdfd_getseg(SMgrRelation reln, ForkNumber forkno,
			 BlockNumber blkno, bool skipFsync, int behavior);
static BlockNumber _mdnblocks(SMgrRelation reln, ForkNumber forknum,
		   MdfdVec *seg);
static BlockNumber polar_cache_mdnblocks(SMgrRelation reln, ForkNumber forknum, MdfdVec *seg);


/*
 *	mdinit() -- Initialize private state for magnetic disk storage manager.
 */
void
mdinit(void)
{
	MdCxt = AllocSetContextCreate(TopMemoryContext,
								  "MdSmgr",
								  ALLOCSET_DEFAULT_SIZES);

	/*
	 * Create pending-operations hashtable if we need it.  Currently, we need
	 * it if we are standalone (not under a postmaster) or if we are a startup
	 * or checkpointer auxiliary process.
	 */
	if (!IsUnderPostmaster || AmStartupProcess() || AmCheckpointerProcess())
	{
		HASHCTL		hash_ctl;

		/*
		 * XXX: The checkpointer needs to add entries to the pending ops table
		 * when absorbing fsync requests.  That is done within a critical
		 * section, which isn't usually allowed, but we make an exception. It
		 * means that there's a theoretical possibility that you run out of
		 * memory while absorbing fsync requests, which leads to a PANIC.
		 * Fortunately the hash table is small so that's unlikely to happen in
		 * practice.
		 */
		pendingOpsCxt = AllocSetContextCreate(MdCxt,
											  "Pending ops context",
											  ALLOCSET_DEFAULT_SIZES);
		MemoryContextAllowInCriticalSection(pendingOpsCxt, true);

		MemSet(&hash_ctl, 0, sizeof(hash_ctl));
		hash_ctl.keysize = sizeof(RelFileNode);
		hash_ctl.entrysize = sizeof(PendingOperationEntry);
		hash_ctl.hcxt = pendingOpsCxt;
		pendingOpsTable = hash_create("Pending Ops Table",
									  100L,
									  &hash_ctl,
									  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
		pendingUnlinks = NIL;
	}

	polar_env_init();
}

/*
 * In archive recovery, we rely on checkpointer to do fsyncs, but we will have
 * already created the pendingOpsTable during initialization of the startup
 * process.  Calling this function drops the local pendingOpsTable so that
 * subsequent requests will be forwarded to checkpointer.
 */
void
SetForwardFsyncRequests(void)
{
	/* Perform any pending fsyncs we may have queued up, then drop table */
	if (pendingOpsTable)
	{
		mdsync();
		hash_destroy(pendingOpsTable);
	}
	pendingOpsTable = NULL;

	/*
	 * We should not have any pending unlink requests, since mdunlink doesn't
	 * queue unlink requests when isRedo.
	 */
	Assert(pendingUnlinks == NIL);
}

/*
 *	mdexists() -- Does the physical file exist?
 *
 * Note: this will return true for lingering files, with pending deletions
 */
bool
mdexists(SMgrRelation reln, ForkNumber forkNum)
{
	/*
	 * Close it first, to ensure that we notice if the fork has been unlinked
	 * since we opened it.
	 */
	mdclose(reln, forkNum);

	return (mdopen(reln, forkNum, EXTENSION_RETURN_NULL) != NULL);
}

/*
 *	mdcreate() -- Create a new relation on magnetic disk.
 *
 * If isRedo is true, it's okay for the relation to exist already.
 */
void
mdcreate(SMgrRelation reln, ForkNumber forkNum, bool isRedo)
{
	MdfdVec    *mdfd;
	char	   *path;
	File		fd;

	if (isRedo && reln->md_num_open_segs[forkNum] > 0)
		return;					/* created and opened already... */

	Assert(reln->md_num_open_segs[forkNum] == 0);

	path = relpath(reln->smgr_rnode, forkNum);

	fd = polar_path_name_open_file(path, O_RDWR | O_CREAT | O_EXCL | PG_BINARY);

	if (fd < 0)
	{
		int			save_errno = errno;

		/*
		 * During bootstrap, there are cases where a system relation will be
		 * accessed (by internal backend processes) before the bootstrap
		 * script nominally creates it.  Therefore, allow the file to exist
		 * already, even if isRedo is not set.  (See also mdopen)
		 */
		if (isRedo || IsBootstrapProcessingMode())
			fd = polar_path_name_open_file(path, O_RDWR | PG_BINARY);
		if (fd < 0)
		{
			/* be sure to report the error reported by create, not open */
			errno = save_errno;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not create file \"%s\": %m", path)));
		}
	}

	pfree(path);

	_fdvec_resize(reln, forkNum, 1);
	mdfd = &reln->md_seg_fds[forkNum][0];
	mdfd->mdfd_vfd = fd;
	mdfd->mdfd_segno = 0;
}

/*
 *	mdunlink() -- Unlink a relation.
 *
 * Note that we're passed a RelFileNodeBackend --- by the time this is called,
 * there won't be an SMgrRelation hashtable entry anymore.
 *
 * forkNum can be a fork number to delete a specific fork, or InvalidForkNumber
 * to delete all forks.
 *
 * For regular relations, we don't unlink the first segment file of the rel,
 * but just truncate it to zero length, and record a request to unlink it after
 * the next checkpoint.  Additional segments can be unlinked immediately,
 * however.  Leaving the empty file in place prevents that relfilenode
 * number from being reused.  The scenario this protects us from is:
 * 1. We delete a relation (and commit, and actually remove its file).
 * 2. We create a new relation, which by chance gets the same relfilenode as
 *	  the just-deleted one (OIDs must've wrapped around for that to happen).
 * 3. We crash before another checkpoint occurs.
 * During replay, we would delete the file and then recreate it, which is fine
 * if the contents of the file were repopulated by subsequent WAL entries.
 * But if we didn't WAL-log insertions, but instead relied on fsyncing the
 * file after populating it (as for instance CLUSTER and CREATE INDEX do),
 * the contents of the file would be lost forever.  By leaving the empty file
 * until after the next checkpoint, we prevent reassignment of the relfilenode
 * number until it's safe, because relfilenode assignment skips over any
 * existing file.
 *
 * We do not need to go through this dance for temp relations, though, because
 * we never make WAL entries for temp rels, and so a temp rel poses no threat
 * to the health of a regular rel that has taken over its relfilenode number.
 * The fact that temp rels and regular rels have different file naming
 * patterns provides additional safety.
 *
 * All the above applies only to the relation's main fork; other forks can
 * just be removed immediately, since they are not needed to prevent the
 * relfilenode number from being recycled.  Also, we do not carefully
 * track whether other forks have been created or not, but just attempt to
 * unlink them unconditionally; so we should never complain about ENOENT.
 *
 * If isRedo is true, it's unsurprising for the relation to be already gone.
 * Also, we should remove the file immediately instead of queuing a request
 * for later, since during redo there's no possibility of creating a
 * conflicting relation.
 *
 * Note: any failure should be reported as WARNING not ERROR, because
 * we are usually not in a transaction anymore when this is called.
 */
void
mdunlink(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo)
{
	/*
	 * We have to clean out any pending fsync requests for the doomed
	 * relation, else the next mdsync() will fail.  There can't be any such
	 * requests for a temp relation, though.  We can send just one request
	 * even when deleting multiple forks, since the fsync queuing code accepts
	 * the "InvalidForkNumber = all forks" convention.
	 */
	if (!RelFileNodeBackendIsTemp(rnode))
		ForgetRelationFsyncRequests(rnode.node, forkNum);

	/* Now do the per-fork work */
	if (forkNum == InvalidForkNumber)
	{
		for (forkNum = 0; forkNum <= MAX_FORKNUM; forkNum++)
			mdunlinkfork(rnode, forkNum, isRedo);
	}
	else
		mdunlinkfork(rnode, forkNum, isRedo);
}

static void
mdunlinkfork(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo)
{
	char	   *path;
	int			ret;

	path = relpath(rnode, forkNum);

	/*
	 * Delete or truncate the first segment.
	 */
	if (isRedo || forkNum != MAIN_FORKNUM || RelFileNodeBackendIsTemp(rnode))
	{
		ret = polar_unlink(path);
		if (ret < 0 && errno != ENOENT)
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("could not remove file \"%s\": %m", path)));
	}
	else
	{
		/* truncate(2) would be easier here, but Windows hasn't got it */
		int			fd;

		fd = polar_open_transient_file(path, O_RDWR | PG_BINARY);
		if (fd >= 0)
		{
			int			save_errno;

			ret = polar_ftruncate(fd, 0);
			save_errno = errno;
			CloseTransientFile(fd);
			errno = save_errno;
		}
		else
			ret = -1;
		if (ret < 0 && errno != ENOENT)
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("could not truncate file \"%s\": %m", path)));

		/* Register request to unlink first segment later */
		register_unlink(rnode);
	}

	/*
	 * Delete any additional segments.
	 */
	if (ret >= 0)
	{
		char	   *segpath = (char *) palloc(strlen(path) + 12);
		BlockNumber segno;

		/*
		 * Note that because we loop until getting ENOENT, we will correctly
		 * remove all inactive segments as well as active ones.
		 */
		for (segno = 1;; segno++)
		{
			sprintf(segpath, "%s.%u", path, segno);
			if (polar_unlink(segpath) < 0)
			{
				/* ENOENT is expected after the last segment... */
				if (errno != ENOENT)
					ereport(WARNING,
							(errcode_for_file_access(),
							 errmsg("could not remove file \"%s\": %m", segpath)));
				break;
			}
		}
		pfree(segpath);
	}

	pfree(path);
}

/*
 *	mdextend() -- Add a block to the specified relation.
 *
 *		The semantics are nearly the same as mdwrite(): write at the
 *		specified position.  However, this is to be used for the case of
 *		extending a relation (i.e., blocknum is at or beyond the current
 *		EOF).  Note that we assume writing a block beyond current EOF
 *		causes intervening file space to become filled with zeroes.
 */
void
mdextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		 char *buffer, bool skipFsync)
{
	off_t		seekpos;
	int			nbytes = 0;
	MdfdVec    *v;

	/* POLAR: bulk extend */
	if (polar_smgr_being_bulk_extend(reln, forknum) == true)
	{
		Assert(reln->polar_nblocks_faked_for_bulk_extend[forknum] == blocknum);
		++reln->polar_nblocks_faked_for_bulk_extend[forknum];
		return;
	}
	/* POLAR end */

	/* This assert is too expensive to have on normally ... */
#ifdef CHECK_WRITE_VS_EXTEND
	Assert(blocknum >= mdnblocks(reln, forknum, false));
#endif

	/*
	 * If a relation manages to grow to 2^32-1 blocks, refuse to extend it any
	 * more --- we mustn't create a block whose number actually is
	 * InvalidBlockNumber.
	 */
	if (blocknum == InvalidBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("cannot extend file \"%s\" beyond %u blocks",
						relpath(reln->smgr_rnode, forknum),
						InvalidBlockNumber)));

	v = _mdfd_getseg(reln, forknum, blocknum, skipFsync, EXTENSION_CREATE);

	seekpos = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));

	Assert(seekpos < (off_t) BLCKSZ * RELSEG_SIZE);

	/* POLAR: use pwrite to replace lseek + write */
	if (POLAR_ENABLE_PWRITE())
	{
		nbytes = polar_file_pwrite(v->mdfd_vfd, buffer, BLCKSZ, seekpos,
				WAIT_EVENT_DATA_FILE_PWRITE_EXTEND);
	}
	else
	{
		/*
		 * Note: because caller usually obtained blocknum by calling mdnblocks,
		 * which did a seek(SEEK_END), this seek is often redundant and will be
		 * optimized away by fd.c.  It's not redundant, however, if there is a
		 * partial page at the end of the file. In that case we want to try to
		 * overwrite the partial page with a full page.  It's also not redundant
		 * if bufmgr.c had to dump another buffer of the same file to make room
		 * for the new page's buffer.
		 */
		if (FileSeek(v->mdfd_vfd, seekpos, SEEK_SET) != seekpos)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not seek to block %u in file \"%s\": %m",
							blocknum, FilePathName(v->mdfd_vfd))));

		nbytes = FileWrite(v->mdfd_vfd, buffer, BLCKSZ, WAIT_EVENT_DATA_FILE_EXTEND);
	}

	if (nbytes != BLCKSZ)
	{
		if (nbytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not extend file \"%s\": %m",
							FilePathName(v->mdfd_vfd)),
					 errhint("Check free disk space.")));
			/* short write: complain appropriately */
			ereport(ERROR,
				(errcode(ERRCODE_DISK_FULL),
				 errmsg("could not extend file \"%s\": wrote only %d of %d bytes at block %u",
						FilePathName(v->mdfd_vfd),
						nbytes, BLCKSZ, blocknum),
				 errhint("Check free disk space.")));
	}

	if (!skipFsync && !SmgrIsTemp(reln))
		register_dirty_segment(reln, forknum, v);

	Assert(_mdnblocks(reln, forknum, v) <= ((BlockNumber) RELSEG_SIZE));
}

/*
 *	mdopen() -- Open the specified relation.
 *
 * Note we only open the first segment, when there are multiple segments.
 *
 * If first segment is not present, either ereport or return NULL according
 * to "behavior".  We treat EXTENSION_CREATE the same as EXTENSION_FAIL;
 * EXTENSION_CREATE means it's OK to extend an existing relation, not to
 * invent one out of whole cloth.
 */
static MdfdVec *
mdopen(SMgrRelation reln, ForkNumber forknum, int behavior)
{
	MdfdVec    *mdfd;
	char	   *path;
	File		fd;
	int			flags = PG_BINARY;

	/* POLAR: read datafile only need O_RDONLY flag in replica */
	if (polar_openfile_with_readonly_in_replica &&
		polar_in_replica_mode())
		flags |= O_RDONLY;
	else
		flags |= O_RDWR;

	/* No work if already open */
	if (reln->md_num_open_segs[forknum] > 0)
		return &reln->md_seg_fds[forknum][0];

	path = relpath(reln->smgr_rnode, forknum);

	fd = polar_path_name_open_file(path, flags);

	if (fd < 0)
	{
		/*
		 * During bootstrap, there are cases where a system relation will be
		 * accessed (by internal backend processes) before the bootstrap
		 * script nominally creates it.  Therefore, accept mdopen() as a
		 * substitute for mdcreate() in bootstrap mode only. (See mdcreate)
		 */
		if (IsBootstrapProcessingMode())
		{
			Assert(!polar_in_replica_mode());
			fd = polar_path_name_open_file(path, O_RDWR | O_CREAT | O_EXCL | PG_BINARY);
		}
		if (fd < 0)
		{
			if ((behavior & EXTENSION_RETURN_NULL) &&
				FILE_POSSIBLY_DELETED(errno))
			{
				pfree(path);
				return NULL;
			}
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\": %m", path)));
		}
	}

	pfree(path);

	_fdvec_resize(reln, forknum, 1);
	mdfd = &reln->md_seg_fds[forknum][0];
	mdfd->mdfd_vfd = fd;
	mdfd->mdfd_segno = 0;

	Assert(_mdnblocks(reln, forknum, mdfd) <= ((BlockNumber) RELSEG_SIZE));

	return mdfd;
}

/*
 *	mdclose() -- Close the specified relation, if it isn't closed already.
 */
void
mdclose(SMgrRelation reln, ForkNumber forknum)
{
	int			nopensegs = reln->md_num_open_segs[forknum];

	/* No work if already closed */
	if (nopensegs == 0)
		return;

	/* close segments starting from the end */
	while (nopensegs > 0)
	{
		MdfdVec    *v = &reln->md_seg_fds[forknum][nopensegs - 1];

		FileClose(v->mdfd_vfd);
		_fdvec_resize(reln, forknum, nopensegs - 1);
		nopensegs--;
	}
}

/*
 *	mdprefetch() -- Initiate asynchronous read of the specified block of a relation
 */
void
mdprefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
#ifdef USE_PREFETCH
	off_t		seekpos;
	MdfdVec    *v;

	v = _mdfd_getseg(reln, forknum, blocknum, false, EXTENSION_FAIL);

	seekpos = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));

	Assert(seekpos < (off_t) BLCKSZ * RELSEG_SIZE);

	(void) FilePrefetch(v->mdfd_vfd, seekpos, BLCKSZ, WAIT_EVENT_DATA_FILE_PREFETCH);
#endif							/* USE_PREFETCH */
}

/*
 * mdwriteback() -- Tell the kernel to write pages back to storage.
 *
 * This accepts a range of blocks because flushing several pages at once is
 * considerably more efficient than doing so individually.
 */
void
mdwriteback(SMgrRelation reln, ForkNumber forknum,
			BlockNumber blocknum, BlockNumber nblocks)
{
	/*
	 * Issue flush requests in as few requests as possible; have to split at
	 * segment boundaries though, since those are actually separate files.
	 */
	while (nblocks > 0)
	{
		BlockNumber nflush = nblocks;
		off_t		seekpos;
		MdfdVec    *v;
		int			segnum_start,
					segnum_end;

		v = _mdfd_getseg(reln, forknum, blocknum, true /* not used */ ,
						 EXTENSION_RETURN_NULL);

		/*
		 * We might be flushing buffers of already removed relations, that's
		 * ok, just ignore that case.
		 */
		if (!v)
			return;

		/* compute offset inside the current segment */
		segnum_start = blocknum / RELSEG_SIZE;

		/* compute number of desired writes within the current segment */
		segnum_end = (blocknum + nblocks - 1) / RELSEG_SIZE;
		if (segnum_start != segnum_end)
			nflush = RELSEG_SIZE - (blocknum % ((BlockNumber) RELSEG_SIZE));

		Assert(nflush >= 1);
		Assert(nflush <= nblocks);

		seekpos = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));

		FileWriteback(v->mdfd_vfd, seekpos, (off_t) BLCKSZ * nflush, WAIT_EVENT_DATA_FILE_FLUSH);

		nblocks -= nflush;
		blocknum += nflush;
	}
}

/*
 *	mdread() -- Read the specified block from a relation.
 */
void
mdread(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
	   char *buffer)
{
	off_t		seekpos;
	int			nbytes;
	MdfdVec    *v;

	TRACE_POSTGRESQL_SMGR_MD_READ_START(forknum, blocknum,
										reln->smgr_rnode.node.spcNode,
										reln->smgr_rnode.node.dbNode,
										reln->smgr_rnode.node.relNode,
										reln->smgr_rnode.backend);

	v = _mdfd_getseg(reln, forknum, blocknum, false,
					 EXTENSION_FAIL | EXTENSION_CREATE_RECOVERY);

	seekpos = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));

	Assert(seekpos < (off_t) BLCKSZ * RELSEG_SIZE);

	if (POLAR_ENABLE_PREAD())
		nbytes = polar_file_pread(v->mdfd_vfd, buffer, BLCKSZ, seekpos, WAIT_EVENT_DATA_FILE_PREAD);
	else
	{
		if (FileSeek(v->mdfd_vfd, seekpos, SEEK_SET) != seekpos)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not seek to block %u in file \"%s\": %m",
							blocknum, FilePathName(v->mdfd_vfd))));

		nbytes = FileRead(v->mdfd_vfd, buffer, BLCKSZ, WAIT_EVENT_DATA_FILE_READ);
	}

	TRACE_POSTGRESQL_SMGR_MD_READ_DONE(forknum, blocknum,
									   reln->smgr_rnode.node.spcNode,
									   reln->smgr_rnode.node.dbNode,
									   reln->smgr_rnode.node.relNode,
									   reln->smgr_rnode.backend,
									   nbytes,
									   BLCKSZ);

	if (nbytes != BLCKSZ)
	{
		if (nbytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read block %u in file \"%s\": %m",
							blocknum, FilePathName(v->mdfd_vfd))));

		/*
		 * Short read: we are at or past EOF, or we read a partial block at
		 * EOF.  Normally this is an error; upper levels should never try to
		 * read a nonexistent block.  However, if zero_damaged_pages is ON or
		 * we are InRecovery, we should instead return zeroes without
		 * complaining.  This allows, for example, the case of trying to
		 * update a block that was later truncated away.
		 */
		/*
		 * POLAR: If it's in logindex background process it may replay xlog which block doesn't exist in the storage,
		 * like FPI or init page.So we should to init this buffer to be zero if it doesn't exist.
		 */
		if (zero_damaged_pages || InRecovery
				|| POLAR_IN_LOGINDEX_PARALLEL_REPLAY())
			MemSet(buffer, 0, BLCKSZ);
		else
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("could not read block %u in file \"%s\": read only %d of %d bytes",
							blocknum, FilePathName(v->mdfd_vfd),
							nbytes, BLCKSZ)));
	}
}

/*
 *  POLAR: bulk read
 *
 *	polar_mdbulkread() -- Read the specified continuous blocks from a relation.
 *
 *  Caller must ensure that the blockcount does not exceed the length of the relation file.
 */
void
polar_mdbulkread(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
				 int blockCount, char *buffer)
{
	off_t		seekpos;
	int			nbytes;
	MdfdVec    *v;
	int         amount = blockCount * BLCKSZ;

	TRACE_POSTGRESQL_SMGR_MD_READ_START(forknum, blocknum,
										reln->smgr_rnode.node.spcNode,
										reln->smgr_rnode.node.dbNode,
										reln->smgr_rnode.node.relNode,
										reln->smgr_rnode.backend);

	v = _mdfd_getseg(reln, forknum, blocknum, false,
					 EXTENSION_FAIL | EXTENSION_CREATE_RECOVERY);

	seekpos = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));

	Assert(seekpos < (off_t) BLCKSZ * RELSEG_SIZE);
	Assert(seekpos + (off_t) amount <= (off_t) BLCKSZ * RELSEG_SIZE);

	if (POLAR_ENABLE_PREAD())
		nbytes = polar_file_pread(v->mdfd_vfd, buffer, amount, seekpos, WAIT_EVENT_DATA_FILE_PREAD);
	else
	{
		if (FileSeek(v->mdfd_vfd, seekpos, SEEK_SET) != seekpos)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not seek to block %u in file \"%s\": %m",
							blocknum, FilePathName(v->mdfd_vfd))));

		nbytes = FileRead(v->mdfd_vfd, buffer, amount, WAIT_EVENT_DATA_FILE_READ);
	}

	TRACE_POSTGRESQL_SMGR_MD_READ_DONE(forknum, blocknum,
									   reln->smgr_rnode.node.spcNode,
									   reln->smgr_rnode.node.dbNode,
									   reln->smgr_rnode.node.relNode,
									   reln->smgr_rnode.backend,
									   nbytes,
									   amount);

	if (nbytes != amount)
	{
		if (nbytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read block %u in file \"%s\": %m",
							blocknum, FilePathName(v->mdfd_vfd))));

		/*
		 * Short read: we are at or past EOF, or we read a partial block at
		 * EOF.  Normally this is an error; upper levels should never try to
		 * read a nonexistent block.  However, if zero_damaged_pages is ON or
		 * we are InRecovery, we should instead return zeroes without
		 * complaining.  This allows, for example, the case of trying to
		 * update a block that was later truncated away.
		 */
		if (zero_damaged_pages || InRecovery)
		{
			/* only zero damaged_pages */
			int damaged_pages_start_offset = nbytes - nbytes % BLCKSZ;
			MemSet((char*)buffer + damaged_pages_start_offset, 0, amount - damaged_pages_start_offset);
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("could not bulk read block %u in file \"%s\": read only %d of %d bytes",
							blocknum, FilePathName(v->mdfd_vfd),
							nbytes, amount)));
	}
}
/* POLAR end */

/*
 *	mdwrite() -- Write the supplied block at the appropriate location.
 *
 *		This is to be used only for updating already-existing blocks of a
 *		relation (ie, those before the current EOF).  To extend a relation,
 *		use mdextend().
 */
void
mdwrite(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		char *buffer, bool skipFsync)
{
	off_t		seekpos;
	int			nbytes;
	MdfdVec    *v;

	/* This assert is too expensive to have on normally ... */
#ifdef CHECK_WRITE_VS_EXTEND
	Assert(blocknum < mdnblocks(reln, forknum, false));
#endif

	TRACE_POSTGRESQL_SMGR_MD_WRITE_START(forknum, blocknum,
										 reln->smgr_rnode.node.spcNode,
										 reln->smgr_rnode.node.dbNode,
										 reln->smgr_rnode.node.relNode,
										 reln->smgr_rnode.backend);

	v = _mdfd_getseg(reln, forknum, blocknum, skipFsync,
					 EXTENSION_FAIL | EXTENSION_CREATE_RECOVERY);

	seekpos = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));

	Assert(seekpos < (off_t) BLCKSZ * RELSEG_SIZE);

	/*no cover begin*/
#ifdef FAULT_INJECTOR
#define MAX_ATOM_WRITE_SIZE 4096
	if (SIMPLE_FAULT_INJECTOR("polar_partial_write_fault") == FaultInjectorTypeEnable)
	{
		/* POLAR: use pwrite to replace lseek + write */
		if (POLAR_ENABLE_PWRITE())
		{
			nbytes = polar_file_pwrite(v->mdfd_vfd, buffer, MAX_ATOM_WRITE_SIZE, seekpos,
				WAIT_EVENT_DATA_FILE_PWRITE);
		}
		else
		{
			if (FileSeek(v->mdfd_vfd, seekpos, SEEK_SET) != seekpos)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not seek to block %u in file \"%s\": %m",
								blocknum, FilePathName(v->mdfd_vfd))));

			nbytes = FileWrite(v->mdfd_vfd, buffer, MAX_ATOM_WRITE_SIZE, WAIT_EVENT_DATA_FILE_WRITE);
		}

		elog(PANIC, "The block ([%u, %u, %u]), %u, %u is partial written.",
					        reln->smgr_rnode.node.spcNode, reln->smgr_rnode.node.dbNode,
					        reln->smgr_rnode.node.relNode, forknum, blocknum);
	}
	/*no cover end*/
#endif

	/* POLAR: use pwrite to replace lseek + write */
	if (POLAR_ENABLE_PWRITE())
	{
		nbytes = polar_file_pwrite(v->mdfd_vfd, buffer, BLCKSZ, seekpos, 
			WAIT_EVENT_DATA_FILE_PWRITE);
	}
	else
	{
		if (FileSeek(v->mdfd_vfd, seekpos, SEEK_SET) != seekpos)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not seek to block %u in file \"%s\": %m",
							blocknum, FilePathName(v->mdfd_vfd))));

		nbytes = FileWrite(v->mdfd_vfd, buffer, BLCKSZ, WAIT_EVENT_DATA_FILE_WRITE);
	}

	TRACE_POSTGRESQL_SMGR_MD_WRITE_DONE(forknum, blocknum,
										reln->smgr_rnode.node.spcNode,
										reln->smgr_rnode.node.dbNode,
										reln->smgr_rnode.node.relNode,
										reln->smgr_rnode.backend,
										nbytes,
										BLCKSZ);

	if (nbytes != BLCKSZ)
	{
		if (nbytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write block %u in file \"%s\": %m",
							blocknum, FilePathName(v->mdfd_vfd))));
		/* short write: complain appropriately */
		ereport(ERROR,
				(errcode(ERRCODE_DISK_FULL),
				 errmsg("could not write block %u in file \"%s\": wrote only %d of %d bytes",
						blocknum,
						FilePathName(v->mdfd_vfd),
						nbytes, BLCKSZ),
				 errhint("Check free disk space.")));
	}

	if (!skipFsync && !SmgrIsTemp(reln))
		register_dirty_segment(reln, forknum, v);
}

/*
 *	POLAR: bulk write
 *	mdwrite() -- Write the supplied continuous block at the appropriate location.
 *
 *		This is to be used only for updating already-existing blocks of a
 *		relation (ie, those before the current EOF).  To extend a relation,
 *		use mdextend().
 */
void
polar_mdbulkwrite(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		int blockCount, char *buffer, bool skipFsync)
{
	off_t		seekpos;
	int			nbytes;
	MdfdVec    *v;
	int 		amount = blockCount * BLCKSZ;

	/* This assert is too expensive to have on normally ... */
#ifdef CHECK_WRITE_VS_EXTEND
	Assert(blocknum < mdnblocks(reln, forknum, false));
#endif

	TRACE_POSTGRESQL_SMGR_MD_WRITE_START(forknum, blocknum,
										 reln->smgr_rnode.node.spcNode,
										 reln->smgr_rnode.node.dbNode,
										 reln->smgr_rnode.node.relNode,
										 reln->smgr_rnode.backend);

	v = _mdfd_getseg(reln, forknum, blocknum, skipFsync,
					 EXTENSION_FAIL | EXTENSION_CREATE_RECOVERY);

	seekpos = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));

	Assert(seekpos < (off_t) BLCKSZ * RELSEG_SIZE);
	Assert(seekpos + (off_t) amount <= (off_t) BLCKSZ * RELSEG_SIZE);

	/* POLAR: use pwrite to replace lseek + write */
	if (POLAR_ENABLE_PWRITE())
	{
		nbytes = polar_file_pwrite(v->mdfd_vfd, buffer, amount, seekpos, 
			WAIT_EVENT_DATA_FILE_PWRITE);
	}
	else
	{
		if (FileSeek(v->mdfd_vfd, seekpos, SEEK_SET) != seekpos)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not seek to block %u in file \"%s\": %m",
							blocknum, FilePathName(v->mdfd_vfd))));

		nbytes = FileWrite(v->mdfd_vfd, buffer, amount, WAIT_EVENT_DATA_FILE_WRITE);
	}

	TRACE_POSTGRESQL_SMGR_MD_WRITE_DONE(forknum, blocknum,
									reln->smgr_rnode.node.spcNode,
									reln->smgr_rnode.node.dbNode,
									reln->smgr_rnode.node.relNode,
									reln->smgr_rnode.backend,
									nbytes,
									amount);

	if (nbytes != amount)
	{
		if (nbytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write block %u in file \"%s\": %m",
							blocknum, FilePathName(v->mdfd_vfd))));
		/* short write: complain appropriately */
		ereport(ERROR,
				(errcode(ERRCODE_DISK_FULL),
				 errmsg("could not write block %u in file \"%s\": wrote only %d of %d bytes",
						blocknum,
						FilePathName(v->mdfd_vfd),
						nbytes, BLCKSZ),
				 errhint("Check free disk space.")));
	}

	if (!skipFsync && !SmgrIsTemp(reln))
		register_dirty_segment(reln, forknum, v);
}


/*
 *	mdnblocks() -- Get the number of blocks stored in a relation.
 *
 *		Important side effect: all active segments of the relation are opened
 *		and added to the mdfd_seg_fds array.  If this routine has not been
 *		called, then only segments up to the last one actually touched
 *		are present in the array.
 */
BlockNumber
mdnblocks(SMgrRelation reln, ForkNumber forknum, bool polar_use_file_cache)
{
	MdfdVec    *v = NULL;
	BlockNumber nblocks;
	BlockNumber segno = 0;

	/* POLAR: bulk extend */
	if (polar_smgr_being_bulk_extend(reln, forknum) == true)
		return reln->polar_nblocks_faked_for_bulk_extend[forknum];
	/* POLAR end */

	v = mdopen(reln, forknum, EXTENSION_FAIL);

	/* mdopen has opened the first segment */
	Assert(reln->md_num_open_segs[forknum] > 0);

	/*
	 * Start from the last open segments, to avoid redundant seeks.  We have
	 * previously verified that these segments are exactly RELSEG_SIZE long,
	 * and it's useless to recheck that each time.
	 *
	 * NOTE: this assumption could only be wrong if another backend has
	 * truncated the relation.  We rely on higher code levels to handle that
	 * scenario by closing and re-opening the md fd, which is handled via
	 * relcache flush.  (Since the checkpointer doesn't participate in
	 * relcache flush, it could have segment entries for inactive segments;
	 * that's OK because the checkpointer never needs to compute relation
	 * size.)
	 */
	segno = reln->md_num_open_segs[forknum] - 1;
	v = &reln->md_seg_fds[forknum][segno];

	for (;;)
	{
		if (polar_use_file_cache)
			nblocks = polar_cache_mdnblocks(reln, forknum, v);
		else
			nblocks = _mdnblocks(reln, forknum, v);
		if (nblocks > ((BlockNumber) RELSEG_SIZE))
			elog(FATAL, "segment too big");
		if (nblocks < ((BlockNumber) RELSEG_SIZE))
			return (segno * ((BlockNumber) RELSEG_SIZE)) + nblocks;

		/*
		 * If segment is exactly RELSEG_SIZE, advance to next one.
		 */
		segno++;

		/*
		 * We used to pass O_CREAT here, but that's has the disadvantage that
		 * it might create a segment which has vanished through some operating
		 * system misadventure.  In such a case, creating the segment here
		 * undermines _mdfd_getseg's attempts to notice and report an error
		 * upon access to a missing segment.
		 */
		v = _mdfd_openseg(reln, forknum, segno, 0);
		if (v == NULL)
			return segno * ((BlockNumber) RELSEG_SIZE);
	}
}

/*
 *	mdtruncate() -- Truncate relation to specified number of blocks.
 */
void
mdtruncate(SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks)
{
	BlockNumber curnblk;
	BlockNumber priorblocks;
	int			curopensegs;

	/*
	 * NOTE: mdnblocks makes sure we have opened all active segments, so that
	 * truncation loop will get them all!
	 */
	curnblk = mdnblocks(reln, forknum, false);
	if (nblocks > curnblk)
	{
		/* Bogus request ... but no complaint if InRecovery */
		if (InRecovery)
			return;
		ereport(ERROR,
				(errmsg("could not truncate file \"%s\" to %u blocks: it's only %u blocks now",
						relpath(reln->smgr_rnode, forknum),
						nblocks, curnblk)));
	}
	if (nblocks == curnblk)
		return;					/* no work */

	/*
	 * Truncate segments, starting at the last one. Starting at the end makes
	 * managing the memory for the fd array easier, should there be errors.
	 */
	curopensegs = reln->md_num_open_segs[forknum];
	while (curopensegs > 0)
	{
		MdfdVec    *v;

		priorblocks = (curopensegs - 1) * RELSEG_SIZE;

		v = &reln->md_seg_fds[forknum][curopensegs - 1];

		if (priorblocks > nblocks)
		{
			/*
			 * This segment is no longer active. We truncate the file, but do
			 * not delete it, for reasons explained in the header comments.
			 */
			if (FileTruncate(v->mdfd_vfd, 0, WAIT_EVENT_DATA_FILE_TRUNCATE) < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not truncate file \"%s\": %m",
								FilePathName(v->mdfd_vfd))));

			if (!SmgrIsTemp(reln))
				register_dirty_segment(reln, forknum, v);

			/* we never drop the 1st segment */
			Assert(v != &reln->md_seg_fds[forknum][0]);

			FileClose(v->mdfd_vfd);
			_fdvec_resize(reln, forknum, curopensegs - 1);
		}
		else if (priorblocks + ((BlockNumber) RELSEG_SIZE) > nblocks)
		{
			/*
			 * This is the last segment we want to keep. Truncate the file to
			 * the right length. NOTE: if nblocks is exactly a multiple K of
			 * RELSEG_SIZE, we will truncate the K+1st segment to 0 length but
			 * keep it. This adheres to the invariant given in the header
			 * comments.
			 */
			BlockNumber lastsegblocks = nblocks - priorblocks;

			if (FileTruncate(v->mdfd_vfd, (off_t) lastsegblocks * BLCKSZ, WAIT_EVENT_DATA_FILE_TRUNCATE) < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not truncate file \"%s\" to %u blocks: %m",
								FilePathName(v->mdfd_vfd),
								nblocks)));
			if (!SmgrIsTemp(reln))
				register_dirty_segment(reln, forknum, v);
		}
		else
		{
			/*
			 * We still need this segment, so nothing to do for this and any
			 * earlier segment.
			 */
			break;
		}
		curopensegs--;
	}
}

/*
 *	mdimmedsync() -- Immediately sync a relation to stable storage.
 *
 * Note that only writes already issued are synced; this routine knows
 * nothing of dirty buffers that may exist inside the buffer manager.
 */
void
mdimmedsync(SMgrRelation reln, ForkNumber forknum)
{
	int			segno;

	/*
	 * NOTE: mdnblocks makes sure we have opened all active segments, so that
	 * fsync loop will get them all!
	 */
	mdnblocks(reln, forknum, false);

	segno = reln->md_num_open_segs[forknum];

	while (segno > 0)
	{
		MdfdVec    *v = &reln->md_seg_fds[forknum][segno - 1];

		if (FileSync(v->mdfd_vfd, WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC) < 0)
			ereport(data_sync_elevel(ERROR),
					(errcode_for_file_access(),
					 errmsg("could not fsync file \"%s\": %m",
							FilePathName(v->mdfd_vfd))));
		segno--;
	}
}

/*
 *	mdsync() -- Sync previous writes to stable storage.
 */
void
mdsync(void)
{
	static bool mdsync_in_progress = false;

	HASH_SEQ_STATUS hstat;
	PendingOperationEntry *entry;
	int			absorb_counter;

	/* Statistics on sync times */
	int			processed = 0;
	instr_time	sync_start,
				sync_end,
				sync_diff;
	uint64		elapsed;
	uint64		longest = 0;
	uint64		total_elapsed = 0;

	/*
	 * This is only called during checkpoints, and checkpoints should only
	 * occur in processes that have created a pendingOpsTable.
	 */
	if (!pendingOpsTable)
		elog(ERROR, "cannot sync without a pendingOpsTable");

	/*
	 * If we are in the checkpointer, the sync had better include all fsync
	 * requests that were queued by backends up to this point.  The tightest
	 * race condition that could occur is that a buffer that must be written
	 * and fsync'd for the checkpoint could have been dumped by a backend just
	 * before it was visited by BufferSync().  We know the backend will have
	 * queued an fsync request before clearing the buffer's dirtybit, so we
	 * are safe as long as we do an Absorb after completing BufferSync().
	 */
	AbsorbFsyncRequests();

	/*
	 * To avoid excess fsync'ing (in the worst case, maybe a never-terminating
	 * checkpoint), we want to ignore fsync requests that are entered into the
	 * hashtable after this point --- they should be processed next time,
	 * instead.  We use mdsync_cycle_ctr to tell old entries apart from new
	 * ones: new ones will have cycle_ctr equal to the incremented value of
	 * mdsync_cycle_ctr.
	 *
	 * In normal circumstances, all entries present in the table at this point
	 * will have cycle_ctr exactly equal to the current (about to be old)
	 * value of mdsync_cycle_ctr.  However, if we fail partway through the
	 * fsync'ing loop, then older values of cycle_ctr might remain when we
	 * come back here to try again.  Repeated checkpoint failures would
	 * eventually wrap the counter around to the point where an old entry
	 * might appear new, causing us to skip it, possibly allowing a checkpoint
	 * to succeed that should not have.  To forestall wraparound, any time the
	 * previous mdsync() failed to complete, run through the table and
	 * forcibly set cycle_ctr = mdsync_cycle_ctr.
	 *
	 * Think not to merge this loop with the main loop, as the problem is
	 * exactly that that loop may fail before having visited all the entries.
	 * From a performance point of view it doesn't matter anyway, as this path
	 * will never be taken in a system that's functioning normally.
	 */
	if (mdsync_in_progress)
	{
		/* prior try failed, so update any stale cycle_ctr values */
		hash_seq_init(&hstat, pendingOpsTable);
		while ((entry = (PendingOperationEntry *) hash_seq_search(&hstat)) != NULL)
		{
			entry->cycle_ctr = mdsync_cycle_ctr;
		}
	}

	/* Advance counter so that new hashtable entries are distinguishable */
	mdsync_cycle_ctr++;

	/* Set flag to detect failure if we don't reach the end of the loop */
	mdsync_in_progress = true;

	/* Now scan the hashtable for fsync requests to process */
	absorb_counter = FSYNCS_PER_ABSORB;
	hash_seq_init(&hstat, pendingOpsTable);
	while ((entry = (PendingOperationEntry *) hash_seq_search(&hstat)) != NULL)
	{
		ForkNumber	forknum;

		/*
		 * If the entry is new then don't process it this time; it might
		 * contain multiple fsync-request bits, but they are all new.  Note
		 * "continue" bypasses the hash-remove call at the bottom of the loop.
		 */
		if (entry->cycle_ctr == mdsync_cycle_ctr)
			continue;

		/* Else assert we haven't missed it */
		Assert((CycleCtr) (entry->cycle_ctr + 1) == mdsync_cycle_ctr);

		/*
		 * Scan over the forks and segments represented by the entry.
		 *
		 * The bitmap manipulations are slightly tricky, because we can call
		 * AbsorbFsyncRequests() inside the loop and that could result in
		 * bms_add_member() modifying and even re-palloc'ing the bitmapsets.
		 * So we detach it, but if we fail we'll merge it with any new
		 * requests that have arrived in the meantime.
		 */
		for (forknum = 0; forknum <= MAX_FORKNUM; forknum++)
		{
			Bitmapset  *requests = entry->requests[forknum];
			int			segno;

			entry->requests[forknum] = NULL;
			entry->canceled[forknum] = false;

			segno = -1;
			while ((segno = bms_next_member(requests, segno)) >= 0)
			{
				int			failures;

				/*
				 * If fsync is off then we don't have to bother opening the
				 * file at all.  (We delay checking until this point so that
				 * changing fsync on the fly behaves sensibly.)
				 */
				if (!enableFsync)
					continue;

				/*
				 * If in checkpointer, we want to absorb pending requests
				 * every so often to prevent overflow of the fsync request
				 * queue.  It is unspecified whether newly-added entries will
				 * be visited by hash_seq_search, but we don't care since we
				 * don't need to process them anyway.
				 */
				if (--absorb_counter <= 0)
				{
					AbsorbFsyncRequests();
					absorb_counter = FSYNCS_PER_ABSORB;
				}

				/*
				 * The fsync table could contain requests to fsync segments
				 * that have been deleted (unlinked) by the time we get to
				 * them. Rather than just hoping an ENOENT (or EACCES on
				 * Windows) error can be ignored, what we do on error is
				 * absorb pending requests and then retry.  Since mdunlink()
				 * queues a "cancel" message before actually unlinking, the
				 * fsync request is guaranteed to be marked canceled after the
				 * absorb if it really was this case. DROP DATABASE likewise
				 * has to tell us to forget fsync requests before it starts
				 * deletions.
				 */
				for (failures = 0;; failures++) /* loop exits at "break" */
				{
					SMgrRelation reln;
					MdfdVec    *seg;
					char	   *path;
					int			save_errno;

					/*
					 * Find or create an smgr hash entry for this relation.
					 * This may seem a bit unclean -- md calling smgr?	But
					 * it's really the best solution.  It ensures that the
					 * open file reference isn't permanently leaked if we get
					 * an error here. (You may say "but an unreferenced
					 * SMgrRelation is still a leak!" Not really, because the
					 * only case in which a checkpoint is done by a process
					 * that isn't about to shut down is in the checkpointer,
					 * and it will periodically do smgrcloseall(). This fact
					 * justifies our not closing the reln in the success path
					 * either, which is a good thing since in non-checkpointer
					 * cases we couldn't safely do that.)
					 */
					reln = smgropen(entry->rnode, InvalidBackendId);

					/* Attempt to open and fsync the target segment */
					seg = _mdfd_getseg(reln, forknum,
									   (BlockNumber) segno * (BlockNumber) RELSEG_SIZE,
									   false,
									   EXTENSION_RETURN_NULL
									   | EXTENSION_DONT_CHECK_SIZE);

					INSTR_TIME_SET_CURRENT(sync_start);

					if (seg != NULL &&
						FileSync(seg->mdfd_vfd, WAIT_EVENT_DATA_FILE_SYNC) >= 0)
					{
						/* Success; update statistics about sync timing */
						INSTR_TIME_SET_CURRENT(sync_end);
						sync_diff = sync_end;
						INSTR_TIME_SUBTRACT(sync_diff, sync_start);
						elapsed = INSTR_TIME_GET_MICROSEC(sync_diff);
						if (elapsed > longest)
							longest = elapsed;
						total_elapsed += elapsed;
						processed++;
						requests = bms_del_member(requests, segno);
						if (log_checkpoints)
							elog(DEBUG1, "checkpoint sync: number=%d file=%s time=%.3f msec",
								 processed,
								 FilePathName(seg->mdfd_vfd),
								 (double) elapsed / 1000);

						break;	/* out of retry loop */
					}

					/* Compute file name for use in message */
					save_errno = errno;
					path = _mdfd_segpath(reln, forknum, (BlockNumber) segno);
					errno = save_errno;

					/*
					 * It is possible that the relation has been dropped or
					 * truncated since the fsync request was entered.
					 * Therefore, allow ENOENT, but only if we didn't fail
					 * already on this file.  This applies both for
					 * _mdfd_getseg() and for FileSync, since fd.c might have
					 * closed the file behind our back.
					 *
					 * XXX is there any point in allowing more than one retry?
					 * Don't see one at the moment, but easy to change the
					 * test here if so.
					 */
					if (!FILE_POSSIBLY_DELETED(errno) ||
						failures > 0)
					{
						Bitmapset  *new_requests;

						/*
						 * We need to merge these unsatisfied requests with
						 * any others that have arrived since we started.
						 */
						new_requests = entry->requests[forknum];
						entry->requests[forknum] =
							bms_join(new_requests, requests);

						errno = save_errno;
						ereport(data_sync_elevel(ERROR),
								(errcode_for_file_access(),
								 errmsg("could not fsync file \"%s\": %m",
										path)));
					}
					else
					{
						/* POLAR: force log */
						ereport(WARNING,
								(errcode_for_file_access(),
								 errmsg("could not fsync file \"%s\" but retrying: %m",
										path)));
					}

					pfree(path);

					/*
					 * Absorb incoming requests and check to see if a cancel
					 * arrived for this relation fork.
					 */
					AbsorbFsyncRequests();
					absorb_counter = FSYNCS_PER_ABSORB; /* might as well... */

					if (entry->canceled[forknum])
						break;
				}				/* end retry loop */
			}
			bms_free(requests);
		}

		/*
		 * We've finished everything that was requested before we started to
		 * scan the entry.  If no new requests have been inserted meanwhile,
		 * remove the entry.  Otherwise, update its cycle counter, as all the
		 * requests now in it must have arrived during this cycle.
		 */
		for (forknum = 0; forknum <= MAX_FORKNUM; forknum++)
		{
			if (entry->requests[forknum] != NULL)
				break;
		}
		if (forknum <= MAX_FORKNUM)
			entry->cycle_ctr = mdsync_cycle_ctr;
		else
		{
			/* Okay to remove it */
			if (hash_search(pendingOpsTable, &entry->rnode,
							HASH_REMOVE, NULL) == NULL)
				elog(ERROR, "pendingOpsTable corrupted");
		}
	}							/* end loop over hashtable entries */

	/* Return sync performance metrics for report at checkpoint end */
	CheckpointStats.ckpt_sync_rels = processed;
	CheckpointStats.ckpt_longest_sync = longest;
	CheckpointStats.ckpt_agg_sync_time = total_elapsed;

	/* Flag successful completion of mdsync */
	mdsync_in_progress = false;
}

/*
 * mdpreckpt() -- Do pre-checkpoint work
 *
 * To distinguish unlink requests that arrived before this checkpoint
 * started from those that arrived during the checkpoint, we use a cycle
 * counter similar to the one we use for fsync requests. That cycle
 * counter is incremented here.
 *
 * This must be called *before* the checkpoint REDO point is determined.
 * That ensures that we won't delete files too soon.
 *
 * Note that we can't do anything here that depends on the assumption
 * that the checkpoint will be completed.
 */
void
mdpreckpt(void)
{
	/*
	 * Any unlink requests arriving after this point will be assigned the next
	 * cycle counter, and won't be unlinked until next checkpoint.
	 */
	mdckpt_cycle_ctr++;
}

/*
 * mdpostckpt() -- Do post-checkpoint work
 *
 * Remove any lingering files that can now be safely removed.
 */
void
mdpostckpt(void)
{
	int			absorb_counter;

	absorb_counter = UNLINKS_PER_ABSORB;
	while (pendingUnlinks != NIL)
	{
		PendingUnlinkEntry *entry = (PendingUnlinkEntry *) linitial(pendingUnlinks);
		char	   *path;

		/*
		 * New entries are appended to the end, so if the entry is new we've
		 * reached the end of old entries.
		 *
		 * Note: if just the right number of consecutive checkpoints fail, we
		 * could be fooled here by cycle_ctr wraparound.  However, the only
		 * consequence is that we'd delay unlinking for one more checkpoint,
		 * which is perfectly tolerable.
		 */
		if (entry->cycle_ctr == mdckpt_cycle_ctr)
			break;

		/* Unlink the file */
		path = relpathperm(entry->rnode, MAIN_FORKNUM);
		if (polar_unlink(path) < 0)
		{
			/*
			 * There's a race condition, when the database is dropped at the
			 * same time that we process the pending unlink requests. If the
			 * DROP DATABASE deletes the file before we do, we will get ENOENT
			 * here. rmtree() also has to ignore ENOENT errors, to deal with
			 * the possibility that we delete the file first.
			 */
			if (errno != ENOENT)
				ereport(WARNING,
						(errcode_for_file_access(),
						 errmsg("could not remove file \"%s\": %m", path)));
		}
		pfree(path);

		/* And remove the list entry */
		pendingUnlinks = list_delete_first(pendingUnlinks);
		pfree(entry);

		/*
		 * As in mdsync, we don't want to stop absorbing fsync requests for a
		 * long time when there are many deletions to be done.  We can safely
		 * call AbsorbFsyncRequests() at this point in the loop (note it might
		 * try to delete list entries).
		 */
		if (--absorb_counter <= 0)
		{
			AbsorbFsyncRequests();
			absorb_counter = UNLINKS_PER_ABSORB;
		}
	}
}

/*
 * register_dirty_segment() -- Mark a relation segment as needing fsync
 *
 * If there is a local pending-ops table, just make an entry in it for
 * mdsync to process later.  Otherwise, try to pass off the fsync request
 * to the checkpointer process.  If that fails, just do the fsync
 * locally before returning (we hope this will not happen often enough
 * to be a performance problem).
 */
static void
register_dirty_segment(SMgrRelation reln, ForkNumber forknum, MdfdVec *seg)
{
	/* Temp relations should never be fsync'd */
	Assert(!SmgrIsTemp(reln));

	if (pendingOpsTable)
	{
		/* push it into local pending-ops table */
		RememberFsyncRequest(reln->smgr_rnode.node, forknum, seg->mdfd_segno);
	}
	else
	{
		if (ForwardFsyncRequest(reln->smgr_rnode.node, forknum, seg->mdfd_segno))
			return;				/* passed it off successfully */

		ereport(DEBUG1,
				(errmsg("could not forward fsync request because request queue is full")));

		if (FileSync(seg->mdfd_vfd, WAIT_EVENT_DATA_FILE_SYNC) < 0)
			ereport(data_sync_elevel(ERROR),
					(errcode_for_file_access(),
					 errmsg("could not fsync file \"%s\": %m",
							FilePathName(seg->mdfd_vfd))));
	}
}

/*
 * register_unlink() -- Schedule a file to be deleted after next checkpoint
 *
 * We don't bother passing in the fork number, because this is only used
 * with main forks.
 *
 * As with register_dirty_segment, this could involve either a local or
 * a remote pending-ops table.
 */
static void
register_unlink(RelFileNodeBackend rnode)
{
	/* Should never be used with temp relations */
	Assert(!RelFileNodeBackendIsTemp(rnode));

	if (pendingOpsTable)
	{
		/* push it into local pending-ops table */
		RememberFsyncRequest(rnode.node, MAIN_FORKNUM,
							 UNLINK_RELATION_REQUEST);
	}
	else
	{
		/*
		 * Notify the checkpointer about it.  If we fail to queue the request
		 * message, we have to sleep and try again, because we can't simply
		 * delete the file now.  Ugly, but hopefully won't happen often.
		 *
		 * XXX should we just leave the file orphaned instead?
		 */
		Assert(IsUnderPostmaster);
		while (!ForwardFsyncRequest(rnode.node, MAIN_FORKNUM,
									UNLINK_RELATION_REQUEST))
			pg_usleep(10000L);	/* 10 msec seems a good number */
	}
}

/*
 * RememberFsyncRequest() -- callback from checkpointer side of fsync request
 *
 * We stuff fsync requests into the local hash table for execution
 * during the checkpointer's next checkpoint.  UNLINK requests go into a
 * separate linked list, however, because they get processed separately.
 *
 * The range of possible segment numbers is way less than the range of
 * BlockNumber, so we can reserve high values of segno for special purposes.
 * We define three:
 * - FORGET_RELATION_FSYNC means to cancel pending fsyncs for a relation,
 *	 either for one fork, or all forks if forknum is InvalidForkNumber
 * - FORGET_DATABASE_FSYNC means to cancel pending fsyncs for a whole database
 * - UNLINK_RELATION_REQUEST is a request to delete the file after the next
 *	 checkpoint.
 * Note also that we're assuming real segment numbers don't exceed INT_MAX.
 *
 * (Handling FORGET_DATABASE_FSYNC requests is a tad slow because the hash
 * table has to be searched linearly, but dropping a database is a pretty
 * heavyweight operation anyhow, so we'll live with it.)
 */
void
RememberFsyncRequest(RelFileNode rnode, ForkNumber forknum, BlockNumber segno)
{
	Assert(pendingOpsTable);

	if (segno == FORGET_RELATION_FSYNC)
	{
		/* Remove any pending requests for the relation (one or all forks) */
		PendingOperationEntry *entry;

		entry = (PendingOperationEntry *) hash_search(pendingOpsTable,
													  &rnode,
													  HASH_FIND,
													  NULL);
		if (entry)
		{
			/*
			 * We can't just delete the entry since mdsync could have an
			 * active hashtable scan.  Instead we delete the bitmapsets; this
			 * is safe because of the way mdsync is coded.  We also set the
			 * "canceled" flags so that mdsync can tell that a cancel arrived
			 * for the fork(s).
			 */
			if (forknum == InvalidForkNumber)
			{
				/* remove requests for all forks */
				for (forknum = 0; forknum <= MAX_FORKNUM; forknum++)
				{
					bms_free(entry->requests[forknum]);
					entry->requests[forknum] = NULL;
					entry->canceled[forknum] = true;
				}
			}
			else
			{
				/* remove requests for single fork */
				bms_free(entry->requests[forknum]);
				entry->requests[forknum] = NULL;
				entry->canceled[forknum] = true;
			}
		}
	}
	else if (segno == FORGET_DATABASE_FSYNC)
	{
		/* Remove any pending requests for the entire database */
		HASH_SEQ_STATUS hstat;
		PendingOperationEntry *entry;
		ListCell   *cell,
				   *prev,
				   *next;

		/* Remove fsync requests */
		hash_seq_init(&hstat, pendingOpsTable);
		while ((entry = (PendingOperationEntry *) hash_seq_search(&hstat)) != NULL)
		{
			if (entry->rnode.dbNode == rnode.dbNode)
			{
				/* remove requests for all forks */
				for (forknum = 0; forknum <= MAX_FORKNUM; forknum++)
				{
					bms_free(entry->requests[forknum]);
					entry->requests[forknum] = NULL;
					entry->canceled[forknum] = true;
				}
			}
		}

		/* Remove unlink requests */
		prev = NULL;
		for (cell = list_head(pendingUnlinks); cell; cell = next)
		{
			PendingUnlinkEntry *entry = (PendingUnlinkEntry *) lfirst(cell);

			next = lnext(cell);
			if (entry->rnode.dbNode == rnode.dbNode)
			{
				pendingUnlinks = list_delete_cell(pendingUnlinks, cell, prev);
				pfree(entry);
			}
			else
				prev = cell;
		}
	}
	else if (segno == UNLINK_RELATION_REQUEST)
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(pendingOpsCxt);
		PendingUnlinkEntry *entry;

		/* PendingUnlinkEntry doesn't store forknum, since it's always MAIN */
		Assert(forknum == MAIN_FORKNUM);

		entry = palloc(sizeof(PendingUnlinkEntry));
		entry->rnode = rnode;
		entry->cycle_ctr = mdckpt_cycle_ctr;

		pendingUnlinks = lappend(pendingUnlinks, entry);

		MemoryContextSwitchTo(oldcxt);
	}
	else
	{
		/* Normal case: enter a request to fsync this segment */
		MemoryContext oldcxt = MemoryContextSwitchTo(pendingOpsCxt);
		PendingOperationEntry *entry;
		bool		found;

		entry = (PendingOperationEntry *) hash_search(pendingOpsTable,
													  &rnode,
													  HASH_ENTER,
													  &found);
		/* if new entry, initialize it */
		if (!found)
		{
			entry->cycle_ctr = mdsync_cycle_ctr;
			MemSet(entry->requests, 0, sizeof(entry->requests));
			MemSet(entry->canceled, 0, sizeof(entry->canceled));
		}

		/*
		 * NB: it's intentional that we don't change cycle_ctr if the entry
		 * already exists.  The cycle_ctr must represent the oldest fsync
		 * request that could be in the entry.
		 */

		entry->requests[forknum] = bms_add_member(entry->requests[forknum],
												  (int) segno);

		MemoryContextSwitchTo(oldcxt);
	}
}

/*
 * ForgetRelationFsyncRequests -- forget any fsyncs for a relation fork
 *
 * forknum == InvalidForkNumber means all forks, although this code doesn't
 * actually know that, since it's just forwarding the request elsewhere.
 */
void
ForgetRelationFsyncRequests(RelFileNode rnode, ForkNumber forknum)
{
	if (pendingOpsTable)
	{
		/* standalone backend or startup process: fsync state is local */
		RememberFsyncRequest(rnode, forknum, FORGET_RELATION_FSYNC);
	}
	else if (IsUnderPostmaster)
	{
		/*
		 * Notify the checkpointer about it.  If we fail to queue the cancel
		 * message, we have to sleep and try again ... ugly, but hopefully
		 * won't happen often.
		 *
		 * XXX should we CHECK_FOR_INTERRUPTS in this loop?  Escaping with an
		 * error would leave the no-longer-used file still present on disk,
		 * which would be bad, so I'm inclined to assume that the checkpointer
		 * will always empty the queue soon.
		 */
		while (!ForwardFsyncRequest(rnode, forknum, FORGET_RELATION_FSYNC))
			pg_usleep(10000L);	/* 10 msec seems a good number */

		/*
		 * Note we don't wait for the checkpointer to actually absorb the
		 * cancel message; see mdsync() for the implications.
		 */
	}
}

/*
 * ForgetDatabaseFsyncRequests -- forget any fsyncs and unlinks for a DB
 */
void
ForgetDatabaseFsyncRequests(Oid dbid)
{
	RelFileNode rnode;

	rnode.dbNode = dbid;
	rnode.spcNode = 0;
	rnode.relNode = 0;

	if (pendingOpsTable)
	{
		/* standalone backend or startup process: fsync state is local */
		RememberFsyncRequest(rnode, InvalidForkNumber, FORGET_DATABASE_FSYNC);
	}
	else if (IsUnderPostmaster)
	{
		/* see notes in ForgetRelationFsyncRequests */
		while (!ForwardFsyncRequest(rnode, InvalidForkNumber,
									FORGET_DATABASE_FSYNC))
			pg_usleep(10000L);	/* 10 msec seems a good number */
	}
}

/*
 * DropRelationFiles -- drop files of all given relations
 */
void
DropRelationFiles(RelFileNode *delrels, int ndelrels, bool isRedo)
{
	SMgrRelation *srels;
	int			i;

	srels = palloc(sizeof(SMgrRelation) * ndelrels);
	for (i = 0; i < ndelrels; i++)
	{
		SMgrRelation srel = smgropen(delrels[i], InvalidBackendId);

		if (isRedo)
		{
			ForkNumber	fork;

			for (fork = 0; fork <= MAX_FORKNUM; fork++)
				XLogDropRelation(delrels[i], fork);
		}
		srels[i] = srel;
	}

	smgrdounlinkall(srels, ndelrels, isRedo);

	for (i = 0; i < ndelrels; i++)
		smgrclose(srels[i]);
	pfree(srels);
}


/*
 *	_fdvec_resize() -- Resize the fork's open segments array
 */
static void
_fdvec_resize(SMgrRelation reln,
			  ForkNumber forknum,
			  int nseg)
{
	if (nseg == 0)
	{
		if (reln->md_num_open_segs[forknum] > 0)
		{
			pfree(reln->md_seg_fds[forknum]);
			reln->md_seg_fds[forknum] = NULL;
		}
	}
	else if (reln->md_num_open_segs[forknum] == 0)
	{
		reln->md_seg_fds[forknum] =
			MemoryContextAlloc(MdCxt, sizeof(MdfdVec) * nseg);
	}
	else
	{
		/*
		 * It doesn't seem worthwhile complicating the code to amortize
		 * repalloc() calls.  Those are far faster than PathNameOpenFile() or
		 * FileClose(), and the memory context internally will sometimes avoid
		 * doing an actual reallocation.
		 */
		reln->md_seg_fds[forknum] =
			repalloc(reln->md_seg_fds[forknum],
					 sizeof(MdfdVec) * nseg);
	}

	reln->md_num_open_segs[forknum] = nseg;
}

/*
 * Return the filename for the specified segment of the relation. The
 * returned string is palloc'd.
 */
static char *
_mdfd_segpath(SMgrRelation reln, ForkNumber forknum, BlockNumber segno)
{
	char	   *path,
			   *fullpath;

	path = relpath(reln->smgr_rnode, forknum);

	if (segno > 0)
	{
		fullpath = psprintf("%s.%u", path, segno);
		pfree(path);
	}
	else
		fullpath = path;

	return fullpath;
}

/*
 * Open the specified segment of the relation,
 * and make a MdfdVec object for it.  Returns NULL on failure.
 */
static MdfdVec *
_mdfd_openseg(SMgrRelation reln, ForkNumber forknum, BlockNumber segno,
			  int oflags)
{
	MdfdVec    *v;
	int			fd;
	char	   *fullpath;
	int			flags = PG_BINARY | oflags;

	/* POLAR: read datafile only need O_RDONLY flag in replica */
	if (polar_openfile_with_readonly_in_replica &&
		polar_in_replica_mode())
		flags |= O_RDONLY;
	else
		flags |= O_RDWR;

	fullpath = _mdfd_segpath(reln, forknum, segno);

	/* open the file */
	fd = polar_path_name_open_file(fullpath, flags);

	pfree(fullpath);

	if (fd < 0)
		return NULL;

	if (segno <= reln->md_num_open_segs[forknum])
		_fdvec_resize(reln, forknum, segno + 1);

	/* fill the entry */
	v = &reln->md_seg_fds[forknum][segno];
	v->mdfd_vfd = fd;
	v->mdfd_segno = segno;

	Assert(_mdnblocks(reln, forknum, v) <= ((BlockNumber) RELSEG_SIZE));

	/* all done */
	return v;
}

/*
 *	_mdfd_getseg() -- Find the segment of the relation holding the
 *		specified block.
 *
 * If the segment doesn't exist, we ereport, return NULL, or create the
 * segment, according to "behavior".  Note: skipFsync is only used in the
 * EXTENSION_CREATE case.
 */
static MdfdVec *
_mdfd_getseg(SMgrRelation reln, ForkNumber forknum, BlockNumber blkno,
			 bool skipFsync, int behavior)
{
	MdfdVec    *v;
	BlockNumber targetseg;
	BlockNumber nextsegno;

	/* some way to handle non-existent segments needs to be specified */
	Assert(behavior &
		   (EXTENSION_FAIL | EXTENSION_CREATE | EXTENSION_RETURN_NULL));

	targetseg = blkno / ((BlockNumber) RELSEG_SIZE);

	/* if an existing and opened segment, we're done */
	if (targetseg < reln->md_num_open_segs[forknum])
	{
		v = &reln->md_seg_fds[forknum][targetseg];
		return v;
	}

	/*
	 * The target segment is not yet open. Iterate over all the segments
	 * between the last opened and the target segment. This way missing
	 * segments either raise an error, or get created (according to
	 * 'behavior'). Start with either the last opened, or the first segment if
	 * none was opened before.
	 */
	if (reln->md_num_open_segs[forknum] > 0)
		v = &reln->md_seg_fds[forknum][reln->md_num_open_segs[forknum] - 1];
	else
	{
		v = mdopen(reln, forknum, behavior);
		if (!v)
			return NULL;		/* if behavior & EXTENSION_RETURN_NULL */
	}

	for (nextsegno = reln->md_num_open_segs[forknum];
		 nextsegno <= targetseg; nextsegno++)
	{
		BlockNumber nblocks = _mdnblocks(reln, forknum, v);
		int			flags = 0;

		Assert(nextsegno == v->mdfd_segno + 1);

		if (nblocks > ((BlockNumber) RELSEG_SIZE))
			elog(FATAL, "segment too big");

		/* POLAR: replica cannot create/extend file anytime */
		if ((behavior & EXTENSION_CREATE) ||
			(!polar_in_replica_mode() && InRecovery && (behavior & EXTENSION_CREATE_RECOVERY)))
		{
			/*
			 * Normally we will create new segments only if authorized by the
			 * caller (i.e., we are doing mdextend()).  But when doing WAL
			 * recovery, create segments anyway; this allows cases such as
			 * replaying WAL data that has a write into a high-numbered
			 * segment of a relation that was later deleted. We want to go
			 * ahead and create the segments so we can finish out the replay.
			 * However if the caller has specified
			 * EXTENSION_REALLY_RETURN_NULL, then extension is not desired
			 * even in recovery; we won't reach this point in that case.
			 *
			 * We have to maintain the invariant that segments before the last
			 * active segment are of size RELSEG_SIZE; therefore, if
			 * extending, pad them out with zeroes if needed.  (This only
			 * matters if in recovery, or if the caller is extending the
			 * relation discontiguously, but that can happen in hash indexes.)
			 */
			if (nblocks < ((BlockNumber) RELSEG_SIZE))
			{
				char	   *zerobuf = palloc0(BLCKSZ);

				mdextend(reln, forknum,
						 nextsegno * ((BlockNumber) RELSEG_SIZE) - 1,
						 zerobuf, skipFsync);
				pfree(zerobuf);
			}
			flags = O_CREAT;
		}
		else if (!(behavior & EXTENSION_DONT_CHECK_SIZE) &&
				 nblocks < ((BlockNumber) RELSEG_SIZE))
		{
			/*
			 * When not extending (or explicitly including truncated
			 * segments), only open the next segment if the current one is
			 * exactly RELSEG_SIZE.  If not (this branch), either return NULL
			 * or fail.
			 */
			if (behavior & EXTENSION_RETURN_NULL)
			{
				/*
				 * Some callers discern between reasons for _mdfd_getseg()
				 * returning NULL based on errno. As there's no failing
				 * syscall involved in this case, explicitly set errno to
				 * ENOENT, as that seems the closest interpretation.
				 */
				errno = ENOENT;
				return NULL;
			}

			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\" (target block %u): previous segment is only %u blocks",
							_mdfd_segpath(reln, forknum, nextsegno),
							blkno, nblocks)));
		}

		v = _mdfd_openseg(reln, forknum, nextsegno, flags);

		if (v == NULL)
		{
			if ((behavior & EXTENSION_RETURN_NULL) &&
				FILE_POSSIBLY_DELETED(errno))
				return NULL;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\" (target block %u): %m",
							_mdfd_segpath(reln, forknum, nextsegno),
							blkno)));
		}
	}

	return v;
}

/*
 * Get number of blocks present in a single disk file
 */
static BlockNumber
_mdnblocks(SMgrRelation reln, ForkNumber forknum, MdfdVec *seg)
{
	off_t		len;

	len = FileSeek(seg->mdfd_vfd, 0L, SEEK_END);
	if (len < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not seek to end of file \"%s\": %m",
						FilePathName(seg->mdfd_vfd))));
	/* note that this calculation will ignore any partial block at EOF */
	return (BlockNumber) (len / BLCKSZ);
}

/*
 *	mdextendbatch() -- Add a block to the specified relation.
 *
 *		The semantics are nearly the same as mdwrite(): write at the
 *		specified position.  However, this is to be used for the case of
 *		extending a relation (i.e., blocknum is at or beyond the current
 *		EOF).  Note that we assume writing a block beyond current EOF
 *		causes intervening file space to become filled with zeroes.
 */
void
mdextendbatch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		 int blockCount, char *buffer, bool skipFsync)
{
	off_t		seekpos;
	int			nbytes;
	MdfdVec    *v;

	/* This assert is too expensive to have on normally ... */
#ifdef CHECK_WRITE_VS_EXTEND
	Assert(blocknum >= mdnblocks(reln, forknum, false));
#endif

	/*
	 * If a relation manages to grow to 2^32-1 blocks, refuse to extend it any
	 * more --- we mustn't create a block whose number actually is
	 * InvalidBlockNumber.
	 */
	if (blocknum == InvalidBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("cannot extend file \"%s\" beyond %u blocks",
						relpath(reln->smgr_rnode, forknum),
						InvalidBlockNumber)));

	v = _mdfd_getseg(reln, forknum, blocknum, skipFsync, EXTENSION_CREATE);

	seekpos = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));

	Assert(seekpos < (off_t) BLCKSZ * RELSEG_SIZE);
	/* We assum all the block in the same segment file */
	Assert((off_t) BLCKSZ * ((blocknum + blockCount) % ((BlockNumber) RELSEG_SIZE)) < (off_t) BLCKSZ * RELSEG_SIZE);

	if ((nbytes = polar_file_pwrite(v->mdfd_vfd, buffer, BLCKSZ * blockCount, seekpos,
			WAIT_EVENT_DATA_FILE_BATCH_PWRITE_EXTEND)) != BLCKSZ * blockCount)
	{
		if (nbytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not extend file \"%s\": %m",
							FilePathName(v->mdfd_vfd)),
					 errhint("Check free disk space.")));
		/* short write: complain appropriately */
		ereport(ERROR,
				(errcode(ERRCODE_DISK_FULL),
				 errmsg("could not extend file \"%s\": wrote only %d of %d bytes at block %u",
						FilePathName(v->mdfd_vfd),
						nbytes, BLCKSZ * blockCount, blocknum),
				 errhint("Check free disk space.")));
	}

	if (!skipFsync && !SmgrIsTemp(reln))
		register_dirty_segment(reln, forknum, v);
}

bool
polar_need_skip_request(BlockNumber segno)
{
	bool polar_need_skip = true;

	/* POLAR: bgwriter not unlink file and sync file from other process */
	if (AmCheckpointerProcess())
		polar_need_skip = false;
	else if (AmPolarBackgroundWriterProcess())
	{
		if (segno == FORGET_DATABASE_FSYNC)
			polar_need_skip = false;
		else if(segno == FORGET_RELATION_FSYNC)
			polar_need_skip = false;
	}

	return polar_need_skip;
}

static BlockNumber
polar_cache_mdnblocks(SMgrRelation reln, ForkNumber forknum, MdfdVec *seg)
{
	off_t		len;
	File		file = seg->mdfd_vfd;

	len = polar_file_seek_end(file);
	if (len < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not seek to end of file \"%s\": %m",
						FilePathName(seg->mdfd_vfd))));
	/* note that this calculation will ignore any partial block at EOF */
	return (BlockNumber) (len / BLCKSZ);
}

