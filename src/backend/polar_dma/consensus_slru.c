/*-------------------------------------------------------------------------
 *
 * consensus_slru.c
 *		Simple LRU buffering for consensus logfiles
 *
 * Portions Copyright (c) 2021, Alibaba Group Holding Limited
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 * 	src/backend/polar_dma/censensus_slru.c
 *
 *-------------------------------------------------------------------------
 */

#include "easy_log.h"

#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common/file_perm.h"
#include "miscadmin.h"
#include "polar_dma/consensus_slru.h"
#include "storage/shmem.h"
#include "utils/guc.h"


/*
 * During ConsensusSimpleLruFlush(), we will usually not need to fsync for every 
 * writes, because we may need to write several pages per file.  We can 
 * consolidate the I/O requests by leaving the file open until we flush next file.  
 */
typedef struct ConsensusSlruFlushFile
{
	int			fd;
	uint64	segno;
	bool		create_if_not_exists;	
	bool		fsync;
} ConsensusSlruFlushFile;

#define ConsensusSlruRecentlyUsed(shared, slotno)	\
	do { \
		int		new_lru_count = (shared)->cur_lru_count; \
		if (new_lru_count != (shared)->page_lru_count[slotno]) { \
			(shared)->cur_lru_count = ++new_lru_count; \
			(shared)->page_lru_count[slotno] = new_lru_count; \
		} \
	} while (0)

#define CONSENSUS_SLRU_VICTIM_WINDOW	128      /* victim slot window */

#define	CONSENSUS_SLRU_FILE_IN_SHARED_STORAGE(ctl)	\
												(polar_enable_shared_storage_mode &&  \
												(ctl)->shared->slru_file_in_shared_storage)

const consensus_slru_stat *consensus_slru_stats[CONSENSUS_SLRU_STATS_NUM];
int n_consensus_slru_stats = 0;

static void consensus_slru_wait_io(ConsensusSlruCtl ctl, int slotno);
static bool consensus_slru_internal_write_page(ConsensusSlruCtl ctl, int slotno, ConsensusSlruFlushFile *fdata);
static bool consensus_slru_physical_read_page(ConsensusSlruCtl ctl, uint64 pageno, int slotno);
static bool consensus_slru_physical_write_page(ConsensusSlruCtl ctl, uint64 pageno, int slotno, 
													int offset, ConsensusSlruFlushFile *fdata);
static int	consensus_slru_select_lru_page(ConsensusSlruCtl ctl, uint64 pageno);

static void consensus_slru_internal_delete_segment(ConsensusSlruCtl ctl, char *filename);

static void consensus_slru_file_dir(ConsensusSlruCtl ctl, char *path);
static void consensus_slru_file_name_by_seg(ConsensusSlruCtl ctl, char *path, uint64 seg);
static void consensus_slru_file_name_by_name(ConsensusSlruCtl ctl, char *path, char *filename);

static inline void consensus_slru_set_page_status(ConsensusSlruShared shared, 
																			int slotno, ConsensusSlruPageStatus pagestatus);

/*
 * Initialization of shared memory
 */
Size
ConsensusSimpleLruShmemSize(int nslots, int szblock, int nlsns)
{
	Size		sz;

	sz = MAXALIGN(sizeof(ConsensusSlruSharedData));
	sz += MAXALIGN(nslots * sizeof(char *));	/* page_buffer[] */
	sz += MAXALIGN(nslots * sizeof(ConsensusSlruPageStatus));	/* page_status[] */
	sz += MAXALIGN(nslots * sizeof(bool));	/* page_dirty[] */
	sz += MAXALIGN(nslots * sizeof(uint64));	/* page_number[] */
	sz += MAXALIGN(nslots * sizeof(int));	/* page_next_dirty[] */
	sz += MAXALIGN(nslots * sizeof(int));	/* page_lru_count[] */
	sz += MAXALIGN(nslots * sizeof(pthread_rwlock_t));	/* buffer_locks[] */

	return MAXALIGN(BUFFERALIGN(sz) + szblock * nslots);
}

/*
 * Init share memory, so must be called by consensus master thread before start other thread 
 */
void
ConsensusSimpleLruInit(ConsensusSlruCtl ctl, const char *name, int szblock, int nslots, int nlsns, 
							pthread_rwlock_t *ctllock, flush_hook before_flush_hook, 
							const char *subdir, bool polar_shared_file)
{
	ConsensusSlruShared	shared;
	bool		found;
	char 		path[MAXPGPATH];

	shared = (ConsensusSlruShared) ShmemInitStruct(name,
										  ConsensusSimpleLruShmemSize(nslots, szblock, nlsns),
										  &found);

	if (!IsUnderPostmaster)
	{
		char	   *ptr;
		Size		offset;
		int			slotno;

		pthread_rwlockattr_t lock_attr;

		pthread_rwlockattr_init(&lock_attr);
		pthread_rwlockattr_setpshared(&lock_attr,PTHREAD_PROCESS_SHARED);

		Assert(!found);
		memset(shared, 0, sizeof(ConsensusSlruSharedData));

		shared->control_lock = ctllock;
		pthread_rwlock_init(shared->control_lock, &lock_attr);

		shared->slru_file_in_shared_storage = polar_shared_file;
		shared->num_slots = nslots;

		shared->cur_lru_count = 0;
		shared->first_dirty_slot = nslots;
		shared->first_dirty_offset = 0;
		shared->last_dirty_slot = nslots;

		/* shared->latest_page_number will be set later */

		ptr = (char *) shared;
		offset = MAXALIGN(sizeof(ConsensusSlruSharedData));
		shared->page_buffer = (char **) (ptr + offset);
		offset += MAXALIGN(nslots * sizeof(char *));
		shared->page_status = (ConsensusSlruPageStatus *) (ptr + offset);
		offset += MAXALIGN(nslots * sizeof(ConsensusSlruPageStatus));
		shared->page_dirty = (bool *) (ptr + offset);
		offset += MAXALIGN(nslots * sizeof(bool));
		shared->page_number = (uint64 *) (ptr + offset);
		offset += MAXALIGN(nslots * sizeof(uint64));
		shared->next_dirty_slot = (int *) (ptr + offset);
		offset += MAXALIGN(nslots * sizeof(int));
		shared->page_lru_count = (int *) (ptr + offset);
		offset += MAXALIGN(nslots * sizeof(int));
		shared->buffer_locks = (pthread_rwlock_t *) (ptr + offset);
		offset += MAXALIGN(nslots * sizeof(pthread_rwlock_t));

		Assert(strlen(name) + 1 < CONSENSUS_SLRU_MAX_NAME_LENGTH);
		strlcpy(shared->name, name, CONSENSUS_SLRU_MAX_NAME_LENGTH);

		ptr += BUFFERALIGN(offset);
		for (slotno = 0; slotno < nslots; slotno++)
		{
			pthread_rwlock_init(&shared->buffer_locks[slotno], &lock_attr);

			shared->page_buffer[slotno] = ptr;
			consensus_slru_set_page_status(shared, slotno, SLRU_PAGE_EMPTY);
			shared->page_dirty[slotno] = false;
			shared->page_number[slotno] = 0;
			shared->page_lru_count[slotno] = 0;
			shared->next_dirty_slot[slotno] = nslots;

			ptr += szblock;
		}

		/* for slru stat */
		MemSet(&shared->stat, 0, sizeof(consensus_slru_stat));
		shared->stat.name = shared->name;
		shared->stat.n_slots = (uint)nslots;
		shared->stat.n_page_status_stat[SLRU_PAGE_EMPTY] = (uint)nslots;

		if (n_consensus_slru_stats < CONSENSUS_SLRU_STATS_NUM)
			consensus_slru_stats[n_consensus_slru_stats++] = &shared->stat;

		/* Should fit to estimated shmem size */
		Assert(ptr - (char *) shared <= ConsensusSimpleLruShmemSize(nslots, szblock, nlsns));
	}
	else
		Assert(found);

	/*
	 * Initialize the unshared control struct, including directory path 
	 * and block size.
	 */
	ctl->shared = shared;
	ctl->szblock = szblock;
	ctl->before_flush_hook = before_flush_hook;

	StrNCpy(ctl->Dir, subdir, sizeof(ctl->Dir));

	consensus_slru_file_dir(ctl, path);
	ctl->vfs_api = polar_vfs_mgr(path);
}

bool
ConsensusSimpleLruValidateDir(ConsensusSlruCtl ctl)
{
	char		path[MAXPGPATH];
	struct stat stat_buf;

	consensus_slru_file_dir(ctl, path);

	if (ctl->vfs_api->vfs_stat(path, &stat_buf) == 0)
	{
		if (!S_ISDIR(stat_buf.st_mode))
		{
			easy_error_log("required consensus slru directory \"%s\" is not a directory", path);
			return false;
		}
	}
	else
	{
		easy_warn_log("creating missing consensus slru directory \"%s\"", path);
		if (ctl->vfs_api->vfs_mkdir(path, pg_dir_create_mode) != 0)
		{
			easy_fatal_log("Could not create directory \"%s\": %s.", path, strerror(errno));
			return false;
		}
	}

	return true;
}

/*
 * Initialize (or reinitialize) a page to zeroes.
 *
 * The page is not actually written, just set up in shared memory.
 * The slot number of the new page is returned.
 *
 * Control lock must be held at entry, and will be held at exit.
 */
int
ConsensusSimpleLruZeroPage(ConsensusSlruCtl ctl, uint64 pageno)
{
	ConsensusSlruShared	shared = ctl->shared;
	int			slotno;
	/* POLAR: slru stat */
	consensus_slru_stat *stat = &shared->stat;

	stat->n_slru_zero_count++;

	/* Find a suitable buffer slot for the page */
	slotno = consensus_slru_select_lru_page(ctl, pageno);
	Assert(shared->page_status[slotno] == SLRU_PAGE_EMPTY ||
		   (shared->page_status[slotno] == SLRU_PAGE_VALID &&
			!shared->page_dirty[slotno]) ||
		   shared->page_number[slotno] == pageno);

	/* Mark the slot as containing this page */
	shared->page_number[slotno] = pageno;
	consensus_slru_set_page_status(shared, slotno, SLRU_PAGE_VALID);
	consensus_slru_push_dirty(ctl, slotno, 0, false);

	ConsensusSlruRecentlyUsed(shared, slotno);

	/* Set the buffer to zeroes */
	MemSet(shared->page_buffer[slotno], 0, BLCKSZ);

	/* Assume this page is now the latest active page */
	shared->latest_page_number = pageno;

	return slotno;
}

/*
 * Wait for any active I/O on a page slot to finish.  (This does not
 * guarantee that new I/O hasn't been started before we return, though.
 * In fact the slot might not even contain the same page anymore.)
 *
 * Control lock must be held at entry, and will be held at exit.
 */
static void
consensus_slru_wait_io(ConsensusSlruCtl ctl, int slotno)
{
	ConsensusSlruShared	shared = ctl->shared;
	consensus_slru_stat *stat = &shared->stat;

	bool wait_reading = false;
	bool wait_writing = false;
	if (shared->page_status[slotno] == SLRU_PAGE_READ_IN_PROGRESS)
	{
		wait_reading = true;
		stat->n_wait_reading_count++;
	}
	else if (shared->page_status[slotno] == SLRU_PAGE_WRITE_IN_PROGRESS)
	{
		wait_writing = true;
		stat->n_wait_writing_count++;
	}

	pthread_rwlock_unlock(shared->control_lock);
	pthread_rwlock_rdlock(&shared->buffer_locks[slotno]);
	pthread_rwlock_unlock(&shared->buffer_locks[slotno]);
	pthread_rwlock_wrlock(shared->control_lock);

	if (wait_reading)
		stat->n_wait_reading_count--;
	else if (wait_writing)
		stat->n_wait_writing_count--;
}

/*
 * Find a page in a shared buffer, reading it in if necessary.
 * The page number must correspond to an already-initialized page.
 *
 * If write_ok is true then it is OK to return a page that is in
 * WRITE_IN_PROGRESS state; it is the caller's responsibility to be sure
 * that modification of the page is safe.  If write_ok is false then we
 * will not return the page until it is not undergoing active I/O.
 *
 * Return value is the shared-buffer slot number now holding the page.
 * The buffer's LRU access info is updated.
 *
 * Control lock must be held at entry, and will be held at exit.
 */
int
ConsensusSimpleLruReadPage(ConsensusSlruCtl ctl, uint64 pageno, bool write_ok)
{
	ConsensusSlruShared	shared = ctl->shared;
	/* POLAR: slru stat */
	consensus_slru_stat *stat = &shared->stat;
	stat->n_slru_read_count++;

	/* Outer loop handles restart if we must wait for someone else's I/O */
	for (;;)
	{
		int			slotno;
		bool		ok;

		/* See if page already is in memory; if not, pick victim slot */
		slotno = consensus_slru_select_lru_page(ctl, pageno);

		/* Did we find the page in memory? */
		if (shared->page_number[slotno] == pageno &&
			shared->page_status[slotno] != SLRU_PAGE_EMPTY)
		{
			/*
			 * If page is still being read in, we must wait for I/O.  Likewise
			 * if the page is being written and the caller said that's not OK.
			 */
			if (shared->page_status[slotno] == SLRU_PAGE_READ_IN_PROGRESS ||
				(shared->page_status[slotno] == SLRU_PAGE_WRITE_IN_PROGRESS &&
				 !write_ok))
			{
				consensus_slru_wait_io(ctl, slotno);
				/* Now we must recheck state from the top */
				continue;
			}
			/* Otherwise, it's ready to use */
			ConsensusSlruRecentlyUsed(shared, slotno);
			return slotno;
		}

		/* We found no match; assert we selected a freeable slot */
		Assert(shared->page_status[slotno] == SLRU_PAGE_EMPTY ||
			   (shared->page_status[slotno] == SLRU_PAGE_VALID &&
				!shared->page_dirty[slotno]));

		/* Mark the slot read-busy */
		shared->page_number[slotno] = pageno;
		consensus_slru_set_page_status(shared, slotno, SLRU_PAGE_READ_IN_PROGRESS);
		shared->page_dirty[slotno] = false;

		/* Acquire per-buffer lock (cannot deadlock, see notes at top) */
		pthread_rwlock_wrlock(&shared->buffer_locks[slotno]);

		/* Release control lock while doing I/O */
		pthread_rwlock_unlock(shared->control_lock);

		/* Do the read */
		ok = consensus_slru_physical_read_page(ctl, pageno, slotno);

		/* Re-acquire control lock and update page state */
		pthread_rwlock_wrlock(shared->control_lock);

		Assert(shared->page_number[slotno] == pageno &&
			   shared->page_status[slotno] == SLRU_PAGE_READ_IN_PROGRESS &&
			   !shared->page_dirty[slotno]);

		consensus_slru_set_page_status(shared, slotno, ok ? SLRU_PAGE_VALID : SLRU_PAGE_EMPTY);
		pthread_rwlock_unlock(&shared->buffer_locks[slotno]);

		if (!ok)
		{
			return -1;
		}

		ConsensusSlruRecentlyUsed(shared, slotno);
		return slotno;
	}
}

/*
 * Find a page in a shared buffer, reading it in if necessary.
 * The page number must correspond to an already-initialized page.
 * The caller must intend only read-only access to the page.
 *
 * Return value is the shared-buffer slot number now holding the page.
 * The buffer's LRU access info is updated.
 *
 * Control lock must NOT be held at entry, but will be held at exit.
 * It is unspecified whether the lock will be shared or exclusive.
 */
int
ConsensusSimpleLruReadPage_ReadOnly(ConsensusSlruCtl ctl, uint64 pageno)
{
	ConsensusSlruShared	shared = ctl->shared;
	int			slotno;
	consensus_slru_stat *stat = &shared->stat;

	/* Try to find the page while holding only shared lock */
	pthread_rwlock_rdlock(shared->control_lock);

	stat->n_slru_read_only_count++;

	for (slotno = 0; slotno < shared->num_slots; slotno++)
	{
		if (shared->page_number[slotno] == pageno &&
				shared->page_status[slotno] != SLRU_PAGE_EMPTY &&
				shared->page_status[slotno] != SLRU_PAGE_READ_IN_PROGRESS)
		{
			/* See comments for ConsensusSlruRecentlyUsed macro */
			ConsensusSlruRecentlyUsed(shared, slotno);
			return slotno;
		}
	}


	/* No luck, so switch to normal exclusive lock and do regular read */
	pthread_rwlock_unlock(shared->control_lock);
	pthread_rwlock_wrlock(shared->control_lock);
	stat->n_slru_read_upgrade_count++;

	return ConsensusSimpleLruReadPage(ctl, pageno, true);
}

static void
consensus_slru_try_write_page(ConsensusSlruCtl ctl, int slotno)
{
	if (!ctl->before_flush_hook || ctl->before_flush_hook(slotno))
	{
		consensus_slru_internal_write_page(ctl, slotno, NULL);
	}
}

/*
 * Write a page from a shared buffer, if necessary.
 * Does nothing if the specified slot is not dirty.
 *
 * NOTE: only one write attempt is made here.  Hence, it is possible that
 * the page is still dirty at exit (if someone else re-dirtied it during
 * the write).  However, we *do* attempt a fresh write even if the page
 * is already being written; this is for checkpoints.
 *
 * Control lock must be held at entry, and will be held at exit.
 */
static bool 
consensus_slru_internal_write_page(ConsensusSlruCtl ctl, int slotno, ConsensusSlruFlushFile *fdata)
{
	ConsensusSlruShared	shared = ctl->shared;
	uint64	pageno = shared->page_number[slotno];
	int			offset;
	bool		ok;
	/* POLAR: stat */
	consensus_slru_stat *stat = &shared->stat;

	stat->n_slru_write_count++;

	/* If a write is in progress, wait for it to finish */
	while (shared->page_status[slotno] == SLRU_PAGE_WRITE_IN_PROGRESS &&
		   shared->page_number[slotno] == pageno)
	{
		consensus_slru_wait_io(ctl, slotno);
	}

	/*
	 * Do nothing if page is not dirty, or if buffer no longer contains the
	 * same page we were called for.
	 */
	if (!shared->page_dirty[slotno] ||
		shared->page_status[slotno] != SLRU_PAGE_VALID ||
		shared->page_number[slotno] != pageno)
		return true;

	Assert(shared->first_dirty_slot == slotno);
	offset = shared->first_dirty_offset;

	Assert(!ctl->before_flush_hook || ctl->before_flush_hook(slotno));

	/*
	 * Mark the slot write-busy, and clear the dirtybit.  After this point, a
	 * transaction status update on this page will mark it dirty again.
	 */
	consensus_slru_set_page_status(shared, slotno, SLRU_PAGE_WRITE_IN_PROGRESS);

	/* Acquire per-buffer lock (cannot deadlock, see notes at top) */
	pthread_rwlock_wrlock(&shared->buffer_locks[slotno]);

	/* Release control lock while doing I/O */
	pthread_rwlock_unlock(shared->control_lock);

	/* Do the write */
	ok = consensus_slru_physical_write_page(ctl, pageno, slotno, offset, fdata);

	/* Re-acquire control lock and update page state */
	pthread_rwlock_wrlock(shared->control_lock);

	Assert(shared->page_number[slotno] == pageno &&
		   shared->page_status[slotno] == SLRU_PAGE_WRITE_IN_PROGRESS);

	if (ok)
		consensus_slru_pop_dirty(ctl, slotno);

	consensus_slru_set_page_status(shared, slotno, SLRU_PAGE_VALID);

	pthread_rwlock_unlock(&shared->buffer_locks[slotno]);

	return ok;
}


/*
 * Physical read of a (previously existing) page into a buffer slot
 *
 * For now, assume it's not worth keeping a file pointer open across
 * read/write operations.  We could cache one virtual file pointer ...
 */
static bool
consensus_slru_physical_read_page(ConsensusSlruCtl ctl, uint64 pageno, int slotno)
{
	ConsensusSlruShared	shared = ctl->shared;
	uint64		segno = pageno / CONSENSUS_SLRU_PAGES_PER_SEGMENT;
	int			rpageno = pageno % CONSENSUS_SLRU_PAGES_PER_SEGMENT;
	int			offset = rpageno * BLCKSZ;
	char		path[MAXPGPATH];
	int			fd;
	consensus_slru_stat *stat = &shared->stat;

	stat->n_storage_read_count++;

	consensus_slru_file_name_by_seg(ctl, path, segno);

	easy_debug_log("Consensus slru \"%s\" physical read page, segno: %llu, rpageno: %d", 
			ctl->Dir, segno, rpageno);

	fd = ctl->vfs_api->vfs_open(path, O_RDWR | PG_BINARY, pg_file_create_mode);
	if (fd < 0)
	{
		easy_warn_log("Could not open file \"%s\": %s.", path, strerror(errno));
		return false;
	}

	if (ctl->vfs_api->vfs_lseek(fd, (off_t) offset, SEEK_SET) < 0)
	{
		easy_warn_log("Could not seek in file \"%s\" to offset %u: %s.", path, offset, strerror(errno));
		ctl->vfs_api->vfs_close(fd);
		return false;
	}

	errno = 0;
	if (ctl->vfs_api->vfs_read(fd, shared->page_buffer[slotno], ctl->szblock) != ctl->szblock)
	{
		ctl->vfs_api->vfs_close(fd);
		easy_warn_log("Could not read from file \"%s\" at offset %u: %s.", path, offset, strerror(errno));
		return false;
	}

	if (ctl->vfs_api->vfs_close(fd))
	{
		easy_fatal_log("Could not close file \"%s\": %s.", path, strerror(errno));
		return false;
	}

	return true;
}

/*
 * Physical write of a page from a buffer slot
 *
 * On failure, we cannot just ereport(ERROR) since caller has put state in
 * shared memory that must be undone.  So, we return false and save enough
 * info in static variables to let SlruReportIOError make the report.
 *
 * For now, assume it's not worth keeping a file pointer open across
 * independent read/write operations.  We do batch operations during
 * ConsensusSimpleLruFlush, though.
 *
 * fdata is NULL for a standalone write, pointer to open-file info during
 * ConsensusSimpleLruFlush.
 *
 */
static bool
consensus_slru_physical_write_page(ConsensusSlruCtl ctl, uint64 pageno, int slotno, 
													int offset, ConsensusSlruFlushFile *fdata)
{
	ConsensusSlruShared	shared = ctl->shared;
	uint64		segno = pageno / CONSENSUS_SLRU_PAGES_PER_SEGMENT;
	int			rpageno = pageno % CONSENSUS_SLRU_PAGES_PER_SEGMENT;
	int			segoffset;
	char		path[MAXPGPATH];
	int			fd = -1;
	int 		flag;
	consensus_slru_stat *stat = &shared->stat;

	/* POLAR: slru stat */
	stat->n_storage_write_count++;

	if (fdata && fdata->fd >= 0)
	{
		easy_info_log("Consensus slru \"%s\" reuse segno: %llu (fd: %d)", ctl->Dir, segno, fdata->fd);
		Assert(fdata->segno == segno);
		fd = fdata->fd;
	}

	easy_info_log("Consensus slru \"%s\" physical write page, segno: %llu, rpageno: %d, offset: %d", 
			ctl->Dir, segno, rpageno, offset);

	consensus_slru_file_name_by_seg(ctl, path, segno);

	if (fd < 0)
	{
		flag = O_RDWR | PG_BINARY;
		if (rpageno == 0 || (fdata && fdata->create_if_not_exists))
			flag |= O_CREAT;

		fd = ctl->vfs_api->vfs_open(path, flag, pg_file_create_mode);
		if (fd < 0)
		{
			easy_warn_log("Could not open file \"%s\": %s.", path, strerror(errno));
			return false;
		}
		easy_debug_log("Consensus slru \"%s\" vfs_open, path: %s (fd: %d)", ctl->Dir, path, fd);

		if (fdata && !fdata->fsync)
		{
			fdata->fd = fd;
		}
	}

	segoffset = offset + rpageno * ctl->szblock;

	if (fdata && fdata->create_if_not_exists && 
			ctl->vfs_api->vfs_fallocate(fd, 0, (off_t) (rpageno+1)*ctl->szblock) != 0)
	{
		if (!fdata || fdata->fsync)
			ctl->vfs_api->vfs_close(fd);
		easy_warn_log("Could not seek in file \"%s\" (fd:%d) to offset %u: %s.", path, fd, segoffset, strerror(errno));
		return false;
	}

	easy_debug_log("Consensus slru \"%s\" vfs_lseek path: %s (fd: %d) to offset %u", ctl->Dir, path, fd, segoffset);
	if (ctl->vfs_api->vfs_lseek(fd, (off_t) segoffset, SEEK_SET) < 0)
	{
		if (!fdata || fdata->fsync)
			ctl->vfs_api->vfs_close(fd);
		easy_warn_log("Could not seek in file \"%s\" (fd:%d) to offset %u: %s.", path, fd, segoffset, strerror(errno));
		return false;
	}

	errno = 0;
	easy_debug_log("Consensus slru \"%s\" vfs_write path: %s (fd: %d)", ctl->Dir, path, fd);
	if (ctl->vfs_api->vfs_write(fd, shared->page_buffer[slotno] + offset, ctl->szblock - offset) != 
			ctl->szblock - offset)
	{
		if (errno == 0)
			errno = ENOSPC;
		if (!fdata || fdata->fsync)
			ctl->vfs_api->vfs_close(fd);
		easy_fatal_log("Could not write to file \"%s\" at offset %u: %s.", path, segoffset, strerror(errno));
		return false;
	}

	/*
	 * If not part of Flush, need to fsync now.  We assume this happens
	 * infrequently enough that it's not a performance issue.
	 */
	if ((!fdata || fdata->fsync) && ctl->vfs_api->vfs_fsync(fd))
	{
		ctl->vfs_api->vfs_close(fd);
		easy_fatal_log("Could not fsync file \"%s\": %s.", path, strerror(errno));
		return false;
	}

	if ((!fdata || fdata->fsync) && ctl->vfs_api->vfs_close(fd))
	{
		easy_fatal_log("Could not close file \"%s\": %s.", path, strerror(errno));
		return false;
	}

	return true;
}

/*
 * Select the slot to re-use when we need a free slot.
 *
 * The target page number is passed because we need to consider the
 * possibility that some other process reads in the target page while
 * we are doing I/O to free a slot.  Hence, check or recheck to see if
 * any slot already holds the target page, and return that slot if so.
 * Thus, the returned slot is *either* a slot already holding the pageno
 * (could be any state except EMPTY), *or* a freeable slot (state EMPTY
 * or CLEAN).
 *
 * Control lock must be held at entry, and will be held at exit.
 */
static int
consensus_slru_select_lru_page(ConsensusSlruCtl ctl, uint64 pageno)
{
	ConsensusSlruShared	shared = ctl->shared;
	consensus_slru_stat *stat = &shared->stat;

	/* Outer loop handles restart after I/O */
	for (;;)
	{
		int			slotno;
		int			cur_count;
		int			bestvalidslot = 0;	/* keep compiler quiet */
		int			best_valid_delta = -1;
		int			best_valid_page_number = 0; /* keep compiler quiet */
		int			bestinvalidslot = 0;	/* keep compiler quiet */
		int			best_invalid_delta = -1;
		int			best_invalid_page_number = 0;	/* keep compiler quiet */

		for (slotno = 0; slotno < shared->num_slots; slotno++)
		{
			if (shared->page_number[slotno] == pageno &&
					shared->page_status[slotno] != SLRU_PAGE_EMPTY)
				return slotno;
		}

		cur_count = (shared->cur_lru_count)++;
		for (slotno = shared->victim_pivot; 
				slotno < shared->victim_pivot + CONSENSUS_SLRU_VICTIM_WINDOW && 
				slotno < shared->num_slots;
				slotno++)
		{
			int			this_delta;
			int			this_page_number;

			if (shared->page_status[slotno] == SLRU_PAGE_EMPTY)
				return slotno;

			if (shared->page_dirty[slotno] && 
					shared->page_status[slotno] != SLRU_PAGE_WRITE_IN_PROGRESS)
				continue;

			this_delta = cur_count - shared->page_lru_count[slotno];
			if (this_delta < 0)
			{
				/*
				 * Clean up in case shared updates have caused cur_count
				 * increments to get "lost".  We back off the page counts,
				 * rather than trying to increase cur_count, to avoid any
				 * question of infinite loops or failure in the presence of
				 * wrapped-around counts.
				 */
				shared->page_lru_count[slotno] = cur_count;
				this_delta = 0;
			}

			this_page_number = shared->page_number[slotno];
			if (this_page_number == shared->latest_page_number)
				continue;

			if (shared->page_status[slotno] == SLRU_PAGE_VALID)
			{
				if (this_delta > best_valid_delta ||
					(this_delta == best_valid_delta &&
					 this_page_number < best_valid_page_number))
				{
					bestvalidslot = slotno;
					best_valid_delta = this_delta;
					best_valid_page_number = this_page_number;
				}
			}
			else
			{
				if (this_delta > best_invalid_delta ||
					(this_delta == best_invalid_delta &&
					 this_page_number < best_invalid_page_number))
				{
					bestinvalidslot = slotno;
					best_invalid_delta = this_delta;
					best_invalid_page_number = this_page_number;
				}
			}
		}

		shared->victim_pivot = (slotno == shared->num_slots) ? 0 : slotno;
		stat->n_victim_count++;

		if (best_valid_delta >= 0)
			return bestvalidslot;

		if (best_invalid_delta >= 0)
		{
			consensus_slru_wait_io(ctl, bestinvalidslot);
			continue;
		}

		if (shared->first_dirty_slot != shared->num_slots)
		{
			stat->n_victim_write_count++;
			consensus_slru_try_write_page(ctl, shared->first_dirty_slot);
		}
	}
}

/*
 * Flush dirty pages before pageno 
 *
 * Control lock must be held at entry, and will be held at exit.
 */
bool
ConsensusSimpleLruFlush(ConsensusSlruCtl ctl, uint64 pageno)
{
	ConsensusSlruShared	shared = ctl->shared;
	int			slotno, next_slotno;
	uint64	pagesegno;
	consensus_slru_stat *stat = &shared->stat;
	ConsensusSlruFlushFile fdata;
	bool		ok = true;

	stat->n_slru_flush_count++;

	fdata.fsync = false;
	fdata.create_if_not_exists = false;

	while (ok && shared->first_dirty_slot != shared->num_slots)
	{
		slotno = shared->first_dirty_slot;

		if (pageno > 0 && shared->page_number[slotno] > pageno)
			break;

		pagesegno = shared->page_number[slotno] / CONSENSUS_SLRU_PAGES_PER_SEGMENT;
		fdata.segno = pagesegno;
		fdata.fd = -1;
		while (ok && slotno != shared->num_slots && fdata.segno == pagesegno)
		{
			easy_debug_log("Consensus slru \"%s\" flush page, slotno: %d, pageno: %lld", 
					ctl->Dir, slotno, shared->page_number[slotno]);
			next_slotno = shared->next_dirty_slot[slotno];
			ok = consensus_slru_internal_write_page(ctl, slotno, &fdata);

			slotno = next_slotno;
			if (pageno > 0 && shared->page_number[slotno] > pageno)
				break;

			pagesegno = shared->page_number[slotno] / CONSENSUS_SLRU_PAGES_PER_SEGMENT;
		}
		pthread_rwlock_unlock(shared->control_lock);

		easy_debug_log("Consensus slru \"%s\" fsync segno: %llu (fd: %d)", 
				ctl->Dir, fdata.segno, fdata.fd);
		if (fdata.fd > 0 && ctl->vfs_api->vfs_fsync(fdata.fd))
		{
			easy_fatal_log("Could not fsync file, segno: %llu, fd: %d, error: %s.", 
					fdata.segno, fdata.fd, strerror(errno));
			ctl->vfs_api->vfs_close(fdata.fd);
			pthread_rwlock_wrlock(shared->control_lock);
			return false;
		}

		easy_debug_log("Consensus slru \"%s\" close segno: %llu (fd: %d)", 
				ctl->Dir, fdata.segno, fdata.fd);
		if (fdata.fd > 0 && ctl->vfs_api->vfs_close(fdata.fd))
		{
			easy_fatal_log("Could not close file, segno: %llu, fd: %d, error: %s.", 
					fdata.segno, fdata.fd, strerror(errno));
			pthread_rwlock_wrlock(shared->control_lock);
			return false;
		}

		pthread_rwlock_wrlock(shared->control_lock);
	}

	return ok;
}

/*
 * Flush dirty pages 
 *
 * Control lock must be held at entry, and will be held at exit.
 */
bool
ConsensusSimpleLruWritePage(ConsensusSlruCtl ctl, int slotno, bool create_if_not_exists)
{
	ConsensusSlruFlushFile fdata;

	fdata.segno = -1;
	fdata.fd = -1;
	fdata.fsync = true;
	fdata.create_if_not_exists = create_if_not_exists;

	return consensus_slru_internal_write_page(ctl, slotno, &fdata);
}

void
consensus_slru_push_dirty(ConsensusSlruCtl ctl, int slotno, int write_offset, bool head)
{
	ConsensusSlruShared	shared = ctl->shared;
	
	if (shared->page_dirty[slotno])
		return;

	if (shared->first_dirty_slot == shared->num_slots)
	{
		shared->first_dirty_slot = slotno;
		shared->last_dirty_slot = slotno;
		shared->first_dirty_offset = write_offset; 
	}
	else if (!head)
	{
		Assert(shared->last_dirty_slot < shared->num_slots);
		Assert(shared->page_number[shared->last_dirty_slot] < shared->page_number[slotno]);
		shared->next_dirty_slot[shared->last_dirty_slot] = slotno;
		shared->last_dirty_slot = slotno;
	}
	else
	{
		Assert(shared->page_number[shared->first_dirty_slot] > shared->page_number[slotno]);
		shared->next_dirty_slot[slotno] = shared->first_dirty_slot;
		shared->first_dirty_slot = slotno;
	}
	shared->page_dirty[slotno] = true;
	easy_debug_log("Consensus slru \"%s\" push dirty page, slotno: %d, offset: %d", ctl->Dir, slotno, write_offset);
}

void
consensus_slru_pop_dirty(ConsensusSlruCtl ctl, int slotno)
{
	ConsensusSlruShared	shared = ctl->shared;
	
	if (!shared->page_dirty[slotno])
		return;

	Assert(shared->first_dirty_slot == slotno);

	shared->first_dirty_slot = shared->next_dirty_slot[slotno];
	shared->first_dirty_offset = 0; 

	shared->next_dirty_slot[slotno] = shared->num_slots; 

	if (shared->first_dirty_slot == shared->num_slots)
	{
		shared->last_dirty_slot = shared->num_slots;
	}
	shared->page_dirty[slotno] = false;
	easy_debug_log("Consensus slru \"%s\" pop dirty page, slotno: %d", ctl->Dir, slotno);
}

/*
 * Remove all segments before the one holding the passed page number
 *
 * Control lock must be held at entry, and will be held at exit.
 */
void
ConsensusSimpleLruTruncateForward(ConsensusSlruCtl ctl, uint64 cutoffPage)
{
	ConsensusSlruShared	shared = ctl->shared;
	int			slotno;
	consensus_slru_stat *stat = &shared->stat;
	bool		ok = true;

	stat->n_slru_truncate_forward_count++;

	/*
	 * The cutoff point is the start of the segment containing cutoffPage.
	 */
	cutoffPage -= cutoffPage % CONSENSUS_SLRU_PAGES_PER_SEGMENT;

	/*
	 * While we are holding the lock, make an important safety check: the
	 * planned cutoff point must be <= the current endpoint page. 
	 */
	if (shared->latest_page_number <= cutoffPage)
	{
		return;
	}

	while ((slotno = shared->first_dirty_slot) != shared->num_slots)
	{
		if (shared->page_number[slotno] >= cutoffPage)
			break;

		if (shared->page_status[slotno] == SLRU_PAGE_VALID)
			ok = consensus_slru_internal_write_page(ctl, slotno, NULL);
		else
			consensus_slru_wait_io(ctl, slotno);

		if (!ok)
			break;
	}

	for (slotno = 0; ok && slotno < shared->num_slots; slotno++)
	{ 
		if (shared->page_status[slotno] == SLRU_PAGE_EMPTY)
			continue;

		if (shared->page_number[slotno] >= cutoffPage)
			continue;

		/*
		 * change state to EMPTY (expected case).
		 */
		Assert (shared->page_status[slotno] == SLRU_PAGE_VALID);
		consensus_slru_set_page_status(shared, slotno, SLRU_PAGE_EMPTY);
	}
}

/*
 * empty the page after the one holding the passed page number
 *
 * Control lock must be held at entry, and will be held at exit.
 */
void
ConsensusSimpleLruTruncateBackward(ConsensusSlruCtl ctl, uint64 cutoffPage)
{
	ConsensusSlruShared	shared = ctl->shared;
	int			slotno;
	consensus_slru_stat *stat = &shared->stat;
	bool		ok = true;

	stat->n_slru_truncate_backward_count++;

	Assert (shared->latest_page_number >= cutoffPage);

	for (slotno = 0; ok && slotno < shared->num_slots; slotno++)
	{ 
		if (shared->page_status[slotno] == SLRU_PAGE_EMPTY)
			continue;

		if (shared->page_number[slotno] <= cutoffPage)
			continue;

		/*
		 * change state to EMPTY (expected case).
		 */
		Assert (shared->page_status[slotno] == SLRU_PAGE_VALID);
		consensus_slru_set_page_status(shared, slotno, SLRU_PAGE_EMPTY);
	}
}

/*
 * Delete an individual SLRU segment, identified by the filename.
 *
 * NB: This does not touch the SLRU buffers themselves, callers have to ensure
 * they either can't yet contain anything, or have already been cleaned out.
 */
static void
consensus_slru_internal_delete_segment(ConsensusSlruCtl ctl, char *filename)
{
	char		path[MAXPGPATH];

	consensus_slru_file_name_by_name(ctl, path, filename);
	easy_info_log("removing file \"%s\"", path);
	ctl->vfs_api->vfs_unlink(path);
}

/*
 * ConsensusSlruScanDirectory callback
 *		This callback reports true if there's any segment prior to the one
 *		containing the page passed as "data".
 */
bool
ConsensusSlruScanDirCbReportPresenceForward(ConsensusSlruCtl ctl, 
																			char *filename, uint64 segpage, void *data)
{
	uint64		cutoffPage = *(uint64 *) data;

	cutoffPage -= cutoffPage % CONSENSUS_SLRU_PAGES_PER_SEGMENT;

	if (segpage < cutoffPage)
		return true;			/* found one; don't iterate any more */

	return false;				/* keep going */
}

/*
 * ConsensusConsensusSlruScanDirectory callback.
 *		This callback deletes segments prior to the one passed in as "data".
 */
bool
consensus_slru_scan_dir_callback_delete_cutoff_forward(ConsensusSlruCtl ctl, 
																			char *filename, uint64 segpage, void *data)
{
	uint64		cutoffPage = *(uint64 *) data;

	cutoffPage -= cutoffPage % CONSENSUS_SLRU_PAGES_PER_SEGMENT;

	if (segpage < cutoffPage)
		consensus_slru_internal_delete_segment(ctl, filename);

	return false;				/* keep going */
}

/*
 * ConsensusSlruScanDirectory callback
 *		This callback reports true if there's any segment after the one
 *		containing the page passed as "data".
 */
bool
ConsensusSlruScanDirCbReportPresenceBackward(ConsensusSlruCtl ctl, char *filename, 
		uint64 segpage, void *data)
{
	uint64		cutoffPage = *(uint64 *) data;

	cutoffPage -= cutoffPage % CONSENSUS_SLRU_PAGES_PER_SEGMENT;

	if (segpage > cutoffPage)
		return true;			/* found one; don't iterate any more */

	return false;				/* keep going */
}

/*
 * ConsensusSlruScanDirectory callback.
 *		This callback deletes segments after the one passed in as "data".
 */
bool
consensus_slru_scan_dir_callback_delete_cutoff_backward(ConsensusSlruCtl ctl, 
																		char *filename, uint64 segpage, void *data)
{
	uint64		cutoffPage = *(uint64 *) data;

	cutoffPage -= cutoffPage % CONSENSUS_SLRU_PAGES_PER_SEGMENT;

	if (segpage > cutoffPage)
		consensus_slru_internal_delete_segment(ctl, filename);

	return false;				/* keep going */
}


/*
 * Scan the SimpleLRU directory and apply a callback to each file found in it.
 *
 * If the callback returns true, the scan is stopped.  The last return value
 * from the callback is returned.
 *
 * The callback receives the following arguments: 1. the ConsensusSlruCtl struct for the
 * slru being truncated; 2. the filename being considered; 3. the page number
 * for the first page of that file; 4. a pointer to the opaque data given to us
 * by the caller.
 *
 * Note that the ordering in which the directory is scanned is not guaranteed.
 *
 * Note that no locking is applied.
 */
bool
ConsensusSlruScanDirectory(ConsensusSlruCtl ctl, ConsensusSlruScanCallback callback, void *data)
{
	bool		retval = false;
	DIR		   *cldir;
	struct dirent *clde;
	uint64	segno;
	uint64	segpage;
	char		path[MAXPGPATH];

	consensus_slru_file_dir(ctl, path);
	cldir = ctl->vfs_api->vfs_opendir(path);

	while ((clde = ctl->vfs_api->vfs_readdir(cldir)) != NULL)
	{
		size_t		len;

		len = strlen(clde->d_name);

		if (len == 16 && strspn(clde->d_name, "0123456789ABCDEF") == len)
		{
			segno = (uint64) strtoul(clde->d_name, NULL, 16);
			segpage = segno * CONSENSUS_SLRU_PAGES_PER_SEGMENT;

			easy_info_log("ConsensusSlruScanDirectory invoking callback on %s/%s",
										ctl->Dir, clde->d_name);
			retval = callback(ctl, clde->d_name, segpage, data);
			if (retval)
				break;
		}
	}
	ctl->vfs_api->vfs_closedir(cldir);

	return retval;
}

static void
consensus_slru_file_name_by_seg(ConsensusSlruCtl ctl, char *path, uint64 seg)
{
	if (CONSENSUS_SLRU_FILE_IN_SHARED_STORAGE(ctl))
		snprintf(path, MAXPGPATH, "%s/%s/%016lX", polar_path_remove_protocol(polar_datadir), (ctl)->Dir, seg);
	else
		snprintf(path, MAXPGPATH, "%s/%016lX", (ctl)->Dir, seg);

	return;
}

static void
consensus_slru_file_dir(ConsensusSlruCtl ctl, char *path)
{
	if (CONSENSUS_SLRU_FILE_IN_SHARED_STORAGE(ctl))
		snprintf(path, MAXPGPATH, "%s/%s", polar_path_remove_protocol(polar_datadir), (ctl)->Dir);
	else
		snprintf(path, MAXPGPATH, "%s", (ctl)->Dir);

	return;
}

static void
consensus_slru_file_name_by_name(ConsensusSlruCtl ctl, char *path, char *filename)
{
	if (CONSENSUS_SLRU_FILE_IN_SHARED_STORAGE(ctl))
		snprintf(path, MAXPGPATH, "%s/%s/%s", polar_path_remove_protocol(polar_datadir), ctl->Dir, filename);
	else
		snprintf(path, MAXPGPATH, "%s/%s", ctl->Dir, filename);

	return;
}

static inline void
consensus_slru_set_page_status(ConsensusSlruShared shared, int slotno, ConsensusSlruPageStatus pagestatus)
{
	consensus_slru_stat *stat = &shared->stat;
	ConsensusSlruPageStatus oldstatus = shared->page_status[slotno];

	if (oldstatus != pagestatus)
	{
		stat->n_page_status_stat[oldstatus]--;
		stat->n_page_status_stat[pagestatus]++;
	}

	shared->page_status[slotno] = pagestatus;
}

void
ConsensusSlruStatsInit(void)
{
	if (IsUnderPostmaster)
		return;

	MemSet(consensus_slru_stats, 0, sizeof(consensus_slru_stat *) * CONSENSUS_SLRU_STATS_NUM);
	n_consensus_slru_stats = 0;
}

