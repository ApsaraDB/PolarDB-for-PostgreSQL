#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>
#include "miscadmin.h"

#include "access/polar_logindex.h"
#include "access/polar_logindex_internal.h"
#include "access/polar_fullpage.h"
#include "access/xlog_internal.h"
#include "access/xlog.h"
#include "catalog/pg_control.h"
#include "storage/polar_fd.h"
#include "utils/guc.h"
#include "utils/memutils.h"

static int open_fullpage_file = -1;
static uint64 open_fullpage_file_seg_no = -1;

static polar_fullpage_ctl_t *polar_fullpage_ctl = NULL;


static void remove_old_fullpage_files(log_index_snapshot_t *logindex_snapshot);
static uint64 polar_xlog_read_fullpage_no(log_index_snapshot_t *logindex_snapshot, XLogRecPtr lsn);


/*
 * Initialization of shared memory for fullpage control
 */
Size
polar_fullpage_shmem_size(void)
{
	/* polar_fullpage_ctl_t */
	return sizeof(polar_fullpage_ctl_t);
}

void
polar_fullpage_shmem_init(void)
{
	bool    found_fullpage_ctl = false;

	polar_fullpage_ctl = (polar_fullpage_ctl_t *)
						 ShmemInitStruct("Fullpage Ctl", polar_fullpage_shmem_size(), &found_fullpage_ctl);

	if (!IsUnderPostmaster)
	{
		Assert(!found_fullpage_ctl);
		memset(polar_fullpage_ctl, 0, sizeof(polar_fullpage_ctl_t));

		pg_atomic_init_u64(&polar_fullpage_ctl->max_fullpage_no, 0);

		/* Init fullpage lock */
		LWLockRegisterTranche(LWTRANCHE_FULLPAGE_FILE, "fullpage_file_lock");
		LWLockInitialize(&polar_fullpage_ctl->file_lock.lock, LWTRANCHE_FULLPAGE_FILE);
		LWLockRegisterTranche(LWTRANCHE_FULLPAGE_WRITE, "fullpage_write_lock");
		LWLockInitialize(&polar_fullpage_ctl->write_lock.lock, LWTRANCHE_FULLPAGE_WRITE);

	}
	else
		Assert(found_fullpage_ctl);
}

/*
 * When initializing, we can only get max_fullpage_no from wal file, because we
 * don't save max_fullpage_no in meta file for compatibility.
 * If wal file is removed, it assume fullpage file isn't needed by anyone, just
 * reset fullpage_no to 0
 */
void
polar_logindex_calc_max_fullpage_no(log_index_snapshot_t *logindex_snapshot)
{
	char        path[MAXPGPATH] = {0};
	uint64      fullpage_no = 0;
	struct stat statbuf;
	XLogSegNo   xlog_seg_no = 0;
	XLogRecPtr lsn = InvalidXLogRecPtr;

	if (!POLAR_ENABLE_FULLPAGE_SNAPSHOT())
		return;

	/* if max_fullpage_no has been set, nothing to do */
	if (pg_atomic_read_u64(&polar_fullpage_ctl->max_fullpage_no) > 0)
	{
		elog(LOG, "redo done at max_fullpage_no = %ld", pg_atomic_read_u64(&polar_fullpage_ctl->max_fullpage_no));
		return;
	}

	LWLockAcquire(LOG_INDEX_IO_LOCK, LW_EXCLUSIVE);
	lsn = logindex_snapshot->max_lsn;
	LWLockRelease(LOG_INDEX_IO_LOCK);

	if (XLogRecPtrIsInvalid(lsn))
		return;

	/* check wal file is exist or not */
	XLByteToSeg(lsn, xlog_seg_no, wal_segment_size);
	XLogFilePath(path, ThisTimeLineID, xlog_seg_no, wal_segment_size);

	/* wal file has been removed olready, we cannot get fullpage_no by lsn */
	if (polar_lstat(path, &statbuf) < 0)
	{
		pg_atomic_write_u64(&polar_fullpage_ctl->max_fullpage_no, 0);
		return;
	}
	else
	{
		fullpage_no = polar_xlog_read_fullpage_no(logindex_snapshot, lsn);
		pg_atomic_write_u64(&polar_fullpage_ctl->max_fullpage_no, fullpage_no);
	}

	elog(LOG, "redo done at max_fullpage_no = %ld", pg_atomic_read_u64(&polar_fullpage_ctl->max_fullpage_no));
}

/*
 * POLAR: we update max_fullpage_no when replay wal, we guarantee that fullpage_no
 * keep the same order with wal lsn, so last fullpage_no must be max_fullpage_no
 */
void
polar_update_max_fullpage_no(uint64 fullpage_no)
{
	pg_atomic_write_u64(&polar_fullpage_ctl->max_fullpage_no, fullpage_no);
}

/*
 * Install a new FULLPAGE segment file as a current or future log segment.
 */
static bool
install_fullpage_file_segment(log_index_snapshot_t *logindex_snapshot, uint64 *seg_no,
							  const char *tmppath, uint64 max_segno)
{
	char        path[MAXPGPATH];
	struct stat stat_buf;

	FULLPAGE_SEG_FILE_NAME(path, *seg_no);

	/*
	 * We want to be sure that only one process does this at a time.
	 */
	LWLockAcquire(LOG_INDEX_FULLPAGE_FILE_LOCK, LW_EXCLUSIVE);

	/* Find a free slot to put it in */
	while (polar_stat(path, &stat_buf) == 0)
	{
		if ((*seg_no) >= max_segno)
		{
			/* Failed to find a free slot within specified range */
			LWLockRelease(LOG_INDEX_FULLPAGE_FILE_LOCK);
			return false;
		}

		(*seg_no)++;
		FULLPAGE_SEG_FILE_NAME(path, *seg_no);
	}

	/*
	 * Perform the rename using link if available, paranoidly trying to avoid
	 * overwriting an existing file (there shouldn't be one).
	 */
	if (durable_link_or_rename(tmppath, path, LOG) != 0)
	{
		LWLockRelease(LOG_INDEX_FULLPAGE_FILE_LOCK);
		/* durable_link_or_rename already emitted log message */
		return false;
	}

	LWLockRelease(LOG_INDEX_FULLPAGE_FILE_LOCK);

	return true;
}

static void
fill_fullpage_file_zero_pages(int fd, char *tmppath)
{
#define ONE_MB (1024 * 1024L)
	typedef union PGAlignedFullpageBlock
	{
		char        data[ONE_MB];
		double      force_align_d;
		int64       force_align_i64;
	} PGAlignedFullpageBlock;

	PGAlignedFullpageBlock  polar_zbuffer;
	int     nbytes = 0;

	memset(polar_zbuffer.data, 0, ONE_MB);

	pgstat_report_wait_start(WAIT_EVENT_FULLPAGE_FILE_INIT_WRITE);

	for (nbytes = 0; nbytes < FULLPAGE_SEGMENT_SIZE; nbytes += ONE_MB)
	{
		int     rc = 0;

		errno = 0;
		rc = (int) polar_pwrite(fd, polar_zbuffer.data, ONE_MB, nbytes);

		if (rc != ONE_MB)
		{
			int         save_errno = errno;

			/*
			 * If we fail to make the file, delete it to release disk space
			 */
			polar_unlink(tmppath);

			polar_close(fd);

			/* if write didn't set errno, assume problem is no disk space */
			errno = save_errno ? save_errno : ENOSPC;

			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write to file \"%s\": %m", tmppath)));
		}
	}

	pgstat_report_wait_end();

	if (polar_fsync(fd) != 0)
	{
		int         save_errno = errno;

		polar_close(fd);
		errno = save_errno;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m", tmppath)));
	}

	return;
}

/*
 * Create a new FULLPAGE file segment, or open a pre-existing one.
 */
int
polar_fullpage_file_init(log_index_snapshot_t *logindex_snapshot, uint64 fullpage_no)
{
	char        path[MAXPGPATH];
	char        tmppath[MAXPGPATH];
	uint64  installed_segno;
	uint64  max_segno;
	int         fd;
	char        polar_tmppath[MAXPGPATH];

	FULLPAGE_FILE_NAME(path, fullpage_no);

	/*
	 * Try to use existent file (polar worker maker may have created it already)
	 */

	fd = BasicOpenFile(path, O_RDWR | PG_BINARY | get_sync_bit(sync_method), true);

	if (fd < 0)
	{
		if (errno != ENOENT)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\": %m", path)));
	}
	else
		return fd;

	/*
	 * Initialize an empty (all zeroes) segment.  NOTE: it is possible that
	 * another process is doing the same thing.  If so, we will end up
	 * pre-creating an extra log segment.  That seems OK, and better than
	 * holding the lock throughout this lengthy process.
	 */
	elog(LOG, "creating and filling new FULLPAGE file");

	snprintf(polar_tmppath, MAXPGPATH, POLAR_FULLPAGE_DIR "/fullpagetemp.%d", (int) getpid());
	polar_make_file_path_level2(tmppath, polar_tmppath);
	polar_unlink(tmppath);

	/* do not use get_sync_bit() here --- want to fsync only at end of fill */
	fd = BasicOpenFile(tmppath, O_RDWR | O_CREAT | O_EXCL | PG_BINARY, true);

	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m", tmppath)));

	/* POLAR: File allocate, juse change file metadata once */
	if (polar_fallocate(fd, 0, FULLPAGE_SEGMENT_SIZE) != 0)
		elog(ERROR, "polar_fallocate fail in polar_fullpage_file_init");

	fill_fullpage_file_zero_pages(fd, tmppath);

	if (polar_close(fd))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", tmppath)));

	/*
	 * Now move the segment into place with its final name.
	 *
	 * If caller didn't want to use a pre-existing file, get rid of any
	 * pre-existing file.  Otherwise, cope with possibility that someone else
	 * has created the file while we were filling ours: if so, use ours to
	 * pre-create a future log segment.
	 */
	installed_segno = FULLPAGE_FILE_SEG_NO(fullpage_no);

	max_segno = FULLPAGE_FILE_SEG_NO(fullpage_no) + polar_fullpage_keep_segments;

	if (!install_fullpage_file_segment(logindex_snapshot, &installed_segno, tmppath, max_segno))
	{
		/*
		 * No need for any more future segments, or install_fullpage_file_segment()
		 * failed to rename the file into place. If the rename failed, opening
		 * the file below will fail.
		 */
		polar_unlink(tmppath);
	}

	/* Now open original target segment (might not be file I just made) */
	fd = BasicOpenFile(path, O_RDWR | PG_BINARY | get_sync_bit(sync_method), true);

	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));

	elog(LOG, "done creating and filling new FULLPAGE file %s", path);

	return fd;
}


/*
 * Open a fullpage file segment for read or write.
 *
 */
static int
fullpage_file_open(log_index_snapshot_t *logindex_snapshot, uint64 fullpage_no)
{
	char        path[MAXPGPATH];
	int         fd;

	FULLPAGE_FILE_NAME(path, fullpage_no);

	fd = BasicOpenFile(path, O_RDWR | PG_BINARY, true);

	if (fd >= 0)
		return fd;

	if (errno != ENOENT) /* unexpected failure? */
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));

	return polar_fullpage_file_init(logindex_snapshot, fullpage_no);
}

uint64
polar_log_fullpage_begin(void)
{
	/*
	 * fullpage_no must keep the same order with lsn.
	 */
	LWLockAcquire(LOG_INDEX_FULLPAGE_FILE_LOCK, LW_EXCLUSIVE);

	return pg_atomic_fetch_add_u64(&polar_fullpage_ctl->max_fullpage_no, 1);
}

void
polar_log_fullpage_end(void)
{
	LWLockRelease(LOG_INDEX_FULLPAGE_FILE_LOCK);
}

/*
 * POLAR: write old version page to fullpage file
 */
void
polar_write_fullpage(Page page, uint64 fullpage_no)
{
	if (open_fullpage_file < 0)
	{
		open_fullpage_file = fullpage_file_open(POLAR_LOGINDEX_FULLPAGE_SNAPSHOT, fullpage_no);
		open_fullpage_file_seg_no = FULLPAGE_FILE_SEG_NO(fullpage_no);
	}
	else if (open_fullpage_file_seg_no != FULLPAGE_FILE_SEG_NO(fullpage_no))
	{
		polar_close(open_fullpage_file);
		open_fullpage_file = fullpage_file_open(POLAR_LOGINDEX_FULLPAGE_SNAPSHOT, fullpage_no);
		open_fullpage_file_seg_no = FULLPAGE_FILE_SEG_NO(fullpage_no);
	}

	polar_pwrite(open_fullpage_file, (void *)page, BLCKSZ, FULLPAGE_FILE_OFFSET(fullpage_no));
}

/*
 * POLAR: read old version page from fullpage file
 */
void
polar_read_fullpage(Page page, uint64 fullpage_no)
{
	if (open_fullpage_file < 0)
	{
		open_fullpage_file = fullpage_file_open(POLAR_LOGINDEX_FULLPAGE_SNAPSHOT, fullpage_no);
		open_fullpage_file_seg_no = FULLPAGE_FILE_SEG_NO(fullpage_no);
	}
	else if (open_fullpage_file_seg_no != FULLPAGE_FILE_SEG_NO(fullpage_no))
	{
		polar_close(open_fullpage_file);
		open_fullpage_file = fullpage_file_open(POLAR_LOGINDEX_FULLPAGE_SNAPSHOT, fullpage_no);
		open_fullpage_file_seg_no = FULLPAGE_FILE_SEG_NO(fullpage_no);
	}

	polar_pread(open_fullpage_file, (void *)page, BLCKSZ, FULLPAGE_FILE_OFFSET(fullpage_no));
}

/*
 * Preallocate fullpage files
 */
void
polar_prealloc_fullpage_files(void)
{
	log_index_snapshot_t *logindex_snapshot = POLAR_LOGINDEX_FULLPAGE_SNAPSHOT;
	uint64      max_fullpage_no = 0;
	int     file = 0;
	int     count = 0;

	if (!POLAR_ENABLE_FULLPAGE_SNAPSHOT())
		return;

	max_fullpage_no = pg_atomic_read_u64(&polar_fullpage_ctl->max_fullpage_no);

	while (count < 3)
	{
		file = polar_fullpage_file_init(logindex_snapshot, max_fullpage_no);
		polar_close(file);
		max_fullpage_no += FULLPAGE_NUM_PER_FILE;
		count++;
	}
}


/*
 * Recycle or remove a fullpage file that's no longer needed.
 */
void
polar_remove_old_fullpage_file(log_index_snapshot_t *logindex_snapshot, const char *segname, uint64 min_fullpage_seg_no)
{
	struct stat statbuf;
	uint64 end_fullpage_seg_no = FULLPAGE_FILE_SEG_NO(pg_atomic_read_u64(&polar_fullpage_ctl->max_fullpage_no));
	uint64 recycle_seg_no = min_fullpage_seg_no + polar_fullpage_keep_segments;

	/*
	 * Before deleting the file, see if it can be recycled as a future fullpage
	 * segment.
	 */
	if (end_fullpage_seg_no <= recycle_seg_no &&
			polar_lstat(segname, &statbuf) == 0 && S_ISREG(statbuf.st_mode) &&
			install_fullpage_file_segment(logindex_snapshot, &end_fullpage_seg_no, segname,
										  recycle_seg_no))
	{
		/* POLAR: force log */
		ereport(LOG,
				(errmsg("recycled fullpage file \"%s\"",
						segname)));
	}
	else
	{
		/* No need for any more future segments... */
		int         rc;

		/* POLAR: force log */
		ereport(LOG,
				(errmsg("removing fullpage file \"%s\"",
						segname)));

		rc = durable_unlink(segname, LOG);

		if (rc != 0)
		{
			/* Message already logged by durable_unlink() */
			return;
		}
	}
}

/*
 * Recycle or remove all fullpage files older to passed segno.
 * It's a extern function
 */
void
polar_remove_old_fullpage_files(void)
{
	int i = 0;

	for (i = 0; i < LOGINDEX_SNAPSHOT_NUM; i++)
		remove_old_fullpage_files(polar_logindex_snapshot[i]);
}

static uint64
polar_xlog_read_fullpage_no(log_index_snapshot_t *logindex_snapshot, XLogRecPtr lsn)
{
	XLogRecord *record;
	XLogReaderState *xlogreader;
	char       *errormsg;
	uint64  fullpage_no = 0;

	xlogreader = XLogReaderAllocate(wal_segment_size, &read_local_xlog_page,
									NULL);

	if (!xlogreader)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Failed while allocating a WAL reading processor.")));

	record = XLogReadRecord(xlogreader, lsn, &errormsg);

	if (record == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read fullpage wal state from WAL at %X/%X",
						(uint32)(lsn >> 32),
						(uint32) lsn)));

	if (XLogRecGetRmid(xlogreader) != RM_XLOG_ID ||
			(XLogRecGetInfo(xlogreader) & ~XLR_INFO_MASK) != XLOG_FPSI)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("expected fullpage wal state data is not present in WAL at %X/%X",
						(uint32)(lsn >> 32),
						(uint32) lsn)));

	/* get fullpage_no from record */
	memcpy(&fullpage_no, XLogRecGetData(xlogreader), sizeof(uint64));
	XLogReaderFree(xlogreader);
	return fullpage_no;
}

/*
 * Recycle or remove all fullpage files older to passed segno.
 */
static void
remove_old_fullpage_files(log_index_snapshot_t *logindex_snapshot)
{
	log_index_meta_t            *meta = &logindex_snapshot->meta;
	log_index_file_segment_t    *min_seg = &meta->min_segment_info;
	log_idx_table_data_t *table = NULL;
	char        path[MAXPGPATH] = {0};
	uint64      min_fullpage_seg_no = 0;
	uint64      fullpage_no = 0;
	struct stat statbuf;
	XLogSegNo   xlog_seg_no = 0;
	XLogRecPtr  min_lsn = InvalidXLogRecPtr;

	if (polar_in_replica_mode())
		return;

	if (logindex_snapshot != POLAR_LOGINDEX_FULLPAGE_SNAPSHOT)
		return;

	LWLockAcquire(LOG_INDEX_IO_LOCK, LW_EXCLUSIVE);

	if (meta->crc == 0 || XLogRecPtrIsInvalid(meta->max_lsn)
			|| XLogRecPtrIsInvalid(min_seg->max_lsn)
			|| min_seg->min_idx_table_id == LOG_INDEX_TABLE_INVALID_ID
			|| min_seg->max_idx_table_id == LOG_INDEX_TABLE_INVALID_ID)
	{
		LWLockRelease(LOG_INDEX_IO_LOCK);
		return;
	}

	LWLockRelease(LOG_INDEX_IO_LOCK);

	table = palloc(sizeof(log_idx_table_data_t));

	if (log_index_read_table_data(logindex_snapshot, table, min_seg->min_idx_table_id, LOG) == false)
	{
		POLAR_LOG_LOGINDEX_META_INFO(meta);
		ereport(PANIC,
				(errmsg("Failed to read log index which tid=%ld when truncate logindex",
						min_seg->min_idx_table_id)));
	}
	min_lsn = table->min_lsn;

	pfree(table);

	/* check wal file is exist or not */
	XLByteToSeg(min_lsn, xlog_seg_no, wal_segment_size);
	XLogFilePath(path, ThisTimeLineID, xlog_seg_no, wal_segment_size);

	/* wal file has been removed already, we cannot get fullpage_no by lsn */
	if (polar_lstat(path, &statbuf) < 0)
		return;

	fullpage_no = polar_xlog_read_fullpage_no(logindex_snapshot, min_lsn);
	min_fullpage_seg_no = FULLPAGE_FILE_SEG_NO(fullpage_no);

	while (min_fullpage_seg_no > 0)
	{
		/* Must remove segments older than min_fullpage_seg_no */
		min_fullpage_seg_no--;

		FULLPAGE_SEG_FILE_NAME(path, min_fullpage_seg_no);

		/* File has been removed already */
		if (polar_lstat(path, &statbuf) < 0)
			break;

		elog(LOG, "attempting to remove FULLPAGE segments older than log file %s",
			 path);
		polar_remove_old_fullpage_file(logindex_snapshot, path, min_fullpage_seg_no);
	}
}

/*
 * POLAR: write a WAL record containing a full snapshot image of a page. Caller is
 * responsible for writing the page to disk after calling this routine.
 */
XLogRecPtr
polar_log_fullpage_snapshot_image(Buffer buffer, XLogRecPtr oldest_apply_lsn)
{
	SMgrRelation smgr = NULL;
	int         flags;
	RelFileNode rnode;
	ForkNumber  forkNum;
	BlockNumber blkno;
	XLogRecPtr  recptr;
	BufferDesc *buf_hdr = GetBufferDescriptor(buffer - 1);
	uint64      fullpage_no = 0;
	static char *fullpage = NULL;

	/*
	 * We allocate the old page space once and use it over on each subsequent
	 * call.
	 */
	if (fullpage == NULL)
		fullpage = MemoryContextAlloc(TopMemoryContext, BLCKSZ);

	Assert(!polar_in_replica_mode());

	flags = REGBUF_NO_IMAGE;
	BufferGetTag(buffer, &rnode, &forkNum, &blkno);

	/*
	 * If we have copy buffer, and copy buffer can be flushed,
	 * just treat copy buffer as fullpage, so we can avoid reading
	 * older page from storage
	 */
	if (buf_hdr->copy_buffer &&
			polar_copy_buffer_get_lsn(buf_hdr->copy_buffer) <= oldest_apply_lsn)
		memcpy(fullpage, (char *)CopyBufHdrGetBlock(buf_hdr->copy_buffer), BLCKSZ);
	else
	{
		/* Find smgr relation for buffer */
		smgr = smgropen(rnode, InvalidBackendId);

		smgrread(smgr, forkNum, blkno, (char *) fullpage);

		/* check for garbage data, decrypt page if it has beed encrypted */
		if (!PageIsVerified((Page) fullpage, forkNum))
		{
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("invalid page in block %u of relation %s",
							blkno,
							relpath(smgr->smgr_rnode, forkNum))));
		}
	}

	/*
	 * POLAR: get fullpage write lock, only one process can
	 * write fullpage wal record at the same time, we expected
	 * that fullpage_no keep the same order with lsn, it's used
	 * for easily cleaning fullpage file
	 * NOTE: we assume that XLogInsert is more faster then smgrread
	 * and polar_write_fullpage
	 */
	fullpage_no = polar_log_fullpage_begin();

	XLogBeginInsert();
	XLogRegisterBuffer(0, buffer, flags);
	XLogRegisterData((char *)(&fullpage_no), sizeof(uint64));

	recptr = XLogInsert(RM_XLOG_ID, XLOG_FPSI);

	/* POLAR: release fullpage write lock */
	polar_log_fullpage_end();

	polar_write_fullpage(fullpage, fullpage_no);
	return recptr;
}
