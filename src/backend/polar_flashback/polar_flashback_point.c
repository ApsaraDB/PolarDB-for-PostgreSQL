/*-------------------------------------------------------------------------
 *
 * polar_flashback_point.c
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, Alibaba Group Holding limited
 *
 * IDENTIFICATION
 *    src/backend/polar_flashback/polar_flashback_point.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "miscadmin.h"
#include "polar_flashback/polar_fast_recovery_area.h"
#include "polar_flashback/polar_flashback_log_file.h"
#include "polar_flashback/polar_flashback_log_index.h"
#include "polar_flashback/polar_flashback_point.h"

/* GUCs */
int polar_flashback_point_segments;
int polar_flashback_point_timeout;

void
polar_set_fbpoint_wal_info(flog_buf_ctl_t buf_ctl, XLogRecPtr fbpoint_lsn,
						   pg_time_t fbpoint_time, XLogRecPtr bg_replayed_lsn, bool is_restart_point)
{
	XLogRecPtr prior_fbpoint_lsn;

	SpinLockAcquire(&buf_ctl->info_lck);
	prior_fbpoint_lsn = buf_ctl->wal_info.fbpoint_lsn;

	/*
	 * There are some ignored cases to update WAL information:
	 * 1. The flashback point WAL lsn is unchanged.
	 * 2. The current flashback point lsn less than
	 * prior flashback point lsn in the standby.
	 * 3. Max sequential background replay lsn less than
	 * prior flashback point lsn in the standby.
	 *
	 * NB: But it don't means that there are no flashback log record.
	 */
	if (likely(fbpoint_lsn != prior_fbpoint_lsn))
	{
		if ((fbpoint_lsn > prior_fbpoint_lsn) &&
				(!is_restart_point || (bg_replayed_lsn >= prior_fbpoint_lsn)))
		{
			buf_ctl->wal_info.prior_fbpoint_lsn = prior_fbpoint_lsn;
			buf_ctl->wal_info.fbpoint_lsn = fbpoint_lsn;
			buf_ctl->wal_info.fbpoint_time = fbpoint_time;
		}
		/*no cover begin*/
		else if (!is_restart_point)
		{
			elog(PANIC, "The previous flashback point WAL lsn %X/%X is larger than "
				 "the current flashback point WAL lsn %X/%X in the RW node",
				 (uint32)(prior_fbpoint_lsn >> 32), (uint32) prior_fbpoint_lsn,
				 (uint32)(fbpoint_lsn >> 32), (uint32) fbpoint_lsn);
		}

		/*no cover end*/
	}

	SpinLockRelease(&buf_ctl->info_lck);

	if (unlikely(polar_flashback_log_debug))
	{
		elog(LOG, "The previous flashback point WAL lsn now is %X/%X, "
			 "the current flashback point WAL lsn now is %X/%X",
			 (uint32)(prior_fbpoint_lsn >> 32), (uint32) prior_fbpoint_lsn,
			 (uint32)(fbpoint_lsn >> 32), (uint32) fbpoint_lsn);
	}
}

XLogRecPtr
polar_get_curr_fbpoint_lsn(flog_buf_ctl_t buf_ctl)
{
	XLogRecPtr curr_fbpoint_lsn = InvalidXLogRecPtr;

	SpinLockAcquire(&buf_ctl->info_lck);
	curr_fbpoint_lsn = buf_ctl->wal_info.fbpoint_lsn;
	SpinLockRelease(&buf_ctl->info_lck);
	return curr_fbpoint_lsn;
}

/*
 * POLAR: Get the local flashback point lsn without spinlock.
 *
 * NB: Must call the function before call polar_flog_insert because the
 * add_buf_to_list will use the local flashback point lsn.
 */
XLogRecPtr
polar_get_local_fbpoint_lsn(flog_buf_ctl_t buf_ctl, XLogRecPtr page_lsn, XLogRecPtr redo_lsn)
{
	static XLogRecPtr local_fbpoint_lsn = InvalidXLogRecPtr;

	if (redo_lsn != InvalidXLogRecPtr)
		local_fbpoint_lsn = redo_lsn;
	else if (local_fbpoint_lsn == InvalidXLogRecPtr || page_lsn > local_fbpoint_lsn)
		local_fbpoint_lsn = polar_get_curr_fbpoint_lsn(buf_ctl);

	return local_fbpoint_lsn;
}

void
polar_get_fbpoint_file_path(uint32 seg_no, const char *fra_dir, char *path)
{
	char relative_path[MAXPGPATH];

	snprintf(relative_path, MAXPGPATH, "%s/%s/%08X", fra_dir, FBPOINT_DIR, seg_no);

	polar_make_file_path_level2(path, relative_path);
}

static int
fbpoint_file_init(const char *fra_dir, uint32 seg_no, fbpoint_io_error_t *io_error)
{
	char        path[MAXPGPATH];
	int fd;

	polar_get_fbpoint_file_path(seg_no, fra_dir, path);
	fd = BasicOpenFile(path, O_RDWR | PG_BINARY, true);

	if (fd < 0)
	{
		if (errno == ENOENT)
		{
			fd = BasicOpenFile(path, O_RDWR | O_CREAT | O_EXCL | PG_BINARY, true);

			if (fd < 0)
			{
				io_error->errcause = FBPOINT_FILE_CREATE_FAILED;
				io_error->save_errno = errno;
			}
			else
			{
				polar_fill_segment_file_zero(fd, path, FBPOINT_SEG_SIZE,
											 WAIT_EVENT_FLASHBACK_POINT_FILE_WRITE, WAIT_EVENT_FLASHBACK_POINT_FILE_SYNC,
											 "checkpoint info");
				elog(DEBUG2, "done creating and filling new flashback point file %s", path);
			}
		}
		else
		{
			io_error->errcause = FBPOINT_FILE_OPEN_FAILED;
			io_error->save_errno = errno;
		}
	}

	return fd;
}

static pg_crc32c
fbpoint_page_comp_crc(fbpoint_page_header_t *header)
{
	pg_crc32c   crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, (char *) header + FBPOINT_PAGE_HEADER_SIZE, FBPOINT_PAGE_SIZE - FBPOINT_PAGE_HEADER_SIZE);
	COMP_CRC32C(crc, (char *) header, offsetof(fbpoint_page_header_t, crc));
	FIN_CRC32C(crc);

	return crc;
}

static void
insert_fbpoint_rec(fpoint_ctl_t ctl, fbpoint_rec_data_t *rec_data)
{
	fbpoint_rec_data_t *start_pos;
	uint64  fbpoint_rec_no;

	fbpoint_rec_no = ctl->next_fbpoint_rec_no;

	/* Update the flashback point record, hold the exclusive lock to protect the memory */
	LWLockAcquire(&ctl->fbpoint_rec_buf_lock, LW_EXCLUSIVE);

	/* A new segment, so reset the buffer */
	if (fbpoint_rec_no % FBPOINT_REC_PER_SEG == 0)
		MemSet(ctl->fbpoint_rec_buf, 0, FBPOINT_REC_END_POS);

	start_pos = (fbpoint_rec_data_t *) FBPOINT_GET_OFFSET_BY_REC_NO(ctl->fbpoint_rec_buf, fbpoint_rec_no);
	memcpy(start_pos, rec_data, sizeof(fbpoint_rec_data_t));

	LWLockRelease(&ctl->fbpoint_rec_buf_lock);
}

/*
 * Search right flashback point record whose time is newest of older than or equals to
 * the target time within a segment.
 *
 * NB: Please make true the minimal flashback point record is older than or equals to
 * the target time.
 */
static uint64
search_right_fbpoint_rec(char *start_ptr, pg_time_t target_time, uint32 seg_no,
						 uint64 max_rec_no)
{
	uint64 l = 0;
	uint64 r = 0;
	fbpoint_rec_data_t *first_fbpoint_rec;

	l = seg_no * FBPOINT_REC_PER_SEG;
	first_fbpoint_rec = (fbpoint_rec_data_t *) FBPOINT_GET_OFFSET_BY_REC_NO(start_ptr, l);
	/* The time of the minal record will be older than or equals to the target time */
	Assert(first_fbpoint_rec->time <= target_time);
	r = Min(max_rec_no, (seg_no + 1) * FBPOINT_REC_PER_SEG - 1);

	/* The first is which we want */
	if (unlikely(first_fbpoint_rec->time == target_time))
		return l;

	while (l < r)
	{
		uint64 mid = (l + r + 1) / 2;
		fbpoint_rec_data_t *fbpoint_rec;

		fbpoint_rec = (fbpoint_rec_data_t *) FBPOINT_GET_OFFSET_BY_REC_NO(start_ptr, mid);

		if (fbpoint_rec->time < target_time)
			l = mid;
		else if (unlikely(fbpoint_rec->time == target_time))
			return mid;
		else
		{
			/* Can't overflow in here */
			Assert(mid > 0);
			r = mid - 1;
		}
	}

	return l;
}

void
polar_fbpoint_report_io_error(const char *fra_dir, fbpoint_io_error_t *io_error, int log_level)
{
	char path[MAXPGPATH];

	polar_get_fbpoint_file_path(io_error->segno, fra_dir, path);
	errno = io_error->save_errno;

	switch (io_error->errcause)
	{
		case FBPOINT_FILE_CREATE_FAILED:
			ereport(log_level,
					(errcode_for_file_access(),
					 "Could not create file \"%s\": %m.", path));
			break;

		case FBPOINT_FILE_OPEN_FAILED:
			ereport(log_level,
					(errcode_for_file_access(),
					 "Could not open file \"%s\": %m.", path));
			break;

		case FBPOINT_FILE_READ_FAILED:
			ereport(log_level,
					(errcode_for_file_access(),
					 "Could not read from file \"%s\" at offset %u: %m.",
					 path, io_error->offset));
			break;

		case FBPOINT_FILE_WRITE_FAILED:
			ereport(log_level,
					(errcode_for_file_access(),
					 "Could not write to file \"%s\" at offset %u: %m.",
					 path, io_error->offset));
			break;

		case FBPOINT_FILE_FSYNC_FAILED:
			ereport(data_sync_elevel(log_level),
					(errcode_for_file_access(),
					 "Could not fsync file \"%s\": %m.", path));
			break;

		case FBPOINT_FILE_CLOSE_FAILED:
			ereport(log_level,
					(errcode_for_file_access(),
					 "Could not close file \"%s\": %m.",  path));
			break;

		default:
			/* can't get here, we trust */
			elog(PANIC, "unrecognized flashback point file io error cause: %d", (int) io_error->errcause);
			break;
	}
}

bool
polar_read_fbpoint_file(const char *fra_dir, char *data, uint32 seg_no, uint32 offset, uint32 size, fbpoint_io_error_t *io_error)
{
	char    path[MAXPGPATH];
	int     fd;
	ssize_t     read_len;

	Assert(size <= FBPOINT_SEG_SIZE);
	/* Init the io error */
	io_error->segno = seg_no;
	io_error->size = size;
	io_error->offset = offset;

	polar_get_fbpoint_file_path(seg_no, fra_dir, path);
	fd = polar_open_transient_file(path, O_RDONLY | PG_BINARY);

	if (fd < 0)
	{
		/*no cover begin*/
		io_error->save_errno = errno;
		io_error->errcause = FBPOINT_FILE_OPEN_FAILED;
		return false;
		/*no cover end*/
	}

	pgstat_report_wait_start(WAIT_EVENT_FLASHBACK_POINT_FILE_READ);
	read_len = polar_pread(fd, data, size, offset);

	if (read_len != size)
	{
		/*no cover begin*/
		io_error->save_errno = errno;
		io_error->errcause = FBPOINT_FILE_READ_FAILED;
		io_error->io_return = read_len;
		CloseTransientFile(fd);
		return false;
		/*no cover end*/
	}

	pgstat_report_wait_end();

	if (CloseTransientFile(fd))
	{
		/*no cover begin*/
		io_error->save_errno = errno;
		io_error->errcause = FBPOINT_FILE_CLOSE_FAILED;
		return false;
		/*no cover end*/
	}

	return true;
}

/*
 * POLAR: Write and sync the flashback point file.
 *
 * NB: we use static fd and seg_no_open and use BasicOpenFile to avoid to be closed by vfd.
 */
bool
polar_write_fbpoint_file(const char *fra_dir, char *data, uint32 seg_no, uint32 offset, uint32 size, fbpoint_io_error_t *io_error)
{
	static int fd = -1;
	static uint32  seg_no_open = 0;
	uint32  write_len;

	/* Init the io error */
	io_error->segno = seg_no_open;
	io_error->size = size;
	io_error->offset = offset;

	if (fd < 0 || seg_no_open != seg_no)
	{
		if (fd >= 0 && polar_close(fd))
		{
			/*no cover begin*/
			io_error->errcause = FBPOINT_FILE_CLOSE_FAILED;
			io_error->save_errno = errno;
			/*no cover end*/
		}

		seg_no_open = seg_no;
		io_error->segno = seg_no;
		fd = fbpoint_file_init(fra_dir, seg_no, io_error);

		if (fd < 0)
			return false;
	}

	/* Write data */
	pgstat_report_wait_start(WAIT_EVENT_FLASHBACK_POINT_FILE_WRITE);
	write_len = (uint32) polar_pwrite(fd, data, size, offset);

	if (write_len != size)
	{
		/*no cover begin*/
		pgstat_report_wait_end();
		io_error->errcause = FBPOINT_FILE_WRITE_FAILED;
		io_error->save_errno = errno ? errno : ENOSPC;
		io_error->io_return = write_len;
		return false;
		/*no cover end*/
	}

	pgstat_report_wait_end();

	/* Sync file */
	pgstat_report_wait_start(WAIT_EVENT_FLASHBACK_POINT_FILE_SYNC);

	if (polar_fsync(fd) != 0)
	{
		/*no cover begin*/
		pgstat_report_wait_end();
		io_error->errcause = FBPOINT_FILE_FSYNC_FAILED;
		io_error->save_errno = errno;
		return false;
		/*no cover end*/
	}

	pgstat_report_wait_end();

	return true;
}

static bool
read_fbpoint_records(const char *fra_dir, char *buf, uint32 seg_no, uint64 max_rec_no, bool ignore_noent)
{
	fbpoint_io_error_t io_error;

	if (polar_read_fbpoint_file(fra_dir, buf, seg_no, 0, FBPOINT_REC_END_POS, &io_error))
	{
		uint64 page_no;
		uint64 max_page_no;
		fbpoint_page_header_t *header;

		/* Get the max page no has record */
		if (FBPOINT_REC_IN_SEG(max_rec_no, seg_no))
			max_page_no = FBPOINT_GET_PAGE_NO_BY_REC_NO(max_rec_no);
		else
			max_page_no = FBPOINT_PAGE_PER_SEG - 1;

		/* Check the crc of each page */
		header = (fbpoint_page_header_t *) buf;

		for (page_no = 0; page_no <= max_page_no; page_no++)
		{
			pg_crc32c crc;

			/* Verify CRC */
			crc = fbpoint_page_comp_crc(header);

			if (!EQ_CRC32C(crc, header->crc))
			{
				/*no cover begin*/
				char    path[MAXPGPATH];

				polar_get_fbpoint_file_path(seg_no, fra_dir, path);
				ereport(FATAL,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("calculated CRC checksum does not match value stored in file \"%s\" page %lu",
								path, page_no)));
				/*no cover end*/
			}

			header = (fbpoint_page_header_t *)((char *) header + FBPOINT_PAGE_SIZE);
		}
	}
	/* Ignore report the io error when the file is non-existence */
	else if (ignore_noent && io_error.errcause == FBPOINT_FILE_OPEN_FAILED && io_error.save_errno == ENOENT)
		return false;
	else
		polar_fbpoint_report_io_error(fra_dir, &io_error, ERROR);

	return true;
}

static void
startup_fbpoint_rec_buf(fpoint_ctl_t ctl, const char *fra_dir)
{
	uint64 next_fbpoint_rec_no = ctl->next_fbpoint_rec_no;

	if (next_fbpoint_rec_no % FBPOINT_REC_PER_SEG != 0)
	{
		uint32 seg_no = FBPOINT_GET_SEG_NO_BY_REC_NO(next_fbpoint_rec_no);

		read_fbpoint_records(fra_dir, ctl->fbpoint_rec_buf, seg_no, next_fbpoint_rec_no - 1, false);
	}
}

static void
write_fbpoint_rec(fpoint_ctl_t ctl, const char *fra_dir)
{
	uint64      rec_no;
	uint32      start_pos;
	uint32      seg_no;
	char        *start_ptr;
	fbpoint_page_header_t *header;
	fbpoint_io_error_t io_error;

	/* Fill the info page header */
	rec_no = ctl->next_fbpoint_rec_no;
	start_pos = FBPOINT_GET_PAGE_NO_BY_REC_NO(rec_no) * FBPOINT_PAGE_SIZE;
	seg_no = FBPOINT_GET_SEG_NO_BY_REC_NO(rec_no);
	start_ptr = ctl->fbpoint_rec_buf + start_pos;
	header = (fbpoint_page_header_t *) start_ptr;
	header->version = FBPOINT_REC_VERSION;
	header->crc = fbpoint_page_comp_crc(header);

	if (!polar_write_fbpoint_file(fra_dir, start_ptr, seg_no, start_pos, FBPOINT_PAGE_SIZE, &io_error))
		/*no cover line*/
		polar_fbpoint_report_io_error(fra_dir, &io_error, ERROR);
}

/* POLAR: Is the page first modified after the redo_lsn (or flashback point lsn) */
bool
polar_is_page_first_modified(flog_buf_ctl_t buf_ctl, XLogRecPtr page_lsn, XLogRecPtr redo_lsn)
{
	bool is_first_modified = false;
	XLogRecPtr curr_fbpoint_lsn = InvalidXLogRecPtr;

	curr_fbpoint_lsn = polar_get_local_fbpoint_lsn(buf_ctl, page_lsn, redo_lsn);

	is_first_modified = (page_lsn <= curr_fbpoint_lsn);

	if (unlikely(polar_flashback_log_debug && is_first_modified))
		elog(LOG, "The page lsn %X/%X is the first modify after the current flashback point lsn %X/%X",
			 (uint32)(page_lsn >> 32), (uint32) page_lsn,
			 (uint32)(curr_fbpoint_lsn >> 32), (uint32) curr_fbpoint_lsn);

	return is_first_modified;
}

XLogRecPtr
polar_get_prior_fbpoint_lsn(flog_buf_ctl_t buf_ctl)
{
	XLogRecPtr fbpoint_lsn;

	SpinLockAcquire(&buf_ctl->info_lck);
	fbpoint_lsn = buf_ctl->wal_info.prior_fbpoint_lsn;

	/*
	 * The prior_fbpoint_lsn is 0 before the first flashback point.
	 * We can't set the flashback log record redo lsn is 0, just set
	 * to wal_info.fbpoint_lsn;
	 */
	if (XLogRecPtrIsInvalid(fbpoint_lsn))
		fbpoint_lsn = buf_ctl->wal_info.fbpoint_lsn;

	SpinLockRelease(&buf_ctl->info_lck);
	return fbpoint_lsn;
}

/*
 * Get the flashback point start pointer.
 * The flog_start_ptr will be updated by flashback point.
 */
polar_flog_rec_ptr
polar_get_fbpoint_start_ptr(flog_buf_ctl_t ctl)
{
	polar_flog_rec_ptr ptr;

	LWLockAcquire(&ctl->ctl_file_lock, LW_SHARED);
	ptr = ctl->fbpoint_info.flog_start_ptr;
	LWLockRelease(&ctl->ctl_file_lock);
	return ptr;
}

/*
 * POLAR: Check whether we've consumed enough xlog space or time that
 * a flashback point is needed.
 */
bool
polar_is_flashback_point(flog_ctl_t instance, XLogRecPtr checkpoint_lsn, XLogRecPtr bg_replayed_lsn,
						 int *flags, bool is_restart_point)
{
	XLogRecPtr  fbpoint_lsn;
	pg_time_t   fbpoint_time;
	pg_time_t   now;
	flog_buf_ctl_t buf_ctl;
	XLogRecPtr  prior_fbpoint_lsn;
	XLogSegNo   old_segno;
	XLogSegNo   new_segno;
	bool is_shutdown = *flags & CHECKPOINT_IS_SHUTDOWN;

	if (!polar_is_flog_enabled(instance))
		return false;

	buf_ctl = instance->buf_ctl;
	Assert(POLAR_IS_FLOG_BUF_READY(buf_ctl));

	SpinLockAcquire(&buf_ctl->info_lck);
	fbpoint_lsn = buf_ctl->wal_info.fbpoint_lsn;
	fbpoint_time =  buf_ctl->wal_info.fbpoint_time;
	prior_fbpoint_lsn = buf_ctl->wal_info.prior_fbpoint_lsn;
	SpinLockRelease(&buf_ctl->info_lck);

	/*
	 * Something has not init in end of recovery checkpoint,
	 * but flashback point need it (like ShmemVariableCache->latestCompletedXid), so skip it.
	 */
	if (unlikely(*flags & CHECKPOINT_END_OF_RECOVERY))
		return false;

	/* First checkpoint after flashback log enable is a flashback point */
	if (unlikely(prior_fbpoint_lsn == InvalidXLogRecPtr))
	{
		elog(LOG, "It is a first checkpoint after flashback log enable, treat it as a flashback point.");
		*flags = *flags | CHECKPOINT_FLASHBACK;
		return true;
	}

	/* If it is a shutdown checkpoint, it will be treated as a flashback point */
	if (unlikely(is_shutdown))
	{
		elog(LOG, "It is a shutdown checkpoint, treat it as a flashback point.");
		*flags = *flags | CHECKPOINT_FLASHBACK;
		return true;
	}

	/* Process the parallel standby mode */
	if (bg_replayed_lsn > InvalidXLogRecPtr && bg_replayed_lsn < prior_fbpoint_lsn)
	{
		Assert(prior_fbpoint_lsn > InvalidXLogRecPtr);

		return false;
	}

	/*
	 * Ugh, it is a end of recovery checkpoint or lazy checkpoint,
	 * it cann't get a right snapshot, so return false.
	 */
	if ((*flags & CHECKPOINT_END_OF_RECOVERY) || (*flags & CHECKPOINT_LAZY))
		return false;

	/* There are nothing to do */
	if (unlikely(checkpoint_lsn == fbpoint_lsn))
		return false;

	/*
	 * In some cases, the checkpoint lsn is less than
	 * the previous lsn in the standby mode.
	 */
	if (unlikely(checkpoint_lsn < fbpoint_lsn))
	{
		if (is_restart_point)
			return false;
		else
			/*no cover line*/
			elog(FATAL, "The WAL lsn %X/%X is less than last flashback point lsn %X/%X, "
				 "something is wrong.", (uint32)(checkpoint_lsn >> 32), (uint32)(checkpoint_lsn),
				 (uint32)(fbpoint_lsn >> 32), (uint32)(fbpoint_lsn));
	}

	/* Check the timeout */
	now = (pg_time_t) time(NULL);

	if (now - fbpoint_time >= polar_flashback_point_timeout)
	{
		elog(LOG, "The checkpoint is treated as a flashback point cause to timeout");
		*flags = *flags | CHECKPOINT_FLASHBACK;
		return true;
	}

	/* Check the WAL segments */
	XLByteToSeg(fbpoint_lsn, old_segno, wal_segment_size);
	XLByteToSeg(checkpoint_lsn, new_segno, wal_segment_size);

	if ((new_segno - old_segno) >= polar_flashback_point_segments)
	{
		elog(LOG, "The checkpoint is treated as a flashback point cause to WAL distance");
		*flags = *flags | CHECKPOINT_FLASHBACK;
		return true;
	}

	return false;
}

Size
polar_flashback_point_shmem_size(void)
{
	Size        size = 0;

	/* flashback point control data */
	size = sizeof(fbpoint_ctl_data_t);
	/* extra alignment padding for checkpoint info I/O buffers */
	size = add_size(size, FBPOINT_PAGE_SIZE);
	/* The flashback point record file is so small, so we can load it in the buffer */
	size = add_size(size, FBPOINT_REC_END_POS);

	return size;
}

void
polar_flashback_point_shmem_init_data(fpoint_ctl_t ctl, const char *name)
{
	char *allocptr;
	char buf_lock_name[FL_OBJ_MAX_NAME_LEN];

	MemSet(ctl, 0, sizeof(fbpoint_ctl_data_t));
	allocptr = (char *) ctl + sizeof(fbpoint_ctl_data_t);
	allocptr = (char *) TYPEALIGN(FBPOINT_PAGE_SIZE, allocptr);
	ctl->fbpoint_rec_buf = allocptr;
	MemSet(ctl->fbpoint_rec_buf, 0, FBPOINT_REC_END_POS);

	FLOG_GET_OBJ_NAME(buf_lock_name, name, FBPOINT_REC_BUF_LOCK_NAME_SUFFIX);
	LWLockRegisterTranche(LWTRANCHE_POLAR_FLASHBACK_POINT_REC_BUF, buf_lock_name);
	LWLockInitialize(&ctl->fbpoint_rec_buf_lock, LWTRANCHE_POLAR_FLASHBACK_POINT_REC_BUF);
}

fpoint_ctl_t
polar_flashback_point_shmem_init(const char *name)
{
	fpoint_ctl_t ctl;
	bool found;
	char ctl_name[FL_OBJ_MAX_NAME_LEN];

	FLOG_GET_OBJ_NAME(ctl_name, name, FBPOINT_CTL_NAME_SUFFIX);

	ctl = (fpoint_ctl_t)ShmemInitStruct(ctl_name, polar_flashback_point_shmem_size(), &found);

	if (!IsUnderPostmaster)
	{
		Assert(!found);
		polar_flashback_point_shmem_init_data(ctl, name);
	}
	else
		Assert(found);

	return ctl;
}

void
polar_startup_flashback_point(fpoint_ctl_t ctl, const char *fra_dir, uint64 next_fbpoint_rec_no)
{
	ctl->next_fbpoint_rec_no = next_fbpoint_rec_no;
	startup_fbpoint_rec_buf(ctl, fra_dir);
}

/*
 * POLAR: Get the right flashback point record
 * whose time is last one less than or equal to target_time.
 * Return true when found, otherwise return false.
 *
 * NB: When keep_seg_no isn't NULL, we will get the oldest flashback point record
 * while we can't find a right flashback point and the keep_seg_no will be set to
 * the minimal seg_no.
 */
bool
polar_get_right_fbpoint(fpoint_ctl_t ctl, const char *fra_dir,
						pg_time_t target_time, fbpoint_rec_data_t *result, uint32 *keep_seg_no)
{
	fbpoint_rec_data_t *fbpoint_rec;
	uint64  max_rec_no = ctl->next_fbpoint_rec_no;
	uint32  seg_no;
	char *start_ptr;
	bool found = false;
	bool is_in_memory = true;

	if (max_rec_no == 0)
		return false;
	else
		max_rec_no--;

	seg_no = FBPOINT_GET_SEG_NO_BY_REC_NO(max_rec_no);
	start_ptr = ctl->fbpoint_rec_buf;

	/* Search from memory need buf lock */
	LWLockAcquire(&ctl->fbpoint_rec_buf_lock, LW_SHARED);

	do
	{
		fbpoint_rec = FBPOINT_GET_FIRST_REC_IN_SEG(start_ptr);

		/* Find the segment no */
		if (fbpoint_rec->time <= target_time)
		{
			uint64 rec_no;

			rec_no = search_right_fbpoint_rec(start_ptr, target_time, seg_no, max_rec_no);
			fbpoint_rec = (fbpoint_rec_data_t *) FBPOINT_GET_OFFSET_BY_REC_NO(start_ptr, rec_no);
			Assert(fbpoint_rec->time <= target_time);
			memcpy(result, fbpoint_rec, sizeof(fbpoint_rec_data_t));

			if (keep_seg_no)
				*keep_seg_no = seg_no;

			found = true;
			break;
		}

		/* We can't find the right one so we will keep the minimal one */
		if (keep_seg_no)
		{
			memcpy(result, fbpoint_rec, sizeof(fbpoint_rec_data_t));
			*keep_seg_no = seg_no;
			found = true;
		}

		if (seg_no == 0)
			break;

		if (is_in_memory)
		{
			LWLockRelease(&ctl->fbpoint_rec_buf_lock);
			is_in_memory = false;
			start_ptr = palloc(FBPOINT_REC_END_POS);
		}

		Assert(seg_no > 0);
		seg_no--;
	}
	while (read_fbpoint_records(fra_dir, start_ptr, seg_no, max_rec_no, true));

	if (is_in_memory)
		LWLockRelease(&ctl->fbpoint_rec_buf_lock);
	else
		pfree(start_ptr);

	return found;
}

void
polar_flush_fbpoint_rec(fpoint_ctl_t point_ctl, const char *fra_dir,
						fbpoint_rec_data_t *rec_data)
{
	insert_fbpoint_rec(point_ctl, rec_data);
	write_fbpoint_rec(point_ctl, fra_dir);
	point_ctl->next_fbpoint_rec_no++;
}

void
polar_truncate_fbpoint_files(const char *fra_dir, uint32 keep_seg_no)
{
	char        path[MAXPGPATH];

	/* There is only one file, just return */
	if (keep_seg_no == 0)
		return;
	else
		keep_seg_no--;

	polar_get_fbpoint_file_path(keep_seg_no, fra_dir, path);

	while (polar_file_exists(path))
	{
		durable_unlink(path, ERROR);

		/* There is nothing */
		if (keep_seg_no == 0)
			break;

		keep_seg_no--;
		polar_get_fbpoint_file_path(keep_seg_no, fra_dir, path);
	}
}

/*
 * POLAR: get the flashback point keep record.
 *
 * Return segment no of flashback point keep record.
 *
 * TODO: Protect the flashback point used by flashback table.
 */
uint32
polar_get_keep_fbpoint(fpoint_ctl_t ctl, const char *fra_dir, fbpoint_rec_data_t *fbpoint_rec,
					   polar_flog_rec_ptr *keep_ptr, XLogRecPtr *keep_lsn)
{
	pg_time_t now;
	pg_time_t keep_time;
	uint32 seg_no = 0;

	/* The fbpoint record must be valid */
	Assert(fbpoint_rec->time);
	now = (pg_time_t) time(NULL);
	keep_time = now - polar_fast_recovery_area_rotation * SECS_PER_MINUTE;

	polar_get_right_fbpoint(ctl, fra_dir, keep_time, fbpoint_rec, &seg_no);

	/* Update the flashback log keep pointer and wal keep lsn */
	*keep_ptr = Min(*keep_ptr, fbpoint_rec->flog_ptr);
	*keep_lsn = fbpoint_rec->redo_lsn;

	return seg_no;
}
