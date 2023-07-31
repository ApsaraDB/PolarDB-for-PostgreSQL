/*-------------------------------------------------------------------------
 *
 * polar_flashback.c
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, Alibaba Group Holding limited
 *
 * IDENTIFICATION
 *    src/backend/polar_flashback/polar_flashback.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "polar_flashback/polar_flashback.h"
#include "polar_flashback/polar_flashback_log.h"

/*
 * POLAR: Write a small (less than 512B) control info file atomically.
 *
 * When the control file doesn't exist, create and write it are not atomic.
 * So we create a tmp one, write it and rename it to make the operation atomic.
 * However, something the tmp one will be left, we think it is harmless and
 * will use it again.
 *
 * NB: We think the control file is very important, so error report with PANIC.
 */
void
polar_write_ctl_file_atomic(const char *path, void *data, size_t size,
							uint32 write_event_info, uint32 fsync_event_info)
{
#define TMP_FILE_SUFFIX "tmp"
	int fd;
	int file_flags = O_RDWR | PG_BINARY;
	bool need_rename = false;
	char    tmp_path[MAXPGPATH];

	fd = BasicOpenFile(path, file_flags, true);

	/* Create a tmp file */
	if (fd < 0)
	{
		if (errno == ENOENT)
		{
			/* The ctl file doesn't exist, so create a tmp one and write */
			need_rename = true;
			file_flags |= (O_CREAT | O_EXCL);
			/* Write to tmp file to avoid create a empty file and crash */
			snprintf(tmp_path, MAXPGPATH, "%s%s", path, TMP_FILE_SUFFIX);
			polar_unlink(tmp_path);

			fd = BasicOpenFile(tmp_path, file_flags, true);

			if (fd < 0)
			{
				/*no cover begin*/
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("could not open file \"%s\": %m", tmp_path)));
				/*no cover end*/
			}
		}
		else
			/*no cover line*/
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\": %m", path)));
	}

	/* Write it */
	pgstat_report_wait_start(write_event_info);

	/* Log error */
	if (polar_write(fd, data, size) != size)
	{
		/*no cover begin*/
		int         save_errno = errno;

		/*
		 * If we fail to write the file, delete it to release disk
		 * space.
		 */
		if (need_rename)
			polar_unlink(tmp_path);

		/* if write didn't set errno, assume problem is no disk space */
		errno = save_errno ? save_errno : ENOSPC;
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m", (need_rename ? tmp_path : path))));
		/*no cover end*/
	}

	pgstat_report_wait_end();

	/* Fsync it */
	pgstat_report_wait_start(fsync_event_info);

	if (polar_fsync(fd) != 0)
	{
		/*no cover line*/
		pgstat_report_wait_end();
		ereport(PANIC,
				(errcode_for_file_access(),
				 (errmsg("could not sync file \"%s\": %m", (need_rename ? tmp_path : path)))));
	}

	pgstat_report_wait_end();

	if (polar_close(fd))
		/*no cover line*/
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", (need_rename ? tmp_path : path))));

	/* Rename the tmp one to real one */
	if (unlikely(need_rename))
		polar_durable_rename(tmp_path, path, PANIC);
}

/*
 * POLAR: Get all flashback relative memory.
 */
Size
polar_flashback_shmem_size(void)
{
	Size size = 0;

	/* Check the guc */
	if (polar_enable_flashback_log)
	{
		/*no cover begin*/
		if (polar_flashback_logindex_mem_size == 0)
			elog(FATAL, "Cannot enable flashback log when \"polar_flashback_logindex_mem_size\" is zero.");

		if (polar_logindex_mem_size == 0)
			elog(FATAL, "Cannot enable flashback log when \"polar_logindex_mem_size\" is zero.");

		/*no cover end*/

		size = POLAR_FLOG_SHMEM_SIZE();

		if (polar_enable_fast_recovery_area)
			return add_size(size, polar_fra_shmem_size());
	}

	return size;
}

/*
 * POLAR: Initialization of shared memory for flashback log internal function
 */
void
polar_flashback_shmem_init(void)
{
	if (polar_is_flog_mem_enabled())
	{
		POLAR_FLOG_SHMEM_INIT();

		if (polar_enable_fast_recovery_area)
			FRA_SHMEM_INIT();
	}
}

/*
 * POLAR: startup all about flashback.
 */
void
polar_startup_flashback(CheckPoint *checkpoint)
{
	/* Just start up in non-replica node */
	if (!polar_in_replica_mode())
	{
		if (flog_instance == NULL)
		{
			polar_remove_all_flog_data(flog_instance);
			Assert(fra_instance == NULL);
			FRA_REMOVE_ALL_DATA();
			return;
		}
		else
			polar_startup_flog(checkpoint, flog_instance);

		if (fra_instance == NULL)
			FRA_REMOVE_ALL_DATA();
		else
			polar_startup_fra(fra_instance);
	}
}

/*
 * POLAR: Do something after the checkpoint is done.
 *
 * 1. Flush the fast recovery data and remove the old data.
 * 2. Flush the flashback log data and remove the old data.
 */
void
polar_do_flashback_point(polar_flog_rec_ptr ckp_start, flashback_snapshot_header_t snapshot, bool shutdown)
{
	polar_flog_rec_ptr keep_ptr = ckp_start;
	flog_buf_ctl_t buf_ctl = flog_instance->buf_ctl;

	polar_fra_do_fbpoint(fra_instance, &(buf_ctl->wal_info), &keep_ptr, snapshot);
	polar_flog_do_fbpoint(flog_instance, ckp_start, keep_ptr, shutdown);
}

XLogRecPtr
polar_get_flashback_keep_wal(XLogRecPtr keep)
{
	XLogRecPtr flashback_keep;

	if (!polar_is_flog_enabled(flog_instance))
		return keep;

	flashback_keep = flog_instance->buf_ctl->redo_lsn;

	if (polar_enable_fra(fra_instance))
		flashback_keep = Min(flashback_keep, fra_instance->min_keep_lsn);

	/* Get the minimal of keep and flashback keep */
	if (XLogRecPtrIsInvalid(flashback_keep))
		return keep;
	else if (XLogRecPtrIsInvalid(keep))
		return flashback_keep;
	else
		return Min(keep, flashback_keep);
}
