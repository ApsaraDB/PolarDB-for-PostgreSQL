/*-------------------------------------------------------------------------
 *
 * polar_datamax.c
 *	       Datamax for DMA logger	
 *
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * src/backend/polar_datamax/polar_datamax.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/mvccvars.h"
#include "access/timeline.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "polar_datamax/polar_datamax.h"
#include "postmaster/pgarch.h"
#include "postmaster/startup.h"
#include "replication/slot.h"
#include "replication/walreceiver.h"
#include "storage/fd.h"
#include "storage/pmsignal.h"
#include "access/transam.h"
#include "utils/guc.h"
#include "utils/ps_status.h"
#include "utils/timestamp.h"

#define RECOVERY_COMMAND_FILE   "recovery.conf"

/* initial meta means just initilized meta */
#define DATAMAX_IS_INITIAL_META(meta) \
	((meta)->last_timeline_id == POLAR_INVALID_TIMELINE_ID && \
	 XLogRecPtrIsInvalid((meta)->last_received_lsn))

/*
 * We use mechanism which depend on polar_vfs extension to maintain DataMax mode.
 * polar_vfs extension will initialize node type from recovery.conf while extension
 * initialization in polar_vfs_shmem_startup. So we relay on polar_vfs and its
 * startup function being executed while shared memory initialization.
 *
 * So the DataMax mode will be set before all other operations in DB, and its value
 * wil be kept while postmaster fork another sub-process. Eventually, all processes
 * know the DataMax mode.
 */
bool polar_is_dma_logger_mode = false;
polar_datamax_ctl_t *polar_datamax_ctl = NULL;
/* POLAR: set true when received shutdown request */
bool polar_datamax_shutdown_requested = false;

/* POLAR: polar datamax guc parameters */
int polar_datamax_remove_archivedone_wal_timeout = 60000;
int polar_datamax_archive_timeout = 60000;
int polar_datamax_save_replication_slots_timeout = 300000;
int polar_datamax_prealloc_walfile_timeout = 30000;
int polar_datamax_prealloc_walfile_num = 2;


/*
 * POLAR: datamax shared memory size.
 */
Size
polar_datamax_shmem_size(void)
{
	Size size = 0;

	if (polar_is_dma_logger_node())
		size = add_size(size, sizeof(polar_datamax_ctl_t));

	return size;
}

/*
 * POLAR: datamax shared memory initialization.
 */
void
polar_datamax_shmem_init(void)
{
	bool found = false;

	if (!polar_is_dma_logger_node())
		return;

	polar_datamax_ctl = (polar_datamax_ctl_t *)
						ShmemInitStruct("Datamax control", sizeof(polar_datamax_ctl_t), &found);

	if (!IsUnderPostmaster)
	{
		int lock_tranche = LWLockNewTrancheId();

		Assert(!found);
		LWLockRegisterTranche(lock_tranche, "datamax meta lock");
		LWLockInitialize(&polar_datamax_ctl->meta_lock, lock_tranche);
		polar_datamax_init_meta(POLAR_DATAMAX_META, false);

		SpinLockInit(&polar_datamax_ctl->lock);
	}
	else
		Assert(found);
}

/*
 * POLAR: Whether DataMax is a initial one or not.
 *
 * Return true if it's a initial DataMax node, otherwise false which means
 * it's not an initial DataMax node or not even a DataMax node.
 */
bool
polar_datamax_is_initial(polar_datamax_ctl_t *ctl)
{
	bool ret = false;

	if (!polar_is_dma_logger_node())
		return false;

	if (ctl)
	{
		LWLockAcquire(&ctl->meta_lock, LW_SHARED);
		ret = DATAMAX_IS_INITIAL_META(&ctl->meta);
		LWLockRelease(&ctl->meta_lock);
		return ret;
	}
	else
		return false;
}

/*
 * POLAR: Init datamax meta data info.
 *
 * If create is true, we initialize all variables with some initial value.
 * Otherwise, if not we set all variables 0.
 */
void
polar_datamax_init_meta(polar_datamax_meta_data_t *meta, bool create)
{
	if (create)
	{
		meta->magic         = POLAR_DATAMAX_MAGIC;
		meta->version       = POLAR_DATAMAX_VERSION;
	}
	else
	{
		meta->magic         = 0;
		meta->version       = 0;
	}

	meta->min_timeline_id   = POLAR_INVALID_TIMELINE_ID;
	meta->min_received_lsn  = InvalidXLogRecPtr;
	meta->last_timeline_id  = POLAR_INVALID_TIMELINE_ID;
	meta->last_received_lsn = InvalidXLogRecPtr;
	meta->last_valid_received_lsn = InvalidXLogRecPtr;
	meta->upstream_last_removed_segno = 0;
	meta->crc       = 0;
	meta->crc       = polar_datamax_calc_meta_crc(
						  (unsigned char *)meta, sizeof(polar_datamax_meta_data_t));
}

/*
 * POLAR: Load datamax meta info from storage.
 * We will check magic, version and crc of meta from data corruption.
 */
void
polar_datamax_load_meta(polar_datamax_ctl_t *ctl)
{
	char        meta_path[MAXPGPATH];
	pg_crc32c   crc;
	int         fd;
	int         ret;
	Size        meta_size = sizeof(polar_datamax_meta_data_t);
	polar_datamax_meta_data_t *meta = NULL;

	Assert(ctl);
	meta = &ctl->meta;

	snprintf(meta_path, MAXPGPATH, "%s/%s/%s",
			 DataDir, POLAR_DATAMAX_DIR, POLAR_DATAMAX_META_FILE);

	LWLockAcquire(&ctl->meta_lock, LW_EXCLUSIVE);

	if ((fd = PathNameOpenFile(meta_path, O_RDONLY | PG_BINARY)) < 0)
	{
		if (errno == ENOENT)
		{
			elog(WARNING, "datamax meta file not exist, init memory one.");
			polar_datamax_init_meta(&ctl->meta, true);
			LWLockRelease(&ctl->meta_lock);
			return;
		}

		LWLockRelease(&ctl->meta_lock);
		elog(FATAL, "fail to open datamax meta file, errno: %d", errno);

		return;
	}

	ret = FileRead(fd, (char *)meta, meta_size, WAIT_EVENT_DATAMAX_META_READ);
	if (ret != meta_size)
	{
		int saved_errno = errno;
		LWLockRelease(&ctl->meta_lock);
		elog(FATAL, "fail to read whole datamax meta data, read %d of %lu, errno: %d",
			 ret, meta_size, saved_errno);
		return;
	}

	if (meta->magic != POLAR_DATAMAX_MAGIC)
	{
		LWLockRelease(&ctl->meta_lock);
		POLAR_LOG_DATAMAX_META_INFO(meta);
		elog(FATAL, "invalid datamax meta magic, got %d, expect %d",
			 meta->magic, POLAR_DATAMAX_MAGIC);
		return;
	}

	/* version compatibility will be added if new version required */
	if (meta->version != POLAR_DATAMAX_VERSION)
	{
		LWLockRelease(&ctl->meta_lock);
		POLAR_LOG_DATAMAX_META_INFO(meta);
		elog(FATAL, "datamax meta version is not compatible, got %d, expect %d",
			 meta->version, POLAR_DATAMAX_VERSION);
		return;
	}

	crc = meta->crc;
	meta->crc = 0;
	meta->crc = polar_datamax_calc_meta_crc((unsigned char *)meta, meta_size);

	if (crc != meta->crc)
	{
		LWLockRelease(&ctl->meta_lock);
		POLAR_LOG_DATAMAX_META_INFO(meta);
		elog(FATAL, "invalid datamax meta crc, got %d, expect %d", crc, meta->crc);
		return;
	}

	LWLockRelease(&ctl->meta_lock);
}

/*
 * POLAR: Write datamax meta data to storage.
 *
 * This func can only be called in datamax process, so no need to get lock first.
 */
void
polar_datamax_write_meta(polar_datamax_ctl_t *ctl, bool update)
{
	File    fd;
	char    meta_path[MAXPGPATH];
	int     flag = O_RDWR | PG_BINARY;
	Size    meta_size = sizeof(polar_datamax_meta_data_t);
	int     saved_errno;
	polar_datamax_meta_data_t *meta = NULL;

	if (ctl)
		meta = &ctl->meta;
	else
	{
		elog(FATAL, "fail to write datamax meta data, invalid datamax meta");
		return;
	}

	snprintf(meta_path, MAXPGPATH, "%s/%s/%s", DataDir, POLAR_DATAMAX_DIR, POLAR_DATAMAX_META_FILE);

	if (!update)
		flag |= O_CREAT;

	LWLockAcquire(&ctl->meta_lock, LW_EXCLUSIVE);

	meta->magic = POLAR_DATAMAX_MAGIC;
	meta->version = POLAR_DATAMAX_VERSION;
	meta->crc = 0;
	meta->crc = polar_datamax_calc_meta_crc((unsigned char *)meta, meta_size);

	if ((fd = PathNameOpenFile(meta_path, flag)) < 0)
	{
		saved_errno = errno;

		LWLockRelease(&ctl->meta_lock);

		POLAR_LOG_DATAMAX_META_INFO(meta);
		elog(FATAL, "fail to open datamax meta file, errno: %d", saved_errno);
		return;
	}

	if (FileWrite(fd, (char *)meta, meta_size, WAIT_EVENT_DATAMAX_META_WRITE) != meta_size)
	{
		saved_errno = errno;

		LWLockRelease(&ctl->meta_lock);

		POLAR_LOG_DATAMAX_META_INFO(meta);
		elog(FATAL, "fail to write datamax meta data, errno: %d", saved_errno);
		return;
	}

	if (FileSync(fd, WAIT_EVENT_DATAMAX_META_WRITE) != 0)
	{
		saved_errno = errno;

		LWLockRelease(&ctl->meta_lock);

		POLAR_LOG_DATAMAX_META_INFO(meta);
		elog(FATAL, "fail to sync datamax meta data to storage, errno: %d", saved_errno);
		return;
	}

	FileClose(fd);

	LWLockRelease(&ctl->meta_lock);
}

void
polar_datamax_update_min_received_info(polar_datamax_ctl_t *ctl, TimeLineID tli, XLogRecPtr lsn)
{
	Assert(ctl);
	LWLockAcquire(&ctl->meta_lock, LW_EXCLUSIVE);

	if (tli != POLAR_INVALID_TIMELINE_ID)
		ctl->meta.min_timeline_id = tli;

	if (!XLogRecPtrIsInvalid(lsn))
		ctl->meta.min_received_lsn = lsn;

	LWLockRelease(&ctl->meta_lock);
}

void
polar_datamax_update_upstream_last_removed_segno(polar_datamax_ctl_t *ctl, XLogSegNo segno)
{
	Assert(ctl);
	LWLockAcquire(&ctl->meta_lock, LW_EXCLUSIVE);

	/*
	 * POLAR: parameter segno could be smaller than meta.upstream_last_removed_segno
	 * eg. it maybe 0 when primary restart and there is no wal file has been removed
	 */
	if (segno > ctl->meta.upstream_last_removed_segno)
		ctl->meta.upstream_last_removed_segno = segno;

	LWLockRelease(&ctl->meta_lock);
}

XLogRecPtr
polar_datamax_get_min_received_lsn(polar_datamax_ctl_t *ctl, TimeLineID *tli)
{
	XLogRecPtr min_received_lsn = InvalidXLogRecPtr;
	Assert(ctl);

	LWLockAcquire(&ctl->meta_lock, LW_SHARED);

	if (tli)
		*tli = ctl->meta.min_timeline_id;

	min_received_lsn = ctl->meta.min_received_lsn;

	LWLockRelease(&ctl->meta_lock);

	return min_received_lsn;
}

/*
 * POLAR: get last removed segno of upstream
 */
XLogSegNo
polar_datamax_get_upstream_last_removed_segno(polar_datamax_ctl_t *ctl)
{
	XLogRecPtr upstream_last_removed_segno = 0;
	Assert(ctl);
	LWLockAcquire(&ctl->meta_lock, LW_SHARED);
	upstream_last_removed_segno = ctl->meta.upstream_last_removed_segno;
	LWLockRelease(&ctl->meta_lock);
	return upstream_last_removed_segno;
}

/*
 * POLAR: try to remove old wal.
 *
 * lsn is not involved in removed segment.
 */
void
polar_datamax_remove_old_wal(XLogRecPtr reserved_lsn, bool force)
{
	DIR         *xldir;
	struct dirent   *xlde;
	char        path[MAXPGPATH];
	char        last_seg[MAXPGPATH];
	XLogRecPtr  min_lsn = InvalidXLogRecPtr;
	XLogSegNo   segno, upstream_last_removed_segno;
	TimeLineID  min_reserved_tli = POLAR_INVALID_TIMELINE_ID;
	XLogRecPtr  min_reserved_lsn = InvalidXLogRecPtr;

	if (!polar_is_dma_logger_node())
		return;

	/* parameter check */
	if (XLogRecPtrIsInvalid(reserved_lsn))
		return;

	/* calculate slot min lsn first */
	ReplicationSlotsComputeRequiredLSN();

	min_lsn = XLogGetReplicationSlotMinimumLSN();

	/* use smaller one between reserved_lsn and slot min restart_lsn */
	if (XLogRecPtrIsInvalid(min_lsn) || reserved_lsn < min_lsn)
		min_lsn = reserved_lsn;

	/* calculate last removed segment number */
	XLByteToSeg(min_lsn, segno, wal_segment_size);
	segno--;

	/* calcute DMA purge lsn */
	{
		XLogRecPtr consensus_keep  = ConsensusGetPurgeLSN();
		if (consensus_keep != InvalidXLogRecPtr)
		{
			XLogSegNo	slotSegNo;

			XLByteToSeg(consensus_keep, slotSegNo, wal_segment_size);

			if (slotSegNo <= 0)
				segno = 1;
			else if (slotSegNo < segno)
				segno = slotSegNo;

			ereport(LOG,
					(errmsg("purged consensus all matched's segno: %lu", segno)));
		}
		else
		{
			segno = 1;
		}
	}

	/* get last removed segno of upstream */
	upstream_last_removed_segno = polar_datamax_get_upstream_last_removed_segno(polar_datamax_ctl);
	/* keep wal which haven't been removed in upstream node */
	if (segno > upstream_last_removed_segno)
		segno = upstream_last_removed_segno;

	snprintf(path, MAXPGPATH, "%s", POLAR_DATAMAX_WAL_DIR);

	XLogFileName(last_seg, 0, segno, wal_segment_size);

	elog(LOG, "datamax attempting to remove WAL segment file older than log file %s",
		 last_seg);

	xldir = AllocateDir(path);

	while ((xlde = ReadDir(xldir, path)) != NULL)
	{
		bool removed = false;

		if (!IsXLogFileName(xlde->d_name) &&
				!IsPartialXLogFileName(xlde->d_name))
			continue;

		/*
		 * only compare segment name without timeline info, detail reason
		 * is described at RemoveOldXlogFiles.
		 */
		if (strcmp(xlde->d_name + 8, last_seg + 8) <= 0)
		{
			/* if force is true or WAL segment has been archived, we will remove it. */
			if (force || XLogArchiveCheckDone(xlde->d_name))
			{
				if (polar_datamax_remove_wal_file(xlde->d_name) == 0)
					removed = true;
			}
		}

		if (!removed)
		{
			uint32      tmp_tli;
			XLogSegNo   tmp_segno;
			XLogRecPtr  tmp_seg_lsn;

			/* try to find min received info */
			XLogFromFileName(xlde->d_name, &tmp_tli, &tmp_segno, wal_segment_size);
			XLogSegNoOffsetToRecPtr(tmp_segno, 0, wal_segment_size, tmp_seg_lsn);

			if (XLogRecPtrIsInvalid(min_reserved_lsn) || min_reserved_lsn > tmp_seg_lsn)
			{
				min_reserved_tli = tmp_tli;
				min_reserved_lsn = tmp_seg_lsn;
			}
		}
	}

	FreeDir(xldir);

	Assert(!XLogRecPtrIsInvalid(min_reserved_lsn) &&
		   min_reserved_tli != POLAR_INVALID_TIMELINE_ID);

	/* update min received info according to actual WAL file */
	polar_datamax_update_min_received_info(polar_datamax_ctl, min_reserved_tli, min_reserved_lsn);
	polar_datamax_write_meta(polar_datamax_ctl, true);
	elog(LOG, "remove useless WAL segment done, min_reserved_tli %d, min_received_lsn %lX",
		 min_reserved_tli, min_reserved_lsn);
}

/*
 * POLAR: Remove one WAL segment file.
 *
 * We will also remove tatus file related with current segment.
 */
int
polar_datamax_remove_wal_file(char *seg_name)
{
	int     rc = 0;
	char    seg_path[MAXPGPATH];

	polar_update_last_removed_ptr(seg_name);

	snprintf(seg_path, MAXPGPATH, "%s/%s", POLAR_DATAMAX_WAL_DIR, seg_name);

	elog(LOG, "removing WAL segment file %s", seg_name);
	rc = durable_unlink(seg_path, LOG);

	if (rc != 0)
	{
		/* detail log was printed in durable_unlink */
		elog(WARNING, "remove WAL segment file failed.");
		return rc;
	}

	/* remove status files */
	XLogArchiveCleanup(seg_name);
	return rc;
}

/*
 * POLAR: archive wal file, need archive_mode == always
 */
void
polar_datamax_archive(void)
{
	static TimestampTz last_archive_time = 0;

	if (unlikely(last_archive_time == 0))
		last_archive_time = GetCurrentTimestamp();

	if (XLogArchivingAlways() && polar_is_dma_logger_node() && polar_datamax_archive_timeout)
	{
		TimestampTz archive_now = GetCurrentTimestamp();

		if (TimestampDifferenceExceeds(last_archive_time, archive_now, polar_datamax_archive_timeout))
		{
			polar_datamax_ArchiverCopyLoop();
			last_archive_time = archive_now;
		}
	}
}

/*
 * POLAR: remove wal files whose archive status are .done
 * if archive_mode = off or archive_mode = on, the .done file will be created forcibly in walreceiver.c
 * if archive_mode > on, the .done file will be created only when corresponding walfile has been archived
 * so there is no need to judge archive_mode value in this func
 */
void
polar_datamax_remove_archivedone_wal(polar_datamax_ctl_t *ctl)
{
	static TimestampTz last_remove_archivedone_wal_time = 0;

	if (unlikely(last_remove_archivedone_wal_time == 0))
		last_remove_archivedone_wal_time = GetCurrentTimestamp();

	/* only remove when timeout is set valid and in datamax mode */
	if (polar_is_dma_logger_node() && polar_datamax_remove_archivedone_wal_timeout)
	{
		TimestampTz now = GetCurrentTimestamp();

		if (TimestampDifferenceExceeds(last_remove_archivedone_wal_time, now, polar_datamax_remove_archivedone_wal_timeout))
		{
			polar_datamax_handle_remove_archivedone_wal(ctl);
			last_remove_archivedone_wal_time = now;
		}
	}
}

/*
 * POLAR: handle remove wal files whose archive status are .done
 * besides, don't remove wal files those haven't been removed in upstream instance
 * in case we lose walfile which is needed by downstream after re-create datamax node
 * which will be judged in func polar_datamax_remove_old_wal
 */
void
polar_datamax_handle_remove_archivedone_wal(polar_datamax_ctl_t *ctl)
{
	XLogRecPtr  last_rec_lsn = InvalidXLogRecPtr;
	XLogRecPtr  reserved_lsn = InvalidXLogRecPtr;
	XLogSegNo   segno;

	if (!polar_is_dma_logger_node())
		return;

	/* get last received lsn */
	last_rec_lsn = polar_dma_get_received_lsn();
	XLByteToSeg(last_rec_lsn, segno, wal_segment_size);

	/* compute limit for wal_keep_segments */
	if (wal_keep_segments > 0)
	{
		if (segno <= wal_keep_segments)
			segno = 1;
		else
			segno = segno - wal_keep_segments;
	}

	XLogSegNoOffsetToRecPtr(segno, 0, wal_segment_size, reserved_lsn);
	polar_datamax_remove_old_wal(reserved_lsn, false);
}

/*
 * POLAR: Check DataMax directory, if directory is missing, we will create it.
 */
void
polar_datamax_validate_dir(void)
{
	char    path[MAXPGPATH];
	int     i = 0;
	const char *dirs[] =
	{
		POLAR_DATAMAX_DIR,
		POLAR_DATAMAX_WAL_DIR,
		POLAR_DATAMAX_WAL_DIR "/archive_status"
	};

	for (i = 0; i < lengthof(dirs); ++i)
	{
		snprintf(path, MAXPGPATH, "%s", dirs[i]);

		/* check and create dir */
		polar_datamax_check_mkdir(path, FATAL);
	}
}

/*
 * POLAR: Utility to check dir existence, if not create one.
 *
 * parameter path should be an absolute path.
 */
void
polar_datamax_check_mkdir(const char *path, int emode)
{
	struct stat stat_buf;

	if (stat(path, &stat_buf) == 0)
	{
		if (!S_ISDIR(stat_buf.st_mode))
			elog(emode, "path \"%s\" is not a directory", path);
	}
	else
	{
		elog(LOG, "create missing directory \"%s\"", path);

		if (MakePGDirectory(path) < 0)
			elog(emode, "could not create directory \"%s\"", path);
	}
}

/*
 * POLAR: Return start pos of next WAL segment file after restart_lsn of slot.
 *
 * It is used while establishing initial replication from Datamax. Primary will
 * send WAL data as much as possible, and to avoid hole of WAL file in DataMax,
 * replication should begin at a start posistion of WAL file. That is why we
 * need this func.
 */
XLogRecPtr
polar_datamax_replication_start_lsn(ReplicationSlot *slot)
{
	XLogRecPtr restart_lsn = InvalidXLogRecPtr;

	Assert(slot);
	SpinLockAcquire(&slot->mutex);
	restart_lsn = slot->data.restart_lsn;
	SpinLockRelease(&slot->mutex);

	Assert(!XLogRecPtrIsInvalid(restart_lsn));

	return restart_lsn - XLogSegmentOffset(restart_lsn, wal_segment_size);
}

/*
 * POLAR: iterate the waldir to get the lsn of smallest walfile
 * called when XLogGetLastRemovedSegno is 0
 */
XLogRecPtr
polar_get_smallest_walfile_lsn(void)
{
	XLogRecPtr	smallest_lsn, tmp_smallest_lsn;
	XLogSegNo 	segno;
	char 		xlog_file[MAXFNAMELEN];
	char		polar_waldir_path[MAXPGPATH];
	DIR			*polar_waldir;
	struct dirent *polar_waldirent;
	uint32		tli;

	/* get the possibly smallest lsn */
	if (!polar_is_dma_logger_node())
		smallest_lsn = GetRedoRecPtr();
	else
		smallest_lsn = polar_datamax_get_min_received_lsn(polar_datamax_ctl, NULL);
	
	/* iterate the wal dir to get the smallest lsn */
	if (!polar_is_dma_logger_node())
		snprintf(polar_waldir_path, MAXPGPATH, "%s", XLOGDIR);
	else
		snprintf(polar_waldir_path, MAXPGPATH, "%s", POLAR_DATAMAX_WAL_DIR);
	
	polar_waldir = AllocateDir(polar_waldir_path);
	XLByteToSeg(smallest_lsn, segno, wal_segment_size);	
	XLogFileName(xlog_file, 0, segno, wal_segment_size);
	while ((polar_waldirent = ReadDir(polar_waldir, polar_waldir_path)) != NULL)
	{
		if (!IsXLogFileName(polar_waldirent->d_name) &&
			!IsPartialXLogFileName(polar_waldirent->d_name))
			continue;
		if (strcmp(polar_waldirent->d_name + 8, xlog_file + 8) < 0)
		{
			/* found wal file smaller than xlog_file */
			XLogFromFileName(polar_waldirent->d_name, &tli, &segno, wal_segment_size);
			XLogSegNoOffsetToRecPtr(segno, 0, wal_segment_size, tmp_smallest_lsn);
			/* update restart_lsn when current wal file hasn't been removed */
			if (tmp_smallest_lsn < smallest_lsn)
				smallest_lsn = tmp_smallest_lsn;
		}
	}
	FreeDir(polar_waldir);
	return smallest_lsn;
}

/*
 * POLAR: save replication slots to pg_replslot
 * slots are saved by CreateCheckPoint/CreateRestartPoint when not in datamax mode
 * there is no checkpoint process in datamax mode
 * add this func to save replication slot in datamax mode
 */
void
polar_datamax_save_replication_slots(void)
{
	static TimestampTz last_saveslots_time = 0;

	if (unlikely(last_saveslots_time == 0))
		last_saveslots_time = GetCurrentTimestamp();

	if (polar_is_dma_logger_node() && polar_datamax_save_replication_slots_timeout)
	{
		TimestampTz saveslots_now = GetCurrentTimestamp();

		if (TimestampDifferenceExceeds(last_saveslots_time, saveslots_now, polar_datamax_save_replication_slots_timeout))
		{
			CheckPointReplicationSlots();
			last_saveslots_time = saveslots_now;
		}
	}
}

/*
 * POLAR: WAL file path for datamax
 */
void
polar_datamax_wal_file_path(char *path, TimeLineID tli, XLogSegNo logSegNo, int wal_segsz_bytes)
{
	snprintf(path, MAXPGPATH, POLAR_DATAMAX_WAL_DIR "/%08X%08X%08X", tli,
			(uint32)((logSegNo) / XLogSegmentsPerXLogId(wal_segsz_bytes)),
			(uint32)((logSegNo) % XLogSegmentsPerXLogId(wal_segsz_bytes)));
}

/*
 * POLAR: WAL status file path for DataMax
 */
void
polar_datamax_status_file_path(char *path, const char *xlog, char *suffix)
{
	snprintf(path, MAXPGPATH, POLAR_DATAMAX_WAL_DIR "/archive_status/%s%s", xlog, suffix);
}

void
polar_datamax_tl_history_file_path(char *path, TimeLineID tli)
{
	snprintf(path, MAXPGPATH, POLAR_DATAMAX_WAL_DIR "/%08X.history", tli);
}

/*
 * POLAR: prealloc wal file
 * this work is done by polar worker process when not in datamax mode
 * add this func so it's unnecessary to start polar worker process in datamax mode
 */
void
polar_datamax_prealloc_wal_file(polar_datamax_ctl_t *ctl)
{
	static TimestampTz last_prealloc_walfile_time = 0;

	if (unlikely(last_prealloc_walfile_time == 0))
		last_prealloc_walfile_time = GetCurrentTimestamp();

	if (polar_is_dma_logger_node() && WalRcvStreaming() && polar_datamax_prealloc_walfile_timeout)
	{
		TimestampTz prealloc_walfile_now = GetCurrentTimestamp();

		if (TimestampDifferenceExceeds(last_prealloc_walfile_time, prealloc_walfile_now, polar_datamax_prealloc_walfile_timeout))
		{
			polar_datamax_handle_prealloc_walfile(ctl);
			last_prealloc_walfile_time = prealloc_walfile_now;
		}
	}
}

/*
 * POLAR: handle prealloc wal file
 * need polar_is_dma_logger_mode = true when call this function alone,
 * which is used to find the correct pg_wal path in datamax mode
 */
void
polar_datamax_handle_prealloc_walfile(polar_datamax_ctl_t *ctl)
{
	XLogRecPtr insert_ptr;
	XLogSegNo  _log_seg_no;
	int         lf;
	bool        use_existent;
	int         count = 0;

	insert_ptr = polar_dma_get_received_lsn();

	if (!XLogRecPtrIsInvalid(insert_ptr))
	{
		XLByteToPrevSeg(insert_ptr, _log_seg_no, wal_segment_size);

		for (count = 0; count < polar_datamax_prealloc_walfile_num; count++)
		{
			use_existent = true;
			lf = XLogFileInit(++_log_seg_no, &use_existent, true);
			close(lf);
		}
	}
}

/*
 * POLAR: judge whether meta file exists
 * no meta file when it is an initial created datamax
 */
bool
polar_datamax_meta_file_exist(void)
{
	char    meta_path[MAXPGPATH];
	struct stat stat_buf;

	snprintf(meta_path, MAXPGPATH, "%s/%s/%s", DataDir, POLAR_DATAMAX_DIR, POLAR_DATAMAX_META_FILE);
	return (stat(meta_path, &stat_buf) == 0);
}

/*
 * POLAR: Main entry of DataMax node.
 * It will take some operations of Startup, like revoking stream replication,
 * config read. Also, it will do some DataMax operations, like config maintainment,
 * meta data managment, etc.
 */
void
polar_datamax_main(void)
{
	TimestampTz last_handle_interrupts_time = GetCurrentTimestamp();
	char        activitymsg[MAXFNAMELEN + 16];
	XLogRecPtr 	consensusReceivedUpto = InvalidXLogRecPtr;
	TimeLineID 	consensusReceivedTLI = 0;
	char		*consensusPrimaryConnInfo = NULL;
	TransactionId latestCompletedXid;

	/*
	 * validate datamax directory. for shared storage, we keep meta data
	 * info in local storage, for standalone storage, we keep meta and
	 * wal data all in same storage.
	 */
	polar_datamax_validate_dir();

	/* read necessary config in recovery.conf */
	ereport(LOG, (errmsg("entering datamax mode")));

	/* load datamax meta info */
	polar_datamax_load_meta(polar_datamax_ctl);

	/* create meta file if it doesn't exist */
	if (!polar_datamax_meta_file_exist())
		polar_datamax_write_meta(polar_datamax_ctl, false);

	/* also initialize latestCompletedXid, to nextXid - 1 */
	latestCompletedXid = ShmemVariableCache->nextXid;
	TransactionIdRetreat(latestCompletedXid);
	pg_atomic_write_u32(&ShmemVariableCache->latestCompletedXid, latestCompletedXid);

	/* notify postmaster turn into DATAMAX status */
	SendPostmasterSignal(PGSIGNAL_BEGIN_DATAMAX);

	/* update process title info */
	snprintf(activitymsg, sizeof(activitymsg), "polar_datamax");
	set_ps_display(activitymsg, false);

	/* Ready to go */
	while (true)
	{
		TimestampTz current_time = GetCurrentTimestamp();
		int rc = 0;

		if (TimestampDifferenceExceeds(last_handle_interrupts_time, current_time, 5))
		{
			/* save replication slot info when shutdown */
			if (polar_datamax_shutdown_requested)
				CheckPointReplicationSlots();

			/* Handle interrupt signals of startup process */
			HandleStartupProcInterrupts();
			/* POLAR: every 5 ms call this */
			last_handle_interrupts_time = current_time;
		}

		{
			TimeLineID	tli = 0;
			bool 		next_tli = false; 

			polar_is_dma_logger_mode = true;

			if (polar_dma_check_logger_status(&consensusPrimaryConnInfo, 
						&consensusReceivedUpto , &consensusReceivedTLI, &next_tli))
			{
				if (next_tli)
					tli = consensusReceivedTLI + 1;
				else
					tli = consensusReceivedTLI;

				elog(LOG, "RequestXLogStreaming, PrimaryConnInfo: %s, TimeLineID: %d, LSN: \"%X/%X\"",
						consensusPrimaryConnInfo, tli,
						(uint32) (consensusReceivedUpto >> 32), (uint32) consensusReceivedUpto);

				RequestXLogStreaming(tli, consensusReceivedUpto, consensusPrimaryConnInfo, NULL);
			}
		}

		/* save replication slots */
		polar_datamax_save_replication_slots();

		/* start archive */
		polar_datamax_archive();

		/* remove archive done wal */
		polar_datamax_remove_archivedone_wal(polar_datamax_ctl);

		/* prealloc wal file */
		polar_datamax_prealloc_wal_file(polar_datamax_ctl);

		rc = polar_wait_recovery_wakeup(WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   1 * 1000 /* ms */, WAIT_EVENT_DATAMAX_MAIN);

		if (rc & WL_POSTMASTER_DEATH)
			exit(1);
	}
}


