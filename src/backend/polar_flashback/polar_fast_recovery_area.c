/*-------------------------------------------------------------------------
 *
 * polar_fast_recovery_area.c
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, Alibaba Group Holding limited
 *
 * Fast recovery area store data for flashback table.
 * Now there are three sub directories:
 * 1. pg_xact which contains each truncated clog.
 * 2. fbpoint which contains the flashback point files (fbpoint records and snapshot data).
 *
 * IDENTIFICATION
 *    src/backend/polar_flashback/polar_fast_recovery_area.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "polar_flashback/polar_fast_recovery_area.h"
#include "polar_flashback/polar_flashback.h"
#include "polar_flashback/polar_flashback_snapshot.h"

fra_ctl_t fra_instance = NULL;
bool polar_enable_fast_recovery_area;
int polar_fast_recovery_area_rotation;

/*
 * POLAR: Read fast recovery area control file data
 */
static bool
read_fra_ctl_file(fra_ctl_t ctl, fra_ctl_file_data_t *ctl_file_data)
{
	char ctl_file_path[MAXPGPATH];
	int fd;
	pg_crc32c   crc;

	polar_make_file_path_level3(ctl_file_path, ctl->dir, FRA_CTL_FILE_NAME);

	/* The control file may be non-exist */
	if (!polar_file_exists(ctl_file_path))
	{
		elog(WARNING, "Can't find %s", ctl_file_path);
		return false;
	}

	fd = polar_open_transient_file(ctl_file_path, O_RDONLY | PG_BINARY);

	if (fd < 0)
		/*no cover line*/
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", ctl_file_path)));

	pgstat_report_wait_start(WAIT_EVENT_FRA_CTL_FILE_READ);

	if (polar_read(fd, ctl_file_data, sizeof(fra_ctl_file_data_t)) != sizeof(fra_ctl_file_data_t))
		/*no cover line*/
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not read from file \"%s\": %m", ctl_file_path)));

	pgstat_report_wait_end();

	if (CloseTransientFile(fd))
		/*no cover line*/
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", ctl_file_path)));

	/* Verify CRC */
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, (char *) ctl_file_data, offsetof(fra_ctl_file_data_t, crc));
	FIN_CRC32C(crc);

	if (!EQ_CRC32C(crc, ctl_file_data->crc))
		/*no cover line*/
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("calculated CRC checksum does not match value stored in file \"%s\"",
						ctl_file_path)));

	/* Check the version */
	if (ctl_file_data->version_no != FRA_CTL_FILE_VERSION)
		/*no cover line*/
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Expected version %d does not match value stored in file %d",
						FRA_CTL_FILE_VERSION, ctl_file_data->version_no)));

	return true;
}

/*
 * POLAR: write fast recovery area control file by checkpoint.
 * Caller must hold the file lock.
 */
static void
write_fra_ctl_file(fra_ctl_t ctl)
{
	fra_ctl_file_data_t ctl_file;
	char            ctl_file_path[MAXPGPATH];

	/* Update the data */
	ctl_file.version_no = FRA_CTL_FILE_VERSION;
	ctl_file.next_fbpoint_rec_no = ctl->next_fbpoint_rec_no;
	ctl_file.min_keep_lsn = ctl->min_keep_lsn;
	ctl_file.min_clog_seg_no = ctl->clog_ctl->min_clog_seg_no;
	ctl_file.next_clog_subdir_no = pg_atomic_read_u32(&(ctl->clog_ctl->next_clog_subdir_no));
	ctl_file.snapshot_end_pos.seg_no = ctl->snapshot_end_pos.seg_no;
	ctl_file.snapshot_end_pos.offset = ctl->snapshot_end_pos.offset;

	/* Compute the CRC */
	INIT_CRC32C(ctl_file.crc);
	COMP_CRC32C(ctl_file.crc, &ctl_file,
				offsetof(fra_ctl_file_data_t, crc));
	FIN_CRC32C(ctl_file.crc);

	/* Write the control file */
	polar_make_file_path_level3(ctl_file_path, ctl->dir, FRA_CTL_FILE_NAME);
	polar_write_ctl_file_atomic(ctl_file_path, &ctl_file, sizeof(fra_ctl_file_data_t),
								WAIT_EVENT_FRA_CTL_FILE_WRITE, WAIT_EVENT_FRA_CTL_FILE_SYNC);

	elog(DEBUG2, "The fast recovery control info now is: "
		 "the next flashback point record number: %X/%X, "
		 "the min flashback clog segment number in the first sub directory: %04X"
		 "the next flashback log sub directory number: %08X",
		 (uint32)(ctl_file.next_fbpoint_rec_no >> 32), (uint32)(ctl_file.next_fbpoint_rec_no),
		 ctl_file.min_clog_seg_no, ctl_file.next_clog_subdir_no);
}

static void
validate_fra_subdir(const char *fra_dir, const char *sub_dir)
{
	char path[MAXPGPATH];

	FRA_GET_SUBDIR_PATH(fra_dir, sub_dir, path);
	polar_validate_dir(path);
}

inline bool
polar_enable_fra(fra_ctl_t ctl)
{
	return ctl && !polar_in_replica_mode() && !polar_in_standby_mode();
}

Size
polar_fra_shmem_size(void)
{
	Size size = 0;

	Assert(polar_enable_flashback_log && polar_enable_fast_recovery_area);
	/* Fast recovery area control data */
	size = sizeof(fra_ctl_data_t);
	/* Add flashback point shared memory size */
	size += polar_flashback_point_shmem_size();
	/* Add flashback clog shared memory size */
	size += FLASHBACK_CLOG_SHMEM_SIZE;
	return size;
}

void
fra_shmem_init_data(fra_ctl_t ctl, const char *name)
{
	char fra_ctl_file_lock[FL_OBJ_MAX_NAME_LEN];

	FLOG_GET_OBJ_NAME(fra_ctl_file_lock, name, FRA_CTL_FILE_LOCK_NAME_SUFFIX);

	MemSet(ctl, 0, sizeof(fra_ctl_data_t));
	ctl->clog_ctl = polar_flashback_clog_shmem_init(name);
	ctl->point_ctl = polar_flashback_point_shmem_init(name);
	StrNCpy(ctl->dir, name, FL_INS_MAX_NAME_LEN);
	LWLockRegisterTranche(LWTRANCHE_POLAR_FRA_CTL_FILE, fra_ctl_file_lock);
	LWLockInitialize(&ctl->ctl_file_lock, LWTRANCHE_POLAR_FRA_CTL_FILE);
}

fra_ctl_t
fra_shmem_init_internal(const char *name)
{
	fra_ctl_t ctl;
	bool found;
	char ctl_name[FL_OBJ_MAX_NAME_LEN];

	FLOG_GET_OBJ_NAME(ctl_name, name, FRA_CTL_NAME_SUFFIX);

	ctl = (fra_ctl_t) ShmemInitStruct(ctl_name, sizeof(fra_ctl_data_t), &found);

	if (!IsUnderPostmaster)
	{
		Assert(!found);
		fra_shmem_init_data(ctl, name);
	}
	else
		Assert(found);

	return ctl;
}

void
polar_startup_fra(fra_ctl_t ctl)
{
	char path[MAXPGPATH];
	fra_ctl_file_data_t ctl_file_data;
	int min_clog_seg_no = FLASHBACK_CLOG_INVALID_SEG;
	uint32 next_clog_subdir_no = 0;

	/* validate direcotry */
	polar_make_file_path_level2(path, ctl->dir);
	polar_validate_dir(path);

	/* validate sub direcotry */
	validate_fra_subdir(ctl->dir, FLASHBACK_CLOG_DIR);
	validate_fra_subdir(ctl->dir, FBPOINT_DIR);

	/* Read control file to init */
	if (read_fra_ctl_file(ctl, &ctl_file_data))
	{
		min_clog_seg_no = ctl_file_data.min_clog_seg_no;
		next_clog_subdir_no = ctl_file_data.next_clog_subdir_no;
		ctl->next_fbpoint_rec_no = ctl_file_data.next_fbpoint_rec_no;
		ctl->min_keep_lsn = ctl_file_data.min_keep_lsn;
		ctl->snapshot_end_pos.seg_no = ctl_file_data.snapshot_end_pos.seg_no;
		ctl->snapshot_end_pos.offset = ctl_file_data.snapshot_end_pos.offset;
	}

	if (XLogRecPtrIsInvalid(ctl->min_keep_lsn))
		ctl->min_keep_lsn = GetRedoRecPtr();

	if (FBPOINT_POS_IS_INVALID(ctl->snapshot_end_pos))
		ctl->snapshot_end_pos.offset = FBPOINT_SEG_SIZE;

	polar_startup_flashback_clog(ctl->clog_ctl, min_clog_seg_no, next_clog_subdir_no);
	polar_startup_flashback_point(ctl->point_ctl, ctl->dir, ctl->next_fbpoint_rec_no);
}

bool
polar_slru_seg_need_mv(fra_ctl_t fra_ctl, SlruCtl slru_ctl)
{
	if (strcmp(slru_ctl->Dir, FLASHBACK_CLOG_DIR) == 0 && polar_enable_fra(fra_ctl))
		return true;
	else
		return false;
}

/*
 * POLAR: Move SLRU segment to fra, now just clog.
 */
void
polar_mv_slru_seg_to_fra(fra_ctl_t ctl, const char *fname, const char *old_path)
{
	bool need_update_ctl = false;

	polar_mv_clog_seg_to_fra(ctl->clog_ctl, ctl->dir, fname, old_path, &need_update_ctl);

	/* Update the control file when it is necessary */
	if (need_update_ctl)
	{
		LWLockAcquire(&ctl->ctl_file_lock, LW_EXCLUSIVE);
		write_fra_ctl_file(ctl);
		LWLockRelease(&ctl->ctl_file_lock);
	}
}

/*
 * POLAR: Remove the old fast recovery data
 *
 * NB: The fbpoint_seg_no is keep semgent no + 1.
 * When fbpoint_seg_no is zero, there is no flashback point segment to removed.
 *
 */
static void
polar_remove_fra_data(fra_ctl_t ctl, fbpoint_rec_data_t *fbpoint_rec, uint32 fbpoint_seg_no)
{
	if (fbpoint_seg_no > 0)
	{
		/* The snapshot data and flashback point record may be not in the same segment, we keep the minimal one */
		fbpoint_seg_no = Min(fbpoint_rec->snapshot_pos.seg_no, fbpoint_seg_no);
		polar_truncate_fbpoint_files(ctl->dir, fbpoint_seg_no);
	}

	/* Truncate the flashback clog subdir */
	polar_truncate_flashback_clog_subdir(ctl->dir, fbpoint_rec->next_clog_subdir_no);
}

/*
 * Fast recovery area do flashback point.
 *
 * NB: Now the fast recovery area is disable for standby.
 */
void
polar_fra_do_fbpoint(fra_ctl_t ctl, fbpoint_wal_info_data_t *wal_info,
					 polar_flog_rec_ptr *keep_ptr, flashback_snapshot_header_t snapshot)
{
	fbpoint_rec_data_t rec_data;
	uint32 fbpoint_seg_no = 0;
	fbpoint_pos_t snapshot_pos;
	fbpoint_pos_t snapshot_end_pos;
	XLogRecPtr min_keep_lsn;

	if (!polar_enable_fra(ctl))
		return;

	snapshot_end_pos = ctl->snapshot_end_pos;
	Assert(snapshot);
	/* Backup the snapshot to fast recovery area */
	snapshot_pos = polar_backup_snapshot_to_fra(snapshot, &snapshot_end_pos, ctl->dir);

	/* The size of fbpoint_rec_data_t can not be larger than FBPOINT_REC_SIZE */
	StaticAssertStmt(sizeof(fbpoint_rec_data_t) < FBPOINT_REC_SIZE,
					 "fbpoint_rec_data_t is larger than 64");
	/* Flush the flashback point record */
	rec_data.flog_ptr = *keep_ptr;
	rec_data.redo_lsn = wal_info->fbpoint_lsn;
	rec_data.time = wal_info->fbpoint_time;
	rec_data.next_clog_subdir_no = FLSHBAK_GET_SNAPSHOT_DATA(snapshot)->next_clog_subdir_no;
	rec_data.snapshot_pos = snapshot_pos;
	polar_flush_fbpoint_rec(ctl->point_ctl, ctl->dir, &rec_data);

	/* Get the keep flashback point record */
	fbpoint_seg_no = polar_get_keep_fbpoint(ctl->point_ctl, ctl->dir, &rec_data, keep_ptr, &min_keep_lsn);

	/* Write the control file */
	LWLockAcquire(&ctl->ctl_file_lock, LW_EXCLUSIVE);
	/* Update the var about flashback point */
	ctl->next_fbpoint_rec_no = ctl->point_ctl->next_fbpoint_rec_no;
	ctl->snapshot_end_pos = snapshot_end_pos;
	ctl->min_keep_lsn = min_keep_lsn;
	write_fra_ctl_file(ctl);
	LWLockRelease(&ctl->ctl_file_lock);

	/* Remove the old data */
	polar_remove_fra_data(ctl, &rec_data, fbpoint_seg_no);
}
