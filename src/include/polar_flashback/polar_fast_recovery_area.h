/*-------------------------------------------------------------------------
 *
 * polar_fast_recovery_area.h
 *
 *
 * Copyright (c) 2021, Alibaba Group Holding limited
 *
 * src/include/polar_flashback/polar_fast_recovery_area.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_FAST_RECOVERY_AREA_H
#define POLAR_FAST_RECOVERY_AREA_H

#include "access/slru.h"
#include "polar_flashback/polar_flashback_clog.h"
#include "polar_flashback/polar_flashback_log_internal.h"
#include "polar_flashback/polar_flashback_log_mem.h"
#include "polar_flashback/polar_flashback_point.h"
#include "polar_flashback/polar_flashback_table.h"

#define POLAR_FRA_DEFAULT_DIR "polar_fra"
#define FRA_CTL_NAME_SUFFIX "fra ctl"
#define FRA_CTL_FILE_LOCK_NAME_SUFFIX "_ctl_file"
#define FRA_CTL_FILE_NAME "fra_control"
#define TMP_FRA_CTL_FILE_NAME "tmp_fra_ctl"
#define FRA_CTL_FILE_VERSION (1)

typedef struct fra_ctl_file_data_t
{
	uint32      version_no;
	uint32      next_clog_subdir_no; /* Next clog sub director number */
	uint64      next_fbpoint_rec_no; /* The max flashback point record number */
	fbpoint_pos_t      snapshot_end_pos; /* The snapshot data end pos, high 32 bits is segment no, lower 32 bit is offset in a segment */
	XLogRecPtr  min_keep_lsn; /* Minimal keep lsn while enable fast recover area */
	int         min_clog_seg_no; /* Minimal clog segment while enable fast recover area */
	pg_crc32c   crc;
} fra_ctl_file_data_t;

typedef struct fra_ctl_data_t
{
	fpoint_ctl_t    point_ctl;
	flashback_clog_ctl_t    clog_ctl;
	char    dir[FL_INS_MAX_NAME_LEN]; /* The directory of the fast recovery area */
	uint64      next_fbpoint_rec_no; /* A copy of point_ctl->next_fbpoint_rec_no */
	fbpoint_pos_t      snapshot_end_pos; /* The snapshot data end position, high 32 bits is segment no, lower 32 bit is offset in file */
	XLogRecPtr  min_keep_lsn; /* Minimal wal keep lsn */
	LWLock  ctl_file_lock; /* protect the control file lock */
} fra_ctl_data_t;

typedef fra_ctl_data_t *fra_ctl_t;

#define FRA_GET_SUBDIR_PATH(fra_dir, sub_dir, path) \
	polar_make_file_path_level3(path, fra_dir, sub_dir)

#define FRA_REMOVE_ALL_DATA() rmtree(POLAR_FRA_DEFAULT_DIR, true)

#define FRA_SHMEM_INIT() \
	do { \
		fra_instance = fra_shmem_init_internal(POLAR_FRA_DEFAULT_DIR); \
	}while (0)

extern fra_ctl_t fra_instance;

extern bool polar_enable_fra(fra_ctl_t ctl);
extern Size polar_fra_shmem_size(void);
extern void fra_shmem_init_data(fra_ctl_t ctl, const char *name);
extern fra_ctl_t fra_shmem_init_internal(const char *name);

extern void polar_startup_fra(fra_ctl_t ctl);

extern bool polar_slru_seg_need_mv(fra_ctl_t fra_ctl, SlruCtl slru_ctl);
extern void polar_mv_slru_seg_to_fra(fra_ctl_t ctl, const char *fname, const char *old_path);
#endif
