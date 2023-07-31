/*-------------------------------------------------------------------------
 *
 * polar_flashback_point.h
 *
 *
 * Copyright (c) 2021, Alibaba Group Holding limited
 *
 * src/include/polar_flashback/polar_flashback_point.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_FLASHBACK_POINT_H
#define POLAR_FLASHBACK_POINT_H

#include "polar_flashback/polar_flashback_log.h"

#define FBPOINT_DIR "fbpoint"

#define FBPOINT_PAGE_SIZE (512)
/* Segment size is 4MB */
#define FBPOINT_SEG_SIZE (4 * 1024 * 1024)

#define FBPOINT_REC_VERSION (1)

#define FBPOINT_PAGE_HEADER_SIZE (offsetof(fbpoint_page_header_t, crc) + sizeof(pg_crc32c))
#define FBPOINT_PAGE_SIZE_VALID (FBPOINT_PAGE_SIZE - FBPOINT_PAGE_HEADER_SIZE)

#define FBPOINT_PAGE_PER_SEG (FBPOINT_REC_END_POS / FBPOINT_PAGE_SIZE)

#define FBPOINT_REC_SIZE (64)
#define FBPOINT_REC_PER_PAGE (FBPOINT_PAGE_SIZE_VALID / FBPOINT_REC_SIZE)
#define FBPOINT_REC_PER_SEG (FBPOINT_PAGE_PER_SEG * FBPOINT_REC_PER_PAGE)

#define FBPOINT_SEG_FNAME_MAX_LEN (16)

#define FBPOINT_CTL_NAME_SUFFIX "point ctl"
#define FBPOINT_REC_BUF_LOCK_NAME_SUFFIX "_fbp_file"

#define FBPOINT_GET_PAGE_NO_BY_REC_NO(rec_no) (((rec_no) % FBPOINT_REC_PER_SEG) / FBPOINT_REC_PER_PAGE)
#define FBPOINT_GET_SEG_NO_BY_REC_NO(rec_no) ((rec_no) / FBPOINT_REC_PER_SEG)
#define FBPOINT_GET_OFFSET_BY_REC_NO(start_ptr, rec_no) \
	((char *) (start_ptr) + FBPOINT_GET_PAGE_NO_BY_REC_NO(rec_no) * FBPOINT_PAGE_SIZE + FBPOINT_PAGE_HEADER_SIZE + ((rec_no) % FBPOINT_REC_PER_PAGE) * FBPOINT_REC_SIZE)
#define FBPOINT_GET_FIRST_REC_IN_SEG(seg_start_ptr) \
	((fbpoint_rec_data_t *) ((seg_start_ptr) + FBPOINT_PAGE_HEADER_SIZE))

#define FBPOINT_REC_IN_SEG(rec_no, seg_no) \
	((FBPOINT_GET_SEG_NO_BY_REC_NO(rec_no)) == (seg_no))

#define FBPOINT_GET_SEG_FROM_FNAME(fname, seg_no) \
	do {                                                \
		sscanf((fname), "%08X", &(seg_no)); \
	} while (0)

#define FBPOINT_POS_IS_INVALID(pos) (pos.seg_no == 0 && pos.offset == 0)

/*
 * The flashback point record page struct like:
 *
 * page_header
 * fbpoint_rec_data_t[FBPOINT_REC_PER_PAGE]
 * padding
 */
typedef struct fbpoint_page_header_t
{
	uint32 version;
	pg_crc32c crc; /* A crc */
} fbpoint_page_header_t;

/*
 * The size of fbpoint_rec_data_t (now is 36 without aligned) must less than FBPOINT_REC_SIZE (64).
 * The remained size (now is 28) is left for extra info.
 * If the size is larger than FBPOINT_REC_SIZE, please import another file.
 */
typedef struct fbpoint_rec_data_t
{
	XLogRecPtr redo_lsn; /* The redo lsn of the flashback point */
	polar_flog_rec_ptr flog_ptr; /* The flashback point start pointer of the flashback point */
	pg_time_t time; /* The time of the flashback point */
	fbpoint_pos_t  snapshot_pos; /* The position of the snapshot data */
	uint32  next_clog_subdir_no; /* The next clog subdir no, copy from flashback snapshot data, just for truncating old clog subdir */
} fbpoint_rec_data_t;

#define FBPOINT_RECORD_DATA_SIZE 36

/* Flashback point record end position in segment is 64KB */
#define FBPOINT_REC_END_POS (64 * 1024)

typedef struct fbpoint_ctl_data_t
{
	LWLock  fbpoint_rec_buf_lock; /* protect the flashback point record buffer */
	uint64  next_fbpoint_rec_no; /* The max flashback point record number */
	char    *fbpoint_rec_buf; /* buffers for flashback point record file */
} fbpoint_ctl_data_t;

typedef fbpoint_ctl_data_t *fpoint_ctl_t;

typedef enum
{
	FBPOINT_FILE_CREATE_FAILED,
	FBPOINT_FILE_OPEN_FAILED,
	FBPOINT_FILE_READ_FAILED,
	FBPOINT_FILE_WRITE_FAILED,
	FBPOINT_FILE_FSYNC_FAILED,
	FBPOINT_FILE_CLOSE_FAILED
} fbpoint_io_errcause_t;

typedef struct fbpoint_io_error_t
{
	uint64                  segno;
	off_t                   offset;
	ssize_t                 size;
	ssize_t                 io_return;
	int                     save_errno;
	fbpoint_io_errcause_t   errcause;
} fbpoint_io_error_t;

extern void polar_get_fbpoint_file_path(uint32 seg_no, const char *fra_dir, char *path);

extern bool polar_read_fbpoint_file(const char *fra_dir, char *data, uint32 seg_no, uint32 offset, uint32 size, fbpoint_io_error_t *io_error);
extern bool polar_write_fbpoint_file(const char *fra_dir, char *data, uint32 seg_no, uint32 offset, uint32 size, fbpoint_io_error_t *io_error);
extern void polar_fbpoint_report_io_error(const char *fra_dir, fbpoint_io_error_t *io_error, int log_level);

extern void polar_set_fbpoint_wal_info(flog_buf_ctl_t buf_ctl, XLogRecPtr fbpoint_lsn, pg_time_t fbpoint_time, XLogRecPtr bg_replayed_lsn, bool is_restart_point);
extern XLogRecPtr polar_get_curr_fbpoint_lsn(flog_buf_ctl_t buf_ctl);
extern XLogRecPtr polar_get_prior_fbpoint_lsn(flog_buf_ctl_t buf_ctl);
extern XLogRecPtr polar_get_local_fbpoint_lsn(flog_buf_ctl_t buf_ctl, XLogRecPtr page_lsn, XLogRecPtr redo_lsn);
extern polar_flog_rec_ptr polar_get_fbpoint_start_ptr(flog_buf_ctl_t ctl);

extern bool polar_is_page_first_modified(flog_buf_ctl_t buf_ctl, XLogRecPtr page_lsn, XLogRecPtr redo_lsn);
extern bool polar_is_flashback_point(flog_ctl_t instance, XLogRecPtr checkpoint_lsn, XLogRecPtr bg_replayed_lsn, int *flags, bool is_restart_point);

extern Size polar_flashback_point_shmem_size(void);
extern void polar_flashback_point_shmem_init_data(fpoint_ctl_t ctl, const char *name);
extern fpoint_ctl_t polar_flashback_point_shmem_init(const char *name);
extern void polar_startup_flashback_point(fpoint_ctl_t ctl, const char *fra_dir, uint64 next_fbpoint_rec_no);

extern bool polar_get_right_fbpoint(fpoint_ctl_t ctl, const char *fra_dir, pg_time_t target_time, fbpoint_rec_data_t *result, uint32 *keep_seg_no);

extern void polar_flush_fbpoint_rec(fpoint_ctl_t ctl, const char *fra_dir, fbpoint_rec_data_t *rec_data);
extern void polar_truncate_fbpoint_files(const char *fra_dir, uint32 keep_seg_no);
extern uint32 polar_get_keep_fbpoint(fpoint_ctl_t ctl, const char *fra_dir, fbpoint_rec_data_t *fbpoint_rec,
									 polar_flog_rec_ptr *keep_ptr, XLogRecPtr *keep_lsn);
#endif
