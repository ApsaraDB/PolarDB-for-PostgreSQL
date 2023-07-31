/*-------------------------------------------------------------------------
 *
 * polar_flashback_clog.h
 *
 *
 * Copyright (c) 2021, Alibaba Group Holding limited
 *
 * src/include/polar_flashback/polar_flashback_clog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_FLASHBACK_CLOG_H
#define POLAR_FLASHBACK_CLOG_H

#include "access/clog.h"
#include "port/atomics.h"

#define FLASHBACK_CLOG_DIR "pg_xact"
#define FLASHBACK_CLOG_CTL_NAME_SUFFIX " clog ctl"

#define FLASHBACK_CLOG_SUBDIR_NAME_LEN (8)

typedef struct flashback_clog_ctl_data_t
{
	pg_atomic_uint32 next_clog_subdir_no;
	int min_clog_seg_no; /* Minimal of the clog segment after polar_enable_flashback is on */
} flashback_clog_ctl_data_t;

typedef flashback_clog_ctl_data_t *flashback_clog_ctl_t;

#define FLASHBACK_CLOG_SHMEM_SIZE (sizeof(flashback_clog_ctl_data_t))
#define FLASHBACK_CLOG_INVALID_SEG (-1)
#define FLASHBACK_CLOG_MIN_SUBDIR (0)
#define FLASHBACK_CLOG_MIN_NEXT_SUBDIR (FLASHBACK_CLOG_MIN_SUBDIR + 1)

#define FLASHBACK_CLOG_IS_EMPTY(next_clog_subdir) (next_clog_subdir == FLASHBACK_CLOG_MIN_SUBDIR)

extern void polar_flashback_clog_shmem_init_data(flashback_clog_ctl_t ctl);
extern flashback_clog_ctl_t polar_flashback_clog_shmem_init(const char *name);
extern void polar_startup_flashback_clog(flashback_clog_ctl_t clog_ctl, int min_clog_seg_no, uint32 next_clog_subdir_no);

extern void polar_mv_clog_seg_to_fra(flashback_clog_ctl_t clog_ctl, const char *fra_dir, const char *fname, const char *old_path, bool *need_update_ctl);

extern void polar_truncate_flashback_clog_subdir(const char *fra_dir, uint32 next_clog_subdir_no);
extern XidStatus polar_flashback_get_xid_status(TransactionId xid, TransactionId max_xid, uint32 next_clog_subdir_no, const char *fra_dir);
#endif
