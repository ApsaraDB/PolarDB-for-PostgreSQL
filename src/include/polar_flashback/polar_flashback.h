/*-------------------------------------------------------------------------
 *
 * polar_flashback.h
 *
 *
 * Copyright (c) 2021, Alibaba Group Holding limited
 *
 * src/include/polar_flashback/polar_flashback.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_FLASHBACK_H
#define POLAR_FLASHBACK_H

#include "catalog/pg_control.h"
#include "polar_flashback/polar_flashback_snapshot.h"
#include "polar_flashback/polar_flashback_log_internal.h"

extern Size polar_flashback_shmem_size(void);
extern void polar_flashback_shmem_init(void);
extern void polar_startup_flashback(CheckPoint *checkpoint);
extern void polar_do_flashback_point(polar_flog_rec_ptr ckp_start, flashback_snapshot_header_t snapshot, bool shutdown);
extern XLogRecPtr polar_get_flashback_keep_wal(XLogRecPtr keep);
extern void polar_write_ctl_file_atomic(const char *path, void *data, size_t size, uint32 write_event_info,
										uint32 fsync_event_info);
#endif
