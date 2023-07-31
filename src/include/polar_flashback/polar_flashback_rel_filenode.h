/*-------------------------------------------------------------------------
 *
 * polar_flashback_rel_filenode.h
 *
 *
 * Copyright (c) 2021, Alibaba Group Holding limited
 *
 * src/include/polar_flashback/polar_flashback_rel_filenode.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_FLASHBACK_REL_FILENODE_H
#define POLAR_FLASHBACK_REL_FILENODE_H

#include "datatype/timestamp.h"
#include "polar_flashback/polar_fast_recovery_area.h"
#include "polar_flashback/polar_flashback_log.h"
#include "storage/buf_internals.h"

/* It is a fake fork, just be used by search the origin filenode. */
#define FILENODE_FORK (MAX_FORKNUM + 1)

/* Every thing for relation file node record (xl_rmid = REL_FILENODE_ID in the flashback log) */
#define REL_CAN_FLASHBACK 0x01
#define REL_FILENODE_TYPE_MASK 0x01

#define FL_FILENODE_REC_SIZE (offsetof(fl_filenode_rec_data_t, time) + sizeof(TimestampTz))
#define FL_GET_FILENODE_REC_DATA(rec) \
	((fl_filenode_rec_data_t *)(FL_GET_REC_DATA(rec))

typedef struct fl_filenode_rec_data_t
{
	RelFileNode old_filenode;
	RelFileNode new_filenode;
	TimestampTz  time;
} fl_filenode_rec_data_t;

#define POLAR_GET_FILENODE_REC_LEN(rec_len) ((rec_len) + FL_FILENODE_REC_SIZE)

extern void polar_flog_filenode_update(flog_ctl_t flog_ins, fra_ctl_t fra_ins, Oid relid, Oid new_rnode, Oid new_tablespace, bool change_persistence, bool can_flashback);
extern bool polar_find_origin_filenode(flog_ctl_t ins, RelFileNode *filenode, TimestampTz target_time, polar_flog_rec_ptr start_ptr, polar_flog_rec_ptr end_ptr, flog_reader_state *reader);
#endif
