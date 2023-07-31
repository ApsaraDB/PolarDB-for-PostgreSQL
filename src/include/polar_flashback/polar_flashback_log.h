/*-------------------------------------------------------------------------
 *
 * polar_flashback_log.h
 *
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
 * IDENTIFICATION
 *    src/include/polar_flashback/polar_flashback_log.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_FLASHBACK_LOG_H
#define POLAR_FLASHBACK_LOG_H

#include "access/polar_log.h"
#include "access/polar_logindex.h"
#include "access/polar_logindex_redo.h"
#include "access/xlogdefs.h"
#include "catalog/pg_control.h"
#include "polar_flashback/polar_flashback_log_index_queue.h"
#include "polar_flashback/polar_flashback_log_internal.h"
#include "polar_flashback/polar_flashback_log_list.h"
#include "polar_flashback/polar_flashback_log_mem.h"
#include "polar_flashback/polar_flashback_log_reader.h"
#include "storage/buf_internals.h"

#define POLAR_IS_NEED_FLOG(forkno) ((forkno) == MAIN_FORKNUM)
#define POLAR_CHECK_BUF_FLOG_STATE(buf_hdr, state) polar_redo_check_state(buf_hdr, state)

#define POLAR_LOG_FLOG_LOST(buf_tag, elevel) \
	do { \
		elog(elevel, "The origin page of " POLAR_LOG_BUFFER_TAG_FORMAT " is lost", \
				POLAR_LOG_BUFFER_TAG(buf_tag)); \
	}while (0)

/*
 * POLAR: Flashback log do checkpoint. Now be called by create checkpoint
 * or restartpoint.
 * NB: It is Different from polar_flog_do_fbpoint.
 * It is called in every checkpoint not only flashback point.
 */
#define POLAR_CHECK_POINT_FLOG(ins, lsn) \
	do { \
		if (ins) {(ins)->buf_ctl->redo_lsn = (lsn);} \
	}while (0)

#define POLAR_FLOG_SHMEM_SIZE() \
	polar_flog_shmem_size_internal(polar_flashback_log_insert_locks, \
			polar_flashback_log_buffers, polar_flashback_logindex_mem_size, \
			polar_flashback_logindex_bloom_blocks, polar_flashback_logindex_queue_buffers) \

#define POLAR_FLOG_SHMEM_INIT() \
	do { \
		flog_instance = polar_flog_shmem_init_internal(POLAR_FL_DEFAULT_DIR, \
				polar_flashback_log_insert_locks, polar_flashback_log_buffers, \
				polar_flashback_logindex_mem_size, polar_flashback_logindex_bloom_blocks, \
				polar_flashback_logindex_queue_buffers); \
	}while (0)

typedef struct flog_ctl_data_t
{
	flog_buf_ctl_t buf_ctl;
	flog_list_ctl_t list_ctl;
	logindex_snapshot_t logindex_snapshot;
	flog_index_queue_ctl_t queue_ctl;
	Latch *bgwriter_latch;
	pg_atomic_uint32 state;
} flog_ctl_data_t;

#define FLOG_INIT 0
#define FLOG_STARTUP 1
#define FLOG_READY 2

typedef flog_ctl_data_t *flog_ctl_t;

extern flog_ctl_t flog_instance;

typedef struct flshbak_buf_context_t
{
	polar_flog_rec_ptr start_ptr; /* The flashback log start point to flashback buffer. */
	polar_flog_rec_ptr end_ptr; /* The flashback log end point to flashback buffer. */
	XLogRecPtr start_lsn; /* The WAL start lsn to replay. */
	XLogRecPtr end_lsn; /* The WAL end lsn to replay. */
	logindex_snapshot_t logindex_snapshot; /* The flashback logindex snapshot to find origin page */
	BufferTag *tag; /* The origin page buffer tag. NB: This may be not as same as the target buffer */
	flog_reader_state *reader; /* The flashback log reader */
	Buffer buf; /* The target buffer */
	int elevel; /* The elog level when we can't find a valid origin page */
	bool apply_fpi; /* Apply the full page image wal record or not. */
} flshbak_buf_context_t;

#define INIT_FLSHBAK_BUF_CONTEXT(a, xx_start_ptr, xx_end_ptr, xx_start_lsn, \
		xx_end_lsn, xx_flog_index, xx_reader, xx_tag, xx_buf, xx_elevel, xx_apply_fpi) \
	do { \
		(a).start_ptr = (xx_start_ptr); \
		(a).end_ptr = (xx_end_ptr); \
		(a).start_lsn = (xx_start_lsn); \
		(a).end_lsn = (xx_end_lsn); \
		(a).logindex_snapshot = (xx_flog_index); \
		(a).reader = (xx_reader); \
		(a).tag = (xx_tag); \
		(a).buf = (xx_buf); \
		(a).elevel = (xx_elevel); \
		(a).apply_fpi = (xx_apply_fpi); \
		Assert((a).end_ptr >= (a).start_ptr); \
	}while (0)

/* GUCs */
extern int polar_flashback_logindex_mem_size;
extern int polar_flashback_logindex_bloom_blocks;
extern int polar_flashback_logindex_queue_buffers;
/* For the flashback log buffers */
extern int polar_flashback_log_buffers;
extern int polar_flashback_log_insert_locks;

extern void set_buf_flog_state(BufferDesc *buf_desc, uint32 flog_state);
extern void clean_buf_flog_state(BufferDesc *buf_hdr, uint32 flog_state);

extern bool polar_is_flog_mem_enabled(void);
extern bool polar_is_flog_enabled(flog_ctl_t instance);
extern bool polar_is_flog_needed(flog_ctl_t flog_ins, polar_logindex_redo_ctl_t redo_ins, ForkNumber forkno, Page page, bool is_permanent, XLogRecPtr fbpoint_lsn, XLogRecPtr redo_lsn);
extern bool polar_is_buf_flog_enabled(flog_ctl_t instance, Buffer buf);

extern bool polar_is_flog_ready(flog_ctl_t instance);
extern bool polar_has_flog_startup(flog_ctl_t instance);

extern Size polar_flog_shmem_size_internal(int insert_locks_num,
										   int log_buffers, int logindex_mem_size, int logindex_bloom_blocks, int queue_buffers_MB);

extern void polar_flog_ctl_init_data(flog_ctl_t ctl);
extern flog_ctl_t polar_flog_shmem_init_internal(const char *name, int insert_locks_num, int log_buffers,
												 int logindex_mem_size, int logindex_bloom_blocks, int queue_buffers_MB);

extern void polar_flog_do_fbpoint(flog_ctl_t instance, polar_flog_rec_ptr ckp_start, polar_flog_rec_ptr keep_ptr, bool shutdown);

extern void polar_flog_insert(flog_ctl_t instance, Buffer buf, bool is_candidate, bool is_recovery);
extern void polar_check_fpi_origin_page(RelFileNode rnode, ForkNumber forkno, BlockNumber block, uint8 xl_info);
extern void polar_flush_buf_flog_rec(BufferDesc *buf_hdr, flog_ctl_t instance, bool invalidate);

extern void polar_remove_all_flog_data(flog_ctl_t instance);

extern void polar_startup_flog(CheckPoint *checkpoint, flog_ctl_t instance);
extern void polar_recover_flog_buf(flog_ctl_t instance);
extern void polar_recover_flog(flog_ctl_t instance);
extern void polar_set_buf_flog_lost_checked(flog_ctl_t flog_ins, polar_logindex_redo_ctl_t redo_ins, Buffer buffer);
extern bool polar_may_buf_lost_flog(flog_ctl_t flog_ins, polar_logindex_redo_ctl_t redo_ins, BufferDesc *buf_desc);
extern void polar_make_true_no_flog(flog_ctl_t instance, BufferDesc *buf);

extern void polar_get_buffer_tag_in_flog_rec(flog_record *rec, BufferTag *tag);

extern void polar_flog_rel_bulk_extend(flog_ctl_t instance, Buffer buffer);

extern bool polar_can_flog_repair(flog_ctl_t instance, BufferDesc *buf_hdr, bool has_redo_action);
extern void polar_repair_partial_write(flog_ctl_t instance, BufferDesc *bufHdr);

typedef struct flog_insert_context
{
	BufferTag   *buf_tag;
	void        *data;
	XLogRecPtr  redo_lsn;
	RmgrId      rmgr;
	uint8       info;
} flog_insert_context;

extern flog_record *polar_assemble_filenode_rec(flog_insert_context *insert_context, uint32 xl_tot_len);
extern polar_flog_rec_ptr polar_flog_insert_into_buffer(flog_ctl_t instance, flog_insert_context *insert_context);
extern polar_flog_rec_ptr polar_insert_buf_flog_rec(flog_ctl_t instance, BufferTag *tag,
		XLogRecPtr redo_lsn, XLogRecPtr fbpoint_lsn, uint8 info, Page origin_page, bool from_origin_buf);
extern bool polar_process_buf_flog_list(flog_ctl_t instance, BufferDesc *buf_hdr,
		bool is_background, bool invalidate);
extern void polar_process_flog_list_bg(flog_ctl_t instance);

extern bool polar_get_origin_page(flshbak_buf_context_t *context, Page page, XLogRecPtr *replay_start_lsn);
extern bool polar_flashback_buffer(flshbak_buf_context_t *context);
#endif
