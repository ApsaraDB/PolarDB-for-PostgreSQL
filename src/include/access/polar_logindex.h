/*-------------------------------------------------------------------------
 *
 * polar_logindex.h
 *  Implementation of wal logindex snapshot.
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
 *  src/include/access/polar_logindex.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_LOG_INDEX_H
#define POLAR_LOG_INDEX_H

#include "access/polar_log.h"
#include "access/rmgr.h"
#include "access/xlogdefs.h"
#include "access/xlogreader.h"
#include "access/xlogutils.h"
#include "storage/block.h"
#include "storage/buf_internals.h"
#include "storage/relfilenode.h"
#include "utils/guc.h"

typedef uint16 log_seg_id_t;
typedef uint64 log_idx_table_id_t;
typedef uint64 log_range_id_t;
typedef uint32 polar_page_lock_t;

#define LOG_INDEX_SUPPORT_NO_PREVIOUS_LSN
#define POLAR_INVALID_PAGE_LOCK 0

struct log_mem_table_t;
struct log_index_iter_data_t;
typedef struct log_index_snapshot_t *logindex_snapshot_t;
typedef struct log_index_page_iter_data_t *log_index_page_iter_t;
typedef struct log_index_lsn_iter_data_t *log_index_lsn_iter_t;

typedef struct log_index_lsn_t
{
	BufferTag  *tag;
	XLogRecPtr	lsn;
	XLogRecPtr	prev_lsn;
} log_index_lsn_t;


typedef struct xlog_bg_redo_state_t
{
	log_index_lsn_iter_t log_index_iter;	/* record iterator */
	log_index_lsn_t *log_index_page;	/* current iterator page */
	XLogReaderState *state;
} xlog_bg_redo_state_t;

/* User-settable parameters */
extern int	polar_log_index_bloom_buffers;


extern Size polar_log_index_shmem_size(void);

extern void polar_log_index_shmem_init(void);
extern XLogRecPtr polar_log_index_snapshot_init(XLogRecPtr checkpoint_lsn, bool replica_mode);
extern void polar_log_index_add_lsn(logindex_snapshot_t logindex_snapshot, BufferTag *tag, XLogRecPtr prev, XLogRecPtr lsn);
extern bool polar_log_index_write_table(logindex_snapshot_t logindex_snapshot, struct log_mem_table_t *table);
extern void polar_log_index_bg_write(void);
extern bool polar_log_index_truncate_mem_table(XLogRecPtr lsn);
extern void polar_log_index_remove_all(void);
extern XLogRecPtr polar_log_index_start_lsn(void);

extern log_index_page_iter_t polar_log_index_create_page_iterator(logindex_snapshot_t logindex_snapshot, BufferTag *tag, XLogRecPtr min_lsn, XLogRecPtr max_lsn);
extern void polar_log_index_release_page_iterator(log_index_page_iter_t iter);
extern void polar_log_index_abort_page_iterator(void);
extern log_index_lsn_t *polar_log_index_page_iterator_next(log_index_page_iter_t iter);
extern bool polar_log_index_page_iterator_end(log_index_page_iter_t iter);

extern log_index_lsn_iter_t polar_log_index_create_lsn_iterator(logindex_snapshot_t logindex_snapshot, XLogRecPtr lsn);
extern void polar_log_index_release_lsn_iterator(log_index_lsn_iter_t iter);
extern log_index_lsn_t *polar_log_index_lsn_iterator_next(logindex_snapshot_t logindex_snapshot, log_index_lsn_iter_t iter);

extern int	polar_log_index_mini_trans_start(XLogRecPtr lsn);
extern int	polar_log_index_mini_trans_end(XLogRecPtr lsn);
extern void polar_log_index_abort_mini_transaction(void);

extern void polar_log_index_abort_replaying_buffer(void);

extern polar_page_lock_t polar_log_index_mini_trans_lock(BufferTag *tag, LWLockMode mode, XLogRecPtr *lsn);
extern polar_page_lock_t polar_log_index_mini_trans_cond_lock(BufferTag *tag, LWLockMode mode, XLogRecPtr *lsn);
extern void polar_log_index_mini_trans_unlock(polar_page_lock_t lock);
extern XLogRecPtr polar_log_index_mini_trans_find(BufferTag *tag);

extern void polar_log_index_truncate(logindex_snapshot_t logindex_snapshot, XLogRecPtr lsn);
extern bool polar_log_index_check_state(logindex_snapshot_t logindex_snapshot, uint32 state);

#define POLAR_LOGINDEX_FULLPAGE_SNAPSHOT_WORKER() (!AmStartupProcess())

#define POLAR_LOG_INDEX_ADD_LSN(state, tag) \
	do { \
		if (POLAR_LOGINDEX_FULLPAGE_SNAPSHOT_WORKER() && \
			(XLogRecGetRmid(state) == RM_XLOG_ID && \
			(XLogRecGetInfo(state) & ~XLR_INFO_MASK) == XLOG_FPSI)) \
			polar_log_index_add_lsn(POLAR_LOGINDEX_FULLPAGE_SNAPSHOT, (tag), InvalidXLogRecPtr, (state)->ReadRecPtr); \
		else \
			polar_log_index_add_lsn(POLAR_LOGINDEX_WAL_SNAPSHOT, (tag), InvalidXLogRecPtr, (state)->ReadRecPtr); \
	} while (0)

#define POLAR_GET_LOG_TAG(state, tag, block_id) \
	do { \
		RelFileNode rnode; \
		ForkNumber  forknum; \
		BlockNumber blkno; \
		if (!XLogRecGetBlockTag(state, block_id, &rnode, &forknum, &blkno)) \
			elog(PANIC, "Failed to locate blod_id %d, rmid=%d,info=%d", \
				 block_id, XLogRecGetRmid(state), XLogRecGetInfo(state)); \
		INIT_BUFFERTAG(tag, rnode, forknum, blkno); \
	} while (0)

#define POLAR_READ_MODE(buffer) (BufferIsValid(buffer) ? RBM_NORMAL_VALID : RBM_NORMAL)

#define POLAR_READ_BUFFER_FOR_REDO(record, block_id, buffer) \
	XLogReadBufferForRedoExtended((record), (block_id),\
								  POLAR_READ_MODE(*(buffer)), false, (buffer))

#define POLAR_INIT_BUFFER_FOR_REDO(record, block_id, buffer) \
	do { \
		*(buffer) = (!BufferIsValid(*buffer)) ? XLogInitBufferForRedo(record, block_id) : *(buffer); \
	} while (0)

#define POLAR_LOGINDEX_MASTER_WRITE_TEST (1 << 0)
#define POLAR_LOGINDEX_REPLICA_WRITE_TEST (1 << 1)
#define POLAR_LOGINDEX_WRITE_MASK (POLAR_LOGINDEX_MASTER_WRITE_TEST | POLAR_LOGINDEX_REPLICA_WRITE_TEST)

#define POLAR_ENABLE_PAGE_OUTDATE() (polar_in_replica_mode() && polar_enable_redo_logindex && polar_enable_page_outdate)
#define POLAR_UPDATE_BACKEND_LSN_INTERVAL (1 * 1000)

/* define logindex_snapshot bit state */
#define POLAR_LOGINDEX_STATE_INITIALIZED (1U << 0)	/* The logindex snapshot
													 * is initialized */
#define POLAR_LOGINDEX_STATE_ADDING      (1U << 1)	/* Finish checking saved
													 * lsn, and it's ready to
													 * add new lsn */
#define POLAR_LOGINDEX_STATE_WAITING     (1U << 2)	/* Wait for new active
													 * table */

extern bool polar_log_index_parse_xlog(RmgrId rmid, XLogReaderState *state, XLogRecPtr redo_start_lsn, XLogRecPtr *mini_trans_lsn);

extern void polar_log_index_apply_buffer(Buffer *buffer, XLogRecPtr start_lsn, polar_page_lock_t page_lock);
extern void polar_log_index_lock_apply_buffer(Buffer *buffer);
extern void polar_log_index_lock_apply_page_from(XLogRecPtr start_lsn, BufferTag *tag, Buffer *buffer);
extern bool polar_log_index_restore_fullpage_snapshot_if_needed(BufferTag *tag, Buffer *buffer);

extern void polar_log_index_apply_one_record(XLogReaderState *state, BufferTag *tag, Buffer *buffer);

extern void polar_log_index_truncate_lsn(void);
extern void polar_log_index_flush_table(logindex_snapshot_t logindex_snapshot, XLogRecPtr checkpoint_lsn);

extern bool polar_enable_logindex_parse(void);
extern void polar_log_index_trigger_bgwriter(XLogRecPtr lsn);
extern void log_index_validate_dir(void);

extern void polar_bg_redo_init_replayed_lsn(XLogRecPtr lsn);
extern XLogRecPtr polar_bg_redo_get_replayed_lsn(void);
extern void polar_bg_redo_mark_logindex_ready(void);
extern bool polar_log_index_apply_xlog_background(void);
extern XLogRecPtr polar_max_xlog_rec_ptr(XLogRecPtr a, XLogRecPtr b);
extern XLogRecPtr polar_min_xlog_rec_ptr(XLogRecPtr a, XLogRecPtr b);
extern void polar_log_page_iter_context(void);
extern XLogRecPtr polar_get_logindex_snapshot_max_lsn(logindex_snapshot_t logindex_snapshot);
extern void polar_load_logindex_snapshot_from_storage(logindex_snapshot_t logindex_snapshot, XLogRecPtr start_lsn, bool is_init);
extern void polar_log_index_invalid_bloom_cache(logindex_snapshot_t logindex_snapshot, log_idx_table_id_t tid);

/*
 * POLAR: Flags for buffer redo state
 */
#define POLAR_REDO_LOCKED               (1U << 1)	/* redo state is locked */
#define POLAR_REDO_READ_IO_END          (1U << 2)	/* Finish to read buffer
													 * content from storage */
#define POLAR_REDO_REPLAYING            (1U << 3)	/* It's replaying buffer
													 * content */
#define POLAR_REDO_OUTDATE              (1U << 4)	/* The buffer content is
													 * outdated */

/*
 * Functions for acquiring/releasing a shared buffer redo state's spinlock.
 */
extern uint32 polar_lock_redo_state(BufferDesc *desc);
inline static void
polar_unlock_redo_state(BufferDesc *desc, uint32 state)
{
	do
	{
		pg_write_barrier();
		pg_atomic_write_u32(&(desc->polar_redo_state), state & (~POLAR_REDO_LOCKED));
	}
	while (0);
}

inline static bool
polar_redo_check_state(BufferDesc *desc, uint32 state)
{
	pg_read_barrier();
	return pg_atomic_read_u32(&(desc->polar_redo_state)) & state;
}
#endif
