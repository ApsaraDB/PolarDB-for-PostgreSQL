/*-------------------------------------------------------------------------
 *
 * polar_logindex.h
 *
 * Copyright (c) 2025, Alibaba Group Holding Limited
 *
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
 *	  src/include/access/polar_logindex.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_LOG_INDEX_H
#define POLAR_LOG_INDEX_H

#include "access/polar_logindex_internal.h"
#include "access/rmgr.h"
#include "access/xlogdefs.h"
#include "access/xlogreader.h"
#include "access/xlogutils.h"
#include "access/xlogrecovery.h"
#include "storage/block.h"
#include "storage/buf_internals.h"
#include "storage/relfilelocator.h"
#include "utils/guc.h"
#include "utils/polar_log.h"

#define LOG_INDEX_SUPPORT_NO_PREVIOUS_LSN
#define POLAR_MAX_SHMEM_NAME (128)

#define POLAR_FILE_PATH(path, orign) \
	snprintf((path), MAXPGPATH, "%s/%s", POLAR_DATA_DIR(), (orign));

typedef void (*log_index_init_lwlock_func) (log_index_snapshot logindex_snapshot);

extern int	polar_logindex_table_batch_size;
extern int	polar_max_logindex_files;
extern int	polar_trace_logindex_messages;

extern Size polar_logindex_shmem_size(uint64 logindex_mem_tbl_size, int bloom_blocks);

extern log_index_snapshot polar_logindex_snapshot_shmem_init(const char *name, uint64 logindex_mem_tbl_size, int bloom_blocks,
															 log_index_init_lwlock_func init_lwlock_func,
															 int tranche_buffer_id, int tranche_bank_id);

extern uint64 polar_logindex_convert_mem_tbl_size(uint64 mem_size);

extern MemoryContext polar_logindex_memory_context(void);
extern uint64 polar_logindex_mem_tbl_size(log_index_snapshot logindex_snapshot);
extern uint64 polar_logindex_used_mem_tbl_size(log_index_snapshot logindex_snapshot);
extern void polar_logindex_set_start_lsn(log_index_snapshot logindex_snapshot, XLogRecPtr start_lsn);
extern XLogRecPtr polar_logindex_snapshot_init(log_index_snapshot logindex_snapshot, XLogRecPtr checkpoint_lsn,
											   TimeLineID checkpoint_tli, bool read_only, bool flush_active_table);
extern void polar_logindex_add_lsn(log_index_snapshot logindex_snapshot, BufferTag *tag, XLogRecPtr prev, XLogRecPtr lsn);
extern bool polar_logindex_bg_write(log_index_snapshot logindex_snapshot);
extern XLogRecPtr polar_logindex_start_lsn(log_index_snapshot logindex_snapshot);

extern log_index_page_iter polar_logindex_create_page_iterator(log_index_snapshot logindex_snapshot, BufferTag *tag,
															   XLogRecPtr min_lsn, XLogRecPtr max_lsn, bool before_promote);
extern void polar_logindex_release_page_iterator(log_index_page_iter iter);
extern log_index_lsn_info polar_logindex_page_iterator_next(log_index_page_iter iter);
extern bool polar_logindex_page_iterator_end(log_index_page_iter iter);
extern log_index_iter_state_t polar_logindex_page_iterator_state(log_index_page_iter iter);
extern XLogRecPtr polar_logindex_page_iterator_max_lsn(log_index_page_iter iter);
extern XLogRecPtr polar_logindex_page_iterator_min_lsn(log_index_page_iter iter);
extern BufferTag *polar_logindex_page_iterator_buf_tag(log_index_page_iter iter);

extern log_index_lsn_iter polar_logindex_create_lsn_iterator(log_index_snapshot logindex_snapshot, XLogRecPtr lsn);
extern void polar_logindex_release_lsn_iterator(log_index_lsn_iter iter);
extern log_index_lsn_info polar_logindex_lsn_iterator_next(log_index_snapshot logindex_snapshot, log_index_lsn_iter iter);

extern void polar_logindex_truncate(log_index_snapshot logindex_snapshot, XLogRecPtr lsn);
extern bool polar_logindex_check_state(log_index_snapshot logindex_snapshot, uint32 state);

#define POLAR_LOGINDEX_PRIMARY_WRITE_TEST (1 << 0)
#define POLAR_LOGINDEX_REPLICA_WRITE_TEST (1 << 1)
#define POLAR_LOGINDEX_WRITE_MASK (POLAR_LOGINDEX_PRIMARY_WRITE_TEST | POLAR_LOGINDEX_REPLICA_WRITE_TEST)

#define POLAR_UPDATE_BACKEND_LSN_INTERVAL (1 * 1000)

/* define logindex_snapshot bit state */
#define POLAR_LOGINDEX_STATE_INITIALIZED    (1U << 0)	/* The logindex snapshot
														 * is initialized */
#define POLAR_LOGINDEX_STATE_ADDING         (1U << 1)	/* Finish checking saved
														 * lsn, and it's ready
														 * to add new lsn */
#define POLAR_LOGINDEX_STATE_WRITABLE       (1U << 2)	/* It's enabled to write
														 * table to shared
														 * storage */

#define LOOP_NEXT_VALUE(id, size) \
	(((id) == ((size) - 1)) ? 0 : ((id) + 1))

#define LOOP_PREV_VALUE(id, size) \
	(((id) == 0) ? ((size) - 1) : ((id) -1))

#define POLAR_LOG_INDEX_ADD_LSN(logindex_snapshot, state, tag) \
	do { \
		polar_logindex_add_lsn((logindex_snapshot), (tag), InvalidXLogRecPtr, (state)->ReadRecPtr); \
	} while (0)

#define POLAR_GET_LOG_TAG(state, tag, block_id) \
	do { \
		RelFileLocator _rlocator; \
		ForkNumber  _forknum; \
		BlockNumber _blkno; \
		if (!XLogRecGetBlockTagExtended(state, block_id, &_rlocator, &_forknum, &_blkno, NULL)) \
			elog(PANIC, "Failed to locate blod_id %d, rmid=%d,info=%d", \
				 block_id, XLogRecGetRmid(state), XLogRecGetInfo(state)); \
		InitBufferTag(&tag, &_rlocator, _forknum, _blkno); \
	} while (0)

#define NOTIFY_LOGINDEX_BG_WORKER(latch) \
	do { \
		if (latch) \
			SetLatch(latch); \
	} while (0)

extern void polar_logindex_flush_table(log_index_snapshot logindex_snapshot, XLogRecPtr checkpoint_lsn, bool flush_active);

extern XLogRecPtr polar_get_logindex_snapshot_max_lsn(log_index_snapshot logindex_snapshot);

/* Read logindex meta and get min lsn of logindex tables which are flushed to the storage */
extern XLogRecPtr polar_get_logindex_snapshot_storage_min_lsn(log_index_snapshot logindex_snapshot);

extern const char *polar_get_logindex_snapshot_dir(log_index_snapshot logindex_snapshot);

extern void polar_load_logindex_snapshot_from_storage(log_index_snapshot logindex_snapshot, XLogRecPtr start_lsn);
extern void polar_logindex_invalid_bloom_cache(log_index_snapshot logindex_snapshot, log_idx_table_id_t tid);

extern void polar_logindex_set_bgworker_latch(log_index_snapshot logindex_snapshot, struct Latch *latch);
extern void polar_replica_promote_logindex(log_index_snapshot logindex_snapshot, bool missing_ok);
extern XLogRecPtr polar_logindex_check_valid_start_lsn(log_index_snapshot logindex_snapshot);
extern int	polar_trace_logindex(int trace_level);
extern void polar_logindex_update_promoted_info(log_index_snapshot logindex_snapshot, XLogRecPtr last_replayed_lsn);
extern void polar_logindex_create_local_cache(log_index_snapshot logindex_snapshot, const char *cache_name, uint32 max_segments);
extern uint64 polar_get_estimate_fullpage_segment_size(log_index_snapshot logindex_snapshot);
#endif
