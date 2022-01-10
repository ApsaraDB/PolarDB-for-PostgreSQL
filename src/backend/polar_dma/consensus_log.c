/**
 * consensus_log.c
 * 	Implementation of concensus log writer
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
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
 * 	src/backend/polar_dma/consensus_log.c
 *
 *-------------------------------------------------------------------------
 *
 */

#include "easy_log.h"

#include "postgres.h"

#include "polar_consensus_c.h"

#include "common/file_perm.h"
#include "miscadmin.h"
#include "polar_dma/polar_dma.h"
#include "polar_dma/consensus_repl.h"
#include "polar_dma/consensus_log.h"
#include "polar_dma/consensus_slru.h"
#include "port/atomics.h"
#include "port/pg_crc32c.h"
#include "storage/shmem.h"
#include "storage/polar_fd.h"
#include "utils/guc.h"


typedef struct ConsensusMetaCtlData
{
	pthread_rwlock_t lock;
	ConsensusMetaHeader header;
	char		member_info_array[MEMBER_INFO_MAX_LENGTH];
	char 		learner_info_array[MEMBER_INFO_MAX_LENGTH];
	char		*member_info_str;
	char 		*learner_info_str;
	const vfs_mgr *vfs_api;

} ConsensusMetaCtlData;

typedef struct ConsensusLogCtlData
{
	uint64 	variable_length_log_next_offset;
	pthread_rwlock_t fixed_length_log_lock;
	pthread_rwlock_t variable_length_log_lock;

	uint64 	current_term;
	pthread_rwlock_t 	term_lock;

	uint64  current_index;
	uint64  sync_index;
	XLogRecPtr	last_write_lsn;
	TimeLineID last_write_timeline;
	uint64  last_append_term;
	uint64 	next_append_term;
	bool		disable_purge; /* disable Consensus Log truncate backward */
	uint64  mock_start_index;
	pthread_rwlock_t 	info_lock;

#ifndef PG_HAVE_ATOMIC_U32_SIMULATION 
	pg_atomic_uint32	log_keep_size;
#else
	uint32	log_keep_size; /* Protected by _info_lock */
#endif

#ifndef PG_HAVE_ATOMIC_U64_SIMULATION
	struct
	{
		pg_atomic_uint64 log_reads;
		pg_atomic_uint64 variable_log_reads;
		pg_atomic_uint64 log_appends;
		pg_atomic_uint64 variable_log_appends;
		pg_atomic_uint64 log_flushes;
		pg_atomic_uint64 meta_flushes;
		pg_atomic_uint64 xlog_flush_tries;
		pg_atomic_uint64 xlog_flush_failed_tries;
		pg_atomic_uint64 xlog_transit_waits;
	} stats;
#endif
} ConsensusLogCtlData;

#define FixedLengthLogIndexToPage(log_index) \
	((log_index - 1) / FIXED_LENGTH_LOGS_PER_PAGE)
#define FixedLengthLogIndexToEntry(log_index) \
	((log_index - 1) % FIXED_LENGTH_LOGS_PER_PAGE)

/* We store the latest LSN for each group of log index */
#define FIXED_LENGTH_LOGS_PER_LSN_GROUP	8	/* keep this a power of 2 */
#define FIXED_LENGTH_LOG_LSNS_PER_PAGE	(FIXED_LENGTH_LOGS_PER_PAGE / FIXED_LENGTH_LOGS_PER_LSN_GROUP)

#define GetLSNIndex(slotno, logindex)	((slotno) * FIXED_LENGTH_LOG_LSNS_PER_PAGE + \
	((logindex) % FIXED_LENGTH_LOG_LSNS_PER_PAGE) / FIXED_LENGTH_LOGS_PER_LSN_GROUP )

#define VariableLengthLogOffsetToPage(offset) \
	((offset) / CONSENSUS_CC_LOG_PAGE_SIZE)
#define VariableLengthLogOffsetToPos(offset) \
	((offset) % CONSENSUS_CC_LOG_PAGE_SIZE)

static ConsensusMetaCtlData *ConsensusMetaCtl;

static ConsensusLogCtlData *ConsensusLogCtl;

static ConsensusSlruCtlData ConsensusFixedLengthLogCtlData;
static ConsensusSlruCtlData ConsensusVariableLengthLogCtlData;

#define ConsensusFixedLengthLogCtl	(&ConsensusFixedLengthLogCtlData)
#define ConsensusVariableLengthLogCtl	(&ConsensusVariableLengthLogCtlData)

static bool consensus_log_try_flush_fixed_length_page(int slotno);
static void consensus_meta_dir(char *subdir, char *path);
static void consensus_meta_file(char *subdir, char *path);
static bool consensus_meta_flush_internal(void);
static uint64 consensus_log_get_variable_length_offset(void);

static void
consensus_log_stats_init(void)
{
#ifndef PG_HAVE_ATOMIC_U64_SIMULATION
	pg_atomic_init_u64(&ConsensusLogCtl->stats.log_reads, 0);
	pg_atomic_init_u64(&ConsensusLogCtl->stats.variable_log_reads, 0);
	pg_atomic_init_u64(&ConsensusLogCtl->stats.log_appends, 0);
	pg_atomic_init_u64(&ConsensusLogCtl->stats.variable_log_appends, 0);
	pg_atomic_init_u64(&ConsensusLogCtl->stats.log_flushes, 0);
	pg_atomic_init_u64(&ConsensusLogCtl->stats.meta_flushes, 0);
	pg_atomic_init_u64(&ConsensusLogCtl->stats.xlog_flush_tries, 0);
	pg_atomic_init_u64(&ConsensusLogCtl->stats.xlog_flush_failed_tries, 0);
	pg_atomic_init_u64(&ConsensusLogCtl->stats.xlog_transit_waits, 0);
#endif
}

void
ConsensusLogGetStats(ConsensusLogStats *log_stats)
{
#ifndef PG_HAVE_ATOMIC_U64_SIMULATION
	log_stats->log_reads =
		pg_atomic_read_u64(&ConsensusLogCtl->stats.log_reads);
	log_stats->variable_log_reads =
		pg_atomic_read_u64(&ConsensusLogCtl->stats.variable_log_reads);
	log_stats->log_appends =
		pg_atomic_read_u64(&ConsensusLogCtl->stats.log_appends);
	log_stats->variable_log_appends =
		pg_atomic_read_u64(&ConsensusLogCtl->stats.variable_log_appends);
	log_stats->log_flushes =
		pg_atomic_read_u64(&ConsensusLogCtl->stats.log_flushes);
	log_stats->meta_flushes =
		pg_atomic_read_u64(&ConsensusLogCtl->stats.meta_flushes);
	log_stats->xlog_flush_tries =
		pg_atomic_read_u64(&ConsensusLogCtl->stats.xlog_flush_tries);
	log_stats->xlog_flush_failed_tries =
		pg_atomic_read_u64(&ConsensusLogCtl->stats.xlog_flush_failed_tries);
	log_stats->xlog_transit_waits =
		pg_atomic_read_u64(&ConsensusLogCtl->stats.xlog_transit_waits);
#else
	memset(log_stats, 0, sizeof(ConsensusLogStats));
#endif
}

void
ConsensusLogGetStatus(ConsensusLogStatus *log_status)
{
	pthread_rwlock_rdlock(&ConsensusLogCtl->info_lock);
	log_status->current_index = ConsensusLogCtl->current_index;
	log_status->sync_index = ConsensusLogCtl->sync_index;
	log_status->last_write_lsn = ConsensusLogCtl->last_write_lsn;
	log_status->last_write_timeline = ConsensusLogCtl->last_write_timeline;
	log_status->last_append_term = ConsensusLogCtl->last_append_term;
	log_status->next_append_term = ConsensusLogCtl->next_append_term;
	log_status->disable_purge = ConsensusLogCtl->disable_purge;
	pthread_rwlock_unlock(&ConsensusLogCtl->info_lock);

	log_status->current_term = ConsensusLOGGetTerm();
	log_status->variable_length_log_next_offset =
		consensus_log_get_variable_length_offset();
}

void
ConsensusMetaGetStatus(ConsensusMetaStatus *meta_status)
{
	pthread_rwlock_rdlock(&ConsensusMetaCtl->lock);
	meta_status->current_term = ConsensusMetaCtl->header.current_term;
	meta_status->vote_for = ConsensusMetaCtl->header.vote_for;
	meta_status->last_leader_term = ConsensusMetaCtl->header.last_leader_term;
	meta_status->last_leader_log_index = ConsensusMetaCtl->header.last_leader_log_index;
	meta_status->scan_index = ConsensusMetaCtl->header.scan_index;
	meta_status->cluster_id = ConsensusMetaCtl->header.cluster_id;
	meta_status->commit_index = ConsensusMetaCtl->header.commit_index;
	meta_status->purge_index = ConsensusMetaCtl->header.purge_index;
	pthread_rwlock_unlock(&ConsensusMetaCtl->lock);
}


/*
 * Initialization of shared memory for CLOG
 */
Size
ConsensusLOGShmemSize(void)
{
	Size sz;

	sz = ConsensusSimpleLruShmemSize(polar_dma_log_slots,
															 CONSENSUS_LOG_PAGE_SIZE, FIXED_LENGTH_LOG_LSNS_PER_PAGE);
	sz = add_size(sz, ConsensusSimpleLruShmemSize(128, CONSENSUS_CC_LOG_PAGE_SIZE, 0));

	sz = add_size(sz, sizeof(ConsensusLogCtlData));
	sz = add_size(sz, sizeof(ConsensusMetaCtlData));

	return sz;
}

void
ConsensusLOGShmemInit(void)
{
	bool		found;
	char 		path[MAXPGPATH];
	pthread_rwlockattr_t lock_attr;

	pthread_rwlockattr_init(&lock_attr);
	pthread_rwlockattr_setpshared(&lock_attr,PTHREAD_PROCESS_SHARED);

	easy_debug_log("Shared Memory Init for Consensus Service Log");

	/* Initialize our shared state struct */
	ConsensusLogCtl = ShmemInitStruct("Shared Consensus Log State",
									 sizeof(ConsensusLogCtlData),
									 &found);

	if (!IsUnderPostmaster)
	{
		Assert(!found);

		/* Make sure we zero out the per-backend state */
		MemSet(ConsensusLogCtl, 0, sizeof(ConsensusLogCtlData));

		consensus_log_stats_init();

		pthread_rwlock_init(&ConsensusLogCtl->info_lock, &lock_attr);
	}
	else
		Assert(found);

	ConsensusMetaCtl = ShmemInitStruct("Shared Consensus Meta State",
									 sizeof(ConsensusMetaCtlData),
									 &found);

	if (!IsUnderPostmaster)
	{
		Assert(!found);

		/* Make sure we zero out the per-backend state */
		MemSet(ConsensusMetaCtl, 0, sizeof(ConsensusMetaCtlData));

		consensus_meta_dir("polar_dma", path);
		ConsensusMetaCtl->vfs_api = polar_vfs_mgr(path);

		pthread_rwlock_init(&ConsensusMetaCtl->lock, &lock_attr);
	}
	else
		Assert(found);

	ConsensusSlruStatsInit();

	ConsensusSimpleLruInit(ConsensusFixedLengthLogCtl,
				  "consensus_log", CONSENSUS_LOG_PAGE_SIZE, polar_dma_log_slots,
					FIXED_LENGTH_LOG_LSNS_PER_PAGE, &ConsensusLogCtl->fixed_length_log_lock,
					consensus_log_try_flush_fixed_length_page,
					"polar_dma/consensus_log",
				  polar_enable_shared_storage_mode);
	ConsensusSimpleLruInit(ConsensusVariableLengthLogCtl,
				  "consensus_cc_log", CONSENSUS_CC_LOG_PAGE_SIZE, 128,
					0, &ConsensusLogCtl->variable_length_log_lock, NULL,
					"polar_dma/consensus_cc_log",
				  polar_enable_shared_storage_mode);
}

/*
 * This func must be called ONCE on system install.
 */
static bool
consensus_log_validate_dir(void)
{
	if (!ConsensusSimpleLruValidateDir(ConsensusFixedLengthLogCtl))
	{
		return false;
	}

	if (!ConsensusSimpleLruValidateDir(ConsensusVariableLengthLogCtl))
	{
		return false;
	}

	return true;
}

bool
ConsensusLOGStartup(void)
{
	uint64 	pageno;
	int			slotno;
	char		*page_header_ptr;
	uint64 	last_commit_index = 0;
	uint64 	last_log_index = 0;
	uint64	last_offset = 0;
	XLogRecPtr  last_write_lsn = 0;
	TimeLineID last_write_timeline = 0;
	uint64  last_append_term = 0;
	ConsensusLogEntry *log_entry;
	FixedLengthLogPageTrailer *page_trailer;
	uint64 mock_start_index = 0;
	uint64 mock_start_tli = 0;
	XLogRecPtr mock_start_lsn = InvalidXLogRecPtr;

	if (!consensus_log_validate_dir())
		return false;

	ConsensusMetaGetInt64(CommitIndexMetaKey, &last_commit_index);
	ConsensusMetaGetInt64(MockStartIndex, &mock_start_index);
	ConsensusMetaGetInt64(MockStartTLI, &mock_start_tli);
	ConsensusMetaGetInt64(MockStartLSN, &mock_start_lsn);

	if (last_commit_index == 0)
		pageno = 0;
	else
		pageno = FixedLengthLogIndexToPage(last_commit_index);

	pthread_rwlock_wrlock(&ConsensusLogCtl->info_lock);

	do
	{
		slotno = ConsensusSimpleLruReadPage_ReadOnly(ConsensusFixedLengthLogCtl, pageno);
		if (slotno == -1)
		{
			pthread_rwlock_unlock(&ConsensusLogCtl->fixed_length_log_lock);
			break;
		}

		page_header_ptr = ConsensusFixedLengthLogCtl->shared->page_buffer[slotno];
		page_trailer = (FixedLengthLogPageTrailer *)(page_header_ptr +
				CONSENSUS_LOG_PAGE_SIZE - sizeof(FixedLengthLogPageTrailer));

		if (page_trailer->end_entry == 0)
		{
			pthread_rwlock_unlock(&ConsensusLogCtl->fixed_length_log_lock);
			break;
		}

		last_log_index = pageno * FIXED_LENGTH_LOGS_PER_PAGE + page_trailer->end_entry;
		last_offset = page_trailer->end_offset;

		log_entry = (ConsensusLogEntry *) (page_header_ptr +
				(page_trailer->end_entry - 1) * sizeof(ConsensusLogEntry));
		last_write_lsn = log_entry->log_lsn;
		last_write_timeline = log_entry->log_timeline;
		last_append_term = log_entry->header.log_term;

		ConsensusFixedLengthLogCtl->shared->latest_page_number = pageno;

		pthread_rwlock_unlock(&ConsensusLogCtl->fixed_length_log_lock);

		pageno++;
	} while (page_trailer->end_entry == FIXED_LENGTH_LOGS_PER_PAGE);

	pthread_rwlock_wrlock(&ConsensusLogCtl->variable_length_log_lock);
	ConsensusVariableLengthLogCtl->shared->latest_page_number =
		VariableLengthLogOffsetToPage(last_offset);
	ConsensusLogCtl->variable_length_log_next_offset = last_offset;
	pthread_rwlock_unlock(&ConsensusLogCtl->variable_length_log_lock);

	if (last_log_index >= mock_start_index)
	{
		ConsensusLogCtl->sync_index = last_log_index;
		ConsensusLogCtl->current_index = last_log_index + 1;
		ConsensusLogCtl->last_write_lsn = last_write_lsn;
		ConsensusLogCtl->last_write_timeline = last_write_timeline;
		ConsensusLogCtl->last_append_term = last_append_term;
		ConsensusLogCtl->mock_start_index = mock_start_index;
	}
	else
	{
		ConsensusLogCtl->sync_index = mock_start_index > 1 ? mock_start_index - 1 : 0;
		ConsensusLogCtl->current_index = mock_start_index;
		ConsensusLogCtl->last_write_lsn = mock_start_lsn;
		ConsensusLogCtl->last_write_timeline = (uint32)mock_start_tli;
		ConsensusLogCtl->last_append_term = last_append_term;
		ConsensusLogCtl->mock_start_index = mock_start_index;
	}

	pthread_rwlock_unlock(&ConsensusLogCtl->info_lock);

	if (last_commit_index != 0 && last_log_index == 0)
	{
		easy_error_log("Consensus log startup failed, could not read "
				"last committed index %llu from consensus log", last_commit_index);
		return false;
	}

	easy_warn_log("Consensus log startup finished, last_log_index: %llu, last_write_lsn: %X/%X, "
			"last_write_timeline: %u, last_append_term: %llu, last_offset: %llu, "
			"mock_start_index: %llu, mock_start_lsn: %X/%X, mock_start_timeline: %u",
			pageno, (uint32) (last_write_lsn >> 32), (uint32) last_write_lsn,
			last_write_timeline, last_append_term, last_offset,
			mock_start_index, (uint32) (mock_start_lsn >> 32), (uint32) mock_start_lsn,
			(uint32)mock_start_tli);

	return true;
}

void
ConsensusLOGNormalPayloadInit(ConsensusLogPayload *log_payload,
		XLogRecPtr log_lsn, TimeLineID log_timeline)
{
	log_payload->fixed_value.log_lsn = log_lsn;
	log_payload->fixed_value.log_timeline = log_timeline;
}

static bool
consensus_log_append_fixed_length_log(ConsensusLogEntry *log_entry, uint64 mock_start_index,
		uint64 last_term, TimeLineID last_timeline, XLogRecPtr last_lsn,
		uint64 start_offset, uint64 end_offset, bool flush)
{
	uint64	pageno;
	uint64	log_index;
	int			entryno;
	int			slotno;
	ConsensusLogEntry *log_entry_ptr;
	char		*page_header_ptr;
	FixedLengthLogPageTrailer *page_trailer;
	pg_crc32c	crc;
	bool 		ok = true;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, ((char *) log_entry), offsetof(ConsensusLogEntry, log_crc));
	log_entry->log_crc = crc;

	log_index = log_entry->header.log_index;
	pageno = FixedLengthLogIndexToPage(log_index);
	entryno = FixedLengthLogIndexToEntry(log_index);

	easy_info_log("Consensus log append fixed entry, log index: %llu, pageno: %llu, entryno :%d",
			log_index, pageno, entryno);

	pthread_rwlock_wrlock(&ConsensusLogCtl->fixed_length_log_lock);

	/* extend new page */
	if (entryno == 0 || mock_start_index == log_index)
	{
		slotno = ConsensusSimpleLruZeroPage(ConsensusFixedLengthLogCtl, pageno);

		page_header_ptr = ConsensusFixedLengthLogCtl->shared->page_buffer[slotno];
		page_trailer = (FixedLengthLogPageTrailer *)(page_header_ptr +
				CONSENSUS_LOG_PAGE_SIZE - sizeof(FixedLengthLogPageTrailer));

		page_trailer->last_term = last_term;
		page_trailer->start_lsn = last_lsn;
		page_trailer->start_timeline = last_timeline;
		page_trailer->start_offset = start_offset;

		easy_info_log("Consensus fixed length log new page %llu, last_term: %llu, start_lsn: %X/%X, "
				"start_timeline: %u, start_offset: %llu", pageno, last_term,
				(uint32) (last_lsn>> 32), (uint32) last_lsn, last_timeline, start_offset);
	}
	else
	{

		slotno = ConsensusSimpleLruReadPage(ConsensusFixedLengthLogCtl, pageno, true);
		if (slotno == -1)
		{
			pthread_rwlock_unlock(&ConsensusLogCtl->fixed_length_log_lock);
			easy_error_log("could not read block %llu of consensus log \"%s\"", pageno,
					ConsensusFixedLengthLogCtl->Dir);
			return false;
		}

		page_header_ptr = ConsensusFixedLengthLogCtl->shared->page_buffer[slotno];
		page_trailer = (FixedLengthLogPageTrailer *)(page_header_ptr +
				CONSENSUS_LOG_PAGE_SIZE - sizeof(FixedLengthLogPageTrailer));
	}

	page_trailer->end_entry = entryno + 1;
	page_trailer->end_offset = end_offset;

	log_entry_ptr = (ConsensusLogEntry *)(page_header_ptr +
																			entryno * sizeof(ConsensusLogEntry));
	memcpy(log_entry_ptr, log_entry, sizeof(ConsensusLogEntry));

	consensus_slru_push_dirty(ConsensusFixedLengthLogCtl, slotno,
							entryno * sizeof(ConsensusLogEntry), false);

	if (flush || mock_start_index == log_index)
	{
		ok = ConsensusSimpleLruWritePage(ConsensusFixedLengthLogCtl, 
							slotno, (mock_start_index == log_index));
		if (!ok)
		{
			easy_error_log("could not write block %llu of consensus log \"%s\"", pageno,
											ConsensusFixedLengthLogCtl->Dir);
			abort();
		}
	}

	pthread_rwlock_unlock(&ConsensusLogCtl->fixed_length_log_lock);

	return ok;
}

static bool consensus_log_append_variable_length_log(
		ConsensusLogPayload *log_payload, uint64 log_index, uint64 mock_start_index,
		uint64 *offset, uint32 *length)
{
	uint64	pageno;
	int			slotno;
	int			page_leftsize;
	int			cur_pos;
	char		*cc_log_entry, *log_index_ptr;
	pg_crc32c	crc;
	bool		ok;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, (char *)log_payload->variable_value.buffer,
			log_payload->variable_value.buffer_size);

	*length = log_payload->variable_value.buffer_size + sizeof(crc);

	pthread_rwlock_wrlock(&ConsensusLogCtl->variable_length_log_lock);

	*offset = ConsensusLogCtl->variable_length_log_next_offset;
	pageno = VariableLengthLogOffsetToPage(*offset);
	cur_pos = VariableLengthLogOffsetToPos(*offset);

	page_leftsize = CONSENSUS_CC_LOG_PAGE_SIZE - cur_pos - sizeof(log_index);
	/* not enough free space on current page */
	if (page_leftsize < *length)
	{
		pageno += 1;
		cur_pos = 0;
		*offset = pageno * CONSENSUS_CC_LOG_PAGE_SIZE;
		page_leftsize = CONSENSUS_CC_LOG_PAGE_SIZE - sizeof(log_index);
	}
	Assert(*length <= page_leftsize);

	if (cur_pos == 0 || log_index == mock_start_index)
	{
		ConsensusSimpleLruZeroPage(ConsensusVariableLengthLogCtl, pageno);
		easy_info_log("Consensus variable length log extend new page %llu", pageno);
	}

	easy_info_log("Consensus log append variable length entry, log index: %llu, offset: %d",
			log_index, *offset);

	slotno = ConsensusSimpleLruReadPage(ConsensusVariableLengthLogCtl, pageno, true);
	if (slotno == -1)
	{
		pthread_rwlock_unlock(&ConsensusLogCtl->variable_length_log_lock);
		easy_error_log("could not read block %llu of consensus log \"%s\"", pageno,
				ConsensusVariableLengthLogCtl->Dir);
		return false;
	}

	cc_log_entry = ConsensusVariableLengthLogCtl->shared->page_buffer[slotno] + cur_pos;
	/* last log index ptr at the page trailer */
	log_index_ptr = ConsensusVariableLengthLogCtl->shared->page_buffer[slotno] +
															ConsensusVariableLengthLogCtl->szblock - sizeof(log_index);

	memcpy(cc_log_entry, log_payload->variable_value.buffer, log_payload->variable_value.buffer_size);
	memcpy(cc_log_entry + log_payload->variable_value.buffer_size, &crc, sizeof(crc));
	memcpy(log_index_ptr, &log_index, sizeof(log_index));

	consensus_slru_push_dirty(ConsensusVariableLengthLogCtl, slotno, cur_pos, false);

	ok = ConsensusSimpleLruWritePage(ConsensusVariableLengthLogCtl,
							slotno, (mock_start_index == log_index));
	if (!ok)
	{
		easy_error_log("could not write block %llu of consensus log \"%s\"",
				pageno, ConsensusVariableLengthLogCtl->Dir);
		abort();
	}

	ConsensusLogCtl->variable_length_log_next_offset = *offset + *length;

	pthread_rwlock_unlock(&ConsensusLogCtl->variable_length_log_lock);

	return ok;
}

/*
 * This func must be called by consensus append thread.
 */
bool ConsensusLOGAppend(ConsensusLogEntryHeader *log_entry_header,
								ConsensusLogPayload *log_payload, uint64* consensus_index,
								bool flush, bool check_lsn)
{
	uint64	log_index;
	uint64	mock_start_index;
	uint64	last_term;
	TimeLineID last_timeline;
	TimeLineID current_timeline;
	XLogRecPtr	last_lsn;
	XLogRecPtr 	current_lsn;
	ConsensusLogEntry log_entry;
	bool 		ok = true;
	bool 		fixed_length;
	uint64 		xlog_term = ConsensusGetXLogTerm();
	uint64 		offset;
	uint64 		end_offset;

	fixed_length = CONSENSUS_ENTRY_IS_FIXED(log_entry_header->op_type);

	pthread_rwlock_wrlock(&ConsensusLogCtl->info_lock);
	log_index = ConsensusLogCtl->current_index;
	last_term = ConsensusLogCtl->last_append_term;
	current_lsn = last_lsn = ConsensusLogCtl->last_write_lsn;
	current_timeline = last_timeline = ConsensusLogCtl->last_write_timeline;
	mock_start_index = ConsensusLogCtl->mock_start_index;

	Assert(log_entry_header->log_term > 0);

	if (log_entry_header->log_term > ConsensusLogCtl->next_append_term)
	{
		ConsensusLogCtl->next_append_term = log_entry_header->log_term;
	}
	else if (log_entry_header->log_term < ConsensusLogCtl->next_append_term)
	{
		/* For logger, if it degrade immediately after became leader and 
		 * new leader's log is newer than it, the log term will reverse*/
		easy_warn_log("Consensus log term reversed, current append term: %llu, "
				"log term: %llu", ConsensusLogCtl->next_append_term, 
				log_entry_header->log_term);
		ConsensusLogCtl->next_append_term = log_entry_header->log_term;
	}

	if (log_entry_header->op_type == ConsensusEntryNormalType &&
		xlog_term != log_entry_header->log_term)
	{
		pthread_rwlock_unlock(&ConsensusLogCtl->info_lock);
#ifndef PG_HAVE_ATOMIC_U64_SIMULATION
		pg_atomic_fetch_add_u64(&ConsensusLogCtl->stats.xlog_transit_waits, 1);
#endif
		easy_warn_log("Consensus log term check failed, current xlog term: %llu, "
				"log term: %llu", xlog_term, log_entry_header->log_term);
		return false;
	}

	if (log_entry_header->log_index != 0 && log_entry_header->log_index != log_index)
	{
		pthread_rwlock_unlock(&ConsensusLogCtl->info_lock);
		easy_fatal_log("Consensus log index check failed, current log index: %llu, "
				"entry log index: %llu", log_index, log_entry_header->log_index);
		return false;
	}

	if (fixed_length && log_payload->fixed_value.log_lsn > 0)
	{
		current_lsn = log_payload->fixed_value.log_lsn;
		current_timeline = log_payload->fixed_value.log_timeline;
	}

	if (check_lsn)
	{
		if (current_lsn != InvalidXLogRecPtr &&
				!consensus_check_physical_flush_lsn(log_entry_header->log_term,
					current_lsn, current_timeline, true))
		{
			pthread_rwlock_unlock(&ConsensusLogCtl->info_lock);
			easy_warn_log("Timeout for wait WAL flush at %X/%X, current term: %llu",
								(uint32) (current_lsn >> 32), (uint32) current_lsn,
								log_entry_header->log_term);
			return false;
		}
	}

#ifndef PG_HAVE_ATOMIC_U64_SIMULATION
	pg_atomic_fetch_add_u64(&ConsensusLogCtl->stats.log_appends, 1);
#endif

	log_entry_header->log_index = log_index;
	if (!fixed_length)
	{
		uint32 length;

#ifndef PG_HAVE_ATOMIC_U64_SIMULATION
		pg_atomic_fetch_add_u64(&ConsensusLogCtl->stats.variable_log_appends, 1);
#endif

		if (!consensus_log_append_variable_length_log(log_payload, log_index, mock_start_index,
					&offset, &length))
		{
			pthread_rwlock_unlock(&ConsensusLogCtl->info_lock);
			return false;
		}

		log_entry.variable_offset = offset;
		log_entry.variable_length = length;
		end_offset = offset + length;
	}
	else
	{
		offset = end_offset = consensus_log_get_variable_length_offset();
		log_entry.variable_offset = 0;
		log_entry.variable_length = 0;
	}

	if (!fixed_length || log_payload->fixed_value.log_lsn == 0)
	{
		log_entry.log_lsn = current_lsn;
		log_entry.log_timeline = current_timeline;
	}
	else
	{
		log_entry.log_lsn = log_payload->fixed_value.log_lsn;
		log_entry.log_timeline = log_payload->fixed_value.log_timeline;
	}

	log_entry.header = *log_entry_header;
	if (!consensus_log_append_fixed_length_log(&log_entry, mock_start_index,
				last_term, last_timeline, last_lsn, offset, end_offset, flush))
	{
		pthread_rwlock_unlock(&ConsensusLogCtl->info_lock);
		return false;
	}

	*consensus_index = log_index;
	ConsensusLogCtl->current_index++;
	if (flush)
		ConsensusLogCtl->sync_index++;
	if (fixed_length && log_entry.log_lsn > 0)
	{
		ConsensusLogCtl->last_write_lsn = current_lsn;
		ConsensusLogCtl->last_write_timeline = current_timeline;
	}
	ConsensusLogCtl->last_append_term = log_entry_header->log_term;

	easy_info_log("consensus log append success, term: %llu, log index: %llu, "
			"log type: %d, lsn: %X/%X, timeline: %u", log_entry_header->log_term,
			log_index, log_entry_header->op_type,
			(uint32) (log_entry.log_lsn >> 32), (uint32) log_entry.log_lsn,
			log_entry.log_timeline);

	pthread_rwlock_unlock(&ConsensusLogCtl->info_lock);

	return ok;
}

bool ConsensusLOGFlush(uint64 pageno)
{
	bool 	ok;
	uint64 	sync_index;
#ifdef USE_ASSERT_CHECKING
	XLogRecPtr	last_lsn;
	TimeLineID 	last_timeline;
#endif

	pthread_rwlock_rdlock(&ConsensusLogCtl->info_lock);

#ifdef USE_ASSERT_CHECKING
	last_lsn = ConsensusLogCtl->last_write_lsn;
	last_timeline = ConsensusLogCtl->last_write_timeline;
#endif

	Assert(last_lsn == InvalidXLogRecPtr ||
			consensus_check_physical_flush_lsn(0, last_lsn, last_timeline, false));

	pthread_rwlock_wrlock(&ConsensusLogCtl->fixed_length_log_lock);
#ifndef PG_HAVE_ATOMIC_U64_SIMULATION
	pg_atomic_fetch_add_u64(&ConsensusLogCtl->stats.log_flushes, 1);
#endif
	ok = ConsensusSimpleLruFlush(ConsensusFixedLengthLogCtl, pageno);
	pthread_rwlock_unlock(&ConsensusLogCtl->fixed_length_log_lock);

	sync_index = ConsensusLogCtl->sync_index = ConsensusLogCtl->current_index - 1;

	pthread_rwlock_unlock(&ConsensusLogCtl->info_lock);

	easy_info_log("Consensus log flush %s, synced log index: %llu",
			ok ? "sucess" : "failed", sync_index);

	return ok;
}

/*
 * Note: for variable length log entry, caller must free the buffer manually
 */
bool ConsensusLOGRead(uint64_t log_index,
											ConsensusLogEntryHeader *log_entry_header,
											ConsensusLogPayload *log_payload,
											bool fixed_payload)
{
	uint64	pageno;
	int			entryno;
	int			slotno;
	ConsensusLogEntry *log_entry;
	pg_crc32c	crc;

	pthread_rwlock_rdlock(&ConsensusLogCtl->info_lock);
	if (log_index == 0 || log_index >= ConsensusLogCtl->current_index || 
		log_index < ConsensusLogCtl->mock_start_index)
	{
		pthread_rwlock_unlock(&ConsensusLogCtl->info_lock);
		return false;
	}
	pthread_rwlock_unlock(&ConsensusLogCtl->info_lock);

#ifndef PG_HAVE_ATOMIC_U64_SIMULATION
	pg_atomic_fetch_add_u64(&ConsensusLogCtl->stats.log_reads, 1);
#endif

	pageno = FixedLengthLogIndexToPage(log_index);
	entryno = FixedLengthLogIndexToEntry(log_index);

	slotno = ConsensusSimpleLruReadPage_ReadOnly(ConsensusFixedLengthLogCtl, pageno);
	if (slotno == -1)
	{
		pthread_rwlock_unlock(&ConsensusLogCtl->fixed_length_log_lock);
		easy_warn_log("Could not read block %llu of consensus log \"%s\" for log index: %llu", pageno,
									 ConsensusFixedLengthLogCtl->Dir, log_index);
		return false;
	}

	log_entry = (ConsensusLogEntry *)(ConsensusFixedLengthLogCtl->shared->page_buffer[slotno] +
									entryno * sizeof(ConsensusLogEntry));
	memcpy(log_entry_header, &log_entry->header, sizeof(ConsensusLogEntryHeader));

	if (fixed_payload || CONSENSUS_ENTRY_IS_FIXED(log_entry_header->op_type))
	{
		log_payload->fixed_value.log_lsn = log_entry->log_lsn;
		log_payload->fixed_value.log_timeline = log_entry->log_timeline;
	}

	pthread_rwlock_unlock(&ConsensusLogCtl->fixed_length_log_lock);

	easy_info_log("Consensus log read fixed log entry success, term: %llu, log index: %llu, "
			"log type: %d, lsn: %X/%X, timeline: %u", log_entry_header->log_term, log_index,
			log_entry_header->op_type,
			(uint32) (log_payload->fixed_value.log_lsn >> 32),
			(uint32) log_payload->fixed_value.log_lsn,
			log_payload->fixed_value.log_timeline);

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, ((char *) log_entry), offsetof(ConsensusLogEntry, log_crc));
	if (log_entry->log_crc != crc)
	{
			easy_fatal_log("Incorrect checksum in log index %llu of consensus log \"%s\"",
										 log_index, ConsensusFixedLengthLogCtl->Dir);
			Assert(false);
			return false;
	}

	if (!fixed_payload && !CONSENSUS_ENTRY_IS_FIXED(log_entry_header->op_type))
	{
		char		*page_ptr, *cc_log_entry;
		uint64 	offset;
		uint32	length;
		int			cur_pos;
		pg_crc32c	log_crc;
		char 		*cc_log_buffer;

		cc_log_buffer = malloc(log_entry->variable_length - sizeof(crc));
		if (cc_log_buffer == NULL)
		{
			easy_error_log("Out of memory while read log entry %llu of consensus log \"%s\"",
										 log_index, ConsensusVariableLengthLogCtl->Dir);
			return false;
		}

#ifndef PG_HAVE_ATOMIC_U64_SIMULATION
		pg_atomic_fetch_add_u64(&ConsensusLogCtl->stats.variable_log_reads, 1);
#endif

		offset = log_entry->variable_offset;
		length = log_entry->variable_length;
		pageno = VariableLengthLogOffsetToPage(offset);
		cur_pos = VariableLengthLogOffsetToPos(offset);
		Assert(length >= sizeof(log_crc) && length <
					CONSENSUS_CC_LOG_PAGE_SIZE - cur_pos - sizeof(log_index));

		slotno =  ConsensusSimpleLruReadPage_ReadOnly(ConsensusVariableLengthLogCtl, pageno);
		if (slotno == -1)
		{
			pthread_rwlock_unlock(&ConsensusLogCtl->variable_length_log_lock);
			easy_warn_log("Could not read block %llu of consensus log \"%s\"",
							pageno, ConsensusVariableLengthLogCtl->Dir);
			free(cc_log_buffer);
			return false;
		}

		page_ptr = ConsensusVariableLengthLogCtl->shared->page_buffer[slotno];
		cc_log_entry = page_ptr + cur_pos;

		log_payload->variable_value.buffer_size = length - sizeof(log_crc);
		log_payload->variable_value.buffer = cc_log_buffer;
		memcpy(log_payload->variable_value.buffer, cc_log_entry, length - sizeof(log_crc));
		memcpy(&log_crc, cc_log_entry + length - sizeof(log_crc), sizeof(log_crc));

		pthread_rwlock_unlock(&ConsensusLogCtl->variable_length_log_lock);

		easy_info_log("Consensus log read variable log entry success, term: %llu, log index: %llu, "
				"log type: %d, length: %d", log_entry_header->log_term, log_index,
				log_entry_header->op_type, log_payload->variable_value.buffer_size);

		INIT_CRC32C(crc);
		COMP_CRC32C(crc, (char *)log_payload->variable_value.buffer,
								log_payload->variable_value.buffer_size);
		if (log_crc != crc)
		{
			easy_error_log("Incorrect checksum in log index %llu of consensus log \"%s\"",
							log_index, ConsensusVariableLengthLogCtl->Dir);
			free(cc_log_buffer);
			return false;
		}
	}

	easy_info_log("Consensus log read success, term: %llu, log index: %llu, log type: %d",
			log_entry_header->log_term, log_index, log_entry_header->op_type);

	return true;
}

bool
consensus_log_get_entry_lsn(uint64 log_index, XLogRecPtr *lsn, TimeLineID *tli)
{
	ConsensusLogEntryHeader log_entry_header;
	ConsensusLogPayload log_entry_payload;
	if (log_index ==0 ||
			!ConsensusLOGRead(log_index, &log_entry_header, &log_entry_payload, true))
	{
		*lsn = InvalidXLogRecPtr;
		*tli = 0;
		return false;
	}

	*lsn = log_entry_payload.fixed_value.log_lsn;
	*tli = log_entry_payload.fixed_value.log_timeline;

	return true;
}

static uint64
consensus_log_get_variable_length_offset(void)
{
	uint64 offset;

	pthread_rwlock_rdlock(&ConsensusLogCtl->variable_length_log_lock);
	offset = ConsensusLogCtl->variable_length_log_next_offset;
	pthread_rwlock_unlock(&ConsensusLogCtl->variable_length_log_lock);

	return offset;
}

/*
 * fixed_length_log_lock must be held at entry, and will be held at exit.
 */
static bool
consensus_log_get_page_start_offset(uint64 pageno, uint64 *start_offset)
{
	int			slotno;
	char		*page_header_ptr;
	FixedLengthLogPageTrailer *page_trailer;

	slotno = ConsensusSimpleLruReadPage(ConsensusFixedLengthLogCtl, pageno, false);
	if (slotno == -1)
	{
		easy_error_log("could not read block %llu of consensus log \"%s\"", pageno,
									 ConsensusFixedLengthLogCtl->Dir);
		return false;
	}

	page_header_ptr = ConsensusFixedLengthLogCtl->shared->page_buffer[slotno];
	page_trailer = (FixedLengthLogPageTrailer *)(page_header_ptr +
									CONSENSUS_LOG_PAGE_SIZE - sizeof(FixedLengthLogPageTrailer));

	*start_offset = page_trailer->start_offset;

	return true;
}

static bool
ConsensusLOGGetPageLSN(int slotno, XLogRecPtr *start_lsn, XLogRecPtr *end_lsn)
{
	char		*page_header_ptr;
	FixedLengthLogPageTrailer *page_trailer;

	page_header_ptr = ConsensusFixedLengthLogCtl->shared->page_buffer[slotno];
	page_trailer = (FixedLengthLogPageTrailer *)(page_header_ptr +
									CONSENSUS_LOG_PAGE_SIZE - sizeof(FixedLengthLogPageTrailer));

	if (page_trailer->end_entry == 0)
	{
		return false;
	}

	if (start_lsn)
		*start_lsn = page_trailer->start_lsn;

	if (end_lsn)
	{
		ConsensusLogEntry *log_entry = (ConsensusLogEntry *) (page_header_ptr +
						(page_trailer->end_entry - 1) * sizeof(ConsensusLogEntry));
		*end_lsn = log_entry->log_lsn;
	}

	return true;
}

static bool
consensus_log_try_flush_fixed_length_page(int slotno)
{
	XLogRecPtr start_lsn, end_lsn;
	bool ok;

	if (!ConsensusLOGGetPageLSN(slotno, &start_lsn, &end_lsn))
		return true;

#ifndef PG_HAVE_ATOMIC_U64_SIMULATION
	pg_atomic_fetch_add_u64(&ConsensusLogCtl->stats.xlog_flush_tries, 1);
#endif

	ok = (end_lsn == InvalidXLogRecPtr) || consensus_check_physical_flush_lsn(0, end_lsn, 0, false);

#ifndef PG_HAVE_ATOMIC_U64_SIMULATION
	if (!ok)
		pg_atomic_fetch_add_u64(&ConsensusLogCtl->stats.xlog_flush_failed_tries, 1);
#endif

	return ok;
}

/*
 * fixed_length_log_lock must be held at entry, and will be held at exit.
 */
static bool
consensus_log_get_page_lsn(uint64 pageno, XLogRecPtr *start_lsn, XLogRecPtr *end_lsn)
{
	int			slotno;
	char		*page_header_ptr;
	FixedLengthLogPageTrailer *page_trailer;

	slotno = ConsensusSimpleLruReadPage(ConsensusFixedLengthLogCtl, pageno, false);
	if (slotno == -1)
	{
		easy_error_log("could not read block %llu of consensus log \"%s\"", pageno,
									 ConsensusFixedLengthLogCtl->Dir);
		return false;
	}

	page_header_ptr = ConsensusFixedLengthLogCtl->shared->page_buffer[slotno];
	page_trailer = (FixedLengthLogPageTrailer *)(page_header_ptr +
									CONSENSUS_LOG_PAGE_SIZE - sizeof(FixedLengthLogPageTrailer));

	if (start_lsn)
		*start_lsn = page_trailer->start_lsn;

	if (end_lsn)
	{
		if (page_trailer->end_entry == 0)
		{
			*end_lsn = page_trailer->start_lsn;
		}
		else
		{
			ConsensusLogEntry *log_entry = (ConsensusLogEntry *) (page_header_ptr +
					(page_trailer->end_entry - 1) * sizeof(ConsensusLogEntry));
			*end_lsn = log_entry->log_lsn;
		}
	}

	return true;
}

void ConsensusLOGTruncateForward(uint64 log_index, bool force)
{
	uint64	cutoffPage;
	uint64 	start_offset;
	uint32 	log_size;
	uint32 	log_keep_size;
	XLogRecPtr start_lsn;
	uint64	commit_index;

	if (log_index == 0)
	{
		easy_warn_log("Consensus log truncate forward, invalid log index: %llu", log_index);
		return;
	}

	commit_index = ConsensusGetCommitIndex();
	if (log_index > commit_index)
	{
		easy_warn_log("Consensus log truncate forward, purge log index(%ld) is "
				"more than commit index(%ld)", log_index, commit_index);

		if (commit_index > 0)
			log_index = commit_index - 1;
		else
			return;
	}

	log_keep_size = ConsensusLOGGetKeepSize();
	log_size = ConsensusLOGGetLength() / FIXED_LENGTH_LOGS_PER_PAGE * 
		CONSENSUS_LOG_PAGE_SIZE / (1024 * 1024);
	if (log_keep_size > 0 && log_size <= log_keep_size)
	{
		easy_info_log("Consensus log truncate forward ignore, current log size(MB): %d, "
				"log keep size(MB): %d", log_size, log_keep_size);
		return;
	}

	if (log_keep_size > 0)
	{
		uint64 log_keep_index = ConsensusLOGGetLastIndex() - 
			log_keep_size * 1024 * 1024 / CONSENSUS_LOG_PAGE_SIZE * FIXED_LENGTH_LOGS_PER_PAGE;
		log_index = log_index > log_keep_index ? log_keep_index : log_index;
	}

	if (!force)
		ConsensusMetaSetInt64(PurgeIndexMetaKey, log_index, false);
	ConsensusMetaSetInt64(CommitIndexMetaKey, commit_index, false);
	ConsensusMetaFlush();

	pthread_rwlock_rdlock(&ConsensusLogCtl->info_lock);

	/*
	 * The cutoff point is the start of the segment containing log_index. We
	 * pass the *page* containing log_index to ConsensusSimpleLruTruncate.
	 */
	cutoffPage = FixedLengthLogIndexToPage(log_index);

	easy_info_log("Consensus log truncate forward, log index: %llu, cutoffPage: %llu", log_index, cutoffPage);

	/* Check to see if there's any files that could be removed */
	if (!ConsensusSlruScanDirectory(ConsensusFixedLengthLogCtl,
															ConsensusSlruScanDirCbReportPresenceForward, &cutoffPage))
	{
		pthread_rwlock_unlock(&ConsensusLogCtl->info_lock);
		return;					/* nothing to remove */
	}

	/*
	 * Scan shared memory and remove any pages preceding the cutoff page, to
	 * ensure we won't rewrite them later.
	 */
	pthread_rwlock_wrlock(&ConsensusLogCtl->fixed_length_log_lock);

	if (!consensus_log_get_page_lsn(cutoffPage, &start_lsn, NULL))
	{
		pthread_rwlock_unlock(&ConsensusLogCtl->fixed_length_log_lock);
		pthread_rwlock_unlock(&ConsensusLogCtl->info_lock);
		easy_error_log("Consensus get truncate cutoff page start offset failed, "
				"log_index: %llu. ", log_index);
		return;
	}
	else if (!consensus_log_get_page_start_offset(cutoffPage, &start_offset))
	{
		pthread_rwlock_unlock(&ConsensusLogCtl->fixed_length_log_lock);
		pthread_rwlock_unlock(&ConsensusLogCtl->info_lock);
		easy_error_log("Consensus get truncate cutoff page lsn failed, "
				"log_index: %llu. ", log_index);
		return;
	}

	ConsensusSimpleLruTruncateForward(ConsensusFixedLengthLogCtl, cutoffPage);

	ConsensusSetPurgeLSN(start_lsn);

	pthread_rwlock_unlock(&ConsensusLogCtl->fixed_length_log_lock);
	pthread_rwlock_unlock(&ConsensusLogCtl->info_lock);

		/* Now we can remove the old segment(s) */
	(void) ConsensusSlruScanDirectory(ConsensusFixedLengthLogCtl,
			consensus_slru_scan_dir_callback_delete_cutoff_forward, &cutoffPage);

	cutoffPage = VariableLengthLogOffsetToPage(start_offset);

	/* Check to see if there's any files that could be removed */
	if (!ConsensusSlruScanDirectory(ConsensusVariableLengthLogCtl,
															ConsensusSlruScanDirCbReportPresenceForward, &cutoffPage))
		return;					/* nothing to remove */

	pthread_rwlock_wrlock(&ConsensusLogCtl->variable_length_log_lock);

	ConsensusSimpleLruTruncateForward(ConsensusVariableLengthLogCtl, cutoffPage);

	pthread_rwlock_unlock(&ConsensusLogCtl->variable_length_log_lock);

	/* Now we can remove the old segment(s) */
	(void) ConsensusSlruScanDirectory(ConsensusVariableLengthLogCtl,
			consensus_slru_scan_dir_callback_delete_cutoff_forward, &cutoffPage);

	return;
}

bool ConsensusLOGTruncateBackward(uint64 log_index)
{
	uint64		cutoffPage;
	int			cutoffEntry;
	int			slotno;
	char		*page_header_ptr;
	FixedLengthLogPageTrailer *page_trailer;

	if (log_index == 0)
	{
		easy_warn_log("Consensus log truncate backward, invalid log index: %llu", log_index);
		return false;
	}

	easy_info_log("Consensus log truncate backward, log index: %llu", log_index);

	pthread_rwlock_wrlock(&ConsensusLogCtl->info_lock);
	if (ConsensusLogCtl->disable_purge)
	{
		pthread_rwlock_unlock(&ConsensusLogCtl->info_lock);
		easy_warn_log("Consensus log truncate backward disabled, log_index: %llu.",
				log_index);
		return false;
	}

	ConsensusMetaFlush();

	cutoffPage = FixedLengthLogIndexToPage(log_index);
	cutoffEntry = FixedLengthLogIndexToEntry(log_index);

	pthread_rwlock_wrlock(&ConsensusLogCtl->fixed_length_log_lock);

	if (!ConsensusSimpleLruFlush(ConsensusFixedLengthLogCtl, 0))
	{
		pthread_rwlock_unlock(&ConsensusLogCtl->fixed_length_log_lock);
		pthread_rwlock_unlock(&ConsensusLogCtl->info_lock);
		easy_error_log("Consensus log truncate backward flush failed, log_index: %llu.",
				log_index);
		return false;
	}

	slotno = ConsensusSimpleLruReadPage(ConsensusFixedLengthLogCtl, cutoffPage, true);
	if (slotno == -1)
	{
		pthread_rwlock_unlock(&ConsensusLogCtl->fixed_length_log_lock);
		pthread_rwlock_unlock(&ConsensusLogCtl->info_lock);
		easy_error_log("could not read block %llu of consensus log \"%s\"", cutoffPage,
									 ConsensusFixedLengthLogCtl->Dir);
		return false;
	}

	/* empty the page after cutoffPage */
	ConsensusSimpleLruTruncateBackward(ConsensusFixedLengthLogCtl, cutoffPage);

	page_header_ptr = ConsensusFixedLengthLogCtl->shared->page_buffer[slotno];
	page_trailer = (FixedLengthLogPageTrailer *)(page_header_ptr +
									CONSENSUS_LOG_PAGE_SIZE - sizeof(FixedLengthLogPageTrailer));

	Assert(page_trailer->end_entry > cutoffEntry);
	page_trailer->end_entry = cutoffEntry;

	consensus_slru_push_dirty(ConsensusFixedLengthLogCtl, slotno,
							cutoffEntry * sizeof(ConsensusLogEntry), false);

	ConsensusFixedLengthLogCtl->shared->latest_page_number = cutoffPage;

	if (cutoffEntry == 0)
	{
		ConsensusLogCtl->last_append_term = page_trailer->last_term;
		ConsensusLogCtl->last_write_lsn = page_trailer->start_lsn;
		ConsensusLogCtl->last_write_timeline = page_trailer->start_timeline;
	}
	else
	{
		ConsensusLogEntry *log_entry = (ConsensusLogEntry *)
			(page_header_ptr + (cutoffEntry - 1) * sizeof(ConsensusLogEntry));
		ConsensusLogCtl->last_append_term = log_entry->header.log_term;
		ConsensusLogCtl->last_write_lsn = log_entry->log_lsn;
		ConsensusLogCtl->last_write_timeline = log_entry->log_timeline;
	}
	ConsensusLogCtl->current_index = log_index;
	ConsensusLogCtl->sync_index = log_index - 1;

	(void) ConsensusSimpleLruWritePage(ConsensusFixedLengthLogCtl, slotno, false);

	pthread_rwlock_unlock(&ConsensusLogCtl->fixed_length_log_lock);
	pthread_rwlock_unlock(&ConsensusLogCtl->info_lock);

	/* Now we can remove the new segment(s) */
	(void) ConsensusSlruScanDirectory(ConsensusFixedLengthLogCtl,
			consensus_slru_scan_dir_callback_delete_cutoff_backward, &cutoffPage);

	return true;
}


void
ConsensusLOGSetDisablePurge(bool disable)
{
	pthread_rwlock_wrlock(&ConsensusLogCtl->info_lock);
	ConsensusLogCtl->disable_purge = disable;
	pthread_rwlock_unlock(&ConsensusLogCtl->info_lock);
}

uint64
ConsensusLOGGetLastIndex(void)
{
	uint64 sync_index;
	pthread_rwlock_rdlock(&ConsensusLogCtl->info_lock);
	sync_index = ConsensusLogCtl->sync_index;
	pthread_rwlock_unlock(&ConsensusLogCtl->info_lock);
	return sync_index;
}

inline static void
consensus_set_member_info_array(char *info_array, char *info_str, int info_size)
{
	int size = info_size > MEMBER_INFO_MAX_LENGTH ? MEMBER_INFO_MAX_LENGTH : info_size;
	memcpy(info_array, info_str, size-1);
	info_array[size-1] = '\0';
}

#if 0
static void
ConsensusLOGLockTermShared(void)
{
	pthread_rwlock_rdlock(&ConsensusLogCtl->term_lock);
}

static void
ConsensusLOGUnlockTerm(void)
{
	pthread_rwlock_unlock(&ConsensusLogCtl->term_lock);
}
#endif

bool
ConsensusMetaStartup(void)
{
	char		path[MAXPGPATH];
	int			fd;
	struct stat stat_buf;
	pg_crc32c		crc;

	consensus_meta_dir("polar_dma", path);

	if (ConsensusMetaCtl->vfs_api->vfs_stat(path, &stat_buf) == 0)
	{
		if (!S_ISDIR(stat_buf.st_mode))
		{
			easy_error_log("required polar dma directory \"%s\" is not a directory", path);
			return false;
		}
	}
	else
	{
		easy_warn_log("creating missing polar dma directory \"%s\"", path);
		if (ConsensusMetaCtl->vfs_api->vfs_mkdir(path, pg_dir_create_mode) != 0)
		{
			easy_fatal_log("Could not create directory \"%s\": %s.", path, strerror(errno));
			return false;
		}
	}

	consensus_meta_file("polar_dma", path);

	pthread_rwlock_wrlock(&ConsensusMetaCtl->lock);

	fd = ConsensusMetaCtl->vfs_api->vfs_open(path, O_RDWR | PG_BINARY,
																					 pg_file_create_mode);
	if (fd < 0)
	{
		if (errno != ENOENT)
		{
			pthread_rwlock_unlock(&ConsensusMetaCtl->lock);
			easy_fatal_log("Could not open consensus meta file \"%s\": %s.", path, strerror(errno));
			return false;
		}
		else
		{
			memset(&ConsensusMetaCtl->header, 0, sizeof(ConsensusMetaCtl->header));
			memset(&ConsensusMetaCtl->member_info_array, 0, MEMBER_INFO_MAX_LENGTH);
			memset(&ConsensusMetaCtl->learner_info_array, 0, MEMBER_INFO_MAX_LENGTH);
			ConsensusMetaCtl->header.version = DMA_META_VERSION;
			ConsensusMetaCtl->member_info_str = NULL;
			ConsensusMetaCtl->learner_info_str = NULL;
			pthread_rwlock_unlock(&ConsensusMetaCtl->lock);
			easy_info_log("Consensus meta file \"%s\" doesn't exist, reading as zeroes", path);
			return true;
		}
	}

	if (ConsensusMetaCtl->vfs_api->vfs_read(fd, &ConsensusMetaCtl->header,
				sizeof(ConsensusMetaCtl->header)) != sizeof(ConsensusMetaCtl->header))
	{
		ConsensusMetaCtl->vfs_api->vfs_close(fd);
		pthread_rwlock_unlock(&ConsensusMetaCtl->lock);
		easy_fatal_log("Could not read from consensus meta file \"%s\" at offset 0: %s.",
									 path, strerror(errno));
		return false;
	}

	if (ConsensusMetaCtl->header.version != DMA_META_VERSION)
	{
		ConsensusMetaCtl->vfs_api->vfs_close(fd);
		pthread_rwlock_unlock(&ConsensusMetaCtl->lock);
		easy_fatal_log("meta version unmatched \"%s\" .",
									 path, strerror(errno));
		return false;
	}

	if (ConsensusMetaCtl->header.member_info_size > 0)
	{
		int member_info_size = ConsensusMetaCtl->header.member_info_size;
		char *member_info = malloc(member_info_size);
		if (!member_info)
		{
			ConsensusMetaCtl->vfs_api->vfs_close(fd);
			pthread_rwlock_unlock(&ConsensusMetaCtl->lock);
			easy_fatal_log("out of memory while consensus meta file load.");
			return false;
		}

		if (ConsensusMetaCtl->vfs_api->vfs_read(fd, member_info, member_info_size) !=
									member_info_size)
		{
			ConsensusMetaCtl->vfs_api->vfs_close(fd);
			pthread_rwlock_unlock(&ConsensusMetaCtl->lock);
			easy_fatal_log("Could not read from consensus meta file \"%s\" at offset %u: %s.",
					path, sizeof(ConsensusMetaCtl->header), strerror(errno));
			return false;
		}
		Assert(ConsensusMetaCtl->member_info_str == NULL);
		ConsensusMetaCtl->member_info_str = member_info;
		consensus_set_member_info_array(ConsensusMetaCtl->member_info_array,
				member_info, member_info_size);
	}

	if (ConsensusMetaCtl->header.learner_info_size > 0)
	{
		int learner_info_size = ConsensusMetaCtl->header.learner_info_size;
		char *learner_info = malloc(learner_info_size);
		if (!learner_info)
		{
			ConsensusMetaCtl->vfs_api->vfs_close(fd);
			pthread_rwlock_unlock(&ConsensusMetaCtl->lock);
			easy_fatal_log("out of memory while consensus meta file load.");
			return false;
		}

		if (ConsensusMetaCtl->vfs_api->vfs_read(fd, learner_info, learner_info_size) !=
									learner_info_size)
		{
			ConsensusMetaCtl->vfs_api->vfs_close(fd);
			pthread_rwlock_unlock(&ConsensusMetaCtl->lock);
			easy_fatal_log("Could not read from consensus meta file \"%s\" at offset %u: %s.",
					path, sizeof(ConsensusMetaCtl->header) + ConsensusMetaCtl->header.member_info_size,
					strerror(errno));
			return false;
		}
		Assert(ConsensusMetaCtl->learner_info_str == NULL);
		ConsensusMetaCtl->learner_info_str = learner_info;
		consensus_set_member_info_array(ConsensusMetaCtl->learner_info_array,
				learner_info, learner_info_size);
	}


	if (ConsensusMetaCtl->vfs_api->vfs_close(fd))
	{
		pthread_rwlock_unlock(&ConsensusMetaCtl->lock);
		easy_fatal_log("Could not close consensus meta file \"%s\": %s.", path, strerror(errno));
		return false;
	}

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, ((char *) &ConsensusMetaCtl->header),
			offsetof(ConsensusMetaHeader, crc));
	if (ConsensusMetaCtl->header.member_info_size > 0)
		COMP_CRC32C(crc, ConsensusMetaCtl->member_info_str,
				ConsensusMetaCtl->header.member_info_size);
	if (ConsensusMetaCtl->header.learner_info_size > 0)
		COMP_CRC32C(crc, ConsensusMetaCtl->learner_info_str,
				ConsensusMetaCtl->header.learner_info_size);

	if (ConsensusMetaCtl->header.crc != crc)
	{
		pthread_rwlock_unlock(&ConsensusMetaCtl->lock);
		easy_error_log("incorrect checksum in consensus meta file");
		return false;
	}

	pthread_rwlock_unlock(&ConsensusMetaCtl->lock);
	return true;
}

bool
ConsensusMetaForceChange(int current_term,
							char *cluster_info,
							int cluster_info_size,
							char *learner_info,
							int learner_info_size,
							uint64 mock_start_index,
							TimeLineID mock_start_tli,
							XLogRecPtr mock_start_lsn,
							bool is_learner,
							bool reset)
{
	bool ok;

	pthread_rwlock_wrlock(&ConsensusMetaCtl->lock);

	if (cluster_info)
	{
		ConsensusMetaCtl->header.member_info_size = cluster_info_size;

		if (cluster_info_size > 0)
		{
			ConsensusMetaCtl->member_info_str = realloc(ConsensusMetaCtl->member_info_str,
					cluster_info_size);
			memcpy(ConsensusMetaCtl->member_info_str, cluster_info, cluster_info_size);
			consensus_set_member_info_array(ConsensusMetaCtl->member_info_array,
					cluster_info, cluster_info_size);
		}
		else if (ConsensusMetaCtl->member_info_str)
		{
			free(ConsensusMetaCtl->member_info_str);
			ConsensusMetaCtl->member_info_str = NULL;
		}
	}

	if (learner_info)
	{
		ConsensusMetaCtl->header.learner_info_size = learner_info_size;

		if (learner_info_size > 0)
		{
			ConsensusMetaCtl->learner_info_str = realloc(ConsensusMetaCtl->learner_info_str,
					learner_info_size);
			memcpy(ConsensusMetaCtl->learner_info_str, learner_info, learner_info_size);
			consensus_set_member_info_array(ConsensusMetaCtl->learner_info_array,
					learner_info, learner_info_size);
		}
		else if (ConsensusMetaCtl->learner_info_str)
		{
			free(ConsensusMetaCtl->learner_info_str);
			ConsensusMetaCtl->learner_info_str = NULL;
		}
	}

	if (reset)
	{
		ConsensusMetaCtl->header.vote_for = 0;
		ConsensusMetaCtl->header.last_leader_term = 0;
		ConsensusMetaCtl->header.last_leader_log_index = 0;
		ConsensusMetaCtl->header.scan_index = 0;
	}

	if (mock_start_index > 0)
		ConsensusMetaCtl->header.mock_start_index = mock_start_index;

	if (mock_start_tli > 0 && mock_start_lsn != InvalidXLogRecPtr)
	{
		ConsensusMetaCtl->header.mock_start_tli = mock_start_tli;
		ConsensusMetaCtl->header.mock_start_lsn = mock_start_lsn;
	}

	if (current_term > 0)
		ConsensusMetaCtl->header.current_term = current_term;

	if (is_learner)
		ConsensusMetaCtl->header.last_leader_term = 0;

	ok = consensus_meta_flush_internal();

	pthread_rwlock_unlock(&ConsensusMetaCtl->lock);

	return ok;
}

void
ConsensusMetaSetMemberInfo(char *member_info, int member_info_size, bool flush)
{
	pthread_rwlock_wrlock(&ConsensusMetaCtl->lock);

	ConsensusMetaCtl->header.member_info_size = member_info_size;
	if (member_info_size > 0)
	{
		Assert(*(member_info + member_info_size - 1) == '\0');
		ConsensusMetaCtl->member_info_str = realloc(ConsensusMetaCtl->member_info_str,
				member_info_size);
		memcpy(ConsensusMetaCtl->member_info_str, member_info, member_info_size);
		consensus_set_member_info_array(ConsensusMetaCtl->member_info_array,
				member_info, member_info_size);
	}
	else if (ConsensusMetaCtl->member_info_str)
	{
		free(ConsensusMetaCtl->member_info_str);
		ConsensusMetaCtl->member_info_str = NULL;
	}

	if (flush && !consensus_meta_flush_internal())
	{
		pthread_rwlock_unlock(&ConsensusMetaCtl->lock);
		easy_fatal_log("Consensus meta file write failed.");
		abort();
	}

	pthread_rwlock_unlock(&ConsensusMetaCtl->lock);
}

static int
consensus_meta_get_member_info(char **member_info, bool from_array)
{
	int member_info_size;

	pthread_rwlock_rdlock(&ConsensusMetaCtl->lock);

	member_info_size = ConsensusMetaCtl->header.member_info_size;
	if (member_info_size == 0)
	{
		*member_info = NULL;
		pthread_rwlock_unlock(&ConsensusMetaCtl->lock);
		return 0;
	}

	if (from_array && member_info_size > MEMBER_INFO_MAX_LENGTH)
	{
		member_info_size = MEMBER_INFO_MAX_LENGTH;
	}

	*member_info = malloc(member_info_size);
	if (!(*member_info))
	{
		pthread_rwlock_unlock(&ConsensusMetaCtl->lock);
		easy_fatal_log("out of memory while get consensus meta info.");
		abort();
	}

	memcpy(*member_info, from_array ? ConsensusMetaCtl->member_info_array :
			ConsensusMetaCtl->member_info_str, member_info_size);

	pthread_rwlock_unlock(&ConsensusMetaCtl->lock);

	return member_info_size;
}

int
ConsensusMetaGetMemberInfo(char **member_info)
{
	return consensus_meta_get_member_info(member_info, false);
}

int
ConsensusMetaGetMemberInfoFromArray(char **member_info)
{
	return consensus_meta_get_member_info(member_info, true);
}

void
ConsensusMetaSetLearnerInfo(char *learner_info, int learner_info_size, bool flush)
{
	pthread_rwlock_wrlock(&ConsensusMetaCtl->lock);

	ConsensusMetaCtl->header.learner_info_size = learner_info_size;
	if (learner_info_size > 0)
	{
		Assert(*(learner_info + learner_info_size - 1) == '\0');
		ConsensusMetaCtl->learner_info_str = realloc(ConsensusMetaCtl->learner_info_str,
				learner_info_size);
		memcpy(ConsensusMetaCtl->learner_info_str, learner_info, learner_info_size);
		consensus_set_member_info_array(ConsensusMetaCtl->learner_info_array,
				learner_info, learner_info_size);
	}
	else if (ConsensusMetaCtl->learner_info_str)
	{
		free(ConsensusMetaCtl->learner_info_str);
		ConsensusMetaCtl->learner_info_str = NULL;
	}

	if (flush && !consensus_meta_flush_internal())
	{
		pthread_rwlock_unlock(&ConsensusMetaCtl->lock);
		easy_fatal_log("Consensus meta file write failed.");
		abort();
	}

	pthread_rwlock_unlock(&ConsensusMetaCtl->lock);
}

static int
consensus_meta_get_learner_info(char **learner_info, bool from_array)
{
	int learner_info_size;

	pthread_rwlock_rdlock(&ConsensusMetaCtl->lock);

	learner_info_size = ConsensusMetaCtl->header.learner_info_size;
	if (learner_info_size == 0)
	{
		*learner_info = NULL;
		pthread_rwlock_unlock(&ConsensusMetaCtl->lock);
		return 0;
	}

	if (from_array && learner_info_size > MEMBER_INFO_MAX_LENGTH)
	{
		learner_info_size = MEMBER_INFO_MAX_LENGTH;
	}

	*learner_info = malloc(learner_info_size);
	if (!(*learner_info))
	{
		pthread_rwlock_unlock(&ConsensusMetaCtl->lock);
		easy_fatal_log("out of memory while get consensus meta info.");
		abort();
	}

	memcpy(*learner_info, from_array ? ConsensusMetaCtl->learner_info_array :
			ConsensusMetaCtl->learner_info_str, learner_info_size);

	pthread_rwlock_unlock(&ConsensusMetaCtl->lock);

	return learner_info_size;
}

int
ConsensusMetaGetLearnerInfo(char **learner_info)
{
	return consensus_meta_get_learner_info(learner_info, false);
}

int
ConsensusMetaGetLearnerInfoFromArray(char **learner_info)
{
	return consensus_meta_get_learner_info(learner_info, true);
}

bool
ConsensusMetaSetInt64(int key, uint64 value, bool flush)
{
	pthread_rwlock_wrlock(&ConsensusMetaCtl->lock);

	if (key == CurrentTermMetaKey)
	{
		ConsensusMetaCtl->header.current_term = value;
	}
	else if (key == VoteForMetaKey)
	{
		ConsensusMetaCtl->header.vote_for = value;
	}
	else if (key == LastLeaderTermMetaKey)
	{
		ConsensusMetaCtl->header.last_leader_term = value;
	}
	else if (key == LastLeaderIndexMetaKey)
	{
		ConsensusMetaCtl->header.last_leader_log_index = value;
	}
	else if (key == ScanIndexMetaKey)
	{
		ConsensusMetaCtl->header.scan_index = value;
	}
	else if (key == ClusterIdMetaKey)
	{
		ConsensusMetaCtl->header.cluster_id = value;
	}
	else if (key == CommitIndexMetaKey)
	{
		ConsensusMetaCtl->header.commit_index = value;
	}
	else if (key == PurgeIndexMetaKey)
	{
		ConsensusMetaCtl->header.purge_index = value;
	}
	else if (key == MockStartIndex)
	{
		ConsensusMetaCtl->header.mock_start_index = value;
	}
	else if (key == MockStartTLI)
	{
		ConsensusMetaCtl->header.mock_start_tli = value;
	}
	else if (key == MockStartLSN)
	{
		ConsensusMetaCtl->header.mock_start_lsn = value;
	}
	else
	{
		pthread_rwlock_unlock(&ConsensusMetaCtl->lock);
		easy_fatal_log("Invalid consensus meta key.");
		return false;
	}

	if (flush && !consensus_meta_flush_internal())
	{
		pthread_rwlock_unlock(&ConsensusMetaCtl->lock);
		easy_fatal_log("Consensus meta file write failed.");
		Assert(false);
		abort();
	}

	pthread_rwlock_unlock(&ConsensusMetaCtl->lock);
	return true;
}

bool
ConsensusMetaGetInt64(int key, uint64 *value)
{
	pthread_rwlock_rdlock(&ConsensusMetaCtl->lock);

	if (key == CurrentTermMetaKey)
	{
		*value = ConsensusMetaCtl->header.current_term;
	}
	else if (key == VoteForMetaKey)
	{
		*value = ConsensusMetaCtl->header.vote_for;
	}
	else if (key == LastLeaderTermMetaKey)
	{
		*value = ConsensusMetaCtl->header.last_leader_term;
	}
	else if (key == LastLeaderIndexMetaKey)
	{
		*value = ConsensusMetaCtl->header.last_leader_log_index;
	}
	else if (key == ScanIndexMetaKey)
	{
		*value = ConsensusMetaCtl->header.scan_index;
	}
	else if (key == ClusterIdMetaKey)
	{
		*value = ConsensusMetaCtl->header.cluster_id;
	}
	else if (key == CommitIndexMetaKey)
	{
		*value = ConsensusMetaCtl->header.commit_index;
	}
	else if (key == PurgeIndexMetaKey)
	{
		*value = ConsensusMetaCtl->header.purge_index;
	}
	else if (key == MockStartIndex)
	{
		*value = ConsensusMetaCtl->header.mock_start_index;
	}
	else if (key == MockStartTLI)
	{
		*value = ConsensusMetaCtl->header.mock_start_tli;
	}
	else if (key == MockStartLSN)
	{
		*value = ConsensusMetaCtl->header.mock_start_lsn;
	}
	else
	{
		pthread_rwlock_unlock(&ConsensusMetaCtl->lock);
		easy_fatal_log("invalid consensus meta key.");
		return false;
	}

	pthread_rwlock_unlock(&ConsensusMetaCtl->lock);

	return true;
}

bool
ConsensusMetaFlush(void)
{
	bool ok;

	pthread_rwlock_wrlock(&ConsensusMetaCtl->lock);
	ok = consensus_meta_flush_internal();
	pthread_rwlock_unlock(&ConsensusMetaCtl->lock);

	return ok;
}

static bool
consensus_meta_flush_internal(void)
{
	int 	meta_size;
	char 	*meta_buffer;
	char	path[MAXPGPATH];
	int		fd;
	pg_crc32c		crc;

	meta_size = sizeof(ConsensusMetaHeader) +
				ConsensusMetaCtl->header.member_info_size +
				ConsensusMetaCtl->header.learner_info_size;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, ((char *) &ConsensusMetaCtl->header),
			offsetof(ConsensusMetaHeader, crc));
	if (ConsensusMetaCtl->header.member_info_size > 0)
		COMP_CRC32C(crc, ConsensusMetaCtl->member_info_str,
				ConsensusMetaCtl->header.member_info_size);
	if (ConsensusMetaCtl->header.learner_info_size > 0)
		COMP_CRC32C(crc, ConsensusMetaCtl->learner_info_str,
				ConsensusMetaCtl->header.learner_info_size);
	ConsensusMetaCtl->header.crc = crc;

	meta_buffer = malloc(meta_size);
	if (!meta_buffer)
	{
		easy_fatal_log("out of memory while consensus meta file flush.");
		return false;
	}

	memcpy(meta_buffer, &ConsensusMetaCtl->header, sizeof(ConsensusMetaHeader));
	if (ConsensusMetaCtl->header.member_info_size > 0)
	{
		memcpy(meta_buffer + sizeof(ConsensusMetaHeader),
				ConsensusMetaCtl->member_info_str, ConsensusMetaCtl->header.member_info_size);
	}
	if (ConsensusMetaCtl->header.learner_info_size > 0)
	{
		memcpy(meta_buffer + sizeof(ConsensusMetaHeader) + ConsensusMetaCtl->header.member_info_size,
				ConsensusMetaCtl->learner_info_str, ConsensusMetaCtl->header.learner_info_size);
	}

#ifndef PG_HAVE_ATOMIC_U64_SIMULATION
	pg_atomic_fetch_add_u64(&ConsensusLogCtl->stats.meta_flushes, 1);
#endif

	consensus_meta_file("polar_dma", path);

	fd = ConsensusMetaCtl->vfs_api->vfs_open(path, O_RDWR | PG_BINARY | O_CREAT,
																					 pg_file_create_mode);
	if (fd < 0)
	{
		free(meta_buffer);
		easy_fatal_log("Could not open consensus meta file \"%s\"", path);
		return false;
	}

	if (ConsensusMetaCtl->vfs_api->vfs_write(fd, meta_buffer, meta_size) != meta_size)
	{
		if (errno == 0)
			errno = ENOSPC;
		ConsensusMetaCtl->vfs_api->vfs_close(fd);
		free(meta_buffer);
		easy_fatal_log("Could not write to consensus meta file \"%s\" : %s.",
									 path, strerror(errno));
		return false;
	}

	free(meta_buffer);

	if (ConsensusMetaCtl->vfs_api->vfs_close(fd))
	{
		easy_fatal_log("Could not close consensus meta file \"%s\": %s.", path, strerror(errno));
		return false;
	}

	return true;
}

static void
consensus_meta_dir(char *subdir, char *path)
{
	if (polar_enable_shared_storage_mode)
		snprintf(path, MAXPGPATH, "%s/%s", polar_path_remove_protocol(polar_datadir), subdir);
	else
		snprintf(path, MAXPGPATH, "%s", subdir);

	return;
}

static void
consensus_meta_file(char *subdir, char *path)
{
	if (polar_enable_shared_storage_mode)
		snprintf(path, MAXPGPATH, "%s/%s/consensus_meta", polar_path_remove_protocol(polar_datadir), subdir);
	else
		snprintf(path, MAXPGPATH, "%s/consensus_meta", subdir);
}

void
ConsensusLOGSetTerm(uint64 term)
{
	pthread_rwlock_wrlock(&ConsensusLogCtl->term_lock);
	ConsensusLogCtl->current_term = term;
	pthread_rwlock_unlock(&ConsensusLogCtl->term_lock);
}

uint64
ConsensusLOGGetTerm(void)
{
	uint64 term;

	pthread_rwlock_rdlock(&ConsensusLogCtl->term_lock);
	term = ConsensusLogCtl->current_term;
	pthread_rwlock_unlock(&ConsensusLogCtl->term_lock);

	return term;
}

uint64
ConsensusLOGGetLength(void)
{
	uint64 purge_index;

	ConsensusMetaGetInt64(PurgeIndexMetaKey, &purge_index);

	return ConsensusLOGGetLastIndex() - purge_index;
}

void
ConsensusLOGSetLogTerm(uint64 term)
{
	pthread_rwlock_wrlock(&ConsensusLogCtl->info_lock);
	ConsensusLogCtl->next_append_term = term;
	pthread_rwlock_unlock(&ConsensusLogCtl->info_lock);
}

uint64
ConsensusLOGGetLogTerm(void)
{
	uint64 term;

	pthread_rwlock_rdlock(&ConsensusLogCtl->info_lock);
	term = ConsensusLogCtl->next_append_term;
	pthread_rwlock_unlock(&ConsensusLogCtl->info_lock);

	return term;
}

void
ConsensusLOGGetLastLSN(XLogRecPtr *last_write_lsn, TimeLineID *last_write_timeline)
{
	pthread_rwlock_rdlock(&ConsensusLogCtl->info_lock);
	*last_write_lsn = ConsensusLogCtl->last_write_lsn;
	*last_write_timeline = ConsensusLogCtl->last_write_timeline;
	pthread_rwlock_unlock(&ConsensusLogCtl->info_lock);
}

uint32
ConsensusLOGGetKeepSize(void)
{
#ifdef PG_HAVE_ATOMIC_U32_SIMULATION 
	uint32 log_keep_size;
	pthread_rwlock_rdlock(&ConsensusLogCtl->info_lock);
	log_keep_size = ConsensusLogCtl->log_keep_size;
	pthread_rwlock_unlock(&ConsensusLogCtl->info_lock);
	return log_keep_size;
#else
	return pg_atomic_read_u32(&ConsensusLogCtl->log_keep_size);
#endif
}

void
ConsensusLOGSetKeepSize(uint32 log_keep_size)
{
#ifdef PG_HAVE_ATOMIC_U32_SIMULATION
	pthread_rwlock_wrlock(&ConsensusLogCtl->info_lock);
	ConsensusLogCtl->log_keep_size = log_keep_size;
	pthread_rwlock_unlock(&ConsensusLogCtl->info_lock);
#else
	pg_atomic_write_u32(&ConsensusLogCtl->log_keep_size, log_keep_size);
#endif
}
