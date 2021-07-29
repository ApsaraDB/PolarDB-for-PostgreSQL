/*-------------------------------------------------------------------------
 *
 * polar_log_index_redo.c
 *   Implementation of parse xlog states and replay.
 *
 *
 * Portions Copyright (c) 2019, Alibaba.inc
 *
 * src/backend/access/logindex/polar_log_index_redo.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bufmask.h"
#include "access/heapam_xlog.h"
#include "access/nbtxlog.h"
#include "access/polar_logindex.h"
#include "access/polar_logindex_internal.h"
#include "access/rmgr.h"
#include "access/visibilitymap.h"
#include "access/xlog.h"
#include "access/xlogutils.h"
#include "access/xlog_internal.h"
#include "catalog/pg_control.h"
#include "executor/instrument.h"
#include "pg_trace.h"
#include "miscadmin.h"
#include "replication/walreceiver.h"
#include "storage/buf_internals.h"
#include "storage/polar_bufmgr.h"
#include "utils/resowner_private.h"

#define PG_RMGR(symname,name,redo,polar_idx_save, polar_idx_parse, polar_idx_redo, polar_redo,desc,identify,startup,cleanup,mask) \
	{name, polar_idx_save, polar_idx_parse, polar_idx_redo},

static BufferDesc *polar_bg_replay_in_progress_buffer = NULL;
static BufferDesc *polar_replaying_buffer = NULL;
static log_index_page_iter_t iter_context = NULL;

static const log_index_redo_t polar_idx_redo[RM_MAX_ID + 1] =
{
#include "access/rmgrlist.h"
};

static xlog_bg_redo_state_t xlog_bg_redo_state = {0};
static void polar_log_index_apply_one_page(log_index_snapshot_t *logindex_snapshot, BufferTag *tag, Buffer *buffer, log_index_page_iter_t iter);
static void polar_bg_redo_apply_one_record_on_page(log_index_lsn_t *log_index_page);
static XLogRecPtr polar_log_index_apply_page_from(XLogRecPtr start_lsn, BufferTag *tag, Buffer *buffer, polar_page_lock_t page_lock, bool outdate);
static bool polar_evict_buffer(Buffer buffer, LWLock *buf_lock);


static void
polar_xlog_log(int level, XLogReaderState *record, const char *func)
{
	RmgrId      rmid = XLogRecGetRmid(record);
	uint8       info = XLogRecGetInfo(record);
	XLogRecPtr  lsn = record->ReadRecPtr;
	const char *id;

	id = RmgrTable[rmid].rm_identify(info);

	if (id == NULL)
		elog(level, "%s lsn=%X/%X UNKNOWN (%X)", func,
			 (uint32)(lsn >> 32),
			 (uint32)lsn,
			 info & ~XLR_INFO_MASK);
	else
		elog(level, "%s lsn=%X/%X %s/%s", func,
			 (uint32)(lsn >> 32),
			 (uint32)lsn, RmgrTable[rmid].rm_name, id);
}

/*
 * Call this function when we abort transaction.
 * If transaction is aborted, the buffer replaying is stopped,
 * so we have to clear its POLAR_REDO_REPLAYING flag.
 */
void
polar_log_index_abort_replaying_buffer(void)
{
	if (polar_replaying_buffer != NULL)
	{
		uint32 redo_state = polar_lock_redo_state(polar_replaying_buffer);
		redo_state &= (~POLAR_REDO_REPLAYING);
		polar_unlock_redo_state(polar_replaying_buffer, redo_state);
		polar_replaying_buffer = NULL;
	}
}

void
polar_log_index_apply_one_record(XLogReaderState *state, BufferTag *tag, Buffer *buffer)
{
	if (polar_idx_redo[state->decoded_record->xl_rmid].rm_polar_idx_redo == NULL)
	{
		POLAR_LOG_CONSISTENT_LSN();
		POLAR_LOG_XLOG_RECORD_INFO(state);
		POLAR_LOG_BUFFER_TAG_INFO(tag);
		elog(PANIC, "rmid = %d has not polar_idx_redo function",
			 state->decoded_record->xl_rmid);
	}

	if (polar_enable_debug)
	{
		elog(LOG, "%s %d %X/%X, ([%u, %u, %u]), %u, %u", __func__,
			 *buffer,
			 (uint32)(state->ReadRecPtr >> 32),
			 (uint32)state->ReadRecPtr,
			 tag->rnode.spcNode,
			 tag->rnode.dbNode,
			 tag->rnode.relNode,
			 tag->forkNum,
			 tag->blockNum);
		polar_xlog_log(LOG, state, __func__);
	}

	polar_idx_redo[state->decoded_record->xl_rmid].rm_polar_idx_redo(state, tag, buffer);
}

XLogRecord *
polar_log_index_read_xlog(XLogReaderState *state, XLogRecPtr lsn)
{
	XLogRecord *record = NULL;
	char        *errormsg = NULL;
	int i = 1;

	while (record == NULL)
	{
		record = XLogReadRecord(state, lsn, &errormsg);

		if (record == NULL)
		{
			if (i % 100000 == 0)
				elog(WARNING, "Failed to read record which lsn=%X/%X",
					 (uint32)(lsn >> 32), (uint32)lsn);

			i++;
			pg_usleep(1000);
		}
	}

	return record;
}

static Buffer
polar_read_vm_buffer(BufferTag *heap_tag)
{
	Buffer buf = InvalidBuffer;
	Relation rel = CreateFakeRelcacheEntry(heap_tag->rnode);

	visibilitymap_pin(rel, heap_tag->blockNum, &buf);

	FreeFakeRelcacheEntry(rel);

	return buf;
}

Buffer
polar_log_index_outdate_parse(XLogReaderState *state, BufferTag *heap_tag,
							  bool get_cleanup_lock, polar_page_lock_t *page_lock, bool vm_parse)
{
	uint32      page_hash;
	LWLock      *partition_lock;    /* buffer partition lock for it */
	int         buf_id;
	Buffer      buffer = InvalidBuffer;
	BufferTag   vm_tag;
	BufferTag   *tag;

	if (!vm_parse)
		tag = heap_tag;
	else
	{
		vm_tag.rnode = heap_tag->rnode;
		vm_tag.forkNum = VISIBILITYMAP_FORKNUM;
		vm_tag.blockNum = HEAPBLK_TO_MAPBLOCK(heap_tag->blockNum);
		tag = &vm_tag;
	}

	page_hash = BufTableHashCode(tag);
	partition_lock = BufMappingPartitionLock(page_hash);

	/* Check whether the block is already in the buffer pool */
	LWLockAcquire(partition_lock, LW_SHARED);
	buf_id = BufTableLookup(tag, page_hash);

	/* Found the buffer */
	if (buf_id >= 0)
	{
		uint32 redo_state;
		BufferDesc *buf_desc = GetBufferDescriptor(buf_id);

		polar_pin_buffer(buf_desc, NULL);
		LWLockRelease(partition_lock);

		redo_state = polar_lock_redo_state(buf_desc);

		/*
		 * 1. A backend process is reading this buffer from storage and it will redo for this buffer.
		 * 2. The mini transaction lock is acquired by the startup process.
		 * 3. We add lsn to logindex in startup process with the acquired mini transaction lock.
		 * 4. When backend process compete buffer read and start to redo for buffer, it will redo to this lsn
		 */
		if (!(redo_state & POLAR_REDO_READ_IO_END) ||
				((redo_state & POLAR_REDO_REPLAYING) && !get_cleanup_lock))
		{
			redo_state |= POLAR_REDO_OUTDATE;
			polar_unlock_redo_state(buf_desc, redo_state);

			ReleaseBuffer(BufferDescriptorGetBuffer(buf_desc));
			POLAR_LOG_INDEX_ADD_LSN(state, tag);
		}
		else
		{
			polar_unlock_redo_state(buf_desc, redo_state);
			ReleaseBuffer(BufferDescriptorGetBuffer(buf_desc));
			polar_log_index_mini_trans_unlock(*page_lock);

			if (!vm_parse)
				buffer = XLogReadBufferExtended(tag->rnode, tag->forkNum, tag->blockNum, RBM_NORMAL_NO_LOG);
			else
				buffer = polar_read_vm_buffer(heap_tag);

			if (BufferIsValid(buffer))
			{
				if (get_cleanup_lock)
					polar_lock_buffer_for_cleanup_ext(buffer, false);
				else
					polar_lock_buffer_ext(buffer, BUFFER_LOCK_EXCLUSIVE, false);
			}
			else
			{
				POLAR_LOG_CONSISTENT_LSN();
				POLAR_LOG_XLOG_RECORD_INFO(state);
				POLAR_LOG_BUFFER_TAG_INFO(tag);
				elog(FATAL, "Failed to read page");
			}

			*page_lock = polar_log_index_mini_trans_lock(tag, LW_EXCLUSIVE, NULL);
			POLAR_LOG_INDEX_ADD_LSN(state, tag);

			redo_state = polar_lock_redo_state(buf_desc);
			redo_state |= POLAR_REDO_OUTDATE;
			polar_unlock_redo_state(buf_desc, redo_state);
		}

		if (polar_enable_debug)
			ereport(LOG, (errmsg("Mark page buffer outdate, buf_id=%d", buf_id)));
	}
	else
	{
		LWLockRelease(partition_lock);
		POLAR_LOG_INDEX_ADD_LSN(state, tag);
	}

	return buffer;
}

static Buffer
polar_log_index_fresh_parse(XLogReaderState *state, BufferTag *tag,
							bool get_cleanup_lock, polar_page_lock_t *page_lock)
{
	uint32      page_hash;
	LWLock      *partition_lock;    /* buffer partition lock for it */
	int         buf_id;
	Buffer      buffer = InvalidBuffer;

	page_hash = BufTableHashCode(tag);
	partition_lock = BufMappingPartitionLock(page_hash);

	/* Check whether the block is already in the buffer pool */
	LWLockAcquire(partition_lock, LW_SHARED);
	buf_id = BufTableLookup(tag, page_hash);
	LWLockRelease(partition_lock);

	/* Found the buffer */
	if (buf_id >= 0)
	{
		polar_log_index_mini_trans_unlock(*page_lock);
		buffer = XLogReadBufferExtended(tag->rnode, tag->forkNum, tag->blockNum, RBM_NORMAL_NO_LOG);

		if (BufferIsValid(buffer))
		{
			if (get_cleanup_lock)
				polar_lock_buffer_for_cleanup_ext(buffer, false);
			else
				polar_lock_buffer_ext(buffer, BUFFER_LOCK_EXCLUSIVE, false);
		}
		else
		{
			POLAR_LOG_CONSISTENT_LSN();
			POLAR_LOG_XLOG_RECORD_INFO(state);
			POLAR_LOG_BUFFER_TAG_INFO(tag);
			elog(FATAL, "Failed to read page");
		}

		*page_lock = polar_log_index_mini_trans_lock(tag, LW_EXCLUSIVE, NULL);
	}

	POLAR_LOG_INDEX_ADD_LSN(state, tag);

	if (BufferIsValid(buffer))
	{
		if (state->noPayload)
			polar_log_index_apply_buffer(&buffer, state->ReadRecPtr, *page_lock);
		else
			polar_log_index_apply_one_record(state, tag, &buffer);
	}

	return buffer;
}

Buffer
polar_log_index_parse(XLogReaderState *state, BufferTag *tag,
					  bool get_cleanup_lock, polar_page_lock_t *page_lock)
{
	Buffer      buffer = InvalidBuffer;

	if (POLAR_ENABLE_PAGE_OUTDATE())
		buffer = polar_log_index_outdate_parse(state, tag, get_cleanup_lock, page_lock, false);
	else
		buffer = polar_log_index_fresh_parse(state, tag, get_cleanup_lock, page_lock);

	return buffer;
}

static void
polar_log_index_parse_tag(XLogReaderState *state, BufferTag *tag,
						  bool get_cleanup_lock)
{
	polar_page_lock_t   page_lock;
	Buffer              buffer;

	page_lock = polar_log_index_mini_trans_lock(tag, LW_EXCLUSIVE, NULL);
	buffer = polar_log_index_parse(state, tag, get_cleanup_lock, &page_lock);

	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);

	polar_log_index_mini_trans_unlock(page_lock);
}

void
polar_log_index_save_block(XLogReaderState *state, uint8 block_id)
{
	BufferTag tag;
	DecodedBkpBlock *blk;

	Assert(block_id <= XLR_MAX_BLOCK_ID);

	blk = &state->blocks[block_id];

	Assert(blk->in_use);

	INIT_BUFFERTAG(tag, blk->rnode, blk->forknum, blk->blkno);

	if (XLogRecGetRmid(state) == RM_XLOG_ID &&
			(XLogRecGetInfo(state) & ~XLR_INFO_MASK) == XLOG_FPSI)
		polar_log_index_add_lsn(POLAR_LOGINDEX_FULLPAGE_SNAPSHOT, &tag, InvalidXLogRecPtr, state->ReadRecPtr);
	else
		polar_log_index_add_lsn(POLAR_LOGINDEX_WAL_SNAPSHOT, &tag, InvalidXLogRecPtr, state->ReadRecPtr);
}

void
polar_log_index_redo_parse(XLogReaderState *state, uint8 block_id)
{
	BufferTag tag;

	POLAR_GET_LOG_TAG(state, tag, block_id);
	polar_log_index_parse_tag(state, &tag, false);
}

void
polar_log_index_cleanup_parse(XLogReaderState *state, uint8 block_id)
{
	BufferTag tag;

	POLAR_GET_LOG_TAG(state, tag, block_id);
	polar_log_index_parse_tag(state, &tag, true);
}

void
polar_log_index_save_lsn(XLogReaderState *state)
{
	RmgrId rmid = XLogRecGetRmid(state);

	if (polar_idx_redo[rmid].rm_polar_idx_save != NULL)
		polar_idx_redo[rmid].rm_polar_idx_save(state);
	else
	{
		POLAR_LOG_CONSISTENT_LSN();
		POLAR_LOG_XLOG_RECORD_INFO(state);
		elog(PANIC, "rm_polar_idx_save is not set for rmid=%d", rmid);
	}
}

bool
polar_enable_logindex_parse(void)
{
	/*
	 * We don't call logindex parse if it's not replica mode or logindex is not enabled
	 */
	if (!(polar_in_replica_mode() && polar_enable_redo_logindex))
		return false;

	/*
	 * If it's replica mode and logindex is enabled, we parse XLOG and save it to logindex.
	 * During recovery we read XLOG from checkpoint, master node can drop or truncate table
	 * after this checkpoint. We can't read these removed data blocks, so it will be PANIC
	 * if we read data block and replay XLOG.
	 * In master node it will create table if it does not exist, but we can not do this in
	 * replica mode.
	 */
	return true;
}

bool
polar_log_index_parse_xlog(RmgrId rmid, XLogReaderState *state, XLogRecPtr redo_start_lsn, XLogRecPtr *mini_trans_lsn)
{
	bool redo = false;

	Assert(mini_trans_lsn != NULL);
	*mini_trans_lsn = InvalidXLogRecPtr;

	if (!polar_enable_redo_logindex)
		return false;


	if (polar_in_replica_mode()
			&& polar_idx_redo[rmid].rm_polar_idx_parse != NULL)
	{
		if (polar_enable_debug)
			polar_xlog_log(LOG, state, __func__);

		polar_log_index_mini_trans_start(state->EndRecPtr);
		redo = polar_idx_redo[rmid].rm_polar_idx_parse(state);
		/*
		 * We can not end mini transaction here because XLogCtl->lastReplayedEndRecPtr is not updated.
		 * If we end mini transaction here, and one backend start to do buffer replay,
		 * it can not acquire mini transaction lock and replay to XLogCtl->lastReplayedEndRecPtr,
		 * so the current record which was parsed and saved to logindex will be lost.
		 * We will end mini transaction after startup process update XLogCtl->lastReplayedEndRecPtr.
		 */
		*mini_trans_lsn = state->EndRecPtr;
	}
	/* POLAR: create and save logindex in master and standby. */
	else if (polar_streaming_xlog_meta)
	{
		if (polar_idx_redo[rmid].rm_polar_idx_save != NULL)
			polar_idx_redo[rmid].rm_polar_idx_save(state);
	}

	/* If current record lsn is smaller than redo start lsn, then we only parse xlog and create logindex. */
	if (state->ReadRecPtr < redo_start_lsn)
		redo = true;

	return redo;
}

static XLogRecPtr
polar_log_index_apply_page_from(XLogRecPtr start_lsn, BufferTag *tag, Buffer *buffer, polar_page_lock_t page_lock, bool outdate)
{
	XLogRecPtr end_lsn;
	log_index_page_iter_t iter;
	Page page;

	Assert(BufferIsValid(*buffer));
	page = BufferGetPage(*buffer);

	if (!PageIsNew(page))
		start_lsn = Max(start_lsn, PageGetLSN(page));

	/*
	 * If this buffer replaying is protected by mini transaction page_lock then we replay to the record
	 * which is currently replaying.Otherwise we replay to the last record which is successfully replayed.
	 * If we replay to the currently replaying record without mini transaction page_lock, we may get inconsistent
	 * date structure in memory.
	 */
	if (page_lock != POLAR_INVALID_PAGE_LOCK)
		end_lsn = polar_get_replay_end_rec_ptr(NULL);
	else
		end_lsn = GetXLogReplayRecPtr(NULL);

	if (polar_enable_debug)
	{
		elog(LOG, "%s %d %X/%X %X/%X, ([%u, %u, %u]), %u, %u", __func__,
			 *buffer,
			 (uint32)(start_lsn >> 32),
			 (uint32)start_lsn,
			 (uint32)(end_lsn >> 32),
			 (uint32)end_lsn,
			 tag->rnode.spcNode,
			 tag->rnode.dbNode,
			 tag->rnode.relNode,
			 tag->forkNum,
			 tag->blockNum);
	}

	/*
	 * Logindex record the start position of XLOG and we search LSN between [start_lsn, end_lsn].
	 * And end_lsn points to the end position of the last xlog, so we should subtract 1 here .
	 */
	iter = polar_log_index_create_page_iterator(POLAR_LOGINDEX_WAL_SNAPSHOT, tag, start_lsn, end_lsn - 1);

	if (iter->state != ITERATE_STATE_FINISHED)
	{
		elog(PANIC, "Failed to iterate data for ([%u, %u, %u]), %u, %u, which start_lsn=%X/%X and end_lsn=%X/%X",
			 tag->rnode.spcNode,
			 tag->rnode.dbNode,
			 tag->rnode.relNode,
			 tag->forkNum,
			 tag->blockNum,
			 (uint32)(start_lsn >> 32), (uint32)start_lsn,
			 (uint32)(end_lsn >> 32), (uint32)end_lsn);
	}

	polar_log_index_apply_one_page(POLAR_LOGINDEX_WAL_SNAPSHOT, tag, buffer, iter);

	polar_log_index_release_page_iterator(iter);

	return end_lsn;
}

void
polar_log_index_lock_apply_page_from(XLogRecPtr start_lsn, BufferTag *tag, Buffer *buffer)
{
	BufferDesc *buf_hdr;
	polar_page_lock_t page_lock;
	uint32 redo_state;
	bool outdate;

	Assert(BufferIsValid(*buffer));

	/*
	 * Record the buffer that is replaying.If we abort transaction in
	 * this backend process, we need to clear POLAR_REDO_REPLAYING from
	 * buffer redo state
	 */
	polar_replaying_buffer = buf_hdr = GetBufferDescriptor(*buffer - 1);

	redo_state = polar_lock_redo_state(buf_hdr);
	redo_state |= POLAR_REDO_REPLAYING;
	outdate = redo_state & POLAR_REDO_OUTDATE;
	redo_state &= (~POLAR_REDO_OUTDATE);
	polar_unlock_redo_state(buf_hdr, redo_state);

	do
	{
		page_lock = polar_log_index_mini_trans_cond_lock(tag, LW_EXCLUSIVE, NULL);

		start_lsn = polar_log_index_apply_page_from(start_lsn, tag, buffer, page_lock, outdate);

		if (page_lock != POLAR_INVALID_PAGE_LOCK)
			polar_log_index_mini_trans_unlock(page_lock);

		redo_state = polar_lock_redo_state(buf_hdr);

		if (redo_state & POLAR_REDO_OUTDATE)
		{
			redo_state &= (~POLAR_REDO_OUTDATE);
			outdate = true;
		}
		else
		{
			redo_state &= (~POLAR_REDO_REPLAYING);
			outdate = false;
		}

		polar_unlock_redo_state(buf_hdr, redo_state);
	}
	while (redo_state & POLAR_REDO_REPLAYING);

	polar_replaying_buffer = NULL;
}

/*
 * Apply the specific buffer from the start lsn to the last parsed lsn.
 */
void
polar_log_index_apply_buffer(Buffer *buffer, XLogRecPtr start_lsn, polar_page_lock_t page_lock)
{
	BufferDesc *buf_hdr;

	Assert(BufferIsValid(*buffer));

	buf_hdr = GetBufferDescriptor(*buffer - 1);

	polar_log_index_apply_page_from(start_lsn, &buf_hdr->tag, buffer, page_lock, false);
}


/*
 * Apply the specific buffer from the start lsn to the last parsed lsn.
 * We must acquire mini transaction page lock
 * to avoid add lsn for this page.This is used when enable page invalid and
 * apply buffer when a backend process use this buffer.
 */
void
polar_log_index_lock_apply_buffer(Buffer *buffer)
{
	BufferDesc *buf_desc;
	XLogRecPtr bg_replayed_lsn;

	Assert(BufferIsValid(*buffer));
	buf_desc = GetBufferDescriptor(*buffer - 1);

	if (polar_redo_check_state(buf_desc, POLAR_REDO_OUTDATE))
	{
		bg_replayed_lsn = polar_bg_redo_get_replayed_lsn();
		POLAR_SET_BACKEND_READ_MIN_LSN(bg_replayed_lsn);

		polar_log_index_lock_apply_page_from(bg_replayed_lsn, &buf_desc->tag, buffer);

		POLAR_RESET_BACKEND_READ_MIN_LSN();
	}
}

static void
polar_log_index_apply_one_page(log_index_snapshot_t *logindex_snapshot, BufferTag *tag, Buffer *buffer, log_index_page_iter_t iter)
{
	static XLogReaderState *state = NULL;
	log_index_lsn_t *lsn_info;
	MemoryContext oldcontext;

	iter_context = iter;

	oldcontext = MemoryContextSwitchTo(logindex_snapshot->mem_cxt);

	if (state == NULL)
	{
		state = XLogReaderAllocate(wal_segment_size, &read_local_xlog_page, NULL);

		if (!state)
		{
			ereport(FATAL,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory"),
					 errdetail("Failed while allocating a WAL reading processor.")));
		}
	}

	while ((lsn_info = polar_log_index_page_iterator_next(iter)) != NULL)
	{
		XLogRecPtr lsn = lsn_info->lsn;

		polar_log_index_read_xlog(state, lsn);
		polar_log_index_apply_one_record(state, tag, buffer);

		CHECK_FOR_INTERRUPTS();
	}

	MemoryContextSwitchTo(oldcontext);
}

static bool
polar_bg_redo_ready(void)
{
	return polar_log_index_check_state(POLAR_LOGINDEX_WAL_SNAPSHOT, POLAR_LOGINDEX_STATE_INITIALIZED)
		   && !XLogRecPtrIsInvalid(polar_bg_redo_get_replayed_lsn());
}

/*
 * POLAR: Apply record in background to catch up replayed lsn updated in Startup.
 *
 * In this func, it use a iterator from logindex to go through xlog record, and
 * apply record on related page if page buffer is in buffer pool. If current record
 * is not in logindex, such as clog record, it will skip replay of it and advance
 * bg_replayed_lsn directly. Initial xlog record lsn is set when reachedConsistency
 * turns true in Startup process.
 *
 * This func is called in Bgwriter process in Replica node. Each run, it apply
 * polar_bg_replay_batch_size xlog records at most.
 *
 * It will advance bg_replayed_lsn which will be replied to master as restart_lsn of
 * replication slot. And that will stop master from deleting wal segments before
 * bg_replayed_lsn.
 *
 * Return true if no more records need replay, otherwise false.
 */
bool
polar_log_index_apply_xlog_background(void)
{
	int     replayed_count = 0;
	XLogRecPtr  replayed_lsn;

	if (xlog_bg_redo_state.state == NULL)
	{
		/* ensure we can control memory */
		MemoryContext oldcontext = MemoryContextSwitchTo(POLAR_LOGINDEX_WAL_SNAPSHOT->mem_cxt);
		xlog_bg_redo_state.state = XLogReaderAllocate(wal_segment_size, &read_local_xlog_page, NULL);
		MemoryContextSwitchTo(oldcontext);

		if (!xlog_bg_redo_state.state)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory"),
					 errdetail("Failed while allocating a WAL reading processor.")));

		xlog_bg_redo_state.state->EndRecPtr = InvalidXLogRecPtr;
	}

	/* replay start lsn not set yet, do nothing */
	if (!polar_bg_redo_ready())
		return true;
	else if (xlog_bg_redo_state.log_index_iter == NULL)
	{
		XLogRecPtr start_lsn = polar_bg_redo_get_replayed_lsn();

		/* Init log index iterator */
		xlog_bg_redo_state.log_index_iter =
			polar_log_index_create_lsn_iterator(POLAR_LOGINDEX_WAL_SNAPSHOT, start_lsn);

		ereport(LOG, (errmsg("Start background replay iter from %X/%X",
							 (uint32)(start_lsn >> 32),
							 (uint32)(start_lsn))));
	}

	replayed_lsn = GetXLogReplayRecPtr(NULL);

	for (replayed_count = 0; replayed_count < polar_bg_replay_batch_size; replayed_count++)
	{
		XLogRecPtr current_ptr;

		if (xlog_bg_redo_state.log_index_page == NULL)
			xlog_bg_redo_state.log_index_page =
				polar_log_index_lsn_iterator_next(POLAR_LOGINDEX_WAL_SNAPSHOT, xlog_bg_redo_state.log_index_iter);

		/* If no more item in logindex, exit and retry next run */
		if (xlog_bg_redo_state.log_index_page == NULL)
			break;

		/* Now, we get one record from logindex lsn iterator */

		/* If current xlog record is beyond replayed lsn, just break */
		if (xlog_bg_redo_state.log_index_page->lsn >= replayed_lsn)
			break;

		/* we get a new record now, set current_ptr as the lsn of it */
		current_ptr = xlog_bg_redo_state.log_index_page->lsn;

		/*
		 * replay each page of current record. because we checked log_index_page->lsn < replayed_lsn,
		 * we will get all related pages of the record, so use do-while to go through each page replay
		 */
		do
		{
			polar_bg_redo_apply_one_record_on_page(xlog_bg_redo_state.log_index_page);
			xlog_bg_redo_state.log_index_page =
				polar_log_index_lsn_iterator_next(POLAR_LOGINDEX_WAL_SNAPSHOT, xlog_bg_redo_state.log_index_iter);
		}
		while (xlog_bg_redo_state.log_index_page != NULL &&
				current_ptr == xlog_bg_redo_state.log_index_page->lsn);

		/* now, record related pages have all been replayed, so we can advance bg_replayed_lsn */

		if (polar_enable_debug)
			ereport(LOG, (errmsg("apply record at %X/%X done",
								 (uint32)(current_ptr >> 32), (uint32)(current_ptr))));

		polar_bg_redo_set_replayed_lsn(current_ptr);
	}

	if (polar_enable_debug)
		ereport(LOG, (errmsg("Background replay %d records at this run.", replayed_count)));

	return (replayed_count < polar_bg_replay_batch_size);
}

/*
 * POLAR: Evict selectd buffer.
 *
 * In some case, we need to release buffer's content lock if we have before
 * put it back to free list. In other word, if the buffer is locked and pined
 * before this func, and it will be released and un-pined while being evicted.
 */
static bool
polar_evict_buffer(Buffer buffer, LWLock *buf_lock)
{
	uint32 buf_state;
	BufferDesc *buf_desc = GetBufferDescriptor(buffer - 1);
	BufferTag tag = buf_desc->tag;
	uint32 hash = BufTableHashCode(&tag);
	LWLock *partition_lock = BufMappingPartitionLock(hash);
	bool evict = false;

	LWLockAcquire(partition_lock, LW_EXCLUSIVE);
	buf_state = LockBufHdr(buf_desc);

	/* We should already pin this buffer, and then evict it */
	if (BUF_STATE_GET_REFCOUNT(buf_state) == 1 && !(buf_state & BM_DIRTY)
			&& (buf_state & BM_TAG_VALID))
	{
		CLEAR_BUFFERTAG(buf_desc->tag);
		buf_state &= ~(BUF_FLAG_MASK | BUF_USAGECOUNT_MASK);
		evict = true;
	}

	UnlockBufHdr(buf_desc, buf_state);

	if (evict)
	{
		if (buf_lock)
			LWLockRelease(buf_lock);

		BufTableDelete(&tag, hash);
		ReleaseBuffer(buffer);
		/*
		 * Insert the buffer at the head of the list of free buffers.
		 */
		StrategyFreeBuffer(buf_desc);
	}

	LWLockRelease(partition_lock);
	return evict;
}

static void
polar_bg_redo_apply_read_record(log_index_lsn_t *log_index_page, Buffer buffer)
{
	char        *errormsg = NULL;
	BufferDesc *buf_desc = GetBufferDescriptor(buffer - 1);
	Page page = BufferGetPage(BufferDescriptorGetBuffer(buf_desc));

	/* If needed record has not been read, just read it */
	if (xlog_bg_redo_state.state->ReadRecPtr != log_index_page->lsn)
	{
		XLogRecord *record = NULL;
		/* In XLogReadRecord, it may enlarge buffer of XlogReaderState, so we should switch context */
		MemoryContext oldcontext = MemoryContextSwitchTo(POLAR_LOGINDEX_WAL_SNAPSHOT->mem_cxt);

		record = XLogReadRecord(xlog_bg_redo_state.state, log_index_page->lsn, &errormsg);
		MemoryContextSwitchTo(oldcontext);

		if (record == NULL)
		{
			POLAR_LOG_BUFFER_DESC(buf_desc);
			POLAR_LOG_LOGINDEX_LSN_INFO(log_index_page);
			POLAR_LOG_CONSISTENT_LSN();
			POLAR_LOG_XLOG_RECORD_INFO(xlog_bg_redo_state.state);

			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not read record from WAL at %X/%X",
							(uint32)(xlog_bg_redo_state.state->EndRecPtr >> 32),
							(uint32) xlog_bg_redo_state.state->EndRecPtr)));
		}
	}

	if (!polar_redo_check_state(buf_desc, POLAR_REDO_REPLAYING))
	{
		/*
		 * Check redo state again, if it's replaying then this xlog will be replayed by replaying
		 * process.
		 */
		polar_lock_buffer_ext(buffer, BUFFER_LOCK_EXCLUSIVE, false);

		/* Critical section begin */

		/* Apply current record on page */
		if (PageGetLSN(page) <= log_index_page->lsn)
			polar_log_index_apply_one_record(
				xlog_bg_redo_state.state, log_index_page->tag, &buffer);

		if (polar_enable_debug)
		{
			elog(LOG, "%s background apply page", __func__);
			POLAR_LOG_BUFFER_DESC(buf_desc);
			POLAR_LOG_PAGE_INFO(page);
			POLAR_LOG_LOGINDEX_LSN_INFO(log_index_page);
			POLAR_LOG_CONSISTENT_LSN();
			POLAR_LOG_XLOG_RECORD_INFO(xlog_bg_redo_state.state);
		}

		/* Critical section end */
		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	}

}
/*
 * POLAR: Apply one record on one page.
 *
 * We will check existence of buffer page in buffer pool first. If hit, replay record on it,
 * otherwise do nothing. While buffer hit, buffer should be pinned before replaying from being
 * evicted in which target buffer may be not the requested one.
 *
 * If pinned buffer is invalid, buffer is doing io now. After io operation, page will be replayed,
 * so no need to replay it in background.
 */
static void
polar_bg_redo_apply_one_record_on_page(log_index_lsn_t *log_index_page)
{
	uint32      new_hash = 0;                /* hash value for new_tag */
	LWLock      *new_partition_lock = NULL;    /* buffer partition lock for it */
	int     buf_id = 0;

	Assert(log_index_page != NULL);

	new_hash = BufTableHashCode(log_index_page->tag);
	new_partition_lock = BufMappingPartitionLock(new_hash);

	/* See if the block is in the buffer pool already */
	LWLockAcquire(new_partition_lock, LW_SHARED);
	buf_id = BufTableLookup(log_index_page->tag, new_hash);

	/* If page is in buffer, we can apply record, otherwise we do nothing */
	if (buf_id >= 0)
	{
		Buffer buffer;
		BufferDesc *buf_desc = GetBufferDescriptor(buf_id);
		bool valid;

		/* Pin buffer from being evicted */
		valid = polar_pin_buffer(buf_desc, NULL);
		/* Can release the mapping lock as soon as possible */
		LWLockRelease(new_partition_lock);

		polar_bg_replay_in_progress_buffer = buf_desc;
		buffer = BufferDescriptorGetBuffer(buf_desc);

		if (!valid || polar_redo_check_state(buf_desc, POLAR_REDO_REPLAYING))
		{
			/*
			 * Invalid buffer or redo state is replaying  means some other process is reading or replaying this page
			 * currently. After reading or replaying, xlog replay will be done, so we don't need to
			 * replay it. Even error occurs while io, invalid buffer will be read from
			 * disk next access try, or evicted from buffer pool if no one else access it.
			 */
			ReleaseBuffer(buffer);
		}
		else
		{
			Page page = BufferGetPage(BufferDescriptorGetBuffer(buf_desc));
			XLogRecPtr consist_lsn = polar_get_primary_consist_ptr();
			bool evicted = false;

			/*
			 * At this point, we expect buffer valid already. If not, error occurs.
			 */
			if (!BufferIsValid(buffer))
			{
				POLAR_LOG_BUFFER_DESC(buf_desc);
				POLAR_LOG_LOGINDEX_LSN_INFO(log_index_page);
				POLAR_LOG_CONSISTENT_LSN();
				POLAR_LOG_XLOG_RECORD_INFO(xlog_bg_redo_state.state);
				elog(PANIC, "invalid page buffer while background apply");
			}

			/*
			 * 1. In this function the buffer is marked outdate
			 * 2. If consistent lsn is larger than page lsn then the data in storage is newer than in buffer
			 * 3. We will try to remove this page from buffer pool, and then backend will read newer data from storage
			 */
			if (!PageIsNew(page) && consist_lsn > PageGetLSN(page))
				evicted = polar_evict_buffer(buffer, NULL);

			if (!evicted)
			{
				polar_bg_redo_apply_read_record(log_index_page, buffer);
				ReleaseBuffer(buffer);
			}
		}

		polar_bg_replay_in_progress_buffer = NULL;
	}
	else
		LWLockRelease(new_partition_lock);
}

/*
 * POLAR: Return max lsn of a and b.
 */
XLogRecPtr
polar_max_xlog_rec_ptr(XLogRecPtr a, XLogRecPtr b)
{
	if (XLogRecPtrIsInvalid(a))
		return b;
	else if (XLogRecPtrIsInvalid(b))
		return a;
	else
		return Max(a, b);
}

/*
 * POLAR: Return min lsn of a and b.
 */
XLogRecPtr
polar_min_xlog_rec_ptr(XLogRecPtr a, XLogRecPtr b)
{
	if (XLogRecPtrIsInvalid(a))
		return b;
	else if (XLogRecPtrIsInvalid(b))
		return a;
	else
		return Min(a, b);
}

/*
 * POLAR: Lock redo state - set POLAR_REDO_LOCKED in redo state
 */
uint32
polar_lock_redo_state(BufferDesc *desc)
{
	SpinDelayStatus delayStatus;
	uint32      old_redo_state;

	init_local_spin_delay(&delayStatus);

	while (true)
	{
		/* set POLAR_REDO_LOCKED flag */
		old_redo_state = pg_atomic_fetch_or_u32(&desc->polar_redo_state, POLAR_REDO_LOCKED);

		/* if it wasn't set before we're OK */
		if (!(old_redo_state & POLAR_REDO_LOCKED))
			break;

		perform_spin_delay(&delayStatus);
	}

	finish_spin_delay(&delayStatus);
	return old_redo_state | POLAR_REDO_LOCKED;
}

void
polar_log_page_iter_context(void)
{
	if (iter_context != NULL)
	{
		elog(LOG, "Page iter start_lsn=%lX, end_lsn=%lX", iter_context->min_lsn, iter_context->max_lsn);
		POLAR_LOG_BUFFER_TAG_INFO(&iter_context->tag);
	}
}

/*
 * POLAR; if we read a future page, then call this function to restore old version page
 */
bool
polar_log_index_restore_fullpage_snapshot_if_needed(BufferTag *tag, Buffer *buffer)
{
	static XLogReaderState *state = NULL;
	log_index_lsn_t *lsn_info = NULL;
	MemoryContext oldcontext = NULL;
	XLogRecPtr replayed_lsn = InvalidXLogRecPtr;
	XLogRecPtr end_lsn = PG_INT64_MAX;
	log_index_page_iter_t iter;
	Page page;
	bool    success = false;
	TimestampTz start_time = GetCurrentTimestamp();

	Assert(BufferIsValid(*buffer));
	page = BufferGetPage(*buffer);

retry:

	/*
	 * POLAR: If page lsn is larger than replayed xlog lsn,
	 * then this replica read a future page
	 */
	if (!polar_is_future_page(GetBufferDescriptor(*buffer - 1)))
		return false;

	pgstat_report_wait_start(WAIT_EVENT_LOGINDEX_WAIT_FULLPAGE);

	replayed_lsn = GetXLogReplayRecPtr(&ThisTimeLineID);

	/*
	 * Logindex record the start position of XLOG and we search LSN between [replayed_lsn, end_lsn].
	 * And end_lsn points to the end position of the last xlog, so we should subtract 1 here .
	 */
	iter = polar_log_index_create_page_iterator(POLAR_LOGINDEX_FULLPAGE_SNAPSHOT, tag, replayed_lsn, end_lsn - 1);

	if (iter->state != ITERATE_STATE_FINISHED)
	{
		elog(PANIC, "Failed to iterate data for ([%u, %u, %u]), %u, %u, which replayedlsn=%X/%X and end_lsn=%X/%X",
			 tag->rnode.spcNode,
			 tag->rnode.dbNode,
			 tag->rnode.relNode,
			 tag->forkNum,
			 tag->blockNum,
			 (uint32)(replayed_lsn >> 32), (uint32)replayed_lsn,
			 (uint32)(end_lsn >> 32), (uint32)end_lsn);
	}

	oldcontext = MemoryContextSwitchTo(POLAR_LOGINDEX_FULLPAGE_SNAPSHOT->mem_cxt);

	if (state == NULL)
	{
		state = XLogReaderAllocate(wal_segment_size, &read_local_xlog_page, NULL);

		if (!state)
		{
			ereport(FATAL,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory"),
					 errdetail("Failed while allocating a WAL reading processor.")));
		}
	}

	lsn_info = polar_log_index_page_iterator_next(iter);

	if (lsn_info != NULL)
	{
		XLogRecPtr lsn = lsn_info->lsn;

		polar_log_index_read_xlog(state, lsn);
		polar_log_index_apply_one_record(state, tag, buffer);
		success = true;
	}

	MemoryContextSwitchTo(oldcontext);

	polar_log_index_release_page_iterator(iter);

	if (!success)
	{
		CHECK_FOR_INTERRUPTS();
		/* Force FATAL future page if we wait fullpage logindex too long */
		if (TimestampDifferenceExceeds(start_time, GetCurrentTimestamp(),
					polar_wait_old_version_page_timeout))
			elog(FATAL, "Read a future page due to timeout, page lsn = %lx, replayed_lsn = %lx, page_tag = '([%u, %u, %u]), %u, %u'",
					PageGetLSN(page), replayed_lsn, tag->rnode.spcNode, tag->rnode.dbNode,
					tag->rnode.relNode, tag->forkNum, tag->blockNum);
		pg_usleep(100); /* 0.1ms */
		pgstat_report_wait_end();
		goto retry;
	}

	pgstat_report_wait_end();

	if (polar_enable_redo_debug)
		elog(LOG, "%s restore page ([%u, %u, %u]), %u, %u, LSN=%X/%X", __func__,
			 tag->rnode.spcNode,
			 tag->rnode.dbNode,
			 tag->rnode.relNode,
			 tag->forkNum,
			 tag->blockNum,
			 (uint32)(PageGetLSN(page) >> 32), (uint32)PageGetLSN(page));

	return true;
}
