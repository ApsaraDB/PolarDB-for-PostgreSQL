/*-------------------------------------------------------------------------
 *
 * polar_bufmgr.c
 *    polardb buffer manager interface routines
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
 *    src/backend/storage/buffer/polar_bufmgr.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/polar_log.h"
#include "access/xlog.h"
#include "postmaster/polar_parallel_bgwriter.h"
#include "storage/polar_bufmgr.h"
#include "storage/polar_copybuf.h"
#include "storage/polar_fd.h"
#include "storage/polar_flushlist.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

#define BUF_WRITTEN				0x01
#define STOP_PARALLEL_BGWRITER_DELAY_FACTOR	10

extern ParallelBgwriterInfo *polar_parallel_bgwriter_info;
extern void ResourceOwnerEnlargeBuffers(ResourceOwner owner);

static int        prev_sync_count = 0;
static int64      consistent_lsn_delta = 0;

static XLogRecPtr polar_cal_cur_consistent_lsn(void);
static void polar_sync_buffer_from_copy_buffer(WritebackContext *wb_context, int flags);
static void polar_start_or_stop_parallel_bgwriter(bool is_normal_bgwriter, uint64 lag);
static int evaluate_sync_buffer_num(uint64 lag);
static XLogRecPtr update_consistent_lsn_delta(XLogRecPtr cur_consistent_lsn);

/* Reset oldest lsn to invalid and remove it from flushlist. */
void
polar_reset_buffer_oldest_lsn(BufferDesc *buf_hdr)
{
	XLogRecPtr oldest_lsn = polar_buffer_get_oldest_lsn(buf_hdr);
	XLogRecPtr consistent_lsn = polar_get_consistent_lsn();

	if (XLogRecPtrIsInvalid(oldest_lsn))
	{
		Assert(buf_hdr->recently_modified_count == 0);
		/*
		 * In master, we should check all flush related flag clear,
		 * so we assert each flags.
		 */
		Assert((buf_hdr->polar_flags & POLAR_BUF_OLDEST_LSN_IS_FAKE) == 0);
		Assert((buf_hdr->polar_flags & POLAR_BUF_FIRST_TOUCHED_AFTER_COPY) == 0);
	}
	else
	{
		/* This should not happen */
		if (!XLogRecPtrIsInvalid(consistent_lsn) && oldest_lsn < consistent_lsn)
		{
			elog(WARNING, "Buffer ([%u,%u,%u], %u, %d) oldest lsn %X/%X is less than consistent lsn %X/%X",
				 buf_hdr->tag.rnode.spcNode,
				 buf_hdr->tag.rnode.dbNode,
				 buf_hdr->tag.rnode.relNode,
				 buf_hdr->tag.blockNum,
				 buf_hdr->tag.forkNum,
				 (uint32)(oldest_lsn >> 32), (uint32) oldest_lsn,
				 (uint32)(consistent_lsn >> 32), (uint32) consistent_lsn);
		}

		buf_hdr->recently_modified_count = 0;
		buf_hdr->polar_flags = 0;

		polar_remove_buffer_from_flush_list(buf_hdr);
	}
}

/*
 * polar_bg_buffer_sync - Write out some dirty buffers in the flushlist.
 *
 * This is called periodically by the background writer process.
 *
 * Returns true if it's appropriate for the bgwriter process to go into
 * low-power hibernation mode.
 */
bool
polar_bg_buffer_sync(WritebackContext *wb_context, int flags)
{
	bool       res = false;
	XLogRecPtr consistent_lsn = InvalidXLogRecPtr;

	/* Use normal BgBufferSync */
	if (!polar_enable_shared_storage_mode || polar_in_replica_mode())
		return BgBufferSync(wb_context, flags);

	/*
	 * For polardb, it should enable the flush list, otherwise, the consistent
	 * lsn always is invalid.
	 */
	if (polar_flush_list_enabled())
	{
		if (polar_enable_normal_bgwriter)
			res = BgBufferSync(wb_context, flags);
		res = polar_buffer_sync(wb_context, &consistent_lsn, true, flags) || res;
	}
	else
		res = BgBufferSync(wb_context, flags);

	polar_set_consistent_lsn(consistent_lsn);

	return res;
}

/* Whether we should control the vm buffer to flush. */
bool
polar_donot_control_vm_buffer(BufferDesc *buf_hdr)
{
	Assert(buf_hdr != NULL);
	return buf_hdr->tag.forkNum == VISIBILITYMAP_FORKNUM &&
		   !polar_enable_control_vm_flush;
}

/*
 * Calculate current consistent lsn, it is the minimum between first buffer of
 * flush list and all copy buffers. If oldest lsn of all buffers are invalid,
 * we use current insert RecPtr, because some commands generate wal records
 * but do not set any buffers oldest lsn.
 */
static XLogRecPtr
polar_cal_cur_consistent_lsn(void)
{
	BufferDesc *buf;
	XLogRecPtr  clsn;
	XLogRecPtr  lsn;

	Assert(polar_flush_list_enabled());

	SpinLockAcquire(&polar_flush_list_ctl->flushlist_lock);

	if (polar_flush_list_is_empty())
	{
		lsn = polar_max_valid_lsn();
		SpinLockRelease(&polar_flush_list_ctl->flushlist_lock);

		if (unlikely(polar_enable_debug))
			elog(DEBUG1,
				 "The flush list is empty, so use current insert lsn %X/%X as consistent lsn.",
				 (uint32)(lsn >> 32),
				 (uint32) lsn);

		return lsn;
	}

	Assert(polar_flush_list_ctl->first_flush_buffer >= 0);

	buf = GetBufferDescriptor(polar_flush_list_ctl->first_flush_buffer);
	lsn = pg_atomic_read_u64((pg_atomic_uint64 *) &buf->oldest_lsn);

	SpinLockRelease(&polar_flush_list_ctl->flushlist_lock);

	clsn = polar_copy_buffers_get_oldest_lsn();

	if (!XLogRecPtrIsInvalid(clsn))
		lsn = Min(lsn, clsn);

	return lsn;
}

bool
polar_buffer_can_be_flushed(BufferDesc *buf_hdr,
							XLogRecPtr oldest_apply_lsn,
							bool use_cbuf)
{
	uint32              buf_state;
	bool                res = false;
	XLogRecPtr          latest_lsn = BufferGetLSN(buf_hdr);
	CopyBufferDesc      *cbuf = NULL;

	if (!polar_enable_shared_storage_mode ||
		XLogRecPtrIsInvalid(oldest_apply_lsn) ||
		(latest_lsn <= oldest_apply_lsn) ||
		(buf_hdr->tag.forkNum == FSM_FORKNUM) ||
		(buf_hdr->tag.forkNum == INIT_FORKNUM) ||
		(polar_donot_control_vm_buffer(buf_hdr)))
		return true;
	else if (use_cbuf)
	{
		buf_state = LockBufHdr(buf_hdr);
		cbuf = buf_hdr->copy_buffer;

		if (cbuf)
			res = polar_copy_buffer_get_lsn(cbuf) <= oldest_apply_lsn;

		UnlockBufHdr(buf_hdr, buf_state);

		if (cbuf && !res)
			pg_atomic_add_fetch_u32(&cbuf->pass_count, 1);
	}

	return res;
}

/*
 * For lazy checkpoint, we only flush buffers that background writer does not
 * flush; for normal checkpoint, we check whether the buffer can be flushed,
 * buffers that can be flushed will be flushed by checkpoint.
 */
bool
polar_buffer_can_be_flushed_by_checkpoint(BufferDesc *buf_hdr,
										  XLogRecPtr oldest_apply_lsn,
										  int flags)
{
	if (flags & CHECKPOINT_LAZY)
	{
		if (buf_hdr->tag.forkNum == FSM_FORKNUM ||
			buf_hdr->tag.forkNum == INIT_FORKNUM)
			return true;
		else
			return false;
	}
	else
		return polar_buffer_can_be_flushed(buf_hdr, oldest_apply_lsn, false);
}

/* 
 * polar_buffer_sync - Sync buffers that are getting from flushlist.
 *
 * Returns true if it's appropriate for the bgwriter process to go into
 * low-power hibernation mode.
 */
bool
polar_buffer_sync(WritebackContext *wb_context,
				  XLogRecPtr *consistent_lsn,
				  bool is_normal_bgwriter,
				  int flags)
{
	static int       *batch_buf = NULL;

	XLogRecPtr cur_consistent_lsn;
	XLogRecPtr oldest_apply_lsn;
	int        num_written = 0;
	int        num_to_sync = 0;
	int        sync_count = 0;
	uint64     sleep_lag = 0;
	uint64     lag = 0;

	if (batch_buf == NULL)
		/* This is a static variable, do not need to free it explicitly. */
		batch_buf = (int *) malloc(polar_bgwriter_batch_size_flushlist * sizeof(int));

	cur_consistent_lsn = polar_get_consistent_lsn();
	sleep_lag = polar_bgwriter_sleep_lsn_lag * 1024 * 1024L;
	oldest_apply_lsn = polar_get_oldest_applied_lsn();

	if (XLogRecPtrIsInvalid(oldest_apply_lsn))
		lag = polar_max_valid_lsn() - cur_consistent_lsn;
	/* POLAR: when enable fullpage snapshot, we don't care about oledest_applied_lsn anymore */
	else if (polar_enable_fullpage_snapshot)
	{
		uint64	consistent_lsn_lag = polar_max_valid_lsn() - cur_consistent_lsn;
		/* If consistent_lsn is too old, start to trigger to write fullpage */
		if (consistent_lsn_lag > polar_fullpage_snapshot_oldest_lsn_delay_threshold)
			lag = consistent_lsn_lag - polar_fullpage_snapshot_oldest_lsn_delay_threshold;
		else
			lag = 0;
	}
	else
		lag = oldest_apply_lsn > cur_consistent_lsn ? oldest_apply_lsn - cur_consistent_lsn : 0;

	/* Normal background writer will create or close some parallel background workers. */
	polar_start_or_stop_parallel_bgwriter(is_normal_bgwriter, lag);

	/* Evaluate the number of buffers to sync. */
	num_to_sync = evaluate_sync_buffer_num(lag);

	/* Only normal bgwriter sync buffers from the copy buffer. */
	if (is_normal_bgwriter)
		polar_sync_buffer_from_copy_buffer(wb_context, flags);

	if (unlikely(polar_enable_debug))
		elog(DEBUG1, "Try to get %d buffers to flush from flushlist", num_to_sync);

	/* Make sure we can handle the pin inside SyncOneBuffer */
	ResourceOwnerEnlargeBuffers(CurrentResourceOwner);

	while (sync_count < num_to_sync)
	{
		int num = 0;
		int i = 0;

		/* Get buffers from flush list */
		num = polar_get_batch_flush_buffer(batch_buf);

		if (num == 0 || batch_buf == NULL)
			break;

		/* Sync buffers */
		while (i < num)
		{
			int sync_state = SyncOneBuffer(batch_buf[i], false, wb_context, flags);

			i++;
			if (sync_state & BUF_WRITTEN)
				num_written++;
		}

		sync_count += num;
	}

	BgWriterStats.m_buf_written_clean += num_written;

	/*
	 * Reset StrategyControl->numBufferAllocs and report buffer alloc counts to pgstat.
	 *
	 * If BgBufferSync() is enabled, we can't do it. We should let BgBufferSync() handle it.
	 * Because BgBufferSync() use StrategyControl->numBufferAllocs as an important parameter
	 * to calculate the counter of pages to be scanned/flushed.
	 */
	if (!polar_enable_normal_bgwriter)
	{
		uint32     recent_alloc = 0;
		StrategySyncStart(NULL, &recent_alloc);
		BgWriterStats.m_buf_alloc += recent_alloc;
	}

	*consistent_lsn = update_consistent_lsn_delta(cur_consistent_lsn);

	return lag < sleep_lag;
}

static XLogRecPtr
update_consistent_lsn_delta(XLogRecPtr cur_consistent_lsn)
{
	XLogRecPtr next_consistent_lsn;
	next_consistent_lsn = polar_cal_cur_consistent_lsn();
	consistent_lsn_delta = next_consistent_lsn - cur_consistent_lsn;

	return next_consistent_lsn;
}

/* Evaluate the number of buffers to sync. */
static int
evaluate_sync_buffer_num(uint64 lag)
{
	int        num_to_sync;
	double     sync_per_lsn;
	uint64     max_lag;
	double     cons_delta_per_worker;
	int        current_workers = 0;

	if (polar_parallel_bgwriter_enabled())
		current_workers = CURRENT_PARALLEL_WORKERS;

	cons_delta_per_worker = consistent_lsn_delta / (current_workers + 1);
	sync_per_lsn = (double) prev_sync_count / (cons_delta_per_worker + 1);

	/* Avoid overflow */
	max_lag = polar_bgwriter_max_batch_size / (sync_per_lsn + 1);

	if (lag > max_lag)
		num_to_sync = polar_bgwriter_max_batch_size;
	else
		num_to_sync = lag * sync_per_lsn;

	Assert(num_to_sync >= 0);

	if (num_to_sync == 0)
		num_to_sync = polar_bgwriter_batch_size_flushlist / 10 + 1;

	prev_sync_count = num_to_sync;

	return num_to_sync;
}

/* Normal background writer can start or stop some parallel background workers. */
static void
polar_start_or_stop_parallel_bgwriter(bool is_normal_bgwriter, uint64 lag)
{
	static TimestampTz last_check_start_tz = 0;
	static TimestampTz last_check_stop_tz = 0;

	bool        ok;
	int         at_most_workers;
	int         current_workers = 0;

	if (!is_normal_bgwriter || RecoveryInProgress() || lag <= 0 ||
		!polar_parallel_bgwriter_enabled() ||
		!polar_enable_dynamic_parallel_bgwriter)
		return;

	/*
	 * The consistent lsn does not be updated. It may be unhelpful to add or
	 * stop parallel writers, so we do nothing.
	 */
	if (consistent_lsn_delta == 0)
	{
		if (polar_enable_debug)
		{
			XLogRecPtr  consistent_lsn = polar_get_consistent_lsn();
			elog(LOG,
				 "Consistent lsn does not update, current consistent lsn is %X/%X",
				 (uint32) (consistent_lsn >> 32),
				 (uint32) consistent_lsn);
		}

		return;
	}

	Assert(consistent_lsn_delta > 0);
	current_workers = CURRENT_PARALLEL_WORKERS;
	at_most_workers = POLAR_MAX_BGWRITER_WORKERS;

	/* Try to start one parallel background writer */
	if (lag > (polar_parallel_new_bgwriter_threshold_lag * 1024 * 1024L))
	{
		/* Reset the last check stop timestamp */
		last_check_stop_tz = 0;

		if (current_workers >= at_most_workers)
		{
			if (polar_enable_debug)
				elog(LOG,
					 "Can not start new parallel background workers, current workers: %d, at most workers %d",
					 current_workers, at_most_workers);

			return;
		}

		if (last_check_start_tz == 0)
		{
			last_check_start_tz = GetCurrentTimestamp();
			return;
		}

		ok = TimestampDifferenceExceeds(last_check_start_tz,
										GetCurrentTimestamp(),
										polar_parallel_new_bgwriter_threshold_time * 1000L);

		/* Once start one */
		if (ok)
		{
			polar_register_parallel_bgwriter_workers(1, true);
			last_check_start_tz = 0;
		}
	}

	/* Try to stop one parallel background writer */
	else if (current_workers > polar_parallel_bgwriter_workers)
	{
		/* Reset the last check start timestamp */
		last_check_start_tz = 0;

		if (last_check_stop_tz == 0)
		{
			last_check_stop_tz = GetCurrentTimestamp();
			return;
		}

		/* Stop a background writer slowly, so wait more time. */
		ok = TimestampDifferenceExceeds(last_check_stop_tz,
										GetCurrentTimestamp(),
										polar_parallel_new_bgwriter_threshold_time *
										1000L * STOP_PARALLEL_BGWRITER_DELAY_FACTOR);

		/* Once stop one */
		if (ok)
		{
			polar_shutdown_parallel_bgwriter_workers(1);
			last_check_stop_tz = 0;
		}
	}
}

/*
 * polar_redo_set_buffer_oldest_lsn - Set the buffer oldest lsn when redo.
 *
 * When redo, we have a valid record, so we set buffer oldest lsn to
 * record->ReadRecPtr. Do not use record->EndRecPtr, because oldest lsn
 * is always the start of wal not the end.
 */
void
polar_redo_set_buffer_oldest_lsn(Buffer buffer, XLogRecPtr lsn)
{
	BufferDesc *buf_hdr;
	XLogRecPtr  oldest_lsn;

	if (!polar_flush_list_enabled())
		return;

	if (!BufferIsValid(buffer))
		elog(ERROR, "bad buffer ID: %d", buffer);

	buf_hdr = GetBufferDescriptor(buffer - 1);

	if (XLogRecPtrIsInvalid(lsn))
	{
		POLAR_LOG_BUFFER_DESC(buf_hdr);
		elog(PANIC, "Set invalid oldest lsn.");
	}

	Assert(BufferIsPinned(buffer));
	Assert(LWLockHeldByMeInMode(BufferDescriptorGetContentLock(buf_hdr), LW_EXCLUSIVE));

	oldest_lsn = polar_buffer_get_oldest_lsn(buf_hdr);

	/*
	 * For standby or master in recovery, it always call this function to set
	 * a valid oldest lsn.
	 */
	if (XLogRecPtrIsInvalid(oldest_lsn))
		polar_put_buffer_to_flush_list(buf_hdr, lsn);

	buf_hdr->recently_modified_count++;
}

/*
 * polar_set_buffer_fake_oldest_lsn - Set a fake oldest_lsn for the buffer.
 *
 * Since generating lsn and inserting buffer to flushlist is not atomic, when
 * call MarkBufferDirty, we set a fake lsn for buffer and put it into flushlist.
 * The fake lsn is from GetXLogInsertRecPtr, it is incremental.
 */
void
polar_set_buffer_fake_oldest_lsn(BufferDesc *buf_hdr)
{
	XLogRecPtr  oldest_lsn;
	bool        set = false;

	/*
	 * If server is in recovery, the buffer should be set a real lsn, so do not
	 * need to set a fake.
	 */
	if (!polar_flush_list_enabled() || (RecoveryInProgress()))
		return;

	Assert(BufferIsPinned(BufferDescriptorGetBuffer(buf_hdr)));

	/*
	 * POLAR: If ro promoted to be rw, then we will mark buffer dirty when read
	 * buffer from storage and replay xlog.
	 */
	Assert(LWLockHeldByMeInMode(BufferDescriptorGetContentLock(buf_hdr), LW_EXCLUSIVE));

	oldest_lsn = polar_buffer_get_oldest_lsn(buf_hdr);

	/*
	 * If oldest lsn is invalid, set a fake one and put buffer into flush list.
	 * If already has a valid oldest lsn, and it is first touched after copy,
	 * we also set one new fake oldest lsn.
	 */
	if (XLogRecPtrIsInvalid(oldest_lsn))
	{
		polar_put_buffer_to_flush_list(buf_hdr, InvalidXLogRecPtr);
		buf_hdr->polar_flags |= POLAR_BUF_OLDEST_LSN_IS_FAKE;
		set = true;
	}
	else if (polar_buffer_first_touch_after_copy(buf_hdr))
	{
		/* Adjust its position in the flush list */
		polar_adjust_position_in_flush_list(buf_hdr);

		/* First touch after copy done */
		buf_hdr->polar_flags |= POLAR_BUF_FIRST_TOUCHED_AFTER_COPY;
		set = true;
	}

	if (polar_enable_debug && set)
		POLAR_LOG_BUFFER_DESC(buf_hdr);

	buf_hdr->recently_modified_count++;
}

static void
polar_sync_buffer_from_copy_buffer(WritebackContext *wb_context, int flags)
{
	int            i;
	CopyBufferDesc *cbuf;
	int32          buf_id;

	if (!polar_enable_copy_buffer)
		return;

	/* Make sure we can handle the pin inside SyncOneBuffer */
	ResourceOwnerEnlargeBuffers(CurrentResourceOwner);

	for (i = 0; i < polar_copy_buffers; i++)
	{
		cbuf = polar_get_copy_buffer_descriptor(i);
		buf_id = pg_atomic_read_u32((pg_atomic_uint32 *) &cbuf->origin_buffer) - 1;

		if (buf_id < 0)
			continue;

		/*
		 * SyncOneBuffer try to flush the original buffer, if it can be flushed,
		 * flush it and free its copy buffer, otherwise, flush its copy buffer.
		 */
		SyncOneBuffer(buf_id, false, wb_context, flags);
	}
}

/*
 * In most scenarios, we always call MarkBufferDirty first, then call XLogInsert
 * to generate WAL and real lsn, so the buffer's oldest lsn will be set by
 * MarkBufferDirty. But there are always exceptions that call XLogInsert first
 * to generate WAL, then call MarkBufferDirty. The later MarkBufferDirty will
 * set a oldest lsn that greater than the real lsn, that will cause a wrong
 * consistent lsn, it is not as expected.
 *
 * So if we call XLogInsert first, we will call this function to set a fake oldest
 * lsn like MarkBufferDirty.
 */
void
polar_set_reg_buffer_oldest_lsn(Buffer buffer)
{
	BufferDesc *buf_hdr;
	XLogRecPtr  oldest_lsn;

	if (BufferIsInvalid(buffer))
		return;

	/*
	 * If server is in recovery, the buffer should be set a real lsn, so do not
	 * need to set a fake.
	 */
	if (!polar_flush_list_enabled() || (RecoveryInProgress()))
		return;

	buf_hdr = GetBufferDescriptor(buffer - 1);
	Assert(BufferIsPinned(buffer));

	/*
	 * Not all registered buffers hold the content exclusive lock, like
	 * hashbucketcleanup, if buffer does not hold the lock, we do not set
	 * its oldest lsn.
	 */
	if (!LWLockHeldByMeInMode(BufferDescriptorGetContentLock(buf_hdr),
							  LW_EXCLUSIVE))
		return;

	/*
	 * If buffer oldest lsn is not set or it is first touch after copy, we
	 * believe that the MarkBufferDirty is not called, it's time to set buffer
	 * oldest lsn and mark it dirty. If buffer oldest lsn is set, but it is
	 * not mark dirty, it will always stay in the flush list, that will block
	 * the consistent lsn.
	 */
	oldest_lsn = polar_buffer_get_oldest_lsn(buf_hdr);

	if (XLogRecPtrIsInvalid(oldest_lsn) ||
		polar_buffer_first_touch_after_copy(buf_hdr))
		MarkBufferDirty(buffer);
}

/*
 * Check whether we can use lazy checkpoint, if yes, set CHECKPOINT_LAZY and
 * set consistent lsn as lazy checkpoint redo lsn.
 */
bool
polar_check_lazy_checkpoint(bool shutdown, int *flags, XLogRecPtr *lazy_redo)
{
	bool is_lazy = false;
	XLogRecPtr consistent_lsn;

	if (!polar_lazy_checkpoint_enabled())
	{
		*flags &= ~CHECKPOINT_LAZY;
		return false;
	}

	/*
	 * polar can use lazy checkpoint for end of recovery, if we use
	 * lazy checkpoint, the end of recovery checkpoint will be ignored,
	 * we will create a normal lazy checkpoint, we set the lazy flag at
	 * StartupXLOG.
	 */
	if (*flags & CHECKPOINT_LAZY)
		is_lazy = true;
	/*
	 * for CHECKPOINT_CAUSE_XLOG or CHECKPOINT_CAUSE_TIME, we always use
	 * lazy checkpoint.
	 */
	else if (!shutdown && (*flags & (CHECKPOINT_CAUSE_TIME | CHECKPOINT_CAUSE_XLOG)))
		is_lazy = true;

	if (!is_lazy)
		return false;

	/*
	 * shutdown checkpoint must not be lazy. For lazy checkpoint, we use
	 * the consistent lsn as its redo lsn, so we should check whether the
	 * consistent lsn is valid, if not, reset the lazy flag and make a normal
	 * checkpoint.
	 */
	Assert(!shutdown || !is_lazy);

	consistent_lsn = polar_get_consistent_lsn();

	/* We have a valid consistent lsn, use it */
	if (!XLogRecPtrIsInvalid(consistent_lsn))
		*lazy_redo = consistent_lsn;
	else
	{
		/*
		 * Try to calculate the consistent lsn by myself, maybe bgwriter
		 * is not started, nobody update the consistent lsn.
		 */
		consistent_lsn = polar_cal_cur_consistent_lsn();

		if (!XLogRecPtrIsInvalid(consistent_lsn))
		{
			*lazy_redo = consistent_lsn;
			polar_set_consistent_lsn(consistent_lsn);
		}
		else
			is_lazy = false;
	}

	if (is_lazy)
	{
		*flags |= CHECKPOINT_LAZY;
		elog(LOG,
			 "Try to create lazy checkpoint, redo lsn is %X/%X",
			 (uint32)((*lazy_redo) >> 32),
			 (uint32) *lazy_redo);
	}
	else
		*flags &= ~CHECKPOINT_LAZY;

	return is_lazy;
}

bool
polar_pin_buffer(BufferDesc *buf_desc, BufferAccessStrategy strategy)
{
	return PinBuffer(buf_desc, strategy);
}

/*
 * POLAR: An extended version ConditionalLockBuffer. It can detect outdate status
 * before obtaining the lock.
 *
 * If fresh_check is True, it will try to check flag of corresponding buffer
 * descriptor and redo the buffer, otherwise it will do what the origin one do.
 */
bool
polar_conditional_lock_buffer_ext(Buffer buffer, bool fresh_check)
{
	BufferDesc  *buf_desc;
	bool        result;

	Assert(BufferIsValid(buffer));

	if (BufferIsLocal(buffer))
		return true;            /* act as though we got it */

	buf_desc = GetBufferDescriptor(buffer - 1);

	result = LWLockConditionalAcquire(BufferDescriptorGetContentLock(buf_desc),
									  LW_EXCLUSIVE);

	if (fresh_check && result)
		polar_log_index_lock_apply_buffer(&buffer);

	return result;
}

/*
 * POLAR: An extended version LockBuffer. It can detect outdate status
 * before obtaining the lock.
 *
 * If fresh_check is True, it will try to check flag of corresponding buffer
 * descriptor and do replay, otherwise it will do what the origin one do.
 */
void
polar_lock_buffer_ext(Buffer buffer, int mode, bool fresh_check)
{
	BufferDesc  *buf_desc;

	Assert(BufferIsValid(buffer));

	if (BufferIsLocal(buffer))
		return;                 /* local buffers need no lock */

	buf_desc = GetBufferDescriptor(buffer - 1);

	do
	{
		if (mode == BUFFER_LOCK_UNLOCK)
		{
			LWLockRelease(BufferDescriptorGetContentLock(buf_desc));
			return;
		}
		else if (mode == BUFFER_LOCK_SHARE)
			LWLockAcquire(BufferDescriptorGetContentLock(buf_desc), LW_SHARED);
		else if (mode == BUFFER_LOCK_EXCLUSIVE)
			LWLockAcquire(BufferDescriptorGetContentLock(buf_desc), LW_EXCLUSIVE);
		else
		{
			elog(ERROR, "unrecognized buffer lock mode: %d", mode);
			return;
		}

		/* Is this fresh buffer? */
		if (!fresh_check || !polar_redo_check_state(buf_desc, POLAR_REDO_OUTDATE))
			break;

		switch (mode)
		{
			case BUFFER_LOCK_SHARE:
				/* release s-lock and acquire x-lock for redo */
				LWLockRelease(BufferDescriptorGetContentLock(buf_desc));
				LWLockAcquire(BufferDescriptorGetContentLock(buf_desc), LW_EXCLUSIVE);
				break;

			case BUFFER_LOCK_EXCLUSIVE:
				break;

			default:
				elog(ERROR, "unrecognized buffer lock mode: %d", mode);
				return;
		}

		/*
		 * Now, we hold exclusive lock. Because we released lock
		 * while BUFFER_LOCK_SHARE mode, so we should re-check
		 * buffer outdate state.
		 */
		polar_log_index_lock_apply_buffer(&buffer);

		switch (mode)
		{
			case BUFFER_LOCK_SHARE:
				/*
				 * target lock mode is shared one, so we release exclusive lock
				 * and try to hold shared one
				 */
				LWLockRelease(BufferDescriptorGetContentLock(buf_desc));
				continue;

			case BUFFER_LOCK_EXCLUSIVE:
				/* target lock mode is exclusive, so we are done here, just return. */
				return;

			default:
				elog(ERROR, "unrecognized buffer lock mode: %d", mode);
				return;
		}
	}
	while (true);
}

bool
polar_is_future_page(BufferDesc *buf_hdr)
{
	XLogRecPtr replayed_lsn = GetXLogReplayRecPtr(NULL);
	Page page = BufferGetPage(BufferDescriptorGetBuffer(buf_hdr));

	if (!XLogRecPtrIsInvalid(replayed_lsn) &&
			PageGetLSN(page) > replayed_lsn &&
			buf_hdr->tag.forkNum == MAIN_FORKNUM)
	{
		if (!POLAR_ENABLE_FULLPAGE_SNAPSHOT())
			elog(FATAL, "Read a future page, page lsn = %lx, replayed_lsn = %lx, page_tag = '([%u, %u, %u]), %u, %u'",
					PageGetLSN(page), replayed_lsn, buf_hdr->tag.rnode.spcNode, buf_hdr->tag.rnode.dbNode,
					buf_hdr->tag.rnode.relNode, buf_hdr->tag.forkNum, buf_hdr->tag.blockNum);
		return true;
	}
	return false;
}

/*
 * POLAR: check buffer need write fullpage snapshot image
 */
bool
polar_buffer_need_fullpage_snapshot(BufferDesc *buf_hdr, XLogRecPtr oldest_apply_lsn)
{
	XLogRecPtr	cur_insert_lsn = InvalidXLogRecPtr;
	XLogRecPtr	buf_oldest_lsn = pg_atomic_read_u64((pg_atomic_uint64 *) &buf_hdr->oldest_lsn);
	XLogRecPtr	page_latest_lsn = BufferGetLSN(buf_hdr);
	CopyBufferDesc *copy_buf = NULL;
	uint32		buf_state = 0;
	int		replay_threshold = polar_fullpage_snapshot_replay_delay_threshold;
	int		oldest_lsn_threshold = polar_fullpage_snapshot_oldest_lsn_delay_threshold;
	int		min_modified_count = polar_fullpage_snapshot_min_modified_count;

	/*
	 * 1. if it's not a future page, quick check
	 * 2. only support main fork data page
	 */
	if (PageIsNew(BufHdrGetBlock(buf_hdr)) ||
		page_latest_lsn <= oldest_apply_lsn ||
		buf_hdr->tag.forkNum != MAIN_FORKNUM)
		return false;

	buf_state = LockBufHdr(buf_hdr);
	copy_buf = buf_hdr->copy_buffer;
	if (copy_buf)
	{
		Assert(!XLogRecPtrIsInvalid(buf_oldest_lsn));
		Assert(!XLogRecPtrIsInvalid(polar_copy_buffer_get_lsn(copy_buf)));
		buf_oldest_lsn = Min(pg_atomic_read_u64((pg_atomic_uint64 *) &copy_buf->oldest_lsn), buf_oldest_lsn);
	}
	UnlockBufHdr(buf_hdr, buf_state);

	if (!POLAR_ENABLE_FULLPAGE_SNAPSHOT() ||
		RecoveryInProgress() || /* Standby don't support fullpage snapshot */
		XLogRecPtrIsInvalid(oldest_apply_lsn) ||
		XLogRecPtrIsInvalid(buf_oldest_lsn))
		return false;

#define ONE_MB (1024 * 1024L)
	cur_insert_lsn = GetXLogInsertRecPtr();

	/*
	 * In following case togather, we need to write fullpage
	 * 1. buf_oldest_lsn is too old, block to advance consist_lsn
	 * 2. replica is too slow
	 * 3. is hot page
	 */
	if (cur_insert_lsn >= buf_oldest_lsn + oldest_lsn_threshold * ONE_MB &&
		(page_latest_lsn >= oldest_apply_lsn + replay_threshold * ONE_MB ||
		buf_hdr->recently_modified_count > min_modified_count))
		return true;

	return false;
}