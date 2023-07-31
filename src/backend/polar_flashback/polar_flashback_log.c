/*-------------------------------------------------------------------------
 *
 * polar_flashback_log.c
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, Alibaba Group Holding limited
 *
 * IDENTIFICATION
 *    src/backend/polar_flashback/polar_flashback_log.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "common/pg_lzcompress.h"
#include "miscadmin.h"
#include "polar_flashback/polar_flashback_log.h"
#include "polar_flashback/polar_flashback_log_file.h"
#include "polar_flashback/polar_flashback_log_index.h"
#include "polar_flashback/polar_flashback_log_worker.h"
#include "polar_flashback/polar_flashback_point.h"
#include "polar_flashback/polar_flashback_rel_filenode.h"
#include "postmaster/startup.h"
#include "storage/buf_internals.h"
#include "storage/bufpage.h"
#include "storage/bufmgr.h"
#include "storage/checksum.h"
#include "utils/guc.h"

/* Buffer size required to store a compressed version of origin page */
#define PGLZ_MAX_BLCKSZ PGLZ_MAX_OUTPUT(BLCKSZ)

/* GUCs */
bool polar_enable_flashback_log;
bool polar_has_partial_write;
bool polar_flashback_log_debug;
/* For the logindex */
int polar_flashback_logindex_mem_size;
int polar_flashback_logindex_bloom_blocks;
int polar_flashback_logindex_queue_buffers;
/* For the flashback log buffers */
int polar_flashback_log_buffers;
int polar_flashback_log_insert_locks;

flog_ctl_t flog_instance = NULL;

/*
 * POLAR: Remove all the flashback relative files contain log files and
 * logindex files. And keep the flashback logindex dir.
 */
void
polar_remove_all_flog_data(flog_ctl_t instance)
{
	logindex_snapshot_t snapshot = NULL;
	flog_buf_ctl_t buf_ctl = NULL;

	if (instance)
	{
		snapshot = instance->logindex_snapshot;
		buf_ctl = instance->buf_ctl;
	}

	polar_flog_index_remove_all(snapshot);
	polar_flog_remove_all(buf_ctl);
}

static inline Size
flog_ctl_size(void)
{
	return MAXALIGN(sizeof(flog_ctl_data_t));
}

/*
 * POLAR: Set the flashback log buffer state according the database state.
 *
 * NB: Only startup process will call the function, so it is lock free.
 */
static inline void
polar_set_flog_state(flog_ctl_t instance, uint32 state)
{
	pg_atomic_write_u32(&instance->state, state);
}

inline void
polar_flog_ctl_init_data(flog_ctl_t ctl)
{
	MemSet(ctl, 0, sizeof(flog_ctl_data_t));
	pg_atomic_init_u32(&ctl->state, FLOG_INIT);
}

static flog_ctl_t
polar_flog_ctl_init(const char *name)
{
	flog_ctl_t ctl;
	bool found;

	ctl = (flog_ctl_t)ShmemInitStruct(name, flog_ctl_size(), &found);

	if (!IsUnderPostmaster)
	{
		Assert(!found);
		polar_flog_ctl_init_data(ctl);
	}
	else
		Assert(found);

	return ctl;
}

static polar_flog_rec_ptr
cal_min_keep_flog_ptr(logindex_snapshot_t snapshot, polar_flog_rec_ptr ptr)
{
	polar_flog_rec_ptr keep_ptr;

	keep_ptr = polar_get_flog_index_meta_max_ptr(snapshot);
	keep_ptr = keep_ptr < ptr ? keep_ptr : ptr;
	return keep_ptr;
}

/*
 * POLAR: Remove the flashback log and logindex after the flashback log record pointer.
 *
 * NB: Must keep the flashback log for flashback logindex.
 */
static void
polar_remove_flog_data(flog_ctl_t instance, polar_flog_rec_ptr ptr)
{
	polar_flog_rec_ptr keep_ptr;

	keep_ptr = cal_min_keep_flog_ptr(instance->logindex_snapshot, ptr);
	polar_logindex_truncate(instance->logindex_snapshot, keep_ptr);
	polar_truncate_flog_before(instance->buf_ctl, keep_ptr);
}

/*
 * POLAR: Flush all the flashback data and update control file.
 *
 * NB: The flashback log must have been flushed before flush the buffer, so we don't
 * need to flush the flashback log in this function. If need flush all the flashback
 * log, we can call polar_flashback_log_flush.
 *
 * We don't need a precise value of flashback log point after checkpoint, but
 * a little larger value. When the database in crash recovery mode, ignore the error
 * flashback log after the value.
 * And we can't find the previous point of the checkpoint end point, but it is
 * fine and can be updated in the shutdown checkpoint.
 */
static void
polar_flush_flog_data(flog_buf_ctl_t ctl, polar_flog_rec_ptr ckp_start, bool shutdown)
{
	polar_flog_rec_ptr ckp_end;
	polar_flog_rec_ptr ckp_end_prev = POLAR_INVALID_FLOG_REC_PTR;

	if (shutdown)
	{
		ckp_end =
			polar_get_curr_flog_ptr(ctl, &ckp_end_prev);
		/*
		 * Flush all the flashback log.
		 *
		 * NB: In the RW, this work has been done in buffer flushed to disk
		 * while the buffer isn't always flushed in shutdown restartpoint of standby.
		 */
		polar_flog_flush(ctl, ckp_end);
		ctl->buf_state = FLOG_BUF_SHUTDOWNED;
	}
	else
		ckp_end = polar_get_flog_write_result(ctl);

	if (polar_flashback_log_debug)
		elog(LOG, "The point of flashback log in the checkpoint end is %X/%X",
				(uint32) (ckp_end >> 32), (uint32) ckp_end);

	if (!FLOG_REC_PTR_IS_INVAILD(ckp_end))
		polar_flush_fbpoint_info(ctl, ckp_start,
								  ckp_end, ckp_end_prev);
}

void
set_buf_flog_state(BufferDesc *buf_desc, uint32 flog_state)
{
	uint32 state;

	state = polar_lock_redo_state(buf_desc);
	state |= flog_state;
	polar_unlock_redo_state(buf_desc, state);
}

void
clean_buf_flog_state(BufferDesc *buf_hdr, uint32 flog_state)
{
	uint32 state;

	state = polar_lock_redo_state(buf_hdr);
	state &= ~(flog_state);
	polar_unlock_redo_state(buf_hdr, state);
}

inline bool
polar_is_flog_mem_enabled(void)
{
	return polar_enable_flashback_log && !polar_is_datamax() && !IsBootstrapProcessingMode();
}

/*
 * POLAR: Is the flashback log enabled?
 */
inline bool
polar_is_flog_enabled(flog_ctl_t instance)
{
	return instance && !polar_in_replica_mode();
}

inline bool
polar_is_flog_ready(flog_ctl_t instance)
{
	return pg_atomic_read_u32(&instance->state) == FLOG_READY;
}

/*
 * POLAR: Is the flashback log startup?
 *
 * The state will larger than FLOG_INIT after call polar_startup_flog.
 * The startup action contain:
 * 1. read the control file and fill the flashback log buf_ctl info.
 * 2. call polar_logindex_snapshot_init to do logindex snapshot init.
 */
inline bool
polar_has_flog_startup(flog_ctl_t instance)
{
	return pg_atomic_read_u32(&instance->state) > FLOG_INIT;
}

/*
 * POLAR: Get the flashback log relative memory internal function.
 */
Size
polar_flog_shmem_size_internal(int insert_locks_num, int log_buffers, int logindex_mem_size,
							   int logindex_bloom_blocks, int queue_buffers_MB)
{
	Size size = 0;

	size = add_size(size, flog_ctl_size());
	size = add_size(size, polar_flog_buf_size(insert_locks_num, log_buffers));
	size = add_size(size, polar_flog_index_shmem_size(logindex_mem_size, logindex_bloom_blocks));
	size = add_size(size, polar_flog_index_queue_shmem_size(queue_buffers_MB));
	size = add_size(size, polar_flog_async_list_shmem_size());
	return size;
}

/*
 * POLAR: Initialization of shared memory for flashback log internal function
 */
flog_ctl_t
polar_flog_shmem_init_internal(const char *name, int insert_locks_num,
							   int log_buffers, int logindex_mem_size, int logindex_bloom_blocks, int queue_buffers_MB)
{
	flog_ctl_t ctl;

	ctl = polar_flog_ctl_init(name);
	ctl->list_ctl = polar_flog_async_list_init(name);
	ctl->buf_ctl = polar_flog_buf_init(name, insert_locks_num, log_buffers);
	ctl->logindex_snapshot = polar_flog_index_shmem_init(name, logindex_mem_size,
														 logindex_bloom_blocks, ctl->buf_ctl);
	ctl->queue_ctl = polar_flog_index_queue_shmem_init(name, queue_buffers_MB);
	return ctl;
}

/*
 * POLAR: Do something after the flashback point is done.
 *
 * 1. Flush the flashback log.
 * 2. remove the old flashback log.
 */
void
polar_flog_do_fbpoint(flog_ctl_t instance, polar_flog_rec_ptr ckp_start,
		polar_flog_rec_ptr keep_ptr, bool shutdown)
{
	flog_buf_ctl_t buf_ctl = instance->buf_ctl;

	polar_flush_flog_data(buf_ctl, ckp_start, shutdown);
	polar_remove_flog_data(instance, keep_ptr);
}

/*
 * POLAR: Is the buffer flashback log enabled?
 *
 * Now we will disable the flashback log for the buffer while repair its
 * partial problem or flashback it.
 */
bool
polar_is_buf_flog_enabled(flog_ctl_t instance, Buffer buf)
{
	BufferDesc *buf_hdr;

	buf_hdr = GetBufferDescriptor(buf - 1);
	return polar_is_flog_enabled(flog_instance) &&
			!POLAR_CHECK_BUF_FLOG_STATE(buf_hdr, POLAR_BUF_FLOG_DISABLE);
}

/*
 * POLAR: Is the page need a flashback log.
 *
 * instance: flog_instance.
 * forkno: The forkno of the page, now just MAIN_FORKNUM fork need a flashback log.
 * page: The page
 * is_premanent: is a permanent page?
 * fbpoint_lsn: The WAL redo point of the flashback point, just vaild in insertxlog.
 * redo_lsn: The end+1 to replay, use it in the standby recovery mode or ro online promote.
 */
bool
polar_is_flog_needed(flog_ctl_t flog_ins, polar_logindex_redo_ctl_t redo_ins,
		ForkNumber forkno, Page page, bool is_permanent,
		XLogRecPtr fbpoint_lsn, XLogRecPtr redo_lsn)
{
	Assert(flog_ins);

	if (!POLAR_IS_NEED_FLOG(forkno) || !is_permanent)
		return false;

	if (redo_lsn != InvalidXLogRecPtr)
	{
		Assert(fbpoint_lsn == InvalidXLogRecPtr);

		fbpoint_lsn = polar_get_curr_fbpoint_lsn(flog_ins->buf_ctl);

		if (POLAR_IN_PARALLEL_REPLAY_STANDBY_MODE(redo_ins) && redo_lsn <= fbpoint_lsn)
			fbpoint_lsn = polar_get_prior_fbpoint_lsn(flog_ins->buf_ctl);

		if (redo_lsn <= fbpoint_lsn)
			return false;
	}

	return  polar_is_page_first_modified(flog_ins->buf_ctl, PageGetLSN(page), fbpoint_lsn);
}

/*
 * POLAR: Insert into flashback log.
 *
 * buf:         The buffer id.
 * is_candidate:Is a candidate or not?
 * is_recovery:	Is in recovery.
 *
 * NB: Now we can get the origin page in memory in the recovery mode, so just insert it
 * to flashback log shared buffer to avoid to read from disk which may cause more time to
 * recovery. In the product mode, just insert into the flashback log async list.
 */
void
polar_flog_insert(flog_ctl_t instance, Buffer buf, bool is_candidate, bool is_recovery)
{
	Assert(BufferIsValid(buf));

	/* Add the buffer to origin buffer in the recovery mode */
	if (is_recovery)
	{
		BufferDesc *buf_desc = GetBufferDescriptor(buf - 1);

		polar_add_origin_buf(instance->list_ctl, buf_desc);
	}

	polar_push_buf_to_flog_list(instance->list_ctl, instance->buf_ctl, buf, is_candidate);
}

/*
 * POLAR: Check the origin page before the page with a full page image wal record
 * is a empty page.
 *
 * NB: When the page is registered by log_newpage, the origin page before it must be a empty page.
 */
void
polar_check_fpi_origin_page(RelFileNode rnode, ForkNumber forkno, BlockNumber block, uint8 xl_info)
{
	BufferTag tag;
	PGAlignedBlock  page_tmp;

	if (xl_info != XLOG_FPI_FOR_HINT)
	{
		INIT_BUFFERTAG(tag, rnode, forkno, block);
		read_origin_page_from_file(&tag, page_tmp.data);

		if (!PageIsNew(page_tmp.data) && !polar_page_is_just_inited(page_tmp.data))
			/*no cover line*/
			elog(PANIC, "The page " POLAR_LOG_BUFFER_TAG_FORMAT " has a full page image wal record, "
					"but its origin page is not a empty page", rnode.spcNode, rnode.dbNode,
					rnode.relNode, forkno, block);
	}
}

/*
 * POLAR: Flush the flashback log record of the buffer.
 *
 * invalidate: Invalidate the buffer?
 *
 * When we drop the relation or database, the buffer is invalidate and its flashback log is
 * unnecessary.
 */
void
polar_flush_buf_flog_rec(BufferDesc *buf_hdr, flog_ctl_t instance, bool invalidate)
{
	if (!polar_is_flog_enabled(instance))
		return;

	if (IS_BUF_IN_FLOG_LIST(buf_hdr))
		polar_process_buf_flog_list(instance, buf_hdr, false, invalidate);

	polar_flush_buf_flog(instance->list_ctl, instance->buf_ctl, buf_hdr, invalidate);
}

/*
 * POLAR: startup all about flashback log.
 *
 * state: the database state.
 * instance: the flashback log instance.
 *
 * NB: It will switch to a new file when the flashback log is in crash recovery.
 */
void
polar_startup_flog(CheckPoint *checkpoint, flog_ctl_t instance)
{
	/* Validate directory and startup the flashback log and flashback logindex in the rw and standby */
	logindex_snapshot_t snapshot = instance->logindex_snapshot;
	flog_buf_ctl_t buf_ctl = instance->buf_ctl;

	Assert(polar_is_flog_enabled(instance));
	polar_validate_flog_dir(buf_ctl);
	polar_validate_flog_index_dir(snapshot);

	/* Remove all the temporary flashback log files */
	polar_remove_tmp_flog_file(buf_ctl->dir);

	polar_startup_flog_buf(buf_ctl, checkpoint);
	polar_startup_flog_index(snapshot,
							 VALID_FLOG_PTR(polar_get_fbpoint_start_ptr(buf_ctl)));

	if (buf_ctl->buf_state == FLOG_BUF_READY)
		polar_set_flog_state(instance, FLOG_READY);
	else
	{
		/*
		 * Recover the flashback log buffer, so we can insert flashback log
		 * in the startup process without wait.
		 */
		polar_recover_flog_buf(instance);
		polar_set_flog_state(instance, FLOG_STARTUP);
	}
}

/*
 * POLAR: Recover the flashback log.
 *
 * Set the flashback log record write result and initalize_upto,
 * insert->curr_pos.
 *
 * We don't care about the flashback logindex here.
 */
void
polar_recover_flog_buf(flog_ctl_t instance)
{
	flog_buf_ctl_t  buf_ctl = instance->buf_ctl;
	flog_ctl_insert *insert = &buf_ctl->insert;
	polar_flog_rec_ptr ckp_end_ptr = POLAR_INVALID_FLOG_REC_PTR;
	polar_flog_rec_ptr ptr = POLAR_INVALID_FLOG_REC_PTR;
	polar_flog_rec_ptr prev_ptr = POLAR_INVALID_FLOG_REC_PTR;
	polar_flog_rec_ptr pos;
	polar_flog_rec_ptr prev_pos;
	polar_flog_rec_ptr block_end_ptr;
	uint64    seg_no = 0;

	LWLockAcquire(&buf_ctl->ctl_file_lock, LW_SHARED);
	ckp_end_ptr = buf_ctl->fbpoint_info.flog_end_ptr;
	prev_ptr = buf_ctl->fbpoint_info.flog_end_ptr_prev;
	seg_no = buf_ctl->max_seg_no;
	LWLockRelease(&buf_ctl->ctl_file_lock);

	Assert(prev_ptr <= ckp_end_ptr);

	/* If there is no flashback log record, just return */
	if (seg_no == POLAR_INVALID_FLOG_SEGNO)
	{
		buf_ctl->buf_state = FLOG_BUF_READY;
		polar_set_flog_state(instance, FLOG_READY);
		elog(LOG, "There is no flashback log data, so just skip the recovery of"
			 " the flashback log and logindex");
		return;
	}

	polar_log_flog_buf_state(buf_ctl->buf_state);

	/*
	 * Note the previous pointer is not correct. Its expected value
	 * is start of a valid flashback log record, but it is a value bigger
	 * than ckp_end_ptr. It is nothing serious, we just process this case
	 * in the reader function.
	 */
	if (buf_ctl->buf_state == FLOG_BUF_SHUTDOWN_RECOVERY)
		ptr = ckp_end_ptr;
	else if (buf_ctl->buf_state == FLOG_BUF_CRASH_RECOVERY)
	{
		ptr = (seg_no + 1) * POLAR_FLOG_SEG_SIZE;
		prev_ptr = POLAR_INVALID_FLOG_REC_PTR;

		Assert(ptr % POLAR_FLOG_BLCKSZ == 0);

		/* Write the history and control file */
		if (ptr != ckp_end_ptr)
		{
			/*
			 * Must be inside CRIT_SECTION.
			 * The shared memory has been updated already, so we must
			 * write the file successfully.
			 */
			START_CRIT_SECTION();

			polar_write_flog_history_file(buf_ctl->dir, buf_ctl->tli, ckp_end_ptr, ptr);

			/* Change the flashback log control checkpoint info and write file */
			LWLockAcquire(&buf_ctl->ctl_file_lock, LW_SHARED);
			buf_ctl->fbpoint_info.flog_end_ptr = ptr;
			buf_ctl->fbpoint_info.flog_end_ptr_prev = prev_ptr;
			LWLockRelease(&buf_ctl->ctl_file_lock);

			END_CRIT_SECTION();

			elog(LOG, "The flashback log will switch from %X/%X to %X/%X",
				 (uint32)(ckp_end_ptr >> 32), (uint32)ckp_end_ptr,
				 (uint32)(ptr >> 32), (uint32)ptr);
		}
	}
	else
		/*no cover line*/
		ereport(PANIC, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("The invalid flashback log buffer recovery state: %u", buf_ctl->buf_state)));

	/* Write control file, update the flashback state area */
	LWLockAcquire(&buf_ctl->ctl_file_lock, LW_EXCLUSIVE);
	polar_write_flog_ctl_file(buf_ctl);
	LWLockRelease(&buf_ctl->ctl_file_lock);

	/*
	 * When the buffer is in shutdown recovery mode, copy the
	 * last block to the buffer.
	 *
	 * NB: When it is in crash recovery mode, the ptr % POLAR_FLASHBACK_LOG_BLCKSZ
	 * must be zero.
	 */
	if (ptr % POLAR_FLOG_BLCKSZ != 0)
	{
		char       *page;
		int         len;
		int         first_idx;
		polar_flog_rec_ptr  page_begin_ptr;

		page_begin_ptr = ptr - (ptr % POLAR_FLOG_BLCKSZ);
		first_idx = FLOG_PTR2BUF_IDX(ptr, buf_ctl);

		/* Copy the valid part of the last block, and zero the rest */
		page = &buf_ctl->pages[first_idx * POLAR_FLOG_BLCKSZ];
		len = ptr % POLAR_FLOG_BLCKSZ;
		polar_flog_read(page, POLAR_FLOG_SEG_SIZE, page_begin_ptr,
						POLAR_FLOG_BLCKSZ, buf_ctl->dir);
		MemSet(page + len, 0, POLAR_FLOG_BLCKSZ - len);
		block_end_ptr = page_begin_ptr + POLAR_FLOG_BLCKSZ;
		buf_ctl->blocks[first_idx] = block_end_ptr;
	}
	else
		block_end_ptr = ptr;

	pos = polar_flog_ptr2pos(ptr);
	/*
	 * The prev_pos may be not right after crash recovery. So we ignore the
	 * check in some cases.
	 */
	prev_pos = polar_flog_ptr2pos(prev_ptr);

	SpinLockAcquire(&buf_ctl->info_lck);
	buf_ctl->write_result = ptr;
	buf_ctl->initalized_upto = block_end_ptr;
	buf_ctl->write_request = ptr;
	SpinLockRelease(&buf_ctl->info_lck);

	SpinLockAcquire(&insert->insertpos_lck);
	insert->curr_pos = pos;
	insert->prev_pos = prev_pos;
	SpinLockRelease(&insert->insertpos_lck);
	buf_ctl->buf_state = FLOG_BUF_READY;
	buf_ctl->min_recover_lsn = ptr;

	elog(LOG, "The flashback log shared buffer is ready now, the current point(position)"
				 " is %X/%X(%X/%X), previous point(position) is %X/%X(%X/%X), initalized upto point is"
				 " %X/%X", (uint32)(ptr >> 32), (uint32)(ptr), (uint32)(pos >> 32), (uint32)(pos),
				 (uint32)(prev_ptr >> 32), (uint32)(prev_ptr),
				 (uint32)(prev_pos >> 32), (uint32)(prev_pos),
				 (uint32)(block_end_ptr >> 32), (uint32)(block_end_ptr));

	Assert(polar_flog_pos2endptr(pos) == ptr ||
		   polar_flog_pos2ptr(pos) == ptr);
	Assert(polar_flog_pos2endptr(prev_pos) == prev_ptr ||
		   polar_flog_pos2ptr(prev_pos) == prev_ptr);
}

/*
 * POLAR: Set the buffer has been flashback log lost checked.
 */
void
polar_set_buf_flog_lost_checked(flog_ctl_t flog_ins,
		polar_logindex_redo_ctl_t redo_ins, Buffer buffer)
{
	BufferDesc *buf_desc;

	buf_desc = GetBufferDescriptor(buffer - 1);

	if (polar_is_flog_enabled(flog_ins) &&
			polar_get_bg_redo_state(redo_ins) == POLAR_BG_ONLINE_PROMOTE)
		set_buf_flog_state(buf_desc, POLAR_BUF_FLOG_LOST_CHECKED);
}

/*
 * POLAR: Mark the buffer lost some flashback log records in the online promote.
 *
 * We can't protect the flashback log to lose nothing in the rw crash,
 * so we must do something to recheck in the online promote.
 * Because the flashback log flush to disk before the page, so the buffer whose
 * page lsn is larger than max page lsn in the disk may lose some flashback log.
 * We think the max page lsn in the disk is the max value of current flashback point lsn
 * and primary consist lsn. The pages whose lsn less than the value must have been
 * flushed to disk already.
 */
bool
polar_may_buf_lost_flog(flog_ctl_t flog_ins, polar_logindex_redo_ctl_t redo_ins,
		BufferDesc *buf_desc)
{
	static XLogRecPtr max_page_lsn_in_disk = InvalidXLogRecPtr;
	static XLogRecPtr replay_end_lsn = InvalidXLogRecPtr;
	XLogRecPtr page_lsn;

	if (!polar_is_flog_enabled(flog_ins))
		return false;

	/* Only used by online promote */
	if (polar_get_bg_redo_state(redo_ins) != POLAR_BG_ONLINE_PROMOTE)
		return false;

	/* The buffer have been checked already */
	if (POLAR_CHECK_BUF_FLOG_STATE(buf_desc, POLAR_BUF_FLOG_LOST_CHECKED))
		return false;

	page_lsn = BufferGetLSN(buf_desc);

	/* The max page lsn in the disk is fixed in the online promote */
	if (max_page_lsn_in_disk == InvalidXLogRecPtr)
		max_page_lsn_in_disk = Max(polar_get_local_fbpoint_lsn(flog_ins->buf_ctl,
				InvalidXLogRecPtr, InvalidXLogRecPtr), polar_get_primary_consist_ptr());

	if (replay_end_lsn == InvalidXLogRecPtr)
		replay_end_lsn = GetXLogReplayRecPtr(NULL);

	/*
	 * Note this: the page whose lsn is equal to consist_ptr may be not flushed.
	 */
	if (page_lsn < max_page_lsn_in_disk)
		return false;
	else if (page_lsn <= replay_end_lsn)
	{
		set_buf_flog_state(buf_desc, POLAR_BUF_FLOG_LOST_CHECKED);
		return true;
	}

	return false;
}

/*
 * The flashback log must be flushed before the buffer evicted.
 */
void
polar_make_true_no_flog(flog_ctl_t instance, BufferDesc *buf)
{
	if (!polar_is_flog_enabled(instance))
		return;

	if (IS_BUF_IN_FLOG_LIST(buf))
	{
		/*no cover line*/
		elog(PANIC, "The buffer %d " POLAR_LOG_BUFFER_TAG_FORMAT " will be evicted "
			"but there is flashback log record of it not flushed", buf->buf_id,
			POLAR_LOG_BUFFER_TAG(&(buf->tag)));
	}
}

/* Recover the flashback log, now just recover the logindex */
void
polar_recover_flog(flog_ctl_t instance)
{
	polar_recover_flog_index(instance->logindex_snapshot, instance->queue_ctl, instance->buf_ctl);
	polar_set_flog_state(instance, FLOG_READY);
}

void
polar_get_buffer_tag_in_flog_rec(flog_record *rec, BufferTag *tag)
{
	switch (rec->xl_rmid)
	{
		case ORIGIN_PAGE_ID:
			INIT_BUFFERTAG(*tag, FL_GET_ORIGIN_PAGE_REC_DATA(rec)->tag.rnode,
					FL_GET_ORIGIN_PAGE_REC_DATA(rec)->tag.forkNum,
					FL_GET_ORIGIN_PAGE_REC_DATA(rec)->tag.blockNum);
			break;

		case ORIGIN_PAGE_FULL:
			INIT_BUFFERTAG(*tag, FL_GET_FILENODE_REC_DATA(rec)->new_filenode, FILENODE_FORK, 0);
			break;

		default:
			/*no cover line*/
			elog(ERROR, "Unknown flashback log record rmid %d", rec->xl_rmid);
	}
}

/* POLAR: Just crash will cause partial write */
static inline bool
may_be_partial_write(void)
{
	return AmStartupProcess() && !reachedConsistency;
}

/*
 * POLAR: Write the repaired buffer.
 *
 * NB: The buffer must be invaild, so write it without
 * any lock is safe.
 */
static void
write_repaired_buf(BufferDesc *buf)
{
	SMgrRelation reln;
	Block       buf_block;
	char       *buf_write;

	Assert(pg_atomic_read_u32(&buf->polar_redo_state) & POLAR_BUF_FLOG_DISABLE);

	buf_block = BufHdrGetBlock(buf);
	buf_write = PageEncryptCopy((Page) buf_block, buf->tag.forkNum,
								buf->tag.blockNum);
	buf_write = PageSetChecksumCopy((Page) buf_write, buf->tag.blockNum);
	reln = smgropen(buf->tag.rnode, InvalidBackendId);
	smgrwrite(reln,
			  buf->tag.forkNum,
			  buf->tag.blockNum,
			  buf_write,
			  false);
}

/*
 * POLAR: Get origin page to solve partial write problem.
 *
 * instance: The flashback log instance.
 * buf: The target buffer.
 * tag: The buffer tag.
 */
static bool
get_origin_page_for_partial_write(flog_ctl_t instance, Buffer *buf, BufferTag *tag)
{
	flshbak_buf_context_t context;
	bool found = false;
	flog_reader_state * reader;

	/* Allocate a flashback log reader */
	FLOG_ALLOC_PAGE_READER(reader, instance->buf_ctl, ERROR);

	INIT_FLSHBAK_BUF_CONTEXT(context, polar_get_fbpoint_start_ptr(instance->buf_ctl),
			polar_get_flog_write_result(instance->buf_ctl),
			polar_get_curr_fbpoint_lsn(instance->buf_ctl), GetRedoRecPtr(),
			instance->logindex_snapshot, reader, tag, *buf, ERROR, true);

	found = polar_flashback_buffer(&context);

	polar_flog_reader_free(reader);
	return found;
}

/*
 * POLAR: The flashback log can repair the PERMANENT buffer
 * when it meet a partial write.
 */
bool
polar_can_flog_repair(flog_ctl_t instance, BufferDesc *buf_hdr, bool has_redo_action)
{
	uint32  buf_state;

	if (!polar_is_flog_enabled(instance))
		return false;

	if (may_be_partial_write() || has_redo_action)
	{
		buf_state = pg_atomic_read_u32(&buf_hdr->state);
		return buf_state & BM_PERMANENT;
	}

	return false;
}

/*
 * To repair the partial write problem.
 * Partial write problem will occur in three scenarios:
 * 1. RW crash recovery.
 * 2. Standby crash recovery.
 * 3. RO to RW online promote.
 */
void
polar_repair_partial_write(flog_ctl_t instance, BufferDesc *bufHdr)
{
	BufferTag *tag = &bufHdr->tag;
	Buffer buf;

	Assert((pg_atomic_read_u32(&bufHdr->state) & BM_VALID) == 0);
	buf = bufHdr->buf_id + 1;

	/* Wait for the flashback logindex ready */
	while (!polar_is_flog_ready(instance))
	{
		/* Handle interrupt signals of startup process to avoid hang */
		if (AmStartupProcess())
			HandleStartupProcInterrupts();
		else
			CHECK_FOR_INTERRUPTS();

		pg_usleep(1000L);
	}

	if (!get_origin_page_for_partial_write(instance, &buf, tag))
	{
		/*no cover line*/
		elog(ERROR, "Can't find a valid origin page for " POLAR_LOG_BUFFER_TAG_FORMAT " from flashback log",
				POLAR_LOG_BUFFER_TAG(tag));
	}
	else
	{
		/* Flush the buffer to protect the next first modify after checkpoint. */
		write_repaired_buf(bufHdr);

		elog(LOG, "The page " POLAR_LOG_BUFFER_TAG_FORMAT " has been repaired by flashback log",
				POLAR_LOG_BUFFER_TAG(tag));
	}
}

static void
log_flog_rec(flog_record *record, polar_flog_rec_ptr ptr)
{
#define MAX_EXTRA_INFO_SIZE 512

	char extra_info[MAX_EXTRA_INFO_SIZE];
	fl_origin_page_rec_data *origin_page_rec;
	fl_filenode_rec_data_t *filenode_rec;

	switch (record->xl_rmid)
	{
		case ORIGIN_PAGE_ID:
			origin_page_rec = FL_GET_ORIGIN_PAGE_REC_DATA(record);
			snprintf(extra_info, MAX_EXTRA_INFO_SIZE, "It is a origin page record, "
					 "the origin page is %s page. The redo lsn of the origin page is %X/%X, "
					 "the page tag is " POLAR_LOG_BUFFER_TAG_FORMAT,
					 (record->xl_info == ORIGIN_PAGE_EMPTY ? "empty" : "not empty"),
					 (uint32)(origin_page_rec->redo_lsn >> 32), (uint32)(origin_page_rec->redo_lsn),
					 POLAR_LOG_BUFFER_TAG(&(origin_page_rec->tag)));
			break;
		case REL_FILENODE_ID:
			filenode_rec = FL_GET_FILENODE_REC_DATA(record);
			snprintf(extra_info, MAX_EXTRA_INFO_SIZE, "It is a relation file node change record, "
					"the origin relation file node for [%u, %u, %u] is [%u, %u, %u] before %s",
					filenode_rec->new_filenode.spcNode, filenode_rec->new_filenode.dbNode,
					filenode_rec->new_filenode.relNode,
					filenode_rec->old_filenode.spcNode, filenode_rec->old_filenode.dbNode,
					filenode_rec->old_filenode.relNode, timestamptz_to_str(filenode_rec->time));
			break;
		default:
			/*no cover begin*/
			elog(ERROR, "The type of the record %X/%08X is wrong\n",
				 (uint32)(ptr >> 32), (uint32)ptr);
			break;
			/*no cover end*/
	}

	elog(LOG, "Insert a flashback log record at %X/%X: total length is %u, "
		 "the previous pointer is %X/%X. %s",
		 (uint32)(ptr >> 32), (uint32)ptr, record->xl_tot_len,
		 (uint32)(record->xl_prev >> 32), (uint32)(record->xl_prev), extra_info);
}

static uint32
get_origin_page_rec_len(uint32 xl_tot_len, flog_insert_context *insert_context,
		fl_rec_img_header *b_img, fl_rec_img_comp_header *cb_img, char *data)
{
	Page page;
	uint32 result = xl_tot_len;

	page = (Page) insert_context->data;
	result += FL_ORIGIN_PAGE_REC_INFO_SIZE;

	/* Process the unempty origin page record */
	if (page && !PageIsNew(page) && !polar_page_is_just_inited(page))
	{
		BlockNumber block_num;
		bool need_checksum_again;
		uint16 data_len = BLCKSZ;
		bool from_origin_buf = false;
		uint16      lower;
		uint16      upper;

		need_checksum_again = from_origin_buf = insert_context->info & FROM_ORIGIN_BUF;
		block_num = insert_context->buf_tag->blockNum;

		/* Assume we can omit data between pd_lower and pd_upper */
		lower = ((PageHeader) page)->pd_lower;
		upper = ((PageHeader) page)->pd_upper;

		if (lower >= SizeOfPageHeaderData &&
				upper > lower &&
				upper <= BLCKSZ)
		{
			b_img->hole_offset = lower;
			b_img->bimg_info |= IMAGE_HAS_HOLE;
			cb_img->hole_length = upper - lower;
			/*
			 * Check the checksum before compute it again, so we will
			 * not change the rightness of checksum.
			 *
			 * When it is from origin buffer, the checksum may be wrong,
			 * so we don't check the pages from origin page buffer.
			 *
			 * We do nothing while the checksum is wrong here, but
			 * the decoder will verify the page.
			 */
			if ((!from_origin_buf) && DataChecksumsEnabled())
				need_checksum_again =
					(pg_checksum_page((char *) page, block_num) == ((PageHeader) page)->pd_checksum);
		}
		else
		{
			/* No "hole" to compress out */
			b_img->hole_offset = 0;
			cb_img->hole_length = 0;
		}

		/* Clean the hole */
		MemSet((char *)page + b_img->hole_offset, 0, cb_img->hole_length);

		/* Compute checksum again */
		if (need_checksum_again && DataChecksumsEnabled())
			((PageHeader) page)->pd_checksum = pg_checksum_page((char *) page, block_num);

		/* Try to compress flashback log */
		if (polar_compress_block_in_log(page, b_img->hole_offset, cb_img->hole_length,
				data, &data_len, FL_REC_IMG_COMP_HEADER_SIZE))
		{
			b_img->bimg_info |= IMAGE_IS_COMPRESSED;
			b_img->length = data_len;

			if (cb_img->hole_length != 0)
				result += FL_REC_IMG_COMP_HEADER_SIZE;
		}
		else
			b_img->length = BLCKSZ - cb_img->hole_length;

		result += FL_REC_IMG_HEADER_SIZE + b_img->length;
	}

	/* The empty origin page record just a fl_origin_page_rec_data */
	return result;
}

static flog_record *
assemble_origin_page_rec(flog_insert_context *insert_context, uint32 xl_tot_len)
{
	fl_rec_img_header b_img = {0, 0, 0};
	fl_rec_img_comp_header cb_img = {0};
	char data[PGLZ_MAX_BLCKSZ];
	fl_origin_page_rec_data rec_data;
	flog_record *rec;
	char    *scratch;

	Assert(insert_context->rmgr == ORIGIN_PAGE_ID);
	xl_tot_len = get_origin_page_rec_len(xl_tot_len, insert_context, &b_img, &cb_img, data);

	/* Construct the flashback log record */
	rec = polar_palloc_in_crit(xl_tot_len);
	rec->xl_tot_len = xl_tot_len;
	rec->xl_info = insert_context->info;

	/* Copy the record data for the origin page. */
	rec_data.redo_lsn = insert_context->redo_lsn;
	INIT_BUFFERTAG(rec_data.tag, insert_context->buf_tag->rnode,
			insert_context->buf_tag->forkNum, insert_context->buf_tag->blockNum);
	scratch = (char *)rec + FLOG_REC_HEADER_SIZE;
	memcpy(scratch, &rec_data, FL_ORIGIN_PAGE_REC_INFO_SIZE);
	scratch += FL_ORIGIN_PAGE_REC_INFO_SIZE;

	/* An empty origin page record */
	if (b_img.length == 0)
	{
		Assert(xl_tot_len == (FLOG_REC_HEADER_SIZE + FL_ORIGIN_PAGE_REC_INFO_SIZE));
		rec->xl_info = ORIGIN_PAGE_EMPTY;
	}
	else
	{
		Assert(xl_tot_len >= FLOG_REC_HEADER_SIZE + FL_ORIGIN_PAGE_REC_INFO_SIZE +
			   FL_REC_IMG_HEADER_SIZE + b_img.length);

		rec->xl_info = ORIGIN_PAGE_FULL;
		memcpy(scratch, &b_img, FL_REC_IMG_HEADER_SIZE);
		scratch += FL_REC_IMG_HEADER_SIZE;

		/* Process compressed one */
		if (b_img.bimg_info & IMAGE_IS_COMPRESSED)
		{
			if (cb_img.hole_length != 0)
			{
				memcpy(scratch, &cb_img, FL_REC_IMG_COMP_HEADER_SIZE);
				scratch += FL_REC_IMG_COMP_HEADER_SIZE;
				Assert(xl_tot_len == FLOG_REC_HEADER_SIZE + FL_ORIGIN_PAGE_REC_INFO_SIZE +
					   FL_REC_IMG_HEADER_SIZE + FL_REC_IMG_COMP_HEADER_SIZE + b_img.length);
			}

			memcpy(scratch, data, b_img.length);
		}
		else
		{
			Assert(xl_tot_len == FLOG_REC_HEADER_SIZE + FL_ORIGIN_PAGE_REC_INFO_SIZE +
				   FL_REC_IMG_HEADER_SIZE + b_img.length);

			if (cb_img.hole_length != 0)
			{
				Assert(b_img.length < BLCKSZ);
				Assert(b_img.hole_offset >= SizeOfPageHeaderData);

				memcpy(scratch, (char *) (insert_context->data), b_img.hole_offset);
				scratch += b_img.hole_offset;
				memcpy(scratch, (char *) (insert_context->data) + b_img.hole_offset + cb_img.hole_length,
					   b_img.length - b_img.hole_offset);
			}
			else
			{
				Assert(b_img.length == BLCKSZ);
				memcpy(scratch, (char *) (insert_context->data), b_img.length);
			}
		}
	}

	return rec;
}

/*
 * Assemble a flashback log record from the buffers into an
 * polar_flashback_log_record, ready for insertion with
 * polar_flashback_log_insert_record().
 *
 * tag is the buffer tag of buffer which is a origin page.
 * redo_ptr is the last checkpoint XLOG record pointer.
 *
 * Return the flashback log record which palloc in this function.
 * The caller can switch the memory context and pfree.
 *
 * The record will be contain the (compressed) block data.
 * The record header fields are filled in, except for the CRC field and
 * previous pointer.
 *
 */
static flog_record *
flog_rec_assemble(flog_insert_context *insert_context)
{
	flog_record *rec;
	uint32 xl_tot_len = FLOG_REC_HEADER_SIZE;

	if (insert_context->rmgr == ORIGIN_PAGE_ID)
		rec = assemble_origin_page_rec(insert_context, xl_tot_len);
	else if (insert_context->rmgr == REL_FILENODE_ID)
		rec = polar_assemble_filenode_rec(insert_context, xl_tot_len);
	else
		/*no cover line*/
		elog(PANIC, "Unknown flashback log record rmid %d", insert_context->rmgr);

	/* Set the common part */
	rec->xl_prev = POLAR_INVALID_FLOG_REC_PTR;
	rec->xl_rmid = insert_context->rmgr;
	rec->xl_xid = 0;

	return rec;
}

/*
 * POLAR: Insert a flashback log record to flashback log shared buffer.
 *
 * insert_context: Everything about the insertion.
 */
polar_flog_rec_ptr
polar_flog_insert_into_buffer(flog_ctl_t instance, flog_insert_context *insert_context)
{
	polar_flog_rec_ptr start_ptr = POLAR_INVALID_FLOG_REC_PTR;
	polar_flog_rec_ptr end_ptr = POLAR_INVALID_FLOG_REC_PTR;
	flog_record *rec = NULL;

	/* Assemble the flashback log record without the previous pointer and the CRC field */
	rec = flog_rec_assemble(insert_context);
	/*
	 * Fill the previous pointer and the CRC field and insert the flashback log record to flashback log
	 * shared buffers.
	 */
	end_ptr = polar_flog_rec_insert(instance->buf_ctl, instance->queue_ctl, rec, &start_ptr);

	if (unlikely(polar_flashback_log_debug))
		log_flog_rec(rec, start_ptr);

	pfree(rec);

	return end_ptr;
}

polar_flog_rec_ptr
polar_insert_buf_flog_rec(flog_ctl_t instance, BufferTag *tag, XLogRecPtr redo_lsn,
		XLogRecPtr fbpoint_lsn, uint8 info, Page origin_page, bool from_origin_buf)
{
	flog_insert_context insert_context;

	/*
	 * It is candidate, check it in here.
	 */
	if ((info & FLOG_LIST_SLOT_CANDIDATE) && PageGetLSN(origin_page) > fbpoint_lsn)
		return POLAR_INVALID_FLOG_REC_PTR;

	Assert(PageGetLSN(origin_page) <= fbpoint_lsn);

	/* Construct the insert context */
	insert_context.buf_tag = tag;
	insert_context.data = origin_page;
	insert_context.redo_lsn = redo_lsn;
	insert_context.rmgr = ORIGIN_PAGE_ID;
	/* This will be update in the flashback log record */
	insert_context.info = ORIGIN_PAGE_FULL;

	if (from_origin_buf)
		insert_context.info |= FROM_ORIGIN_BUF;

	return polar_flog_insert_into_buffer(instance, &insert_context);
}

static polar_flog_rec_ptr
polar_insert_rel_extend_flog_rec(flog_ctl_t instance, Buffer buffer)
{
	flog_insert_context insert_context;
	XLogRecPtr redo_lsn;

	/* The buffer is just extended, so the redo_lsn of the record is just the wal lsn in the disk */
	redo_lsn = GetFlushRecPtr();

	/* Construct the insert context */
	insert_context.buf_tag = &(GetBufferDescriptor(buffer - 1)->tag);
	insert_context.data = NULL;
	insert_context.redo_lsn = redo_lsn;
	insert_context.rmgr = ORIGIN_PAGE_ID;
	/* This will be update in the flashback log record */
	insert_context.info = ORIGIN_PAGE_EMPTY;

	return polar_flog_insert_into_buffer(instance, &insert_context);
}

void
polar_flog_rel_bulk_extend(flog_ctl_t instance, Buffer buffer)
{
	polar_flog_rec_ptr ptr;

	Assert(polar_is_flog_enabled(instance));

	ptr = polar_insert_rel_extend_flog_rec(instance, buffer);
	/* Flush it right now to avoid to miss it */
	polar_flog_flush(instance->buf_ctl, ptr);
}

/*
 * POLAR: Get the origin page from flashback log.
 * Return true when we get a right origin page.
 *
 * context: contain the start point and end point of flashback log to search and the target buffer tag.
 * Page: The target page.
 * replay_start_lsn: The replay start WAL lsn.
 *
 * NB: Please make true the context->end_ptr larger than context->start_ptr.
 *
 */
bool
polar_get_origin_page(flshbak_buf_context_t *context, Page page, XLogRecPtr *replay_start_lsn)
{

	log_index_page_iter_t originpage_iter;
	log_index_lsn_t *lsn_info = NULL;
	polar_flog_rec_ptr ptr;
	bool found = false;

	Assert(context->start_ptr < context->end_ptr);

	originpage_iter =
		polar_logindex_create_page_iterator(context->logindex_snapshot, context->tag, context->start_ptr, context->end_ptr - 1, false);

	if (polar_logindex_page_iterator_state(originpage_iter) != ITERATE_STATE_FINISHED)
	{
		/*no cover begin*/
		polar_logindex_release_page_iterator(originpage_iter);
		elog(ERROR, "Failed to iterate data for flashback log of " POLAR_LOG_BUFFER_TAG_FORMAT
				", which start pointer =%X/%X and end pointer =%X/%X",
				POLAR_LOG_BUFFER_TAG(context->tag),
				(uint32)((context->start_ptr) >> 32), (uint32)(context->start_ptr),
				(uint32)((context->end_ptr - 1) >> 32), (uint32)(context->end_ptr - 1));
		return false;
		/*no cover end*/
	}

	if ((lsn_info = polar_logindex_page_iterator_next(originpage_iter)) != NULL)
	{
		ptr = (polar_flog_rec_ptr) lsn_info->lsn;
		Assert(BUFFERTAGS_EQUAL(*(lsn_info->tag), *(context->tag)));
		found = polar_decode_origin_page_rec(context->reader, ptr, page, replay_start_lsn, context->tag);
	}

	polar_logindex_release_page_iterator(originpage_iter);

	if (!found)
	{
		/*no cover line*/
		elog(LOG, "Can't find a valid origin page for page " POLAR_LOG_BUFFER_TAG_FORMAT
			 " with flashback log start location %X/%X and end location %X/%X",
			 POLAR_LOG_BUFFER_TAG(context->tag),
			 (uint32)((context->start_ptr) >> 32), (uint32)(context->start_ptr),
			 (uint32)((context->end_ptr - 1) >> 32), (uint32)(context->end_ptr - 1));
	}
	else if (unlikely(polar_flashback_log_debug))
	{
		*replay_start_lsn = Max(*replay_start_lsn, PageGetLSN(page));
		elog(LOG, "We find a valid origin page for page " POLAR_LOG_BUFFER_TAG_FORMAT
			 " with flashback log start location %X/%X and end location %X/%X, its "
			 "WAL replay start lsn is %X/%X", POLAR_LOG_BUFFER_TAG(context->tag),
			 (uint32)((context->start_ptr) >> 32), (uint32)(context->start_ptr),
			 (uint32)((context->end_ptr - 1) >> 32), (uint32)(context->end_ptr - 1),
			 (uint32)(*replay_start_lsn >> 32), (uint32)(*replay_start_lsn));
	}

	return found;
}

/*
 * POLAR: Flashback the buffer.
 *
 * context: everything we need. You can see the detail in its definition.
 *
 * NB: Please make sure context->end_ptr >= context->start_ptr.
 */
bool
polar_flashback_buffer(flshbak_buf_context_t *context)
{
	Page page;
	XLogRecPtr lsn = InvalidXLogRecPtr;
	BufferDesc *buf_desc;

	if (unlikely(context->start_ptr == context->end_ptr))
	{
		elog(WARNING, "The page " POLAR_LOG_BUFFER_TAG_FORMAT " has no origin page between "
				"flashback log %X/%X and %X/%X", POLAR_LOG_BUFFER_TAG(context->tag),
				(uint32) (context->start_ptr >> 32), (uint32) (context->start_ptr),
				(uint32) (context->end_ptr >> 32), (uint32) context->end_ptr);
		return false;
	}
	else if (unlikely(context->start_ptr > context->end_ptr))
	{
		/*no cover begin*/
		elog(ERROR, "The range to flashback page " POLAR_LOG_BUFFER_TAG_FORMAT " is wrong, "
				"the flashback log start pointer %X/%X, the flashback log end pointer %X/%X",
				POLAR_LOG_BUFFER_TAG(context->tag),
				(uint32) (context->start_ptr >> 32), (uint32) (context->start_ptr),
				(uint32) (context->end_ptr >> 32), (uint32) (context->end_ptr));
		/*no cover end*/
	}

	/* Disable the flashback log for the buffer in the flashback. */
	buf_desc = GetBufferDescriptor(context->buf - 1);
	set_buf_flog_state(buf_desc, POLAR_BUF_FLOG_DISABLE);

	page = BufferGetPage(context->buf);

	if (!polar_get_origin_page(context, page, &lsn))
	{
		/* If not found, check its first modify is a XLOG_FPI_MULTI/XLOG_FPI/XLOG_FPI_FOR_HINT record? */
		lsn = polar_logindex_find_first_fpi(polar_logindex_redo_instance,
						context->start_lsn, context->end_lsn, context->tag, &(context->buf), context->apply_fpi);

		if (!XLogRecPtrIsInvalid(lsn))
		{
			elog(LOG, "The first modify of " POLAR_LOG_BUFFER_TAG_FORMAT " after %X/%X "
					"is a new full page image, its origin page is a empty page or the image",
					POLAR_LOG_BUFFER_TAG(context->tag),
					(uint32) (context->start_lsn >> 32), (uint32) (context->start_lsn));

			if (context->apply_fpi)
				lsn = PageGetLSN(page);
			else
				MemSet(page, 0, BLCKSZ);
		}
		else
		{
			elog(context->elevel, "Can't find a valid origin page for " POLAR_LOG_BUFFER_TAG_FORMAT " from flashback log",
					POLAR_LOG_BUFFER_TAG(context->tag));
			return false;
		}
	}

	Assert(!XLogRecPtrIsInvalid(lsn));

	if (unlikely(polar_flashback_log_debug))
	{
		elog(LOG, "The origin page " POLAR_LOG_BUFFER_TAG_FORMAT " need to replay from %X/%X to %X/%X",
				POLAR_LOG_BUFFER_TAG(context->tag),
				(uint32) (lsn >> 32), (uint32) lsn,
				(uint32) (context->end_lsn >> 32), (uint32) (context->end_lsn));
	}

	/* The lsn can be larger than or equal to context->end_lsn, it means no WAL record to replay */
	polar_logindex_apply_page(polar_logindex_redo_instance, lsn, context->end_lsn, context->tag, &(context->buf));
	return true;
}

