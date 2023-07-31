/*-------------------------------------------------------------------------
 *
 * polar_flashback_log_index.c
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, Alibaba Group Holding limited
 *
 * IDENTIFICATION
 *    src/backend/polar_flashback/polar_flashback_log_index.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/polar_queue_manager.h"
#include "access/xlog.h"
#include "miscadmin.h"
#include "polar_flashback/polar_flashback_log.h"
#include "polar_flashback/polar_flashback_log_file.h"
#include "polar_flashback/polar_flashback_log_index.h"
#include "polar_flashback/polar_flashback_log_mem.h"
#include "polar_flashback/polar_flashback_log_reader.h"
#include "polar_flashback/polar_flashback_log_record.h"
#include "postmaster/startup.h"
#include "storage/lwlock.h"

#define FLOG_GET_INDEX_DIR_FULL_PATH(path, snapshot) \
	polar_make_file_path_level2(path, get_flog_index_dir(snapshot))

/*
 * Get the flashback log index directory.
 *
 * NB: When the flashback log is unenable, we will
 * return the default one which will be used by remove
 * the flashback logindex dir.
 */
static char *
get_flog_index_dir(logindex_snapshot_t snapshot)
{
	if (snapshot)
		return snapshot->dir;
	else
		return POLAR_FL_INDEX_DEFAULT_DIR;
}

/*
 * POLAR: Insert one flashback logindex lsn_info.
 * NB: The caller must hold a EXCLUSIVE lock or just one process
 * and ensure flashback_logindex_snapshot is not NULL.
 */
static void
polar_flog_index_add_lsn(logindex_snapshot_t snapshot, BufferTag *tag, polar_flog_rec_ptr prev,
						 polar_flog_rec_ptr ptr)
{
	polar_logindex_add_lsn(snapshot, tag, prev, (XLogRecPtr)ptr);

	if (unlikely(polar_flashback_log_debug))
	{
		elog(LOG, "Add flashback logindex: tag is " POLAR_LOG_BUFFER_TAG_FORMAT " , lsn is %X/%X, "
				"previous lsn is %X/%X", POLAR_LOG_BUFFER_TAG(tag), (uint32)(ptr >> 32),
				(uint32)(ptr), (uint32)(prev >> 32), (uint32)(prev));
	}
}

/*
 * Read lsn info <tag, lsn, prev_lsn> from flashback log files and insert into
 * the logindex.
 */
static bool
insert_flog_index_from_file(logindex_snapshot_t snapshot, flog_reader_state *state, polar_flog_rec_ptr *ptr_expected)
{
	char *errormsg = NULL;
	flog_record *record;

	/* Read the flashback log record */
	record = polar_read_flog_record(state, *ptr_expected, &errormsg);

	if (record != NULL)
	{
		BufferTag tag;

		polar_get_buffer_tag_in_flog_rec(record, &tag);

		/* The add lsn can ignore the first insert which is inserted already */
		polar_flog_index_add_lsn(snapshot, &tag, POLAR_INVALID_FLOG_REC_PTR, state->read_rec_ptr);

		*ptr_expected = convert_to_first_valid_ptr(state->end_rec_ptr);

		if (unlikely(polar_flashback_log_debug))
			elog(LOG, "We found the flashback log record at %X/%X from file, "
				 "total length is %u, the tag is " POLAR_LOG_BUFFER_TAG_FORMAT,
				 (uint32)(*ptr_expected >> 32), (uint32)(*ptr_expected), record->xl_tot_len,
				 POLAR_LOG_BUFFER_TAG(&tag));
	}
	/*
	 * Ignore the switch point invalid flashback log and skip to next ptr.
	 *
	 * NB: Change the lsn_info->lsn to zero to flag it is success without
	 * insert to logidnex.
	 */
	else if (state->in_switch_region)
		*ptr_expected = state->end_rec_ptr;
	/* Ignore the error when read the record which is over the write result. */
	else if (strncmp(errormsg, REC_UNFLUSHED_ERROR_MSG, strlen(REC_UNFLUSHED_ERROR_MSG)) == 0)
		return false;
	else
		/*no cover line*/
		elog(PANIC, "Failed to read record %X/%08X from flashback log file with error: %s",
			 (uint32)(*ptr_expected >> 32), (uint32)*ptr_expected, errormsg);

	/* The max end lsn is the next one */
	polar_set_logindex_max_parsed_lsn(snapshot, *ptr_expected);
	return true;
}

static bool
insert_flog_index_from_queue(logindex_snapshot_t snapshot, polar_ringbuf_ref_t *ref, polar_flog_rec_ptr *ptr_expected, polar_flog_rec_ptr max_ptr)
{
	BufferTag tag;
	uint32 log_len = 0;

	CLEAR_BUFFERTAG(tag);

	if (polar_flog_read_info_from_queue(ref, *ptr_expected, &tag, &log_len, max_ptr))
	{
		polar_flog_rec_ptr next_ptr;

		next_ptr = polar_get_next_flog_ptr(*ptr_expected, log_len);
		polar_flog_index_add_lsn(snapshot, &tag, POLAR_INVALID_FLOG_REC_PTR, *ptr_expected);
		polar_set_logindex_max_parsed_lsn(snapshot, next_ptr);
		*ptr_expected = next_ptr;
		return true;
	}
	/* The last record is contrecord, set max end lsn to max_ptr */
	else if (log_len)
		polar_set_logindex_max_parsed_lsn(snapshot, max_ptr);

	return false;
}

Size
polar_flog_index_shmem_size(int logindex_mem_size, int logindex_bloom_blocks)
{
	Size shmem_size = 0;

	Assert(polar_flashback_logindex_mem_size > 0);
	shmem_size = add_size(shmem_size,
						  polar_logindex_shmem_size(polar_logindex_convert_mem_tbl_size(logindex_mem_size),
													logindex_bloom_blocks));
	return shmem_size;
}

/*
 * The the lsn in the flashback logindex larger than write result
 * of flashback log can be flushed.
 */
bool
polar_flog_index_table_flushable(struct log_mem_table_t *table, void *data)
{
	return polar_get_flog_write_result((flog_buf_ctl_t) data) >= (polar_flog_rec_ptr)(polar_logindex_mem_table_max_lsn(table));
}

logindex_snapshot_t
polar_flog_index_shmem_init(const char *name, int logindex_mem_size, int logindex_bloom_blocks, void *extra_data)
{
	logindex_snapshot_t snapshot;
	char logindex_name[FL_OBJ_MAX_NAME_LEN];

	Assert(polar_flashback_logindex_mem_size > 0);
	/* Memory init for flashback logindex snapshot. */
	snprintf(logindex_name, FL_OBJ_MAX_NAME_LEN, "%s%s", name, FL_LOGINDEX_SUFFIX);
	snapshot = polar_logindex_snapshot_shmem_init(logindex_name,
												  polar_logindex_convert_mem_tbl_size(logindex_mem_size),
												  logindex_bloom_blocks, LWTRANCHE_FLOG_LOGINDEX_BEGIN,
												  LWTRANCHE_FLOG_LOGINDEX_END, polar_flog_index_table_flushable, extra_data);
	return snapshot;
}

/* validate the flashback logindex dir */
void
polar_validate_flog_index_dir(logindex_snapshot_t snapshot)
{
	char path[MAXPGPATH];

	FLOG_GET_INDEX_DIR_FULL_PATH(path, snapshot);
	polar_validate_dir(path);
}

/* POLAR: Remove all the flashback logindex files and keep the flashback logindex dir. */
void
polar_flog_index_remove_all(logindex_snapshot_t snapshot)
{
	char        path[MAXPGPATH];

	FLOG_GET_INDEX_DIR_FULL_PATH(path, snapshot);
	polar_flog_clean_dir_internal(path);
}

/*
 * POLAR: Startup the flashback logindex.
 *
 * NB: Must get the flashback log buffer checkpoint location first.
 */
void
polar_startup_flog_index(logindex_snapshot_t snapshot, polar_flog_rec_ptr checkpoint_ptr)
{
	log_index_meta_t *meta;

	/* Create a new logindex meta file when there is no one. */
	meta = &snapshot->meta;

	if (!log_index_get_meta(snapshot, meta))
		polar_log_index_write_meta(snapshot, meta, false);

	if (!polar_logindex_check_state(snapshot, POLAR_LOGINDEX_STATE_INITIALIZED))
		polar_logindex_snapshot_init(snapshot, checkpoint_ptr, false);
}

polar_flog_rec_ptr
polar_get_flog_index_max_ptr(logindex_snapshot_t snapshot)
{
	return (polar_flog_rec_ptr)polar_get_logindex_snapshot_max_lsn(snapshot);
}

/*
 * POLAR: Insert the flashback logindex to the max_ptr.
 * Read the flashback logindex info from logindex queue or
 * flashback log file and insert it into flashback logindex.
 *
 * max_ptr: the max pointer of flashback log index can be inserted.
 * source : LOG_FILE or LOGINDEX_QUEUE or ANY, ANY will use LOGINDEX_QUEUE
 * first and then LOG_FILE if failed.
 * flog_dir : the directory of the flashback log.
 *
 * NB: the max_ptr must be a multiple of POLAR_FLOG_BLCKSZ, when is_background is true
 * while the reader will cache the block in the reader_buf.
 */
void
polar_flog_index_insert(logindex_snapshot_t snapshot, flog_index_queue_ctl_t queue_ctl,
		flog_buf_ctl_t buf_ctl, polar_flog_rec_ptr max_ptr, flashback_log_source source)
{
#define QUEUE_REF_NAME "flog_index_insert_bg"
#define NEED_READER(s) \
	(s == LOG_FILE || s == ANY)
#define NEED_QUEUE_REF(s) \
	(s == LOGINDEX_QUEUE || s == ANY)
#define NEED_REFRESH(s) \
	(s == NONE)

	static flog_reader_state *state = NULL;
	static polar_ringbuf_ref_t ref = { .slot = -1};
	/* The start pointer of next flashback log record. */
	static polar_flog_rec_ptr insert_ptr = POLAR_INVALID_FLOG_REC_PTR;
	bool inserted = true;
	bool insert_something = false;
	uint64        read_from_queue_rec_num = 0;
	uint64        read_from_file_rec_num = 0;

	Assert(snapshot);

	/* Need to refresh the queue, now just for test */
	if (unlikely(NEED_REFRESH(source)))
	{
		state = NULL;
		ref.slot = -1;
		insert_ptr = POLAR_INVALID_FLOG_REC_PTR;
		return;
	}

	/* If the logindex is empty, the next ptr is FLOG_LONG_PHD_SIZE*/
	if (FLOG_REC_PTR_IS_INVAILD(insert_ptr))
		insert_ptr = VALID_FLOG_PTR(polar_get_flog_index_max_ptr(snapshot));

	/* There is nothing to do, just return */
	if (insert_ptr >= max_ptr)
		return;

	if (unlikely(!queue_ctl))
		source = LOG_FILE;

	if (NEED_QUEUE_REF(source))
	{
		/* Init the reference for logindex queue */
		if (unlikely(ref.slot == -1))
			POLAR_XLOG_QUEUE_NEW_REF(&ref, queue_ctl->queue, false, QUEUE_REF_NAME);

		/*
		 * 1. If it's weak reference, try to promote to strong reference.
		 * 2. If reference is evicted, then create a new weak reference
		 *    and promote new weak reference to strong reference.
		 */
		while (!ref.strong && !polar_ringbuf_get_ref(&ref))
			/*no cover line*/
			POLAR_XLOG_QUEUE_NEW_REF(&ref, queue_ctl->queue, false, ref.ref_name);
	}

	if (NEED_READER(source) && state == NULL)
		FLOG_ALLOC_PAGE_READER(state, buf_ctl, PANIC);

	/* Pop the flashback log lsn_info from logindex queue or read it from log files */
	while (insert_ptr < max_ptr && inserted)
	{
		if (unlikely(polar_flashback_log_debug))
		{
			elog(LOG, "The flashback log will be insert into logindex is record at "
				 "%X/%X", (uint32)(insert_ptr >> 32),
				 (uint32)insert_ptr);
		}

		switch (source)
		{
			case LOGINDEX_QUEUE:
				inserted = insert_flog_index_from_queue(snapshot, &ref, &insert_ptr, max_ptr);

				if (inserted)
					read_from_queue_rec_num++;

				break;

			case LOG_FILE:
				inserted = insert_flog_index_from_file(snapshot, state, &insert_ptr);

				if (inserted)
					read_from_file_rec_num++;

				break;

			case ANY:
				inserted = insert_flog_index_from_queue(snapshot, &ref, &insert_ptr, max_ptr);

				if (!inserted)
				{
					inserted = insert_flog_index_from_file(snapshot, state, &insert_ptr);

					if(inserted)
						read_from_file_rec_num++;
				}
				else
					read_from_queue_rec_num++;

				break;

			default:
				/*no cover begin*/
				elog(PANIC, "The source of flashback log is unknown.");
				break;
				/*no cover end*/
		}

		insert_something = true;
	}

	if (likely(queue_ctl))
		polar_update_flog_index_queue_stat(queue_ctl->queue_stat, read_from_file_rec_num, read_from_queue_rec_num);

	if (source == LOGINDEX_QUEUE || source == ANY)
	{
		if (!polar_ringbuf_clear_ref(&ref))
			/*no cover line*/
			elog(PANIC, "Failed to clear flashback logindex queue reference");
	}

	if (unlikely(polar_flashback_log_debug && insert_something))
		elog(LOG, "The flashback log have been inserted into logindex at %X/%X expected location at "
			 "%X/%X, read from queue %lu times, read from file %lu times",
			 (uint32)(insert_ptr << 32), (uint32)insert_ptr,
			 (uint32)(max_ptr << 32), (uint32)max_ptr,
			 read_from_queue_rec_num, read_from_file_rec_num);

	/* Don't need to release the reader */
}

polar_flog_rec_ptr
polar_get_flog_index_meta_max_ptr(logindex_snapshot_t snapshot)
{
	return (polar_flog_rec_ptr)(snapshot->meta.max_lsn);
}

/* Recovery the flashback logindex */
void
polar_recover_flog_index(logindex_snapshot_t snapshot, flog_index_queue_ctl_t queue_ctl, flog_buf_ctl_t buf_ctl)
{
	Assert(snapshot);
	polar_flog_index_insert(snapshot, queue_ctl, buf_ctl, buf_ctl->min_recover_lsn, LOG_FILE);

	elog(LOG, "Recover the flashback logindex to %X/%X",
		 (uint32) (buf_ctl->min_recover_lsn >> 32), (uint32) buf_ctl->min_recover_lsn);
}
