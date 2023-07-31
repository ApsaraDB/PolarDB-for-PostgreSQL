/*-------------------------------------------------------------------------
 *
 * test_flashback_log.c
 *
 *
 * Copyright (c) 2020-2120, Alibaba-inc PolarDB Group
 *
 * IDENTIFICATION
 *	  src/test/modules/test_flashback_log/test_flashback_log.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/polar_logindex_redo.h"
#include "access/polar_ringbuf.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "catalog/pg_class.h"
#include "catalog/pg_tablespace_d.h"
#include "catalog/pg_type_d.h"
#include "catalog/pg_extension_d.h"
#include "catalog/pg_cast_d.h"
#include "catalog/pg_aggregate_d.h"
#include "catalog/pg_database_d.h"
#include "catalog/pg_am_d.h"
#include "miscadmin.h"
#include "polar_flashback/polar_flashback_log.h"
#include "polar_flashback/polar_flashback_log_file.h"
#include "polar_flashback/polar_flashback_log_index.h"
#include "polar_flashback/polar_flashback_log_index_queue.h"
#include "polar_flashback/polar_flashback_log_internal.h"
#include "polar_flashback/polar_flashback_log_list.h"
#include "polar_flashback/polar_flashback_log_mem.h"
#include "polar_flashback/polar_flashback_log_reader.h"
#include "polar_flashback/polar_flashback_log_record.h"
#include "polar_flashback/polar_flashback_log_worker.h"
#include "polar_flashback/polar_flashback_point.h"
#include "postmaster/bgworker.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/buf_internals.h"
#include "storage/checksum.h"
#include "storage/ipc.h"
#include "storage/pg_shmem.h"

PG_MODULE_MAGIC;

#define TEST_FLOG_NAME "testflog"
#define TEST_FLOG_INSERT_LOCKS 2
#define TEST_FLOG_BUFS 10
#define TEST_FLOG_INDEX_MEM_SIZE 3
#define TEST_FLOG_INDEX_BLOOM_BLKS 8
#define TEST_FLOG_QUEUE_BUFS 1
#define QUEUE_PKT_SIZE FLOG_INDEX_QUEUE_PKT_SIZE(FLOG_INDEX_QUEUE_DATA_SIZE)
#define FLOG_REC_EMPTY_PAGE_SIZE (FLOG_REC_HEADER_SIZE + FL_ORIGIN_PAGE_REC_INFO_SIZE)
#define TEST_FLOG_PREALLOC_FILE_NUM 2

#define TEST_BUF_LIST_NUM 6

/* Saved hook values in case of unload */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static int insert_empty_page_rec_num = 0;

static const Oid test_relnodes[TEST_BUF_LIST_NUM] =
{
	TypeRelationId,
	TableSpaceRelationId,
	CastRelationId,
	AggregateRelationId,
	DatabaseRelationId,
	AccessMethodRelationId
};

/* Test buffer list, the buffer id from 0 */
static int test_bufs[TEST_BUF_LIST_NUM];

static flog_ctl_t test_instance = NULL;
static flog_buf_ctl_t buf_ctl_test = NULL;
flog_list_ctl_t flog_list_test = NULL;
flog_index_queue_ctl_t flog_index_queue_test = NULL;
logindex_snapshot_t flog_index_test = NULL;

/*---- Function declarations ----*/
void		_PG_init(void);
void		_PG_fini(void);

static bool
check_dir_validate(char *path)
{
	struct stat st;

	if ((polar_stat(path, &st) == 0) && S_ISDIR(st.st_mode))
		return true;
	else
		return false;
}

static polar_flog_rec_ptr
flog_valid_ptr_from_end(polar_flog_rec_ptr ptr)
{
	if (ptr % POLAR_FLOG_SEG_SIZE == 0)
		return ptr + FLOG_LONG_PHD_SIZE;
	else if (ptr % POLAR_FLOG_BLCKSZ == 0)
		return ptr + FLOG_SHORT_PHD_SIZE;
	else
		return ptr;
}

static void
init_flog_buf_ctl_data(flog_ctl_file_data_t *ctl_file_data,
		uint64 max_seg_no, polar_flog_rec_ptr flog_end_ptr,
		polar_flog_rec_ptr flog_end_ptr_prev,
		polar_flog_rec_ptr flog_start_ptr,
		fbpoint_wal_info_data_t wal_info, bool is_shutdown)
{
	memset(ctl_file_data, 0, sizeof(flog_ctl_file_data_t));
	ctl_file_data->max_seg_no = max_seg_no;
	ctl_file_data->fbpoint_info.flog_end_ptr = flog_end_ptr;
	ctl_file_data->fbpoint_info.flog_end_ptr_prev = flog_end_ptr_prev;
	ctl_file_data->fbpoint_info.flog_start_ptr = flog_start_ptr;
	ctl_file_data->version_no = FLOG_CTL_FILE_VERSION;

	if (is_shutdown)
		ctl_file_data->version_no |= FLOG_SHUTDOWNED;

	ctl_file_data->fbpoint_info.wal_info = wal_info;
}

static polar_flog_rec_ptr
compute_empty_page_next_ptr(polar_flog_rec_ptr start_ptr)
{
	uint64 len;
	uint64 free_space_blk;
	uint64 free_space_seg;

	len = FLOG_REC_EMPTY_PAGE_SIZE;
	start_ptr = flog_valid_ptr_from_end(start_ptr);
	free_space_blk = POLAR_FLOG_BLCKSZ - (start_ptr % POLAR_FLOG_BLCKSZ);
	free_space_seg = POLAR_FLOG_SEG_SIZE - (start_ptr % POLAR_FLOG_SEG_SIZE);

	if (free_space_blk >= len)
		return MAXALIGN64(start_ptr + len);
	else if (free_space_seg < len)
		return MAXALIGN64(start_ptr + FLOG_LONG_PHD_SIZE + len);
	else
		return MAXALIGN64(start_ptr + FLOG_SHORT_PHD_SIZE + len);
}

static polar_flog_rec_ptr
compute_empty_page_start_ptr(polar_flog_rec_ptr end_ptr, Size size)
{
	polar_flog_rec_ptr start_ptr = POLAR_INVALID_FLOG_REC_PTR;
	polar_flog_rec_ptr pos;

	pos = polar_flog_ptr2pos(end_ptr) - MAXALIGN(FLOG_REC_EMPTY_PAGE_SIZE);
	start_ptr = polar_flog_pos2ptr(pos);
	return start_ptr;
}

static void
check_flog_list_init(void)
{
	int i;

	Assert(flog_list_test->head == NOT_IN_FLOG_LIST);
	Assert(flog_list_test->tail == NOT_IN_FLOG_LIST);
	Assert(pg_atomic_read_u64(&flog_list_test->insert_total_num) == 0);
	Assert(pg_atomic_read_u64(&flog_list_test->remove_total_num) == 0);
	Assert(flog_list_test->bg_remove_num == 0);

	for (i = 0; i < NBuffers; i++)
	{
		flog_list_slot slot;

		slot = flog_list_test->flashback_list[i];
		Assert(slot.flashback_ptr == POLAR_INVALID_FLOG_REC_PTR);
		Assert(slot.info == FLOG_LIST_SLOT_EMPTY);
		Assert(slot.prev_buf == NOT_IN_FLOG_LIST);
		Assert(slot.next_buf == NOT_IN_FLOG_LIST);
		Assert(slot.redo_lsn == InvalidXLogRecPtr);
	}
}

static void
check_flog_buf_init(const char *name, int insert_locks_num, int log_buffers)
{
	char buf[POLAR_FLOG_BLCKSZ];
	int i;

	memset(buf, 0, POLAR_FLOG_BLCKSZ);
	Assert(pg_atomic_read_u64(&buf_ctl_test->write_total_num) == 0);
	Assert(pg_atomic_read_u64(&buf_ctl_test->segs_added_total_num) == 0);
	Assert(buf_ctl_test->cache_blck == log_buffers - 1);
	Assert(buf_ctl_test->write_result == POLAR_INVALID_FLOG_REC_PTR);
	Assert(buf_ctl_test->write_request == POLAR_INVALID_FLOG_REC_PTR);
	Assert(buf_ctl_test->max_seg_no == POLAR_INVALID_FLOG_SEGNO);
	Assert(strncmp(buf_ctl_test->dir, name, FL_INS_MAX_NAME_LEN) == 0);
	Assert(buf_ctl_test->insert.insert_locks_num == insert_locks_num);
	Assert(buf_ctl_test->buf_state == FLOG_BUF_INIT);

	for (i = 0; i < log_buffers; i++)
	{
		polar_flog_rec_ptr ptr;

		ptr = buf_ctl_test->blocks[i];
		Assert(ptr == POLAR_INVALID_FLOG_REC_PTR);
		Assert(memcmp(buf, &(buf_ctl_test->pages[i * POLAR_FLOG_BLCKSZ]), POLAR_FLOG_BLCKSZ) == 0);
	}

	for (i = 0; i < insert_locks_num; i++)
	{
		flog_insert_lock_padded *lock;

		lock = buf_ctl_test->insert.insert_locks;
		Assert(lock->l.inserting_at == POLAR_INVALID_FLOG_REC_PTR);
	}

	polar_log_flog_buf_state(buf_ctl_test->buf_state);
}

static void
check_flog_queue_init(int queue_buffers_MB)
{
	polar_ringbuf_t queue = flog_index_queue_test->queue;
	Size queue_size;
	int i;
	uint8 *data_expected;
	uint64 free_up_times;
	uint64 read_from_file_rec_nums;
	uint64 read_from_queue_rec_nums;
	flog_index_queue_stat *queue_stat = flog_index_queue_test->queue_stat;

	queue_size = queue_buffers_MB * 1024L * 1024L;
	pg_atomic_init_u64(&queue->pread, 0);
	pg_atomic_init_u64(&queue->pwrite, 0);
	data_expected = palloc(queue_size - offsetof(polar_ringbuf_data_t, data));

	for (i = 0; i < POLAR_RINGBUF_MAX_SLOT; i++)
	{
		polar_ringbuf_slot_t slot_expected;

		memset(&slot_expected, 0, sizeof(polar_ringbuf_slot_t));
		Assert(memcmp(&slot_expected, &(queue->slot[i]), sizeof(polar_ringbuf_slot_t)) == 0);
	}

	Assert(queue->size == queue_size - offsetof(polar_ringbuf_data_t, data));
	Assert(queue->occupied == 0);
	memset(data_expected, 0, queue_size - offsetof(polar_ringbuf_data_t, data));
	Assert(memcmp(data_expected, queue->data, queue->size) == 0);

	polar_get_flog_index_queue_stat(queue_stat, &free_up_times, &read_from_file_rec_nums,
			&read_from_queue_rec_nums);

	Assert(free_up_times == 0);
	Assert(read_from_file_rec_nums == 0);
	Assert(read_from_queue_rec_nums == 0);
}

static void
check_flog_index_init(const char *name, int flog_index_mem_size)
{
	char flog_index_name[FL_OBJ_MAX_NAME_LEN];

	snprintf(flog_index_name, FL_OBJ_MAX_NAME_LEN, "%s%s", name, FL_LOGINDEX_SUFFIX);
	Assert(flog_index_test->mem_tbl_size == polar_logindex_convert_mem_tbl_size(flog_index_mem_size));
	Assert(pg_atomic_read_u32(&flog_index_test->state) == 0);
	Assert(flog_index_test->max_allocated_seg_no == 0);
	Assert(flog_index_test->table_flushable == polar_flog_index_table_flushable);
	Assert(strcmp(flog_index_test->dir, flog_index_name) == 0);
	Assert(flog_index_test->segment_cache == NULL);
}

static void
check_flog_history_file(polar_flog_rec_ptr switch_ptr, polar_flog_rec_ptr next_ptr)
{
	ListCell   *cell;
	List *switch_ptrs = NIL;
	bool result = false;

	switch_ptrs = polar_read_flog_history_file(buf_ctl_test->dir);

	foreach(cell, switch_ptrs)
	{
		flog_history_entry *tle = (flog_history_entry *) lfirst(cell);

		if (switch_ptr == tle->switch_ptr &&
				next_ptr == tle->next_ptr)
			result = true;
	}
	list_free_deep(switch_ptrs);
	Assert(result);
}

/* Check for flashback log record into buffer */
static polar_flog_rec_ptr
test_flog_insert_to_buffer(BufferTag test_tag, Page page, XLogRecPtr redo_lsn)
{
	return polar_insert_buf_flog_rec(test_instance, &test_tag, redo_lsn,
			polar_get_curr_fbpoint_lsn(buf_ctl_test), 0, page, false);
}

static void
check_flog_index_queue(polar_flog_rec_ptr ptr, BufferTag tag_expected,
					   uint32 log_len_expected)
{
	static polar_ringbuf_ref_t ref = { .slot = -1};
	BufferTag tag;
	uint32 log_len;
	polar_flog_rec_ptr max_ptr_in_disk;

	if (ref.slot == -1)
		polar_ringbuf_new_ref(flog_index_queue_test->queue, false, &ref, "test_queue");

	max_ptr_in_disk = polar_get_flog_write_result(buf_ctl_test);

	/* The flashback log may be not in the disk */
	if (polar_get_next_flog_ptr(ptr, log_len_expected) > max_ptr_in_disk)
	{
		uint32 data_len;
		flog_index_queue_lsn_info lsn_info;
		ssize_t offset = FLOG_INDEX_QUEUE_HEAD_SIZE;

		memset(&lsn_info, 0, sizeof(flog_index_queue_lsn_info));
		Assert(!polar_flog_index_queue_ref_pop(&ref, &lsn_info, max_ptr_in_disk));
		data_len = FLOG_INDEX_QUEUE_DATA_SIZE;
		POLAR_COPY_QUEUE_CONTENT(&ref, offset, &(lsn_info.tag), data_len);
		polar_ringbuf_update_ref(&ref);
		tag = lsn_info.tag;
		log_len = lsn_info.log_len;
	}
	else
	{
		Assert(polar_flog_read_info_from_queue(&ref, ptr, &tag, &log_len, max_ptr_in_disk));
	}

	Assert(BUFFERTAGS_EQUAL(tag_expected, tag));
	Assert(log_len == log_len_expected);
}

static void
check_flog_control_file(polar_flog_rec_ptr start_ptr, polar_flog_rec_ptr end_ptr,
						polar_flog_rec_ptr end_ptr_prev, uint64 max_seg_no,
						fbpoint_wal_info_data_t wal_info, bool is_shutdown)
{
	flog_ctl_file_data_t ctl_file_data;

	/* Read the flashback log control file */
	polar_read_flog_ctl_file(buf_ctl_test, &ctl_file_data);
	Assert((ctl_file_data.version_no & FLOG_CTL_VERSION_MASK) == FLOG_CTL_FILE_VERSION);

	if (is_shutdown)
		Assert(ctl_file_data.version_no & FLOG_SHUTDOWNED);

	Assert(ctl_file_data.fbpoint_info.flog_end_ptr == end_ptr);
	Assert(ctl_file_data.fbpoint_info.flog_end_ptr_prev == end_ptr_prev);
	Assert(ctl_file_data.fbpoint_info.flog_start_ptr == start_ptr);
	Assert(ctl_file_data.max_seg_no == max_seg_no);
	Assert(ctl_file_data.fbpoint_info.wal_info.prior_fbpoint_lsn ==
			wal_info.prior_fbpoint_lsn);
	Assert(ctl_file_data.fbpoint_info.wal_info.fbpoint_lsn ==
			wal_info.fbpoint_lsn);
	Assert(ctl_file_data.fbpoint_info.wal_info.fbpoint_time ==
			wal_info.fbpoint_time);
}

/*
 * Test the flog insert to buffer.
 */
static polar_flog_rec_ptr
test_flog_insert_buffer(polar_flog_rec_ptr start_ptr, bool test_buf_full,
						bool check_queue, int empty_page_num)
{
	polar_flog_rec_ptr end_ptr = POLAR_INVALID_FLOG_REC_PTR;
	int i;

	for (i = 0; i < empty_page_num; i++)
	{
		BufferTag test_tag;
		PGAlignedBlock page;
		XLogRecPtr redo_lsn;
		polar_flog_rec_ptr end_ptr_expected;

		PageInit((Page)page.data, BLCKSZ, 0);
		CLEAR_BUFFERTAG(test_tag);
		test_tag.rnode.dbNode = i;
		test_tag.rnode.relNode = i;
		test_tag.rnode.spcNode = i;
		test_tag.forkNum = MAIN_FORKNUM;
		test_tag.blockNum = i;
		redo_lsn = i;

		end_ptr = test_flog_insert_to_buffer(test_tag, (Page)page.data, redo_lsn);
		end_ptr_expected = compute_empty_page_next_ptr(start_ptr);
		Assert(end_ptr == end_ptr_expected);

		/* Check the buffer is full */
		if (test_buf_full)
		{
			Assert((buf_ctl_test->write_result == POLAR_FLOG_BLCKSZ) ||
				   (buf_ctl_test->write_result == 0));
			if (end_ptr > POLAR_FLOG_BLCKSZ * TEST_FLOG_BUFS)
			{
				test_buf_full = false;
				Assert(buf_ctl_test->write_result == POLAR_FLOG_BLCKSZ);
			}
		}

		/* The queue is full, the buffer will advict */
		if (QUEUE_PKT_SIZE * (i + 1) > flog_index_queue_test->queue->size)
		{
			if (check_queue)
			{
				uint64 free_up_times;
				uint64 read_from_file_rec_nums;
				uint64 read_from_queue_rec_nums;

				polar_get_flog_index_queue_stat(flog_index_queue_test->queue_stat, &free_up_times, &read_from_file_rec_nums,
												&read_from_queue_rec_nums);
				Assert(free_up_times == 1);
				Assert(read_from_file_rec_nums == 0);
				Assert(read_from_queue_rec_nums == 0);
			}

			insert_empty_page_rec_num = i;
			break;
		}

		if (check_queue)
			check_flog_index_queue(VALID_FLOG_PTR(start_ptr), test_tag,
								   FLOG_REC_EMPTY_PAGE_SIZE);

		start_ptr = polar_get_next_flog_ptr(start_ptr,
											FLOG_REC_EMPTY_PAGE_SIZE);
	}
	return end_ptr;
}

static void
check_flog_dir_validate(const char name[FL_INS_MAX_NAME_LEN], bool is_rm)
{
	char flog_path[MAXPGPATH];
	char flog_index_path[MAXPGPATH];
	char logindex_name[2 * FL_INS_MAX_NAME_LEN];
	bool is_valid;

	snprintf(logindex_name, 2 * FL_INS_MAX_NAME_LEN, "%s%s", name, "_index");
	polar_make_file_path_level2(flog_path, name);
	is_valid = check_dir_validate(flog_path);
	if (is_rm)
		Assert(!is_valid);
	else
		Assert(is_valid);
	polar_make_file_path_level2(flog_index_path, logindex_name);
	if (is_rm)
		Assert(!check_dir_validate(flog_index_path));
	else
		Assert(check_dir_validate(flog_index_path));
}

static void
check_flog_truncate(polar_flog_rec_ptr ptr)
{
	DIR		   *xldir;
	struct dirent *xlde;
	char		lastoff[FLOG_MAX_FNAME_LEN];
	char		polar_path[MAXPGPATH];
	uint64      seg_no;

	polar_make_file_path_level2(polar_path, polar_get_flog_dir(buf_ctl_test));
	seg_no = FLOG_PTR_TO_SEG(ptr, POLAR_FLOG_SEG_SIZE);
	if (seg_no == 0)
		return;
	seg_no--;
	FLOG_GET_FNAME(lastoff, seg_no, POLAR_FLOG_SEG_SIZE, FLOG_DEFAULT_TIMELINE);
	xldir = polar_allocate_dir(polar_path);
	while ((xlde = ReadDir(xldir, polar_path)) != NULL)
	{
		/* Ignore files that are not flashback log segments */
		if (!FLOG_IS_LOG_FILE(xlde->d_name))
			continue;
		Assert(strcmp(xlde->d_name, lastoff) > 0);
	}
	FreeDir(xldir);
}

static void
check_flog_prealloc_files(uint64 seg_no)
{
	DIR		   *xldir;
	struct dirent *xlde;
	char		lastoff[FLOG_MAX_FNAME_LEN];
	char		polar_path[MAXPGPATH];

	polar_make_file_path_level2(polar_path, polar_get_flog_dir(buf_ctl_test));
	FLOG_GET_FNAME(lastoff, seg_no, POLAR_FLOG_SEG_SIZE, FLOG_DEFAULT_TIMELINE);
	xldir = polar_allocate_dir(polar_path);
	while ((xlde = ReadDir(xldir, polar_path)) != NULL)
	{
		/* Ignore files that are not flashback log segments */
		if (!FLOG_IS_LOG_FILE(xlde->d_name))
			continue;
		Assert(strcmp(xlde->d_name, lastoff) <= 0);
	}
	FreeDir(xldir);
}

static bool
flog_wal_info_equal(fbpoint_wal_info_data_t info1, fbpoint_wal_info_data_t info2)
{
	return info1.prior_fbpoint_lsn == info2.prior_fbpoint_lsn &&
			info1.fbpoint_lsn == info2.fbpoint_lsn &&
			info1.fbpoint_time == info2.fbpoint_time;
}

static void
fbpoint_info_equal(fbpoint_info_data_t info1, fbpoint_info_data_t info2)
{
	Assert(info1.flog_end_ptr == info2.flog_end_ptr);
	Assert(info1.flog_end_ptr_prev == info2.flog_end_ptr_prev);
	Assert(info1.flog_start_ptr == info2.flog_start_ptr);
	Assert(flog_wal_info_equal(info1.wal_info, info2.wal_info));
	return;
}

static Size
get_test_shmem_size(void)
{
	return polar_flog_shmem_size_internal(TEST_FLOG_INSERT_LOCKS, TEST_FLOG_BUFS,
										  TEST_FLOG_INDEX_MEM_SIZE, TEST_FLOG_INDEX_BLOOM_BLKS, TEST_FLOG_QUEUE_BUFS);
}

static logindex_snapshot_t
init_flog_index(char *name, int logindex_mem_size, int logindex_bloom_blocks)
{
#define LOGINDEX_SNAPSHOT_SUFFIX "_snapshot"
#define LOGINDEX_LOCK_SUFFIX "_lock"
#define LOGINDEX_BLOOM_SUFFIX "_bloom"
#define POLAR_SLRU_HASH_NAME " slru hash index"

	char	logindex_name[POLAR_MAX_SHMEM_NAME];
	char	logindex_snapshot_name[POLAR_MAX_SHMEM_NAME];
	char	logindex_lock_name[POLAR_MAX_SHMEM_NAME];
	char	logindex_bloom_name[POLAR_MAX_SHMEM_NAME];
	char	slru_hash_name[POLAR_MAX_SHMEM_NAME];
	logindex_snapshot_t logindex_snapshot;
	HTAB* shem_index = polar_get_shmem_index();

	snprintf(logindex_name, POLAR_MAX_SHMEM_NAME, "%s%s", name, FL_LOGINDEX_SUFFIX);
	snprintf(logindex_snapshot_name, POLAR_MAX_SHMEM_NAME, "%s%s", logindex_name,
			LOGINDEX_SNAPSHOT_SUFFIX);
	snprintf(logindex_lock_name, POLAR_MAX_SHMEM_NAME, "%s%s", logindex_name,
			LOGINDEX_LOCK_SUFFIX);
	snprintf(logindex_bloom_name, POLAR_MAX_SHMEM_NAME, " %s%s", logindex_name,
			LOGINDEX_BLOOM_SUFFIX);
	snprintf(slru_hash_name, POLAR_MAX_SHMEM_NAME, "%s%s", logindex_bloom_name, POLAR_SLRU_HASH_NAME);

	LWLockAcquire(ShmemIndexLock, LW_EXCLUSIVE);
	hash_search(shem_index, logindex_snapshot_name, HASH_REMOVE, NULL);
	hash_search(shem_index, logindex_lock_name, HASH_REMOVE, NULL);
	hash_search(shem_index, logindex_bloom_name, HASH_REMOVE, NULL);
	hash_search(shem_index, slru_hash_name, HASH_REMOVE, NULL);
	LWLockRelease(ShmemIndexLock);

	IsUnderPostmaster = false;
	logindex_snapshot =
			polar_flog_index_shmem_init(name, logindex_mem_size, logindex_bloom_blocks, buf_ctl_test);
	IsUnderPostmaster = true;

	return logindex_snapshot;
}

static bool
check_add_origin_page(BufferDesc *buf_hdr, int8 buf_index)
{
	Assert(((GET_ORIGIN_BUF_BIT(flog_list_test, buf_index)) & 1) == 1);
	Assert(BUFFERTAGS_EQUAL(flog_list_test->buf_tag[buf_index], buf_hdr->tag));
	Assert(memcmp(flog_list_test->origin_buf + buf_index * BLCKSZ,
			(char *) BufHdrGetBlock(buf_hdr), BLCKSZ) == 0);
	Assert(flog_list_test->flashback_list[buf_hdr->buf_id].origin_buf_index == buf_index);
	return true;
}

static bool
check_clean_origin_page(int buf_id, int8 buf_index)
{
	char origin_buf_clean[BLCKSZ];

	MemSet(origin_buf_clean, 0, BLCKSZ);
	Assert(((GET_ORIGIN_BUF_BIT(flog_list_test, buf_index)) & 1) == 0);
	Assert(memcmp(flog_list_test->origin_buf + buf_index * BLCKSZ, origin_buf_clean, BLCKSZ) == 0);
	Assert(flog_list_test->flashback_list[buf_id].origin_buf_index == -1);
	return true;
}

static bool
check_origin_buf_is_empty(void)
{
	int j;

	for (j = 0; j < POLAR_ORIGIN_PAGE_BUF_ARRAY_NUM; j++)
	{
		Assert(pg_atomic_read_u32(&flog_list_test->origin_buf_bitmap[j]) == 0);
	}

	return true;
}

static bool
check_origin_buf_is_full(void)
{
	int j;

	for (j = 0; j < POLAR_ORIGIN_PAGE_BUF_ARRAY_NUM; j++)
	{
		Assert(pg_atomic_read_u32(&flog_list_test->origin_buf_bitmap[j]) == 0xFFFFFFFF);
	}

	return true;
}

static int8
get_expected_origin_buf_index(int i)
{
	int array_id;
	int8 buf_index;

	array_id = MyProc->pgprocno % POLAR_ORIGIN_PAGE_BUF_ARRAY_NUM;

	array_id = (array_id + i / POLAR_ORIGIN_PAGE_BUF_NUM_PER_ARRAY) % POLAR_ORIGIN_PAGE_BUF_ARRAY_NUM;

	buf_index = (int8) (array_id * POLAR_ORIGIN_PAGE_BUF_NUM_PER_ARRAY +
			i % POLAR_ORIGIN_PAGE_BUF_NUM_PER_ARRAY);

	return buf_index;
}

static int8
test_add_origin_page(BufferDesc *buf_hdr)
{
	int8 buf_index;

	buf_index = polar_add_origin_buf(flog_list_test, buf_hdr);
	check_add_origin_page(buf_hdr, buf_index);

	return buf_index;
}

static void
test_clean_origin_page(int buf_id, int8 buf_index)
{
	polar_clean_origin_buf_bit(flog_list_test, buf_id, buf_index);
	check_clean_origin_page(buf_id, buf_index);
}

static void
test_flog_shmem_startup(void)
{
	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	/*
	 * Create or attach to the shared memory state, including hash table
	 */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	polar_flog_shmem_init_internal(TEST_FLOG_NAME, TEST_FLOG_INSERT_LOCKS, TEST_FLOG_BUFS,
								   TEST_FLOG_INDEX_MEM_SIZE, TEST_FLOG_INDEX_BLOOM_BLKS, TEST_FLOG_QUEUE_BUFS);
	LWLockRelease(AddinShmemInitLock);
}

static void
test_flog_write(polar_flog_rec_ptr ptr)
{
	Assert(ptr > POLAR_FLOG_BLCKSZ);
	Assert(buf_ctl_test->write_result < ptr);
	polar_flog_flush(buf_ctl_test, ptr);
	Assert(buf_ctl_test->write_result == ptr);
}

static void
test_flog_read(polar_flog_rec_ptr start_ptr)
{
	flog_reader_state *state;
	flog_record *record;
	char	   *errormsg;
	int i;

	state = polar_flog_reader_allocate(POLAR_FLOG_SEG_SIZE,
			&polar_flog_page_read, NULL, buf_ctl_test);

	for (i = 0; ; i++)
	{
		fl_origin_page_rec_data *rec_data;
		BufferTag tag;

		tag.rnode.dbNode = i;
		tag.rnode.relNode = i;
		tag.rnode.spcNode = i;
		tag.forkNum = MAIN_FORKNUM;
		tag.blockNum = i;

		/* try to read the next record */
		record = polar_read_flog_record(state, start_ptr, &errormsg);
		if (record == NULL)
			break;
		rec_data = FL_GET_ORIGIN_PAGE_REC_DATA(record);
		Assert(rec_data->redo_lsn == i);
		Assert(BUFFERTAGS_EQUAL(rec_data->tag, tag));
		/* after reading the first record, continue at next one */
		start_ptr = POLAR_INVALID_FLOG_REC_PTR;
	}
	polar_flog_reader_free(state);
}

static void
test_flog_index_write(polar_flog_rec_ptr ptr)
{
	polar_flog_rec_ptr start_ptr;

	/* Get the start ptr of the ptr */
	start_ptr = compute_empty_page_start_ptr(ptr, FLOG_REC_EMPTY_PAGE_SIZE);
	polar_flog_index_insert(flog_index_test, flog_index_queue_test, buf_ctl_test, ptr, ANY);
	Assert(polar_get_flog_index_max_ptr(flog_index_test) == start_ptr);
	polar_logindex_flush_table(flog_index_test, ptr);
	Assert(polar_get_flog_index_meta_max_ptr(flog_index_test) <= start_ptr);
}

static void
test_flog_index_search(polar_flog_rec_ptr start_ptr)
{
	PGAlignedBlock page_empty;
	polar_flog_rec_ptr end_ptr;
	int i;
	flshbak_buf_context_t context;
	flog_reader_state *reader;

	PageInit((Page)page_empty.data, BLCKSZ, 0);
	end_ptr = polar_get_flog_write_result(buf_ctl_test);

	reader = polar_flog_reader_allocate(POLAR_FLOG_SEG_SIZE,
			&polar_flog_page_read, NULL, buf_ctl_test);

	Assert(reader);

	context.logindex_snapshot = flog_index_test;
	context.start_ptr = start_ptr;
	context.end_ptr = end_ptr;
	context.reader = reader;

	for (i = 0; i < insert_empty_page_rec_num; i++)
	{
		BufferTag test_tag;
		PGAlignedBlock page;
		XLogRecPtr redo_lsn;

		PageInit((Page)page.data, BLCKSZ, 0);
		CLEAR_BUFFERTAG(test_tag);
		test_tag.rnode.dbNode = i;
		test_tag.rnode.relNode = i;
		test_tag.rnode.spcNode = i;
		test_tag.forkNum = MAIN_FORKNUM;
		test_tag.blockNum = i;
		context.tag = &test_tag;
		polar_get_origin_page(&context, (Page) page.data, &redo_lsn);
		Assert(redo_lsn == i);
		Assert(memcmp(page_empty.data, page.data, BLCKSZ) == 0);
	}
}

static void
test_flog_init(bool first_init)
{
	if (first_init)
	{
		/* Share memory init first, just update the static in the backend */
		test_instance = polar_flog_shmem_init_internal(TEST_FLOG_NAME, TEST_FLOG_INSERT_LOCKS, TEST_FLOG_BUFS,
								   TEST_FLOG_INDEX_MEM_SIZE, TEST_FLOG_INDEX_BLOOM_BLKS, TEST_FLOG_QUEUE_BUFS);
	}
	else
	{
		polar_flog_ctl_init_data(test_instance);

		polar_flog_buf_init_data(buf_ctl_test, TEST_FLOG_NAME, TEST_FLOG_INSERT_LOCKS, TEST_FLOG_BUFS);
		test_instance->buf_ctl = buf_ctl_test;

		polar_flog_list_init_data(flog_list_test);
		test_instance->list_ctl = flog_list_test;

		polar_flog_index_queue_init_data(flog_index_queue_test, TEST_FLOG_NAME, TEST_FLOG_QUEUE_BUFS);
		test_instance->queue_ctl = flog_index_queue_test;

		flog_index_test = init_flog_index(TEST_FLOG_NAME, TEST_FLOG_INDEX_MEM_SIZE, TEST_FLOG_INDEX_BLOOM_BLKS);
		test_instance->logindex_snapshot = flog_index_test;
	}

	/* Check flashback instance */
	flog_instance = test_instance;
	Assert(test_instance->buf_ctl);
	Assert(test_instance->list_ctl);
	Assert(test_instance->logindex_snapshot);
	Assert(test_instance->queue_ctl);
	Assert(pg_atomic_read_u32(&test_instance->state) == FLOG_INIT);

	/* Check flashback log control */
	buf_ctl_test = test_instance->buf_ctl;
	check_flog_buf_init(TEST_FLOG_NAME, TEST_FLOG_INSERT_LOCKS, TEST_FLOG_BUFS);

	/* Check flashback log list */
	flog_list_test = test_instance->list_ctl;
	check_flog_list_init();

	/* Check flashback log queue */
	flog_index_queue_test = test_instance->queue_ctl;
	check_flog_queue_init(TEST_FLOG_QUEUE_BUFS);

	/* Check flashback logindex */
	flog_index_test = test_instance->logindex_snapshot;
	check_flog_index_init(TEST_FLOG_NAME, TEST_FLOG_INDEX_MEM_SIZE);
}

static void
test_flog_startup(bool is_crash, flog_ctl_file_data_t ctl_file_data)
{
	log_index_meta_t logindex_meta;
	CheckPoint checkpoint;

	checkpoint.redo = GetRedoRecPtr();
	checkpoint.time = (pg_time_t) time(NULL);

	/* first init will set the expected flashback point info in here */
	if (ctl_file_data.fbpoint_info.wal_info.fbpoint_lsn == 0)
	{
		ctl_file_data.fbpoint_info.wal_info.prior_fbpoint_lsn = 0;
		ctl_file_data.fbpoint_info.wal_info.fbpoint_lsn = checkpoint.redo;
		ctl_file_data.fbpoint_info.wal_info.fbpoint_time = checkpoint.time;
	}

	if (is_crash)
	{
		ctl_file_data.fbpoint_info.flog_end_ptr = (ctl_file_data.max_seg_no + 1) * POLAR_FLOG_SEG_SIZE;
		ctl_file_data.fbpoint_info.flog_end_ptr_prev = POLAR_INVALID_FLOG_REC_PTR;
		polar_startup_flog(&checkpoint, test_instance);
		Assert(buf_ctl_test->buf_state == FLOG_BUF_READY);
		Assert(pg_atomic_read_u32(&test_instance->state) == FLOG_STARTUP || pg_atomic_read_u32(&test_instance->state) == FLOG_READY);
	}
	else
	{
		Assert(ctl_file_data.version_no & FLOG_SHUTDOWNED);
		polar_startup_flog(&checkpoint, test_instance);
		Assert(buf_ctl_test->buf_state == FLOG_BUF_READY);
		Assert(pg_atomic_read_u32(&test_instance->state) == FLOG_STARTUP || pg_atomic_read_u32(&test_instance->state) == FLOG_READY);
	}
	check_flog_dir_validate(TEST_FLOG_NAME, false);
	/* Check the flashback log checkpoint info */
	fbpoint_info_equal(buf_ctl_test->fbpoint_info, ctl_file_data.fbpoint_info);
	/* Check the flashback point wal lsn */
	Assert(flog_wal_info_equal(buf_ctl_test->wal_info,
			ctl_file_data.fbpoint_info.wal_info));
	/* Check the flashback log redo lsn */
	Assert(buf_ctl_test->redo_lsn == checkpoint.redo);
	/* Check the flashback log max seg no */
	Assert(buf_ctl_test->max_seg_no == ctl_file_data.max_seg_no);
	/* Check the flashback log index */
	Assert(polar_logindex_check_state(flog_index_test, POLAR_LOGINDEX_STATE_INITIALIZED));
	Assert(log_index_get_meta(flog_index_test, &logindex_meta));
}

static void
test_flog_recover(void)
{
	/* Refresh the flashback log insert function */
	polar_flog_index_insert(flog_index_test, flog_index_queue_test,
			buf_ctl_test, POLAR_INVALID_FLOG_REC_PTR, NONE);

	polar_recover_flog(test_instance);
	Assert(buf_ctl_test->buf_state == FLOG_BUF_READY);
	Assert(pg_atomic_read_u32(&test_instance->state) == FLOG_READY);
}

static void
check_flog_list_insert(int buf_id, XLogRecPtr fbpoint_lsn)
{
	XLogRecPtr redo_lsn;
	BufferDesc *buf_hdr = GetBufferDescriptor(buf_id);

	redo_lsn = buf_ctl_test->redo_lsn;
	Assert(flog_list_test->flashback_list[buf_id].flashback_ptr == POLAR_INVALID_FLOG_REC_PTR);
	Assert(flog_list_test->flashback_list[buf_id].redo_lsn == redo_lsn);
	Assert(flog_list_test->flashback_list[buf_id].fbpoint_lsn == fbpoint_lsn);
	Assert(flog_list_test->flashback_list[buf_id].info & FLOG_LIST_SLOT_READY);
	Assert(POLAR_CHECK_BUF_FLOG_STATE(buf_hdr, POLAR_BUF_IN_FLOG_LIST));
}

static void
check_flog_list_clean(int buf_id, bool is_flog_flushed, bool check_buf_redo_state)
{
	BufferDesc *buf_hdr = GetBufferDescriptor(buf_id);

	if (is_flog_flushed)
		Assert(flog_list_test->flashback_list[buf_id].flashback_ptr == POLAR_INVALID_FLOG_REC_PTR);
	else
		Assert(flog_list_test->flashback_list[buf_id].flashback_ptr != POLAR_INVALID_FLOG_REC_PTR);

	Assert(flog_list_test->flashback_list[buf_id].redo_lsn == InvalidXLogRecPtr);
	Assert(flog_list_test->flashback_list[buf_id].info == FLOG_LIST_SLOT_EMPTY);
	Assert(flog_list_test->flashback_list[buf_id].prev_buf == NOT_IN_FLOG_LIST);
	Assert(flog_list_test->flashback_list[buf_id].next_buf == NOT_IN_FLOG_LIST);
	Assert(flog_list_test->flashback_list[buf_id].origin_buf_index == -1);
	Assert(flog_list_test->head != buf_id);
	if (check_buf_redo_state)
		Assert(!POLAR_CHECK_BUF_FLOG_STATE(buf_hdr, POLAR_BUF_IN_FLOG_LIST));
}

static void
test_flog_insert_list(bool has_origin_buffer)
{
	int i;
	int head;
	int tail;
	XLogRecPtr fbpoint_lsn;

	fbpoint_lsn = polar_get_curr_fbpoint_lsn(buf_ctl_test);
	polar_get_local_fbpoint_lsn(buf_ctl_test, InvalidXLogRecPtr, fbpoint_lsn);

	for (i = 0; i < TEST_BUF_LIST_NUM; i++)
	{
		Relation rel;
		Buffer   buf;
		BufferDesc *buf_hdr;
		int buf_id;
		int8 origin_buf_index = -1;


		rel = try_relation_open(test_relnodes[i], AccessShareLock);
		buf = ReadBuffer(rel, 0);
		relation_close(rel, AccessShareLock);
		Assert(buf > 0 && buf <= NBuffers);
		buf_id = buf - 1;
		buf_hdr = GetBufferDescriptor(buf_id);
		test_bufs[i] = buf_id;
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

		/* Add test for origin buffer */
		if (has_origin_buffer)
		{
			Assert(TEST_BUF_LIST_NUM <= POLAR_ORIGIN_PAGE_BUF_NUM);
			origin_buf_index = test_add_origin_page(buf_hdr);
			Assert(origin_buf_index == get_expected_origin_buf_index(i));
		}

		polar_push_buf_to_flog_list(flog_list_test, buf_ctl_test, buf, false);
		check_flog_list_insert(buf_id, fbpoint_lsn);

		if (i == 0)
		{
			Assert(flog_list_test->head == buf_id);
			Assert(flog_list_test->tail == buf_id);
		}
		else
		{
			int   prev_buf_id;

			prev_buf_id = test_bufs[i - 1];
			Assert(flog_list_test->flashback_list[prev_buf_id].next_buf == buf_id);
			Assert(flog_list_test->flashback_list[buf_id].prev_buf == prev_buf_id);
		}
		Assert(flog_list_test->flashback_list[buf_id].next_buf == NOT_IN_FLOG_LIST);
	}

	polar_flog_get_async_list_info(flog_list_test, &head, &tail);
	Assert(head == test_bufs[0]);
	Assert(tail == test_bufs[TEST_BUF_LIST_NUM - 1]);

	/* Half to insert flog record by background */
	for (i = 0; i < TEST_BUF_LIST_NUM / 2; i++)
	{
		int   buf_id;

		buf_id = test_bufs[i];
		Assert(flog_list_test->flashback_list[buf_id].prev_buf == NOT_IN_FLOG_LIST);
		polar_process_flog_list_bg(test_instance);

		if (has_origin_buffer)
		{
			check_clean_origin_page(buf_id, get_expected_origin_buf_index(i));
		}

		polar_flog_get_async_list_info(flog_list_test, &head, &tail);
		if (head != NOT_IN_FLOG_LIST)
		{
			Assert(head == test_bufs[i + 1]);
			Assert(tail == test_bufs[TEST_BUF_LIST_NUM - 1]);
		}
		else
			Assert(tail == NOT_IN_FLOG_LIST);

		check_flog_list_clean(buf_id, false, true);
	}
}

static void
test_buf_flog_rec_sync(Buffer bufs[TEST_BUF_LIST_NUM], bool has_origin_buffer)
{
	int i;

	for (i = 0; i < TEST_BUF_LIST_NUM; i++)
	{
		int buf = bufs[i];
		polar_flog_rec_ptr ptr;
		polar_flog_rec_ptr write_result;
		BufferDesc *buf_hdr;
		bool is_invaild_buffer;

		is_invaild_buffer = (i&1);
		buf_hdr = GetBufferDescriptor(buf);
		ptr = flog_list_test->flashback_list[buf].flashback_ptr;
		if (i < TEST_BUF_LIST_NUM / 2)
			Assert(ptr != POLAR_INVALID_FLOG_REC_PTR);
		else
		{
			Assert(ptr == POLAR_INVALID_FLOG_REC_PTR);
		}

		write_result = buf_ctl_test->write_result;
		polar_flush_buf_flog_rec(buf_hdr, test_instance, is_invaild_buffer);
		if (!is_invaild_buffer)
		{
			Assert(buf_ctl_test->write_result > write_result);
		}
		else
			Assert(buf_ctl_test->write_result == write_result);

		check_flog_list_clean(buf, true, true);

		if (has_origin_buffer)
		{
			check_clean_origin_page(buf, get_expected_origin_buf_index(i));
		}
	}

	/* The origin buffer is empty */
	if (has_origin_buffer)
	{
		Assert(check_origin_buf_is_empty());
	}
}

static BufferDesc *
test_lock_buffer_in_bp(int buf_id)
{
	BufferDesc *buf_hdr;
	bool locked;

	buf_hdr = GetBufferDescriptor(buf_id);
	locked = LWLockConditionalAcquire(BufferDescriptorGetContentLock(buf_hdr),
								   LW_SHARED);
	if (!locked)
		return NULL;
	return buf_hdr;
}

static BufferDesc *
test_add_origin_buf_from_bp(int buf_id, bool *locked, int8 *buf_index)
{
	BufferDesc *buf_hdr;

	buf_hdr = test_lock_buffer_in_bp(buf_id);

	if (buf_hdr == NULL)
	{
		*locked = false;
		return NULL;
	}
	else
		*locked = true;

	*buf_index = polar_add_origin_buf(flog_list_test, buf_hdr);

	return buf_hdr;
}

static void
test_origin_buf_full(int buf_num)
{
	int i;
	int j = 0;
	int buf_id[buf_num];

	/* Test the origin buffer full */
	for (i = 0; j < buf_num && i < NBuffers; i++)
	{
		BufferDesc *buf_hdr;
		bool locked;
		int8 buf_index;
		int8 buf_index_expected;

		buf_hdr = test_add_origin_buf_from_bp(i, &locked, &buf_index);

		if (!locked)
			continue;

		buf_index_expected = get_expected_origin_buf_index(j);
		Assert(buf_index == buf_index_expected);
		check_add_origin_page(buf_hdr, buf_index);

		LWLockRelease(BufferDescriptorGetContentLock(buf_hdr));

		buf_id[j] = i;
		j++;
	}

	Assert(check_origin_buf_is_full());

	for (i = i + 1, j = 0; j < 5; i++)
	{
		BufferDesc *buf_hdr;
		bool locked;
		int8 buf_index;

		buf_hdr = test_add_origin_buf_from_bp(i, &locked, &buf_index);

		if (!locked)
			continue;

		LWLockRelease(BufferDescriptorGetContentLock(buf_hdr));
		Assert(buf_index == -1);
		j++;
	}

	/* Clean all the origin buffer to avoid to influence others */
	for (i = 0; i < buf_num; i++)
	{
		test_clean_origin_page(buf_id[i], get_expected_origin_buf_index(i));
	}

	Assert(check_origin_buf_is_empty());
}

static void
test_flog_repair_buffer(Buffer bufs[TEST_BUF_LIST_NUM])
{
	int i;
	Buffer buf0;
	BufferDesc *buf_hdr0;
	XLogRecPtr fbpoint_lsn;
	XLogRecPtr end_lsn;

	buf0 = bufs[0] + 1;
	buf_hdr0 = GetBufferDescriptor(buf0 - 1);

	/* Test apply first fpi */
	fbpoint_lsn = polar_get_curr_fbpoint_lsn(buf_ctl_test);
	end_lsn = fbpoint_lsn + 2;
	Assert(!polar_logindex_find_first_fpi(polar_logindex_redo_instance,
			fbpoint_lsn, end_lsn, &buf_hdr0->tag, &buf0, false));

	/* Test polar_can_flog_repair */
	Assert(!polar_can_flog_repair(NULL, buf_hdr0, true));
	Assert(!polar_can_flog_repair(NULL, buf_hdr0, false));
	Assert(polar_can_flog_repair(test_instance, buf_hdr0, true));
	Assert(!polar_can_flog_repair(test_instance, buf_hdr0, false));

	for (i = 0; i < TEST_BUF_LIST_NUM; i++)
	{
		int buf_id = bufs[i];
		BufferDesc *buf_hdr;
		PGAlignedBlock page;
		uint32		buf_state;
		bool		is_invaild_buffer;

		is_invaild_buffer = (i&1);

		if (!is_invaild_buffer)
		{
			buf_hdr = GetBufferDescriptor(buf_id);
			memcpy(page.data, (char *) BufHdrGetBlock(buf_hdr), BLCKSZ);
			/* Memset the hole to zero */
			MemSet(page.data + ((PageHeader) page.data)->pd_lower, 0, ((PageHeader) page.data)->pd_upper - ((PageHeader) page.data)->pd_lower);

			/* Set the buffer invalid first to test */
			buf_state = LockBufHdr(buf_hdr);
			buf_state &= ~BM_VALID;
			UnlockBufHdr(buf_hdr, buf_state);

			/* Break the buffer */
			memset((char *) BufHdrGetBlock(buf_hdr), 0, BLCKSZ);
			polar_repair_partial_write(test_instance, buf_hdr);
			Assert(POLAR_CHECK_BUF_FLOG_STATE(buf_hdr, POLAR_BUF_FLOG_DISABLE));
			Assert(memcmp(page.data, (char *) BufHdrGetBlock(buf_hdr), BLCKSZ) == 0);
			/* Set the buffer valid again */
			buf_state = LockBufHdr(buf_hdr);
			buf_state |= BM_VALID;
			UnlockBufHdr(buf_hdr, buf_state);
		}

		LockBuffer(buf_id + 1, BUFFER_LOCK_UNLOCK);
	}
}

static void
test_flog_checkpoint(bool is_shutdown)
{
	polar_flog_rec_ptr ckp_start = POLAR_INVALID_FLOG_REC_PTR;
	polar_flog_rec_ptr ckp_end = POLAR_INVALID_FLOG_REC_PTR;
	polar_flog_rec_ptr ckp_end_prev = POLAR_INVALID_FLOG_REC_PTR;
	XLogRecPtr lsn;
	XLogRecPtr lsn_prior;
	uint64 max_seg_no;
	pg_time_t ckp_time = (pg_time_t) time(NULL);
	int flags = 0;

	if (is_shutdown)
		flags = CHECKPOINT_IS_SHUTDOWN;

	lsn = polar_get_faked_latest_lsn();

	if (!polar_is_flashback_point(test_instance, lsn, InvalidXLogRecPtr, &flags, false))
	{
		lsn = lsn + wal_segment_size * (polar_flashback_point_segments + 1);
		Assert(polar_is_flashback_point(test_instance, lsn, InvalidXLogRecPtr, &flags, false));
	}

	lsn_prior = buf_ctl_test->wal_info.fbpoint_lsn;

	/* If lsn is equal to prior lsn, add lsn */
	if (lsn == lsn_prior)
		lsn++;

	ckp_start = polar_get_flog_write_result(buf_ctl_test);
	polar_set_fbpoint_wal_info(buf_ctl_test, lsn, ckp_time, InvalidXLogRecPtr, false);
	polar_flog_do_fbpoint(test_instance, ckp_start, ckp_start, is_shutdown);
	/* Just set the redo lsn to lsn prior */
	POLAR_CHECK_POINT_FLOG(test_instance, lsn_prior);

	if (is_shutdown)
	{
		polar_log_flog_buf_state(buf_ctl_test->buf_state);
		Assert(buf_ctl_test->buf_state == FLOG_BUF_SHUTDOWNED);
	}
	Assert(buf_ctl_test->fbpoint_info.flog_start_ptr == ckp_start);

	if (is_shutdown)
	{
		ckp_end =
			polar_get_curr_flog_ptr(buf_ctl_test, &ckp_end_prev);
	}
	else
		ckp_end = polar_get_flog_write_result(buf_ctl_test);

	Assert(buf_ctl_test->wal_info.prior_fbpoint_lsn == lsn_prior);
	Assert(buf_ctl_test->wal_info.fbpoint_lsn == lsn);
	Assert(buf_ctl_test->wal_info.fbpoint_time == ckp_time);
	Assert(buf_ctl_test->fbpoint_info.wal_info.prior_fbpoint_lsn == lsn_prior);
	Assert(buf_ctl_test->fbpoint_info.wal_info.fbpoint_lsn == lsn);
	Assert(buf_ctl_test->fbpoint_info.wal_info.fbpoint_time == ckp_time);

	max_seg_no = buf_ctl_test->max_seg_no;
	check_flog_control_file(ckp_start, ckp_end, ckp_end_prev, max_seg_no,
			buf_ctl_test->wal_info, is_shutdown);

	Assert(buf_ctl_test->redo_lsn == lsn_prior);
	check_flog_truncate(Min(ckp_start, polar_get_flog_index_meta_max_ptr(flog_index_test)));
}

static void
test_flog_crash_recovery(void)
{
	uint64 max_seg_no;
	polar_flog_rec_ptr ptr;
	polar_flog_rec_ptr pos;
	polar_flog_rec_ptr ckp_start;
	polar_flog_rec_ptr ckp_end;
	polar_flog_rec_ptr ckp_end_prev;
	polar_flog_rec_ptr ptr_expected;
	polar_flog_rec_ptr max_in_index;
	flog_ctl_file_data_t ctl_file_data;
	fbpoint_wal_info_data_t  wal_info;

	max_seg_no = buf_ctl_test->max_seg_no;
	Assert(max_seg_no != POLAR_INVALID_FLOG_SEGNO);
	ckp_start = buf_ctl_test->fbpoint_info.flog_start_ptr;
	ckp_end = buf_ctl_test->fbpoint_info.flog_end_ptr;
	ckp_end_prev = buf_ctl_test->fbpoint_info.flog_end_ptr_prev;
	wal_info = buf_ctl_test->fbpoint_info.wal_info;
	max_in_index = polar_get_flog_index_max_ptr(flog_index_test);
	polar_flog_flush(buf_ctl_test, polar_get_curr_flog_ptr(buf_ctl_test, &ptr_expected));
	Assert(max_in_index < ptr_expected);
	init_flog_buf_ctl_data(&ctl_file_data, max_seg_no, ckp_end, ckp_end_prev, ckp_start, wal_info, false);

	/* Reset the flashback thing and recover */
	test_flog_init(false);
	test_flog_startup(true, ctl_file_data);
	test_flog_recover();

	ptr = (max_seg_no + 1) * POLAR_FLOG_SEG_SIZE;
	pos = polar_flog_ptr2pos(ptr);
	Assert(buf_ctl_test->write_result == ptr);
	Assert(buf_ctl_test->max_seg_no == max_seg_no);
	Assert(buf_ctl_test->initalized_upto == ptr);
	Assert(buf_ctl_test->write_request == ptr);
	Assert(buf_ctl_test->insert.curr_pos == pos);
	Assert(buf_ctl_test->insert.prev_pos == POLAR_INVALID_FLOG_REC_PTR);
	Assert(polar_get_flog_index_max_ptr(flog_index_test) == ptr_expected);

	check_flog_control_file(ckp_start, ptr, POLAR_INVALID_FLOG_REC_PTR, max_seg_no, wal_info, false);
	check_flog_history_file(ckp_end, ptr);
}

static void
test_flog_shutdown_recovery(void)
{
	uint64 max_seg_no;
	fbpoint_info_data_t info_expected;
	polar_flog_rec_ptr write_result;
	polar_flog_rec_ptr write_request;
	polar_flog_rec_ptr ckp_end_ptr_prev;
	polar_flog_rec_ptr pos;
	polar_flog_rec_ptr prev_pos;
	polar_flog_rec_ptr end_ptr;
	polar_flog_rec_ptr upto;
	flog_ctl_file_data_t ctl_file_data;

	write_result = buf_ctl_test->write_result;

	/* Insert the flashback log and write */
	end_ptr = test_flog_insert_buffer(write_result, false,
									  false, (TEST_FLOG_BUFS * POLAR_FLOG_BLCKSZ) / FLOG_REC_EMPTY_PAGE_SIZE);
	test_flog_write(end_ptr);
	write_result = buf_ctl_test->write_result;
	write_request = buf_ctl_test->write_request;
	Assert(polar_get_flog_index_max_ptr(flog_index_test) < write_result);

	/* Test the flashback log checkpoint (shutdown) */
	test_flog_checkpoint(true);

	info_expected = buf_ctl_test->fbpoint_info;
	max_seg_no = buf_ctl_test->max_seg_no;
	pos = polar_flog_ptr2pos(write_result);
	ckp_end_ptr_prev = info_expected.flog_end_ptr_prev;
	prev_pos = polar_flog_ptr2pos(ckp_end_ptr_prev);
	upto = polar_get_flog_buf_initalized_upto(buf_ctl_test);
	Assert(info_expected.flog_end_ptr == end_ptr);
	init_flog_buf_ctl_data(&ctl_file_data, max_seg_no, info_expected.flog_end_ptr,
			ckp_end_ptr_prev, info_expected.flog_start_ptr, info_expected.wal_info, true);

	/* Reset the flashback thing and recover */
	test_flog_init(false);
	test_flog_startup(false, ctl_file_data);
	test_flog_recover();

	Assert(buf_ctl_test->write_result == write_result);
	Assert(buf_ctl_test->write_request == write_request);
	Assert(buf_ctl_test->max_seg_no == max_seg_no);
	Assert(buf_ctl_test->initalized_upto == upto);
	Assert(buf_ctl_test->insert.curr_pos == pos);
	Assert(buf_ctl_test->insert.prev_pos == prev_pos);
	fbpoint_info_equal(info_expected, buf_ctl_test->fbpoint_info);
	Assert(polar_get_flog_index_max_ptr(flog_index_test) == ckp_end_ptr_prev);

	check_flog_control_file(info_expected.flog_start_ptr, info_expected.flog_end_ptr,
							info_expected.flog_end_ptr_prev, max_seg_no,
							info_expected.wal_info, false);
}

/*
 * Test the flashback log background worker functions.
 * Now is:
 * polar_flog_flush_bg
 * polar_insert_flog_index_bg
 * polar_flog_index_background_flush
 */
static void
test_flog_bg_worker_functions(void)
{
	polar_flog_rec_ptr start_ptr;
	polar_flog_rec_ptr end_ptr;
	polar_flog_rec_ptr write_result;
	polar_flog_rec_ptr write_result_expected;
	polar_flog_rec_ptr max_ptr_in_index_mem;
	polar_flog_rec_ptr max_ptr_in_index_meta;
	uint64 write_total_num;
	uint64 bg_write_num;
	uint64 segs_added_total_num;

	start_ptr = buf_ctl_test->write_result;
	end_ptr = test_flog_insert_buffer(start_ptr, false, false,
									  (TEST_FLOG_BUFS * POLAR_FLOG_BLCKSZ) / FLOG_REC_EMPTY_PAGE_SIZE);
	write_result = buf_ctl_test->write_result;

	/* Just write one block */
	write_result_expected = (write_result + POLAR_FLOG_BLCKSZ) -
							((write_result + POLAR_FLOG_BLCKSZ) % POLAR_FLOG_BLCKSZ);
	polar_flashback_log_flush_max_size = POLAR_FLOG_BLCKSZ / 1024;
	polar_flog_flush_bg(buf_ctl_test);
	polar_get_flog_write_stat(buf_ctl_test, &write_total_num, &bg_write_num, &segs_added_total_num);
	Assert(bg_write_num == 1);
	Assert(write_total_num > bg_write_num);
	Assert(buf_ctl_test->write_result > write_result);
	write_result = buf_ctl_test->write_result;
	if (end_ptr % POLAR_FLOG_SEG_SIZE + POLAR_FLOG_BLCKSZ < POLAR_FLOG_SEG_SIZE)
		Assert(write_result == write_result_expected);

	max_ptr_in_index_mem = polar_get_flog_index_max_ptr(flog_index_test);
	max_ptr_in_index_meta = polar_get_flog_index_meta_max_ptr(flog_index_test);

	polar_flog_index_insert(flog_index_test, flog_index_queue_test, buf_ctl_test, write_result, ANY);
	polar_flog_index_insert(flog_index_test, NULL, buf_ctl_test, write_result, ANY);

	Assert(polar_get_flog_index_max_ptr(flog_index_test) < write_result);
	Assert(polar_get_flog_index_max_ptr(flog_index_test) > max_ptr_in_index_mem);

	polar_logindex_flush_table(flog_index_test, write_result);
	Assert(polar_get_flog_index_meta_max_ptr(flog_index_test) >= max_ptr_in_index_meta);
	Assert(polar_get_flog_index_meta_max_ptr(flog_index_test) < write_result);
}

static void
test_flog_prealloc_files(int num)
{
	uint64 max_segno;

	max_segno = buf_ctl_test->max_seg_no;
	polar_prealloc_flog_files(buf_ctl_test, num);
	check_flog_prealloc_files(max_segno + num);
}

static void
test_insert_flog_from_bp(int test_bp_buf_nums, bool is_recovery)
{
	int i;
	int j = 0;
	int bufs[test_bp_buf_nums];
	XLogRecPtr fbpoint_lsn;
	polar_logindex_redo_ctl_data_t redo_ins;

	fbpoint_lsn = polar_get_curr_fbpoint_lsn(buf_ctl_test);
	polar_get_local_fbpoint_lsn(buf_ctl_test, InvalidXLogRecPtr, fbpoint_lsn);

	/* Set parallel replaying, but it may be not standby. Sorry for that */
	pg_atomic_write_u32(&(redo_ins.bg_redo_state), POLAR_BG_PARALLEL_REPLAYING);;

	/* Test the origin buffer full */
	for (i = 0; i < NBuffers && j < test_bp_buf_nums; i++)
	{
		BufferDesc *buf_hdr;
		XLogRecPtr replay_lsn = InvalidXLogRecPtr;
		bool run_extra_case = false;
		bool inserted = true;
		bool check_buf_redo_state = true;

		buf_hdr = test_lock_buffer_in_bp(i);

		if (buf_hdr == NULL)
			continue;

		run_extra_case = (j&1);

		if (is_recovery)
		{
			if (run_extra_case)
			{
				replay_lsn = polar_get_curr_fbpoint_lsn(buf_ctl_test) - 1;
				set_buf_flog_state(buf_hdr, POLAR_BUF_FLOG_DISABLE);
				inserted = false;
			}
			else
				replay_lsn = polar_get_curr_fbpoint_lsn(buf_ctl_test) + 1;
		}

		if (POLAR_CHECK_BUF_FLOG_STATE(buf_hdr, POLAR_BUF_IN_FLOG_LIST))
		{
			check_buf_redo_state = false;
			inserted = false;
		}

		if (is_recovery)
		{
			if (polar_is_buf_flog_enabled(test_instance, i + 1) &&
					polar_is_flog_needed(test_instance, &redo_ins, buf_hdr->tag.forkNum,
							BufHdrGetBlock(buf_hdr), pg_atomic_read_u32(&buf_hdr->state) & BM_PERMANENT,
							InvalidXLogRecPtr, replay_lsn))
				polar_flog_insert(test_instance, buf_hdr->buf_id + 1, false, is_recovery);
			else
				inserted = false;
		}
		else if (polar_is_flog_needed(test_instance, &redo_ins, buf_hdr->tag.forkNum,
				BufHdrGetBlock(buf_hdr), pg_atomic_read_u32(&buf_hdr->state) & BM_PERMANENT,
				InvalidXLogRecPtr, InvalidXLogRecPtr))
		{
			polar_flog_insert(test_instance, buf_hdr->buf_id + 1, false, is_recovery);
		}
		else
		{
			polar_flog_insert(test_instance, buf_hdr->buf_id + 1, true, is_recovery);
		}

		if (inserted)
		{
			check_flog_list_insert(i, fbpoint_lsn);
			bufs[j] = i;
			j++;
		}
		else
		{
			check_flog_list_clean(i, true, check_buf_redo_state);
			LWLockRelease(BufferDescriptorGetContentLock(buf_hdr));
		}
	}

	for (i = 0; i < j; i++)
	{
		int buf_id = bufs[i];
		BufferDesc *buf_hdr =  GetBufferDescriptor(buf_id);

		polar_flush_buf_flog_rec(buf_hdr, test_instance, false);
		check_flog_list_clean(buf_hdr->buf_id, true, true);
		LWLockRelease(BufferDescriptorGetContentLock(buf_hdr));
	}
}

static void
test_flashback_log_online_promote(void)
{
	polar_logindex_redo_ctl_data_t redo_instance;
	int i;
	int j = 0;
	XLogRecPtr fbpoint_lsn_perior;

	/* Recovery */
	test_flog_crash_recovery();
	/* Mark a fake flashback point lsn */
	fbpoint_lsn_perior = polar_get_curr_fbpoint_lsn(buf_ctl_test);

	/* Mark online promote */
	pg_atomic_write_u32(&(redo_instance.bg_redo_state), POLAR_BG_ONLINE_PROMOTE);

	/* Test the origin buffer full */
	for (i = 0; i < NBuffers; i++)
	{
		BufferDesc *buf_hdr;
		bool inserted = true;
		bool check_buf_redo_state = true;

		buf_hdr = test_lock_buffer_in_bp(i);

		if (buf_hdr == NULL)
			continue;

		/* Mark a fake local flashback point lsn */
		polar_get_local_fbpoint_lsn(buf_ctl_test, InvalidXLogRecPtr, 1);

		if ((j & 1) ||
				POLAR_CHECK_BUF_FLOG_STATE(buf_hdr, POLAR_BUF_FLOG_LOST_CHECKED))
		{
			polar_set_buf_flog_lost_checked(test_instance, &redo_instance, i + 1);
			inserted = false;
		}

		if (POLAR_CHECK_BUF_FLOG_STATE(buf_hdr, POLAR_BUF_IN_FLOG_LIST))
		{
			inserted = false;
			check_buf_redo_state = false;
		}

		if (polar_may_buf_lost_flog(test_instance, &redo_instance, buf_hdr))
		{
			polar_flog_insert(test_instance, i + 1, true, false);
		}
		else
			inserted = false;

		if (inserted)
		{
			check_flog_list_insert(i, 1);
			j++;
		}
		else
			check_flog_list_clean(i, true, check_buf_redo_state);

		LWLockRelease(BufferDescriptorGetContentLock(buf_hdr));
	}

	/* Recovery the flashback point */
	buf_ctl_test->wal_info.fbpoint_lsn = fbpoint_lsn_perior;
}

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	RequestAddinShmemSpace(get_test_shmem_size());
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = test_flog_shmem_startup;
}

void
_PG_fini(void)
{
	shmem_startup_hook = prev_shmem_startup_hook;
}

PG_FUNCTION_INFO_V1(test_flashback_log);
/*
 * SQL-callable entry point to perform all tests.
 */
Datum
test_flashback_log(PG_FUNCTION_ARGS)
{
	polar_flog_rec_ptr ptr;
	flog_ctl_file_data_t ctl_file_data;
	fbpoint_wal_info_data_t wal_info;

	if (!polar_is_flog_enabled(flog_instance))
		PG_RETURN_VOID();

	/* Test flashback log init */
	test_flog_init(true);

	/* Test the flashback log start up */
	wal_info.fbpoint_lsn = 0;
	init_flog_buf_ctl_data(&ctl_file_data, POLAR_INVALID_FLOG_SEGNO, 0, 0, 0, wal_info, true);
	test_flog_startup(false, ctl_file_data);

	/* Test the flashback log recover */
	test_flog_recover();

	/* Test the flashback log insert to buffer */
	ptr = test_flog_insert_buffer(POLAR_INVALID_FLOG_REC_PTR, true, true, INT_MAX);

	/* Test the flashback log write */
	test_flog_write(ptr);

	/* Test the flashback log read */
	test_flog_read(FLOG_LONG_PHD_SIZE);

	/* Test the flashback logindex write */
	test_flog_index_write(ptr);

	/* Test the flashback logindex search */
	test_flog_index_search(POLAR_INVALID_FLOG_REC_PTR);

	/* Test the flashback log checkpoint (normal) */
	test_flog_checkpoint(false);

	/* Test the flashback log insert to list with origin buffer */
	test_flog_insert_list(true);

	/* Test the buffer sync the flashback log record */
	test_buf_flog_rec_sync(test_bufs, true);

	/* Test the origin buffer full */
	test_origin_buf_full(POLAR_ORIGIN_PAGE_BUF_NUM);

	/* Test flashback log crash recovery */
	test_flog_crash_recovery();

	/* Test flashback log repaire the buffer */
	test_flog_repair_buffer(test_bufs);

	/* Test insert to flashback log from buffer pool in recovery (a standby or crash recovery). */
	test_insert_flog_from_bp(100, true);

	/* Test insert to flashback log from buffer pool. */
	test_insert_flog_from_bp(100, false);

	/* Test flashback log shutdown recovery */
	test_flog_shutdown_recovery();

	/* Test flashback log background work function */
	test_flog_bg_worker_functions();

	/* Test flashback log prealloc files */
	test_flog_prealloc_files(TEST_FLOG_PREALLOC_FILE_NUM);

	/* Test flashback log online promote */
	test_flashback_log_online_promote();

	/* test remove the flashback data */
	polar_remove_all_flog_data(test_instance);
	check_flog_dir_validate(TEST_FLOG_NAME, true);

	PG_RETURN_VOID();
}
