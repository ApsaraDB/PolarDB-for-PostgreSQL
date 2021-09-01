/*-------------------------------------------------------------------------
 *
 * test_logindex.c
 *  Implementation of test case for wal logindex.
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
 *  src/test/modules/test_logindex/test_logindex.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <time.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

#include "access/heapam.h"
#include "access/polar_fullpage.h"
#include "access/polar_logindex.h"
#include "access/polar_logindex_internal.h"
#include "access/xlog_internal.h"
#include "catalog/pg_control.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/bgwriter.h"
#include "postmaster/postmaster.h"
#include "pgstat.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/polar_bitpos.h"
#include "utils/ps_status.h"


PG_MODULE_MAGIC;

#define LSN_TEST_STEP 1000
#define TEST_MAX_BLOCK_NUMBER 10
static volatile sig_atomic_t got_SIGHUP = false;
static volatile sig_atomic_t start_test = false;
static XLogRecPtr   test_max_lsn = InvalidXLogRecPtr;
static XLogRecPtr   test_start_lsn[TEST_MAX_BLOCK_NUMBER];
static XLogRecPtr   test_end_lsn[TEST_MAX_BLOCK_NUMBER];
static XLogRecPtr   test_only_two[2];

void test_logindex_worker_main(Datum main_arg);
static void test_write_fullpage(log_index_snapshot_t *logindex_snapshot);

static void
test_insert_lsn_to_mem(log_index_snapshot_t *logindex_snapshot, BufferTag *tag, XLogRecPtr max_lsn, uint32 key)
{
	log_mem_table_t     *table;
	log_item_head_t *item;
	log_item_seg_t  *item_seg;
	int i, j;
	XLogRecPtr      plsn = InvalidXLogRecPtr;
	log_index_lsn_t lsn_info;

	/* Test insert into log_item_head_t */
	for (i = 1; i <= LOG_INDEX_ITEM_HEAD_LSN_NUM; i++)
	{
		lsn_info.tag = tag;
		lsn_info.lsn = LSN_TEST_STEP * i + max_lsn;
		lsn_info.prev_lsn = plsn;
		log_index_insert_lsn(logindex_snapshot, &lsn_info, key);
		plsn = LSN_TEST_STEP * i + max_lsn;
	}

	table = LOG_INDEX_MEM_TBL_ACTIVE();

	Assert(LOG_INDEX_MEM_TBL_STATE(table) == LOG_INDEX_MEM_TBL_STATE_ACTIVE);

	item = log_index_tbl_find(tag, &table->data, key);

	Assert(item != NULL);
	Assert(item->number == LOG_INDEX_ITEM_HEAD_LSN_NUM);

	for (i = 1; i <= LOG_INDEX_ITEM_HEAD_LSN_NUM; i++)
		Assert(LOG_INDEX_COMBINE_LSN(&table->data, item->suffix_lsn[i - 1]) == LSN_TEST_STEP * i + max_lsn);

	Assert(LOG_INDEX_MEM_ITEM_IS(
			   LOG_INDEX_ITEM_HEAD(&table->data,
								   LOG_INDEX_TBL_SLOT_VALUE(&table->data, key)),
			   tag));

	Assert(item->next_seg == LOG_INDEX_TBL_INVALID_SEG);

	/* Test insert log_item_seg_t after log_item_head_t */
	for (j = 0; j < LOG_INDEX_ITEM_SEG_LSN_NUM; i++, j++)
	{
		lsn_info.tag = tag;
		lsn_info.lsn = LSN_TEST_STEP * i + max_lsn;
		lsn_info.prev_lsn = plsn;
		log_index_insert_lsn(logindex_snapshot, &lsn_info, key);
		plsn = LSN_TEST_STEP * i + max_lsn;
	}

	Assert(item->next_seg != LOG_INDEX_TBL_INVALID_SEG);

	item_seg = LOG_INDEX_ITEM_SEG(&table->data, item->next_seg);
	Assert(item_seg->number == LOG_INDEX_ITEM_SEG_LSN_NUM);
	Assert(item_seg->next_seg == LOG_INDEX_TBL_INVALID_SEG);

	for (j = 0, i = LOG_INDEX_ITEM_HEAD_LSN_NUM + 1;
			j < LOG_INDEX_ITEM_SEG_LSN_NUM; j++, i++)
		Assert(LOG_INDEX_COMBINE_LSN(&table->data, item_seg->suffix_lsn[j]) == i * LSN_TEST_STEP + max_lsn);

	/* Test insert log_item_seg_t after log_item_seg_t */

	for (j = 0; j < LOG_INDEX_ITEM_SEG_LSN_NUM; i++, j++)
	{
		lsn_info.tag = tag;
		lsn_info.lsn = LSN_TEST_STEP * i + max_lsn;
		lsn_info.prev_lsn = plsn;
		log_index_insert_lsn(logindex_snapshot, &lsn_info, key);
		plsn = LSN_TEST_STEP * i + max_lsn;
	}

	test_max_lsn = LSN_TEST_STEP * (i - 1) + max_lsn;

	Assert(item_seg->next_seg != LOG_INDEX_TBL_INVALID_SEG);

	item_seg = LOG_INDEX_ITEM_SEG(&table->data, item_seg->next_seg);

	Assert(item_seg->number == LOG_INDEX_ITEM_SEG_LSN_NUM);
	Assert(item_seg->next_seg == LOG_INDEX_TBL_INVALID_SEG);

	for (j = 0, i =
				(LOG_INDEX_ITEM_HEAD_LSN_NUM + LOG_INDEX_ITEM_SEG_LSN_NUM) + 1;
			j < LOG_INDEX_ITEM_HEAD_LSN_NUM; j++, i++)
		Assert(LOG_INDEX_COMBINE_LSN(&table->data, item_seg->suffix_lsn[j]) == i * LSN_TEST_STEP + max_lsn);
}

static void
test_insert_one_mem_table_full(log_index_snapshot_t *logindex_snapshot, BufferTag *tag)
{
	XLogRecPtr  start_lsn = test_max_lsn;
	int i, total;
	struct timespec start_time, end_time;
	long cost;
	uint32 tid;
	log_mem_table_t *table;
	XLogRecPtr plsn = InvalidXLogRecPtr;

	total = LOG_INDEX_MEM_TBL_SEG_NUM * LOG_INDEX_ITEM_SEG_LSN_NUM;

	clock_gettime(CLOCK_MONOTONIC, &start_time);

	for (i = 1; i <= total; i++)
	{
		polar_log_index_add_lsn(logindex_snapshot, tag, plsn, LSN_TEST_STEP * i + start_lsn);
		plsn = LSN_TEST_STEP * i + start_lsn;
	}

	test_max_lsn = LSN_TEST_STEP * (i - 1) + start_lsn;

	start_lsn = test_max_lsn;

	clock_gettime(CLOCK_MONOTONIC, &end_time);

	cost = (end_time.tv_sec - start_time.tv_sec) * 1000000000 +
		   (end_time.tv_nsec - start_time.tv_nsec);

	ereport(LOG, (errmsg(
					  "insert %d ,cost %ld, qps=%ld", total, cost,
					  total * (1000000000 / cost))));


	sleep(1);
	tid = LOG_INDEX_MEM_TBL_ACTIVE_ID;
	table = LOG_INDEX_MEM_TBL(LOG_INDEX_MEM_TBL_PREV_ID(tid));
	Assert(LOG_INDEX_MEM_TBL_FREE_HEAD(table)
		   == LOG_INDEX_MEM_TBL_SEG_NUM);
	ereport(LOG, (errmsg("mem tbl state %d", LOG_INDEX_MEM_TBL_STATE(table))));
	Assert(LOG_INDEX_MEM_TBL_STATE(table)
		   == LOG_INDEX_MEM_TBL_STATE_FLUSHED);
	Assert(LOG_INDEX_MEM_TBL_TID(table) == 1);
	table = LOG_INDEX_MEM_TBL(tid);
	Assert(LOG_INDEX_MEM_TBL_STATE(table)
		   == LOG_INDEX_MEM_TBL_STATE_ACTIVE);
	Assert(LOG_INDEX_MEM_TBL_TID(table) == 2);

	table = LOG_INDEX_MEM_TBL(LOG_INDEX_MEM_TBL_NEXT_ID(tid));
	Assert(LOG_INDEX_MEM_TBL_STATE(table)
		   == LOG_INDEX_MEM_TBL_STATE_FREE);
	Assert(LOG_INDEX_MEM_TBL_TID(table)
		   == LOG_INDEX_TABLE_INVALID_ID);

	for (i = 1; i <= total; i++)
	{
		polar_log_index_add_lsn(logindex_snapshot, tag, plsn, LSN_TEST_STEP * i + start_lsn);
		plsn = LSN_TEST_STEP * i + start_lsn;
	}

	test_max_lsn = LSN_TEST_STEP * (i - 1) + start_lsn;
}

static void
test_lsn_iterator(log_index_snapshot_t *logindex_snapshot, log_index_lsn_iter_t iter, XLogRecPtr start, XLogRecPtr end)
{
	XLogRecPtr i;
	log_index_lsn_t *lsn_info;

	for (i = start; i <= end; i += LSN_TEST_STEP)
	{
		lsn_info = polar_log_index_lsn_iterator_next(logindex_snapshot, iter);
		Assert(lsn_info != NULL);
		Assert(lsn_info->lsn == i);
	}
}

static void
test_lsn_iterate_from_invalid(log_index_snapshot_t *logindex_snapshot)
{
	log_index_lsn_iter_t lsn_iter = polar_log_index_create_lsn_iterator(logindex_snapshot, InvalidXLogRecPtr);
	test_lsn_iterator(logindex_snapshot, lsn_iter, LSN_TEST_STEP, test_max_lsn);

	polar_log_index_release_lsn_iterator(lsn_iter);

}

static void
test_lsn_iterate_from_super_max(log_index_snapshot_t *logindex_snapshot)
{
	log_index_lsn_t *lsn_info;

	log_index_lsn_iter_t lsn_iter = polar_log_index_create_lsn_iterator(logindex_snapshot, test_max_lsn + 1);
	lsn_info = polar_log_index_lsn_iterator_next(logindex_snapshot, lsn_iter);
	Assert(lsn_info == NULL);

	polar_log_index_release_lsn_iterator(lsn_iter);

}

static void
test_lsn_iterate_from_table_interval(log_index_snapshot_t *logindex_snapshot)
{
	log_index_lsn_t *lsn_info;
	BufferTag tag;
	XLogRecPtr start_lsn = InvalidXLogRecPtr, plsn = InvalidXLogRecPtr;
	int i;
	uint32 total = LOG_INDEX_MEM_TBL_SEG_NUM * LOG_INDEX_ITEM_SEG_LSN_NUM;
	log_mem_table_t *prev_tbl;
	log_index_lsn_iter_t lsn_iter;

	tag.rnode.spcNode = 10;
	tag.rnode.dbNode = 11;
	tag.rnode.relNode = 12;
	tag.forkNum = MAIN_FORKNUM;

	tag.blockNum = 8;

	// insert one table
	for (i = 1; i <= total; i++)
	{
		polar_log_index_add_lsn(logindex_snapshot, &tag, plsn, LSN_TEST_STEP * i + test_max_lsn);
		plsn = LSN_TEST_STEP * i + test_max_lsn;
	}

	test_max_lsn += (LSN_TEST_STEP * (i - 1));

	prev_tbl = LOG_INDEX_MEM_TBL(LOG_INDEX_MEM_TBL_ACTIVE_ID - 1);
	start_lsn = prev_tbl->data.max_lsn;

	lsn_iter = polar_log_index_create_lsn_iterator(logindex_snapshot, start_lsn + (LSN_TEST_STEP / 2));
	lsn_info = polar_log_index_lsn_iterator_next(logindex_snapshot, lsn_iter);
	Assert(lsn_info != NULL);
	Assert(lsn_info->lsn == start_lsn + LSN_TEST_STEP);
}

static void
test_lsn_iterate_from_file(log_index_snapshot_t *logindex_snapshot, XLogRecPtr start)
{
	log_index_lsn_iter_t lsn_iter = polar_log_index_create_lsn_iterator(logindex_snapshot, start);
	test_lsn_iterator(logindex_snapshot, lsn_iter, start, test_max_lsn);

	polar_log_index_release_lsn_iterator(lsn_iter);
}

static void
test_insert_lsn_after_force_flush(log_index_snapshot_t *logindex_snapshot)
{
	BufferTag tag;
	uint32 total = LOG_INDEX_MEM_TBL_SEG_NUM * LOG_INDEX_ITEM_SEG_LSN_NUM;
	log_index_lsn_iter_t   lsn_iter = polar_log_index_create_lsn_iterator(logindex_snapshot, test_max_lsn + LSN_TEST_STEP);
	XLogRecPtr plsn = InvalidXLogRecPtr;
	log_mem_table_t *table = LOG_INDEX_MEM_TBL_ACTIVE();
	log_mem_table_t *next_table;
	XLogRecPtr start_lsn = test_max_lsn + LSN_TEST_STEP;
	uint32 i;

	tag.rnode.spcNode = 10;
	tag.rnode.dbNode = 11;
	tag.rnode.relNode = 12;
	tag.forkNum = MAIN_FORKNUM;
	tag.blockNum = 100;


	for (i = 1; i <= total; i++)
	{
		polar_log_index_add_lsn(logindex_snapshot, &tag, plsn, LSN_TEST_STEP * i + test_max_lsn);
		plsn = LSN_TEST_STEP * i + test_max_lsn;
	}

	test_max_lsn += (LSN_TEST_STEP * (i - 1));
	next_table = LOG_INDEX_MEM_TBL_ACTIVE();

	Assert((LOG_INDEX_MEM_TBL_TID(next_table) - LOG_INDEX_MEM_TBL_TID(table)) == 1);
	test_lsn_iterator(logindex_snapshot, lsn_iter, start_lsn, test_max_lsn);

	polar_log_index_release_lsn_iterator(lsn_iter);
}

#define MAX_FULL_FILE_TEST 5
static XLogRecPtr test_full_lsn[MAX_FULL_FILE_TEST];

static void
test_insert_file_full(log_index_snapshot_t *logindex_snapshot)
{
	BufferTag tag;
	uint32 total = LOG_INDEX_MEM_TBL_SEG_NUM * LOG_INDEX_ITEM_SEG_LSN_NUM;
	int i, j, k;
	XLogRecPtr plsn = InvalidXLogRecPtr;
	struct timespec start_time, end_time;
	long cost;

	tag.rnode.spcNode = 10;
	tag.rnode.dbNode = 11;
	tag.rnode.relNode = 12;
	tag.forkNum = MAIN_FORKNUM;
	tag.blockNum = 100;

	clock_gettime(CLOCK_MONOTONIC, &start_time);

	for (k = 0; k < MAX_FULL_FILE_TEST; k++)
	{
		for (j = 0; j < LOG_INDEX_TABLE_NUM_PER_FILE; j++)
		{
			for (i = 1; i <= total; i++)
			{
				polar_log_index_add_lsn(logindex_snapshot, &tag, plsn, LSN_TEST_STEP * i + test_max_lsn);
				plsn = LSN_TEST_STEP * i + test_max_lsn;
			}

			test_max_lsn += (LSN_TEST_STEP * (i - 1));
		}

		test_full_lsn[k] = test_max_lsn;
	}

	clock_gettime(CLOCK_MONOTONIC, &end_time);
	cost = (end_time.tv_sec - start_time.tv_sec) * 1000000000 +
		   (end_time.tv_nsec - start_time.tv_nsec);

	ereport(LOG, (errmsg("Insert full file test, cost=%.2lf,qps=%.2lf", cost / 1000000000.0,
						 MAX_FULL_FILE_TEST * LOG_INDEX_TABLE_NUM_PER_FILE * total
						 / (cost / 1000000000.0))));
}

static void
test_insert_lsn(log_index_snapshot_t *logindex_snapshot)
{
	BufferTag tag;
	log_item_head_t *item1, *item9;
	log_item_head_t *item;
	log_mem_table_t *table = LOG_INDEX_MEM_TBL_ACTIVE();
	uint32 key;
	log_index_lsn_t lsn_info;
	log_index_lsn_iter_t   lsn_iter = polar_log_index_create_lsn_iterator(logindex_snapshot, test_max_lsn + LSN_TEST_STEP);

	tag.rnode.spcNode = 10;
	tag.rnode.dbNode = 11;
	tag.rnode.relNode = 12;
	tag.forkNum = MAIN_FORKNUM;

	Assert(lsn_iter != NULL);

	test_start_lsn[1] = test_max_lsn;
	tag.blockNum = 1;
	test_insert_lsn_to_mem(logindex_snapshot, &tag, test_max_lsn, LOG_INDEX_MEM_TBL_HASH_PAGE(&tag));
	test_end_lsn[1] = test_max_lsn;

	test_lsn_iterator(logindex_snapshot, lsn_iter, test_start_lsn[1] + LSN_TEST_STEP, test_end_lsn[1]);

	tag.blockNum = 2;
	test_start_lsn[2] = test_max_lsn;
	test_insert_lsn_to_mem(logindex_snapshot, &tag, test_max_lsn, LOG_INDEX_MEM_TBL_HASH_PAGE(&tag));
	test_end_lsn[2] = test_max_lsn;

	test_lsn_iterator(logindex_snapshot, lsn_iter, test_start_lsn[2] + LSN_TEST_STEP, test_end_lsn[2]);
	/* Test hash conflict */
	tag.blockNum = 1;
	key = LOG_INDEX_MEM_TBL_HASH_PAGE(&tag);
	tag.blockNum = 9;
	test_start_lsn[9] = test_max_lsn;
	test_insert_lsn_to_mem(logindex_snapshot, &tag, test_max_lsn, key);
	test_end_lsn[9] = test_max_lsn;
	test_lsn_iterator(logindex_snapshot, lsn_iter, test_start_lsn[9] + LSN_TEST_STEP, test_end_lsn[9]);

	tag.blockNum = 1;
	Assert(log_index_tbl_find(&tag, &(LOG_INDEX_MEM_TBL_ACTIVE()->data),
							  LOG_INDEX_MEM_TBL_HASH_PAGE(&tag))
		   != NULL);

	tag.blockNum = 2;
	Assert(log_index_tbl_find(&tag, &(LOG_INDEX_MEM_TBL_ACTIVE()->data),
							  LOG_INDEX_MEM_TBL_HASH_PAGE(&tag))
		   != NULL);


	/* Test find non-exists item */
	tag.blockNum = 3;
	Assert(log_index_tbl_find(&tag, &(LOG_INDEX_MEM_TBL_ACTIVE()->data),
							  LOG_INDEX_MEM_TBL_HASH_PAGE(&tag))
		   == NULL);


	/* Test hash conflict */
	tag.blockNum = 1;
	key = LOG_INDEX_MEM_TBL_HASH_PAGE(&tag);
	item1 = log_index_tbl_find(&tag, &table->data, key);
	tag.blockNum = 9;
	item9 = log_index_tbl_find(&tag, &table->data, key);

	Assert(item1->next_item == LOG_INDEX_TBL_INVALID_SEG);
	item = LOG_INDEX_ITEM_HEAD(&table->data, item9->next_item);
	Assert(item == item1);
	item = LOG_INDEX_ITEM_HEAD(&table->data, LOG_INDEX_TBL_SLOT_VALUE(&table->data, key));
	Assert(item == item9);


	/* Test insert only one lsn */
	tag.rnode.spcNode = 9;
	tag.blockNum = 3;

	lsn_info.tag = &tag;
	lsn_info.lsn = test_max_lsn + LSN_TEST_STEP;
	lsn_info.prev_lsn = InvalidXLogRecPtr;
	log_index_insert_lsn(logindex_snapshot, &lsn_info, LOG_INDEX_MEM_TBL_HASH_PAGE(&tag));

	test_only_two[0] = test_max_lsn + LSN_TEST_STEP;
	test_max_lsn += LSN_TEST_STEP;

	test_lsn_iterator(logindex_snapshot, lsn_iter, test_only_two[0], test_only_two[0]);
	/* Test insert one mem table full */
	tag.rnode.spcNode = 10;
	tag.blockNum = 3;

	{
		XLogRecPtr start = test_max_lsn;
		test_insert_one_mem_table_full(logindex_snapshot, &tag);
		test_lsn_iterator(logindex_snapshot, lsn_iter, start + LSN_TEST_STEP, test_max_lsn);
	}

	/* Test insert the other one lsn */
	tag.rnode.spcNode = 9;
	tag.blockNum = 3;
	lsn_info.lsn = test_max_lsn + LSN_TEST_STEP;
	lsn_info.prev_lsn = test_only_two[0];
	log_index_insert_lsn(logindex_snapshot, &lsn_info, LOG_INDEX_MEM_TBL_HASH_PAGE(&tag));

	test_only_two[1] = test_max_lsn + LSN_TEST_STEP;
	test_lsn_iterator(logindex_snapshot, lsn_iter, test_only_two[1], test_only_two[1]);
	polar_log_index_release_lsn_iterator(lsn_iter);

	test_max_lsn += LSN_TEST_STEP;
}

#define TEST_PROCESS_TOTAL 10000

#ifdef LOG_INDEX_PARALLEL_INSERT
static void
test_logindex_process(log_index_snapshot_t *logindex_snapshot, BufferTag *tag, XLogRecPtr start_lsn, XLogRecPtr interval)
{
	int i;
	struct timespec start_time, end_time;
	long cost;
	int total = TEST_PROCESS_TOTAL;
	XLogRecPtr plsn = InvalidXLogRecPtr;
	log_index_lsn_t lsn_info;

	clock_gettime(CLOCK_MONOTONIC, &start_time);

	for (i = 0; i < total; i++)
	{
		lsn_info.tag = tag;
		lsn_info.lsn = LSN_TEST_STEP * i + interval + start_lsn;
		lsn_info.prev_lsn = plsn;
		log_index_insert_lsn(logindex_snapshot, &lsn_info, LOG_INDEX_MEM_TBL_HASH_PAGE(tag));
		plsn = LSN_TEST_STEP * i + interval + start_lsn;
	}

	clock_gettime(CLOCK_MONOTONIC, &end_time);

	cost = (end_time.tv_sec - start_time.tv_sec) * 1000000000 +
		   (end_time.tv_nsec - start_time.tv_nsec);

	ereport(LOG, (errmsg(
					  "block %d start_lsn %ld interval %ld insert %d ,cost %ld, qps=%ld",
					  tag->blockNum, start_lsn, interval, total, cost,
					  total * (1000000000 / cost))));

}

static void
test_logindex_sigterm(SIGNAL_ARGS)
{
	int save_errno = errno;
	SetLatch(MyLatch);
	errno = save_errno;
}

static void
test_logindex_sigusr2(SIGNAL_ARGS)
{
	int save_errno = errno;

	start_test = true;
	SetLatch(MyLatch);
	errno = save_errno;
}

#define TEST_PROCESS_START_LSN 41042000
#define TEST_INTERVAL 100
void
test_logindex_worker_main(Datum main_arg)
{
	BufferTag tag;
	int interval;

	test_max_lsn = TEST_PROCESS_START_LSN;

	tag.rnode.spcNode = 11;
	tag.rnode.dbNode = 11;
	tag.rnode.relNode = 12;
	tag.forkNum = MAIN_FORKNUM;
	tag.blockNum = DatumGetInt32(main_arg);
	interval = tag.blockNum * TEST_INTERVAL;

	ereport(LOG, (errmsg("Call worker_main")));

	pqsignal(SIGTERM, test_logindex_sigterm);
	pqsignal(SIGUSR2, test_logindex_sigusr2);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	while (!start_test)
	{
		int rc;

		rc = WaitLatch(&MyProc->procLatch, WL_LATCH_SET |
					   WL_POSTMASTER_DEATH,
					   -1L,
					   PG_WAIT_EXTENSION);

		/* Reset the latch, bail out if postmaster died, otherwise loop. */
		ResetLatch(&MyProc->procLatch);

		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
	}

	ereport(LOG, (errmsg("block %d test_max_lsn %ld", tag.blockNum, test_max_lsn)));
	test_logindex_process(POLAR_LOGINDEX_WAL_SNAPSHOT, &tag, test_max_lsn, interval);
	proc_exit(0);
}


#define TEST_WORKER_NUM 5
static void
test_logindex_bgworker()
{
	BackgroundWorker worker;
	int i;
	BackgroundWorkerHandle *handle[TEST_WORKER_NUM];
	pid_t                   pid[TEST_WORKER_NUM];
	BgwHandleStatus status;
	struct timespec start_time, end_time;
	long cost;
	int total;

	/* set up common data for all our workers */
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
					   BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	sprintf(worker.bgw_library_name, "test_logindex");
	sprintf(worker.bgw_function_name, "test_logindex_worker_main");
	worker.bgw_notify_pid = MyProcPid;

	for (i = 0; i < TEST_WORKER_NUM; i++)
	{
		uint32 block = i + 1;
		snprintf(worker.bgw_name, BGW_MAXLEN, "test_logindex %d", i);
		snprintf(worker.bgw_type, BGW_MAXLEN, "test_logindex");
		worker.bgw_main_arg = Int32GetDatum(block);

		test_start_lsn[block] = TEST_PROCESS_START_LSN + block * TEST_INTERVAL;

		test_end_lsn[block] = TEST_PROCESS_START_LSN +
							  block * TEST_INTERVAL + TEST_PROCESS_TOTAL * LSN_TEST_STEP;
		Assert(RegisterDynamicBackgroundWorker(&worker, &handle[i]));
		status = WaitForBackgroundWorkerStartup(handle[i], &pid[i]);

		Assert(status == BGWH_STARTED);
	}

	sleep(1);

	clock_gettime(CLOCK_MONOTONIC, &start_time);

	for (i = 0; i < TEST_WORKER_NUM; i++)
		kill(pid[i], SIGUSR2);

	//Wait all child process exit
	for (i = 0; i < TEST_WORKER_NUM; i++)
		Assert(WaitForBackgroundWorkerShutdown(handle[i]) == BGWH_STOPPED);

	clock_gettime(CLOCK_MONOTONIC, &end_time);

	cost = (end_time.tv_sec - start_time.tv_sec) * 1000000000 +
		   (end_time.tv_nsec - start_time.tv_nsec);
	total = TEST_PROCESS_TOTAL * TEST_WORKER_NUM;

	ereport(LOG, (errmsg(
					  "total insert %d ,cost %ld, qps=%ld", total, cost,
					  total * (1000000000 / cost))));

}

static void
test_iterate_bgworker_block_lsn(log_index_snapshot_t *logindex_snapshot, BlockNumber block)
{
	log_index_page_iter_t iter;
	BufferTag   tag;
	XLogRecPtr  min_lsn;
	bool found = false;
	log_index_lsn_t *lsn_info;

	tag.rnode.spcNode = 11;
	tag.rnode.dbNode = 11;
	tag.rnode.relNode = 12;
	tag.forkNum = MAIN_FORKNUM;
	tag.blockNum = block;
	iter = polar_log_index_create_page_iterator(logindex_snapshot, &tag,
												test_start_lsn[block], test_end_lsn[block]);

	Assert(iter->state == ITERATE_STATE_FINISHED);
	Assert(iter != NULL);

	min_lsn = test_start_lsn[block];

	while ((lsn_info = polar_log_index_page_iterator_next(iter)) != NULL)
	{
		if (lsn_info->lsn != min_lsn)
			ereport(LOG, (errmsg("block %d lsn %ld min_lsn %ld", block, lsn, min_lsn)));

		Assert(lsn_info->lsn == min_lsn);
		min_lsn += LSN_TEST_STEP;
		found = true;
	}

	if (!found)
		ereport(LOG, (errmsg("block %d", block)));

	Assert(found);
	polar_log_index_release_page_iterator(iter);
}

static void
test_iterate_parallel_lsn(log_index_snapshot_t *logindex_snapshot)
{
	BlockNumber block;

	for (block = 1; block <= TEST_WORKER_NUM; block++)
		test_iterate_bgworker_block_lsn(logindex_snapshot, block);
}

#endif

static void
test_iterate_non_exists_block_lsn(log_index_snapshot_t *logindex_snapshot, BlockNumber block)
{
	log_index_page_iter_t iter;
	BufferTag       tag;

	tag.rnode.spcNode = 10;
	tag.rnode.dbNode = 11;
	tag.rnode.relNode = 12;
	tag.forkNum = MAIN_FORKNUM;
	tag.blockNum = block;

	iter = polar_log_index_create_page_iterator(logindex_snapshot, &tag,
												LSN_TEST_STEP, test_max_lsn);

	Assert(iter->state == ITERATE_STATE_FINISHED);

	Assert(iter != NULL);
	Assert(polar_log_index_page_iterator_end(iter));
	Assert(polar_log_index_page_iterator_next(iter) == NULL);
	polar_log_index_release_page_iterator(iter);
}


static void
test_iterate_in_table0(log_index_snapshot_t *logindex_snapshot)
{
	BufferTag   tag;
	log_index_page_iter_t iter;
	int i = LSN_TEST_STEP;
	XLogRecPtr min_lsn = LSN_TEST_STEP;
	XLogRecPtr max_lsn =
		(LOG_INDEX_ITEM_SEG_LSN_NUM * 2 + LOG_INDEX_ITEM_HEAD_LSN_NUM) * LSN_TEST_STEP;
	log_index_lsn_t *lsn_info;

	max_lsn = LSN_TEST_STEP * 2;
	/*
	 * In this test case we first push lsn in set [min_lsn, max_lsn]
	 * Create iterator to get this set
	 */
	tag.rnode.spcNode = 10;
	tag.rnode.dbNode = 11;
	tag.rnode.relNode = 12;
	tag.forkNum = MAIN_FORKNUM;
	tag.blockNum = 1;

	iter = polar_log_index_create_page_iterator(logindex_snapshot, &tag, min_lsn, max_lsn);

	Assert(iter->state == ITERATE_STATE_FINISHED);

	while ((lsn_info = polar_log_index_page_iterator_next(iter)) != NULL)
	{
		if (i == LSN_TEST_STEP)
			Assert(lsn_info->prev_lsn == InvalidXLogRecPtr);
		else
			Assert(lsn_info->prev_lsn == i - LSN_TEST_STEP);

		Assert(lsn_info->lsn == i);
		i += LSN_TEST_STEP;
	}

	Assert(i == max_lsn + LSN_TEST_STEP);
	polar_log_index_release_page_iterator(iter);
}

static void
test_iterate_release(log_index_snapshot_t *logindex_snapshot)
{
	BufferTag tag;
	log_index_page_iter_t iter;

	XLogRecPtr min_lsn = LSN_TEST_STEP;
	XLogRecPtr max_lsn =
		(LOG_INDEX_ITEM_SEG_LSN_NUM * 2 + LOG_INDEX_ITEM_HEAD_LSN_NUM) * LSN_TEST_STEP;

	tag.rnode.spcNode = 10;
	tag.rnode.dbNode = 11;
	tag.rnode.relNode = 12;
	tag.forkNum = MAIN_FORKNUM;
	tag.blockNum = 1;

	iter = polar_log_index_create_page_iterator(logindex_snapshot, &tag, min_lsn, max_lsn);

	Assert(iter->state == ITERATE_STATE_FINISHED);
	Assert(polar_log_index_page_iterator_end(iter) == false);
	polar_log_index_release_page_iterator(iter);
}

static void
test_iterate_only_two_lsn(log_index_snapshot_t *logindex_snapshot)
{
	BufferTag tag;
	log_index_page_iter_t iter;
	log_index_lsn_t       *lsn_info;

	tag.rnode.spcNode = 9;
	tag.rnode.dbNode = 11;
	tag.rnode.relNode = 12;
	tag.forkNum = MAIN_FORKNUM;
	tag.blockNum = 3;


	iter = polar_log_index_create_page_iterator(logindex_snapshot, &tag, test_only_two[0], test_only_two[1]);
	Assert(iter->state == ITERATE_STATE_FINISHED);
	Assert((lsn_info = polar_log_index_page_iterator_next(iter)) != NULL);
	Assert(lsn_info->lsn == test_only_two[0]);
	Assert(lsn_info->prev_lsn == InvalidXLogRecPtr);
	Assert((lsn_info = polar_log_index_page_iterator_next(iter)) != NULL);
	Assert(lsn_info->lsn == test_only_two[1]);
	Assert(lsn_info->prev_lsn == test_only_two[0]);
	polar_log_index_release_page_iterator(iter);

	iter = polar_log_index_create_page_iterator(logindex_snapshot, &tag, test_only_two[0] - 1, test_only_two[0] + 1);
	Assert(iter->state == ITERATE_STATE_FINISHED);
	Assert((lsn_info = polar_log_index_page_iterator_next(iter)) != NULL);
	Assert(lsn_info->lsn == test_only_two[0]);
	Assert(lsn_info->prev_lsn == InvalidXLogRecPtr);
	polar_log_index_release_page_iterator(iter);

	iter = polar_log_index_create_page_iterator(logindex_snapshot, &tag, test_only_two[1] - 1, test_only_two[1] + 1);
	Assert(iter->state == ITERATE_STATE_FINISHED);
	Assert((lsn_info = polar_log_index_page_iterator_next(iter)) != NULL);
	Assert(lsn_info->lsn == test_only_two[1]);
	Assert(lsn_info->prev_lsn == test_only_two[0]);
	polar_log_index_release_page_iterator(iter);
}


static void
test_iterate_lsn(log_index_snapshot_t *logindex_snapshot)
{
	test_iterate_only_two_lsn(logindex_snapshot);

#ifdef LOG_INDEX_PARALLEL_INSERT
	test_iterate_parallel_lsn(logindex_snapshot);
#endif
	test_iterate_non_exists_block_lsn(logindex_snapshot, 999);
	test_iterate_in_table0(logindex_snapshot);

	test_iterate_release(logindex_snapshot);
}

static void
test_save_memtable(log_index_snapshot_t *logindex_snapshot)
{
	sleep(1);
	Assert(LOG_INDEX_MEM_TBL_STATE(&logindex_snapshot->mem_table[0])
		   == LOG_INDEX_MEM_TBL_STATE_FLUSHED);
	Assert(LOG_INDEX_MEM_TBL_STATE(&logindex_snapshot->mem_table[1])
		   == LOG_INDEX_MEM_TBL_STATE_FLUSHED);
	MemSet(&logindex_snapshot->mem_table[0], 0, sizeof(log_mem_table_t));
	MemSet(&logindex_snapshot->mem_table[1], 0, sizeof(log_mem_table_t));
	/*
	 * Test iterate lsn from saved file
	 */
	test_iterate_in_table0(logindex_snapshot);
}

static void
test_force_flush_table(log_index_snapshot_t *logindex_snapshot)
{
	uint32 mid = LOG_INDEX_MEM_TBL_ACTIVE_ID;
	XLogRecPtr	switchpoint;
	log_mem_table_t *table = LOG_INDEX_MEM_TBL(mid);

	log_index_master_bg_write(logindex_snapshot);
	do
	{
		RequestCheckpoint(CHECKPOINT_FORCE);
		switchpoint = RequestXLogSwitch(false);
	} while(switchpoint <= table->data.max_lsn);
	log_index_force_save_table(logindex_snapshot, table);
	Assert(LOG_INDEX_MEM_TBL_STATE(table) == LOG_INDEX_MEM_TBL_STATE_FLUSHED);
}

static void
test_mini_trans_page(log_index_snapshot_t *logindex_snapshot, BufferTag *tag, XLogRecPtr lsn)
{
	uint32  h;
	XLogRecPtr  plsn;
	mini_trans_t *trans = &logindex_snapshot->mini_transaction;
	mini_trans_info_t *info = trans->info;

	Assert(polar_log_index_mini_trans_find(tag) == InvalidXLogRecPtr);
	Assert(polar_log_index_mini_trans_cond_lock(tag, LW_EXCLUSIVE, &plsn)
		   == 0);

	h = polar_log_index_mini_trans_lock(tag, LW_EXCLUSIVE, &plsn);
	Assert(h > 0);
	Assert(POLAR_BIT_IS_OCCUPIED(trans->occupied, h));
	Assert(pg_atomic_read_u32(&info[h - 1].refcount) == 1);
	Assert(plsn == lsn);
	polar_log_index_mini_trans_unlock(h);
	Assert(POLAR_BIT_IS_OCCUPIED(trans->occupied, h));
	Assert(pg_atomic_read_u32(&info[h - 1].refcount) == 0);

	Assert(polar_log_index_mini_trans_find(tag) == lsn);
	Assert(POLAR_BIT_IS_OCCUPIED(trans->occupied, h));
	Assert(pg_atomic_read_u32(&info[h - 1].refcount) == 0);

	plsn = InvalidXLogRecPtr;
	Assert(polar_log_index_mini_trans_lock(tag, LW_SHARED, &plsn) == h);
	Assert(plsn == lsn);

	Assert(POLAR_BIT_IS_OCCUPIED(trans->occupied, h));
	Assert(pg_atomic_read_u32(&info[h - 1].refcount) == 1);
	polar_log_index_mini_trans_unlock(h);
	Assert(POLAR_BIT_IS_OCCUPIED(trans->occupied, h));
	Assert(pg_atomic_read_u32(&info[h - 1].refcount) == 0);
	plsn = InvalidXLogRecPtr;

	Assert(polar_log_index_mini_trans_cond_lock(tag, LW_EXCLUSIVE, &plsn)
		   == h);
	Assert(plsn == lsn);

	Assert(POLAR_BIT_IS_OCCUPIED(trans->occupied, h));
	Assert(pg_atomic_read_u32(&info[h - 1].refcount) == 1);
	polar_log_index_mini_trans_unlock(h);
	Assert(POLAR_BIT_IS_OCCUPIED(trans->occupied, h));
	Assert(pg_atomic_read_u32(&info[h - 1].refcount) == 0);
}

static void
test_mini_trans_hash_conflict(log_index_snapshot_t *logindex_snapshot, BufferTag *tag, uint32 key, XLogRecPtr lsn)
{
	uint32 h;
	XLogRecPtr  plsn;
	mini_trans_t *trans = &logindex_snapshot->mini_transaction;
	mini_trans_info_t *info = trans->info;

	Assert(polar_log_index_mini_trans_find(tag) == InvalidXLogRecPtr);
	Assert(polar_log_index_mini_trans_cond_lock(tag, LW_EXCLUSIVE, &plsn)
		   == 0);

	h = polar_log_index_mini_trans_key_lock(tag, key, LW_EXCLUSIVE, &plsn);
	Assert(h > 0);
	Assert(POLAR_BIT_IS_OCCUPIED(trans->occupied, h));
	Assert(pg_atomic_read_u32(&info[h - 1].refcount) == 1);
	Assert(plsn == lsn);
	polar_log_index_mini_trans_unlock(h);
	Assert(POLAR_BIT_IS_OCCUPIED(trans->occupied, h));
	Assert(pg_atomic_read_u32(&info[h - 1].refcount) == 0);

	Assert(polar_log_index_mini_trans_key_find(tag, key) == lsn);
	Assert(POLAR_BIT_IS_OCCUPIED(trans->occupied, h));
	Assert(pg_atomic_read_u32(&info[h - 1].refcount) == 0);

	plsn = InvalidXLogRecPtr;
	Assert(polar_log_index_mini_trans_key_lock(tag, key, LW_SHARED, &plsn) == h);
	Assert(plsn == lsn);

	Assert(POLAR_BIT_IS_OCCUPIED(trans->occupied, h));
	Assert(pg_atomic_read_u32(&info[h - 1].refcount) == 1);
	polar_log_index_mini_trans_unlock(h);
	Assert(POLAR_BIT_IS_OCCUPIED(trans->occupied, h));
	Assert(pg_atomic_read_u32(&info[h - 1].refcount) == 0);
	plsn = InvalidXLogRecPtr;

	Assert(polar_log_index_mini_trans_cond_key_lock(tag, key, LW_EXCLUSIVE, &plsn)
		   == h);
	Assert(plsn == lsn);

	Assert(POLAR_BIT_IS_OCCUPIED(trans->occupied, h));
	Assert(pg_atomic_read_u32(&info[h - 1].refcount) == 1);
	polar_log_index_mini_trans_unlock(h);
	Assert(POLAR_BIT_IS_OCCUPIED(trans->occupied, h));
	Assert(pg_atomic_read_u32(&info[h - 1].refcount) == 0);
}

static void
test_mini_trans(log_index_snapshot_t *logindex_snapshot)
{
	BufferTag   tag;
	XLogRecPtr  lsn = 1000, plsn;
	uint32 key;
	uint32 i;

	Assert(polar_log_index_mini_trans_start(lsn) == 0);

	tag.rnode.spcNode = 10;
	tag.rnode.dbNode = 11;
	tag.rnode.relNode = 12;
	tag.forkNum = MAIN_FORKNUM;
	tag.blockNum = 1;

	test_mini_trans_page(logindex_snapshot, &tag, lsn);
	tag.blockNum = 2;
	test_mini_trans_page(logindex_snapshot, &tag, lsn);
	tag.blockNum = 3;
	test_mini_trans_page(logindex_snapshot, &tag, lsn);
	key = MINI_TRANSACTION_HASH_PAGE(&tag);
	tag.blockNum = 4;

	test_mini_trans_hash_conflict(logindex_snapshot, &tag, key, lsn);
	tag.blockNum = 5;

	test_mini_trans_hash_conflict(logindex_snapshot, &tag, key, lsn);

	/* Test hash table full */
	for (i = 6; i <= MINI_TRANSACTION_TABLE_SIZE; i++)
	{
		tag.blockNum = i;
		test_mini_trans_page(logindex_snapshot, &tag, lsn);
	}

	tag.blockNum = i;
	Assert(polar_log_index_mini_trans_key_lock(&tag, MINI_TRANSACTION_HASH_PAGE(&tag), LW_SHARED, &plsn) == 0);

	polar_log_index_mini_trans_end(lsn);

	Assert(polar_log_index_mini_trans_find(&tag) == InvalidXLogRecPtr);
	Assert(polar_log_index_mini_trans_cond_lock(&tag, LW_EXCLUSIVE, &plsn)
		   == 0);

	tag.blockNum = 2;
	Assert(polar_log_index_mini_trans_find(&tag) == InvalidXLogRecPtr);
	Assert(polar_log_index_mini_trans_cond_lock(&tag, LW_EXCLUSIVE, &plsn)
		   == 0);
}

static void
test_check_saved_prepare(log_index_snapshot_t *logindex_snapshot)
{
	BufferTag   tag;

	tag.rnode.spcNode = 12;
	tag.rnode.dbNode = 11;
	tag.rnode.relNode = 12;
	tag.forkNum = MAIN_FORKNUM;
	tag.blockNum = 1;

	polar_log_index_add_lsn(logindex_snapshot, &tag, InvalidXLogRecPtr, LSN_TEST_STEP  + test_max_lsn);

}

static void
test_check_saved(log_index_snapshot_t *logindex_snapshot)
{
	BufferTag   tag;
	log_mem_table_t *table;

	tag.rnode.spcNode = 12;
	tag.rnode.dbNode = 11;
	tag.rnode.relNode = 12;
	tag.forkNum = MAIN_FORKNUM;
	tag.blockNum = 1;

	/* Add LSN which is already exists */
	polar_log_index_add_lsn(logindex_snapshot, &tag, InvalidXLogRecPtr, LSN_TEST_STEP  + test_max_lsn);

	table = LOG_INDEX_MEM_TBL_ACTIVE();

	Assert(LOG_INDEX_MEM_TBL_STATE(table) == LOG_INDEX_MEM_TBL_STATE_FREE ||
			LOG_INDEX_MEM_TBL_IS_NEW(table));

	tag.blockNum = 2;
	polar_log_index_add_lsn(logindex_snapshot, &tag, InvalidXLogRecPtr, LSN_TEST_STEP  + test_max_lsn);

	Assert(LOG_INDEX_MEM_TBL_STATE(table) == LOG_INDEX_MEM_TBL_STATE_ACTIVE);
	Assert(LOG_INDEX_MEM_TBL_FREE_HEAD(table) > 1);

	test_max_lsn += LSN_TEST_STEP;
}

static void
test_iterate_after_truncate(log_index_snapshot_t *logindex_snapshot, XLogRecPtr start_lsn)
{
	BufferTag tag;
	log_index_page_iter_t iter;
	log_index_lsn_t *lsn_info;
	XLogRecPtr i = start_lsn;

	tag.rnode.spcNode = 10;
	tag.rnode.dbNode = 11;
	tag.rnode.relNode = 12;
	tag.forkNum = MAIN_FORKNUM;
	tag.blockNum = 100;

	iter = polar_log_index_create_page_iterator(logindex_snapshot, &tag, start_lsn, test_max_lsn);
	Assert(iter->state == ITERATE_STATE_FINISHED);

	while ((lsn_info = polar_log_index_page_iterator_next(iter)) != NULL)
	{
		Assert(lsn_info->lsn == i);
		i += LSN_TEST_STEP;
	}

	Assert(i == test_max_lsn + LSN_TEST_STEP);
	polar_log_index_release_page_iterator(iter);
}

static void
test_truncate_log(log_index_snapshot_t *logindex_snapshot)
{
	int i;
	uint64 max_seg_no = LOG_INDEX_FILE_TABLE_SEGMENT_NO(logindex_snapshot->meta.max_idx_table_id);
	char path[MAXPGPATH];

	for (i = 0; i < MAX_FULL_FILE_TEST - 1; i++)
	{
		uint64 seg_no = logindex_snapshot->meta.min_segment_info.segment_no;
		FILE *fp;

		polar_log_index_truncate(logindex_snapshot, test_full_lsn[i]);

		Assert(logindex_snapshot->meta.min_segment_info.segment_no - seg_no == 1);

		test_lsn_iterate_from_file(logindex_snapshot, test_full_lsn[i]);
		test_iterate_after_truncate(logindex_snapshot, test_full_lsn[i]);

		LOG_INDEX_FILE_TABLE_NAME(path, max_seg_no + i + 1);

		fp = fopen(path, "r");
		Assert(fp != NULL);
		fclose(fp);
	}
}

static void
test_change_lsn_prefix(log_index_snapshot_t *logindex_snapshot)
{
	log_mem_table_t *table, *next_table;
	BufferTag tag;
	XLogRecPtr start_lsn, end_lsn;
	log_index_lsn_iter_t lsn_iter;

	tag.rnode.spcNode = 100;
	tag.rnode.dbNode = 11;
	tag.rnode.relNode = 12;
	tag.forkNum = MAIN_FORKNUM;
	tag.blockNum = 100;

	table = LOG_INDEX_MEM_TBL(LOG_INDEX_MEM_TBL_ACTIVE_ID);
	start_lsn = (((test_max_lsn >> 32) + 1) << 32);

	test_insert_lsn_to_mem(logindex_snapshot, &tag, start_lsn, LOG_INDEX_MEM_TBL_HASH_PAGE(&tag));
	end_lsn = test_max_lsn;

	next_table = LOG_INDEX_MEM_TBL(LOG_INDEX_MEM_TBL_ACTIVE_ID);

	lsn_iter = polar_log_index_create_lsn_iterator(logindex_snapshot, start_lsn + LSN_TEST_STEP);

	Assert(lsn_iter != NULL);
	Assert(next_table->data.idx_table_id == table->data.idx_table_id + 1);

	test_lsn_iterator(logindex_snapshot, lsn_iter, start_lsn + LSN_TEST_STEP, end_lsn);

	polar_log_index_release_lsn_iterator(lsn_iter);
}

static void
test_bitpos()
{
	unsigned long x = 0xA0000000111112A1, y = x;
	int dst_pos[] = {1, 6, 8, 10, 13, 17, 21, 25, 29, 62, 64};
	int pos, i = 0;
	size_t array_size = sizeof(dst_pos) / sizeof(dst_pos[0]);

	for (i = 0; i < array_size; i++)
	{
		Assert(POLAR_BIT_IS_OCCUPIED(x, dst_pos[i]));
		Assert(!POLAR_BIT_IS_OCCUPIED(x, dst_pos[i] + 1));
		POLAR_BIT_RELEASE_OCCUPIED(x, dst_pos[i]);
		Assert(!POLAR_BIT_IS_OCCUPIED(x, dst_pos[i]));
	}

	Assert(x == 0);
	x = y;
	i = 0;

	while (x)
	{
		POLAR_BIT_LEAST_POS(x, pos);
		Assert(pos == dst_pos[i++]);
		x &= (x - 1);
	}

	Assert(i == array_size);

	for (i = 0; i < array_size; i++)
	{
		POLAR_BIT_OCCUPY(x, dst_pos[i]);
		Assert(POLAR_BIT_IS_OCCUPIED(x, dst_pos[i]));
	}

	Assert(x == y);
}

static void
test_load_table_cache(log_index_snapshot_t *logindex_snapshot)
{
	log_index_meta_t *meta = &logindex_snapshot->meta;
	log_idx_table_data_t *table_data = log_index_read_table(logindex_snapshot, meta->min_segment_info.min_idx_table_id);

	Assert(table_data->idx_table_id == meta->min_segment_info.min_idx_table_id);

	Assert(logindex_table_cache[logindex_snapshot->type].max_idx_table_id == meta->min_segment_info.max_idx_table_id);
	Assert(logindex_table_cache[logindex_snapshot->type].min_idx_table_id == meta->min_segment_info.min_idx_table_id);

	table_data = log_index_read_table(logindex_snapshot, meta->max_idx_table_id);
	Assert(table_data->idx_table_id == meta->max_idx_table_id);

	Assert(logindex_table_cache[logindex_snapshot->type].max_idx_table_id == meta->max_idx_table_id);

}

PG_FUNCTION_INFO_V1(test_logindex);
/*
 * SQL-callable entry point to perform all tests.
 *
 * If a 1% false positive threshold is not met, emits WARNINGs.
 *
 * See README for details of arguments.
 */
Datum
test_logindex(PG_FUNCTION_ARGS)
{
	XLogRecPtr start_lsn;
	polar_log_index_shmem_init();
	Assert(POLAR_LOGINDEX_WAL_SNAPSHOT != NULL);
	Assert(POLAR_LOGINDEX_FULLPAGE_SNAPSHOT != NULL);

	test_bitpos();

	MemSet(test_start_lsn, 0, sizeof(XLogRecPtr)*TEST_MAX_BLOCK_NUMBER);
	MemSet(test_end_lsn, 0, sizeof(XLogRecPtr)*TEST_MAX_BLOCK_NUMBER);
	polar_log_index_remove_all();
	pg_atomic_init_u32(&POLAR_LOGINDEX_WAL_SNAPSHOT->state, 0);
	pg_atomic_init_u32(&POLAR_LOGINDEX_FULLPAGE_SNAPSHOT->state, 0);

	polar_log_index_snapshot_init(LSN_TEST_STEP, false);
	test_insert_lsn(POLAR_LOGINDEX_WAL_SNAPSHOT);
	test_lsn_iterate_from_file(POLAR_LOGINDEX_WAL_SNAPSHOT, LSN_TEST_STEP * 2);
	test_lsn_iterate_from_invalid(POLAR_LOGINDEX_WAL_SNAPSHOT);
	test_lsn_iterate_from_super_max(POLAR_LOGINDEX_WAL_SNAPSHOT);
	test_lsn_iterate_from_table_interval(POLAR_LOGINDEX_WAL_SNAPSHOT);
#ifdef LOG_INDEX_PARALLEL_INSERT
	test_logindex_bgworker(POLAR_LOGINDEX_WAL_SNAPSHOT);
#endif
	test_iterate_lsn(POLAR_LOGINDEX_WAL_SNAPSHOT);
	test_mini_trans(POLAR_LOGINDEX_WAL_SNAPSHOT);
	test_save_memtable(POLAR_LOGINDEX_WAL_SNAPSHOT);
	test_lsn_iterate_from_file(POLAR_LOGINDEX_WAL_SNAPSHOT,LSN_TEST_STEP * 2);
	test_lsn_iterate_from_invalid(POLAR_LOGINDEX_WAL_SNAPSHOT);
	test_check_saved_prepare(POLAR_LOGINDEX_WAL_SNAPSHOT);
	test_force_flush_table(POLAR_LOGINDEX_WAL_SNAPSHOT);

	pg_atomic_init_u32(&POLAR_LOGINDEX_WAL_SNAPSHOT->state, 0);
	pg_atomic_init_u32(&POLAR_LOGINDEX_FULLPAGE_SNAPSHOT->state, 0);
	polar_log_index_snapshot_init(LSN_TEST_STEP, false);
	test_lsn_iterate_from_file(POLAR_LOGINDEX_WAL_SNAPSHOT, LSN_TEST_STEP * 2);

	test_check_saved(POLAR_LOGINDEX_WAL_SNAPSHOT);
	start_lsn  = test_max_lsn + LSN_TEST_STEP;
	test_insert_lsn_after_force_flush(POLAR_LOGINDEX_WAL_SNAPSHOT);

	test_insert_file_full(POLAR_LOGINDEX_WAL_SNAPSHOT);
	test_iterate_lsn(POLAR_LOGINDEX_WAL_SNAPSHOT);
	test_lsn_iterate_from_file(POLAR_LOGINDEX_WAL_SNAPSHOT, start_lsn);

	test_truncate_log(POLAR_LOGINDEX_WAL_SNAPSHOT);

	test_change_lsn_prefix(POLAR_LOGINDEX_WAL_SNAPSHOT);

	test_load_table_cache(POLAR_LOGINDEX_WAL_SNAPSHOT);

	MemoryContextResetAndDeleteChildren(POLAR_LOGINDEX_WAL_SNAPSHOT->mem_cxt);

	PG_RETURN_VOID();
}

static void
test_fullpage_segment_file(log_index_snapshot_t *logindex_snapshot)
{
	char	path[MAXPGPATH] = {0};
	uint64	min_fullpage_seg_no = 0;
	struct stat statbuf;
	int	i = 0;

	polar_fullpage_file_init(logindex_snapshot, 10000000);
	polar_fullpage_file_init(logindex_snapshot, 20000000);
	polar_fullpage_file_init(logindex_snapshot, 30000000);
	polar_fullpage_file_init(logindex_snapshot, 40000000);

	min_fullpage_seg_no = FULLPAGE_FILE_SEG_NO(10000000);
	FULLPAGE_SEG_FILE_NAME(path, min_fullpage_seg_no);
	polar_remove_old_fullpage_file(logindex_snapshot, path, min_fullpage_seg_no);
	Assert(polar_lstat(path, &statbuf) != 0);

	min_fullpage_seg_no = FULLPAGE_FILE_SEG_NO(20000000);
	FULLPAGE_SEG_FILE_NAME(path, min_fullpage_seg_no);
	polar_remove_old_fullpage_file(logindex_snapshot, path, min_fullpage_seg_no);
	Assert(polar_lstat(path, &statbuf) != 0);

	min_fullpage_seg_no = FULLPAGE_FILE_SEG_NO(30000000);
	FULLPAGE_SEG_FILE_NAME(path, min_fullpage_seg_no);
	polar_remove_old_fullpage_file(logindex_snapshot, path, min_fullpage_seg_no);
	Assert(polar_lstat(path, &statbuf) != 0);

	min_fullpage_seg_no = FULLPAGE_FILE_SEG_NO(40000000);
	FULLPAGE_SEG_FILE_NAME(path, min_fullpage_seg_no);
	polar_remove_old_fullpage_file(logindex_snapshot, path, min_fullpage_seg_no);
	Assert(polar_lstat(path, &statbuf) != 0);

	polar_update_max_fullpage_no(500000000);
	for (i = 0; i < 10000; i++)
		test_write_fullpage(logindex_snapshot);
}

static void
test_write_fullpage(log_index_snapshot_t *logindex_snapshot)
{
	Relation        rel;
	Buffer		buf;
	XLogRecPtr	start_lsn;
	XLogRecPtr	end_lsn;
	static XLogReaderState *state = NULL;
	uint64		fullpage_no;
	Page		page;
	/* Open relation and check privileges. */
	rel = relation_open(1260, AccessShareLock);

	/* pg_authid block_no=0 */
	buf = ReadBufferExtended(rel, MAIN_FORKNUM, 0, RBM_NORMAL, NULL);

	end_lsn = polar_log_fullpage_snapshot_image(buf, InvalidXLogRecPtr);

	ReleaseBuffer(buf);

	start_lsn = ProcLastRecPtr;

	/* Close relation, release lock. */
	relation_close(rel, AccessShareLock);

	XLogFlush(end_lsn);

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

	polar_log_index_read_xlog(state, start_lsn);

	Assert(state->decoded_record->xl_rmid == RM_XLOG_ID);
	Assert((state->decoded_record->xl_info & ~XLR_INFO_MASK) == XLOG_FPSI);

	page = palloc0(BLCKSZ);
	/* get fullpage_no from record */
	memcpy(&fullpage_no, XLogRecGetData(state), sizeof(uint64));
	/* read fullpage from file */
	polar_read_fullpage(page, fullpage_no);

	Assert(memcmp((char *)BufferGetPage(buf), (char *)page, BLCKSZ) == 0);
}

PG_FUNCTION_INFO_V1(test_fullpage_logindex);
/*
 * SQL-callable entry point to perform all tests.
 *
 * If a 1% false positive threshold is not met, emits WARNINGs.
 *
 * See README for details of arguments.
 */
Datum
test_fullpage_logindex(PG_FUNCTION_ARGS)
{
	Assert(POLAR_LOGINDEX_WAL_SNAPSHOT != NULL);
	Assert(POLAR_LOGINDEX_FULLPAGE_SNAPSHOT != NULL);

	/* test fullpage */
	test_write_fullpage(POLAR_LOGINDEX_FULLPAGE_SNAPSHOT);
	test_write_fullpage(POLAR_LOGINDEX_FULLPAGE_SNAPSHOT);
	test_write_fullpage(POLAR_LOGINDEX_FULLPAGE_SNAPSHOT);

	/* test fullpage segment file */
	test_fullpage_segment_file(POLAR_LOGINDEX_FULLPAGE_SNAPSHOT);

	MemoryContextResetAndDeleteChildren(POLAR_LOGINDEX_WAL_SNAPSHOT->mem_cxt);

	PG_RETURN_VOID();
}
