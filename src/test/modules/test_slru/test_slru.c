#include "postgres.h"

#include <time.h>
#include <stdlib.h>
#include <unistd.h>

#include "utils/guc.h"
#include "access/slru.h"
#include "access/multixact.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "access/commit_ts.h"
#include "commands/async.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/polar_fd.h"
#include "storage/predicate.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/polar_local_cache.h"
#include "utils/polar_successor_list.h"

static SlruCtlData test_slru_ctl;
static SlruCtlData slru_hash_index_ctl;

static LWLockMinimallyPadded *test_lock;

FILE * fp;

PG_MODULE_MAGIC;

static void
test_rethrow_error(ErrorData *data)
{
	if (data->elevel >= ERROR)
		PG_RE_THROW();
}

static void
test_successor_list(int total_items)
{
	char *buf = malloc(POLAR_SUCCESSOR_LIST_SIZE(total_items));
	polar_successor_list *list;
	int i, j, k;

	list = polar_successor_list_init((void *)buf, total_items);

	Assert(!POLAR_SUCCESSOR_LIST_EMPTY(list));

	for (i=0; i<total_items; i++)
	{
		j = polar_successor_list_pop(list);

		Assert(i==j);
	}

	Assert(polar_successor_list_pop(list) == POLAR_SUCCESSOR_LIST_NIL);
	Assert(POLAR_SUCCESSOR_LIST_EMPTY(list));

	for (k=0; k<10; k++)
	{
		for (i=0; i<total_items; i++)
		{
			polar_successor_list_push(list, i);
		}
		Assert(!POLAR_SUCCESSOR_LIST_EMPTY(list));

		for (i=total_items; i>0; i--)
		{
			j = polar_successor_list_pop(list);

			Assert(j == i - 1);
		}

		Assert(polar_successor_list_pop(list) == POLAR_SUCCESSOR_LIST_NIL);
		Assert(POLAR_SUCCESSOR_LIST_EMPTY(list));
	}

	free(buf);	
}

static bool
test_slru_page_precedes(int page1, int page2)
{
	return page1 < page2;
}

static void
test_slru_page_physical_exists()
{
	int i;
	int slotno;

	for (i=0; i<64; i++)
	{
		LWLockAcquire(&test_lock->lock, LW_EXCLUSIVE);
		Assert(!polar_slru_page_physical_exists(&test_slru_ctl, i));
		slotno = SimpleLruZeroPage(&test_slru_ctl, i);
		SimpleLruWritePage(&test_slru_ctl, slotno);
		Assert(!test_slru_ctl.shared->page_dirty[slotno]);
		slotno = SimpleLruReadPage(&test_slru_ctl, i, false, InvalidTransactionId);
		
		if (i > 0)
			Assert(polar_slru_page_physical_exists(&test_slru_ctl, i-1));
		LWLockRelease(&test_lock->lock);
	}
}

static void
slru_hash_index_precheck(int slot_num)
{
	SlruShared shared = slru_hash_index_ctl.shared;

	Assert(!POLAR_SUCCESSOR_LIST_EMPTY(shared->polar_free_list));
	Assert(shared->victim_pivot == 0);
}

static void
slru_hash_index_postcheck(int page_num)
{
	int i = 0;
	SlruShared shared = slru_hash_index_ctl.shared;

	/* check from mapping slot_no -> page_no */
	for (i = 0; i < page_num; i++)
	{
		polar_slru_hash_entry *entry;
		int slotno;
	   
		LWLockAcquire(&test_lock->lock, LW_SHARED);	
		slotno	= SimpleLruReadPage(&slru_hash_index_ctl,
					i, true, InvalidTransactionId); 
		entry = hash_search(shared->polar_hash_index, (void *)&i, HASH_FIND, NULL);
		Assert(entry != NULL);
		Assert(entry->pageno == i);
		Assert(entry->slotno == slotno);
		LWLockRelease(&test_lock->lock);
	}
}

static void
test_slru_hash_index_internal(int slot_num, int page_num, int test_num)
{
	int i = 0;
	int slotno = 0;
	SlruShared shared = slru_hash_index_ctl.shared;

	slru_hash_index_precheck(slot_num);
	/* prepare data */
	for (i = 0; i < page_num; i++)
	{
		polar_slru_hash_entry *entry;

		LWLockAcquire(&test_lock->lock, LW_EXCLUSIVE);	
		if (i < slot_num)
			Assert(!POLAR_SUCCESSOR_LIST_EMPTY(shared->polar_free_list));
		else
			Assert(POLAR_SUCCESSOR_LIST_EMPTY(shared->polar_free_list));

		slotno = SimpleLruZeroPage(&slru_hash_index_ctl, i);
		SimpleLruWritePage(&slru_hash_index_ctl, slotno);
		entry = hash_search(shared->polar_hash_index, (void *)&i, HASH_FIND, NULL);
		Assert(entry != NULL);
		Assert(entry->pageno == i);
		Assert(entry->slotno == slotno);
		LWLockRelease(&test_lock->lock);
	}

	slru_hash_index_postcheck(page_num);

	/* run chaos test */
	for (i = 0; i < test_num; i++)
	{
		int r = rand();
		int page = rand() % page_num;
		int invalid_page = 0;
		switch(r % 10)
		{
			case 0:
				/* Zero Page */
				LWLockAcquire(&test_lock->lock, LW_EXCLUSIVE);	
				slotno = SimpleLruZeroPage(&slru_hash_index_ctl, page);
				SimpleLruWritePage(&slru_hash_index_ctl, slotno);
				LWLockRelease(&test_lock->lock);
				break;
			case 1:
			case 2:
				/* ReadPage */
				LWLockAcquire(&test_lock->lock, LW_SHARED);	
				SimpleLruReadPage(&slru_hash_index_ctl, page, true, InvalidTransactionId);
				LWLockRelease(&test_lock->lock);
				break;
			case 3:
				LWLockAcquire(&test_lock->lock, LW_EXCLUSIVE);
				SimpleLruWritePage(&slru_hash_index_ctl, rand() % slot_num);
				LWLockRelease(&test_lock->lock);
				break;
			case 4:
			case 5:
				/* ReadOnly */
				SimpleLruReadPage_ReadOnly(&slru_hash_index_ctl, page, InvalidTransactionId);
				LWLockRelease(&test_lock->lock);
				break;
			case 6:
			case 7:
				/* invalid page */
				invalid_page = shared->page_number[rand() % slot_num];
				polar_slru_invalid_page(&slru_hash_index_ctl, invalid_page);
				break;
			case 8:
				// SimpleLruTruncate(&slru_hash_index_ctl, page);
				// if (page > truncate_page)
				// 	truncate_page = page;
				break;
			case 9:
				SimpleLruFlush(&slru_hash_index_ctl, false);
				break;
		}		
		slru_hash_index_postcheck(page_num);
	}

	slru_hash_index_postcheck(page_num);
}

static void
test_promote_slru_with_cache_err(void)
{
	int i = 0;
	MemoryContext   ccxt = CurrentMemoryContext;

	emit_log_hook = test_rethrow_error;

	PG_TRY();
	{
		polar_slru_promote(&test_slru_ctl);
	}
	PG_CATCH();
	{
		ErrorData *err;
		MemoryContextSwitchTo(ccxt);
		err = CopyErrorData();
		Assert(err->elevel == FATAL);
		pfree(err);
		MemoryContextReset(ccxt);
		i++;
	}
	PG_END_TRY();

	emit_log_hook = NULL;

	Assert(i == 1);
}

static void
test_promote_slru_with_cache_dir_err(void)
{
	polar_local_cache cache;
	uint32 io_permission = POLAR_CACHE_LOCAL_FILE_READ | POLAR_CACHE_LOCAL_FILE_WRITE;
	FILE *fp;
	uint64 seg = 100;

	IsUnderPostmaster = false;
	cache = polar_create_local_cache("test_slru", "pg_test_slru",
			   8, (SLRU_PAGES_PER_SEGMENT * BLCKSZ), LWTRANCHE_POLAR_CSNLOG_LOCAL_CACHE,
			   io_permission, NULL);
	if (cache)
		polar_local_cache_move_trash(cache->dir_name);
	IsUnderPostmaster = true;

	polar_slru_reg_local_cache(&test_slru_ctl, cache);
	fp = fopen("pg_test_slru/readonly_flushed_seg_0001", "w");
	fwrite(&seg, sizeof(uint64), 1, fp);
	fclose(fp);
	/* POLAR: filename '0064' corresponds to segno which is 100. */
	fp = fopen("pg_test_slru/0064", "w");
	fwrite(&seg, sizeof(uint64), 1, fp);
	fclose(fp);
	/* POLAR: remove write io permission of file. */
	chmod("pg_test_slru/0064", S_IRUSR|S_IRGRP|S_IROTH);

	test_promote_slru_with_cache_err();
}

PG_FUNCTION_INFO_V1(test_slru);
/*
 * SQL-callable entry point to perform all tests.
 *
 * If a 1% false positive threshold is not met, emits WARNINGs.
 *
 * See README for details of arguments.
 */
Datum
test_slru(PG_FUNCTION_ARGS)
{
	bool found;

	polar_make_pg_directory("test_slru");
	test_lock = (LWLockMinimallyPadded *)
		ShmemInitStruct("test lock", sizeof(LWLockMinimallyPadded), &found);
	LWLockInitialize(&test_lock->lock, LWTRANCHE_FIRST_USER_DEFINED);

	test_slru_ctl.PagePrecedes = test_slru_page_precedes;
	IsUnderPostmaster = false;
	SimpleLruInit(&test_slru_ctl, "test_slru",
			10, 0, &test_lock->lock, "test_slru", LWTRANCHE_FIRST_USER_DEFINED, false);
	IsUnderPostmaster = true;
	test_slru_page_physical_exists();

	//Test error when promote slru without cache
	test_promote_slru_with_cache_err();
	//Test error when cache write error
	test_promote_slru_with_cache_dir_err();

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(test_slru_slot_size_config);
/*
 * SQL-callable entry point to perform all tests.
 *
 * If a 1% false positive threshold is not met, emits WARNINGs.
 *
 * See README for details of arguments.
 */
Datum
test_slru_slot_size_config(PG_FUNCTION_ARGS)
{
	polar_enable_shared_storage_mode = true;
	// CLOGShmemBuffers()
	polar_clog_slot_size = Max(4, NBuffers / 512 * 8) - 1;
	Assert(CLOGShmemBuffers() == polar_clog_slot_size);
	
	polar_clog_slot_size = Max(4, NBuffers / 512 * 8) + 1;
	Assert(CLOGShmemBuffers() == Max(4, NBuffers / 512 * 8));

	// CommitTsShmemBuffers()
	polar_committs_buffer_slot_size = Max(4, NBuffers / 1024 * 8) - 1;
	Assert(CommitTsShmemBuffers() == polar_committs_buffer_slot_size);

	polar_committs_buffer_slot_size = Max(4, NBuffers / 1024 * 8) + 1;
	Assert(CommitTsShmemBuffers() == Max(4, NBuffers / 1024 * 8));

	// just check default value
	Assert(polar_mxact_offset_buffer_slot_size == NUM_MXACTOFFSET_BUFFERS);
	Assert(polar_mxact_member_buffer_slot_size == NUM_MXACTMEMBER_BUFFERS);
	Assert(polar_subtrans_buffer_slot_size == NUM_SUBTRANS_BUFFERS);
	Assert(polar_async_buffer_slot_size == NUM_ASYNC_BUFFERS);
	Assert(polar_oldserxid_buffer_slot_size == NUM_OLDSERXID_BUFFERS);
	Assert(polar_async_buffer_slot_size == NUM_MXACTOFFSET_BUFFERS);

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(test_slru_hash_index);
Datum
test_slru_hash_index(PG_FUNCTION_ARGS)
{
	bool found;
	int slot_num = PG_GETARG_INT32(0);
	int page_num = PG_GETARG_INT32(1);
	int test_num = PG_GETARG_INT32(2);

	test_successor_list(slot_num);

	polar_make_pg_directory("test_slru_hash_index");
	test_lock = (LWLockMinimallyPadded *)
		ShmemInitStruct("test lock hash index", sizeof(LWLockMinimallyPadded), &found);
	LWLockInitialize(&test_lock->lock, LWTRANCHE_FIRST_USER_DEFINED);

	slru_hash_index_ctl.PagePrecedes = test_slru_page_precedes;
	IsUnderPostmaster = false;
	SimpleLruInit(&slru_hash_index_ctl, "test_slru_hash_index",
			slot_num, 0, &test_lock->lock, "test_slru_hash_index", LWTRANCHE_FIRST_USER_DEFINED, false);
	IsUnderPostmaster = true;
	test_slru_hash_index_internal(slot_num, page_num, test_num);

	PG_RETURN_VOID();
}
