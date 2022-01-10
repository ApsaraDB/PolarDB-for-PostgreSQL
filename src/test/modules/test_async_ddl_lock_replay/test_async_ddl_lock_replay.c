#include "postgres.h"

#include "access/xlog.h"
#include "storage/proc.h"
#include "storage/relfilenode.h"
#include "miscadmin.h"
#include "utils/elog.h"
#include "utils/guc.h"

#include "access/polar_async_ddl_lock_replay.h"

PG_MODULE_MAGIC;

#define POLAR_TEST_GET_PENDING_LOCK(lock) ((xl_standby_lock *)&((lock).xid))
#define POLAR_TEST_INIT_PENDING_TBL(lk_tbl) \
		test_add_lock_to_pending_tbl(lk_tbl, sizeof(lk_tbl) / sizeof(polar_pending_lock));
#define POLAR_TEST_GET_LOCK_AND_ASSERT(tx, tx_oldest, oldest) \
	polar_get_lock_by_tx(tx, false); \
	Assert(tx->last_ptr == tx_oldest); \
	Assert(polar_get_async_ddl_lock_replay_oldest_ptr() == oldest);

/* one tx */
static struct polar_pending_lock lock_tbl1[] =
{
	/* ptr, rtime, xid, dbOid, relOid */
	{ 111, 111, 100001, 100002, 100003 },
	{ 112, 112, 100001, 100002, 100004 },
	{ 113, 113, 100001, 100002, 100003 },
	{ 114, 114, 100001, 100002, 100005 },
};

/* two entries */
static struct polar_pending_lock lock_tbl2[] =
{
	{ 111, 111, 100001, 100002, 100003 },
	{ 112, 112, 100002, 100004, 100003 },
	{ 113, 113, 100002, 100004, 100005 },
	{ 114, 114, 100001, 100002, 100005 },
	{ 115, 115, 100001, 100002, 100004 },
	{ 116, 116, 100002, 100004, 100007 },
};

/* five entries */
static struct polar_pending_lock lock_tbl3[] =
{
	{ 111, 111, 100001, 100002, 100003 },
	{ 112, 112, 100002, 100004, 100003 },
	{ 113, 113, 100003, 100004, 100005 },
	{ 114, 114, 100002, 100002, 100005 },
	{ 115, 115, 100001, 100002, 100004 },
	{ 116, 116, 100004, 100004, 100007 },
	{ 117, 117, 100003, 100004, 100007 },
	{ 118, 118, 100005, 100004, 100007 },
	{ 119, 119, 100004, 100004, 100007 },
};

static void
polar_init_async_ddl_lock_replay_myworker()
{
	polar_async_ddl_lock_replay_worker_t **myWorker = polar_async_ddl_lock_replay_get_myworker();
	polar_async_ddl_lock_replay_worker_t *MyWorker;

	MyWorker = palloc0(sizeof(polar_async_ddl_lock_replay_worker_t));
	MyWorker->id = 0;
	MyWorker->pid = MyProcPid;
	MyWorker->cur_tx = NULL;
	MyWorker->head = NULL;
	MyWorker->working = true;
	LWLockRegisterTranche(LWTRANCHE_POLAR_ASYNC_LOCK_WORKER, "async ddl lock replay async worker");
	LWLockInitialize(&MyWorker->lock, LWTRANCHE_POLAR_ASYNC_LOCK_WORKER);
	*myWorker = MyWorker;
}

static void
test_allow_async_ddl_lock_replay()
{
	polar_enable_async_ddl_lock_replay = false;
	polar_async_ddl_lock_replay_worker_num = 3;
	Assert(!polar_allow_async_ddl_lock_replay());

	polar_enable_async_ddl_lock_replay = true;
	polar_async_ddl_lock_replay_worker_num = 0;
	Assert(!polar_allow_async_ddl_lock_replay());

	polar_enable_async_ddl_lock_replay = true;
	polar_async_ddl_lock_replay_worker_num = 3;
	Assert(polar_allow_async_ddl_lock_replay());
}

static void
test_add_lock_to_pending_tbl(polar_pending_lock *locks, size_t size)
{
	int i;
	for (i = 0; i < size; ++i)
		polar_add_lock_to_pending_tbl(POLAR_TEST_GET_PENDING_LOCK(locks[i]), locks[i].last_ptr, locks[i].rtime);
}

static void
test_async_ddl_lock_case1()
{
	polar_pending_tx *tx= NULL;

	Assert(polar_get_async_ddl_lock_replay_oldest_ptr() == InvalidXLogRecPtr);

	POLAR_TEST_INIT_PENDING_TBL(lock_tbl1);

	tx = polar_own_pending_tx();

	Assert(tx->last_ptr == 111);
	Assert(tx->head->last_ptr == 111);
	Assert(tx->cur_lock->last_ptr == 111);
	Assert(tx->cur_lock->next->last_ptr == 112);
	Assert(tx->cur_lock->next->next->last_ptr == 113);
	Assert(tx->cur_lock->next->next->next->last_ptr == 114);
	Assert(tx->cur_lock->next->next->next->next == NULL);
	Assert(polar_get_async_ddl_lock_replay_oldest_ptr() == 111);

	POLAR_TEST_GET_LOCK_AND_ASSERT(tx, 112, 112);
	POLAR_TEST_GET_LOCK_AND_ASSERT(tx, 113, 113);
	POLAR_TEST_GET_LOCK_AND_ASSERT(tx, 114, 114);
	POLAR_TEST_GET_LOCK_AND_ASSERT(tx, InvalidXLogRecPtr, InvalidXLogRecPtr);

	polar_release_all_pending_tx();
	Assert(polar_get_async_ddl_lock_replay_oldest_ptr() == InvalidXLogRecPtr);
}

static void
test_async_ddl_lock_case2()
{
	polar_pending_tx *tx2= NULL;
	polar_pending_tx *tx1= NULL;

	POLAR_TEST_INIT_PENDING_TBL(lock_tbl2);

	tx1 = polar_own_pending_tx();
	tx2 = polar_own_pending_tx();

	Assert(tx1->last_ptr == 111);
	Assert(tx1->head->last_ptr == 111);
	Assert(tx1->cur_lock->last_ptr == 111);
	Assert(tx1->head->next->last_ptr == 114);
	Assert(tx1->head->next->next->last_ptr == 115);
	Assert(tx1->head->next->next->next == NULL);

	Assert(tx2->last_ptr == 112);
	Assert(tx2->head->last_ptr == 112);
	Assert(tx2->cur_lock->last_ptr == 112);
	Assert(tx2->head->next->last_ptr == 113);
	Assert(tx2->head->next->next->last_ptr == 116);
	Assert(tx2->head->next->next->next == NULL);

	POLAR_TEST_GET_LOCK_AND_ASSERT(tx1, 114, 112);
	POLAR_TEST_GET_LOCK_AND_ASSERT(tx2, 113, 113);
	POLAR_TEST_GET_LOCK_AND_ASSERT(tx2, 116, 114);
	POLAR_TEST_GET_LOCK_AND_ASSERT(tx1, 115, 115);
	POLAR_TEST_GET_LOCK_AND_ASSERT(tx1, InvalidXLogRecPtr, 116);
	POLAR_TEST_GET_LOCK_AND_ASSERT(tx2, InvalidXLogRecPtr, InvalidXLogRecPtr);

	polar_release_all_pending_tx();
}

static void
test_async_ddl_lock_case3()
{
	polar_pending_tx *tx1= NULL;
	polar_pending_tx *tx2= NULL;
	polar_pending_tx *tx3= NULL;
	polar_pending_tx *tx4= NULL;
	polar_pending_tx *tx5= NULL;

	POLAR_TEST_INIT_PENDING_TBL(lock_tbl3);

	tx1 = polar_own_pending_tx();
	tx2 = polar_own_pending_tx();
	tx3 = polar_own_pending_tx();
	tx4 = polar_own_pending_tx();
	tx5 = polar_own_pending_tx();

	Assert(tx1->last_ptr == 111);
	Assert(tx2->last_ptr == 112);
	Assert(tx3->last_ptr == 113);
	Assert(tx4->last_ptr == 116);
	Assert(tx5->last_ptr == 118);

	POLAR_TEST_GET_LOCK_AND_ASSERT(tx2, 114, 111);
	POLAR_TEST_GET_LOCK_AND_ASSERT(tx4, 119, 111);
	POLAR_TEST_GET_LOCK_AND_ASSERT(tx3, 117, 111);
	POLAR_TEST_GET_LOCK_AND_ASSERT(tx3, InvalidXLogRecPtr, 111);
	POLAR_TEST_GET_LOCK_AND_ASSERT(tx1, 115, 114);
	POLAR_TEST_GET_LOCK_AND_ASSERT(tx5, InvalidXLogRecPtr, 114);
	POLAR_TEST_GET_LOCK_AND_ASSERT(tx1, InvalidXLogRecPtr, 114);
	POLAR_TEST_GET_LOCK_AND_ASSERT(tx2, InvalidXLogRecPtr, 119);
	POLAR_TEST_GET_LOCK_AND_ASSERT(tx4, InvalidXLogRecPtr, InvalidXLogRecPtr);

	polar_release_all_pending_tx();
	Assert(polar_get_async_ddl_lock_replay_oldest_ptr() == InvalidXLogRecPtr);
}

static void
test_add_many_locks_to_pending_tbl()
{
	int i;
	polar_pending_lock lock;
	polar_pending_tx *tx= NULL;

	for (i = 0; i < 100; ++i)
	{
		lock.last_ptr = i + 100;
		lock.rtime = i + 500;
		lock.xid = i + 1000;
		lock.dbOid = i + 2000;
		lock.relOid = i + 5000;
		polar_add_lock_to_pending_tbl(POLAR_TEST_GET_PENDING_LOCK(lock), lock.last_ptr, lock.rtime);
	}

	while ((tx = polar_own_pending_tx()))
	{
		polar_release_all_pending_tx();
	}

	Assert(polar_get_async_ddl_lock_replay_oldest_ptr() == InvalidXLogRecPtr);
}



PG_FUNCTION_INFO_V1(test_async_ddl_lock_replay);
/*
 * SQL-callable entry point to perform all tests.
 *
 * If a 1% false positive threshold is not met, emits WARNINGs.
 *
 * See README for details of arguments.
 */
Datum
test_async_ddl_lock_replay(PG_FUNCTION_ARGS)
{
	elog(LOG, "start of test case.");

	while (RecoveryInProgress()) 
		pg_usleep(1 * 1000 * 1000);

	polar_enable_async_ddl_lock_replay_unit_test = true;
	IsUnderPostmaster = false;

	test_allow_async_ddl_lock_replay();
	polar_init_async_ddl_lock_replay_myworker();
	polar_init_async_ddl_lock_replay();

	test_async_ddl_lock_case1();
	test_async_ddl_lock_case2();
	test_async_ddl_lock_case3();

	test_add_many_locks_to_pending_tbl();

	PG_RETURN_VOID();
}
