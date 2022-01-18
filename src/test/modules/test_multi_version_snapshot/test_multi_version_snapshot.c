#include "postgres.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/snapmgr.h"
#include "utils/snapshot.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "storage/procarray.h"
#include "pgstat.h"

#define ELOG(err_mode, fmt, ...) \
	elog(err_mode, "Test %s in %d failed, "fmt, __FUNCTION__, __LINE__, ##__VA_ARGS__)

PG_MODULE_MAGIC;

static void
test_snapshot_access_with_set_retry()
{
	TransactionId xip[10];
	int count; 
	bool overflowed;
	TransactionId xmin;
	TransactionId xmax;
	TransactionId replication_slot_xmin;
	TransactionId replication_slot_catalog_xmin;
	int i;

	/*
	 * When all snapshot slots share lock hold, set snapshot should do nothing
	 */
	for (i=0; i< polar_replica_multi_version_snapshot_get_slot_num(); i++)
	{
		polar_test_acquire_slot_lock(i, LW_SHARED);
	}
	//no available slot, should do nothing
	polar_test_KnownAssignedXidsAdd(3, 5);
	polar_test_replica_multi_version_snapshot_set_snapshot();
	for (i=0; i< polar_replica_multi_version_snapshot_get_slot_num(); i++)
	{
		polar_test_release_slot_lock(i);
	}
	if (polar_replica_multi_version_snapshot_get_curr_slot_no() != -1)
		ELOG(ERROR, "snapshot set should do nothing");
	if (polar_replica_multi_version_snapshot_get_next_slot_no() != 3)
		ELOG(ERROR, "snapshot next slot num wrong: wrong value is %d; correct value is %d", 
			 polar_replica_multi_version_snapshot_get_next_slot_no(), 3);
	if (polar_replica_multi_version_snapshot_get_write_retried_times() != polar_replica_multi_version_snapshot_get_retry_times())
		ELOG(ERROR, "snapshot store write retried times wrong: wrong value is"UINT64_FORMAT"; correct value is %d",
			 polar_replica_multi_version_snapshot_get_read_retried_times(), polar_replica_multi_version_snapshot_get_retry_times());
	if (polar_replica_multi_version_snapshot_get_write_switched_times() != 1)
		ELOG(ERROR, "snapshot store write switched times wrong: wrong value is "UINT64_FORMAT"; correct value is %d", 
			 polar_replica_multi_version_snapshot_get_write_switched_times(), 1);

	/*
	 * When all slot share lock released, set snapshot should success
	 */
	polar_test_replica_multi_version_snapshot_set_snapshot();
	if (polar_replica_multi_version_snapshot_get_curr_slot_no() != 3)
		ELOG(ERROR, "snapshot current slot num wrong: wrong value is %d; correct value is %d", 
			 polar_replica_multi_version_snapshot_get_curr_slot_no(), 3);
	if (polar_replica_multi_version_snapshot_get_next_slot_no() != 4)
		ELOG(ERROR, "snapshot next slot num wrong: wrong value is %d; correct value is %d", 
			 polar_replica_multi_version_snapshot_get_next_slot_no(), 4);
	polar_test_replica_multi_version_snapshot_get_snapshot(xip, &count, &overflowed, &xmin, &xmax,
													&replication_slot_xmin, &replication_slot_catalog_xmin);
	if (count != 3)
		ELOG(ERROR, "snapshot transaction count wrong: wrong value is %d; correct value is %d", 
			 count, 3);
	if (xmin != 3)
		ELOG(ERROR, "snapshot transaction xmin wrong: wrong value is %d; correct value is %d",
			 xmin, 3);
	if (xmax != GetLatestSnapshot()->xmax)
		ELOG(ERROR, "snapshot transaction xmax wrong: wrong value is %d; correct value is %d",
			 xmax, GetLatestSnapshot()->xmax);
	if (xip[0] != 3 || xip[1] != 4 || xip[2] != 5)
		ELOG(ERROR, "snapshot transaction array wrong: wrong value is %d-%d-%d; correct value is %d-%d-%d",
			 xip[0], xip[1], xip[2], 3, 4, 5);
	if (polar_replica_multi_version_snapshot_get_write_retried_times() != polar_replica_multi_version_snapshot_get_retry_times())
		ELOG(ERROR, "snapshot store write retried times wrong: wrong value is"UINT64_FORMAT"; correct value is %d",
			 polar_replica_multi_version_snapshot_get_read_retried_times(), polar_replica_multi_version_snapshot_get_retry_times());
	if (polar_replica_multi_version_snapshot_get_write_switched_times() != 1)
		ELOG(ERROR, "snapshot store write switched times wrong: wrong value is "UINT64_FORMAT"; correct value is %d", 
			 polar_replica_multi_version_snapshot_get_write_switched_times(), 1);

	/* 
	 * When next slot share locked, set snapshot should success on next slot with retry 
	 */
	polar_test_acquire_slot_lock(polar_replica_multi_version_snapshot_get_next_slot_no(), LW_SHARED);
	polar_test_replica_multi_version_snapshot_set_snapshot();
	polar_test_release_slot_lock(polar_replica_multi_version_snapshot_get_next_slot_no()-2);
	if (polar_replica_multi_version_snapshot_get_curr_slot_no() != 5)
		ELOG(ERROR, "snapshot current slot num wrong: wrong value is %d; correct value is %d", 
			 polar_replica_multi_version_snapshot_get_curr_slot_no(), 5);
	if (polar_replica_multi_version_snapshot_get_next_slot_no() != 6)
		ELOG(ERROR, "snapshot next slot num wrong: wrong value is %d; correct value is %d", 
			 polar_replica_multi_version_snapshot_get_next_slot_no(), 6);
	polar_test_replica_multi_version_snapshot_get_snapshot(xip, &count, &overflowed, &xmin, &xmax,
													&replication_slot_xmin, &replication_slot_catalog_xmin);
	if (count != 3)
		ELOG(ERROR, "snapshot transaction count wrong: wrong value is %d; correct value is %d", 
			 count, 3);
	if (xmin != 3)
		ELOG(ERROR, "snapshot transaction xmin wrong: wrong value is %d; correct value is %d",
			 xmin, 3);
	if (xmax != GetLatestSnapshot()->xmax)
		ELOG(ERROR, "snapshot transaction xmax wrong: wrong value is %d; correct value is %d",
			 xmax, GetLatestSnapshot()->xmax);
	if (xip[0] != 3 || xip[1] != 4 || xip[2] != 5)
		ELOG(ERROR, "snapshot transaction array wrong: wrong value is %d-%d-%d; correct value is %d-%d-%d",
			 xip[0], xip[1], xip[2], 3, 4, 5);
	if (polar_replica_multi_version_snapshot_get_write_retried_times() != polar_replica_multi_version_snapshot_get_retry_times()+1)
		ELOG(ERROR, "snapshot store write retried times wrong: wrong value is"UINT64_FORMAT"; correct value is %d",
			 polar_replica_multi_version_snapshot_get_read_retried_times(), polar_replica_multi_version_snapshot_get_retry_times()+1);
	if (polar_replica_multi_version_snapshot_get_write_switched_times() != 1)
		ELOG(ERROR, "snapshot store write switched times wrong: wrong value is "UINT64_FORMAT"; correct value is %d", 
			 polar_replica_multi_version_snapshot_get_write_switched_times(), 1);

	/* clear test env */
	polar_test_set_curr_slot_num(-1);
	polar_test_set_next_slot_num(0);
	polar_test_KnownAssignedXidsReset();
}

static void
test_snapshot_access_with_get_retry()
{
	TransactionId xip[10];
	int count; 
	bool overflowed;
	TransactionId xmin;
	TransactionId xmax;
	TransactionId replication_slot_xmin;
	TransactionId replication_slot_catalog_xmin;
	bool get_succ = false;

	/*
	 * When no snapshot set, get snapshot should failed
	 */
	get_succ = polar_test_replica_multi_version_snapshot_get_snapshot(xip, &count, &overflowed, &xmin, &xmax,
														&replication_slot_xmin, &replication_slot_catalog_xmin);
	if (get_succ)
		ELOG(ERROR, "get snapshot should failed");
	if (polar_replica_multi_version_snapshot_get_read_retried_times() != polar_replica_multi_version_snapshot_get_retry_times())
		ELOG(ERROR, "snapshot store read retried times wrong: wrong value is"UINT64_FORMAT"; correct value is %d",
			 polar_replica_multi_version_snapshot_get_read_retried_times(), polar_replica_multi_version_snapshot_get_retry_times());
	if (polar_replica_multi_version_snapshot_get_read_switched_times() != 1)
		ELOG(ERROR, "snapshot store read switched times wrong: wrong value is "UINT64_FORMAT"; correct value is %d", 
			 polar_replica_multi_version_snapshot_get_read_switched_times(), 1);

	/*
	 * When snapshot slot exclusive lock hold, get snapshot should retry or failed
	 */
	polar_test_KnownAssignedXidsAdd(3, 5);
	polar_test_replica_multi_version_snapshot_set_snapshot();
	polar_test_acquire_slot_lock(polar_replica_multi_version_snapshot_get_curr_slot_no(), LW_EXCLUSIVE);
	get_succ = polar_test_replica_multi_version_snapshot_get_snapshot(xip, &count, &overflowed, &xmin, &xmax,
													&replication_slot_xmin, &replication_slot_catalog_xmin);
	polar_test_release_slot_lock(polar_replica_multi_version_snapshot_get_curr_slot_no());
	if (get_succ)
		ELOG(ERROR, "get snapshot should failed");
	if (polar_replica_multi_version_snapshot_get_read_retried_times() != 2*polar_replica_multi_version_snapshot_get_retry_times())
		ELOG(ERROR, "snapshot store read retried times wrong: wrong value is "UINT64_FORMAT"; correct value is %d",
			 polar_replica_multi_version_snapshot_get_read_retried_times(), 2*polar_replica_multi_version_snapshot_get_retry_times());
	if (polar_replica_multi_version_snapshot_get_read_switched_times() != 2)
		ELOG(ERROR, "snapshot store read switched times wrong: wrong value is "UINT64_FORMAT"; correct value is %d", 
			 polar_replica_multi_version_snapshot_get_read_switched_times(), 2);

	/* 
	 * When snapshot slot exclusive lock released, get snapshot should success
	 */
	polar_test_replica_multi_version_snapshot_get_snapshot(xip, &count, &overflowed, &xmin, &xmax,
													&replication_slot_xmin, &replication_slot_catalog_xmin);
	if (count != 3)
		ELOG(ERROR, "snapshot transaction count wrong: wrong value is %d; correct value is %d", 
			 count, 3);
	if (xmin != 3)
		ELOG(ERROR, "snapshot transaction xmin wrong: wrong value is %d; correct value is %d",
			 xmin, 3);
	if (xmax != GetLatestSnapshot()->xmax)
		ELOG(ERROR, "snapshot transaction xmax wrong: wrong value is %d; correct value is %d",
			 xmax, GetLatestSnapshot()->xmax);
	if (xip[0] != 3 || xip[1] != 4 || xip[2] != 5)
		ELOG(ERROR, "snapshot transaction array wrong: wrong value is %d-%d-%d; correct value is %d-%d-%d",
			 xip[0], xip[1], xip[2], 3, 4, 5);
	if (polar_replica_multi_version_snapshot_get_read_retried_times() != 2*polar_replica_multi_version_snapshot_get_retry_times())
		ELOG(ERROR, "snapshot store read retried times wrong: wrong value is "UINT64_FORMAT"; correct value is %d",
			 polar_replica_multi_version_snapshot_get_read_retried_times(), 2*polar_replica_multi_version_snapshot_get_retry_times());
	if (polar_replica_multi_version_snapshot_get_read_switched_times() != 2)
		ELOG(ERROR, "snapshot store read switched times wrong: wrong value is "UINT64_FORMAT"; correct value is %d", 
			 polar_replica_multi_version_snapshot_get_read_switched_times(), 2);

	/* 
	 * When new snapshot set, should get new snapshot
	 */
	polar_test_KnownAssignedXidsAdd(6, 6);
	polar_test_replica_multi_version_snapshot_set_snapshot();
	polar_test_replica_multi_version_snapshot_get_snapshot(xip, &count, &overflowed, &xmin, &xmax,
													&replication_slot_xmin, &replication_slot_catalog_xmin);
	if (count != 4)
		ELOG(ERROR, "snapshot transaction count wrong: wrong value is %d; correct value is %d", 
			 count, 4);
	if (xmax != GetLatestSnapshot()->xmax)
		ELOG(ERROR, "snapshot transaction xmax wrong: wrong value is %d; correct value is %d",
			 xmax, GetLatestSnapshot()->xmax);
	if (xip[0] != 3 || xip[1] != 4 || xip[2] != 5 || xip[3] != 6)
		ELOG(ERROR, "snapshot transaction array wrong: wrong value is %d-%d-%d-%d; correct value is %d-%d-%d-%d",
			 xip[0], xip[1], xip[2], xip[3], 3, 4, 5, 6);
	if (polar_replica_multi_version_snapshot_get_curr_slot_no() != 1)
		ELOG(ERROR, "snapshot current slot num wrong: wrong value is %d; correct value is %d", 
			 polar_replica_multi_version_snapshot_get_curr_slot_no(), 1);
	if (polar_replica_multi_version_snapshot_get_next_slot_no() != 2)
		ELOG(ERROR, "snapshot next slot num wrong: wrong value is %d; correct value is %d", 
			 polar_replica_multi_version_snapshot_get_next_slot_no(), 2);

	/* clear test env */
	polar_test_set_curr_slot_num(-1);
	polar_test_set_next_slot_num(0);
	polar_test_KnownAssignedXidsReset();
}

static void
test_snapshot_access_with_roundwrap()
{
	int i;

	polar_test_KnownAssignedXidsAdd(3, 5);

	for (i=0; i < polar_replica_multi_version_snapshot_get_slot_num(); i++)
	{
		polar_test_replica_multi_version_snapshot_set_snapshot();
	}
	if (polar_replica_multi_version_snapshot_get_curr_slot_no() != polar_replica_multi_version_snapshot_get_slot_num()-1)
		ELOG(ERROR, "snapshot current slot num wrong: wrong value is %d; correct value is %d", 
			 polar_replica_multi_version_snapshot_get_curr_slot_no(), polar_replica_multi_version_snapshot_get_slot_num()-1);
	if (polar_replica_multi_version_snapshot_get_next_slot_no() != 0)
		ELOG(ERROR, "snapshot next slot num wrong: wrong value is %d; correct value is %d", 
			 polar_replica_multi_version_snapshot_get_next_slot_no(), 0);

	/* clear test env */
	polar_test_set_curr_slot_num(-1);
	polar_test_set_next_slot_num(0);
	polar_test_KnownAssignedXidsReset();
}

static void
test_snapshot_access()
{
	TransactionId xip[10];
	int count; 
	bool overflowed;
	TransactionId xmin;
	TransactionId xmax;
	TransactionId replication_slot_xmin;
	TransactionId replication_slot_catalog_xmin;

	polar_test_KnownAssignedXidsAdd(3, 5);

	polar_test_replica_multi_version_snapshot_set_snapshot();

	polar_test_replica_multi_version_snapshot_get_snapshot(xip, &count, &overflowed, &xmin, &xmax,
													&replication_slot_xmin, &replication_slot_catalog_xmin);
	if (count != 3)
		ELOG(ERROR, "snapshot transaction count wrong: wrong value is %d; correct value is %d", 
			 count, 3);
	if (xmin != 3)
		ELOG(ERROR, "snapshot transaction xmin wrong: wrong value is %d; correct value is %d",
			 xmin, 3);
	if (xmax != GetLatestSnapshot()->xmax)
		ELOG(ERROR, "snapshot transaction xmax wrong: wrong value is %d; correct value is %d",
			 xmax, GetLatestSnapshot()->xmax);
	if (xip[0] != 3 || xip[1] != 4 || xip[2] != 5)
		ELOG(ERROR, "snapshot transaction array wrong: wrong value is %d-%d-%d; correct value is %d-%d-%d",
			 xip[0], xip[1], xip[2], 3, 4, 5);
	if (polar_replica_multi_version_snapshot_get_read_retried_times() != 0)
		ELOG(ERROR, "snapshot store read retried times wrong: wrong value is "UINT64_FORMAT"; correct value is %d",
			 polar_replica_multi_version_snapshot_get_read_retried_times(), 0);
	if (polar_replica_multi_version_snapshot_get_write_retried_times() != 0)
		ELOG(ERROR, "snapshot store write retried times wrong: wrong value is "UINT64_FORMAT"; correct value is %d", 
			 polar_replica_multi_version_snapshot_get_write_retried_times(), 0);

	/* clear test env */
	polar_test_set_curr_slot_num(-1);
	polar_test_set_next_slot_num(0);
	polar_test_KnownAssignedXidsReset();
}

static void
test_size()
{
	Size store_size;

	/* 
	 * In this test, we have configurations as below: ref test_multi_version_snapshot.conf
	 * 		polar_replica_multi_version_snapshot_slot_num = 32
	 * 		max_connections = 80	
     * 		max_prepared_transactions = 5
     * 		max_worker_processes = 9
     * 		autovacuum_max_workers = 5
	 * so we have some computed values as below:
	 * 		MaxBackends = max_connections + autovacuum_max_workers + 1 + max_worker_processes 
	 *                  = 80 + 5 + 1 + 9
	 *                  = 95
	 *  	PROCARRAY_MAXPROCS = MaxBackends + max_prepared_xacts = 95 + 5 = 100
	 * 		TOTAL_MAX_CACHED_SUBXIDS = (PGPROC_MAX_CACHED_SUBXIDS + 1) * PROCARRAY_MAXPROCS) 
	 *                               = (64 + 1) * 100
	 *                               = 6500
	 * then 
	 *      sizeof(polar_replica_multi_version_snapshot_t) = 32 + TOTAL_MAX_CACHED_SUBXIDS * 4 
	 *                                                     = 32 + 6500 * 4
	 * 													   = 26032 bytes
	 *      total shared memory size = 64 + 32*128 + 32*26032 = 418624
	 */
	store_size = polar_replica_multi_version_snapshot_store_shmem_size();
	if (store_size != 837184)
		ELOG(ERROR, "snapshot store size computed wrong: wrong value is %d, correct value is %d", 
			 (int)store_size, 837184);
}

PG_FUNCTION_INFO_V1(test_multi_version_snapshot);

/*
 * SQL-callable entry point to perform all tests.
 */
Datum
test_multi_version_snapshot(PG_FUNCTION_ARGS)
{
	test_size();

	test_snapshot_access();

	test_snapshot_access_with_roundwrap();

	test_snapshot_access_with_get_retry();

	test_snapshot_access_with_set_retry();

	PG_RETURN_VOID();
}