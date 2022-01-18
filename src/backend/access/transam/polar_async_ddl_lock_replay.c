/*-------------------------------------------------------------------------
 * polar_async_ddl_lock_replay.c
 *      async ddl lock replay routines.
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
 *      src/backend/access/transam/polar_async_ddl_lock_replay.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>

#include "access/polar_async_ddl_lock_replay.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/startup.h"
#include "replication/walreceiver.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/standby.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/timeout.h"

static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t shutdown_requested = false;
static bool update_ptr = false; /* if true, update lock ptr and wait shorter */

bool	polar_enable_async_ddl_lock_replay = true;
int		polar_async_ddl_lock_replay_worker_num = 1;
bool	polar_enable_async_ddl_lock_replay_unit_test = false;
polar_async_ddl_lock_replay_ctl_t *polar_async_ddl_lock_replay_ctl = NULL;

static HTAB *polar_pending_tx_tbl = NULL;
static HTAB *polar_pending_lock_tbl = NULL;
static polar_async_ddl_lock_replay_worker_t *MyWorker = NULL;

/* bgworker routines */
static bool register_async_ddl_lock_replay_worker(int id);
static void terminate_async_ddl_lock_replay_worker(int id);

static void worker_sighup_handler(SIGNAL_ARGS);
static void worker_sigterm_handler(SIGNAL_ARGS);
static void worker_sigusr1_handler(SIGNAL_ARGS);
static void worker_quit_handler(SIGNAL_ARGS);

static void polar_async_ddl_lock_replay_worker_init(int id);
static void polar_create_pending_tx_tbl(void);
static void polar_create_pending_lock_tbl(void);
static polar_pending_tx *polar_get_oldest_idle_tx(void);
static polar_pending_tx *polar_get_oldest_tx(void);
static bool polar_get_one_lock(polar_pending_tx *tx,
								polar_pending_lock *lock,
								bool dontWait);
static void polar_release_one_pending_lock(polar_pending_tx *tx,
										   polar_pending_lock *lock);
static void polar_remove_released_tx_and_update_ptr(void);

Size
polar_async_ddl_lock_replay_shmem_size(void)
{
	Size size = 0;

	if (!polar_enable_async_ddl_lock_replay)
		return size;

	size = offsetof(polar_async_ddl_lock_replay_ctl_t, workers);
	size = add_size(size, mul_size(sizeof(polar_async_ddl_lock_replay_worker_t),
					polar_async_ddl_lock_replay_worker_num));

	return size;
}

static void
terminate_async_ddl_lock_replay_worker(int id)
{
	TerminateBackgroundWorker((BackgroundWorkerHandle *)&polar_async_ddl_lock_replay_ctl->workers[id].handle);
	polar_async_ddl_lock_replay_ctl->workers[id].working = false;
}

void
polar_init_async_ddl_lock_replay(void)
{
	int i = 0;
	bool found = false;

	if (!polar_allow_async_ddl_lock_replay())
		return;

	polar_async_ddl_lock_replay_ctl = (polar_async_ddl_lock_replay_ctl_t *)
					ShmemInitStruct("async ddl lock replay control", 
							offsetof(polar_async_ddl_lock_replay_ctl_t, workers) + 
							mul_size(sizeof(polar_async_ddl_lock_replay_worker_t),
									 polar_async_ddl_lock_replay_worker_num), 
							&found);

	if (!IsUnderPostmaster)
	{
		polar_create_pending_tx_tbl();
		polar_create_pending_lock_tbl();
		polar_async_ddl_lock_replay_ctl->working = false;
		polar_async_ddl_lock_replay_ctl->entries = polar_pending_tx_tbl;
		polar_async_ddl_lock_replay_ctl->locks = polar_pending_lock_tbl;

		for (i = 0; i < polar_async_ddl_lock_replay_worker_num; ++i)
		{
			polar_async_ddl_lock_replay_ctl->workers[i].id = i;
			polar_async_ddl_lock_replay_ctl->workers[i].pid = 0;
			polar_async_ddl_lock_replay_ctl->workers[i].working = false;
			polar_async_ddl_lock_replay_ctl->workers[i].cur_tx = NULL;
			polar_async_ddl_lock_replay_ctl->workers[i].head = NULL;
		}

		LWLockRegisterTranche(LWTRANCHE_POLAR_PENDING_LOCK_TBL, "async ddl lock replay lock table lock");
		LWLockInitialize(&polar_async_ddl_lock_replay_ctl->lock_tbl_lock, LWTRANCHE_POLAR_PENDING_LOCK_TBL);
		LWLockRegisterTranche(LWTRANCHE_POLAR_PENDING_TX_TBL, "async ddl lock replay transaction table lock");
		LWLockInitialize(&polar_async_ddl_lock_replay_ctl->tx_tbl_lock, LWTRANCHE_POLAR_PENDING_TX_TBL);
		LWLockRegisterTranche(LWTRANCHE_POLAR_ASYNC_LOCK_WORKER, "async ddl lock replay async worker");
		LWLockRegisterTranche(LWTRANCHE_POLAR_ASYNC_LOCK_TX, "async ddl lock replay async transaction");
	}
	else
		Assert(found);
}

/*
 * POLAR: Register async ddl lock replay worker.
 *
 * Return true if RegisterDynamicBackgroundWorker return true, otherwise return false
 * which most likely means max_worker_processes is not enough.
 */
static bool
register_async_ddl_lock_replay_worker(int id)
{
	BackgroundWorker worker;
	polar_async_ddl_lock_replay_worker_handle_t *worker_handle;
	bool ret = false;

	memset(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_PostmasterStart;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	sprintf(worker.bgw_library_name, "postgres");
	sprintf(worker.bgw_function_name, "polar_async_ddl_lock_replay_worker_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, POLAR_ASYNC_DDL_LOCK_REPLAY_WORKER_NAME" %d", id);
	snprintf(worker.bgw_type, BGW_MAXLEN, POLAR_ASYNC_DDL_LOCK_REPLAY_WORKER_NAME);
	worker.bgw_main_arg = Int32GetDatum(id);
	worker.bgw_notify_pid = MyProcPid;

	if (RegisterDynamicBackgroundWorker(&worker, (BackgroundWorkerHandle **)&worker_handle))
	{
		polar_async_ddl_lock_replay_ctl->workers[id].handle.slot = worker_handle->slot;
		polar_async_ddl_lock_replay_ctl->workers[id].handle.generation = worker_handle->generation;
		pfree(worker_handle);
		ret = true;
	}
	else
	{
		if (polar_enable_debug)
		{
			ereport(LOG,
					(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
					 errmsg("could not register async ddl lock replay process"),
					 errhint("You may need to increase max_worker_processes.")));
		}
		ret = false;
	}

	return ret;
}

/*
 * POLAR: worker try to own a transaction.
 *
 * Get an oldest transaction from pending table, and try to own this transaction.
 * Once owned, no other worker will touch this transaction. Only startup and this
 * worker will mark it release, and remove this transaction.
 */
polar_pending_tx *
polar_own_pending_tx(void)
{
	polar_pending_tx *tx;
	tx = polar_get_oldest_idle_tx();

	if (tx != NULL && tx->worker == NULL)
	{
		LWLockAcquire(&tx->lock, LW_EXCLUSIVE);
		/* recheck */
		if (tx->worker != NULL)
		{
			LWLockRelease(&tx->lock);
			return NULL;
		}

		/* POLAR: we put new transaction to list head */
		tx->worker = MyWorker;
		LWLockRelease(&tx->lock);

		LWLockAcquire(&MyWorker->lock, LW_EXCLUSIVE);
		tx->next = MyWorker->head;
		MyWorker->head = tx;
		LWLockRelease(&MyWorker->lock);

		POLAR_ASYNC_DDL_LOCK_REPLAY_LOG_TX_WITH_LOCK("worker: own transaction success!", tx);
	}
	return tx;
}

/*
 * POLAR: worker try to get an oldest transaction.
 *
 * Get an oldest transaction from this worker.
 */
polar_pending_tx *
polar_get_oldest_tx(void)
{
	polar_pending_tx *tx;
	polar_pending_tx *oldest_tx;

	tx = MyWorker->head;
	oldest_tx = NULL;

	while (tx)
	{
		if (!oldest_tx && tx->last_ptr != InvalidXLogRecPtr)
			oldest_tx = tx;

		if (oldest_tx && tx->last_ptr != InvalidXLogRecPtr &&
			oldest_tx->last_ptr > tx->last_ptr)
			oldest_tx = tx;
		tx = tx->next;
	}

	return oldest_tx;
}

/*
 * POLAR: try to get locks of transaction one by one.
 *
 * Once failed, skip the rest and return.
 */
void
polar_get_lock_by_tx(polar_pending_tx *tx, bool dontWait)
{
	polar_pending_lock *lock;

	Assert(tx);

	lock = tx->cur_lock;

	LWLockAcquire(&MyWorker->lock, LW_EXCLUSIVE);
	MyWorker->cur_tx = tx;
	LWLockRelease(&MyWorker->lock);

	while (lock && polar_get_one_lock(tx, lock, dontWait))
	{
		/* POLAR: the lock must be got here, move to next lock */
		LWLockAcquire(&tx->lock, LW_EXCLUSIVE);
		if (tx->cur_lock->next != NULL)
		{
			tx->cur_lock = tx->cur_lock->next;
			tx->last_ptr = tx->cur_lock->last_ptr;
		}
		else
		{
			LWLockRelease(&tx->lock);
			/* all got, mark transaction's last_ptr InvalidXLogRecPtr */
			tx->last_ptr = InvalidXLogRecPtr;

			LWLockAcquire(&MyWorker->lock, LW_EXCLUSIVE);
			MyWorker->cur_tx = NULL;
			LWLockRelease(&MyWorker->lock);

			return;
		}
		LWLockRelease(&tx->lock);
		lock = lock->next;
		/* under unit test, we get one lock every time */
		if (polar_enable_async_ddl_lock_replay_unit_test)
			break;
	}
	LWLockAcquire(&MyWorker->lock, LW_EXCLUSIVE);
	MyWorker->cur_tx = NULL;
	LWLockRelease(&MyWorker->lock);
}

/*
 * POLAR: try to get one lock if not got
 */
static bool
polar_get_one_lock(polar_pending_tx *tx,
				   polar_pending_lock *lock,
				   bool dontWait)
{
	LOCKTAG locktag;
	LockAcquireResult result;

	Assert(lock);

	if (lock->state == POLAR_PENDING_LOCK_GOT)
		return true;

	lock->state = POLAR_PENDING_LOCK_GETTING;
	/* set receipt time, to cancel long query by max_standby_archive_delay */
	polar_set_receipt_time(lock->rtime);

	/* try to get lock */
	SET_LOCKTAG_RELATION(locktag, lock->dbOid, lock->relOid);
	result = LockAcquire(&locktag, AccessExclusiveLock, true, dontWait);

	if (result == LOCKACQUIRE_NOT_AVAIL)
	{
		/* get failed, reset it */
		lock->state = POLAR_PENDING_LOCK_IDLE;
		return false;
	}
	else
	{
		/* we got the lock, mark it */
		lock->state = POLAR_PENDING_LOCK_GOT;
		POLAR_ASYNC_DDL_LOCK_REPLAY_LOG_TX_WITH_LOCK("worker: got lock success!", tx);
		update_ptr = true;
		return true;
	}
}

/*
 * POLAR: try to release one lock if got
 */
static void
polar_release_one_pending_lock(polar_pending_tx *tx,
							   polar_pending_lock *lock)
{
	LOCKTAG locktag;
	Assert(lock);
	SET_LOCKTAG_RELATION(locktag, lock->dbOid, lock->relOid);
	/* if got, release the lock */
	if (lock->state == POLAR_PENDING_LOCK_GOT &&
		!LockRelease(&locktag, AccessExclusiveLock, true))
	{
		POLAR_ASYNC_DDL_LOCK_REPLAY_LOG_LOCK("worker: lock state error!", lock);
		Assert(false);
	}
	POLAR_ASYNC_DDL_LOCK_REPLAY_LOG_TX_WITH_LOCK("worker: release lock success!", tx);
}

/*
 * POLAR: remove released transaction and update ptr.
 */
static void
polar_remove_released_tx_and_update_ptr(void)
{
	polar_pending_tx *tx;
	polar_pending_tx *tx_del;
	polar_pending_tx *tx_it;
	polar_pending_lock *lock;
	polar_pending_lock *lock_next;

	tx = MyWorker->head;
	while (tx)
	{
		tx_del = tx;
		tx = tx->next;

		/* this tx is still in use */
		if (tx_del->commit_state == POLAR_PENDING_TX_UNKNOWN)
			continue;

		LWLockAcquire(&MyWorker->lock, LW_EXCLUSIVE);
		if (MyWorker->head == tx_del)
			MyWorker->head = tx;
		else
		{
			tx_it = MyWorker->head;
			while (tx_it)
			{
				if (tx_it->next == tx_del)
				{
					tx_it->next = tx;
					break;
				}
				tx_it = tx_it->next;
			}
		}
		LWLockRelease(&MyWorker->lock);

		update_ptr = true;

		/* release all locks got in this tx */
		LWLockAcquire(&tx_del->lock, LW_EXCLUSIVE);
		lock = tx_del->head;
		while (lock)
		{
			polar_release_one_pending_lock(tx_del, lock);
			lock_next = lock->next;
			LWLockAcquire(&polar_async_ddl_lock_replay_ctl->lock_tbl_lock, LW_EXCLUSIVE);
			hash_search(polar_pending_lock_tbl, lock, HASH_REMOVE, NULL);
			LWLockRelease(&polar_async_ddl_lock_replay_ctl->lock_tbl_lock);
			lock = lock_next;
		}
		LWLockRelease(&tx_del->lock);

		/* remove the transaction, we log it first */
		POLAR_ASYNC_DDL_LOCK_REPLAY_LOG_TX("worker: remove transaction success!", tx_del);
		LWLockAcquire(&polar_async_ddl_lock_replay_ctl->tx_tbl_lock, LW_EXCLUSIVE);
		hash_search(polar_pending_tx_tbl, tx_del, HASH_REMOVE, NULL);
		LWLockRelease(&polar_async_ddl_lock_replay_ctl->tx_tbl_lock);
	}
	if (update_ptr)
	{
		/* Update apply lsn, and notice wal recevier */
		polar_async_update_last_ptr();
		WalRcvForceReply();
	}
}

void
polar_release_all_pending_tx(void)
{
	LWLockAcquire(&MyWorker->lock, LW_EXCLUSIVE);
	MyWorker->cur_tx = MyWorker->head;
	while (MyWorker->cur_tx)
	{
		MyWorker->cur_tx->commit_state = POLAR_PENDING_TX_RELEASED;
		MyWorker->cur_tx = MyWorker->cur_tx->next;
	}
	LWLockRelease(&MyWorker->lock);
	polar_remove_released_tx_and_update_ptr();
}

static void
polar_async_ddl_lock_replay_worker_init(int id)
{
	InRecovery = true;
	standbyState = STANDBY_SNAPSHOT_READY;
	MyWorker = &polar_async_ddl_lock_replay_ctl->workers[id];
	MyWorker->id = id;
	MyWorker->pid = MyProcPid;
	MyWorker->working = true;
	LWLockInitialize(&MyWorker->lock, LWTRANCHE_POLAR_ASYNC_LOCK_WORKER);
}

/*
 * POLAR: Main Loop for this worker
 * 1. own a new transaction
 * 2. get lock nonblocking until failed for every transaction
 * 3. remove released transaction and updata ptr
 * 4. get an oldest transaction
 * 5. block getting the lock, if got, nonblocking get next one until failed
 * 6. remove released transaction and updata ptr
 * 7. wait_latch(timeout)
 */
void
polar_async_ddl_lock_replay_worker_main(Datum main_arg)
{
	int id = DatumGetInt32(main_arg);
	polar_pending_tx *tx = NULL;

	pqsignal(SIGHUP, worker_sighup_handler);	/* set flag to read config file */
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, worker_sigterm_handler);	/* shutdown */
	pqsignal(SIGQUIT, worker_quit_handler);		/* hard crash time */
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, worker_sigusr1_handler);

	/*
	 * Reset some signals that are accepted by postmaster but not here
	 */
	pqsignal(SIGCHLD, SIG_DFL);
	pqsignal(SIGTTIN, SIG_DFL);
	pqsignal(SIGTTOU, SIG_DFL);
	pqsignal(SIGCONT, SIG_DFL);
	pqsignal(SIGWINCH, SIG_DFL);

	RegisterTimeout(STANDBY_DEADLOCK_TIMEOUT, StandbyDeadLockHandler);
	RegisterTimeout(STANDBY_TIMEOUT, StandbyTimeoutHandler);
	RegisterTimeout(STANDBY_LOCK_TIMEOUT, StandbyLockTimeoutHandler);

	/* We allow SIGQUIT (quickdie) at all times */
	sigdelset(&BlockSig, SIGQUIT);
	BackgroundWorkerUnblockSignals();

	CurrentResourceOwner = ResourceOwnerCreate(NULL, "polar async ddl lock replay worker");
	elog(LOG, "polar async ddl lock replay worker %d", id);

	polar_async_ddl_lock_replay_worker_init(id);

	for (;;)
	{
		int rc = 0;
		update_ptr = false;

		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (shutdown_requested)
		{
			polar_release_all_pending_tx();
			/*
			 * From here on, elog(ERROR) should end with exit(1), not send
			 * control back to the sigsetjmp block above
			 */
			ExitOnAnyError = true;
			/* Normal exit from the bgwriter is here */
			proc_exit(0);	/* done */
		}

		polar_own_pending_tx();

		polar_remove_released_tx_and_update_ptr();

		tx = polar_get_oldest_tx();

		if (tx)
		{
			polar_get_lock_by_tx(tx, false);
			polar_remove_released_tx_and_update_ptr();
		}

		tx = NULL;

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   update_ptr ? 50 : 500 /* ms */, WAIT_EVENT_ASYNC_DDL_LOCK_REPLAY_MAIN);

		if (rc & WL_POSTMASTER_DEATH)
			exit(1);

		ResetLatch(MyLatch);
	}
}

/*
 * POLAR: init the pending transaction table.
 */
static void
polar_create_pending_tx_tbl(void)
{
	HASHCTL		hash_ctl;

	/*
	 * Initialize the hash table for tracking the pending locks
	 */
	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(TransactionId);
	hash_ctl.entrysize = sizeof(polar_pending_tx);
	polar_pending_tx_tbl = ShmemInitHash("polar async ddl lock replay pending entries",
									64, 64,
									&hash_ctl,
									HASH_ELEM | HASH_BLOBS);
}

/*
 * POLAR: init the pending lock table.
 */
static void
polar_create_pending_lock_tbl(void)
{
	HASHCTL		hash_ctl;

	/*
	 * Initialize the hash table for tracking the pending locks
	 */
	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(XLogRecPtr);
	hash_ctl.entrysize = sizeof(polar_pending_lock);
	polar_pending_lock_tbl = ShmemInitHash("polar async ddl lock replay pending locks",
									64, 64,
									&hash_ctl,
									HASH_ELEM | HASH_BLOBS);
}

/*
 * POLAR: Launch all async ddl lock replay workers and init each meta info.
 */
bool
polar_launch_async_ddl_lock_replay_workers(void)
{
	int			i = 0;

	if (!polar_allow_async_ddl_lock_replay())
		return false;

	for (i = 0; i < polar_async_ddl_lock_replay_worker_num; ++i)
	{
		/* start worker */
		if(register_async_ddl_lock_replay_worker(i))
			elog(LOG, "start polar async ddl lock replay worker %d started", i);
	}

	polar_async_ddl_lock_replay_ctl->working = true;

	return true;
}

bool
polar_stop_async_ddl_lock_replay_workers(void)
{
	int i = 0;

	if (!polar_allow_async_ddl_lock_replay())
		return false;

	elog(LOG, "stop all polar async ddl lock replay worker.");

	for (i = 0; i < polar_async_ddl_lock_replay_worker_num; ++i)
	{
		terminate_async_ddl_lock_replay_worker(i);
		elog(LOG, "polar async ddl lock replay worker %d stopped", i);
	}

	polar_async_ddl_lock_replay_ctl->working = false;

	return true;
}

/*
 * POLAR: add lock to pending table, and record the last_ptr, rtime.
 *
 * We will add this lock into lock_tbl and tx_tbl,
 * to store, track lock by transaction.
 */
void
polar_add_lock_to_pending_tbl(xl_standby_lock *lock, XLogRecPtr last_ptr, TimestampTz rtime)
{
	bool found;
	polar_pending_tx *tx;
	polar_pending_lock *newlock;

	LWLockAcquire(&polar_async_ddl_lock_replay_ctl->lock_tbl_lock, LW_EXCLUSIVE);
	newlock = hash_search(polar_pending_lock_tbl, &last_ptr, HASH_ENTER, &found);
	if (!found)
	{
		newlock->xid = lock->xid;
		newlock->dbOid = lock->dbOid;
		newlock->relOid = lock->relOid;

		newlock->last_ptr = last_ptr;
		newlock->rtime = rtime;
		newlock->state = POLAR_PENDING_LOCK_IDLE;

		newlock->tx = NULL;
		newlock->next = NULL;
	}
	else
	{
		POLAR_ASYNC_DDL_LOCK_REPLAY_LOG_LOCK("startup: add lock twice!", newlock);
		LWLockRelease(&polar_async_ddl_lock_replay_ctl->lock_tbl_lock);
		return;
	}
	LWLockRelease(&polar_async_ddl_lock_replay_ctl->lock_tbl_lock);

	LWLockAcquire(&polar_async_ddl_lock_replay_ctl->tx_tbl_lock, LW_EXCLUSIVE);
	tx = hash_search(polar_pending_tx_tbl, lock, HASH_ENTER, &found);
	newlock->tx = tx;
	if (!found)
	{
		tx->xid = lock->xid;
		tx->commit_state = POLAR_PENDING_TX_UNKNOWN;

		tx->head = newlock;
		tx->tail = newlock;
		tx->cur_lock = newlock;
		tx->next = NULL;

		tx->last_ptr = last_ptr;
		tx->worker = NULL;

		LWLockInitialize(&tx->lock, LWTRANCHE_POLAR_ASYNC_LOCK_TX);
		POLAR_ASYNC_DDL_LOCK_REPLAY_LOG_TX_WITH_LOCK("startup: add lock with new transaction success!", tx);
	}
	else
	{
		LWLockAcquire(&tx->lock, LW_EXCLUSIVE);
		tx->tail->next = newlock;
		tx->tail = newlock;
		if (tx->last_ptr == InvalidXLogRecPtr)
			tx->last_ptr = last_ptr;
		LWLockRelease(&tx->lock);
		POLAR_ASYNC_DDL_LOCK_REPLAY_LOG_TX_WITH_LOCK("startup: add lock success!", tx);
	}
	LWLockRelease(&polar_async_ddl_lock_replay_ctl->tx_tbl_lock);
}

/*
 * POLAR: release lock by xid. Actually it releases transaction and it's locks.
 * 
 * Only mark release here. The actual release part is in
 * polar_async_ddl_lock_replay_worker_main, contains two operation,
 * release the lock if got, and remove it.
 */
void
polar_async_ddl_lock_replay_release_one_tx(TransactionId xid)
{
	polar_pending_tx *tx;
	LWLockAcquire(&polar_async_ddl_lock_replay_ctl->tx_tbl_lock, LW_SHARED);

	tx = hash_search(polar_pending_tx_tbl, &xid, HASH_FIND, NULL);
	if (tx)
	{
		tx->commit_state = POLAR_PENDING_TX_RELEASED;
		POLAR_ASYNC_DDL_LOCK_REPLAY_LOG_TX_WITH_LOCK("startup: mark release transaction!", tx);
	}

	LWLockRelease(&polar_async_ddl_lock_replay_ctl->tx_tbl_lock);
}

/*
 * POLAR: release all locks.
 */
void
polar_async_ddl_lock_replay_release_all_tx(void)
{
	HASH_SEQ_STATUS status;
	polar_pending_tx *tx;
	LWLockAcquire(&polar_async_ddl_lock_replay_ctl->tx_tbl_lock, LW_SHARED);

	elog(LOG, "startup: mark release all lock!");

	hash_seq_init(&status, polar_pending_tx_tbl);
	while ((tx = hash_seq_search(&status)))
	{
		/* regard them as commited */
		tx->commit_state = POLAR_PENDING_TX_RELEASED;
		POLAR_ASYNC_DDL_LOCK_REPLAY_LOG_TX_WITH_LOCK("startup: mark release lock!", tx);
	}
	LWLockRelease(&polar_async_ddl_lock_replay_ctl->tx_tbl_lock);
}

bool
polar_allow_async_ddl_lock_replay(void)
{
	if (polar_enable_async_ddl_lock_replay_unit_test)
		return polar_enable_async_ddl_lock_replay && 
			(polar_async_ddl_lock_replay_worker_num > 0);
	return polar_in_replica_mode() &&
		polar_enable_async_ddl_lock_replay &&
		(polar_async_ddl_lock_replay_worker_num > 0);
}

/*
 * POLAR: get the oldest record order by last_ptr.
 *
 * It will return a oldest idle lock to the caller. If none, return NULL.
 */
static polar_pending_tx *
polar_get_oldest_idle_tx(void)
{
	HASH_SEQ_STATUS hash_seq;
	polar_pending_tx  *tx;
	polar_pending_tx  *oldest_tx;

	oldest_tx = NULL;

	LWLockAcquire(&polar_async_ddl_lock_replay_ctl->tx_tbl_lock, LW_SHARED);
	hash_seq_init(&hash_seq, polar_pending_tx_tbl);

	while ((tx = hash_seq_search(&hash_seq)) != NULL)
	{
		LWLockAcquire(&tx->lock, LW_SHARED);
		if (tx->last_ptr != InvalidXLogRecPtr && tx->worker == NULL &&
			oldest_tx == NULL)
			oldest_tx = tx;

		if (tx->last_ptr != InvalidXLogRecPtr && tx->worker == NULL &&
			oldest_tx->last_ptr > tx->last_ptr)
			oldest_tx = tx;
		LWLockRelease(&tx->lock);
	}

	LWLockRelease(&polar_async_ddl_lock_replay_ctl->tx_tbl_lock);
	return oldest_tx;
}

/*
 * POLAR: get the oldest last_ptr in pending transaction table, which is oldest lock ptr.
 *
 * If none, return InvalidXLogRecPtr, means there is no active lock need to get.
 */
XLogRecPtr
polar_get_async_ddl_lock_replay_oldest_ptr(void)
{
	HASH_SEQ_STATUS hash_seq;
	polar_pending_tx  *tx;
	XLogRecPtr oldest;

	oldest = InvalidXLogRecPtr;

	LWLockAcquire(&polar_async_ddl_lock_replay_ctl->tx_tbl_lock, LW_SHARED);
	hash_seq_init(&hash_seq, polar_pending_tx_tbl);

	while ((tx = hash_seq_search(&hash_seq)) != NULL)
	{
		LWLockAcquire(&tx->lock, LW_SHARED);
		if (tx->last_ptr != InvalidXLogRecPtr && oldest == InvalidXLogRecPtr)
			oldest = tx->last_ptr;

		if (tx->last_ptr != InvalidXLogRecPtr && oldest > tx->last_ptr)
			oldest = tx->last_ptr;
		LWLockRelease(&tx->lock);
	}

	LWLockRelease(&polar_async_ddl_lock_replay_ctl->tx_tbl_lock);
	return oldest;
}

/*
 * POLAR: return true if this xid's lock is already requested,
 * and not all lock are got.
 */
bool
polar_async_ddl_lock_replay_tx_is_replaying(TransactionId xid)
{
	bool found = false;
	polar_pending_tx *tx;
	LWLockAcquire(&polar_async_ddl_lock_replay_ctl->tx_tbl_lock, LW_SHARED);
	tx = hash_search(polar_pending_tx_tbl, &xid, HASH_FIND, &found);
	if (found)
		/* it got lock to replay, should wait here */
		found = tx->last_ptr != InvalidXLogRecPtr;
	LWLockRelease(&polar_async_ddl_lock_replay_ctl->tx_tbl_lock);
	return found;
}

/*
 * POLAR: return true if this lock is already requested.
 */
bool
polar_async_ddl_lock_replay_lock_is_replaying(xl_standby_lock *lock)
{
	bool found = false;
	bool result = false;
	polar_pending_tx *pending_tx;
	polar_pending_lock *pending_lock;
	LWLockAcquire(&polar_async_ddl_lock_replay_ctl->tx_tbl_lock, LW_SHARED);
	pending_tx = hash_search(polar_pending_tx_tbl, &lock->xid, HASH_FIND, &found);
	if (found)
	{
		LWLockAcquire(&pending_tx->lock, LW_SHARED);
		for (pending_lock = pending_tx->head; pending_lock != NULL; pending_lock = pending_lock->next)
		{
			if (pending_lock->dbOid == lock->dbOid && pending_lock->relOid == lock->relOid)
			{
				result = true;
				break;
			}
		}
		LWLockRelease(&pending_tx->lock);
	}
	LWLockRelease(&polar_async_ddl_lock_replay_ctl->tx_tbl_lock);
	return result;
}

polar_async_ddl_lock_replay_worker_t **
polar_async_ddl_lock_replay_get_myworker(void)
{
	return &MyWorker;
}

/* Signal handler for SIGTERM */
static void
worker_sigterm_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	shutdown_requested = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/* Signal handler for SIGHUP */
static void
worker_sighup_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sighup = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/* SIGUSR1: used for latch wakeups */
static void
worker_sigusr1_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	latch_sigusr1_handler();

	errno = save_errno;
}

static void
worker_quit_handler(SIGNAL_ARGS)
{
	_exit(2);
}
