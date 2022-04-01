/*-------------------------------------------------------------------------
 *
 * xlogparallel.c
 *	    parallel xlog recovery manager
 *
 * Author:  , 2020-07-07
 *
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *   http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/xlogparallel.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <ctype.h>
#include <fcntl.h>
#include <math.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include "access/clog.h"
#include "access/commit_ts.h"
#include "access/csnlog.h"
#include "access/hash.h"
#include "access/multixact.h"
#include "access/mvccvars.h"
#include "access/rewriteheap.h"
#include "access/subtrans.h"
#include "access/timeline.h"
#include "access/transam.h"
#include "access/tuptoaster.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "access/xlogreader.h"
#include "access/xlogutils.h"
#include "catalog/catversion.h"
#include "catalog/pg_control.h"
#include "catalog/pg_database.h"
#include "commands/tablespace.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "pgstat.h"
#include "port/atomics.h"
#include "postmaster/bgwriter.h"
#include "postmaster/postmaster.h"
#include "postmaster/startup.h"
#include "postmaster/walwriter.h"
#include "replication/basebackup.h"
#include "replication/logical.h"
#include "replication/origin.h"
#include "replication/slot.h"
#include "replication/snapbuild.h"
#include "replication/walreceiver.h"
#include "replication/walsender.h"
#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/large_object.h"
#include "storage/latch.h"
#include "storage/pmsignal.h"
#include "storage/predicate.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/reinit.h"
#include "storage/shm_mq.h"
#include "storage/smgr.h"
#include "storage/spin.h"
#include "tcop/tcopprot.h"
#include "utils/backend_random.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/ps_status.h"
#include "utils/relmapper.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "access/xlog.h"
#ifdef ENABLE_REMOTE_RECOVERY
#include "replication/remote_recovery.h"
#endif

int			max_parallel_replay_workers = 0;
int			max_workload_adjust_period = 10000000;
double		parallel_replay_workload_fluctuation_factor = 0.1;
bool		enable_dynamic_adjust_workload = true;
bool		enable_parallel_recovery_print = false;
bool		enable_parallel_recovery_bypage = false;
bool		enable_parallel_recovery_locklog = false;
bool		AllowHotStandbyInconsistency = true;

#define Dispatch_Func(relOid, forknum, blkno, workernum) ((relOid + (forknum * 7) + blkno + (relOid&3)*(workernum/4)) % (workernum))

#define MAX_QUEUE_SIZE (6 << 20)
#define BUFFER_QUEUE_SIZE (1 << 20)

/* guc control parameter */
int			max_queue_size;
int			replay_buffer_size;

/**  control the periodical logging of mq sizes for load balancing debug */
static uint64 redologcount = 0;

/*
 * Per parallel recovery worker state.
 */
typedef struct XLogParallelWorker
{
	pid_t		pid;			/* this worker's PID, or 0 if not active */

	int			worker_id;		/* this worker's ID starting from 0 */
	/* Protects shared variables shown above. */
	slock_t		mutex;

	/*
	 * Pointer to the worker's latch. Used by backends to wake up this worker
	 * when it has work to do. NULL if the worker isn't active.
	 */
	Latch		latch;

	shm_mq	   *mq;				/* per-worker message queue */
	shm_mq_handle *sender;		/* per-worker sending queue handle */
	shm_mq_handle *recver;		/* per-worker recving queue handle */
	int			msgCnt;
	int			relfilenodeCnt;
}			XLogParallelWorker;

typedef struct XLogParallelCtlData
{
	slock_t		mutex;
	Latch		latch;			/* the main xlog process's latch */
	int			nworkers_ready;
	int			nworkers_sync_done;
	int			msgTotalCnt;
#ifdef ENABLE_REMOTE_RECOVERY
	/* To support parallel standby fetch recovery */
	bool		enableRemoteFetchRecovery;
	XLogRecPtr	checkpointRedo;
	TimeLineID	checkpointTLI;
	char		standbyConn[MAXCONNINFO];
#endif
				XLogParallelWorker
				parallel_workers[FLEXIBLE_ARRAY_MEMBER];	/* parallel recovery
															 * worker state */
}			XLogParallelCtlData;

#define XLogParallelCtlDataSize offsetof(XLogParallelCtlData, parallel_workers)

static XLogParallelCtlData * XLogParallelCtl = NULL;
static XLogParallelWorker * MyXLogWorker = NULL;
static bool IsParallelReplayWorker = false;

typedef enum
{
	PARALLEL_REDO = 0,
	PARALLEL_END,
	PARALLEL_SYNC
}			WalRecordType;

/* Dispatch WAL record header structure */
typedef struct
{
	uint8		type;			/* record type: redo, finish or sync barrier */

	/*
	 * Start and end point of last record read.  EndRecPtr is also used as the
	 * position to read next, if XLogReadRecord receives an invalid recptr.
	 * Used to reconstruct xlogreader state in each worker.
	 */
	XLogRecPtr	ReadRecPtr;		/* start of last record read */
	XLogRecPtr	EndRecPtr;		/* end+1 of last record read */
}			WalRecordDataHeader;

#define SizeOfWalRecordHeader sizeof(WalRecordDataHeader)

/* xlog main process local structures */
typedef struct BufferState
{
	Size		size;
	Size		tail;
	char	   *data;
}			BufferState;

typedef struct
{
	int			nworkers;
	BufferState buffers[FLEXIBLE_ARRAY_MEMBER];
}			BufferQueues;

typedef struct
{
	int			nworkers;
	BackgroundWorkerHandle *handle[FLEXIBLE_ARRAY_MEMBER];
}			WorkerStates;

static WorkerStates * XLogWorkerStates = NULL;

static BufferQueues * XLogBufferQueues = NULL;

static BufferState * LocalReceiveBuffer = NULL; /* for each worker */

#define BufferFreeSpace(bufferState) ((bufferState)->size - (bufferState)->tail)
#define BufferHasData(bufferState) ((bufferState)->size - (bufferState)->tail)

static bool CheckWorkerStatus(void);

static void InitParallelRecoverySlot(void);

static void SetupBackgroundWorkers();

static void WaitForWorkersReady(void);

static void CleanupBackgroundWorkers(void);

static bool DispatchWalRecordToWorker(XLogRecord *record,
						  XLogReaderState *xlogreader);

static void SendWalRecordToQueue(XLogRecord *record,
					 WalRecordDataHeader * walRecordHeader,
					 int workerNum);
static void WaitForBatchWorkersSyncDone(bool *workerBatch, int len);
static int	AssignBlockToWorker(RelFileNode *rnode, ForkNumber forknum, BlockNumber blkno);

bool
IsBlockAssignedToThisWorker(Oid relFile, ForkNumber forknum, BlockNumber blkno)
{
	if (!IsParallelReplayWorker)
		return true;

	Assert(MyXLogWorker);
	if (Dispatch_Func(relFile, forknum, blkno, XLogParallelCtl->nworkers_ready) == MyXLogWorker->worker_id)
		return true;
	/* TODO!!! mod XLogWorkerStates->nworkers ?!!! WTF? */

	return false;
}

int
GetParallelRedoWorkerId(void)
{
	if (NULL == MyXLogWorker)
	{
		return -1;
	}
	return MyXLogWorker->worker_id;
}


Size
ParallelRecoveryShmemSize(void)
{
	Size		size;

	size = offsetof(XLogParallelCtlData, parallel_workers);
	size = add_size(
					size, mul_size(max_parallel_replay_workers, sizeof(XLogParallelWorker)));
	size = add_size(size, mul_size(max_parallel_replay_workers, (max_queue_size << 20)));

	elog(LOG, "parallel recovery: max_queue_size %d mb replay_buffer_size %d kb", max_queue_size, replay_buffer_size);
	return size;
}

static void
InitParallelRecoverySlot(void)
{
	int			i;

	elog(LOG, "xlog worker initialization start");

	Assert(XLogParallelCtl != NULL);
	Assert(MyXLogWorker == NULL);

	for (i = 0; i < max_parallel_replay_workers; i++)
	{
		XLogParallelWorker *worker = &XLogParallelCtl->parallel_workers[i];

		SpinLockAcquire(&worker->mutex);

		if (worker->pid != 0)
		{
			SpinLockRelease(&worker->mutex);
			continue;
		}
		else
		{
			/*
			 * Found a free slot. Reserve it for us.
			 */
			worker->pid = MyProcPid;

			SpinLockRelease(&worker->mutex);
			MyXLogWorker = worker;
			shm_mq_set_receiver(MyXLogWorker->mq, MyProc);
			MyXLogWorker->recver = shm_mq_attach(MyXLogWorker->mq, NULL, NULL);
			OwnLatch(&MyXLogWorker->latch);
			break;
		}
	}

	if (max_parallel_replay_workers && MyXLogWorker == NULL)
		ereport(FATAL, (errcode(ERRCODE_TOO_MANY_CONNECTIONS),
						errmsg("number of requested parallel xlog workers "
							   "exceeds max_parallel_replay_workers (currently %d)",
							   max_parallel_replay_workers)));

	SpinLockAcquire(&XLogParallelCtl->mutex);
	XLogParallelCtl->nworkers_ready++;
	SpinLockRelease(&XLogParallelCtl->mutex);
	SetLatch(&XLogParallelCtl->latch);

	elog(LOG, "xlog worker initialization finish");
}

void
ParallelRecoveryInit(void)
{
	bool		foundParallelCtl;
	int			i;
	char	   *queueStartAddress;

	XLogParallelCtl = (XLogParallelCtlData *) ShmemInitStruct(
															  "XLOG Parallel Ctl", ParallelRecoveryShmemSize(), &foundParallelCtl);

	elog(LOG, "init parallel recovery");
	if (foundParallelCtl)
		return;

	memset(XLogParallelCtl, 0, XLogParallelCtlDataSize);
	SpinLockInit(&XLogParallelCtl->mutex);

	queueStartAddress = (char *) XLogParallelCtl + XLogParallelCtlDataSize +
		max_parallel_replay_workers * sizeof(XLogParallelWorker);

	/*
	 * Setup per-worker queue.
	 */
	for (i = 0; i < max_parallel_replay_workers; i++)
	{
		XLogParallelWorker *worker = &XLogParallelCtl->parallel_workers[i];
		char	   *message_queue;

		SpinLockInit(&worker->mutex);
		worker->pid = 0;		/* unused slot */
		worker->worker_id = i;
		message_queue = queueStartAddress + i * (max_queue_size << 20);
		worker->mq = shm_mq_create(message_queue, (max_queue_size << 20));
		InitSharedLatch(&worker->latch);
		worker->msgCnt = 0;
		worker->relfilenodeCnt = 0;
	}

	XLogParallelCtl->msgTotalCnt = 0;
	elog(LOG, "parallel recovery init: max_queue_size %d replay_buffer_size %d", max_queue_size << 20, replay_buffer_size << 10);

	if ((replay_buffer_size << 10) > (max_queue_size << 20))
		elog(ERROR, "replay buffer size should not be larger than max queue size");

	InitSharedLatch(&XLogParallelCtl->latch);
}

static void
SetupBackgroundWorkers()
{
	BackgroundWorker worker;
	int			i;

	/* Configure a worker. */
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_PostmasterStart;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	sprintf(worker.bgw_library_name, "postgres");
	sprintf(worker.bgw_function_name, "ParallelRecoveryWorkerMain");
	snprintf(worker.bgw_type, BGW_MAXLEN, "parallel xlog worker");
	snprintf(worker.bgw_name, BGW_MAXLEN, "parallel xlog worker for PID %d",
			 MyProcPid);
	worker.bgw_main_arg = UInt32GetDatum(0);
	/* set bgw_notify_pid, so we can detect if the worker stops */
	worker.bgw_notify_pid = MyProcPid;

	/* Register the workers. */
	for (i = 0; i < max_parallel_replay_workers; ++i)
	{
		if (!RegisterDynamicBackgroundWorker(&worker, &XLogWorkerStates->handle[i]))
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
					 errmsg("could not register background process"),
					 errhint("You may need to increase max_worker_processes.")));

		XLogBufferQueues->buffers[i].data = palloc(replay_buffer_size << 10);
		XLogBufferQueues->buffers[i].tail = 0;
		XLogBufferQueues->buffers[i].size = replay_buffer_size << 10;
		++XLogWorkerStates->nworkers;
	}

	XLogBufferQueues->nworkers = XLogWorkerStates->nworkers;

	return;
}

static void
CleanupBackgroundWorkers(void)
{
	int			i;
	WalRecordDataHeader walRecordHeader;

	walRecordHeader.type = PARALLEL_END;

	for (i = 0; i < XLogWorkerStates->nworkers; i++)
	{
		XLogParallelWorker *worker = &XLogParallelCtl->parallel_workers[i];

		elog(LOG, "notify the worker %d of ending redo", worker->pid);

		SendWalRecordToQueue(NULL, &walRecordHeader, i);

		shm_mq_detach(worker->sender);
		pfree(XLogBufferQueues->buffers[i].data);
	}

	pfree(XLogWorkerStates);
	pfree(XLogBufferQueues);
}

static void
WaitForWorkersReady(void)
{
	bool		result = false;
	int			rc;

	for (;;)
	{
		int			nworkers_ready;

		/* If all the workers are ready, we have succeeded. */
		SpinLockAcquire(&XLogParallelCtl->mutex);
		nworkers_ready = XLogParallelCtl->nworkers_ready;
		SpinLockRelease(&XLogParallelCtl->mutex);

		if (nworkers_ready == XLogWorkerStates->nworkers)
		{
			elog(LOG, "parallel xlog workers are ready for replay");
			result = true;
			break;
		}

		/* If any workers (or the postmaster) have died, we have failed. */
		if (!CheckWorkerStatus())
		{
			result = false;
			break;
		}

		/* Wait to be signalled. */
		rc = WaitLatch(&XLogParallelCtl->latch, WL_LATCH_SET | WL_POSTMASTER_DEATH, 0,
					   WAIT_EVENT_BGWORKER_STARTUP);

		if (rc & WL_POSTMASTER_DEATH)
			exit(1);

		/* Reset the latch so we don't spin. */
		ResetLatch(&XLogParallelCtl->latch);

		/* An interrupt may have occurred while we were waiting. */
		CHECK_FOR_INTERRUPTS();
	}

	if (!result)
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
						errmsg("background workers failed to start")));
}

static bool
CheckWorkerStatus(void)
{
	int			n;

	/* If any workers (or the postmaster) have died, we have failed. */
	for (n = 0; n < XLogWorkerStates->nworkers; ++n)
	{
		BgwHandleStatus status;
		pid_t		pid;

		status = GetBackgroundWorkerPid(XLogWorkerStates->handle[n], &pid);
		if (status == BGWH_STOPPED || status == BGWH_POSTMASTER_DIED)
		{
			elog(LOG, "worker status error %d", status);
			return false;
		}
	}

	/* Otherwise, things still look OK. */
	return true;
}

static void
test_send(void)
{
	int			i;
	int			res;
	char		message[16] = "hello world";
	int			message_size = strlen(message) + 1;

	for (i = 0; i < XLogWorkerStates->nworkers; i++)
	{
		XLogParallelWorker *worker = &XLogParallelCtl->parallel_workers[i];
		PGPROC	   *recv;

		res = shm_mq_send(worker->sender, message_size, message, false);
		recv = shm_mq_get_receiver(worker->mq);
		elog(LOG, "send message %d to worker pid %d %d", message_size, worker->pid,
			 recv->pid);

		if (res != SHM_MQ_SUCCESS)
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("could not send message")));
	}
}
static int
test_receive(void)
{
	int			res;
	void	   *data;
	Size		len;

	Assert(MyXLogWorker);

	res = shm_mq_receive(MyXLogWorker->recver, &len, &data, false);
	if (res != SHM_MQ_SUCCESS)
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("could not receive message")));
	elog(LOG, "recv data %s len %lu", (char *) data, len);
	return len;
}

/*
 * The function entry is for xlog main process
 * to start up workers and initialize its per-worker dispatch queue.
 */
void
ParallelRecoveryStart(const char *standbyConnInfo, XLogRecPtr checkpointRedo, TimeLineID checkpointTLI)
{
	int			i;

	/* Create per-worker state object. */
	XLogWorkerStates =
		palloc0(offsetof(WorkerStates, handle) +
				sizeof(BackgroundWorkerHandle *) * max_parallel_replay_workers);

	XLogBufferQueues = palloc0(offsetof(BufferQueues, buffers) +
							   sizeof(BufferState) * max_parallel_replay_workers);

	if (max_parallel_replay_workers == 0)
		return;

	/*
	 * Postmaster does not start background workers in case of FatalError.
	 */
	if (PostmasterIsFatalError())
		return;

#ifdef ENABLE_REMOTE_RECOVERY
	if (EnableRemoteFetchRecovery)
	{
		Assert(standbyConnInfo != NULL);
		XLogParallelCtl->enableRemoteFetchRecovery = true;
		XLogParallelCtl->checkpointRedo = checkpointRedo;
		XLogParallelCtl->checkpointTLI = checkpointTLI;
		strlcpy(XLogParallelCtl->standbyConn, standbyConnInfo, MAXCONNINFO);
	}
	else
	{
		XLogParallelCtl->enableRemoteFetchRecovery = false;
	}
#endif

	OwnLatch(&XLogParallelCtl->latch);

	SetupBackgroundWorkers();
	for (i = 0; i < max_parallel_replay_workers; i++)
	{
		XLogParallelWorker *worker = &XLogParallelCtl->parallel_workers[i];

		shm_mq_set_sender(worker->mq, MyProc);
		worker->sender =
			shm_mq_attach(worker->mq, NULL, XLogWorkerStates->handle[i]);
	}
	WaitForWorkersReady();
	/* test message queue */
	test_send();

	elog(LOG, "start up xlog workers");
}

void
ParallelRecoveryFinish(void)
{
	/*
	 * Wait for all workers to run to completion before notifying them to exit
	 * so as to ensure all WAL entries dispatched have been replayed.
	 */
	WaitForWorkersSyncDone();
	CleanupBackgroundWorkers();
}

static void
SendBuffer(BufferState * bufferState, XLogParallelWorker * worker)
{
	int			res;

	Assert(bufferState->tail);
	res =
		shm_mq_send(worker->sender, bufferState->tail, bufferState->data, false);
	if (res != SHM_MQ_SUCCESS)
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("could not send buffer %d", res)));

	bufferState->tail = 0;
}

static void
SendWalRecordToQueue(XLogRecord *record,
					 WalRecordDataHeader * walRecordHeader,
					 int workerNum)
{
	BufferState *bufferState = &XLogBufferQueues->buffers[workerNum];
	XLogParallelWorker *worker = &XLogParallelCtl->parallel_workers[workerNum];
	Size		send_size = SizeOfWalRecordHeader;
	char	   *ptr;

	if (walRecordHeader->type == PARALLEL_REDO)
		send_size += record->xl_tot_len;

	if (send_size > BufferFreeSpace(bufferState))
		SendBuffer(bufferState, worker);

	ptr = bufferState->data + bufferState->tail;
	memcpy(ptr, (char *) walRecordHeader, SizeOfWalRecordHeader);

	if (walRecordHeader->type == PARALLEL_REDO)
	{
		ptr += SizeOfWalRecordHeader;
		memcpy(ptr, (char *) record, record->xl_tot_len);
	}

	bufferState->tail += send_size;

	if (walRecordHeader->type == PARALLEL_END ||
		walRecordHeader->type == PARALLEL_SYNC)
		SendBuffer(bufferState, worker);
}

static int
AssignBlockToWorker(RelFileNode *rnode, ForkNumber forknum, BlockNumber blkno)
{
	return Dispatch_Func(rnode->relNode, forknum, blkno, XLogWorkerStates->nworkers);
}

static bool
DispatchWalRecordToWorker(XLogRecord *record,
						  XLogReaderState *xlogreader)
{
	BlockNumber blkno;
	RelFileNode target_node;
	uint32		workerNum;
	WalRecordDataHeader walRecordHeader;
	XLogParallelWorker *worker;
	bool		found;
	ParallelRedoRelfilenodeMapEntry *entry = NULL;
	int			i;

	if (!XLogRecGetBlockTag(xlogreader, 0, &target_node, NULL, &blkno))
	{
		uint8		info = XLogRecGetInfo(xlogreader) & ~XLR_INFO_MASK;

		elog(DEBUG2, "WAL record cannot find its referenced relfile %d info %x",
			 record->xl_rmid, info);
		return false;
	}

	if (enable_dynamic_adjust_workload && enable_parallel_recovery_bypage)
	{
		elog(WARNING, "enable_dynamic_adjust_workload and enable_parallel_recovery_bypage can not be on at the same time! Turning off enable_parallel_recovery_bypage!");
		enable_dynamic_adjust_workload = false;
	}
	if (enable_dynamic_adjust_workload)
	{
		if (XLogParallelCtl->msgTotalCnt >= max_workload_adjust_period)
		{
			int			avgCnt = XLogParallelCtl->msgTotalCnt / max_parallel_replay_workers;
			int			targetRange = parallel_replay_workload_fluctuation_factor * avgCnt;
			int			topLine = avgCnt + targetRange;
			int			bottomLine = avgCnt - targetRange;
			int			diffCnt[XLogWorkerStates->nworkers];
			HASH_SEQ_STATUS status;
			ParallelRedoRelfilenodeMapEntry *entryTmp = NULL;
			bool		needAdjust = false;
			bool		needSync[max_parallel_replay_workers];

			hash_seq_init(&status, ParallelRedoRelfilenodeMapHash);
			for (i = 0; i < max_parallel_replay_workers; i++)
			{
				if ((XLogParallelCtl->parallel_workers[i].msgCnt > topLine
					 || XLogParallelCtl->parallel_workers[i].msgCnt < bottomLine))
				{
					diffCnt[i] = XLogParallelCtl->parallel_workers[i].msgCnt - avgCnt;
					if ((!needAdjust) && diffCnt[i] > 0 && XLogParallelCtl->parallel_workers[i].relfilenodeCnt > 1)
						needAdjust = true;
				}
				else
					diffCnt[i] = 0;

				elog(LOG, "worker%d: message count is %d, refinenode conunt is %d, avgCnt is %d, pid is %d",
					 i, XLogParallelCtl->parallel_workers[i].msgCnt,
					 XLogParallelCtl->parallel_workers[i].relfilenodeCnt,
					 avgCnt, XLogParallelCtl->parallel_workers[i].pid);
				XLogParallelCtl->parallel_workers[i].msgCnt = 0;
				needSync[i] = false;
			}
			while ((entryTmp = (ParallelRedoRelfilenodeMapEntry *) hash_seq_search(&status)) != NULL)
			{
				if (needAdjust && entryTmp->msgCnt > 0)
				{
					if (diffCnt[entryTmp->workerIndex] > 0
						&& XLogParallelCtl->parallel_workers[entryTmp->workerIndex].relfilenodeCnt > 1
						&& diffCnt[entryTmp->workerIndex] > targetRange)
					{
						int			j;
						int			minDiff = 0;
						int			maxRelfile = 0;

						for (j = 0; j < max_parallel_replay_workers; j++)
						{
							if (j != entryTmp->workerIndex && diffCnt[j] < 0 &&
								(entryTmp->msgCnt + diffCnt[j] <= targetRange))
								break;
							if (minDiff == 0 || (diffCnt[j] < 0 && diffCnt[j] < diffCnt[minDiff]))
								minDiff = j;
							if (maxRelfile == 0 || (XLogParallelCtl->parallel_workers[j].relfilenodeCnt >
													XLogParallelCtl->parallel_workers[maxRelfile].relfilenodeCnt))
								maxRelfile = j;
						}
						if (j != max_parallel_replay_workers)
						{
							elog(LOG, "refilenode msgCnt is %d, move from %d to %d", entryTmp->msgCnt, entryTmp->workerIndex, j);
							needSync[entryTmp->workerIndex] = true;
							diffCnt[entryTmp->workerIndex] =
								diffCnt[entryTmp->workerIndex] - entryTmp->msgCnt;
							diffCnt[j] = diffCnt[j] + entryTmp->msgCnt;
							XLogParallelCtl->parallel_workers[entryTmp->workerIndex].relfilenodeCnt--;
							XLogParallelCtl->parallel_workers[j].relfilenodeCnt++;
							entryTmp->workerIndex = j;
						}
						else
						{
							int			moveto = entryTmp->workerIndex;

							if (diffCnt[minDiff] < 0)
							{
								if (diffCnt[minDiff] + entryTmp->msgCnt > diffCnt[entryTmp->workerIndex])
								{
									moveto = entryTmp->workerIndex;
								}
								else if (diffCnt[minDiff] + entryTmp->msgCnt < diffCnt[entryTmp->workerIndex])
								{
									moveto = minDiff;
								}
								else if (XLogParallelCtl->parallel_workers[minDiff].relfilenodeCnt > XLogParallelCtl->parallel_workers[entryTmp->workerIndex].relfilenodeCnt)
								{
									moveto = minDiff;
								}
							}

							if (moveto != entryTmp->workerIndex)
							{
								elog(LOG, "large refilenode msgCnt is %d, move from %d to %d", entryTmp->msgCnt, entryTmp->workerIndex, moveto);
								needSync[entryTmp->workerIndex] = true;
								diffCnt[entryTmp->workerIndex] =
									diffCnt[entryTmp->workerIndex] - entryTmp->msgCnt;
								diffCnt[moveto] = diffCnt[moveto] + entryTmp->msgCnt;
								XLogParallelCtl->parallel_workers[entryTmp->workerIndex].relfilenodeCnt--;
								XLogParallelCtl->parallel_workers[moveto].relfilenodeCnt++;
								entryTmp->workerIndex = moveto;
							}
							else
								elog(LOG, "large refilenode msgCnt is %d, worker is %d, is not moved", entryTmp->msgCnt, entryTmp->workerIndex);
						}
					}
				}
				entryTmp->msgCnt = 0;
			}
			if (needAdjust)
				WaitForBatchWorkersSyncDone(needSync, max_parallel_replay_workers);
			XLogParallelCtl->msgTotalCnt = 0;
		}
		entry = hash_search(ParallelRedoRelfilenodeMapHash, (void *) &target_node, HASH_FIND, &found);

		if (found)
		{
			workerNum = entry->workerIndex;
		}
		else
		{
			workerNum = target_node.relNode % XLogWorkerStates->nworkers;
			entry = hash_search(ParallelRedoRelfilenodeMapHash, (void *) &target_node, HASH_ENTER, &found);
			if (found)
				elog(ERROR, "corrupted hashtable");
			entry->workerIndex = workerNum;
			entry->msgCnt = 0;
			XLogParallelCtl->parallel_workers[workerNum].relfilenodeCnt++;
		}

		entry->msgCnt++;
		XLogParallelCtl->msgTotalCnt++;
		XLogParallelCtl->parallel_workers[workerNum].msgCnt++;
	}
	else
	{
		workerNum = target_node.relNode % XLogWorkerStates->nworkers;
	}
	worker = &XLogParallelCtl->parallel_workers[workerNum];

	if (enable_parallel_recovery_bypage)
	{
		RelFileNode rnode;
		ForkNumber	forknum;
		BlockNumber blkno;
		int			block_id;

		for (block_id = 0; block_id <= xlogreader->max_block_id; block_id++)
		{
			if (!XLogRecGetBlockTag(xlogreader, block_id, &rnode, &forknum, &blkno))
			{
				/*
				 * WAL record doesn't contain a block reference with the given
				 * id. Do nothing.
				 */
				continue;
			}

			workerNum = AssignBlockToWorker(&rnode, forknum, blkno);

			worker = &XLogParallelCtl->parallel_workers[workerNum];

			if (enable_parallel_recovery_print)
				elog(LOG, "dispatch reocrd xid %d relnode %d blkno %u redo at %X/%X to worker %d",
					 record->xl_xid, target_node.relNode,
					 blkno,
					 (uint32) (xlogreader->ReadRecPtr >> 32), (uint32) xlogreader->ReadRecPtr,
					 worker->pid);

			walRecordHeader.ReadRecPtr = xlogreader->ReadRecPtr;
			walRecordHeader.EndRecPtr = xlogreader->EndRecPtr;
			walRecordHeader.type = PARALLEL_REDO;
			SendWalRecordToQueue(record, &walRecordHeader, workerNum);
		}
		if ((++redologcount & 0xFFFFFFL) == 0)
		{
			/* debug print every 16M */
			for (i = 0; i < max_parallel_replay_workers; i++)
			{
				elog(LOG, "worker%d: mq size: %lu", i, shm_mq_get_used_bytes(XLogParallelCtl->parallel_workers[i].sender));
			}
		}

	}
	else
	{
		workerNum = target_node.relNode % XLogWorkerStates->nworkers;

		worker = &XLogParallelCtl->parallel_workers[workerNum];

		if (enable_parallel_recovery_print)
			elog(LOG, "dispatch reocrd xid %d relnode %d redo at %X/%X to worker %d",
				 record->xl_xid, target_node.relNode,
				 (uint32) (xlogreader->ReadRecPtr >> 32), (uint32) xlogreader->ReadRecPtr,
				 worker->pid);

		walRecordHeader.ReadRecPtr = xlogreader->ReadRecPtr;
		walRecordHeader.EndRecPtr = xlogreader->EndRecPtr;
		walRecordHeader.type = PARALLEL_REDO;
		SendWalRecordToQueue(record, &walRecordHeader, workerNum);
	}

	return true;
}

void
WaitForWorkersSyncDone(void)
{
	int			i;
	WalRecordDataHeader walRecordHeader;

	walRecordHeader.type = PARALLEL_SYNC;
	elog(LOG, "dispatch sync reocrd");

	XLogParallelCtl->nworkers_sync_done = 0;

	for (i = 0; i < XLogWorkerStates->nworkers; i++)
		SendWalRecordToQueue(NULL, &walRecordHeader, i);

	/*
	 * Wait for completion.
	 */
	for (;;)
	{
		int			nworkers_sync;
		int			rc;

		/* If all the workers are ready, we have succeeded. */
		SpinLockAcquire(&XLogParallelCtl->mutex);
		nworkers_sync = XLogParallelCtl->nworkers_sync_done;
		SpinLockRelease(&XLogParallelCtl->mutex);

		if (nworkers_sync == XLogWorkerStates->nworkers)
		{
			elog(LOG, "workers sync completion");
			break;
		}

		/* If any workers (or the postmaster) have died, we have failed. */
		if (!CheckWorkerStatus())
		{
			ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
							errmsg("background workers failed")));
			break;
		}

		/* Wait to be signalled. */
		rc = WaitLatch(&XLogParallelCtl->latch, WL_LATCH_SET | WL_POSTMASTER_DEATH,
					   0, WAIT_EVENT_PARALLEL_FINISH);

		if (rc & WL_POSTMASTER_DEATH)
			exit(1);
		/* Reset the latch so we don't spin. */
		ResetLatch(&XLogParallelCtl->latch);

		/* An interrupt may have occurred while we were waiting. */
		CHECK_FOR_INTERRUPTS();
	}
}
static void
WaitForBatchWorkersSyncDone(bool *workerBatch, int len)
{
	int			i;
	int			synNum = 0;
	int			nworkers_sync = 0;
	WalRecordDataHeader walRecordHeader;

	walRecordHeader.type = PARALLEL_SYNC;

	XLogParallelCtl->nworkers_sync_done = 0;

	for (i = 0; i < len; i++)
	{
		if (workerBatch[i])
		{
			SendWalRecordToQueue(NULL, &walRecordHeader, i);
			synNum++;
		}
	}
	if (synNum == 0)
		return;

	elog(LOG, "dispatch sync record");

	/*
	 * Wait for completion.
	 */
	for (;;)
	{
		/* If all the workers are ready, we have succeeded. */
		SpinLockAcquire(&XLogParallelCtl->mutex);
		nworkers_sync = XLogParallelCtl->nworkers_sync_done;
		SpinLockRelease(&XLogParallelCtl->mutex);

		if (nworkers_sync == synNum)
		{
			elog(LOG, "workers sync completion");
			break;
		}

		/* If any workers (or the postmaster) have died, we have failed. */
		for (i = 0; i < len; i++)
		{
			if (workerBatch[i])
			{
				BgwHandleStatus status;
				pid_t		pid;

				status = GetBackgroundWorkerPid(XLogWorkerStates->handle[i], &pid);
				if (status == BGWH_STOPPED || status == BGWH_POSTMASTER_DIED)
				{
					elog(LOG, "worker status error %d", status);
					ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
									errmsg("background workers failed")));
					break;
				}
			}
		}
		pg_usleep(1L);
	}
}

/*
 * Dispatch the WAL record to the corresponding worker
 * for parallel replay to catch up the master under high
 * workloads.
 */
bool
DispatchWalRecord(XLogRecord *record, XLogReaderState *xlogreader)
{
	bool		canDispatch = false;

	/*
	 * TODO: currently do not support hot standby.
	 */
	if ((EnableHotStandby && !AllowHotStandbyInconsistency))
		return false;

	if (0 == XLogWorkerStates->nworkers)
		return false;

	switch (record->xl_rmid)
	{
			/*
			 * The WAL records of the below types are replayed in parallel.
			 * Each WAL record is dispatched based on its associated
			 * relfilenode to make sure that modifications to the same relfile
			 * must be replayed by the same worker to resolve dependencies.
			 */
		case RM_HEAP_ID:
		case RM_HEAP2_ID:
		case RM_BTREE_ID:
		case RM_HASH_ID:
		case RM_GIN_ID:
		case RM_GIST_ID:
		case RM_SEQ_ID:
		case RM_SPGIST_ID:
		case RM_BRIN_ID:
		case RM_GENERIC_ID:
			if (DispatchWalRecordToWorker(record, xlogreader))
				canDispatch = true;
			break;

			/*
			 * Xact-related and database operation related WAL records should
			 * be replayed by the main xlog startup process.
			 */
		case RM_XACT_ID:
		case RM_CLOG_ID:
		case RM_CTSLOG_ID:
		case RM_MULTIXACT_ID:
		case RM_COMMIT_TS_ID:
		case RM_STANDBY_ID:
		case RM_REPLORIGIN_ID:
		case RM_LOGICALMSG_ID:
			break;

			/*
			 * Checkpoint-related WAL record should be replayed by the main
			 * xlog process and must be used as a barrier to wait for all
			 * workers to complete all previous WAL record replay to guarantee
			 * correctness.
			 */
		case RM_XLOG_ID:
			/* sync barrier */
			{
				uint8		info = record->xl_info & ~XLR_INFO_MASK;

				if (info == XLOG_CHECKPOINT_SHUTDOWN || info == XLOG_END_OF_RECOVERY ||
					info == XLOG_CHECKPOINT_ONLINE || info == XLOG_BACKUP_END ||
					info == XLOG_PARAMETER_CHANGE || info == XLOG_FPW_CHANGE)
				{
					WaitForWorkersSyncDone();
				}
				else if (info == XLOG_FPI || info == XLOG_FPI_FOR_HINT)
				{
					/*
					 * full page writes for hints should be dispatched
					 * according to relfile
					 */
					if (DispatchWalRecordToWorker(record, xlogreader))
						canDispatch = true;
					else
						elog(ERROR, "full page image hint cannot be dispatched %d",
							 record->xl_rmid);
				}
			}
			break;

			/*
			 * Database, tablespace and relation related WAL records need to
			 * sync with parallel workers to make sure a consistent recovery
			 * point can be reached. Relation drop is handled by xact_redo and
			 * a sync barrier is also added within the xact_redo routine.
			 */
		case RM_SMGR_ID:
		case RM_DBASE_ID:
		case RM_TBLSPC_ID:
		case RM_RELMAP_ID:
			WaitForWorkersSyncDone();
			break;
		default:
			elog(FATAL, "unsupport WAL record type %d", record->xl_rmid);
	}

	return canDispatch;
}

static void
InitRecoveryEnvironment(void)
{
	int			rmid;

	InRecovery = true;
	/* Initialize resource managers */
	for (rmid = 0; rmid <= RM_MAX_ID; rmid++)
	{
		if (RmgrTable[rmid].rm_startup != NULL)
			RmgrTable[rmid].rm_startup();
	}
}

static void
CleanupRecoveryEnvironment(void)
{
	int			rmid;

	/* Cleanup resource managers */
	for (rmid = 0; rmid <= RM_MAX_ID; rmid++)
	{
		if (RmgrTable[rmid].rm_cleanup != NULL)
			RmgrTable[rmid].rm_cleanup();
	}
}

static void
ValidXLogRecord(XLogRecord *record)
{
	pg_crc32c	crc;

	/* Calculate the CRC */
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, ((char *) record) + SizeOfXLogRecord,
				record->xl_tot_len - SizeOfXLogRecord);
	/* include the record header last */
	COMP_CRC32C(crc, (char *) record, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(crc);

	if (!EQ_CRC32C(record->xl_crc, crc))
		elog(ERROR, "WAL record is corrupted crc %u expected %u", crc,
			 record->xl_crc);

	return;
}

/* Buffers dedicated to consistency checks of size BLCKSZ */
static char *replay_image_masked = NULL;
static char *master_image_masked = NULL;

/*
 * Checks whether the current buffer page and backup page stored in the
 * WAL record are consistent or not. Before comparing the two pages, a
 * masking can be applied to the pages to ignore certain areas like hint bits,
 * unused space between pd_lower and pd_upper among other things. This
 * function should be called once WAL replay has been completed for a
 * given record.
 */
static void
checkXLogConsistency(XLogReaderState *record)
{
	RmgrId		rmid = XLogRecGetRmid(record);
	RelFileNode rnode;
	ForkNumber	forknum;
	BlockNumber blkno;
	int			block_id;

	/* Records with no backup blocks have no need for consistency checks. */
	if (!XLogRecHasAnyBlockRefs(record))
		return;

	Assert((XLogRecGetInfo(record) & XLR_CHECK_CONSISTENCY) != 0);

	for (block_id = 0; block_id <= record->max_block_id; block_id++)
	{
		Buffer		buf;
		Page		page;

		if (!XLogRecGetBlockTag(record, block_id, &rnode, &forknum, &blkno))
		{
			/*
			 * WAL record doesn't contain a block reference with the given id.
			 * Do nothing.
			 */
			continue;
		}

		Assert(XLogRecHasBlockImage(record, block_id));

		if (XLogRecBlockImageApply(record, block_id))
		{
			/*
			 * WAL record has already applied the page, so bypass the
			 * consistency check as that would result in comparing the full
			 * page stored in the record with itself.
			 */
			continue;
		}

		/*
		 * Read the contents from the current buffer and store it in a
		 * temporary page.
		 */
		buf = XLogReadBufferExtended(rnode, forknum, blkno, RBM_NORMAL_NO_LOG);
		if (!BufferIsValid(buf))
			continue;

		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buf);

		/*
		 * Take a copy of the local page where WAL has been applied to have a
		 * comparison base before masking it...
		 */
		memcpy(replay_image_masked, page, BLCKSZ);

		/* No need for this page anymore now that a copy is in. */
		UnlockReleaseBuffer(buf);

		/*
		 * If the block LSN is already ahead of this WAL record, we can't
		 * expect contents to match.  This can happen if recovery is
		 * restarted.
		 */
		if (PageGetLSN(replay_image_masked) > record->EndRecPtr)
			continue;

		/*
		 * Read the contents from the backup copy, stored in WAL record and
		 * store it in a temporary page. There is no need to allocate a new
		 * page here, a local buffer is fine to hold its contents and a mask
		 * can be directly applied on it.
		 */
		if (!RestoreBlockImage(record, block_id, master_image_masked))
			elog(ERROR, "failed to restore block image");

		/*
		 * If masking function is defined, mask both the master and replay
		 * images
		 */
		if (RmgrTable[rmid].rm_mask != NULL)
		{
			RmgrTable[rmid].rm_mask(replay_image_masked, blkno);
			RmgrTable[rmid].rm_mask(master_image_masked, blkno);
		}

		/* Time to compare the master and replay images. */
		if (memcmp(replay_image_masked, master_image_masked, BLCKSZ) != 0)
		{
			elog(FATAL, "inconsistent page found, rel %u/%u/%u, forknum %u, blkno %u",
				 rnode.spcNode, rnode.dbNode, rnode.relNode, forknum, blkno);
		}
	}
}

static void
ReconstructXlogReader(WalRecordDataHeader * walRecordHeader,
					  XLogReaderState *xlogreader)
{
	xlogreader->EndRecPtr = walRecordHeader->EndRecPtr;
	xlogreader->ReadRecPtr = walRecordHeader->ReadRecPtr;
}

static void
NotifyDispatcherCompletion(void)
{
	bool		need_notify = false;
	int			sync_done;

	SpinLockAcquire(&XLogParallelCtl->mutex);
	XLogParallelCtl->nworkers_sync_done++;
	sync_done = XLogParallelCtl->nworkers_sync_done;

	SpinLockRelease(&XLogParallelCtl->mutex);

	if (sync_done == XLogParallelCtl->nworkers_ready)
		need_notify = true;

	if (enable_parallel_recovery_print)
		elog(LOG, "sync done num %d", sync_done);

	/*
	 * If it is the last completed worker, we should notify the dispatcher.
	 */
	if (need_notify)
	{
		elog(LOG, "notify the dispatcher of the sync completion");
		SetLatch(&XLogParallelCtl->latch);
	}
}

static void
ReadWalRecordFromBuffer(WalRecordDataHeader * *walRecordHeaderP,
						XLogRecord **recordP)
{
	int			res;
	char	   *ptr;
	WalRecordDataHeader *walRecordHeader;
	XLogRecord *record;

	if (!LocalReceiveBuffer->size || !BufferHasData(LocalReceiveBuffer))
	{
		res = shm_mq_receive(MyXLogWorker->recver, &LocalReceiveBuffer->size,
							 (void **) &LocalReceiveBuffer->data, false);
		if (res != SHM_MQ_SUCCESS)
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("could not receive wal record header %d", res)));

		if (LocalReceiveBuffer->size > (replay_buffer_size << 10))
			elog(ERROR, "could not receive too long data %lu expected < %d",
				 LocalReceiveBuffer->size, replay_buffer_size << 10);

		LocalReceiveBuffer->tail = 0;
	}

	ptr = LocalReceiveBuffer->data + LocalReceiveBuffer->tail;
	*walRecordHeaderP = (WalRecordDataHeader *) ptr;
	if (SizeOfWalRecordHeader > BufferFreeSpace(LocalReceiveBuffer))
		elog(ERROR, "corrupted record header of size %lu expected %lu",
			 BufferFreeSpace(LocalReceiveBuffer), SizeOfWalRecordHeader);
	LocalReceiveBuffer->tail += SizeOfWalRecordHeader;
	walRecordHeader = *walRecordHeaderP;
	if (walRecordHeader->type == PARALLEL_REDO)
	{
		ptr += SizeOfWalRecordHeader;
		*recordP = (XLogRecord *) ptr;
		record = *recordP;

		if (record->xl_tot_len > BufferFreeSpace(LocalReceiveBuffer))
			elog(ERROR, "corrupted wal record of size %lu expected %u",
				 BufferFreeSpace(LocalReceiveBuffer), record->xl_tot_len);

		/* validate the reocrd */
		ValidXLogRecord(record);
		LocalReceiveBuffer->tail += record->xl_tot_len;
	}
}

/*
 * The xlog worker function entry.
 */
void
ParallelRecoveryWorkerMain(Datum main_arg)
{
	XLogReaderState *xlogreader;
	XLogRecord *record;
	WalRecordDataHeader *walRecordHeader;
	char	   *errm;

	/* Establish signal handlers. */
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	InitParallelRecoverySlot();
	/* test message queue */
	test_receive();

#ifdef ENABLE_REMOTE_RECOVERY
	/* Init per-worker standby fetch streaming */
	if (XLogParallelCtl->enableRemoteFetchRecovery)
	{
		checkpointRedo = XLogParallelCtl->checkpointRedo;
		checkpointTLI = XLogParallelCtl->checkpointTLI;
		InitRemoteFetchPageStreaming(XLogParallelCtl->standbyConn);
		EnableRemoteFetchRecovery = true;
	}
#endif

	/* Initialize the recovery environment */
	InitRecoveryEnvironment();

	IsParallelReplayWorker = true;

	/* allocate a xlogreader for redo routine */
	xlogreader = XLogReaderAllocate(wal_segment_size, NULL, NULL);
	if (!xlogreader)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory"),
				 errdetail("Failed while allocating a WAL reading processor.")));

	/* setup local buffer */
	LocalReceiveBuffer = palloc0(sizeof(BufferState));
	LocalReceiveBuffer->size = 0;
	LocalReceiveBuffer->tail = 0;

	/*
	 * Allocate two page buffers dedicated to WAL consistency checks.  We do
	 * it this way, rather than just making static arrays, for two reasons:
	 * (1) no need to waste the storage in most instantiations of the backend;
	 * (2) a static char array isn't guaranteed to have any particular
	 * alignment, whereas palloc() will provide MAXALIGN'd storage.
	 */
	replay_image_masked = (char *) palloc(BLCKSZ);
	master_image_masked = (char *) palloc(BLCKSZ);

	Assert(MyXLogWorker);
	do
	{
		record = NULL;
		walRecordHeader = NULL;
		ReadWalRecordFromBuffer(&walRecordHeader, &record);
		if (walRecordHeader->type == PARALLEL_END)
		{
			elog(LOG, "redo worker %d exits", MyXLogWorker->pid);
			break;
		}
		else if (walRecordHeader->type == PARALLEL_SYNC)
		{
			/*
			 * Notify the xlog process of the completion of replaying previous
			 * WAL records in the queue.
			 */
			NotifyDispatcherCompletion();
			continue;
		}

		Assert(record);
		/* reconstruct xlog reader state */
		ReconstructXlogReader(walRecordHeader, xlogreader);

		if (enable_parallel_recovery_print)
			elog(LOG, "recv redo record xid %d", record->xl_xid);

		if (!DecodeXLogRecord(xlogreader, record, &errm))
			elog(ERROR, "fail to decode dispatched XLog record %s",
				 xlogreader->errormsg_buf);

		if (enable_parallel_recovery_print)
			elog(LOG, "redo at %X/%X", (uint32) (xlogreader->ReadRecPtr >> 32),
				 (uint32) xlogreader->ReadRecPtr);

		/* do the acutal redo stuff */
		RmgrTable[record->xl_rmid].rm_redo(xlogreader);

		/*
		 * After redo, check whether the backup pages associated with the WAL
		 * record are consistent with the existing pages. This check is done
		 * only if consistency check is enabled for this record.
		 */
		if ((record->xl_info & XLR_CHECK_CONSISTENCY) != 0)
			checkXLogConsistency(xlogreader);

		CHECK_FOR_INTERRUPTS();

	} while (true);

	/* cleanup redo environment before exiting */
	CleanupRecoveryEnvironment();
	pfree(LocalReceiveBuffer);
	XLogReaderFree(xlogreader);

	shm_mq_detach(MyXLogWorker->recver);

#ifdef ENABLE_REMOTE_RECOVERY
	if (XLogParallelCtl->enableRemoteFetchRecovery)
		FinishRemoteFetchPageStreaming();
#endif
	proc_exit(1);
}
