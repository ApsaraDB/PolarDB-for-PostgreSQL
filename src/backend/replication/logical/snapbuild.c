/*-------------------------------------------------------------------------
 *
 * snapbuild.c
 *
 *	  Infrastructure for building historic catalog snapshots based on contents
 *	  of the WAL, for the purpose of decoding heapam.c style values in the
 *	  WAL.
 *
 * NOTES:
 *
 * We build snapshots which can *only* be used to read catalog contents and we
 * do so by reading and interpreting the WAL stream. The aim is to build a
 * snapshot that behaves the same as a freshly taken MVCC snapshot would have
 * at the time the XLogRecord was generated.
 *
 * To build the snapshots we reuse the infrastructure built for Hot
 * Standby. The in-memory snapshots we build look different than HS' because
 * we have different needs. To successfully decode data from the WAL we only
 * need to access catalog tables and (sys|rel|cat)cache, not the actual user
 * tables since the data we decode is wholly contained in the WAL
 * records. Also, our snapshots need to be different in comparison to normal
 * MVCC ones because in contrast to those we cannot fully rely on the clog and
 * pg_subtrans for information about committed transactions because they might
 * commit in the future from the POV of the WAL entry we're currently
 * decoding. This definition has the advantage that we only need to prevent
 * removal of catalog rows, while normal table's rows can still be
 * removed. This is achieved by using the replication slot mechanism.
 *
 * As the percentage of transactions modifying the catalog normally is fairly
 * small in comparisons to ones only manipulating user data, we keep track of
 * the committed catalog modifying ones inside [xmin, xmax) instead of keeping
 * track of all running transactions like it's done in a normal snapshot. Note
 * that we're generally only looking at transactions that have acquired an
 * xid. That is we keep a list of transactions between snapshot->(xmin, xmax)
 * that we consider committed, everything else is considered aborted/in
 * progress. That also allows us not to care about subtransactions before they
 * have committed which means this module, in contrast to HS, doesn't have to
 * care about suboverflowed subtransactions and similar.
 *
 * One complexity of doing this is that to e.g. handle mixed DDL/DML
 * transactions we need Snapshots that see intermediate versions of the
 * catalog in a transaction. During normal operation this is achieved by using
 * CommandIds/cmin/cmax. The problem with that however is that for space
 * efficiency reasons only one value of that is stored
 * (cf. combocid.c). Since ComboCids are only available in memory we log
 * additional information which allows us to get the original (cmin, cmax)
 * pair during visibility checks. Check the reorderbuffer.c's comment above
 * ResolveCminCmaxDuringDecoding() for details.
 *
 * To facilitate all this we need our own visibility routine, as the normal
 * ones are optimized for different usecases.
 *
 * To replace the normal catalog snapshots with decoding ones use the
 * SetupHistoricSnapshot() and TeardownHistoricSnapshot() functions.
 *
 *
 *
 * The snapbuild machinery is starting up in several stages, as illustrated
 * by the following graph describing the SnapBuild->state transitions:
 *
 *		   +-------------------------+
 *	  +----|		 START			 |-------------+
 *	  |    +-------------------------+			   |
 *	  |					|						   |
 *	  |					|						   |
 *	  |		   running_xacts #1					   |
 *	  |					|						   |
 *	  |					|						   |
 *	  |					v						   |
 *	  |    +-------------------------+			   v
 *	  |    |   BUILDING_SNAPSHOT	 |------------>|
 *	  |    +-------------------------+			   |
 *	  |					|						   |
 *	  |					|						   |
 *	  | running_xacts #2, xacts from #1 finished   |
 *	  |					|						   |
 *	  |					|						   |
 *	  |					v						   |
 *	  |    +-------------------------+			   v
 *	  |    |	   FULL_SNAPSHOT	 |------------>|
 *	  |    +-------------------------+			   |
 *	  |					|						   |
 * running_xacts		|					   saved snapshot
 * with zero xacts		|				  at running_xacts's lsn
 *	  |					|						   |
 *	  | running_xacts with xacts from #2 finished  |
 *	  |					|						   |
 *	  |					v						   |
 *	  |    +-------------------------+			   |
 *	  +--->|SNAPBUILD_CONSISTENT	 |<------------+
 *		   +-------------------------+
 *
 * Initially the machinery is in the START stage. When an xl_running_xacts
 * record is read that is sufficiently new (above the safe xmin horizon),
 * there's a state transition. If there were no running xacts when the
 * running_xacts record was generated, we'll directly go into CONSISTENT
 * state, otherwise we'll switch to the BUILDING_SNAPSHOT state. Having a full
 * snapshot means that all transactions that start henceforth can be decoded
 * in their entirety, but transactions that started previously can't. In
 * FULL_SNAPSHOT we'll switch into CONSISTENT once all those previously
 * running transactions have committed or aborted.
 *
 * Only transactions that commit after CONSISTENT state has been reached will
 * be replayed, even though they might have started while still in
 * FULL_SNAPSHOT. That ensures that we'll reach a point where no previous
 * changes has been exported, but all the following ones will be. That point
 * is a convenient point to initialize replication from, which is why we
 * export a snapshot at that point, which *can* be used to read normal data.
 *
 * Copyright (c) 2012-2018, PostgreSQL Global Development Group
 *
 *
 * We develop a Commit-Timestamp-Store(CTS) based logical replication
 * by using CTS as a MVCC snapshot to copy inital table and
 * decoding incremental updates in WAL from consistent point.
 *
 * The logical replication consists of two states:start and consistent state.
 * Before entering consistent state, we wait for the running xacts before
 * initial_xmin_horizon to complete, which also indicates the end of the running xacts
 * collected by the start state.
 *
 *
 *		   +-------------------------+
 *	  +----|		 START			 |-------------+
 *	       +-------------------------+
 *	  					|
 *	  					|
 *	  		   running_xacts #1	(RX1)
 *	  					|
 *	  					|
 *	  					v
 *	       +-------------------------+
 *	       |       CONSISTENT        |   running_xacts #1 finished
 *	       +-------------------------+
 *
 *
 * Then we generate a timestamp snapshot (HLC/TSO) to copy inital table and store
 * the snapshot in replication slot for later decoding.
 *
 * Decoding from consistent point only interests in the committed xacts which are not
 * visible to the built snapshot.
 *
 * The key to the correctness is that only transactions from running xacts 1 (RX1) span
 * cross the consistent point in WAL and they are all visisble to the built snapshot.
 * Starting from the consistent point, logical replication can decode entire transactions
 * in WAL only except for RX1.
 *
 * Author:  , 2020.11.07
 *
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 *
 * IDENTIFICATION
 *	  src/backend/replication/snapbuild.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "miscadmin.h"

#include "access/heapam_xlog.h"
#include "access/transam.h"
#include "access/xact.h"

#include "pgstat.h"

#include "replication/logical.h"
#include "replication/reorderbuffer.h"
#include "replication/snapbuild.h"

#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/snapshot.h"
#include "utils/snapmgr.h"
#include "utils/tqual.h"

#include "storage/block.h"		/* debugging output */
#include "storage/fd.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/standby.h"

#ifdef ENABLE_DISTRIBUTED_TRANSACTION
#include "access/ctslog.h"
#include "distributed_txn/txn_timestamp.h"
#endif

/*
 * This struct contains the current state of the snapshot building
 * machinery. Besides a forward declaration in the header, it is not exposed
 * to the public, so we can easily change its contents.
 */
struct SnapBuild
{
	/* how far are we along building our first full snapshot */
	SnapBuildState state;

	/* private memory context used to allocate memory for this module. */
	MemoryContext context;

	/* all transactions < than this have committed/aborted */
	TransactionId xmin;

	/* all transactions >= than this are uncommitted */
	TransactionId xmax;

	/* this determines the state of transactions between xmin and xmax */
	CommitSeqNo snapshotcsn;

	/*
	 * Don't replay commits from an LSN < this LSN. This can be set externally
	 * but it will also be advanced (never retreat) from within snapbuild.c.
	 */
	XLogRecPtr	start_decoding_at;

	/*
	 * Don't start decoding WAL until the "xl_running_xacts" information
	 * indicates there are no running xids with an xid smaller than this.
	 */
	TransactionId initial_xmin_horizon;


	/* Indicates if we are building full snapshot or just catalog one. */
	bool		building_full_snapshot;

	/*
	 * Snapshot that's valid to see the catalog state seen at this moment.
	 */
	Snapshot	snapshot;

	/*
	 * The reorderbuffer we need to update with usable snapshots et al.
	 */
	ReorderBuffer *reorder;
};

/*
 * Starting a transaction -- which we need to do while exporting a snapshot --
 * removes knowledge about the previously used resowner, so we save it here.
 */
static ResourceOwner SavedResourceOwnerDuringExport = NULL;
static bool ExportInProgress = false;

/* snapshot building/manipulation/distribution functions */
static Snapshot SnapBuildBuildSnapshot(SnapBuild *builder);

static void SnapBuildFreeSnapshot(Snapshot snap);

static void SnapBuildSnapIncRefcount(Snapshot snap);

static void SnapBuildDistributeNewCatalogSnapshot(SnapBuild *builder, XLogRecPtr lsn);


/*
 * Allocate a new snapshot builder.
 *
 * xmin_horizon is the xid >= which we can be sure no catalog rows have been
 * removed, start_lsn is the LSN >= we want to replay commits.
 */
SnapBuild *
AllocateSnapshotBuilder(ReorderBuffer *reorder,
						TransactionId xmin_horizon,
						XLogRecPtr start_lsn,
						bool need_full_snapshot)
{
	MemoryContext context;
	MemoryContext oldcontext;
	SnapBuild  *builder;

	/* allocate memory in own context, to have better accountability */
	context = AllocSetContextCreate(CurrentMemoryContext,
									"snapshot builder context",
									ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(context);

	builder = palloc0(sizeof(SnapBuild));

	builder->state = SNAPBUILD_START;
	builder->context = context;
	builder->reorder = reorder;
	/* Other struct members initialized by zeroing via palloc0 above */
	builder->initial_xmin_horizon = xmin_horizon;
	builder->start_decoding_at = start_lsn;
	builder->building_full_snapshot = need_full_snapshot;

	MemoryContextSwitchTo(oldcontext);

	return builder;
}

/*
 * Free a snapshot builder.
 */
void
FreeSnapshotBuilder(SnapBuild *builder)
{
	MemoryContext context = builder->context;

	/* free snapshot explicitly, that contains some error checking */
	if (builder->snapshot != NULL)
	{
		SnapBuildSnapDecRefcount(builder->snapshot);
		builder->snapshot = NULL;
	}

	/* other resources are deallocated via memory context reset */
	MemoryContextDelete(context);
}

/*
 * Free an unreferenced snapshot that has previously been built by us.
 */
static void
SnapBuildFreeSnapshot(Snapshot snap)
{
	/* make sure we don't get passed an external snapshot */
	Assert(snap->satisfies == HeapTupleSatisfiesHistoricMVCC);

	/* make sure nobody modified our snapshot */
	Assert(snap->curcid == FirstCommandId);
	Assert(!snap->takenDuringRecovery);
	Assert(snap->regd_count == 0);

	/* slightly more likely, so it's checked even without c-asserts */
	if (snap->copied)
		elog(ERROR, "cannot free a copied snapshot");

	if (snap->active_count)
		elog(ERROR, "cannot free an active snapshot");

	pfree(snap);
}

/*
 * In which state of snapshot building are we?
 */
SnapBuildState
SnapBuildCurrentState(SnapBuild *builder)
{
	return builder->state;
}

/*
 * Should the contents of transaction ending at 'ptr' be decoded?
 */
bool
SnapBuildXactNeedsSkip(SnapBuild *builder, XLogRecPtr ptr)
{
	return ptr < builder->start_decoding_at;
}

/*
 * Increase refcount of a snapshot.
 *
 * This is used when handing out a snapshot to some external resource or when
 * adding a Snapshot as builder->snapshot.
 */
static void
SnapBuildSnapIncRefcount(Snapshot snap)
{
	snap->active_count++;
}

/*
 * Decrease refcount of a snapshot and free if the refcount reaches zero.
 *
 * Externally visible, so that external resources that have been handed an
 * IncRef'ed Snapshot can adjust its refcount easily.
 */
void
SnapBuildSnapDecRefcount(Snapshot snap)
{
	/* make sure we don't get passed an external snapshot */
	Assert(snap->satisfies == HeapTupleSatisfiesHistoricMVCC);

	/* make sure nobody modified our snapshot */
	Assert(snap->curcid == FirstCommandId);
	Assert(!snap->takenDuringRecovery);

	Assert(snap->regd_count == 0);

	Assert(snap->active_count > 0);

	/* slightly more likely, so it's checked even without casserts */
	if (snap->copied)
		elog(ERROR, "cannot free a copied snapshot");

	snap->active_count--;
	if (snap->active_count == 0)
		SnapBuildFreeSnapshot(snap);
}

/*
 * Build a new snapshot, based on currently committed catalog-modifying
 * transactions.
 *
 * In-progress transactions with catalog access are *not* allowed to modify
 * these snapshots; they have to copy them and fill in appropriate ->curcid
 * and ->subxip/subxcnt values.
 */
static Snapshot
SnapBuildBuildSnapshot(SnapBuild *builder)
{
	Snapshot	snapshot;
	Size		ssize;

	Assert(builder->state >= SNAPBUILD_CONSISTENT);

	ssize = sizeof(SnapshotData)
		+ sizeof(TransactionId) * 1 /* toplevel xid */ ;

	snapshot = MemoryContextAllocZero(builder->context, ssize);

	snapshot->satisfies = HeapTupleSatisfiesHistoricMVCC;

	/*
	 * Snapshots that are used in transactions that have modified the catalog
	 * use the 'this_xip' array to store their toplevel xid and all the
	 * subtransaction xids so we can recognize when we need to treat rows as
	 * visible that would not normally be visible by the CSN test. this_xip
	 * only gets filled when the transaction is copied into the context of a
	 * catalog modifying transaction since we otherwise share a snapshot
	 * between transactions. As long as a txn hasn't modified the catalog it
	 * doesn't need to treat any uncommitted rows as visible, so there is no
	 * need for those xids.
	 *
	 * this_xip array is qsort'ed so that we can use bsearch() on them.
	 */
	Assert(TransactionIdIsNormal(builder->xmin));
	Assert(TransactionIdIsNormal(builder->xmax));
	Assert(builder->snapshotcsn != InvalidCommitSeqNo);

	snapshot->xmin = builder->xmin;
	snapshot->xmax = builder->xmax;
	snapshot->snapshotcsn = builder->snapshotcsn;

	/*
	 * Initially, this_xip is empty, i.e. it's a snapshot to be used by
	 * transactions that don't modify the catalog. Will be filled by
	 * ReorderBufferCopySnap() if necessary.
	 */
	snapshot->this_xcnt = 0;
	snapshot->this_xip = NULL;

	snapshot->takenDuringRecovery = false;
	snapshot->copied = false;
	snapshot->curcid = FirstCommandId;
	snapshot->active_count = 0;
	snapshot->regd_count = 0;

	return snapshot;
}

/*
 * Build the initial slot snapshot and convert it to a normal snapshot that
 * is understood by HeapTupleSatisfiesMVCC.
 *
 * The snapshot will be usable directly in current transaction or exported
 * for loading in different transaction.
 */
Snapshot
SnapBuildInitialSnapshot(SnapBuild *builder)
{
	Snapshot	snap;

	Assert(!FirstSnapshotSet);
	Assert(XactIsoLevel == XACT_REPEATABLE_READ);

	if (builder->state != SNAPBUILD_CONSISTENT)
		elog(ERROR, "cannot build an initial slot snapshot before reaching a consistent state");

	/* so we don't overwrite the existing value */
	if (TransactionIdIsValid(MyPgXact->xmin))
		elog(ERROR, "cannot build an initial slot snapshot when MyPgXact->xmin already is valid");

	snap = SnapBuildBuildSnapshot(builder);

	/*
	 * We know that snap->xmin is alive, enforced by the logical xmin
	 * mechanism. Due to that we can do this without locks, we're only
	 * changing our own value.
	 */
	MyPgXact->snapshotcsn = snap->snapshotcsn;

	return snap;
}

/*
 * Export a snapshot so it can be set in another session with SET TRANSACTION
 * SNAPSHOT.
 *
 * For that we need to start a transaction in the current backend as the
 * importing side checks whether the source transaction is still open to make
 * sure the xmin horizon hasn't advanced since then.
 */
const char *
SnapBuildExportSnapshot(SnapBuild *builder)
{
	Snapshot	snap;
	char	   *snapname;

	if (IsTransactionOrTransactionBlock())
		elog(ERROR, "cannot export a snapshot from within a transaction");

	if (SavedResourceOwnerDuringExport)
		elog(ERROR, "can only export one snapshot at a time");

	SavedResourceOwnerDuringExport = CurrentResourceOwner;
	ExportInProgress = true;

	StartTransactionCommand();

	/* There doesn't seem to a nice API to set these */
	XactIsoLevel = XACT_REPEATABLE_READ;
	XactReadOnly = true;

	snap = SnapBuildInitialSnapshot(builder);

	/*
	 * now that we've built a plain snapshot, make it active and use the
	 * normal mechanisms for exporting it
	 */
	snapname = ExportSnapshot(snap);

	ereport(LOG,
			(errmsg("exported logical decoding snapshot: \"%s\" at %X/%X",
					snapname,
					(uint32) (snap->snapshotcsn >> 32),
					(uint32) snap->snapshotcsn)));
	return snapname;
}

/*
 * Ensure there is a snapshot and if not build one for current transaction.
 */
Snapshot
SnapBuildGetOrBuildSnapshot(SnapBuild *builder, TransactionId xid)
{
	Assert(builder->state == SNAPBUILD_CONSISTENT);

	/* only build a new snapshot if we don't have a prebuilt one */
	if (builder->snapshot == NULL)
	{
		builder->snapshot = SnapBuildBuildSnapshot(builder);
		/* increase refcount for the snapshot builder */
		SnapBuildSnapIncRefcount(builder->snapshot);
	}

	return builder->snapshot;
}

/*
 * Reset a previously SnapBuildExportSnapshot()'ed snapshot if there is
 * any. Aborts the previously started transaction and resets the resource
 * owner back to its original value.
 */
void
SnapBuildClearExportedSnapshot(void)
{
	/* nothing exported, that is the usual case */
	if (!ExportInProgress)
		return;

	if (!IsTransactionState())
		elog(ERROR, "clearing exported snapshot in wrong transaction state");

	/* make sure nothing  could have ever happened */
	AbortCurrentTransaction();

	CurrentResourceOwner = SavedResourceOwnerDuringExport;
	SavedResourceOwnerDuringExport = NULL;
	ExportInProgress = false;
}

/*
 * Handle the effects of a single heap change, appropriate to the current state
 * of the snapshot builder and returns whether changes made at (xid, lsn) can
 * be decoded.
 */
bool
SnapBuildProcessChange(SnapBuild *builder, TransactionId xid, XLogRecPtr lsn)
{
	/*
	 * We can't handle data in transactions if we haven't built a snapshot
	 * yet, so don't store them.
	 */
	if (builder->state < SNAPBUILD_CONSISTENT)
		return false;

	/*
	 * If the reorderbuffer doesn't yet have a snapshot, add one now, it will
	 * be needed to decode the change we're currently processing.
	 */
	if (!ReorderBufferXidHasBaseSnapshot(builder->reorder, xid))
	{
		/* only build a new snapshot if we don't have a prebuilt one */
		if (builder->snapshot == NULL)
		{
			builder->snapshot = SnapBuildBuildSnapshot(builder);
			/* increase refcount for the snapshot builder */
			SnapBuildSnapIncRefcount(builder->snapshot);
		}

		/*
		 * Increase refcount for the transaction we're handing the snapshot
		 * out to.
		 */
		SnapBuildSnapIncRefcount(builder->snapshot);
		ReorderBufferSetBaseSnapshot(builder->reorder, xid, lsn,
									 builder->snapshot);
	}

	return true;
}

/*
 * Do CommandId/ComboCid handling after reading an xl_heap_new_cid record.
 * This implies that a transaction has done some form of write to system
 * catalogs.
 */
void
SnapBuildProcessNewCid(SnapBuild *builder, TransactionId xid,
					   XLogRecPtr lsn, xl_heap_new_cid *xlrec)
{
	CommandId	cid;

	/*
	 * we only log new_cid's if a catalog tuple was modified, so mark the
	 * transaction as containing catalog modifications
	 */
	ReorderBufferXidSetCatalogChanges(builder->reorder, xid, lsn);

	ReorderBufferAddNewTupleCids(builder->reorder, xlrec->top_xid, lsn,
								 xlrec->target_node, xlrec->target_tid,
								 xlrec->cmin, xlrec->cmax,
								 xlrec->combocid);

	/* figure out new command id */
	if (xlrec->cmin != InvalidCommandId &&
		xlrec->cmax != InvalidCommandId)
		cid = Max(xlrec->cmin, xlrec->cmax);
	else if (xlrec->cmax != InvalidCommandId)
		cid = xlrec->cmax;
	else if (xlrec->cmin != InvalidCommandId)
		cid = xlrec->cmin;
	else
	{
		cid = InvalidCommandId; /* silence compiler */
		elog(ERROR, "xl_heap_new_cid record without a valid CommandId");
	}

	ReorderBufferAddNewCommandId(builder->reorder, xid, lsn, cid + 1);
}

/*
 * Add a new Snapshot to all transactions we're decoding that currently are
 * in-progress so they can see new catalog contents made by the transaction
 * that just committed. This is necessary because those in-progress
 * transactions will use the new catalog's contents from here on (at the very
 * least everything they do needs to be compatible with newer catalog
 * contents).
 */
static void
SnapBuildDistributeNewCatalogSnapshot(SnapBuild *builder, XLogRecPtr lsn)
{
	dlist_iter	txn_i;
	ReorderBufferTXN *txn;

	/*
	 * Iterate through all toplevel transactions. This can include
	 * subtransactions which we just don't yet know to be that, but that's
	 * fine, they will just get an unnecessary snapshot queued.
	 */
	dlist_foreach(txn_i, &builder->reorder->toplevel_by_lsn)
	{
		txn = dlist_container(ReorderBufferTXN, node, txn_i.cur);

		Assert(TransactionIdIsValid(txn->xid));

		/*
		 * If we don't have a base snapshot yet, there are no changes in this
		 * transaction which in turn implies we don't yet need a snapshot at
		 * all. We'll add a snapshot when the first change gets queued.
		 *
		 * NB: This works correctly even for subtransactions because
		 * ReorderBufferAssignChild() takes care to transfer the base snapshot
		 * to the top-level transaction, and while iterating the changequeue
		 * we'll get the change from the subtxn.
		 */
		if (!ReorderBufferXidHasBaseSnapshot(builder->reorder, txn->xid))
			continue;

		elog(DEBUG2, "adding a new snapshot to %u at %X/%X",
			 txn->xid, (uint32) (lsn >> 32), (uint32) lsn);

		/*
		 * increase the snapshot's refcount for the transaction we are handing
		 * it out to
		 */
		SnapBuildSnapIncRefcount(builder->snapshot);
		ReorderBufferAddSnapshot(builder->reorder, txn->xid, lsn,
								 builder->snapshot);
	}
}

/*
 * Handle everything that needs to be done when a transaction commits
 */
void
SnapBuildCommitTxn(SnapBuild *builder, XLogRecPtr lsn, TransactionId xid,
				   int nsubxacts, TransactionId *subxacts)
{
	int			nxact;

	bool		forced_timetravel = false;

	TransactionId xmax;

	/*
	 * If we couldn't observe every change of a transaction because it was
	 * already running at the point we started to observe we have to assume it
	 * made catalog changes.
	 *
	 * This has the positive benefit that we afterwards have enough
	 * information to build an exportable snapshot that's usable by pg_dump et
	 * al.
	 */
	if (builder->state < SNAPBUILD_CONSISTENT)
	{
		/* ensure that only commits after this are getting replayed */
		if (builder->start_decoding_at <= lsn)
			builder->start_decoding_at = lsn + 1;

		/*
		 * We could avoid treating !SnapBuildTxnIsRunning transactions as
		 * timetravel ones, but we want to be able to export a snapshot when
		 * we reached consistency.
		 */
		forced_timetravel = true;
		elog(DEBUG1, "forced to assume catalog changes for xid %u because it was running too early", xid);
	}

	xmax = builder->xmax;

	if (NormalTransactionIdFollows(xid, xmax))
		xmax = xid;
	if (!forced_timetravel)
	{
		if (ReorderBufferXidHasCatalogChanges(builder->reorder, xid))
			forced_timetravel = true;
	}
	for (nxact = 0; nxact < nsubxacts; nxact++)
	{
		TransactionId subxid = subxacts[nxact];

		if (NormalTransactionIdFollows(subxid, xmax))
			xmax = subxid;

		if (!forced_timetravel)
		{
			if (ReorderBufferXidHasCatalogChanges(builder->reorder, subxid))
				forced_timetravel = true;
		}
	}

	builder->xmax = xmax;
	/* We use the commit record's LSN as the snapshot */
	builder->snapshotcsn = TxnGetOrGenerateStartTs(true);

	/* if there's any reason to build a historic snapshot, do so now */
	if (forced_timetravel)
	{
		/*
		 * Decrease the snapshot builder's refcount of the old snapshot, note
		 * that it still will be used if it has been handed out to the
		 * reorderbuffer earlier.
		 */
		if (builder->snapshot)
			SnapBuildSnapDecRefcount(builder->snapshot);

		builder->snapshot = SnapBuildBuildSnapshot(builder);

		/* we might need to execute invalidations, add snapshot */
		if (!ReorderBufferXidHasBaseSnapshot(builder->reorder, xid))
		{
			SnapBuildSnapIncRefcount(builder->snapshot);
			ReorderBufferSetBaseSnapshot(builder->reorder, xid, lsn,
										 builder->snapshot);
		}

		/* refcount of the snapshot builder for the new snapshot */
		SnapBuildSnapIncRefcount(builder->snapshot);

		/* add a new catalog snapshot to all currently running transactions */
		SnapBuildDistributeNewCatalogSnapshot(builder, lsn);
	}
}

#ifdef ENABLE_DISTRIBUTED_TRANSACTION
/*
 * We develop a Commit-Timestamp-Store(CTS) based logical replication
 * by using CTS as a MVCC snapshot to copy inital table and
 * decoding incremental updates in WAL from consistent point.
 *
 * The logical replication consists of two states:start and consistent state.
 * Before entering consistent state, we wait for the running xacts before
 * initial_xmin_horizon to complete, which also indicates the end of the running xacts
 * collected by the start state.
 *
 *
 *		   +-------------------------+
 *	  +----|		 START			 |-------------+
 *	       +-------------------------+
 *	  					|
 *	  					|
 *	  		   running_xacts #1	(RX1)
 *	  					|
 *	  					|
 *	  					v
 *	       +-------------------------+
 *	       |       CONSISTENT        |   running_xacts #1 finished
 *	       +-------------------------+
 *
 *
 * Then we generate a timestamp snapshot (HLC/TSO) to copy inital table and store
 * the snapshot in replication slot for later decoding.
 *
 * Decoding from consistent point only interests in the committed xacts which are not
 * visible to the built snapshot.
 *
 * The key to the correctness is that only transactions from running xacts 1 (RX1) span
 * cross the consistent point in WAL and they are all visisble to the built snapshot.
 * Starting from the consistent point, logical replication can decode entire transactions
 * in WAL only except for RX1.
 *
 * Author:  , 2020.11.07
 */

static bool
SnapBuildFindDistriSnapshot(SnapBuild *builder, XLogRecPtr lsn, xl_running_xacts *running)
{
	TransactionId xmax = running->nextXid;
	TransactionId xmin = running->oldestRunningXid;
	TransactionId xid;

	if (enable_distri_print)
		elog(LOG, "logical replication find snapshot xmin %u xmax %u xmin_horizon %u",
			 xmin, xmax, builder->initial_xmin_horizon);
	if (TransactionIdIsNormal(builder->initial_xmin_horizon) &&
		NormalTransactionIdPrecedes(running->oldestRunningXid,
									builder->initial_xmin_horizon))
		xmax = builder->initial_xmin_horizon;

	/*
	 * No running xacts when generating running xacts, enter into consistent
	 * state directly.
	 */
	if (xmin == xmax)
		return true;

	/*
	 * Phase 1: we should wait for running xacts between xmin and xmax to
	 * complete by checking transaction status in Commit Timestamp Store
	 * (CTS).
	 */
	for (;;)
	{
		xid = CTSLogGetNextActiveXid(xmin, xmax);

		if (TransactionIdIsCurrentTransactionId(xid))
			elog(ERROR, "waiting for ourselves");

		if (TransactionIdFollows(xid, xmax))
			elog(ERROR, "xmin %u exceeds xmax %u", xid, xmax);

		if (TransactionIdPrecedes(xid, xmax))
			XactLockTableWait(xid, NULL, NULL, XLTW_None);
		else
			break;

		xmin = xid;
	}

	return true;
}
#endif
/* -----------------------------------
 * Snapshot building functions dealing with xlog records
 * -----------------------------------
 */

/*
 * Process a running xacts record, and use its information to first build a
 * historic snapshot and later to release resources that aren't needed
 * anymore.
 */
void
SnapBuildProcessRunningXacts(SnapBuild *builder, XLogRecPtr lsn, xl_running_xacts *running)
{
	ReorderBufferTXN *txn;
	TransactionId xmin;

	/*
	 * If we're not consistent yet, inspect the record to see whether it
	 * allows to get closer to being consistent. If we are consistent, dump
	 * our snapshot so others or we, after a restart, can use it.
	 */
#ifdef ENABLE_DISTRIBUTED_TRANSACTION
	if (builder->state < SNAPBUILD_CONSISTENT)
	{
		/* returns false if there's no point in performing cleanup just yet */
		if (!SnapBuildFindDistriSnapshot(builder, lsn, running))
			return;
	}
#endif

	/*
	 * Update range of interesting xids based on the running xacts
	 * information.
	 */
	builder->xmin = running->oldestRunningXid;
	builder->xmax = running->nextXid;
	builder->snapshotcsn = TxnGetOrGenerateStartTs(true);

	elog(DEBUG3, "xmin: %u, xmax: %u",
		 builder->xmin, builder->xmax);
	Assert(lsn != InvalidXLogRecPtr);

	/*
	 * Increase shared memory limits, so vacuum can work on tuples we
	 * prevented from being pruned till now.
	 */
	xmin = ReorderBufferGetOldestXmin(builder->reorder);
	if (xmin == InvalidTransactionId)
		xmin = running->oldestRunningXid;
	elog(DEBUG3, "xmin: %u, xmax: %u, oldest running: %u, oldest xmin: %u",
		 builder->xmin, builder->xmax, running->oldestRunningXid, xmin);
	LogicalIncreaseXminForSlot(lsn, xmin);

	/*
	 * Also tell the slot where we can restart decoding from. We don't want to
	 * do that after every commit because changing that implies an fsync of
	 * the logical slot's state file, so we only do it every time we see a
	 * running xacts record.
	 *
	 * Do so by looking for the oldest in progress transaction (determined by
	 * the first LSN of any of its relevant records). Every transaction
	 * remembers the last location we stored the snapshot to disk before its
	 * beginning. That point is where we can restart from.
	 */

	if (builder->state < SNAPBUILD_CONSISTENT)
		builder->state = SNAPBUILD_CONSISTENT;

	txn = ReorderBufferGetOldestTXN(builder->reorder);

	/*
	 * oldest ongoing txn might have started when we didn't yet serialize
	 * anything because we hadn't reached a consistent state yet.
	 */
	if (txn != NULL && txn->restart_decoding_lsn != InvalidXLogRecPtr)
		LogicalIncreaseRestartDecodingForSlot(lsn, txn->restart_decoding_lsn);
}
