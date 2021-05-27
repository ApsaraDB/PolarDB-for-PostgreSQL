/*-------------------------------------------------------------------------
 *
 * snapshot.h
 *	  POSTGRES snapshot definition
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/snapshot.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SNAPSHOT_H
#define SNAPSHOT_H

#include "access/htup.h"
#include "access/xlogdefs.h"
#include "datatype/timestamp.h"
#include "lib/pairingheap.h"
#include "storage/buf.h"


typedef struct SnapshotData *Snapshot;

#define InvalidSnapshot		((Snapshot) NULL)

/*
 * We use SnapshotData structures to represent both "regular" (MVCC)
 * snapshots and "special" snapshots that have non-MVCC semantics.
 * The specific semantics of a snapshot are encoded by the "satisfies"
 * function.
 */
typedef bool (*SnapshotSatisfiesFunc) (HeapTuple htup,
									   Snapshot snapshot, Buffer buffer);

/*
 * Struct representing all kind of possible snapshots.
 *
 * There are several different kinds of snapshots:
 * * Normal MVCC snapshots
 * * MVCC snapshots taken during recovery (in Hot-Standby mode)
 * * Historic MVCC snapshots used during logical decoding
 * * snapshots passed to HeapTupleSatisfiesDirty()
 * * snapshots passed to HeapTupleSatisfiesNonVacuumable()
 * * snapshots used for SatisfiesAny, Toast, Self where no members are
 *	 accessed.
 *
 * TODO: It's probably a good idea to split this struct using a NodeTag
 * similar to how parser and executor nodes are handled, with one type for
 * each different kind of snapshot to avoid overloading the meaning of
 * individual fields.
 */
typedef struct SnapshotData
{
	SnapshotSatisfiesFunc satisfies;	/* tuple test function */

	/*
	 * The remaining fields are used only for MVCC snapshots, and are normally
	 * just zeroes in special snapshots.  (But xmin and xmax are used
	 * specially by HeapTupleSatisfiesDirty, and xmin is used specially by
	 * HeapTupleSatisfiesNonVacuumable.)
	 *
	 * An MVCC snapshot can see the effects of those XIDs that committed
	 * after snapshotlsn. xmin and xmax are stored as an optimization, to
	 * avoid checking the commit LSN for most tuples.
	 */
	TransactionId xmin;			/* all XID < xmin are visible to me */
	TransactionId xmax;			/* all XID >= xmax are invisible to me */

	/*
	 * This snapshot can see the effects of all transactions with CSN <=
	 * snapshotcsn.
	 */
	CommitSeqNo	snapshotcsn;

	bool		takenDuringRecovery;	/* recovery-shaped snapshot? */
	bool		copied;			/* false if it's a static snapshot */

	CommandId	curcid;			/* in my xact, CID < curcid are visible */

	/*
	 * An extra return value for HeapTupleSatisfiesDirty, not used in MVCC
	 * snapshots.
	 */
	uint32		speculativeToken;

	/*
	 * this_xip contains *all* xids assigned to the replayed transaction,
	 * including the toplevel xid. Used only in a historic MVCC snapshot,
	 * used in logical decoding.
	 */
	TransactionId *this_xip;
	uint32		this_xcnt;			/* # of xact ids in this_xip[] */

	/*
	 * Book-keeping information, used by the snapshot manager
	 */
	uint32		active_count;	/* refcount on ActiveSnapshot stack */
	uint32		regd_count;		/* refcount on RegisteredSnapshots */
	pairingheap_node ph_node;	/* link in the RegisteredSnapshots heap */

	TimestampTz whenTaken;		/* timestamp when snapshot was taken */
	XLogRecPtr	lsn;			/* position in the WAL stream when taken */
} SnapshotData;

/*
 * Result codes for HeapTupleSatisfiesUpdate.  This should really be in
 * tqual.h, but we want to avoid including that file elsewhere.
 */
typedef enum
{
	HeapTupleMayBeUpdated,
	HeapTupleInvisible,
	HeapTupleSelfUpdated,
	HeapTupleUpdated,
	HeapTupleBeingUpdated,
	HeapTupleWouldBlock			/* can be returned by heap_tuple_lock */
} HTSU_Result;

#endif							/* SNAPSHOT_H */
