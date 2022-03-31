/*-------------------------------------------------------------------------
 *
 * transam.h
 *	  postgres transaction access method support code
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/transam.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TRANSAM_H
#define TRANSAM_H

#include "access/xlogdefs.h"


/* ----------------
 *		Special transaction ID values
 *
 * BootstrapTransactionId is the XID for "bootstrap" operations, and
 * FrozenTransactionId is used for very old tuples.  Both should
 * always be considered valid.
 *
 * FirstNormalTransactionId is the first "normal" transaction id.
 * Note: if you need to change it, you must change pg_class.h as well.
 * ----------------
 */
#define InvalidTransactionId		((TransactionId) 0)
#define BootstrapTransactionId		((TransactionId) 1)
#define FrozenTransactionId			((TransactionId) 2)
#define FirstNormalTransactionId	((TransactionId) 3)
#define MaxTransactionId			((TransactionId) 0xFFFFFFFF)

/* ----------------
 *		transaction ID manipulation macros
 * ----------------
 */
#define TransactionIdIsValid(xid)		((xid) != InvalidTransactionId)
#define TransactionIdIsNormal(xid)		((xid) >= FirstNormalTransactionId)
#define TransactionIdEquals(id1, id2)	((id1) == (id2))
#define TransactionIdStore(xid, dest)	(*(dest) = (xid))
#define StoreInvalidTransactionId(dest) (*(dest) = InvalidTransactionId)

/* advance a transaction ID variable, handling wraparound correctly */
#define TransactionIdAdvance(dest)	\
	do { \
		(dest)++; \
		if ((dest) < FirstNormalTransactionId) \
			(dest) = FirstNormalTransactionId; \
	} while(0)

/* back up a transaction ID variable, handling wraparound correctly */
#define TransactionIdRetreat(dest)	\
	do { \
		(dest)--; \
	} while ((dest) < FirstNormalTransactionId)

/* compare two XIDs already known to be normal; this is a macro for speed */
#define NormalTransactionIdPrecedes(id1, id2) \
	(AssertMacro(TransactionIdIsNormal(id1) && TransactionIdIsNormal(id2)), \
	(int32) ((id1) - (id2)) < 0)

/* compare two XIDs already known to be normal; this is a macro for speed */
#define NormalTransactionIdFollows(id1, id2) \
	(AssertMacro(TransactionIdIsNormal(id1) && TransactionIdIsNormal(id2)), \
	(int32) ((id1) - (id2)) > 0)

/* ----------
 *		Object ID (OID) zero is InvalidOid.
 *
 *		OIDs 1-9999 are reserved for manual assignment (see the files
 *		in src/include/catalog/).
 *
 *		OIDS 10000-16383 are reserved for assignment during initdb
 *		using the OID generator.  (We start the generator at 10000.)
 *
 *		OIDs beginning at 16384 are assigned from the OID generator
 *		during normal multiuser operation.  (We force the generator up to
 *		16384 as soon as we are in normal operation.)
 *
 * The choices of 10000 and 16384 are completely arbitrary, and can be moved
 * if we run low on OIDs in either category.  Changing the macros below
 * should be sufficient to do this.
 *
 * NOTE: if the OID generator wraps around, we skip over OIDs 0-16383
 * and resume with 16384.  This minimizes the odds of OID conflict, by not
 * reassigning OIDs that might have been assigned during initdb.
 * ----------
 */
#define FirstBootstrapObjectId	10000
#define FirstNormalObjectId		16384


/* ----------------
 *		extern declarations
 * ----------------
 */

/* in transam/xact.c */
extern bool TransactionStartedDuringRecovery(void);

/*
 * prototypes for functions in transam/transam.c
 */
extern bool TransactionIdDidCommit(TransactionId transactionId);
extern bool TransactionIdDidAbort(TransactionId transactionId);


#define COMMITSEQNO_INPROGRESS	UINT64CONST(0x0)
#define COMMITSEQNO_ABORTED		UINT64CONST(0x1)
/*
 * COMMITSEQNO_COMMITING is an intermediate state that is used to set CSN
 * atomically for a top level transaction and its subtransactions.
 * High-level users should not see this value, see TransactionIdGetCommitSeqNo().
 */
#define COMMITSEQNO_COMMITTING	UINT64CONST(0x2)
#define COMMITSEQNO_FROZEN		UINT64CONST(0x3)
#define COMMITSEQNO_FIRST_NORMAL ((unsigned long)1000000 << 16)

#define CSN_SUBTRANS_BIT		(UINT64CONST(1)<<63)
#define CSN_PREPARE_BIT			(UINT64CONST(1)<<62)

#define COMMITSEQNO_IS_SUBTRANS(csn) ((csn) & CSN_SUBTRANS_BIT)
#define COMMITSEQNO_IS_PREPARED(csn) ((csn) & CSN_PREPARE_BIT)
#define MASK_PREPARE_BIT(csn) ((csn) | CSN_PREPARE_BIT)
#define UNMASK_PREPARE_BIT(csn) ((csn) & (~CSN_PREPARE_BIT))

#define COMMITSEQNO_IS_INPROGRESS(csn) ((csn) == COMMITSEQNO_INPROGRESS)
#define COMMITSEQNO_IS_ABORTED(csn) ((csn) == COMMITSEQNO_ABORTED)
#define COMMITSEQNO_IS_FROZEN(csn) ((csn) == COMMITSEQNO_FROZEN)
#define COMMITSEQNO_IS_COMMITTING(csn) ((csn) == COMMITSEQNO_COMMITTING)
#define COMMITSEQNO_IS_COMMITTED(csn) ((csn) >= COMMITSEQNO_FROZEN && !COMMITSEQNO_IS_SUBTRANS(csn) && !COMMITSEQNO_IS_PREPARED(csn))
#define COMMITSEQNO_IS_NORMAL(csn) ((csn) >= COMMITSEQNO_FIRST_NORMAL && !COMMITSEQNO_IS_SUBTRANS(csn) && !COMMITSEQNO_IS_PREPARED(csn))


typedef enum
{
	XID_COMMITTED,
	XID_ABORTED,
	XID_INPROGRESS
} TransactionIdStatus;


extern CommitSeqNo TransactionIdGetCommitSeqNo(TransactionId xid);
extern TransactionIdStatus TransactionIdGetStatus(TransactionId transactionId);
extern void TransactionIdAbort(TransactionId transactionId);
extern void TransactionIdCommitTree(TransactionId xid, int nxids, TransactionId *xids);
extern void TransactionIdAsyncCommitTree(TransactionId xid, int nxids, TransactionId *xids, XLogRecPtr lsn);
extern void TransactionIdAbortTree(TransactionId xid, int nxids, TransactionId *xids);
extern bool TransactionIdPrecedes(TransactionId id1, TransactionId id2);
extern bool TransactionIdPrecedesOrEquals(TransactionId id1, TransactionId id2);
extern bool TransactionIdFollows(TransactionId id1, TransactionId id2);
extern bool TransactionIdFollowsOrEquals(TransactionId id1, TransactionId id2);
extern TransactionId TransactionIdLatest(TransactionId mainxid,
					int nxids, const TransactionId *xids);
extern XLogRecPtr TransactionIdGetCommitLSN(TransactionId xid);

/* in transam/varsup.c */
extern TransactionId GetNewTransactionId(bool isSubXact);
extern TransactionId ReadNewTransactionId(void);
extern void SetTransactionIdLimit(TransactionId oldest_datfrozenxid,
					  Oid oldest_datoid);
extern void AdvanceOldestClogXid(TransactionId oldest_datfrozenxid);
extern bool ForceTransactionIdLimitUpdate(void);
extern Oid	GetNewObjectId(void);

#ifdef POLARDB_X
extern void StorePartNodes(const char *partNodes);
extern void StoreGid(const char *gid);
#endif

#endif							/* TRAMSAM_H */
