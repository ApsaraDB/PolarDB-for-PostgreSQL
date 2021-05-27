/*
 * csnlog.h
 *
 * Commit-Sequence-Number log.
 *
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/clog.h
 */
#ifndef CSNLOG_H
#define CSNLOG_H

#include "access/xlog.h"

typedef struct xl_csnlog_truncate
{
	int			pageno;
	TransactionId oldestXact;
} xl_csnlog_truncate;


extern void CSNLogSetCommitSeqNo(TransactionId xid, int nsubxids,
					 TransactionId *subxids, XLogRecPtr lsn, bool write_xlog, CommitSeqNo csn);
#ifdef ENABLE_DISTRIBUTED_TRANSACTION
extern CommitSeqNo CSNLogAssignCommitSeqNo(TransactionId xid, int nxids, TransactionId *xids, bool fromCoordinator);
extern int delay_before_set_committing_status;
extern int delay_after_set_committing_status;
#endif

extern CommitSeqNo CSNLogGetCommitSeqNo(TransactionId xid);
extern TransactionId CSNLogGetNextActiveXid(TransactionId start,
											TransactionId end);

extern Size CSNLOGShmemBuffers(void);
extern Size CSNLOGShmemSize(void);
extern void CSNLOGShmemInit(void);
extern void BootStrapCSNLOG(void);
extern void StartupCSNLOG(TransactionId oldestActiveXID);
extern void TrimCSNLOG(void);
extern void ShutdownCSNLOG(void);
extern void CheckPointCSNLOG(void);
extern void ExtendCSNLOG(TransactionId newestXact);
extern void TruncateCSNLOG(TransactionId oldestXact);
/* XLOG stuff */
#define CSNLOG_ZEROPAGE		0x00
#define CSNLOG_TRUNCATE		0x10
#define CSNLOG_SETCSN		0x20

typedef struct xl_csn_set
{
	CommitSeqNo csn;
	TransactionId mainxid;
	/* subxact Xids follow */
} xl_csn_set;

#define SizeOfCsnSet	(offsetof(xl_csn_set, mainxid) + \
							 sizeof(TransactionId))


extern void csnlog_redo(XLogReaderState *record);
extern void csnlog_desc(StringInfo buf, XLogReaderState *record);
extern const char *csnlog_identify(uint8 info);

#endif   /* CSNLOG_H */
