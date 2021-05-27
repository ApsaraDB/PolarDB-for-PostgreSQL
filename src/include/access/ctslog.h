/*
 * ctslog.h based on csnlog.h
 *
 * Commit-Timestamp log.
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/ctslog.h
 */
#ifndef CTSLOG_H
#define CTSLOG_H

#include "access/xlog.h"

typedef struct xl_ctslog_truncate
{
	int			pageno;
	TransactionId oldestXact;
} xl_ctslog_truncate;


extern void CTSLogSetCommitTs(TransactionId xid, int nsubxids,
					 TransactionId *subxids, XLogRecPtr lsn, bool write_xlog, CommitTs cts);
extern CommitTs CTSLogAssignCommitTs(TransactionId xid, int nxids, TransactionId *xids, bool fromCoordinator);

extern CommitTs CTSLogGetCommitTs(TransactionId xid);
extern TransactionId CTSLogGetNextActiveXid(TransactionId start,
											TransactionId end);
extern XLogRecPtr CTSLogGetLSN(TransactionId xid);

extern Size CTSLOGShmemBuffers(void);
extern Size CTSLOGShmemSize(void);
extern void CTSLOGShmemInit(void);
extern void BootStrapCTSLOG(void);
extern void StartupCTSLOG(TransactionId oldestActiveXID);
extern void RecoverCTSLOG(TransactionId oldestActiveXID);
extern void TrimCTSLOG(void);
extern void ShutdownCTSLOG(void);
extern void CheckPointCTSLOG(void);
extern void ExtendCTSLOG(TransactionId newestXact);
extern void TruncateCTSLOG(TransactionId oldestXact);
/* XLOG stuff */
#define CTSLOG_ZEROPAGE		0x00
#define CTSLOG_TRUNCATE		0x10
#define CTSLOG_SETCSN		0x20

typedef struct xl_cts_set
{
	CommitTs cts;
	TransactionId mainxid;
	/* subxact Xids follow */
} xl_cts_set;

#define SizeOfCtsSet	(offsetof(xl_cts_set, mainxid) + \
							 sizeof(TransactionId))


extern void ctslog_redo(XLogReaderState *record);
extern void ctslog_desc(StringInfo buf, XLogReaderState *record);
extern const char *ctslog_identify(uint8 info);

#endif   /* CTSLOG_H */
