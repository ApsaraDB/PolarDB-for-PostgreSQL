/*
 * clog.h
 *
 * PostgreSQL transaction-commit-log manager
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/clog.h
 */
#ifndef CLOG_H
#define CLOG_H

#include "access/xlogreader.h"
#include "lib/stringinfo.h"

/*
 * Possible transaction statuses --- note that all-zeroes is the initial
 * state.
 */
typedef int CLogXidStatus;

#define CLOG_XID_STATUS_IN_PROGRESS		0x00
#define CLOG_XID_STATUS_COMMITTED		0x01
#define CLOG_XID_STATUS_ABORTED			0x02

/*
 * A "subcommitted" transaction is a committed subtransaction whose parent
 * hasn't committed or aborted yet. We don't create these anymore, but accept
 * them in existing clog, if we've been pg_upgraded from an older version.
 */
#define CLOG_XID_STATUS_SUB_COMMITTED	0x03

typedef struct xl_clog_truncate
{
	int			pageno;
	TransactionId oldestXact;
	Oid			oldestXactDb;
} xl_clog_truncate;

extern void CLogSetTreeStatus(TransactionId xid, int nsubxids,
				  TransactionId *subxids, CLogXidStatus status, XLogRecPtr lsn);
extern CLogXidStatus CLogGetStatus(TransactionId xid, XLogRecPtr *lsn);
extern XLogRecPtr
			CLogGetLSN(TransactionId xid);

extern Size CLOGShmemBuffers(void);
extern Size CLOGShmemSize(void);
extern void CLOGShmemInit(void);
extern void BootStrapCLOG(void);
extern void StartupCLOG(void);
extern void TrimCLOG(void);
extern void ShutdownCLOG(void);
extern void CheckPointCLOG(void);
extern void ExtendCLOG(TransactionId newestXact);
extern void TruncateCLOG(TransactionId oldestXact, Oid oldestxid_datoid);

/* XLOG stuff */
#define CLOG_ZEROPAGE		0x00
#define CLOG_TRUNCATE		0x10

extern void clog_redo(XLogReaderState *record);
extern void clog_desc(StringInfo buf, XLogReaderState *record);
extern const char *clog_identify(uint8 info);

#endif							/* CLOG_H */
