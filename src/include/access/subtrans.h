/*
 * subtrans.h
 *
 * PostgreSQL subtransaction-log manager
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/subtrans.h
 */
#ifndef SUBTRANS_H
#define SUBTRANS_H

/* these are in csnlog.c now */
extern void SubTransSetParent(TransactionId xid, TransactionId parent);
extern TransactionId SubTransGetParent(TransactionId xid);
extern TransactionId SubTransGetTopmostTransaction(TransactionId xid);

#endif   /* SUBTRANS_H */
