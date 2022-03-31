/*-------------------------------------------------------------------------
 *
 * poolcomm.h
 *
 *      Internal interfaces between different parts of the connection pool.
 *
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * IDENTIFICATION
 *        contrib/polarx/pool/poolcomm.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POOLCOMM_H
#define POOLCOMM_H

#include "lib/stringinfo.h"
#include "storage/s_lock.h"
#include "pgxc/connpool.h"
#include "pool/poolnodes.h"

#define POOL_MGR_PREFIX "PoolMgr: "

#define POOL_BUFFER_SIZE 1024
#define Socket(port) (port).fdsock

#define POOL_ERR_MSG_LEN 256

/* 
 * pooler error code 
 * corresponding with err_msg in poolmgr.c
 */
typedef enum
{
    POOL_ERR_NONE,
    POOL_ERR_GET_CONNECTIONS_POOLER_LOCKED,
    POOL_ERR_GET_CONNECTIONS_TASK_NOT_DONE,
    POOL_ERR_GET_CONNECTIONS_DISPATCH_FAILED,
    POOL_ERR_GET_CONNECTIONS_INVALID_ARGUMENT,
    POOL_ERR_GET_CONNECTIONS_OOM,
    POOL_ERR_GET_CONNECTIONS_CONNECTION_BAD,
    POOL_ERR_CANCEL_TASK_NOT_DONE,
    POOL_ERR_CANCEL_DISPATCH_FAILED,
    POOL_ERR_CANCEL_SEND_FAILED,
    NUMBER_POOL_ERRS
}   PoolErrorCode;

#define PoolErrIsValid(err)    ((bool) (err > POOL_ERR_NONE && err < NUMBER_POOL_ERRS))


typedef struct
{
    /* file descriptors */
    int            fdsock;
    /* receive buffer */
    int            RecvLength;
    int            RecvPointer;
    char        RecvBuffer[POOL_BUFFER_SIZE];
    /* send buffer */
    int            SendPointer;
    char        SendBuffer[POOL_BUFFER_SIZE];    /* error code */
    slock_t     lock;
    int         error_code;
    char        err_msg[POOL_ERR_MSG_LEN];
} PoolPort;

extern int    pool_listen(unsigned short port, const char *unixSocketName);
extern int    pool_connect(unsigned short port, const char *unixSocketName);
extern int    pool_getbyte(PoolPort *port);
extern int    pool_pollbyte(PoolPort *port);
extern int    pool_getmessage(PoolPort *port, StringInfo s, int maxlen);
extern int    pool_getbytes(PoolPort *port, char *s, size_t len);
extern int    pool_putmessage(PoolPort *port, char msgtype, const char *s, size_t len);
extern int    pool_putbytes(PoolPort *port, const char *s, size_t len);
extern int    pool_flush(PoolPort *port);
/*extern int    pool_sendfds(PoolPort *port, int *fds, int count);*/
extern int  pool_sendfds(PoolPort *port, int *fds, int count, char *errbuf, int32 buf_len);
extern int    pool_recvfds(PoolPort *port, int *fds, int count);
extern int    pool_sendres(PoolPort *port, int res, char *errbuf, int32 buf_len, bool need_log);
extern int    pool_recvres(PoolPort *port);
extern int    pool_sendpids(PoolPort *port, int *pids, int count, char *errbuf, int32 buf_len);
extern int    pool_recvpids(PoolPort *port, int **pids);
extern int    pool_sendres_with_command_id(PoolPort *port, int res, CommandId cmdID, char *errbuf, int32 buf_len, char *errmsg, bool need_log);
extern int  pool_recvres_with_commandID(PoolPort *port, CommandId *cmdID, const char *sql);




/**** functions provided by the pool manager ****/

/* Clean pool connections */
extern void PoolManagerCleanConnection(List *datanodelist, List *coordlist, char *dbname, char *username);

/* Check consistency of connection information cached in pooler with catalogs */
extern bool PoolManagerCheckConnectionInfo(void);

/* Reload connection data in pooler and drop all the existing connections of pooler */
extern void PoolManagerReloadConnectionInfo(void);

/* Send Abort signal to transactions being run */
extern int    PoolManagerAbortTransactions(char *dbname, char *username, int **proc_pids);

/* Lock/unlock pool manager */
extern void PoolManagerLock(bool is_lock);

/* Refresh connection data in pooler and drop connections of altered nodes in pooler */
extern int PoolManagerRefreshConnectionInfo(void);

#endif   /* POOLCOMM_H */
