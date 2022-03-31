/*-------------------------------------------------------------------------
 *
 * connpool.h
 *
 *        Public Interface for Postgres-XC Connection Pool
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * contrib/polarx/include/pgxc/connpool.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef CONNPOOL_H
#define CONNPOOL_H

#include "nodes/parsenodes.h"
#include "pgxcnode.h"
#include "utils/guc.h"


/*
 * List of flags related to pooler connection clean up when disconnecting
 * a session or relaeasing handles.
 * When Local SET commands (POOL_CMD_LOCAL_SET) are used, local parameter
 * string is cleaned by the node commit itself.
 * When global SET commands (POOL_CMD_GLOBAL_SET) are used, "RESET ALL"
 * command is sent down to activated nodes to at session end. At the end
 * of a transaction, connections using global SET commands are not sent
 * back to pool.
 * When temporary object commands are used (POOL_CMD_TEMP), "DISCARD ALL"
 * query is sent down to nodes whose connection is activated at the end of
 * a session.
 * At the end of a transaction, a session using either temporary objects
 * or global session parameters has its connections not sent back to pool.
 *
 * Local parameters are used to change within current transaction block.
 * They are sent to remote nodes invloved in the transaction after sending
 * BEGIN TRANSACTION using a special firing protocol.
 * They cannot be sent when connections are obtained, making them having no
 * effect as BEGIN is sent by backend after connections are obtained and
 * obtention confirmation has been sent back to backend.
 * SET CONSTRAINT, SET LOCAL commands are in this category.
 *
 * Global parmeters are used to change the behavior of current session.
 * They are sent to the nodes when the connections are obtained.
 * SET GLOBAL, general SET commands are in this category.
 */
typedef enum
{
    POOL_CMD_TEMP,        /* Temporary object flag */
    POOL_CMD_LOCAL_SET,    /* Local SET flag, current transaction block only */
    POOL_CMD_GLOBAL_SET    /* Global SET flag */
} PoolCommandType;

typedef enum
{
    SIGNAL_SIGINT   = 0,
    SIGNAL_MAX  = 1
}   SignalType;


/** GUC **/
extern int    MinPoolSize;
extern int    MaxPoolSize;
extern int    InitPoolSize;
extern int    MinFreeSize;

extern int    PoolerPort;
extern int    PoolConnKeepAlive;
extern int    PoolMaintenanceTimeout;
extern bool PersistentConnections;

extern char *g_PoolerWarmBufferInfo;
extern char *g_unpooled_database;
extern char *g_unpooled_user;

extern int    PoolSizeCheckGap; 
extern int    PoolConnMaxLifetime; 
extern int    PoolMaxMemoryLimit;
extern int    PoolConnectTimeOut;
extern int  PoolScaleFactor;
extern int  PoolDNSetTimeout;
extern int  PoolCheckSlotTimeout;
extern int  PoolPrintStatTimeout;
extern bool PoolConnectDebugPrint;




/* Status inquiry functions */
extern void PGXCPoolerProcessIam(void);
extern bool IsPGXCPoolerProcess(void);

/* Initialize internal structures */
extern void  PoolManagerInit(Datum arg);

/* Destroy internal structures */
extern int    PoolManagerDestroy(void);

/**
 * Called by a backend, check whether it already
 * connected to the connection pool
 */
extern bool IsConnPoolConnected(void);

/*
 * Save a SET command in Pooler.
 * This command is run on existent agent connections
 * and stored in pooler agent to be replayed when new connections
 * are requested.
 */
#define POOL_SET_COMMAND_ALL  -1
#define POOL_SET_COMMAND_NONE 0


extern int PoolManagerSetCommand(PGXCNodeHandle **connections, int32 count, PoolCommandType command_type, 
                                  const char *set_command);

/* Send commands to alter the behavior of current transaction */
extern int PoolManagerSendLocalCommand(int dn_count, int* dn_list, int co_count, int* co_list);


/* Do pool health check activity */
extern void PoolAsyncPingNodes(void);
extern void PoolPingNodes(void);
extern void PoolPingNodeRecheck(Oid nodeoid);
extern bool check_persistent_connections(bool *newval, void **extra,
        GucSource source);

extern int PoolManagerClosePooledConnections(const char *dbname, const char *username);


/* Results for pooler release pooled connection */
#define POOL_CONN_RELEASE_SUCCESS            0
#define POOL_CONN_RELEASE_FAILED            1

extern PGDLLIMPORT volatile int PoolerReloadHoldoffCount;

#define HOLD_POOLER_RELOAD()  (PoolerReloadHoldoffCount++)

#define RESUME_POOLER_RELOAD() \
do { \
    Assert(PoolerReloadHoldoffCount > 0); \
    if(PoolerReloadHoldoffCount > 0)    \
        PoolerReloadHoldoffCount--; \
} while(0)


extern void DropDBCleanConnection(char *dbname);

/* Handle pooler connection reload/refresh when signaled by SIGUSR1 */
extern void HandlePoolerReload(void);
extern void HandlePoolerRefresh(void);
extern void InitializePoolerShmemStruct(void);
extern void StartupPooler(void);
extern bool IsPoolerWokerService(void);
#endif
