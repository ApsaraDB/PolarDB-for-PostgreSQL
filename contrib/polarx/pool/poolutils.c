/*-------------------------------------------------------------------------
 *
 * poolutils.c
 *
 * Utilities for Postgres-XC pooler
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * IDENTIFICATION
 *        contrib/polarx/pool/poolutils.h
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "polarx.h"
#include "miscadmin.h"
#include "libpq/pqsignal.h"

#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "poolcomm.h"
#include "pgxc/locator.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxcnode.h"
#include "access/xact.h"
#include "commands/dbcommands.h"
#include "commands/prepare.h"
#include "storage/ipc.h"
#include "storage/procarray.h"
#include "storage/latch.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

#define TIMEOUT_CLEAN_LOOP 10 /* Wait 10s for all the transactions to shutdown */

PG_FUNCTION_INFO_V1(pgxc_pool_check);

/*
 * pgxc_pool_check
 *
 * Check if Pooler information in catalog is consistent
 * with information cached.
 */
Datum
pgxc_pool_check(PG_FUNCTION_ARGS)
{
    if (!superuser())
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 (errmsg("must be superuser to manage pooler"))));

    /* A Datanode has no pooler active, so do not bother about that */
    if (IS_PGXC_DATANODE)
        PG_RETURN_BOOL(true);

    /* Simply check with pooler */
    PG_RETURN_BOOL(PoolManagerCheckConnectionInfo());
}


/*
 * DropDBCleanConnection
 *
 * Clean Connection for given database before dropping it
 * FORCE is not used here
 */
void
DropDBCleanConnection(char *dbname)
{
    List    *co_list = GetAllCoordNodes();
    List    *dn_list = GetAllDataNodes();

    /* Check permissions for this database */
    if (!pg_database_ownercheck(get_database_oid(dbname, true), GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_DATABASE,
                       dbname);

    PoolManagerCleanConnection(dn_list, co_list, dbname, NULL);

    /* Clean up memory */
    if (co_list)
        list_free(co_list);
    if (dn_list)
        list_free(dn_list);
}

/*
 * HandlePoolerReload
 *
 * This is called when PROCSIG_PGXCPOOL_RELOAD is activated.
 * Abort the current transaction if any, then reconnect to pooler.
 * and reinitialize session connection information.
 */
void
HandlePoolerReload(void)
{
    if (proc_exit_inprogress)
        return;

    if (PoolerReloadHoldoffCount)
    {
        return;
    }
    
  //  if (false == IsTransactionIdle())
   // {
    //    return;
   // }

    /* Request query cancel, when convenient */
    //InterruptPending = true;
    //QueryCancelPending = true;

    /* Disconnect from the pooler to get new connection infos next time */
    PoolManagerDisconnect();

    /* Prevent using of cached connections to remote nodes */
    //RequestInvalidateRemoteHandles();
    
}

/*
 * HandlePoolerRefresh
 *
 * This is called when PROCSIG_PGXCPOOL_REFRESH is activated.
 * Reconcile local backend connection info with the one in
 * shared memory
 */
void
HandlePoolerRefresh(void)
{
    if (proc_exit_inprogress)
        return;

    InterruptPending = true;

    RequestRefreshRemoteHandles();

    /* make sure the event is processed in due course */
    SetLatch(MyLatch);
}
