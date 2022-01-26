/*-------------------------------------------------------------------------
 *
 * gtm_snap.c
 *    Snapshot handling on GTM
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *      $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#include "gtm/assert.h"
#include "gtm/elog.h"
#include "gtm/gtm.h"
#include "gtm/gtm_client.h"
#include "gtm/gtm_standby.h"
#include "gtm/stringinfo.h"
#include "gtm/gtm_xlog.h"
#include "gtm/libpq.h"
#include "gtm/libpq-int.h"
#include "gtm/pqformat.h"

/*
 * Get snapshot for the given transactions. If this is the first call in the
 * transaction, a fresh snapshot is taken and returned back. For a serializable
 * transaction, repeated calls to the function will return the same snapshot.
 * For a read-committed transaction, fresh snapshot is taken every time and
 * returned to the caller.
 *
 * The returned snapshot includes xmin (lowest still-running xact ID),
 * xmax (highest completed xact ID + 1), and a list of running xact IDs
 * in the range xmin <= xid < xmax.  It is used as follows:
 *        All xact IDs < xmin are considered finished.
 *        All xact IDs >= xmax are considered still running.
 *        For an xact ID xmin <= xid < xmax, consult list to see whether
 *        it is considered running or not.
 * This ensures that the set of transactions seen as "running" by the
 * current xact will not change after it takes the snapshot.
 *
 * All running top-level XIDs are included in the snapshot.
 *
 * We also update the following global variables:
 *        RecentGlobalXmin: the global xmin (oldest TransactionXmin across all
 *            running transactions
 *
 * Note: this function should probably not be called with an argument that's
 * not statically allocated (see xip allocation below).
 */
GTM_Snapshot
GTM_GetTransactionSnapshot(GTM_TransactionHandle handle[], int txn_count, int *status)
{// #lizard forgives
    GlobalTransactionId xmin;
    GlobalTransactionId xmax;
    GlobalTransactionId globalxmin;
    int            count = 0;
    gtm_ListCell *elem = NULL;
    int ii;

    /*
     * Instead of allocating memory for a snapshot, we use the snapshot of the
     * first transaction in the given array. The same snapshot will later be
     * copied to other transaction info structures.
     */
    GTM_TransactionInfo *mygtm_txninfo = NULL;
    GTM_Snapshot snapshot = NULL;

    memset(status, 0, sizeof (int) * txn_count);

    for (ii = 0; ii < txn_count; ii++)
    {
        /*
         * Even if the request does not contain a valid GXID, we still send
         * down a snapshot, but mark the status field acoordingly
         */
        if (handle[ii] != InvalidTransactionHandle)
            mygtm_txninfo = GTM_HandleToTransactionInfo(handle[ii]);
        else
            status[ii] = STATUS_NOT_FOUND;

        /*
         * If the transaction does not exist, just mark the status field with
         * a STATUS_ERROR code
         */
        if ((mygtm_txninfo != NULL) && (snapshot == NULL))
            snapshot = &mygtm_txninfo->gti_current_snapshot;
    }

    /*
     * If no valid transaction exists in the array, we record the snapshot in a
     * thread-specific structure. This allows us to avoid repeated
     * allocation/freeing of the structure.
     *
     * Note that we must use a thread-specific variable and not a global
     * variable because a concurrent thread might compute a new snapshot and
     * overwrite the snapshot information while we are still sending this copy
     * to the client. Using a thread-specific storage avoids that problem.
     */
    if (snapshot == NULL)
        snapshot = &GetMyThreadInfo->thr_snapshot;

    Assert(snapshot != NULL);

    /*
     * This can only happen when using a snapshot from GTMTransactions, as the
     * thread-specific sn_xip array is allocated statically as part of GTM_ThreadInfo.
     */
    if (snapshot->sn_xip == NULL)
    {
        /*
         * First call for this snapshot
         */
        snapshot->sn_xip = (GlobalTransactionId *)
            palloc(GTM_MAX_GLOBAL_TRANSACTIONS * sizeof(GlobalTransactionId));
        if (snapshot->sn_xip == NULL)
            ereport(ERROR,
                    (ENOMEM,
                     errmsg("out of memory")));
    }

    /*
     * It is sufficient to get shared lock on ProcArrayLock, even if we are
     * going to set MyProc->xmin.
     */
    GTM_RWLockAcquire(&GTMTransactions.gt_TransArrayLock, GTM_LOCKMODE_READ);

    /* xmax is always latestCompletedXid + 1 */
    xmax = GTMTransactions.gt_latestCompletedXid;
    Assert(GlobalTransactionIdIsNormal(xmax));
    GlobalTransactionIdAdvance(xmax);

    /* initialize xmin calculation with xmax */
    globalxmin = xmin = xmax;

    /*
     * Spin over transaction list checking xid, xmin, and subxids.  The goal is to
     * gather all active xids and find the lowest xmin
     */
    gtm_foreach(elem, GTMTransactions.gt_open_transactions)
    {
        volatile GTM_TransactionInfo *gtm_txninfo = (GTM_TransactionInfo *)gtm_lfirst(elem);
        GlobalTransactionId xid;

        /* Don't take into account LAZY VACUUMs */
        if (gtm_txninfo->gti_vacuum)
            continue;

        /* Update globalxmin to be the smallest valid xmin */
        xid = gtm_txninfo->gti_xmin;        /* fetch just once */
        if (GlobalTransactionIdIsNormal(xid) &&
            GlobalTransactionIdPrecedes(xid, globalxmin))
            globalxmin = xid;

        /* Fetch xid just once - see GetNewTransactionId */
        xid = gtm_txninfo->gti_gxid;

        /*
         * If the transaction has been assigned an xid < xmax we add it to the
         * snapshot, and update xmin if necessary.    There's no need to store
         * XIDs >= xmax, since we'll treat them as running anyway.  We don't
         * bother to examine their subxids either.
         *
         * We don't include our own XID (if any) in the snapshot, but we must
         * include it into xmin.
         */
        if (GlobalTransactionIdIsNormal(xid))
        {
            /*
             * Unlike Postgres, we include the GXID of the current transaction
             * as well in the snapshot. This is necessary because the same
             * snapshot is shared by multiple backends through GTM proxy and
             * the GXID will vary for each backend.
             *
             * XXX We should confirm that this does not have any adverse effect
             * on the MVCC visibility and check if any changes are related to
             * the MVCC checks because of the change
             */
            if (GlobalTransactionIdFollowsOrEquals(xid, xmax))
                continue;
            if (GlobalTransactionIdPrecedes(xid, xmin))
                xmin = xid;
            snapshot->sn_xip[count++] = xid;
        }
    }

    /*
     * Update globalxmin to include actual process xids.  This is a slightly
     * different way of computing it than GetOldestXmin uses, but should give
     * the same result.
     */
    if (GlobalTransactionIdPrecedes(xmin, globalxmin))
        globalxmin = xmin;


    snapshot->sn_xmin = xmin;
    snapshot->sn_xmax = xmax;
    snapshot->sn_xcnt = count;

    /*
     * Now, before the proc array lock is released, set the xmin in the txninfo
     * structures of all the transactions.
     */
    for (ii = 0; ii < txn_count; ii++)
    {
        GTM_Snapshot mysnap = NULL;

        /*
         * We have already gone through all the transaction handles above and
         * marked the invalid handles with STATUS_ERROR
         */
        if ((status[ii] == STATUS_ERROR) || (status[ii] == STATUS_NOT_FOUND))
            continue;

        mygtm_txninfo = GTM_HandleToTransactionInfo(handle[ii]);
        mysnap = &mygtm_txninfo->gti_current_snapshot;

        if (GTM_IsTransSerializable(mygtm_txninfo))
        {
            if ((mygtm_txninfo->gti_snapshot_set) && (txn_count > 1))
                elog(ERROR, "Grouped snapshot can only include first snapshot in Serializable transaction");

            if (!mygtm_txninfo->gti_snapshot_set)
            {
                /*
                 * For the first transaction in the array, the snapshot is
                 * already set.
                 */
                if (snapshot != mysnap)
                {
                    if (mysnap->sn_xip == NULL)
                    {
                        /*
                         * First call for this snapshot
                         */
                        mysnap->sn_xip = (GlobalTransactionId *)
                            palloc(GTM_MAX_GLOBAL_TRANSACTIONS * sizeof(GlobalTransactionId));
                        if (mysnap->sn_xip == NULL)
                            ereport(ERROR, (ENOMEM, errmsg("out of memory")));
                    }
                    mysnap->sn_xmin = snapshot->sn_xmin;
                    mysnap->sn_xmax = snapshot->sn_xmax;
                    mysnap->sn_xcnt = snapshot->sn_xcnt;
                    memcpy(mysnap->sn_xip, snapshot->sn_xip,
                            sizeof (GlobalTransactionId) * snapshot->sn_xcnt);
                }
                mygtm_txninfo->gti_snapshot_set = true;
            }
        }
        else if (snapshot != mysnap)
        {
            if (mysnap->sn_xip == NULL)
            {
                /*
                 * First call for this snapshot
                 */
                mysnap->sn_xip = (GlobalTransactionId *)
                    palloc(GTM_MAX_GLOBAL_TRANSACTIONS * sizeof(GlobalTransactionId));
                if (mysnap->sn_xip == NULL)
                    ereport(ERROR, (ENOMEM, errmsg("out of memory")));
            }
            mysnap->sn_xmin = snapshot->sn_xmin;
            mysnap->sn_xmax = snapshot->sn_xmax;
            mysnap->sn_xcnt = snapshot->sn_xcnt;
            memcpy(mysnap->sn_xip, snapshot->sn_xip,
                    sizeof (GlobalTransactionId) * snapshot->sn_xcnt);
        }

        if ((mygtm_txninfo != NULL) &&
            (!GlobalTransactionIdIsValid(mygtm_txninfo->gti_xmin)))
            mygtm_txninfo->gti_xmin = xmin;
    }

    GTM_RWLockRelease(&GTMTransactions.gt_TransArrayLock);

    elog(DEBUG1, "GTM_GetTransactionSnapshot: (%u:%u:%u)",
            snapshot->sn_xmin, snapshot->sn_xmax,
            snapshot->sn_xcnt);
    return snapshot;
}

/*
 * Process MSG_SNAPSHOT_GET command
 */
void
ProcessGetSnapshotCommand(Port *myport, StringInfo message, bool get_gxid)
{
    StringInfoData buf;
    GTM_TransactionHandle txn;
    GlobalTransactionId gxid;
    GTM_Snapshot snapshot;
    MemoryContext oldContext;
    int status;
    int txn_count;
    const char *data = NULL;

    txn_count = pq_getmsgint(message, sizeof (int));
    Assert(txn_count == 1);

    data = pq_getmsgbytes(message, sizeof (gxid));
    if (data == NULL)
        ereport(ERROR,
                (EPROTO,
                 errmsg("Message does not contain valid GXID")));
    memcpy(&gxid, data, sizeof(gxid));
    elog(INFO, "Received transaction ID %d for snapshot obtention", gxid);
    txn = GTM_GXIDToHandle(gxid);

    pq_getmsgend(message);

    if (get_gxid)
    {
        gxid = GTM_GetGlobalTransactionId(txn);
        if (gxid == InvalidGlobalTransactionId)
            ereport(ERROR,
                    (EINVAL,
                     errmsg("Failed to get a new transaction id")));
    }

    oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

    /*
     * Get a fresh snapshot
     */
    if ((snapshot = GTM_GetTransactionSnapshot(&txn, 1, &status)) == NULL)
        ereport(ERROR,
                (EINVAL,
                 errmsg("Failed to get a snapshot")));

    MemoryContextSwitchTo(oldContext);
    
    BeforeReplyToClientXLogTrigger();
    
    pq_beginmessage(&buf, 'S');
    pq_sendint(&buf, get_gxid ? SNAPSHOT_GXID_GET_RESULT : SNAPSHOT_GET_RESULT, 4);
    if (myport->remote_type == GTM_NODE_GTM_PROXY)
    {
        GTM_ProxyMsgHeader proxyhdr;
        proxyhdr.ph_conid = myport->conn_id;
        pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
    }
    pq_sendbytes(&buf, (char *)&gxid, sizeof (GlobalTransactionId));
    pq_sendbytes(&buf, (char *)&txn_count, sizeof(txn_count));
    pq_sendbytes(&buf, (char *)&status, sizeof(int) * txn_count);
    pq_sendbytes(&buf, (char *)&snapshot->sn_xmin, sizeof (GlobalTransactionId));
    pq_sendbytes(&buf, (char *)&snapshot->sn_xmax, sizeof (GlobalTransactionId));
    pq_sendint(&buf, snapshot->sn_xcnt, sizeof (int));
    pq_sendbytes(&buf, (char *)snapshot->sn_xip,
                 sizeof(GlobalTransactionId) * snapshot->sn_xcnt);
    pq_endmessage(myport, &buf);

    if (myport->remote_type != GTM_NODE_GTM_PROXY)
        pq_flush(myport);

    return;
}

/*
 * Process MSG_SNAPSHOT_GET_MULTI command
 */
void
ProcessGetSnapshotCommandMulti(Port *myport, StringInfo message)
{
    StringInfoData buf;
    GTM_TransactionHandle txn[GTM_MAX_GLOBAL_TRANSACTIONS];
    GlobalTransactionId gxid[GTM_MAX_GLOBAL_TRANSACTIONS];
    GTM_Snapshot snapshot;
    MemoryContext oldContext;
    int txn_count;
    int ii;
    int status[GTM_MAX_GLOBAL_TRANSACTIONS];

    txn_count = pq_getmsgint(message, sizeof (int));

    for (ii = 0; ii < txn_count; ii++)
    {
        const char *data = pq_getmsgbytes(message, sizeof (gxid[ii]));
        if (data == NULL)
            ereport(ERROR,
                    (EPROTO,
                     errmsg("Message does not contain valid GXID")));
        memcpy(&gxid[ii], data, sizeof (gxid[ii]));
        txn[ii] = GTM_GXIDToHandle(gxid[ii]);
    }

    pq_getmsgend(message);

    oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

    /*
     * Get a fresh snapshot
     */
    if ((snapshot = GTM_GetTransactionSnapshot(txn, txn_count, status)) == NULL)
        ereport(ERROR,
                (EINVAL,
                 errmsg("Failed to get a snapshot")));

    MemoryContextSwitchTo(oldContext);
    
    BeforeReplyToClientXLogTrigger();
    
    pq_beginmessage(&buf, 'S');
    pq_sendint(&buf, SNAPSHOT_GET_MULTI_RESULT, 4);
    if (myport->remote_type == GTM_NODE_GTM_PROXY)
    {
        GTM_ProxyMsgHeader proxyhdr;
        proxyhdr.ph_conid = myport->conn_id;
        pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
    }
    pq_sendbytes(&buf, (char *)&txn_count, sizeof(txn_count));
    pq_sendbytes(&buf, (char *)status, sizeof(int) * txn_count);
    pq_sendbytes(&buf, (char *)&snapshot->sn_xmin, sizeof (GlobalTransactionId));
    pq_sendbytes(&buf, (char *)&snapshot->sn_xmax, sizeof (GlobalTransactionId));
    pq_sendint(&buf, snapshot->sn_xcnt, sizeof (int));
    pq_sendbytes(&buf, (char *)snapshot->sn_xip,
                 sizeof(GlobalTransactionId) * snapshot->sn_xcnt);
    pq_endmessage(myport, &buf);

    if (myport->remote_type != GTM_NODE_GTM_PROXY)
        pq_flush(myport);



    return;
}

/*
 * Free the snapshot data. The snapshot itself is not freed though
 */
void
GTM_FreeSnapshotData(GTM_Snapshot snapshot)
{
    if (snapshot == NULL)
        return;

    if (snapshot->sn_xip != NULL)
    {
        Assert(snapshot->sn_xcnt);
        pfree(snapshot->sn_xip);
        snapshot->sn_xip = NULL;
    }
}
