/*-------------------------------------------------------------------------
 *
 * gtm_msg.h
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#ifndef GTM_MSG_H
#define GTM_MSG_H

/*
 * The following enum symbols are also used in message_name_tab structure
 * in gtm_utils.c.   Modification of the following enum should reflect
 * changes to message_name_tab structure as well.
 */
typedef enum GTM_MessageType
{
    MSG_TYPE_INVALID,
    MSG_SYNC_STANDBY,            /* Message to sync woth GTM-Standby */
    MSG_NODE_REGISTER,            /* Register a PGXC Node with GTM */
    MSG_BKUP_NODE_REGISTER,        /* Backup of MSG_NODE_REGISTER */
    MSG_NODE_UNREGISTER,        /* Unregister a PGXC Node with GTM */
    MSG_BKUP_NODE_UNREGISTER,    /* Backup of MSG_NODE_UNREGISTER */
    MSG_REGISTER_SESSION,        /* Register distributed session with GTM */
    MSG_REPORT_XMIN,            /* Report RecentGlobalXmin to GTM */
    MSG_BKUP_REPORT_XMIN,
    MSG_NODE_LIST,                /* Get node list */
    MSG_NODE_BEGIN_REPLICATION_INIT,
    MSG_NODE_END_REPLICATION_INIT,
    MSG_BEGIN_BACKUP,            /* Start backup by Standby */
    MSG_END_BACKUP,                /* End backup preparation by Standby */
    MSG_TXN_BEGIN,                /* Start a new transaction */
    MSG_BKUP_TXN_BEGIN,            /* Backup of MSG_TXN_BEGIN */
    MSG_BKUP_GLOBAL_TIMESTAMP,  /* Backup of the latest issued global timestmap */
    MSG_TXN_BEGIN_GETGXID,        /* Start a new transaction and get GXID */
    MSG_BKUP_TXN_BEGIN_GETGXID,    /* Backup of MSG_TXN_BEGIN_GETGXID */
    MSG_TXN_BEGIN_GETGXID_MULTI,        /* Start multiple new transactions and get GXIDs */
    MSG_BKUP_TXN_BEGIN_GETGXID_MULTI,    /* Backup of MSG_TXN_BEGIN_GETGXID_MULTI */
    MSG_TXN_START_PREPARED,            /* Begins to prepare a transation for commit */
    MSG_BKUP_TXN_START_PREPARED,    /* Backup of MSG_TXN_START_PREPARED */
    MSG_TXN_COMMIT,            /* Commit a running or prepared transaction */
    MSG_BKUP_TXN_COMMIT,    /* Backup of MSG_TXN_COMMIT */
    MSG_TXN_LOG_GLOBAL_COMMIT,            /* Log a committed transaction*/
    MSG_TXN_LOG_COMMIT,            /* Log a committed transaction*/
    MSG_TXN_LOG_GLOBAL_SCAN,            /* Log a global scan */
    MSG_TXN_LOG_SCAN,            /* Log a scan */
    MSG_TXN_COMMIT_MULTI,        /* Commit multiple running or prepared transactions */
    MSG_BKUP_TXN_COMMIT_MULTI,    /* Bacukp of MSG_TXN_COMMIT_MULTI */
    MSG_TXN_COMMIT_PREPARED,        /* Commit a prepared transaction */
    MSG_BKUP_TXN_COMMIT_PREPARED,    /* Backup of MSG_TXN_COMMIT_PREPARED */
    MSG_TXN_PREPARE,        /* Finish preparing a transaction */
    MSG_BKUP_TXN_PREPARE,    /* Backup of MSG_TXN_PREPARE */
    MSG_TXN_ROLLBACK,        /* Rollback a transaction */
    MSG_BKUP_TXN_ROLLBACK,    /* Backup of MSG_TXN_ROLLBACK */
    MSG_TXN_ROLLBACK_MULTI,            /* Rollback multiple transactions */
    MSG_BKUP_TXN_ROLLBACK_MULTI,    /* Backup of MSG_TXN_ROLLBACK_MULTI */
    MSG_TXN_GET_GID_DATA,    /* Get info associated with a GID, and get a GXID */
    MSG_TXN_GET_GXID,        /* Get a GXID for a transaction */
    MSG_BKUP_TXN_GET_GXID,
    MSG_TXN_GET_NEXT_GXID,    /* Get next GXID */
    MSG_TXN_GXID_LIST,
    MSG_SNAPSHOT_GET,        /* Get a global snapshot */
    MSG_SNAPSHOT_GET_MULTI,    /* Get multiple global snapshots */
    MSG_SNAPSHOT_GXID_GET,    /* Get GXID and snapshot together */
    MSG_SEQUENCE_INIT,        /* Initialize a new global sequence */
    MSG_BKUP_SEQUENCE_INIT,    /* Backup of MSG_SEQUENCE_INIT */
    MSG_SEQUENCE_GET_CURRENT,/* Get the current value of sequence */
    MSG_SEQUENCE_GET_NEXT,        /* Get the next sequence value of sequence */
    MSG_BKUP_SEQUENCE_GET_NEXT,    /* Backup of MSG_SEQUENCE_GET_NEXT */
    MSG_SEQUENCE_GET_LAST,    /* Get the last sequence value of sequence */
    MSG_SEQUENCE_SET_VAL,        /* Set values for sequence */
    MSG_BKUP_SEQUENCE_SET_VAL,    /* Backup of MSG_SEQUENCE_SET_VAL */
    MSG_SEQUENCE_RESET,            /* Reset the sequence */
    MSG_BKUP_SEQUENCE_RESET,    /* Backup of MSG_SEQUENCE_RESET */
    MSG_SEQUENCE_CLOSE,            /* Close a previously inited sequence */
    MSG_BKUP_SEQUENCE_CLOSE,    /* Backup of MSG_SEQUENCE_CLOSE */
    MSG_SEQUENCE_RENAME,        /* Rename a sequence */
    MSG_BKUP_SEQUENCE_RENAME,    /* Backup of MSG_SEQUENCE_RENAME */
    MSG_SEQUENCE_ALTER,            /* Alter a sequence */
    MSG_BKUP_SEQUENCE_ALTER,    /* Backup of MSG_SEQUENCE_ALTER */
    MSG_SEQUENCE_LIST,        /* Get a list of sequences */
    MSG_TXN_GET_STATUS,        /* Get status of a given transaction */
    MSG_TXN_GET_ALL_PREPARED,    /* Get information about all outstanding
                                 * prepared transactions */
    MSG_TXN_BEGIN_GETGXID_AUTOVACUUM,        /* Start a new transaction and get GXID for autovacuum */
    MSG_BKUP_TXN_BEGIN_GETGXID_AUTOVACUUM,    /* Backup of MSG_TXN_BEGIN_GETGXID_AUTOVACUUM */
    MSG_DATA_FLUSH,                    /* flush pending data */
    MSG_BACKEND_DISCONNECT,            /* tell GTM that the backend diconnected from the proxy */
    MSG_BARRIER,                /* Tell the barrier was issued */
    MSG_BKUP_BARRIER,            /* Backup barrier to standby */
#ifdef POLARDB_X
    /* Gtm storage tags. */
    MSG_GET_STORAGE,            /* Backup get storage file */
    MSG_TXN_FINISH_GID,            /* Finish gid transaction in GTM */
    MSG_LIST_GTM_STORE,            /* List  gtm store info */
    MSG_LIST_GTM_STORE_SEQ,            /* List  gtm running sequence info */
    MSG_LIST_GTM_STORE_TXN,            /* List  gtm running transaction info */
    MSG_CHECK_GTM_STORE_SEQ,            /* Check gtm sequence usage info */
    MSG_CHECK_GTM_STORE_TXN,            /* Check gtm transaction usage info */
    MSG_CLEAN_SESSION_SEQ,        /* clean up session related seq */

    /* Global timestamp tags. */
    MSG_GETGTS,                 /* Get a global timestamp */
    MSG_GETGTS_MULTI,            /* Get multiple global timestamps */

    MSG_CHECK_GTM_STATUS,         /* Get global timestamp from both master and slave gtm. */

    MSG_DB_SEQUENCE_RENAME,      /* Rename all sequence in database*/
    MSG_BKUP_DB_SEQUENCE_RENAME,
#endif

#ifdef POLARDB_X
    MSG_START_REPLICATION,
    MSG_GET_REPLICATION_STATUS,
    MSG_GET_REPLICATION_TRANSFER,
#endif

    /*
     * Must be at the end
     */
    MSG_TYPE_COUNT            /* A dummmy entry just to count the message types */
} GTM_MessageType;

/*
 * Symbols in the following enum are usd in result_name_tab defined in gtm_utils.c.
 * Modifictaion to the following enum should be reflected to result_name_tab as well.
 */
typedef enum GTM_ResultType
{
    SYNC_STANDBY_RESULT,
    NODE_REGISTER_RESULT,
    NODE_UNREGISTER_RESULT,
    REGISTER_SESSION_RESULT,
    REPORT_XMIN_RESULT,
    NODE_LIST_RESULT,
    NODE_BEGIN_REPLICATION_INIT_RESULT,
    NODE_END_REPLICATION_INIT_RESULT,
    BEGIN_BACKUP_SUCCEED_RESULT,
#ifdef POLARDB_X
    BEGIN_BACKUP_FAIL_RESULT,
#endif
    END_BACKUP_RESULT,
    TXN_BEGIN_RESULT,
    TXN_BEGIN_GETGXID_RESULT,
    TXN_BEGIN_GETGTS_RESULT,
    TXN_BEGIN_GETGXID_MULTI_RESULT,
    TXN_BEGIN_GETGTS_MULTI_RESULT,
#ifdef POLARDB_X
    TXN_CHECK_GTM_STATUS_RESULT,
#endif
    TXN_PREPARE_RESULT,
    TXN_START_PREPARED_RESULT,
    TXN_LOG_TRANSACTION_RESULT,
    TXN_LOG_SCAN_RESULT,
    TXN_COMMIT_PREPARED_RESULT,
    TXN_COMMIT_RESULT,
    TXN_COMMIT_MULTI_RESULT,
    TXN_ROLLBACK_RESULT,
    TXN_ROLLBACK_MULTI_RESULT,
    TXN_GET_GID_DATA_RESULT,
    TXN_GET_GXID_RESULT,
    TXN_GET_NEXT_GXID_RESULT,
    TXN_GXID_LIST_RESULT,
    SNAPSHOT_GET_RESULT,
    SNAPSHOT_GET_MULTI_RESULT,
    SNAPSHOT_GXID_GET_RESULT,
    SEQUENCE_INIT_RESULT,
    SEQUENCE_GET_CURRENT_RESULT,
    SEQUENCE_GET_NEXT_RESULT,
    SEQUENCE_GET_LAST_RESULT,
    SEQUENCE_SET_VAL_RESULT,
    SEQUENCE_RESET_RESULT,
    SEQUENCE_CLOSE_RESULT,
    SEQUENCE_RENAME_RESULT,
    SEQUENCE_ALTER_RESULT,
    SEQUENCE_LIST_RESULT,
    TXN_GET_STATUS_RESULT,
    TXN_GET_ALL_PREPARED_RESULT,
    TXN_BEGIN_GETGXID_AUTOVACUUM_RESULT,
    BARRIER_RESULT,
#ifdef POLARDB_X
    STORAGE_TRANSFER_RESULT,
    TXN_FINISH_GID_RESULT,
    MSG_LIST_GTM_STORE_RESULT,
    MSG_LIST_GTM_STORE_SEQ_RESULT,    /* List  gtm running sequence info */
    MSG_LIST_GTM_TXN_STORE_RESULT,    /* List  gtm running transaction info */
    MSG_CHECK_GTM_SEQ_STORE_RESULT,    /* Check gtm sequence usage info */
    MSG_CHECK_GTM_TXN_STORE_RESULT,    /* Check gtm transaction usage info */
    MSG_CLEAN_SESSION_SEQ_RESULT,     
    MSG_DB_SEQUENCE_RENAME_RESULT,
#endif

#ifdef POLARDB_X
    MSG_REPLICATION_START_RESULT_SUCCESS,
    MSG_REPLICATION_START_RESULT_FAIL,
    MSG_REPLICATION_STATUS,
    MSG_REPLICATION_CONTENT,
#endif

    RESULT_TYPE_COUNT
} GTM_ResultType;

/*
 * Special message header for the messgaes exchanged between the GTM server and
 * the proxy.
 *
 * ph_conid: connection identifier which is used to route
 * the messages to the right backend.
 */
typedef struct GTM_ProxyMsgHeader
{
    GTMProxy_ConnID    ph_conid;
} GTM_ProxyMsgHeader;

#endif
