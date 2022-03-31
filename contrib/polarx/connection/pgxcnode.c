/*-------------------------------------------------------------------------
 *
 * pgxcnode.c
 *
 *      Functions for the Coordinator communicating with the PGXC nodes:
 *      Datanodes and Coordinators
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *        contrib/polarx/connection/pgxcnode.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "polarx.h"
#include <poll.h>

#ifdef __sun
#include <sys/filio.h>
#endif

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include "../interfaces/libpq/libpq-fe.h"
#include "../interfaces/libpq/libpq-int.h"
#include "access/htup_details.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "commands/prepare.h"
#include "miscadmin.h"
#include "nodes/nodes.h"
#include "pgxc/locator.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxcnode.h"
#include "pool/poolnodes.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "tcop/dest.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/fmgroids.h"
#include "utils/formatting.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tqual.h"
#include "foreign/foreign.h"
#include "catalog/pg_foreign_server.h"
#include "commands/defrem.h"
#include "utils/inval.h"
#include "pgxc/connpool.h"
#include "pgxc/mdcache.h"
#include "commands/polarx_variable_set.h"

#ifdef PATCH_ENABLE_DISTRIBUTED_TRANSACTION
#include "pgxc/transam/txn_util.h"
#include "distributed_txn/txn_timestamp.h"
#endif

#define CMD_ID_MSG_LEN 8
#define PGXC_CANCEL_DELAY 100
#define DATA_ROW_BUFFER_SIZE(n) (DataRowBufferSize * 1024 * 1024 * (n))

/* Number of connections held */
static int datanode_count = 0;
static int coord_count = 0;
static int slavedatanode_count = 0;
int DataRowBufferSize = 0; /* MBytes */
bool enable_log_remote_query = false;

/*
 * Datanode handles saved in Transaction memory context
 * when PostgresMain is launched.
 * Those handles are used inside a transaction by Coordinator to Datanodes.
 */
static PGXCNodeHandle *dn_handles = NULL;
static PGXCNodeHandle *sdn_handles = NULL;

/*
 * Coordinator handles saved in Transaction memory context
 * when PostgresMain is launched.
 * Those handles are used inside a transaction by Coordinator to Coordinators
 */
static PGXCNodeHandle *co_handles = NULL;

/* Hash key: nodeoid value: index in  dn_handles or co_handles */
static HTAB *node_handles_hash = NULL;

typedef struct PGXCNodeHandlesLookupEnt
{
    Oid nodeoid;   /* Node Oid */
    int32 nodeidx; /* Node index*/
    uint32 hashvalue; /* Node index*/
} PGXCNodeHandlesLookupEnt;
static int
pgxc_check_socket_health(int sock, int forRead, int forWrite, time_t end_time);

/* Current size of dn_handles and co_handles */
int NumDataNodes;
int NumCoords;
int NumSlaveDataNodes;

volatile bool HandlesInvalidatePending = false;
volatile bool HandlesRefreshPending = false;

/*
 * Session and transaction parameters need to to be set on newly connected
 * remote nodes.
 */
static List *session_param_list = NIL;
static List *local_param_list = NIL;
static List *saved_param_list = NIL;
static StringInfo session_params;
static StringInfo local_params;

typedef struct
{
    NameData name;
    NameData value;
    int flags;
    bool is_null;
} ParamEntry;

static bool DoInvalidateRemoteHandles(void);
static bool DoRefreshRemoteHandles(void);

static void pgxc_node_init(PGXCNodeHandle *handle, int sock,
                           bool global_session, int pid);
static void pgxc_node_free(PGXCNodeHandle *handle);
static void pgxc_node_all_free(void);

static int get_int(PGXCNodeHandle *conn, size_t len, int *out);
static int get_char(PGXCNodeHandle *conn, char *out);

static ParamEntry *paramlist_get_paramentry(List *param_list, const char *name);
static ParamEntry *paramentry_copy(ParamEntry *src_entry);
static void polar_handles_inval_callback(Datum arg, int cacheid, uint32 hashvalue);

/*
 * Initialize PGXCNodeHandle struct
 */
static void
init_pgxc_handle(PGXCNodeHandle *pgxc_handle)
{
    /*
     * Socket descriptor is small non-negative integer,
     * Indicate the handle is not initialized yet
     */
    pgxc_handle->sock = NO_SOCKET;

    /* Initialise buffers */
    pgxc_handle->error[0] = '\0';
    pgxc_handle->outSize = 16 * 1024;
    pgxc_handle->outBuffer = (char *)palloc(pgxc_handle->outSize);
    pgxc_handle->inSize = 16 * 1024;

    pgxc_handle->inBuffer = (char *)palloc(pgxc_handle->inSize);
    pgxc_handle->combiner = NULL;
    pgxc_handle->inStart = 0;
    pgxc_handle->inEnd = 0;
    pgxc_handle->inCursor = 0;
    pgxc_handle->outEnd = 0;
    pgxc_handle->needSync = false;
    pgxc_handle->sendGxidVersion = 0;

    if (pgxc_handle->outBuffer == NULL || pgxc_handle->inBuffer == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory")));
    }
}
static void init_handles(bool is_force)
{ // #lizard forgives
    int count;
    Oid *coOids, *dnOids, *sdnOids;
    MemoryContext oldcontext;

    /* Init node handles hashtable */
    HASHCTL hinfo;
    int hflags;
    bool found;
    PGXCNodeHandlesLookupEnt *node_handle_ent = NULL;

    /* Free all the existing information first */
    if (is_force)
        pgxc_node_all_free();

    /* This function could get called multiple times because of sigjmp */
    if (dn_handles != NULL &&
        co_handles != NULL)
        return;

    /* Get classified list of node Oids */
    PgxcNodeGetOidsExtend(&coOids, &dnOids, &sdnOids, &NumCoords, &NumDataNodes, &NumSlaveDataNodes, true);

    /*
     * Coordinator and datanode handles should be available during all the
     * session lifetime
     */
    oldcontext = MemoryContextSwitchTo(TopMemoryContext);

    /* Do proper initialization of handles */
    if (NumDataNodes > 0)
        dn_handles = (PGXCNodeHandle *)
            palloc(NumDataNodes * sizeof(PGXCNodeHandle));
    if (NumCoords > 0)
        co_handles = (PGXCNodeHandle *)
            palloc(NumCoords * sizeof(PGXCNodeHandle));
    if (NumSlaveDataNodes > 0)
        sdn_handles = (PGXCNodeHandle *)
            palloc(NumSlaveDataNodes * sizeof(PGXCNodeHandle));

    if ((!dn_handles && NumDataNodes > 0) ||
        (!co_handles && NumCoords > 0) ||
        (!sdn_handles && NumSlaveDataNodes > 0))
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory for node handles")));

    /* destory hash table */
    if (node_handles_hash)
    {
        hash_destroy(node_handles_hash);
        node_handles_hash = NULL;
    }
    else
    {
        CacheRegisterSyscacheCallback(FOREIGNSERVEROID,
                                      polar_handles_inval_callback, (Datum)0);
        on_proc_exit(PGXCNodeCleanAndRelease, 0);
    }

    MemSet(&hinfo, 0, sizeof(hinfo));
    hflags = 0;

    hinfo.keysize = sizeof(Oid);
    hinfo.entrysize = sizeof(PGXCNodeHandlesLookupEnt);
    hflags |= HASH_ELEM;

    hinfo.hash = oid_hash;
    hflags |= HASH_FUNCTION;

    node_handles_hash = hash_create("Node Handles Hash", NumCoords + NumDataNodes + NumSlaveDataNodes,
                                    &hinfo, hflags);

    /* Initialize new empty slots */
    for (count = 0; count < NumDataNodes; count++)
    {

        init_pgxc_handle(&dn_handles[count]);
        dn_handles[count].nodeoid = dnOids[count];
        dn_handles[count].nodeid = get_pgxc_node_id(dnOids[count]);
        strncpy(dn_handles[count].nodename, get_pgxc_nodename(dnOids[count]),
                NAMEDATALEN);
        strncpy(dn_handles[count].nodehost, get_pgxc_nodehost(dnOids[count]),
                NAMEDATALEN);
        dn_handles[count].nodeport = get_pgxc_nodeport(dnOids[count]);
        if (enable_multi_cluster_print)
            elog(LOG, "dn handle %d nodename %s nodehost %s nodeport %d Oid %d NumDataNodes %d", count, dn_handles[count].nodename,
                 dn_handles[count].nodehost, dn_handles[count].nodeport, dnOids[count], NumDataNodes);

        if(IS_PGXC_SINGLE_NODE &&
            is_pgxc_nodelocal(dn_handles[count].nodeoid))
        {
            IS_PGXC_DATANODE = true;
            if (PGXCNodeName == NULL || *PGXCNodeName == '\0')
                PGXCNodeName = strdup(dn_handles[count].nodename);
        }
        node_handle_ent = (PGXCNodeHandlesLookupEnt *)hash_search(node_handles_hash, &(dn_handles[count].nodeoid),
                                                                  HASH_ENTER, &found);
        if (node_handle_ent)
        {
            node_handle_ent->nodeoid = dn_handles[count].nodeoid;
            node_handle_ent->nodeidx = count;
            node_handle_ent->hashvalue = GetSysCacheHashValue1(FOREIGNSERVEROID,
                                                                ObjectIdGetDatum(node_handle_ent->nodeoid));
        }
    }

    for (count = 0; count < NumCoords; count++)
    {
        init_pgxc_handle(&co_handles[count]);
        co_handles[count].nodeoid = coOids[count];
        co_handles[count].nodeid = get_pgxc_node_id(coOids[count]);
        strncpy(co_handles[count].nodename, get_pgxc_nodename(coOids[count]),
                NAMEDATALEN);
        strncpy(co_handles[count].nodehost, get_pgxc_nodehost(coOids[count]),
                NAMEDATALEN);
        co_handles[count].nodeport = get_pgxc_nodeport(coOids[count]);
        if (enable_multi_cluster_print)
            elog(LOG, "cn handle %d nodename %s nodehost %s nodeport %d Oid %d", count, co_handles[count].nodename,
                 co_handles[count].nodehost, co_handles[count].nodeport, coOids[count]);

        node_handle_ent = (PGXCNodeHandlesLookupEnt *)hash_search(node_handles_hash, &(co_handles[count].nodeoid),
                                                                  HASH_ENTER, &found);
        if(IS_PGXC_SINGLE_NODE &&
                is_pgxc_nodelocal(co_handles[count].nodeoid))
        {
            IS_PGXC_COORDINATOR = true;
            if (PGXCNodeName == NULL || *PGXCNodeName == '\0')
                PGXCNodeName = strdup(co_handles[count].nodename);
        }
        if (node_handle_ent)
        {
            node_handle_ent->nodeoid = co_handles[count].nodeoid;
            node_handle_ent->nodeidx = count;
            node_handle_ent->hashvalue = GetSysCacheHashValue1(FOREIGNSERVEROID,
                                                                ObjectIdGetDatum(node_handle_ent->nodeoid));
        }
    }

    datanode_count = 0;
    coord_count = 0;
    slavedatanode_count = 0;
    PGXCNodeId = 0;

    MemoryContextSwitchTo(oldcontext);

    if (IS_PGXC_COORDINATOR)
    {
        for (count = 0; count < NumCoords; count++)
        {
            if (is_pgxc_nodelocal(co_handles[count].nodeoid))
                PGXCNodeId = count + 1;
        }
    }
    else if (IS_PGXC_DATANODE) /* DataNode */
    {
        for (count = 0; count < NumDataNodes; count++)
        {
            if (is_pgxc_nodelocal(dn_handles[count].nodeoid))
                PGXCNodeId = count + 1;
        }

        for (count = 0; count < NumSlaveDataNodes; count++)
        {
            if (is_pgxc_nodelocal(sdn_handles[count].nodeoid))
                PGXCNodeId = count + 1;
        }
    }

    if (strcmp(PGXCMainClusterName, PGXCClusterName) == 0)
        IsPGXCMainCluster = true;
}
/*
 * Allocate and initialize memory to store Datanode and Coordinator handles.
 */
void InitMultinodeExecutor(bool is_force)
{ // #lizard forgives
    /* Update node table in the shared memory */
    if(!GetPoolerWorkerStartStatus() && polarx_has_been_loaded())
    {
        PoolManagerCatchupNodeInfo();
        init_handles(is_force);
    }
}

Oid get_nodeoid_from_nodeid(int nodeid, char node_type)
{
    if (PGXC_NODE_COORDINATOR == node_type)
    {
        if (nodeid >= NumCoords)
        {
            return InvalidOid;
        }
        return co_handles[nodeid].nodeoid;
    }
    else if (PGXC_NODE_DATANODE == node_type)
    {
        if (nodeid >= NumDataNodes)
        {
            return InvalidOid;
        }
        return dn_handles[nodeid].nodeoid;
    }
    return InvalidOid;
}

/*
 * Called when the backend is ending.
 */
void PGXCNodeCleanAndRelease(int code, Datum arg)
{

    /* Disconnect from Pooler, if any connection is still held Pooler close it */
    PoolManagerDisconnect();
}

/* Close the socket handle (this process' copy) and free occupied memory
 *
 * Note that we do not free the handle and its members. This will be
 * taken care of when the transaction ends, when TopTransactionContext
 * is destroyed in xact.c.
 */
static void
pgxc_node_free(PGXCNodeHandle *handle)
{
    if (handle->sock != NO_SOCKET)
    {
        close(handle->sock);
    }
    handle->sock = NO_SOCKET;
}

/*
 * Free all the node handles cached
 */
static void
pgxc_node_all_free(void)
{
    int i, j;

    for (i = 0; i < 3; i++)
    {
        int num_nodes = 0;
        PGXCNodeHandle *array_handles;

        switch (i)
        {
        case 0:
            num_nodes = NumCoords;
            array_handles = co_handles;
            break;
        case 1:
            num_nodes = NumDataNodes;
            array_handles = dn_handles;
            break;
        case 2:
            num_nodes = NumSlaveDataNodes;
            array_handles = sdn_handles;
            break;
        default:
            Assert(0);
        }

        for (j = 0; j < num_nodes; j++)
        {
            PGXCNodeHandle *handle = &array_handles[j];
            pgxc_node_free(handle);
        }
        if (array_handles)
            pfree(array_handles);
    }

    co_handles = NULL;
    dn_handles = NULL;
    sdn_handles = NULL;
    HandlesInvalidatePending = false;
    HandlesRefreshPending = false;
}

/*
 * Create and initialise internal structure to communicate to
 * Datanode via supplied socket descriptor.
 * Structure stores state info and I/O buffers
 */
static void
pgxc_node_init(PGXCNodeHandle *handle, int sock, bool global_session, int pid)
{ // #lizard forgives
    char *init_str;

    handle->sock = sock;
    handle->backend_pid = pid;
    handle->transaction_status = 'I';
    PGXCNodeSetConnectionState(handle, DN_CONNECTION_STATE_IDLE);
    handle->read_only = true;
    handle->ck_resp_rollback = false;
    handle->combiner = NULL;
#ifdef DN_CONNECTION_DEBUG
    handle->have_row_desc = false;
#endif
    memset(handle->error, 0X00, MAX_ERROR_MSG_LENGTH);
    handle->outEnd = 0;
    handle->inStart = 0;
    handle->inEnd = 0;
    handle->inCursor = 0;
    handle->needSync = false;
    handle->recv_datarows = 0;
    handle->sendGxidVersion = 0;
    /*
     * We got a new connection, set on the remote node the session parameters
     * if defined. The transaction parameter should be sent after BEGIN
     */
    if (global_session)
    {
        init_str = PGXCNodeGetSessionParamStr();
        if (init_str)
        {
            pgxc_node_set_query(handle, init_str);
        }
    }
}

/*
 * Wait while at least one of specified connections has data available and read
 * the data into the buffer
 */
int pgxc_node_receive(const int conn_count,
                      PGXCNodeHandle **connections, struct timeval *timeout)
{ // #lizard forgives

    int i,
        sockets_to_poll,
        poll_val;
    bool is_msg_buffered;
    long timeout_ms;
    struct pollfd pool_fd[conn_count];

    /* sockets to be polled index */
    sockets_to_poll = 0;

    is_msg_buffered = false;
    for (i = 0; i < conn_count; i++)
    {
        /* If connection has a buffered message */
        if (HAS_MESSAGE_BUFFERED(connections[i]))
        {
            is_msg_buffered = true;
            break;
        }
    }

    for (i = 0; i < conn_count; i++)
    {
        /* If connection finished sending do not wait input from it */
        if (connections[i]->state == DN_CONNECTION_STATE_IDLE || HAS_MESSAGE_BUFFERED(connections[i]))
        {
            pool_fd[i].fd = -1;
            pool_fd[i].events = 0;
            elog(DEBUG1, "pgxc_node_receive node:%s pid:%d in DN_CONNECTION_STATE_IDLE no need to receive. ", connections[i]->nodename, connections[i]->backend_pid);
            continue;
        }

        /* prepare select params */
        if (connections[i]->sock > 0)
        {
            pool_fd[i].fd = connections[i]->sock;
            pool_fd[i].events = POLLIN | POLLPRI | POLLRDNORM | POLLRDBAND;
            sockets_to_poll++;
        }
        else
        {
            /* flag as bad, it will be removed from the list */
            PGXCNodeSetConnectionState(connections[i], DN_CONNECTION_STATE_ERROR_FATAL);
            pool_fd[i].fd = -1;
            pool_fd[i].events = 0;
        }
    }

    /*
     * Return if we do not have connections to receive input
     */
    if (sockets_to_poll == 0)
    {
        if (is_msg_buffered)
        {
            return DNStatus_OK;
        }
        elog(LOG, "no message in buffer");
        return DNStatus_ERR;
    }

    /* do conversion from the select behaviour */
    if (timeout == NULL)
    {
        timeout_ms = -1;
    }
    else
    {
        timeout_ms = (timeout->tv_sec * (uint64_t)1000) + (timeout->tv_usec / 1000);
    }

retry:
    CHECK_FOR_INTERRUPTS();
    poll_val = poll(pool_fd, conn_count, timeout_ms);
    if (poll_val < 0)
    {
        /* error - retry if EINTR */
        if (errno == EINTR || errno == EAGAIN)
        {
            goto retry;
        }

        if (errno == EBADF)
        {
            elog(LOG, "poll() bad file descriptor set");
        }
        elog(LOG, "poll() failed for error: %d, %s", errno, strerror(errno));

        if (errno)
        {
            return DNStatus_ERR;
        }

        return DNStatus_OK;
    }

    if (poll_val == 0)
    {
        /* Handle timeout */
        elog(DEBUG1, "timeout %ld while waiting for any response from %d connections", timeout_ms, conn_count);

        for (i = 0; i < conn_count; i++)
        {
            PGXCNodeHandle *conn = connections[i];
            elog(DEBUG1, "timeout %ld while waiting for any response from node:%s pid:%d connections", timeout_ms, conn->nodename, conn->backend_pid);
            //PGXCNodeSetConnectionState(connections[i], DN_CONNECTION_STATE_ERROR_FATAL);
        }
        return DNStatus_EXPIRED;
    }

    /* read data */
    for (i = 0; i < conn_count; i++)
    {
        PGXCNodeHandle *conn = connections[i];

        if (pool_fd[i].fd == -1)
            continue;

        if (pool_fd[i].fd == conn->sock)
        {
            if (pool_fd[i].revents & POLLIN)
            {
                int read_status = pgxc_node_read_data(conn, true);
                if (read_status == EOF || read_status < 0)
                {
                    /* Can not read - no more actions, just discard connection */
                    PGXCNodeSetConnectionState(conn,
                                               DN_CONNECTION_STATE_ERROR_FATAL);
                    add_error_message(conn, "unexpected EOF on datanode connection.");
                    elog(LOG, "unexpected EOF on node:%s pid:%d", conn->nodename, conn->backend_pid);
#if 0
                    /*
                     * before returning, also update the shared health
                     * status field to indicate that this node could be
                     * possibly unavailable.
                     *
                     * Note that this error could be due to a stale handle
                     * and it's possible that another backend might have
                     * already updated the health status OR the node
                     * might have already come back since the last disruption
                     */
                    PoolPingNodeRecheck(conn->nodeoid);

                    /* Should we read from the other connections before returning? */
#endif
                    return DNStatus_ERR;
                }
            }
            else if (
                (pool_fd[i].revents & POLLERR) ||
                (pool_fd[i].revents & POLLHUP) ||
                (pool_fd[i].revents & POLLNVAL))
            {
                PGXCNodeSetConnectionState(connections[i],
                                           DN_CONNECTION_STATE_ERROR_FATAL);
                add_error_message(conn, "unexpected network error on datanode connection");
                elog(LOG, "unexpected EOF on datanode:%s pid:%d with event %d", conn->nodename, conn->backend_pid, pool_fd[i].revents);
                /* Should we check/read from the other connections before returning? */
                return DNStatus_ERR;
            }
        }
    }
    return DNStatus_OK;
}

void pgxc_print_pending_data(PGXCNodeHandle *handle, bool reset)
{ // #lizard forgives
    char *msg;
    int32 ret;
    //DNConnectionState estate = 0;
    int msg_len;
    char msg_type;
    //char        txn_status = 'I';
    struct timeval timeout;
    timeout.tv_sec = 0;
    timeout.tv_usec = 1000;

    //estate     = handle->state;
    //txn_status = handle->transaction_status;
    for (;;)
    {
        handle->state = DN_CONNECTION_STATE_QUERY;
        ret = pgxc_node_receive(1, &handle, &timeout);
        if (DNStatus_ERR == ret)
        {
            elog(LOG, "pgxc_print_pending_data pgxc_node_receive LEFT_OVER data ERROR");
            break;
        }
        else if (DNStatus_OK == ret)
        {
            elog(LOG, "pgxc_print_pending_data pgxc_node_receive LEFT_OVER data succeed");
        }
        else
        {
            elog(LOG, "pgxc_print_pending_data pgxc_node_receive LEFT_OVER data timeout");
        }

        /* No data available, exit */
        if (!HAS_MESSAGE_BUFFERED(handle))
        {
            elog(LOG, "pgxc_print_pending_data pgxc_node_receive LEFT_OVER data finished");
            break;
        }

        /* TODO handle other possible responses */
        msg_type = get_message(handle, &msg_len, &msg);
        switch (msg_type)
        {
        case '\0': /* Not enough data in the buffer */
            goto DONE;
        case 'c': /* CopyToCommandComplete */
            elog(LOG, "LEFT_OVER CopyToCommandComplete found");
            break;
        case 'C': /* CommandComplete */
            elog(LOG, "LEFT_OVER CommandComplete found");
            break;
        case 'T': /* RowDescription */
            elog(LOG, "LEFT_OVER RowDescription found");
            break;
        case 'D': /* DataRow */
            elog(LOG, "LEFT_OVER DataRow found");
            break;
        case 's': /* PortalSuspended */
            elog(LOG, "LEFT_OVER PortalSuspended found");
            break;
        case '1': /* ParseComplete */
        case '2': /* BindComplete */
        case '3': /* CloseComplete */
        case 'n': /* NoData */
            /* simple notifications, continue reading */
            break;
        case 'G': /* CopyInResponse */
            elog(LOG, "LEFT_OVER CopyInResponse found");
            break;
        case 'H': /* CopyOutResponse */
            elog(LOG, "LEFT_OVER CopyOutResponse found");
            break;
        case 'd': /* CopyOutDataRow */
            elog(LOG, "LEFT_OVER CopyOutDataRow found");
            break;
        case 'E': /* ErrorResponse */
            elog(LOG, "LEFT_OVER ErrorResponse found");
            break;
        case 'A': /* NotificationResponse */
        case 'N': /* NoticeResponse */
        case 'S': /* SetCommandComplete */
            /*
                 * Ignore these to prevent multiple messages, one from each
                 * node. Coordinator will send one for DDL anyway
                 */
            break;
        case 'Z': /* ReadyForQuery */
        {
            elog(LOG, "LEFT_OVER ReadyForQuery found");
            break;
        }

        case 'Y': /* ReadyForQuery */
        {
            elog(LOG, "LEFT_OVER ReadyForQuery found");
            break;
        }
        case 'M': /* Command Id */
            elog(LOG, "LEFT_OVER Command Id found");
            break;
        case 'b':
            elog(LOG, "LEFT_OVER DN_CONNECTION_STATE_IDLE found");
            break;

        case 'I': /* EmptyQuery */
            elog(LOG, "LEFT_OVER EmptyQuery found");
            break;
        case 'W':
            elog(LOG, "LEFT_OVER W found");
            break;
        case 'x':
            elog(LOG, "LEFT_OVER RESPONSE_ASSIGN_GXID found");
            break;
        default:
            elog(LOG, "LEFT_OVER invalid status found");
            break;
        }
    }

DONE:
    handle->state = DN_CONNECTION_STATE_IDLE;
    handle->transaction_status = 'I';
    handle->error[0] = '\0';

    /* reset the status */
    if (reset)
    {
        if (handle->error[0])
        {
            elog(LOG, "pgxc_print_pending_data LEFT_OVER errmsg:%s", handle->error);
            handle->error[0] = '\0';
        }
        handle->outEnd = 0;
        handle->inStart = 0;
        handle->inEnd = 0;
        handle->inCursor = 0;
        handle->needSync = false;
    }
}

/*
 * Is there any data enqueued in the TCP input buffer waiting
 * to be read sent by the PGXC node connection
 */

int pgxc_node_is_data_enqueued(PGXCNodeHandle *conn)
{
    int ret;
    int enqueued;

    if (conn->sock < 0)
        return 0;
    ret = ioctl(conn->sock, FIONREAD, &enqueued);
    if (ret != 0)
        return 0;

    return enqueued;
}

/* Abort the connections, ensure the connections are clean and empty. */
void pgxc_abort_connections(PGXCNodeAllHandles *all_handles)
{ // #lizard forgives
    int ret = false;
    int i = 0;
    bool need_loop_check = false;

    if (all_handles)
    {
        while (1)
        {
            need_loop_check = false;
            elog(DEBUG1, "pgxc_abort_connections %d coordinator to handle", all_handles->co_conn_count);
            for (i = 0; i < all_handles->co_conn_count; i++)
            {
                PGXCNodeHandle *handle = all_handles->coord_handles[i];
                if (handle->sock != NO_SOCKET && handle->sock < FD_SETSIZE)
                {
                    if (handle->state != DN_CONNECTION_STATE_IDLE || !node_ready_for_query(handle) || pgxc_node_is_data_enqueued(handle))
                    {
                        elog(DEBUG1, "pgxc_abort_connections node:%s not ready for query, status:%d", handle->nodename, handle->state);
                        if (handle->sock != NO_SOCKET && handle->sock < FD_SETSIZE)
                        {
                            pgxc_node_flush_read(handle);
                            handle->state = DN_CONNECTION_STATE_IDLE;
                        }
                        /* Clear any previous error messages */
                        handle->error[0] = '\0';
                    }
                    else
                    {
                        elog(DEBUG1, "pgxc_abort_connections node:%s ready for query", handle->nodename);
                    }
                }
                else
                {
                    if (handle->nodename[0] != '\0')
                    {
                        elog(WARNING, "pgxc_abort_connections cn node:%s invalid socket %u!", handle->nodename, handle->sock);
                    }
                    else
                    {
                        elog(WARNING, "pgxc_abort_connections cn node, invalid socket %u!", handle->sock);
                    }
                }
            }

            /*
             * The same for datanode nodes.
             */
            elog(DEBUG1, "pgxc_abort_connections %d datanode to handle", all_handles->dn_conn_count);
            for (i = 0; i < all_handles->dn_conn_count; i++)
            {
                PGXCNodeHandle *handle = all_handles->datanode_handles[i];
                if (handle->sock != NO_SOCKET && handle->sock < FD_SETSIZE)
                {
                    if (handle->state != DN_CONNECTION_STATE_IDLE || !node_ready_for_query(handle) || pgxc_node_is_data_enqueued(handle))
                    {
                        elog(DEBUG1, "pgxc_abort_connections node:%s not ready for query, status:%d", handle->nodename, handle->state);
                        pgxc_node_flush_read(handle);
                        handle->state = DN_CONNECTION_STATE_IDLE;

                        /* Clear any previous error messages */
                        handle->error[0] = '\0';
                    }
                    else
                    {
                        elog(DEBUG1, "pgxc_abort_connections node:%s ready for query", handle->nodename);
                    }
                }
                else
                {
                    if (handle->nodename[0] != '\0')
                    {
                        elog(WARNING, "pgxc_abort_connections dn node:%s invalid socket %u!", handle->nodename, handle->sock);
                    }
                    else
                    {
                        elog(WARNING, "pgxc_abort_connections dn node, invalid socket %u!", handle->sock);
                    }
                }
            }

            /* Recheck connection status. */
            elog(DEBUG1, " Begin to recheck pgxc_abort_connections %d coordinator handle", all_handles->co_conn_count);
            for (i = 0; i < all_handles->co_conn_count; i++)
            {
                PGXCNodeHandle *handle = all_handles->coord_handles[i];
                if (handle->sock != NO_SOCKET && handle->sock < FD_SETSIZE)
                {
                    if (handle->state != DN_CONNECTION_STATE_IDLE || !node_ready_for_query(handle) || pgxc_node_is_data_enqueued(handle))
                    {
                        elog(DEBUG1, "pgxc_abort_connections recheck node:%s not ready for query, status:%d, sync", handle->nodename, handle->state);
                        ret = pgxc_node_send_sync(handle);
                        if (!ret)
                        {
                            need_loop_check = true;
                        }
                    }
                    else
                    {
                        elog(DEBUG1, "pgxc_abort_connections recheck node:%s ready for query", handle->nodename);
                    }
                }
                else
                {
                    if (handle->nodename[0] != '\0')
                    {
                        elog(WARNING, "pgxc_abort_connections cn node:%s invalid socket %u!", handle->nodename, handle->sock);
                    }
                    else
                    {
                        elog(WARNING, "pgxc_abort_connections cn node, invalid socket %u!", handle->sock);
                    }
                }
            }

            /*
             * recheck datanode nodes.
             */
            elog(DEBUG1, " Begin to recheck pgxc_abort_connections %d datanode handle", all_handles->dn_conn_count);
            for (i = 0; i < all_handles->dn_conn_count; i++)
            {
                PGXCNodeHandle *handle = all_handles->datanode_handles[i];
                if (handle->sock != NO_SOCKET && handle->sock < FD_SETSIZE)
                {
                    if (handle->state != DN_CONNECTION_STATE_IDLE || !node_ready_for_query(handle) || pgxc_node_is_data_enqueued(handle))
                    {
                        elog(DEBUG1, "pgxc_abort_connections recheck node:%s not ready for query, status:%d, sync", handle->nodename, handle->state);
                        ret = pgxc_node_send_sync(handle);
                        if (!ret)
                        {
                            need_loop_check = true;
                        }
                    }
                    else
                    {
                        elog(DEBUG1, "pgxc_abort_connections recheck node:%s ready for query", handle->nodename);
                    }
                }
                else
                {
                    if (handle->nodename[0] != '\0')
                    {
                        elog(WARNING, "pgxc_abort_connections dn node:%s invalid socket %u!", handle->nodename, handle->sock);
                    }
                    else
                    {
                        elog(WARNING, "pgxc_abort_connections dn node, invalid socket %u!", handle->sock);
                    }
                }
            }

            /* no need to recheck, break the loop. */
            if (!need_loop_check)
            {
                break;
            }
            /* Sleep a while. */
#if PGXC_CANCEL_DELAY > 0
            pg_usleep(PGXC_CANCEL_DELAY * 1000);
#endif
        }
    }
}

/*
 * Read up incoming messages from the PGXC node connection
 */
int pgxc_node_read_data(PGXCNodeHandle *conn, bool close_if_error)
{ // #lizard forgives
    int someread = 0;
    int nread;

    if (conn->sock < 0)
    {
        if (close_if_error)
            add_error_message(conn, "bad socket");
        return EOF;
    }

    /* Left-justify any data in the buffer to make room */
    if (conn->inStart < conn->inEnd)
    {
        if (conn->inStart > 0)
        {
            memmove(conn->inBuffer, conn->inBuffer + conn->inStart,
                    conn->inEnd - conn->inStart);
            conn->inEnd -= conn->inStart;
            conn->inCursor -= conn->inStart;
            conn->inStart = 0;
        }
    }
    else
    {
        /* buffer is logically empty, reset it */
        conn->inStart = conn->inCursor = conn->inEnd = 0;
    }

    /*
     * If the buffer is fairly full, enlarge it. We need to be able to enlarge
     * the buffer in case a single message exceeds the initial buffer size. We
     * enlarge before filling the buffer entirely so as to avoid asking the
     * kernel for a partial packet. The magic constant here should be large
     * enough for a TCP packet or Unix pipe bufferload.  8K is the usual pipe
     * buffer size, so...
     */
    if (conn->inSize - conn->inEnd < 8192)
    {
        if (ensure_in_buffer_capacity(conn->inEnd + (size_t)8192, conn) != 0)
        {
            /*
             * We don't insist that the enlarge worked, but we need some room
             */
            if (conn->inSize - conn->inEnd < 100)
            {
                if (close_if_error)
                    add_error_message(conn, "can not allocate buffer");
                return -1;
            }
        }
    }

retry:
    nread = recv(conn->sock, conn->inBuffer + conn->inEnd,
                 conn->inSize - conn->inEnd, 0);

    if (nread < 0)
    {
        if (errno == EINTR)
            goto retry;
            /* Some systems return EAGAIN/EWOULDBLOCK for no data */
#ifdef EAGAIN
        if (errno == EAGAIN)
            return someread;
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
        if (errno == EWOULDBLOCK)
            return someread;
#endif
            /* We might get ECONNRESET here if using TCP and backend died */
#ifdef ECONNRESET
        if (errno == ECONNRESET)
        {
            /*
             * OK, we are getting a zero read even though select() says ready. This
             * means the connection has been closed.  Cope.
             */
            if (close_if_error)
            {
                add_error_message(conn,
                                  "Datanode closed the connection unexpectedly\n"
                                  "\tThis probably means the Datanode terminated abnormally\n"
                                  "\tbefore or while processing the request.\n");
                PGXCNodeSetConnectionState(conn,
                                           DN_CONNECTION_STATE_ERROR_FATAL); /* No more connection to
                                                            * backend */
                closesocket(conn->sock);
                conn->sock = NO_SOCKET;
            }
            return -1;
        }
#endif
        if (close_if_error)
            add_error_message(conn, "could not receive data from server");
        return -1;
    }

    if (nread > 0)
    {
        conn->inEnd += nread;

        /*
         * Hack to deal with the fact that some kernels will only give us back
         * 1 packet per recv() call, even if we asked for more and there is
         * more available.    If it looks like we are reading a long message,
         * loop back to recv() again immediately, until we run out of data or
         * buffer space.  Without this, the block-and-restart behavior of
         * libpq's higher levels leads to O(N^2) performance on long messages.
         *
         * Since we left-justified the data above, conn->inEnd gives the
         * amount of data already read in the current message.    We consider
         * the message "long" once we have acquired 32k ...
         */
        if (conn->inEnd > 32768 &&
            (conn->inSize - conn->inEnd) >= 8192)
        {
            someread = 1;
            goto retry;
        }
        return 1;
    }

    if (nread == 0)
    {
        if (close_if_error)
            elog(DEBUG1, "nread returned 0");
        return EOF;
    }

    if (someread)
        return 1; /* got a zero read after successful tries */

    return 0;
}

/*
 * Get one character from the connection buffer and advance cursor
 */
static int
get_char(PGXCNodeHandle *conn, char *out)
{
    if (conn->inCursor < conn->inEnd)
    {
        *out = conn->inBuffer[conn->inCursor++];
        return 0;
    }
    return EOF;
}

/*
 * Read an integer from the connection buffer and advance cursor
 */
static int
get_int(PGXCNodeHandle *conn, size_t len, int *out)
{
    unsigned short tmp2;
    unsigned int tmp4;

    if (conn->inCursor + len > conn->inEnd)
        return EOF;

    switch (len)
    {
    case 2:
        memcpy(&tmp2, conn->inBuffer + conn->inCursor, 2);
        conn->inCursor += 2;
        *out = (int)ntohs(tmp2);
        break;
    case 4:
        memcpy(&tmp4, conn->inBuffer + conn->inCursor, 4);
        conn->inCursor += 4;
        *out = (int)ntohl(tmp4);
        break;
    default:
        add_error_message(conn, "not supported int size");
        return EOF;
    }

    return 0;
}

/*
 * get_message
 * If connection has enough data read entire message from the connection buffer
 * and returns message type. Message data and data length are returned as
 * var parameters.
 * If buffer does not have enough data leaves cursor unchanged, changes
 * connection status to DN_CONNECTION_STATE_QUERY indicating it needs to
 * receive more and returns \0
 * conn - connection to read from
 * len - returned length of the data where msg is pointing to
 * msg - returns pointer to memory in the incoming buffer. The buffer probably
 * will be overwritten upon next receive, so if caller wants to refer it later
 * it should make a copy.
 */
char get_message(PGXCNodeHandle *conn, int *len, char **msg)
{
    char msgtype;

    if (get_char(conn, &msgtype) || get_int(conn, 4, len))
    {
        /* Successful get_char would move cursor, restore position */
        conn->inCursor = conn->inStart;
        return '\0';
    }

    *len -= 4;

    if (conn->inCursor + *len > conn->inEnd)
    {
        /*
         * Not enough data in the buffer, we should read more.
         * Reading function will discard already consumed data in the buffer
         * till conn->inBegin. Then we want the message that is partly in the
         * buffer now has been read completely, to avoid extra read/handle
         * cycles. The space needed is 1 byte for message type, 4 bytes for
         * message length and message itself which size is currently in *len.
         * The buffer may already be large enough, in this case the function
         * ensure_in_buffer_capacity() will immediately return
         */
        ensure_in_buffer_capacity(5 + (size_t)*len, conn);
        conn->inCursor = conn->inStart;
        return '\0';
    }

    *msg = conn->inBuffer + conn->inCursor;
    conn->inCursor += *len;
    conn->inStart = conn->inCursor;
    return msgtype;
}

/*
 * Release all Datanode and Coordinator connections
 * back to pool and release occupied memory.
 * Release handles when transaction aborts.
 */
void release_handles(bool force)
{ // #lizard forgives
    bool destroy = false;
    int i;
    int nbytes = 0;
    if (!force)
    {
        if (HandlesInvalidatePending)
        {
            DoInvalidateRemoteHandles();
            return;
        }
#if 0
        /* don't free connection if holding a cluster lock */
        if (cluster_ex_lock_held)
        {
            return;
        }
#endif /*0*/

        if (datanode_count == 0 && coord_count == 0 && slavedatanode_count == 0)
        {
            return;
        }
    }

    /* Free Datanodes handles */
    for (i = 0; i < NumDataNodes; i++)
    {
        PGXCNodeHandle *handle = &dn_handles[i];

        if (handle->sock != NO_SOCKET)
        {
            /*
             * Connections at this point should be completely inactive,
             * otherwise abaandon them. We can not allow not cleaned up
             * connection is returned to pool.
             */
            if (handle->state != DN_CONNECTION_STATE_IDLE ||
                handle->transaction_status != 'I')
            {
                destroy = true;
                elog(DEBUG1, "Connection to Datanode %d has unexpected state %d and will be dropped",
                     handle->nodeoid, handle->state);
            }

#ifdef _PG_REGRESS_
            elog(LOG, "release_handles release a connection with datanode %s"
                      "remote backend PID %d",
                 handle->nodename, (int)handle->backend_pid);
#endif
            pgxc_node_free(handle);
        }
        handle->sendGxidVersion = 0;
        nbytes = pgxc_node_is_data_enqueued(handle);
        if (nbytes)
        {
            elog(PANIC, "Connection to Datanode %s has data %d pending",
                 handle->nodename, nbytes);
        }
    }

    for (i = 0; i < NumSlaveDataNodes; i++)
    {
        PGXCNodeHandle *handle = &sdn_handles[i];

        if (handle->sock != NO_SOCKET)
        {
            /*
             * Connections at this point should be completely inactive,
             * otherwise abaandon them. We can not allow not cleaned up
             * connection is returned to pool.
             */
            if (handle->state != DN_CONNECTION_STATE_IDLE ||
                handle->transaction_status != 'I')
            {
                destroy = true;
                elog(DEBUG1, "Connection to Datanode %d has unexpected state %d and will be dropped",
                     handle->nodeoid, handle->state);
            }

#ifdef _PG_REGRESS_
            elog(LOG, "release_handles release a connection with datanode %s"
                      "remote backend PID %d",
                 handle->nodename, (int)handle->backend_pid);
#endif
            pgxc_node_free(handle);
        }
        handle->sendGxidVersion = 0;
        nbytes = pgxc_node_is_data_enqueued(handle);
        if (nbytes)
        {
            elog(PANIC, "Connection to Datanode %s has data %d pending",
                 handle->nodename, nbytes);
        }
    }

    if (IS_PGXC_COORDINATOR)
    {
        /* Collect Coordinator handles */
        for (i = 0; i < NumCoords; i++)
        {
            PGXCNodeHandle *handle = &co_handles[i];

            if (handle->sock != NO_SOCKET)
            {
                /*
                 * Connections at this point should be completely inactive,
                 * otherwise abandon them. We can not allow not cleaned up
                 * connection is returned to pool.
                 */
                if (handle->state != DN_CONNECTION_STATE_IDLE ||
                    handle->transaction_status != 'I')
                {
                    destroy = true;
                    elog(DEBUG1, "Connection to Coordinator %d has unexpected state %d and will be dropped",
                         handle->nodeoid, handle->state);
                }

#ifdef _PG_REGRESS_
                elog(LOG, "release_handles release a connection with coordinator %s"
                          "remote backend PID %d",
                     handle->nodename, (int)handle->backend_pid);
#endif

                pgxc_node_free(handle);
            }
            handle->sendGxidVersion = 0;
            nbytes = pgxc_node_is_data_enqueued(handle);
            if (nbytes)
            {
                elog(PANIC, "Connection to Datanode %s has data %d pending",
                     handle->nodename, nbytes);
            }
        }
    }

    //destroy = true;
    /* And finally release all the connections on pooler */
    PoolManagerReleaseConnections(destroy);

    datanode_count = 0;
    coord_count = 0;
    slavedatanode_count = 0;
}

/*
 * Check whether there bad connections to remote nodes when abort transactions.
 */
bool validate_handles(void)
{ // #lizard forgives
    int i;
    int ret;

    /* Free Datanodes handles */
    for (i = 0; i < NumDataNodes; i++)
    {
        PGXCNodeHandle *handle = &dn_handles[i];

        if (handle->sock != NO_SOCKET)
        {
            /*
             * Connections at this point should be completely inactive,
             * otherwise abaandon them. We can not allow not cleaned up
             * connection is returned to pool.
             */
            if (DN_CONNECTION_STATE_ERROR_FATAL == handle->state)
            {
                ret = pgxc_check_socket_health(handle->sock, 1, 0, 0);
                if (ret < 0)
                {
                    elog(LOG, "Remote node \"%s\", running with pid %d state:%d is bad",
                         handle->nodename, handle->backend_pid, handle->state);
                    return true;
                }
            }
        }
    }

    for (i = 0; i < NumSlaveDataNodes; i++)
    {
        PGXCNodeHandle *handle = &sdn_handles[i];

        if (handle->sock != NO_SOCKET)
        {
            /*
             * Connections at this point should be completely inactive,
             * otherwise abaandon them. We can not allow not cleaned up
             * connection is returned to pool.
             */
            if (DN_CONNECTION_STATE_ERROR_FATAL == handle->state)
            {
                ret = pgxc_check_socket_health(handle->sock, 1, 0, 0);
                if (ret < 0)
                {
                    elog(LOG, "Remote node \"%s\", running with pid %d state:%d is bad",
                         handle->nodename, handle->backend_pid, handle->state);
                    return true;
                }
            }
        }
    }

    if (IS_PGXC_COORDINATOR)
    {
        /* Collect Coordinator handles */
        for (i = 0; i < NumCoords; i++)
        {
            PGXCNodeHandle *handle = &co_handles[i];

            if (handle->sock != NO_SOCKET)
            {
                /*
                 * Connections at this point should be completely inactive,
                 * otherwise abandon them. We can not allow not cleaned up
                 * connection is returned to pool.
                 */
                if (DN_CONNECTION_STATE_ERROR_FATAL == handle->state)
                {
                    ret = pgxc_check_socket_health(handle->sock, 1, 0, 0);
                    if (ret < 0)
                    {
                        elog(LOG, "Remote node \"%s\", running with pid %d state:%d is bad",
                             handle->nodename, handle->backend_pid, handle->state);
                        return true;
                    }
                }
            }
        }
    }
    return false;
}

void clear_handles(void)
{ // #lizard forgives
    int i;

    if (datanode_count == 0 && coord_count == 0 && slavedatanode_count == 0)
        return;

    /* Free Datanodes handles */
    for (i = 0; i < NumDataNodes; i++)
    {
        PGXCNodeHandle *handle = &dn_handles[i];
        handle->sendGxidVersion = 0;
    }

    for (i = 0; i < NumSlaveDataNodes; i++)
    {
        PGXCNodeHandle *handle = &sdn_handles[i];

        handle->sendGxidVersion = 0;
    }

    if (IS_PGXC_COORDINATOR)
    {
        /* Collect Coordinator handles */
        for (i = 0; i < NumCoords; i++)
        {
            PGXCNodeHandle *handle = &co_handles[i];

            handle->sendGxidVersion = 0;
        }
    }

    return;
}

/*
 * Ensure that the supplied buffer has enough capacity and if not, it's
 * extended to an appropriate size.
 *
 * currbuf is the currently used buffer of currsize. bytes_needed is the
 * minimum size required. We shall return the new buffer, if allocated
 * successfully and set newsize_p to contain the size of the repalloced buffer.
 * If allocation fails, NULL is returned.
 *
 * The function checks for requests beyond MaxAllocSize and throw an error.
 */
static char *
ensure_buffer_capacity(char *currbuf, size_t currsize, size_t bytes_needed, size_t *newsize_p)
{
    char *newbuf;
    Size newsize = (Size)currsize;

    if (((Size)bytes_needed) >= MaxAllocSize)
        ereport(ERROR,
                (ENOSPC,
                 errmsg("out of memory"),
                 errdetail("Cannot enlarge buffer containing %ld bytes by %ld more bytes.",
                           currsize, bytes_needed)));

    if (bytes_needed <= newsize)
    {
        *newsize_p = currsize;
        return currbuf;
    }

    /*
     * The current size of the buffer should never be zero (init_pgxc_handle
     * guarantees that.
     */
    Assert(newsize > 0);

    /*
     * Double the buffer size until we have enough space to hold bytes_needed
     */
    while (bytes_needed > newsize)
        newsize = 2 * newsize;

    /*
     * Clamp to MaxAllocSize in case we went past it.  Note we are assuming
     * here that MaxAllocSize <= INT_MAX/2, else the above loop could
     * overflow.  We will still have newsize >= bytes_needed.
     */
    if (newsize > (int)MaxAllocSize)
        newsize = (int)MaxAllocSize;

    newbuf = repalloc(currbuf, newsize);
    if (newbuf)
    {
        /* repalloc succeeded, set new size and return the buffer */
        *newsize_p = newsize;
        return newbuf;
    }

    /*
     * If we fail to double the buffer, try to repalloc a buffer of the given
     * size, rounded to the next multiple of 8192 and see if that works.
     */
    newsize = bytes_needed;
    newsize = ((bytes_needed / 8192) + 1) * 8192;

    newbuf = repalloc(currbuf, newsize);
    if (newbuf)
    {
        /* repalloc succeeded, set new size and return the buffer */
        *newsize_p = newsize;
        return newbuf;
    }

    /* repalloc failed */
    return NULL;
}

/*
 * Ensure specified amount of data can fit to the incoming buffer and
 * increase it if necessary
 */
int ensure_in_buffer_capacity(size_t bytes_needed, PGXCNodeHandle *handle)
{
    size_t newsize;
    char *newbuf = ensure_buffer_capacity(handle->inBuffer, handle->inSize,
                                          bytes_needed, &newsize);
    if (newbuf)
    {
        handle->inBuffer = newbuf;
        handle->inSize = newsize;
        return 0;
    }
    return EOF;
}

/*
 * Ensure specified amount of data can fit to the outgoing buffer and
 * increase it if necessary
 */
int ensure_out_buffer_capacity(size_t bytes_needed, PGXCNodeHandle *handle)
{ // #lizard forgives
    size_t newsize;
    char *newbuf = ensure_buffer_capacity(handle->outBuffer, handle->outSize,
                                          bytes_needed, &newsize);

    if (newbuf)
    {
        handle->outBuffer = newbuf;
        handle->outSize = newsize;
        return 0;
    }
    return EOF;
}

/*
 * Send specified amount of data from the outgoing buffer over the connection
 */
int send_some(PGXCNodeHandle *handle, int len)
{ // #lizard forgives
    char *ptr = handle->outBuffer;
    int remaining = handle->outEnd;
    int result = 0;

    /* while there's still data to send */
    while (len > 0)
    {
        int sent;

#ifndef WIN32
        sent = send(handle->sock, ptr, len, 0);
#else
        /*
         * Windows can fail on large sends, per KB article Q201213. The failure-point
         * appears to be different in different versions of Windows, but 64k should
         * always be safe.
         */
        sent = send(handle->sock, ptr, Min(len, 65536), 0);
#endif

        if (sent < 0)
        {
            /*
             * Anything except EAGAIN/EWOULDBLOCK/EINTR is trouble. If it's
             * EPIPE or ECONNRESET, assume we've lost the backend connection
             * permanently.
             */
            switch (errno)
            {
#ifdef EAGAIN
            case EAGAIN:
                break;
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
            case EWOULDBLOCK:
                break;
#endif
            case EINTR:
                continue;

            case EPIPE:
#ifdef ECONNRESET
            case ECONNRESET:
#endif
                add_error_message(handle, "server closed the connection unexpectedly\n"
                                          "\tThis probably means the server terminated abnormally\n"
                                          "\tbefore or while processing the request.\n");

                /*
                     * We used to close the socket here, but that's a bad idea
                     * since there might be unread data waiting (typically, a
                     * NOTICE message from the backend telling us it's
                     * committing hara-kiri...).  Leave the socket open until
                     * pqReadData finds no more data can be read.  But abandon
                     * attempt to send data.
                     */
                handle->outEnd = 0;
                return -1;

            default:
                add_error_message(handle, "could not send data to server");
                /* We don't assume it's a fatal error... */
                handle->outEnd = 0;
                return -1;
            }
        }
        else
        {
            ptr += sent;
            len -= sent;
            remaining -= sent;
        }

        if (len > 0)
        {
            struct pollfd pool_fd;
            int poll_ret;

            /*
             * Wait for the socket to become ready again to receive more data.
             * For some cases, especially while writing large sums of data
             * during COPY protocol and when the remote node is not capable of
             * handling data at the same speed, we might otherwise go in a
             * useless tight loop, consuming all available local resources
             *
             * Use a small timeout of 1s to avoid infinite wait
             */
            pool_fd.fd = handle->sock;
            pool_fd.events = POLLOUT;

            poll_ret = poll(&pool_fd, 1, 1000);
            if (poll_ret < 0)
            {
                if (errno == EAGAIN || errno == EINTR)
                    continue;
                else
                {
                    add_error_message(handle, "poll failed ");
                    handle->outEnd = 0;
                    return -1;
                }
            }
            else if (poll_ret == 1)
            {
                if (pool_fd.revents & POLLHUP)
                {
                    add_error_message(handle, "remote end disconnected");
                    handle->outEnd = 0;
                    return -1;
                }
            }
        }
    }

    /* shift the remaining contents of the buffer */
    if (remaining > 0)
        memmove(handle->outBuffer, ptr, remaining);
    handle->outEnd = remaining;

    return result;
}

/*
 * Send PARSE message with specified statement down to the Datanode
 */
int pgxc_node_send_parse(PGXCNodeHandle *handle, const char *statement,
                         const char *query, short num_params, Oid *param_types)
{ // #lizard forgives
    /* statement name size (allow NULL) */
    int stmtLen = statement ? strlen(statement) + 1 : 1;
    /* size of query string */
    int strLen = strlen(query) + 1;
    /* total size of parameter type names */
    int paramTypeLen;
    /* message length */
    int msgLen;
    int cnt_params;
#ifdef USE_ASSERT_CHECKING
    size_t old_outEnd = handle->outEnd;
#endif
    Oid inval_oid = InvalidOid;

    /* if there are parameters, param_types should exist */
    Assert(num_params <= 0 || param_types);
    /* 2 bytes for number of parameters, preceding the type names */
    paramTypeLen = 2;
    /* find names of the types of parameters */
    for (cnt_params = 0; cnt_params < num_params; cnt_params++)
    {
        paramTypeLen += 4;
    }

    /* size + stmtLen + strlen + paramTypeLen */
    msgLen = 4 + stmtLen + strLen + paramTypeLen;

    /* msgType + msgLen */
    if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
    {
        add_error_message(handle, "out of memory");
        return EOF;
    }

    handle->outBuffer[handle->outEnd++] = 'P';
    /* size */
    msgLen = htonl(msgLen);
    memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
    handle->outEnd += 4;
    /* statement name */
    if (statement)
    {
        memcpy(handle->outBuffer + handle->outEnd, statement, stmtLen);
        handle->outEnd += stmtLen;
    }
    else
        handle->outBuffer[handle->outEnd++] = '\0';
    /* query */
    memcpy(handle->outBuffer + handle->outEnd, query, strLen);
    handle->outEnd += strLen;
    /* parameter types */
    Assert(sizeof(num_params) == 2);
    *((short *)(handle->outBuffer + handle->outEnd)) = htons(num_params);
    handle->outEnd += sizeof(num_params);
    /*
     * instead of parameter ids we should send parameter names (qualified by
     * schema name if required). The OIDs of types can be different on
     * Datanodes.
     */
    for (cnt_params = 0; cnt_params < num_params; cnt_params++)
    {
        memcpy(handle->outBuffer + handle->outEnd, &inval_oid, 4);
        handle->outEnd += 4;
    }
    Assert(old_outEnd + ntohl(msgLen) + 1 == handle->outEnd);

    return 0;
}

/*
 * Send PLAN message down to the Data node
 */
int pgxc_node_send_plan(PGXCNodeHandle *handle, const char *statement,
                        const char *query, const char *planstr,
                        short num_params, Oid *param_types)
{
    int stmtLen;
    int queryLen;
    int planLen;
    int paramTypeLen;
    int msgLen;
    char **paramTypes = (char **)palloc(sizeof(char *) * num_params);
    int i;
    short tmp_num_params;

    /* Invalid connection state, return error */
    if (handle->state != DN_CONNECTION_STATE_IDLE)
        return EOF;

    /* statement name size (do not allow NULL) */
    stmtLen = strlen(statement) + 1;
    /* source query size (do not allow NULL) */
    queryLen = strlen(query) + 1;
    /* query plan size (do not allow NULL) */
    planLen = strlen(planstr) + 1;
    /* 2 bytes for number of parameters, preceding the type names */
    paramTypeLen = 2;
    /* find names of the types of parameters */
    for (i = 0; i < num_params; i++)
    {
        paramTypes[i] = format_type_be(param_types[i]);
        paramTypeLen += strlen(paramTypes[i]) + 1;
    }
    /* size + pnameLen + queryLen + parameters */
    msgLen = 4 + queryLen + stmtLen + planLen + paramTypeLen;

    /* msgType + msgLen */
    if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
    {
        add_error_message(handle, "out of memory");
        return EOF;
    }

    handle->outBuffer[handle->outEnd++] = 'p';
    /* size */
    msgLen = htonl(msgLen);
    memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
    handle->outEnd += 4;
    /* statement name */
    memcpy(handle->outBuffer + handle->outEnd, statement, stmtLen);
    handle->outEnd += stmtLen;
    /* source query */
    memcpy(handle->outBuffer + handle->outEnd, query, queryLen);
    handle->outEnd += queryLen;
    /* query plan */
    memcpy(handle->outBuffer + handle->outEnd, planstr, planLen);
    handle->outEnd += planLen;
    /* parameter types */
    tmp_num_params = htons(num_params);
    memcpy(handle->outBuffer + handle->outEnd, &tmp_num_params, sizeof(tmp_num_params));
    handle->outEnd += sizeof(tmp_num_params);
    /*
     * instead of parameter ids we should send parameter names (qualified by
     * schema name if required). The OIDs of types can be different on
     * datanodes.
     */
    for (i = 0; i < num_params; i++)
    {
        int plen = strlen(paramTypes[i]) + 1;
        memcpy(handle->outBuffer + handle->outEnd, paramTypes[i], plen);
        handle->outEnd += plen;
        pfree(paramTypes[i]);
    }
    pfree(paramTypes);

    handle->last_command = 'a';

    handle->in_extended_query = true;
    return 0;
}

/*
 * Send BIND message down to the Datanode
 */
int pgxc_node_send_bind(PGXCNodeHandle *handle, const char *portal,
                        const char *statement, int paramlen, char *params)
{ // #lizard forgives
    int pnameLen;
    int stmtLen;
    int paramCodeLen;
    int paramValueLen;
    int paramOutLen;
    int msgLen;

    /* Invalid connection state, return error */
    if (handle->state != DN_CONNECTION_STATE_IDLE)
        return EOF;

    /* portal name size (allow NULL) */
    pnameLen = portal ? strlen(portal) + 1 : 1;
    /* statement name size (allow NULL) */
    stmtLen = statement ? strlen(statement) + 1 : 1;
    /* size of parameter codes array (always empty for now) */
    paramCodeLen = 2;
    /* size of parameter values array, 2 if no params */
    paramValueLen = paramlen ? paramlen : 2;
    /* size of output parameter codes array (always empty for now) */
    paramOutLen = 2;
    /* size + pnameLen + stmtLen + parameters */
    msgLen = 4 + pnameLen + stmtLen + paramCodeLen + paramValueLen + paramOutLen;

    /* msgType + msgLen */
    if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
    {
        add_error_message(handle, "out of memory");
        return EOF;
    }

    handle->outBuffer[handle->outEnd++] = 'B';
    /* size */
    msgLen = htonl(msgLen);
    memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
    handle->outEnd += 4;
    /* portal name */
    if (portal)
    {
        memcpy(handle->outBuffer + handle->outEnd, portal, pnameLen);
        handle->outEnd += pnameLen;
    }
    else
        handle->outBuffer[handle->outEnd++] = '\0';
    /* statement name */
    if (statement)
    {
        memcpy(handle->outBuffer + handle->outEnd, statement, stmtLen);
        handle->outEnd += stmtLen;
    }
    else
        handle->outBuffer[handle->outEnd++] = '\0';
    /* parameter codes (none) */
    handle->outBuffer[handle->outEnd++] = 0;
    handle->outBuffer[handle->outEnd++] = 0;
    /* parameter values */
    if (paramlen)
    {
        memcpy(handle->outBuffer + handle->outEnd, params, paramlen);
        handle->outEnd += paramlen;
    }
    else
    {
        handle->outBuffer[handle->outEnd++] = 0;
        handle->outBuffer[handle->outEnd++] = 0;
    }
    /* output parameter codes (none) */
    handle->outBuffer[handle->outEnd++] = 0;
    handle->outBuffer[handle->outEnd++] = 0;

    handle->in_extended_query = true;
    return 0;
}

/*
 * Send DESCRIBE message (portal or statement) down to the Datanode
 */
int pgxc_node_send_describe(PGXCNodeHandle *handle, bool is_statement,
                            const char *name)
{
    int nameLen;
    int msgLen;

    /* Invalid connection state, return error */
    if (handle->state != DN_CONNECTION_STATE_IDLE)
        return EOF;

    /* statement or portal name size (allow NULL) */
    nameLen = name ? strlen(name) + 1 : 1;

    /* size + statement/portal + name */
    msgLen = 4 + 1 + nameLen;

    /* msgType + msgLen */
    if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
    {
        add_error_message(handle, "out of memory");
        return EOF;
    }

    handle->outBuffer[handle->outEnd++] = 'D';
    /* size */
    msgLen = htonl(msgLen);
    memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
    handle->outEnd += 4;
    /* statement/portal flag */
    handle->outBuffer[handle->outEnd++] = is_statement ? 'S' : 'P';
    /* object name */
    if (name)
    {
        memcpy(handle->outBuffer + handle->outEnd, name, nameLen);
        handle->outEnd += nameLen;
    }
    else
        handle->outBuffer[handle->outEnd++] = '\0';

    handle->in_extended_query = true;
    return 0;
}

/*
 * Send CLOSE message (portal or statement) down to the Datanode
 */
int pgxc_node_send_close(PGXCNodeHandle *handle, bool is_statement,
                         const char *name)
{
    /* statement or portal name size (allow NULL) */
    int nameLen = name ? strlen(name) + 1 : 1;

    /* size + statement/portal + name */
    int msgLen = 4 + 1 + nameLen;

    /* msgType + msgLen */
    if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
    {
        add_error_message(handle, "out of memory");
        return EOF;
    }

    handle->outBuffer[handle->outEnd++] = 'C';
    /* size */
    msgLen = htonl(msgLen);
    memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
    handle->outEnd += 4;
    /* statement/portal flag */
    handle->outBuffer[handle->outEnd++] = is_statement ? 'S' : 'P';
    /* object name */
    if (name)
    {
        memcpy(handle->outBuffer + handle->outEnd, name, nameLen);
        handle->outEnd += nameLen;
    }
    else
        handle->outBuffer[handle->outEnd++] = '\0';

    handle->in_extended_query = true;
    return 0;
}

/*
 * Send EXECUTE message down to the Datanode
 */
int pgxc_node_send_execute(PGXCNodeHandle *handle, const char *portal, int fetch)
{
    /* portal name size (allow NULL) */
    int pnameLen = portal ? strlen(portal) + 1 : 1;

    /* size + pnameLen + fetchLen */
    int msgLen = 4 + pnameLen + 4;

    /* msgType + msgLen */
    if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
    {
        add_error_message(handle, "out of memory");
        return EOF;
    }

    handle->outBuffer[handle->outEnd++] = 'E';
    /* size */
    msgLen = htonl(msgLen);
    memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
    handle->outEnd += 4;
    /* portal name */
    if (portal)
    {
        memcpy(handle->outBuffer + handle->outEnd, portal, pnameLen);
        handle->outEnd += pnameLen;
    }
    else
        handle->outBuffer[handle->outEnd++] = '\0';

    /* fetch */
    fetch = htonl(fetch);
    memcpy(handle->outBuffer + handle->outEnd, &fetch, 4);
    handle->outEnd += 4;

    PGXCNodeSetConnectionState(handle, DN_CONNECTION_STATE_QUERY);

    handle->in_extended_query = true;
    return 0;
}

/*
 * Send FLUSH message down to the Datanode
 */
int pgxc_node_send_flush(PGXCNodeHandle *handle)
{
    /* size */
    int msgLen = 4;

    /* msgType + msgLen */
    if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
    {
        add_error_message(handle, "out of memory");
        return EOF;
    }

    handle->outBuffer[handle->outEnd++] = 'H';
    /* size */
    msgLen = htonl(msgLen);
    memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
    handle->outEnd += 4;

    handle->in_extended_query = true;
    return pgxc_node_flush(handle);
}

/*
 * Send SYNC message down to the Datanode
 */
int pgxc_node_send_sync(PGXCNodeHandle *handle)
{
    /* size */
    int msgLen = 4;

    /* msgType + msgLen */
    if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
    {
        add_error_message(handle, "out of memory");
        return EOF;
    }

    handle->outBuffer[handle->outEnd++] = 'S';
    /* size */
    msgLen = htonl(msgLen);
    memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
    handle->outEnd += 4;

    handle->in_extended_query = false;
    handle->needSync = false;

    return pgxc_node_flush(handle);
}

/*
 * Send series of Extended Query protocol messages to the data node
 */
int pgxc_node_send_query_extended(PGXCNodeHandle *handle, const char *query,
                                  const char *statement, const char *portal,
                                  int num_params, Oid *param_types,
                                  int paramlen, char *params,
                                  bool send_describe, int fetch_size)
{ // #lizard forgives
    /* NULL query indicates already prepared statement */
    if (query)
        if (pgxc_node_send_parse(handle, statement, query, num_params, param_types))
            return EOF;
    if (pgxc_node_send_bind(handle, portal, statement, paramlen, params))
        return EOF;
    if (send_describe)
        if (pgxc_node_send_describe(handle, false, portal))
            return EOF;
    if (fetch_size >= 0)
        if (pgxc_node_send_execute(handle, portal, fetch_size))
            return EOF;
    if (pgxc_node_send_flush(handle))
        return EOF;

    return 0;
}

/*
 * This method won't return until connection buffer is empty or error occurs
 * To ensure all data are on the wire before waiting for response
 */
int pgxc_node_flush(PGXCNodeHandle *handle)
{
    while (handle->outEnd)
    {
        if (send_some(handle, handle->outEnd) < 0)
        {
            int32 error = errno;
            elog(LOG, "pgxc_node_flush data to datanode:%u fd:%d failed for %s", handle->nodeoid, handle->sock, strerror(errno));
            add_error_message(handle, "failed to send data to datanode");
#if 0
            /*
             * before returning, also update the shared health
             * status field to indicate that this node could be
             * possibly unavailable.
             *
             * Note that this error could be due to a stale handle
             * and it's possible that another backend might have
             * already updated the health status OR the node
             * might have already come back since the last disruption
             */
            PoolPingNodeRecheck(handle->nodeoid);
#endif

            errno = error;
            return EOF;
        }
    }
    return 0;
}

/*
 * Has the data node sent Ready For Query
 */
static bool
is_data_node_ready(PGXCNodeHandle *conn)
{
    char *msg;
    int msg_len;
    char msg_type;
    size_t data_len = 0;

    for (;;)
    {
        /* No data available, exit */
        if (!HAS_MESSAGE_BUFFERED(conn))
        {
            return false;
        }

        /*
        * If the length of one data row is longger than MaxAllocSize>>1,
        * it seems there was something wrong,
        * to close this connection should be a better way to save reading loop and avoid overload read buffer.
        */
        data_len = ntohl(*((uint32_t *)((conn)->inBuffer + (conn)->inCursor + 1)));
        if (data_len >= (MaxAllocSize >> 1))
        {
            elog(LOG, "size:%lu too big in buffer, close socket on node:%u now", data_len, conn->nodeoid);
            close(conn->sock);
            conn->sock = NO_SOCKET;
            return true;
        }

        msg_type = get_message(conn, &msg_len, &msg);
        if ('Z' == msg_type)
        {
            /*
             * Return result depends on previous connection state.
             * If it was PORTAL_SUSPENDED Coordinator want to send down
             * another EXECUTE to fetch more rows, otherwise it is done
             * with the connection
             */
            conn->last_command = msg_type;
            conn->transaction_status = msg[0];
            PGXCNodeSetConnectionState(conn, DN_CONNECTION_STATE_IDLE);
            conn->combiner = NULL;
            return true;
        }
    }
    /* never happen, but keep compiler quiet */
    return false;
}
/*
 * This method won't return until network buffer is empty or error occurs
 * To ensure all data in network buffers is read and wasted
 */
void pgxc_node_flush_read(PGXCNodeHandle *handle)
{ // #lizard forgives
    bool is_ready = false;
    int read_result;

    if (handle == NULL)
    {
        return;
    }

    while (true)
    {
#if PGXC_CANCEL_DELAY > 0
        pg_usleep(PGXC_CANCEL_DELAY * 1000);
#endif
        /* consume all data */
        while (HAS_MESSAGE_BUFFERED(handle))
        {
            is_ready = is_data_node_ready(handle);
        }

        if (pgxc_node_is_data_enqueued(handle) != 0)
        {
            pgxc_node_read_data(handle, true);
            continue;
        }

        /* break, only if the connection is ready for query. */
        if (is_ready)
        {
            elog(LOG, "pgxc_node_flush_read node:%s ready for query.", handle->nodename);
            break;
        }

        /* break, only if the connection is broken. */
        read_result = pgxc_node_read_data(handle, true);
        if (read_result <= 0)
        {
            elog(LOG, "pgxc_node_flush_read node:%s read failure.", handle->nodename);
            break;
        }
    }
}

/*
 * Send specified statement down to the PGXC node
 */
static int
pgxc_node_send_query_internal(PGXCNodeHandle *handle, const char *query,
                              bool rollback)
{
    int strLen;
    int msgLen;
    /*
     * Its appropriate to send ROLLBACK commands on a failed connection, but
     * for everything else we expect the connection to be in a sane state
     */

#ifdef PATCH_ENABLE_DISTRIBUTED_TRANSACTION
    if (enable_timestamp_debug_print)
    {
        elog(LOG, "pgxc_node_send_query to %s(%s:%d), query %s, remote backend_pid:%d",
             handle->nodename,
             handle->nodehost, handle->nodeport,
             query,
             handle->backend_pid);
    }
#endif

    if (enable_log_remote_query)
    {
        ereport(LOG, ((errmsg("send query to remote %s: %s", handle->nodename, query)),
                      (errdetail("remote server is %s:%d", handle->nodehost, handle->nodeport))));
    }
    if ((handle->state != DN_CONNECTION_STATE_IDLE) &&
        !(handle->state == DN_CONNECTION_STATE_ERROR_FATAL && rollback))
    {
        elog(LOG, "pgxc_node_send_query_internal datanode:%u invalid status:%d, no need to send data, return NOW", handle->nodeoid, handle->state);
        return EOF;
    }

    strLen = strlen(query) + 1;
    /* size + strlen */
    msgLen = 4 + strLen;

    /* msgType + msgLen */
    if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
    {
        add_error_message(handle, "out of memory");
        return EOF;
    }

    handle->outBuffer[handle->outEnd++] = 'Q';
    msgLen = htonl(msgLen);
    memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
    handle->outEnd += 4;
    memcpy(handle->outBuffer + handle->outEnd, query, strLen);
    handle->outEnd += strLen;

    PGXCNodeSetConnectionState(handle, DN_CONNECTION_STATE_QUERY);

    handle->in_extended_query = false;
    return pgxc_node_flush(handle);
}

int pgxc_node_send_rollback(PGXCNodeHandle *handle, const char *query)
{
    return pgxc_node_send_query_internal(handle, query, true);
}

int pgxc_node_send_query(PGXCNodeHandle *handle, const char *query)
{
    return pgxc_node_send_query_internal(handle, query, false);
}

/*
 * Send the GID down to the PGXC node
 */
int pgxc_node_send_gid(PGXCNodeHandle *handle, char *gid)
{
    int msglen = 4 + strlen(gid) + 1;

    /* Invalid connection state, return error */
    if (handle->state != DN_CONNECTION_STATE_IDLE)
    {
        elog(LOG, "pgxc_node_send_gid datanode:%u invalid stauts:%d, no need to send data, return NOW", handle->nodeoid, handle->state);
        return EOF;
    }

    /* msgType + msgLen */
    if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
    {
        add_error_message(handle, "out of memory");
        return EOF;
    }
    handle->outBuffer[handle->outEnd++] = 'G';
    msglen = htonl(msglen);
    memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
    handle->outEnd += 4;
    memcpy(handle->outBuffer + handle->outEnd, gid, strlen(gid) + 1);
    handle->outEnd += (strlen(gid) + 1);

    return 0;
}

#ifdef PATCH_ENABLE_DISTRIBUTED_TRANSACTION
/*
 * Send the startnode down to the PGXC node
 */
int pgxc_node_send_starter(PGXCNodeHandle *handle, char *startnode)
{
    int msglen = 4 + strnlen(startnode, NAMEDATALEN) + 1;

    /* Invalid connection state, return error */
    if (handle->state != DN_CONNECTION_STATE_IDLE)
    {
        elog(LOG, "pgxc_node_send_starter datanode:%u invalid stauts:%d, no need to send data, return NOW", handle->nodeoid, handle->state);
        return EOF;
    }

    /* msgType + msgLen */
    if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
    {
        add_error_message(handle, "in function pgxc_node_send_starter, error: out of memory");
        return EOF;
    }
    elog(DEBUG5, "send startnode %s", startnode);
    handle->outBuffer[handle->outEnd++] = 'e';
    msglen = htonl(msglen);
    memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
    handle->outEnd += 4;
    memcpy(handle->outBuffer + handle->outEnd, startnode, strnlen(startnode, NAMEDATALEN) + 1);
    handle->outEnd += (strnlen(startnode, NAMEDATALEN) + 1);

    return 0;
}

int pgxc_node_send_startxid(PGXCNodeHandle *handle, GlobalTransactionId transactionid)
{
    int msglen = 4 + sizeof(GlobalTransactionId);
    int i32;

    /* Invalid connection state, return error */
    if (handle->state != DN_CONNECTION_STATE_IDLE)
    {
        elog(LOG, "pgxc_node_send_startxid datanode:%u invalid stauts:%d, no need to send data, return NOW", handle->nodeoid, handle->state);
        return EOF;
    }

    /* msgType + msgLen */
    if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
    {
        add_error_message(handle, "in function pgxc_node_send_startxid, error: out of memory");
        return EOF;
    }
    elog(DEBUG5, "send transactionid %u", transactionid);
    handle->outBuffer[handle->outEnd++] = 'x';
    msglen = htonl(msglen);
    memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
    handle->outEnd += 4;
    i32 = htonl(transactionid);
    memcpy(handle->outBuffer + handle->outEnd, &i32, sizeof(GlobalTransactionId));
    handle->outEnd += sizeof(GlobalTransactionId);

    return 0;
}

/*
 * Send the partnodes down to the PGXC node
 */
int pgxc_node_send_partnodes(PGXCNodeHandle *handle, char *partnodes)
{
    int msglen = 4 + strlen(partnodes) + 1;

    /* Invalid connection state, return error */
    if (handle->state != DN_CONNECTION_STATE_IDLE)
    {
        elog(LOG, "pgxc_node_send_partnodes datanode:%u invalid stauts:%d, no need to send data, return NOW", handle->nodeoid, handle->state);
        return EOF;
    }

    /* msgType + msgLen */
    if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
    {
        add_error_message(handle, "in function pgxc_node_send_partnodes, error: out of memory");
        return EOF;
    }
    elog(DEBUG5, "send partnodes %s", partnodes);
    handle->outBuffer[handle->outEnd++] = 'R';
    msglen = htonl(msglen);
    memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
    handle->outEnd += 4;
    memcpy(handle->outBuffer + handle->outEnd, partnodes, strlen(partnodes) + 1);
    handle->outEnd += (strlen(partnodes) + 1);

    return 0;
}

/*
 * when execute in pg_clean, we allowed to truncate the exists 2pc file
 */
int pgxc_node_send_clean(PGXCNodeHandle *handle)
{
    int msglen = 4;

    /* Invalid connection state, return error */
    if (handle->state != DN_CONNECTION_STATE_IDLE)
    {
        elog(LOG, "pgxc_node_send_clean datanode:%u invalid stauts:%d, no need to send data, return NOW", handle->nodeoid, handle->state);
        return EOF;
    }

    /* msgType + msgLen */
    if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
    {
        add_error_message(handle, "in function pgxc_node_send_clean, error: out of memory");
        return EOF;
    }
    handle->outBuffer[handle->outEnd++] = 'n';
    msglen = htonl(msglen);
    memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
    handle->outEnd += 4;

    return 0;
}

int pgxc_node_send_readonly(PGXCNodeHandle *handle)
{
    int msglen = 4;

    /* Invalid connection state, return error */
    if (handle->state != DN_CONNECTION_STATE_IDLE)
    {
        elog(LOG, "pgxc_node_send_readonly datanode:%u invalid stauts:%d, no need to send data, return NOW", handle->nodeoid, handle->state);
        return EOF;
    }

    /* msgType + msgLen */
    if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
    {
        add_error_message(handle, "in function pgxc_node_send_clean, error: out of memory");
        return EOF;
    }
    handle->outBuffer[handle->outEnd++] = 'r';
    msglen = htonl(msglen);
    memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
    handle->outEnd += 4;

    return 0;
}

int pgxc_node_send_after_prepare(PGXCNodeHandle *handle)
{
    int msglen = 4;

    /* Invalid connection state, return error */
    if (handle->state != DN_CONNECTION_STATE_IDLE)
    {
        elog(LOG, "pgxc_node_send_after_prepare datanode:%u invalid stauts:%d, no need to send data, return NOW", handle->nodeoid, handle->state);
        return EOF;
    }

    /* msgType + msgLen */
    if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
    {
        add_error_message(handle, "in function pgxc_node_send_clean, error: out of memory");
        return EOF;
    }
    handle->outBuffer[handle->outEnd++] = 'A';
    msglen = htonl(msglen);
    memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
    handle->outEnd += 4;

    return 0;
}

/*
 * Send the GXID down to the PGXC node
 */
int pgxc_node_send_gxid(PGXCNodeHandle *handle, char *gxid)
{ // #lizard forgives
    int msglen = 4 + strlen(gxid) + 1;

    /* Invalid connection state, return error */
    if (handle->state != DN_CONNECTION_STATE_IDLE)
    {
        elog(LOG, "pgxc_node_send_gxid datanode:%u invalid stauts:%d, no need to send data, return NOW", handle->nodeoid, handle->state);
        return EOF;
    }

    /* msgType + msgLen */
    if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
    {
        add_error_message(handle, "in function pgxc_node_send_gxid, error: out of memory");
        return EOF;
    }
    elog(DEBUG5, "send gxid %s", gxid);
    handle->outBuffer[handle->outEnd++] = 'g';
    msglen = htonl(msglen);
    memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
    handle->outEnd += 4;
    memcpy(handle->outBuffer + handle->outEnd, gxid, strlen(gxid) + 1);
    handle->outEnd += (strlen(gxid) + 1);

    return 0;
}

/*
 * Send the Command ID down to the PGXC node
 */
int pgxc_node_send_cmd_id(PGXCNodeHandle *handle, CommandId cid)
{
    int msglen = CMD_ID_MSG_LEN;
    int i32;

    /* No need to send command ID if its sending flag is not enabled */
    if (!IsSendCommandId())
    {
        return 0;
    }

    /* Invalid connection state, return error */
    if (handle->state != DN_CONNECTION_STATE_IDLE)
    {
        elog(LOG, "pgxc_node_send_cmd_id datanode:%u invalid stauts:%d, no need to send data, return NOW", handle->nodeoid, handle->state);
        return EOF;
    }

    /* msgType + msgLen */
    if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
    {
        add_error_message(handle, "out of memory");
        return EOF;
    }

    handle->outBuffer[handle->outEnd++] = 'M';
    msglen = htonl(msglen);
    memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
    handle->outEnd += 4;
    i32 = htonl(cid);
    memcpy(handle->outBuffer + handle->outEnd, &i32, 4);
    handle->outEnd += 4;

    return 0;
}

/*
 * Send the snapshot down to the PGXC node
 */
int pgxc_node_send_snapshot(PGXCNodeHandle *handle, Snapshot snapshot)
{ // #lizard forgives
    int msglen PG_USED_FOR_ASSERTS_ONLY;

    /* Invalid connection state, return error */
    if (handle->state != DN_CONNECTION_STATE_IDLE)
    {
        elog(LOG, "pgxc_node_send_snapshot datanode:%u invalid stauts:%d, no need to send data, return NOW", handle->nodeoid, handle->state);
        return EOF;
    }

#ifndef POLARX_TODO
    if (snapshot->local && g_set_global_snapshot)
    {
        elog(DEBUG5, "don't send local snapshot");
        return 0;
    }
#endif
    msglen = 12; /* 4 bytes for msglen and 8 bytes for timestamp (int64) */
    /* msgType + msgLen */
    if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
    {
        add_error_message(handle, "out of memory");
        return EOF;
    }

    handle->outBuffer[handle->outEnd++] = 's';
    msglen = htonl(msglen);
    memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
    handle->outEnd += 4;
    if (enable_distri_print)
    {
        elog(LOG, "send snapshot start_ts" INT64_FORMAT " xid %d.", snapshot->snapshotcsn, GetTopTransactionIdIfAny());
    }
#ifndef POLARX_TODO
    if (snapshot->local)
    {
        GlobalTimestamp start_ts = LocalCommitTimestamp;
        memcpy(handle->outBuffer + handle->outEnd, &start_ts, sizeof(GlobalTimestamp));
    }
    else
    {
        memcpy(handle->outBuffer + handle->outEnd, &snapshot->start_ts, sizeof(GlobalTimestamp));
    }
    handle->outEnd += sizeof(GlobalTimestamp);
#endif /* POLARX_TODO */
    return 0;
}

/*
 * Send the timestamp down to the PGXC node
 */
int pgxc_node_send_prefinish_timestamp(PGXCNodeHandle *handle, GlobalTimestamp timestamp)
{
    int msglen = 12; /* 4 bytes for msglen and 8 bytes for timestamp (int64) */
    uint32 n32;
    int64 i = (int64)timestamp;

    /* Invalid connection state, return error */
    if (handle->state != DN_CONNECTION_STATE_IDLE)
    {
        elog(LOG, "pgxc_node_send_prefinish_timestamp datanode:%u invalid stauts:%d, no need to send data, return NOW", handle->nodeoid, handle->state);
        return EOF;
    }

    elog(DEBUG5, "send prefinish timestamp " INT64_FORMAT, timestamp);
    /* msgType + msgLen */
    if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
    {
        add_error_message(handle, "out of memory");
        return EOF;
    }
    handle->outBuffer[handle->outEnd++] = 'W';
    msglen = htonl(msglen);
    memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
    handle->outEnd += 4;

    /* High order half first */
#ifdef INT64_IS_BUSTED
    /* don't try a right shift of 32 on a 32-bit word */
    n32 = (i < 0) ? -1 : 0;
#else
    n32 = (uint32)(i >> 32);
#endif
    n32 = htonl(n32);
    memcpy(handle->outBuffer + handle->outEnd, &n32, 4);
    handle->outEnd += 4;

    /* Now the low order half */
    n32 = (uint32)i;
    n32 = htonl(n32);
    memcpy(handle->outBuffer + handle->outEnd, &n32, 4);
    handle->outEnd += 4;

    PGXCNodeSetConnectionState(handle, DN_CONNECTION_STATE_QUERY);
    return 0;
}

/*
 * Send the timestamp down to the PGXC node
 */
int pgxc_node_send_prepare_timestamp(PGXCNodeHandle *handle, GlobalTimestamp timestamp)
{                    // #lizard forgives
    int msglen = 12; /* 4 bytes for msglen and 8 bytes for timestamp (int64) */
    uint32 n32;
    int64 i = (int64)timestamp;
    if (enable_timestamp_debug_print)
    {
        elog(LOG, "pgxc_node_send_prepare_timestamp remote backend_pid:%d, host:%s, nodename:%s, timestamp:" INT64_FORMAT,
             handle->backend_pid, handle->nodehost, handle->nodename, timestamp);
    }

    if (!GlobalTimestampIsValid(timestamp))
    {
        elog(ERROR, "prepare timestamp is not valid for sending");
    }
    if (enable_log_remote_query)
    {
        ereport(LOG, (errmsg("send prepare_ts " LOGICALTIME_FORMAT " to server %s",
                             LOGICALTIME_STRING(timestamp), handle->nodename)));
    }

    /* Invalid connection state, return error */
    if (handle->state != DN_CONNECTION_STATE_IDLE)
    {
        elog(LOG, "pgxc_node_send_prepare_timestamp datanode:%u invalid stauts:%d, no need to send data, return NOW", handle->nodeoid, handle->state);
        return EOF;
    }

    /* msgType + msgLen */
    if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
    {
        add_error_message(handle, "out of memory");
        return EOF;
    }
    handle->outBuffer[handle->outEnd++] = 'Z';
    msglen = htonl(msglen);
    memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
    handle->outEnd += 4;

    /* High order half first */
#ifdef INT64_IS_BUSTED
    /* don't try a right shift of 32 on a 32-bit word */
    n32 = (i < 0) ? -1 : 0;
#else
    n32 = (uint32)(i >> 32);
#endif
    n32 = htonl(n32);
    memcpy(handle->outBuffer + handle->outEnd, &n32, 4);
    handle->outEnd += 4;

    /* Now the low order half */
    n32 = (uint32)i;
    n32 = htonl(n32);
    memcpy(handle->outBuffer + handle->outEnd, &n32, 4);
    handle->outEnd += 4;

    return 0;
}

/*
 * Send the timestamp down to the PGXC node
 */
int pgxc_node_send_global_timestamp(PGXCNodeHandle *handle, GlobalTimestamp timestamp)
{
    int msglen = 12; /* 4 bytes for msglen and 8 bytes for timestamp (int64) */
    uint32 n32;
    int64 i = (int64)timestamp;

    if (enable_log_remote_query)
    {
        elog(LOG, "send global timestamp " LOGICALTIME_FORMAT " to server %s",
             LOGICALTIME_STRING(timestamp), handle->nodename);
    }
    if (!GlobalTimestampIsValid(timestamp))
        elog(ERROR, "timestamp is not valid for sending");
    /* Invalid connection state, return error */
    if (handle->state != DN_CONNECTION_STATE_IDLE)
    {
        elog(LOG, "pgxc_node_send_global_timestamp datanode:%u invalid stauts:%d, no need to send data, return NOW", handle->nodeoid, handle->state);
        return EOF;
    }

    /* msgType + msgLen */
    if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
    {
        add_error_message(handle, "out of memory");
        return EOF;
    }
    handle->outBuffer[handle->outEnd++] = 'T';
    msglen = htonl(msglen);
    memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
    handle->outEnd += 4;

    /* High order half first */
#ifdef INT64_IS_BUSTED
    /* don't try a right shift of 32 on a 32-bit word */
    n32 = (i < 0) ? -1 : 0;
#else
    n32 = (uint32)(i >> 32);
#endif
    n32 = htonl(n32);
    memcpy(handle->outBuffer + handle->outEnd, &n32, 4);
    handle->outEnd += 4;

    /* Now the low order half */
    n32 = (uint32)i;
    n32 = htonl(n32);
    memcpy(handle->outBuffer + handle->outEnd, &n32, 4);
    handle->outEnd += 4;

    return 0;
}

/*
 * Send the timestamp down to the PGXC node
 */
int pgxc_node_send_timestamp(PGXCNodeHandle *handle, GlobalTimestamp timestamp)
{
    int msglen = 12; /* 4 bytes for msglen and 8 bytes for timestamp (int64) */
    uint32 n32;
    int64 i = (int64)timestamp;

    /* Invalid connection state, return error */
    if (handle->state != DN_CONNECTION_STATE_IDLE)
    {
        elog(WARNING, "pgxc_node_send_timestamp datanode:%u invalid stauts:%d, no need to send data, return NOW", handle->nodeoid, handle->state);
        return EOF;
    }
    if (enable_log_remote_query)
    {
        elog(LOG, "send start timestamp " UINT64_FORMAT LOGICALTIME_FORMAT " to server %s, backend_pid:%d",
             timestamp, LOGICALTIME_STRING(timestamp), handle->nodename, handle->backend_pid);
    }

    /* msgType + msgLen */
    if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
    {
        add_error_message(handle, "out of memory");
        return EOF;
    }
    handle->outBuffer[handle->outEnd++] = 't';
    msglen = htonl(msglen);
    memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
    handle->outEnd += 4;

    /* High order half first */
#ifdef INT64_IS_BUSTED
    /* don't try a right shift of 32 on a 32-bit word */
    n32 = (i < 0) ? -1 : 0;
#else
    n32 = (uint32)(i >> 32);
#endif
    n32 = htonl(n32);
    memcpy(handle->outBuffer + handle->outEnd, &n32, 4);
    handle->outEnd += 4;

    /* Now the low order half */
    n32 = (uint32)i;
    n32 = htonl(n32);
    memcpy(handle->outBuffer + handle->outEnd, &n32, 4);
    handle->outEnd += 4;

    return 0;
}
#endif
/*
 * Add another message to the list of errors to be returned back to the client
 * at the convenient time
 */
void add_error_message(PGXCNodeHandle *handle, const char *message)
{
    elog(DEBUG1, "Remote node \"%s\", running with pid %d returned an error: %s",
         handle->nodename, handle->backend_pid, message);

    handle->transaction_status = 'E';
    if (handle->error[0] && message)
    {
        int32 offset = 0;
#ifdef _PG_REGRESS_
        elog(LOG, "add_error_message node:%s, running with pid %d non first time error before append: %s, error ptr:%lx",
             handle->nodename, handle->backend_pid, message, (uint64)handle->error);
#endif

        offset = strnlen(handle->error, MAX_ERROR_MSG_LENGTH);
        snprintf(handle->error + offset, MAX_ERROR_MSG_LENGTH - offset, "%s", message);

#ifdef _PG_REGRESS_
        elog(LOG, "add_error_message node:%s, running with pid %d non first time after append error: %s, ptr:%lx",
             handle->nodename, handle->backend_pid, handle->error, (uint64)handle->error);
#endif
    }
    else
    {
        snprintf(handle->error, MAX_ERROR_MSG_LENGTH, "%s", message);

#ifdef _PG_REGRESS_
        elog(LOG, "add_error_message node:%s, running with pid %d first time error: %s, ptr:%lx",
             handle->nodename, handle->backend_pid, handle->error, (uint64)handle->error);
#endif
    }
}

static int load_balancer = 0;
/*
 * Get one of the specified nodes to query replicated data source.
 * If session already owns one or more  of the requested connection,
 * the function returns existing one to avoid contacting pooler.
 * Performs basic load balancing.
 */
PGXCNodeHandle *
get_any_handle(List *datanodelist)
{ // #lizard forgives
    ListCell *lc1;
    int i, node;

    /* sanity check */
    Assert(list_length(datanodelist) > 0);

    if (0 == list_length(datanodelist))
    {
        ereport(PANIC,
                (errcode(ERRCODE_QUERY_CANCELED),
                 errmsg("Invalid NULL node list")));
    }

    if (HandlesInvalidatePending)
        if (DoInvalidateRemoteHandles())
            ereport(ERROR,
                    (errcode(ERRCODE_QUERY_CANCELED),
                     errmsg("canceling transaction due to cluster configuration reset by administrator command")));

    if (HandlesRefreshPending)
        if (DoRefreshRemoteHandles())
            ereport(ERROR,
                    (errcode(ERRCODE_QUERY_CANCELED),
                     errmsg("canceling transaction due to cluster configuration reset by administrator command")));

    /* loop through local datanode handles */
    for (i = 0, node = load_balancer; i < NumDataNodes; i++, node++)
    {
        /* At the moment node is an index in the array, and we may need to wrap it */
        if (node >= NumDataNodes)
        {
            node -= NumDataNodes;
        }

        /* See if handle is already used */
        if (dn_handles[node].sock != NO_SOCKET)
        {
            foreach (lc1, datanodelist)
            {
                if (lfirst_int(lc1) == node)
                {
                    /*
                     * The node is in the list of requested nodes,
                     * set load_balancer for next time and return the handle
                     */
                    load_balancer = node + 1;
                    return &dn_handles[node];
                }
            }
        }
    }

    /*
     * None of requested nodes is in use, need to get one from the pool.
     * Choose one.
     */
    for (i = 0, node = load_balancer; i < NumDataNodes; i++, node++)
    {
        /* At the moment node is an index in the array, and we may need to wrap it */
        if (node >= NumDataNodes)
        {
            node -= NumDataNodes;
        }
        /* Look only at empty slots, we have already checked existing handles */
        if (dn_handles[node].sock == NO_SOCKET)
        {
            foreach (lc1, datanodelist)
            {
                if (lfirst_int(lc1) == node)
                {
                    /* The node is requested */
                    //char   *init_str = NULL;
                    List *allocate = list_make1_int(node);
                    int *pids;
                    int *fds = PoolManagerGetConnections(allocate, NIL,
                                                         &pids);
                    PGXCNodeHandle *node_handle;

                    if (!fds)
                    {
                        Assert(pids != NULL);
                        ereport(ERROR,
                                (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                                 errmsg("Failed to get pooled connections"),
                                 errhint("This may happen because one or more nodes are "
                                         "currently unreachable, either because of node or "
                                         "network failure.\n Its also possible that the target node "
                                         "may have hit the connection limit or the pooler is "
                                         "configured with low connections.\n Please check "
                                         "if all nodes are running fine and also review "
                                         "max_connections and max_pool_size configuration "
                                         "parameters")));
                    }

/*
                     * We got a new connection, set on the remote node the session parameters
                     * if defined. The transaction parameter should be sent after BEGIN
                     */
#if 0
                    init_str = PGXCNodeGetSessionParamStr();
                    if (init_str)
                    {
                        if (PoolManagerSetCommand(POOL_CMD_GLOBAL_SET, init_str) < 0)
                            elog(ERROR, "Postgres-XZ: ERROR SET query");
                        //pgxc_node_set_query(handle, init_str);
                    }
#endif

                    node_handle = &dn_handles[node];
                    pgxc_node_init(node_handle, fds[0], true, pids[0]);
                    datanode_count++;

                    elog(DEBUG1, "Established a connection with datanode \"%s\","
                                 "remote backend PID %d, socket fd %d, global session %c",
                         node_handle->nodename, (int)pids[0], fds[0], 'T');

                    /*
                     * set load_balancer for next time and return the handle
                     */
                    load_balancer = node + 1;
                    return &dn_handles[node];
                }
            }
        }
    }

    /* We should not get here, one of the cases should be met */
    Assert(false);
    /* Keep compiler quiet */
    return NULL;
}

/*
 * for specified list return array of PGXCNodeHandles
 * acquire from pool if needed.
 * the lenth of returned array is the same as of nodelist
 * For Datanodes, Special case is empty or NIL nodeList, in this case return all the nodes.
 * The returned list should be pfree'd when no longer needed.
 * For Coordinator, do not get a connection if Coordinator list is NIL,
 * Coordinator fds is returned only if transaction uses a DDL
 */
PGXCNodeAllHandles *
get_handles(List *datanodelist, List *coordlist, bool is_coord_only_query, bool is_global_session)
{ // #lizard forgives
    PGXCNodeAllHandles *result;
    ListCell *node_list_item;
    List *dn_allocate = NIL;
    List *co_allocate = NIL;
    PGXCNodeHandle *node_handle;
    //char            *init_str    = NULL;

    /* index of the result array */
    int i = 0;

    if (HandlesInvalidatePending)
        if (DoInvalidateRemoteHandles())
            ereport(ERROR,
                    (errcode(ERRCODE_QUERY_CANCELED),
                     errmsg("canceling transaction due to cluster configuration reset by administrator command")));

    if (HandlesRefreshPending)
        if (DoRefreshRemoteHandles())
            ereport(ERROR,
                    (errcode(ERRCODE_QUERY_CANCELED),
                     errmsg("canceling transaction due to cluster configuration reset by administrator command")));

    result = (PGXCNodeAllHandles *)palloc(sizeof(PGXCNodeAllHandles));
    if (!result)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory")));
    }

    result->primary_handle = NULL;
    result->datanode_handles = NULL;
    result->coord_handles = NULL;
    result->co_conn_count = list_length(coordlist);
    result->dn_conn_count = list_length(datanodelist);

    /*
     * Get Handles for Datanodes
     * If node list is empty execute request on current nodes.
     * It is also possible that the query has to be launched only on Coordinators.
     */
    if (!is_coord_only_query)
    {
        if (list_length(datanodelist) == 0)
        {
            /*
             * We do not have to zero the array - on success all items will be set
             * to correct pointers, on error the array will be freed
             */
            result->datanode_handles = (PGXCNodeHandle **)
                palloc(NumDataNodes * sizeof(PGXCNodeHandle *));
            if (!result->datanode_handles)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_OUT_OF_MEMORY),
                         errmsg("out of memory")));
            }

            for (i = 0; i < NumDataNodes; i++)
            {
                node_handle = &dn_handles[i];
                result->datanode_handles[i] = node_handle;
                if (node_handle->sock == NO_SOCKET)
                    dn_allocate = lappend_int(dn_allocate, i);
            }
        }
        else
        {
            /*
             * We do not have to zero the array - on success all items will be set
             * to correct pointers, on error the array will be freed
             */

            result->datanode_handles = (PGXCNodeHandle **)
                palloc(list_length(datanodelist) * sizeof(PGXCNodeHandle *));
            if (!result->datanode_handles)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_OUT_OF_MEMORY),
                         errmsg("out of memory")));
            }

            i = 0;
            foreach (node_list_item, datanodelist)
            {
                int node = lfirst_int(node_list_item);

                if (node < 0 || node >= NumDataNodes)
                {
                    ereport(ERROR,
                            (errcode(ERRCODE_OUT_OF_MEMORY),
                             errmsg("Invalid Datanode number, node number %d, max nodes %d", node, NumDataNodes)));
                }

                node_handle = &dn_handles[node];
                result->datanode_handles[i++] = node_handle;
                if (node_handle->sock == NO_SOCKET)
                    dn_allocate = lappend_int(dn_allocate, node);
            }
        }
    }

    /*
     * Get Handles for Coordinators
     * If node list is empty execute request on current nodes
     * There are transactions where the Coordinator list is NULL Ex:COPY
     */

    if (coordlist)
    {
        if (list_length(coordlist) == 0)
        {
            /*
             * We do not have to zero the array - on success all items will be set
             * to correct pointers, on error the array will be freed
             */
            result->coord_handles = (PGXCNodeHandle **)palloc(NumCoords * sizeof(PGXCNodeHandle *));
            if (!result->coord_handles)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_OUT_OF_MEMORY),
                         errmsg("out of memory")));
            }

            for (i = 0; i < NumCoords; i++)
            {
                node_handle = &co_handles[i];
                result->coord_handles[i] = node_handle;
                if (node_handle->sock == NO_SOCKET)
                    co_allocate = lappend_int(co_allocate, i);
            }
        }
        else
        {
            /*
             * We do not have to zero the array - on success all items will be set
             * to correct pointers, on error the array will be freed
             */
            result->coord_handles = (PGXCNodeHandle **)
                palloc(list_length(coordlist) * sizeof(PGXCNodeHandle *));
            if (!result->coord_handles)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_OUT_OF_MEMORY),
                         errmsg("out of memory")));
            }

            i = 0;
            /* Some transactions do not need Coordinators, ex: COPY */
            foreach (node_list_item, coordlist)
            {
                int node = lfirst_int(node_list_item);

                if (node < 0 || node >= NumCoords)
                {
                    ereport(ERROR,
                            (errcode(ERRCODE_OUT_OF_MEMORY),
                             errmsg("Invalid coordinator number, node number %d, max nodes %d", node, NumCoords)));
                }

                node_handle = &co_handles[node];

                result->coord_handles[i++] = node_handle;
                if (node_handle->sock == NO_SOCKET)
                    co_allocate = lappend_int(co_allocate, node);
            }
        }
    }

    /*
     * Pooler can get activated even if list of Coordinator or Datanode is NULL
     * If both lists are NIL, we don't need to call Pooler.
     */
    if (dn_allocate || co_allocate)
    {
        int j = 0;
        int *pids;
        int *fds = PoolManagerGetConnections(dn_allocate, co_allocate, &pids);

        if (!fds)
        {
            if (coordlist)
                if (result->coord_handles)
                    pfree(result->coord_handles);
            if (datanodelist)
                if (result->datanode_handles)
                    pfree(result->datanode_handles);

            pfree(result);
            if (dn_allocate)
                list_free(dn_allocate);
            if (co_allocate)
                list_free(co_allocate);
            ereport(ERROR,
                    (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                     errmsg("Failed to get pooled connections"),
                     errhint("This may happen because one or more nodes are "
                             "currently unreachable, either because of node or "
                             "network failure.\n Its also possible that the target node "
                             "may have hit the connection limit or the pooler is "
                             "configured with low connections.\n Please check "
                             "if all nodes are running fine and also review "
                             "max_connections and max_pool_size configuration "
                             "parameters")));
        }
#if 0
        /*
         * We got a new connection, set on the remote node the session parameters
         * if defined. The transaction parameter should be sent after BEGIN
         */
        if (is_global_session)
        {
            init_str = PGXCNodeGetSessionParamStr();
            if (init_str)
            {
                if (PoolManagerSetCommand(POOL_CMD_GLOBAL_SET, init_str) < 0)
                    elog(ERROR, "Postgres-XZ: ERROR SET query");
                //pgxc_node_set_query(handle, init_str);
            }
        }
#endif

        /* Initialisation for Datanodes */
        if (dn_allocate)
        {
            foreach (node_list_item, dn_allocate)
            {
                int node = lfirst_int(node_list_item);
                int fdsock = fds[j];
                int be_pid = pids[j++];

                if (node < 0 || node >= NumDataNodes)
                {
                    ereport(ERROR,
                            (errcode(ERRCODE_OUT_OF_MEMORY),
                             errmsg("Invalid Datanode number, node number %d, max nodes %d", node, NumDataNodes)));
                }

                node_handle = &dn_handles[node];
                pgxc_node_init(node_handle, fdsock, is_global_session, be_pid);
                dn_handles[node] = *node_handle;
                datanode_count++;

                elog(DEBUG1, "Established a connection with datanode \"%s\","
                             "remote backend PID %d, socket fd %d, global session %c",
                     node_handle->nodename, (int)be_pid, fdsock,
                     is_global_session ? 'T' : 'F');
#ifdef _PG_REGRESS_
                elog(LOG, "Established a connection with datanode \"%s\","
                          "remote backend PID %d, socket fd %d, global session %c",
                     node_handle->nodename, (int)be_pid, fdsock,
                     is_global_session ? 'T' : 'F');
#endif
            }
        }
        /* Initialisation for Coordinators */
        if (co_allocate)
        {
            foreach (node_list_item, co_allocate)
            {
                int node = lfirst_int(node_list_item);
                int be_pid = pids[j];
                int fdsock = fds[j++];

                if (node < 0 || node >= NumCoords)
                {
                    ereport(ERROR,
                            (errcode(ERRCODE_OUT_OF_MEMORY),
                             errmsg("Invalid coordinator number, node number %d, max nodes %d", node, NumCoords)));
                }

                node_handle = &co_handles[node];
                pgxc_node_init(node_handle, fdsock, is_global_session, be_pid);
                co_handles[node] = *node_handle;
                coord_count++;

                elog(DEBUG1, "Established a connection with coordinator \"%s\","
                             "remote backend PID %d, socket fd %d, global session %c",
                     node_handle->nodename, (int)be_pid, fdsock,
                     is_global_session ? 'T' : 'F');
#ifdef _PG_REGRESS_
                elog(LOG, "Established a connection with datanode \"%s\","
                          "remote backend PID %d, socket fd %d, global session %c",
                     node_handle->nodename, (int)be_pid, fdsock,
                     is_global_session ? 'T' : 'F');
#endif
            }
        }

        pfree(fds);

        if (co_allocate)
            list_free(co_allocate);
        if (dn_allocate)
            list_free(dn_allocate);
    }

    return result;
}

PGXCNodeAllHandles *
get_current_handles(void)
{ // #lizard forgives
    PGXCNodeAllHandles *result;
    PGXCNodeHandle *node_handle;
    int i;

    result = (PGXCNodeAllHandles *)palloc(sizeof(PGXCNodeAllHandles));
    if (!result)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory")));
    }

    result->primary_handle = NULL;
    result->co_conn_count = 0;
    result->dn_conn_count = 0;

    result->datanode_handles = (PGXCNodeHandle **)
        palloc(NumDataNodes * sizeof(PGXCNodeHandle *));
    if (!result->datanode_handles)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory")));
    }

    for (i = 0; i < NumDataNodes; i++)
    {
        node_handle = &dn_handles[i];
        if (node_handle->sock != NO_SOCKET)
            result->datanode_handles[result->dn_conn_count++] = node_handle;
    }

    result->coord_handles = (PGXCNodeHandle **)
        palloc(NumCoords * sizeof(PGXCNodeHandle *));
    if (!result->coord_handles)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory")));
    }

    for (i = 0; i < NumCoords; i++)
    {
        node_handle = &co_handles[i];
        if (node_handle->sock != NO_SOCKET)
            result->coord_handles[result->co_conn_count++] = node_handle;
    }

    return result;
}

/* Free PGXCNodeAllHandles structure */
void pfree_pgxc_all_handles(PGXCNodeAllHandles *pgxc_handles)
{
    if (!pgxc_handles)
        return;

    if (pgxc_handles->primary_handle)
        pfree(pgxc_handles->primary_handle);
    if (pgxc_handles->datanode_handles)
        pfree(pgxc_handles->datanode_handles);
    if (pgxc_handles->coord_handles)
        pfree(pgxc_handles->coord_handles);

    pfree(pgxc_handles);
}

/* Do translation for non-main cluster */

Oid PGXCGetLocalNodeOid(Oid nodeoid)
{

    if (false == IsPGXCMainCluster)
    {
        char *nodename;

        nodename = get_pgxc_nodename(nodeoid);
        nodeoid = get_pgxc_nodeoid(nodename);
        if (InvalidOid == nodeoid)
        {
            elog(ERROR, "no such node:%s on cluster %s", PGXCNodeName, PGXCClusterName);
        }
    }
    return nodeoid;
}

Oid PGXCGetMainNodeOid(Oid nodeoid)
{

    if (false == IsPGXCMainCluster)
    {
        char *nodename;

        nodename = get_pgxc_nodename(nodeoid);
        nodeoid = get_pgxc_nodeoid_extend(nodename, PGXCMainClusterName);
        if (InvalidOid == nodeoid)
        {
            elog(ERROR, "no such node:%s on main cluster %s", PGXCNodeName, PGXCMainClusterName);
        }
    }
    return nodeoid;
}

/*
 * PGXCNodeGetNodeId
 *        Look at the data cached for handles and return node position
 *         If node type is PGXC_NODE_COORDINATOR look only in coordinator list,
 *        if node type is PGXC_NODE_DATANODE look only in datanode list,
 *        if other (assume PGXC_NODE_NODE) search both, in last case return actual
 *        node type.
 */
int PGXCNodeGetNodeId(Oid nodeoid, char *node_type)
{ // #lizard forgives
    PGXCNodeHandlesLookupEnt *entry = NULL;
    bool found = false;

    if (NULL == node_handles_hash)
    {
        goto NOT_FOUND;
    }
    nodeoid = PGXCGetLocalNodeOid(nodeoid);

    entry = (PGXCNodeHandlesLookupEnt *)hash_search(node_handles_hash, &nodeoid, HASH_FIND, &found);
    if (false == found)
    {
        goto NOT_FOUND;
    }

    /* First check datanodes, they referenced more often */
    if (node_type == NULL || ((*node_type != PGXC_NODE_COORDINATOR)))
    {
        if (dn_handles && dn_handles[entry->nodeidx].nodeoid == nodeoid)
        {
            if (node_type)
                *node_type = PGXC_NODE_DATANODE;
            return entry->nodeidx;
        }
    }

    /* Then check coordinators */
    if (node_type == NULL || ((*node_type != PGXC_NODE_DATANODE)))
    {
        if (co_handles && co_handles[entry->nodeidx].nodeoid == nodeoid)
        {
            if (node_type)
                *node_type = PGXC_NODE_COORDINATOR;
            return entry->nodeidx;
        }
    }

NOT_FOUND:
    /* Not found, have caller handling it */
    if (node_type)
        *node_type = PGXC_NODE_NONE;
    return -1;
}

/*
 * PGXCNodeGetNodeOid
 *        Look at the data cached for handles and return node Oid
 */
Oid PGXCNodeGetNodeOid(int nodeid, char node_type)
{
    PGXCNodeHandle *handles;

    switch (node_type)
    {
    case PGXC_NODE_COORDINATOR:
        handles = co_handles;
        break;
    case PGXC_NODE_DATANODE:
        handles = dn_handles;
        break;
    default:
        /* Should not happen */
        Assert(0);
        return InvalidOid;
    }

    return handles[nodeid].nodeoid;
}

/*
 * PGXCNodeGetNodeIdFromName
 *        Return node position in handles array
 */
int PGXCNodeGetNodeIdFromName(char *node_name, char *node_type)
{
    char *nm;
    Oid nodeoid;

    if (node_name == NULL)
    {
        if (node_type)
            *node_type = PGXC_NODE_NONE;
        return -1;
    }

    nm = str_tolower(node_name, strlen(node_name), DEFAULT_COLLATION_OID);

    nodeoid = get_pgxc_nodeoid(nm);
    pfree(nm);
    if (!OidIsValid(nodeoid))
    {
        if (node_type)
            *node_type = PGXC_NODE_NONE;
        return -1;
    }

    return PGXCNodeGetNodeId(nodeoid, node_type);
}

static List *
paramlist_delete_param(List *param_list, const char *name)
{
    ListCell *cur_item;
    ListCell *prev_item;

    prev_item = NULL;
    cur_item = list_head(param_list);

    while (cur_item != NULL)
    {
        ParamEntry *entry = (ParamEntry *)lfirst(cur_item);

        if (strcmp(NameStr(entry->name), name) == 0)
        {
            /* cur_item must be removed */
            param_list = list_delete_cell(param_list, cur_item, prev_item);
            pfree(entry);
            if (prev_item)
                cur_item = lnext(prev_item);
            else
                cur_item = list_head(param_list);
        }
        else
        {
            prev_item = cur_item;
            cur_item = lnext(prev_item);
        }
    }

    return param_list;
}
static ParamEntry *paramlist_get_paramentry(List *param_list, const char *name)
{
    ListCell *cur_item;

    if (name)
    {
        foreach (cur_item, param_list)
        {
            ParamEntry *entry = (ParamEntry *)lfirst(cur_item);

            if (strcmp(NameStr(entry->name), name) == 0)
            {
                return entry;
            }
        }
    }

    return NULL;
}
static ParamEntry *paramentry_copy(ParamEntry *src_entry)
{
    ParamEntry *dst_entry = NULL;
    if (src_entry)
    {
        dst_entry = (ParamEntry *)palloc(sizeof(ParamEntry));
        strlcpy((char *)(&dst_entry->name), (char *)(&src_entry->name), NAMEDATALEN);
        strlcpy((char *)(&dst_entry->value), (char *)(&src_entry->value), NAMEDATALEN);
        dst_entry->flags = src_entry->flags;
    }

    return dst_entry;
}
static void rollback_paramentry(bool isCommit)
{
    if (!isCommit)
    {
        MemoryContext oldcontext;
        ListCell *lc;

        if (session_params)
            resetStringInfo(session_params);
        oldcontext = MemoryContextSwitchTo(TopMemoryContext);
        foreach (lc, saved_param_list)
        {
            ParamEntry *entry = (ParamEntry *)lfirst(lc);
            if (entry->is_null)
            {
                session_param_list = paramlist_delete_param(session_param_list, (char *)(&entry->name));
            }
            else
                PGXCNodeSetParam(false, (char *)(&entry->name), (char *)(&entry->value), entry->flags);
        }
        MemoryContextSwitchTo(oldcontext);
    }
    local_param_list = NIL;
    local_params = NULL;
    saved_param_list = NULL;
}

static void
polarx_param_set_callback(XactEvent event, void *arg)
{
    switch (event)
    {
    case XACT_EVENT_COMMIT:
        rollback_paramentry(true);
        break;
    case XACT_EVENT_ABORT:
        rollback_paramentry(false);
        break;
    default:
        break;
    }
}
static void save_paramentry_for_rollback(List *param_list, const char *name)
{
    if (saved_param_list == NIL)
        RegisterXactCallback(polarx_param_set_callback, NULL);

    if (paramlist_get_paramentry(saved_param_list, name) == NULL)
    {
        MemoryContext oldcontext;
        oldcontext = MemoryContextSwitchTo(TopTransactionContext);

        ParamEntry *entry = paramlist_get_paramentry(param_list, name);
        if (entry)
        {
            saved_param_list = lappend(saved_param_list, paramentry_copy(entry));
        }
        else
        {
            entry = (ParamEntry *)palloc(sizeof(ParamEntry));
            strlcpy((char *)(&entry->name), name, NAMEDATALEN);
            entry->is_null = true;
            saved_param_list = lappend(saved_param_list, entry);
        }
        MemoryContextSwitchTo(oldcontext);
    }
}
/*
 * Remember new value of a session or transaction parameter, and set same
 * values on newly connected remote nodes.
 */
void PGXCNodeSetParam(bool local, const char *name, const char *value, int flags)
{ // #lizard forgives
    List *param_list;
    MemoryContext oldcontext;

    /* Get the target hash table and invalidate command string */
    if (local)
    {
        param_list = local_param_list;
        if (local_params)
            resetStringInfo(local_params);
        oldcontext = MemoryContextSwitchTo(TopTransactionContext);
    }
    else
    {
        param_list = session_param_list;
        save_paramentry_for_rollback(param_list, name);
        if ((strcmp(name, "session_authorization") == 0) && (value == NULL))
            save_paramentry_for_rollback(param_list, "role");
        if (session_params)
            resetStringInfo(session_params);
        oldcontext = MemoryContextSwitchTo(TopMemoryContext);
    }

    param_list = paramlist_delete_param(param_list, name);
    if (value)
    {
        ParamEntry *entry;
        entry = (ParamEntry *)palloc(sizeof(ParamEntry));
        strlcpy((char *)(&entry->name), name, NAMEDATALEN);
        strlcpy((char *)(&entry->value), value, NAMEDATALEN);
        entry->flags = flags;

        param_list = lappend(param_list, entry);
    }

    /*
     * Special case for
     *     RESET SESSION AUTHORIZATION
     *     SET SESSION AUTHORIZATION TO DEFAULT
     *
     * We must also forget any SET ROLE commands since RESET SESSION
     * AUTHORIZATION also resets current role to session default
     */
    if ((strcmp(name, "session_authorization") == 0) && (value == NULL))
        param_list = paramlist_delete_param(param_list, "role");

    if (local)
        local_param_list = param_list;
    else
        session_param_list = param_list;

    MemoryContextSwitchTo(oldcontext);
}

/*
 * Forget all parameter values set either for transaction or both transaction
 * and session.
 */
void PGXCNodeResetParams(bool only_local)
{
    if (!only_local && session_param_list)
    {
        if (session_param_list)
        {
            ListCell *lc;

            foreach (lc, session_param_list)
            {
                ParamEntry *entry = (ParamEntry *)lfirst(lc);
                save_paramentry_for_rollback(session_param_list, (char *)(&entry->name));
            }
        }
        /* need to explicitly pfree session stuff, it is in TopMemoryContext */
        list_free_deep(session_param_list);
        session_param_list = NIL;
        if (session_params)
        {
            pfree(session_params->data);
            pfree(session_params);
            session_params = NULL;
        }
    }
    /*
     * no need to explicitly destroy the local_param_list and local_params,
     * it will gone with the transaction memory context.
     */
    local_param_list = NIL;
    local_params = NULL;
}

static void
get_set_command(List *param_list, StringInfo command, bool local)
{ // #lizard forgives
    ListCell *lc;
    char search_path_value[512] = {0};
    const char *p = NULL;
    const char *pre = NULL;
    const char *tmp = NULL;
    int index = 0;
    int count = 0;
    bool need_set_quota = false;

    if (param_list == NIL)
        return;

    foreach (lc, param_list)
    {
        ParamEntry *entry = (ParamEntry *)lfirst(lc);
        const char *value = NameStr(entry->value);

        if (strlen(value) == 0)
            value = "''";

        value = quote_guc_value(value, entry->flags);

        /* replace $user to "$user" */
        if (strcmp(entry->name.data, "search_path") == 0)
        {
            for (p = value, pre = value; *p != '\0'; p++)
            {
                if (*p == '$')
                {

                    if (*pre != ' ' && *pre != ',' && *pre != '"' && pre != value)
                    {
                        /* no need to add quota before $ */
                    }
                    else
                    {
                        bool break_loop = false;
                        /* find out match $user exactly or not */
                        for (tmp = p + 1, count = 0; *tmp != '\0' && count <= 4; tmp++, count++)
                        {
                            switch (count)
                            {
                            case 0:
                                if (*tmp != 'U' && *tmp != 'u')
                                {
                                    break_loop = true;
                                }
                                break;

                            case 1:
                                if (*tmp != 'S' && *tmp != 's')
                                {
                                    break_loop = true;
                                }
                                break;

                            case 2:
                                if (*tmp != 'E' && *tmp != 'e')
                                {
                                    break_loop = true;
                                }
                                break;

                            case 3:
                                if (*tmp != 'R' && *tmp != 'r')
                                {
                                    break_loop = true;
                                }
                                break;

                            case 4:
                                if (*tmp == ' ' || *tmp == ',')
                                {
                                    need_set_quota = true;
                                }
                                break;
                            default:
                                break_loop = true;
                            }
                            /* NOT match */
                            if (break_loop)
                            {
                                break;
                            }
                        }

                        if (count == 4 && *tmp == '\0' && !break_loop)
                        {
                            need_set_quota = true;
                        }
                    }

                    if (need_set_quota)
                    {
                        search_path_value[index++] = '"';
                    }
                }

                if (need_set_quota && (' ' == *p || ',' == *p))
                {
                    search_path_value[index++] = '"';
                    need_set_quota = false;
                }

                search_path_value[index++] = *p;
                pre = p;

                /* length safety check. */
                if (strlen(search_path_value) + 3 > sizeof(search_path_value))
                {
                    break;
                }
            }
            /* case like $user\0 (reach end of string) */
            if (need_set_quota)
            {
                search_path_value[index++] = '"';
            }
            appendStringInfo(command, "SET %s %s TO %s;", local ? "LOCAL" : "",
                             NameStr(entry->name), search_path_value);
        }
        else
        {
            appendStringInfo(command, "SET %s %s TO %s;", local ? "LOCAL" : "",
                             NameStr(entry->name), value);
        }
    }
}

/*
 * Returns SET commands needed to initialize remote session.
 * The command may already be biult and valid, return it right away if the case.
 * Otherwise build it up.
 * To support Distributed Session machinery coordinator should generate and
 * send a distributed session identifier to remote nodes. Generate it here.
 */
char *
PGXCNodeGetSessionParamStr(void)
{
    /*
     * If no session parameters are set and that is a coordinator we need to set
     * global_session anyway, even if there were no other parameters.
     * We do not want this string to disappear, so create it in the
     * TopMemoryContext. However if we add first session parameter we will need
     * to free the buffer and recreate it in the same context as the hash table
     * to avoid memory leakage.
     */
    if (session_params == NULL)
    {
        MemoryContext oldcontext = MemoryContextSwitchTo(TopMemoryContext);
        session_params = makeStringInfo();
        MemoryContextSwitchTo(oldcontext);
    }

    /* If the paramstr invalid build it up */
    if (session_params->len == 0)
    {
        get_set_command(session_param_list, session_params, false);
    }
    return session_params->len == 0 ? NULL : session_params->data;
}

/*
 * Returns SET commands needed to initialize transaction on a remote session.
 * The command may already be biult and valid, return it right away if the case.
 * Otherwise build it up.
 */
char *
PGXCNodeGetTransactionParamStr(void)
{ // #lizard forgives
    /* If no local parameters defined there is nothing to return */
    if (local_param_list == NIL)
        return NULL;

    /*
     * If the paramstr invalid build it up.
     */
    if (local_params == NULL)
    {
        MemoryContext oldcontext = MemoryContextSwitchTo(TopTransactionContext);
        local_params = makeStringInfo();
        MemoryContextSwitchTo(oldcontext);
    }
    if (session_param_list)
    {
        ParamEntry *entry_txn_iso;
        /* make sure there is 'transaction_isolation' value */
        entry_txn_iso = paramlist_get_paramentry(session_param_list, "transaction_isolation");
        if (NULL != entry_txn_iso)
        {
            /* 'transaction_isolation' could be set only once, if not set, this would be the first time */
            if (paramlist_get_paramentry(local_param_list, "transaction_isolation") == NULL)
            {
                ParamEntry *entry_txn_iso_tmp;
                entry_txn_iso_tmp = paramentry_copy(entry_txn_iso);
                if (entry_txn_iso_tmp)
                {
                    local_param_list = lappend(local_param_list, entry_txn_iso_tmp);
                }
            }
        }
    }

    /*
     * If parameter string exists it is valid, it is truncated when parameters
     * are modified.
     */
    if (local_params->len == 0)
    {
        get_set_command(local_param_list, local_params, true);
    }
    return local_params->len == 0 ? NULL : local_params->data;
}

/*
 * Send down specified query, read and discard all responses until ReadyForQuery
 */
void pgxc_node_set_query(PGXCNodeHandle *handle, const char *set_query)
{
    pgxc_node_send_query(handle, set_query);
    /*
     * Now read responses until ReadyForQuery.
     * XXX We may need to handle possible errors here.
     */
    for (;;)
    {
        char msgtype;
        int msglen;
        char *msg;
        /*
         * If we are in the process of shutting down, we
         * may be rolling back, and the buffer may contain other messages.
         * We want to avoid a procarray exception
         * as well as an error stack overflow.
         */
        if (proc_exit_inprogress)
            PGXCNodeSetConnectionState(handle, DN_CONNECTION_STATE_ERROR_FATAL);

        /* don't read from from the connection if there is a fatal error */
        if (handle->state == DN_CONNECTION_STATE_ERROR_FATAL)
            break;

        /* No data available, read more */
        if (!HAS_MESSAGE_BUFFERED(handle))
        {
            pgxc_node_receive(1, &handle, NULL);
            continue;
        }
        msgtype = get_message(handle, &msglen, &msg);
        handle->last_command = msgtype;

        /*
         * Ignore any response except ErrorResponse and ReadyForQuery
         */

        if (msgtype == 'E') /* ErrorResponse */
        {
            int32 offset = strnlen(handle->error, MAX_ERROR_MSG_LENGTH);
            snprintf(handle->error + offset, MAX_ERROR_MSG_LENGTH - offset, "%s", msg);
            PGXCNodeSetConnectionState(handle, DN_CONNECTION_STATE_ERROR_FATAL);
            break;
        }

        if (msgtype == 'Z') /* ReadyForQuery */
        {
            handle->transaction_status = msg[0];
            PGXCNodeSetConnectionState(handle, DN_CONNECTION_STATE_IDLE);
            handle->combiner = NULL;
            break;
        }
    }
}

void RequestInvalidateRemoteHandles(void)
{
    HandlesInvalidatePending = true;
}

void RequestRefreshRemoteHandles(void)
{
    HandlesRefreshPending = true;
}

bool PoolerMessagesPending(void)
{
    if (HandlesRefreshPending)
        return true;

    return false;
}

/*
 * For all handles, mark as they are not in use and discard pending input/output
 */
static bool
DoInvalidateRemoteHandles(void)
{
    bool result = false;

    HandlesInvalidatePending = false;
    HandlesRefreshPending = false;

    init_handles(true);

    return result;
}

/*
 * Diff handles using shmem, and remove ALTERed handles
 */
static bool
DoRefreshRemoteHandles(void)
{ // #lizard forgives
    List *altered = NIL, *deleted = NIL, *added = NIL;
    Oid *coOids, *dnOids, *sdnOids;
    int numCoords, numDNodes, numSlaveDNodes, total_nodes;
    bool res = true;

    HandlesRefreshPending = false;

    PgxcNodeGetOidsExtend(&coOids, &dnOids, &sdnOids, &numCoords, &numDNodes, &numSlaveDNodes, false);

    total_nodes = numCoords + numDNodes + numSlaveDNodes;
    if (total_nodes > 0)
    {
        int i;
        List *shmoids = NIL;
        Oid *allOids = (Oid *)palloc(total_nodes * sizeof(Oid));

        /* build array with Oids of all nodes (coordinators first) */
        memcpy(allOids, coOids, numCoords * sizeof(Oid));
        memcpy(allOids + numCoords, dnOids, numDNodes * sizeof(Oid));
        memcpy(allOids + numCoords + numDNodes, sdnOids, numSlaveDNodes * sizeof(Oid));

        for (i = 0; i < total_nodes; i++)
        {
            NodeDefinition *nodeDef;
            PGXCNodeHandle *handle;

            int nid;
            Oid nodeoid;
            char ntype = PGXC_NODE_NONE;

            nodeoid = allOids[i];
            shmoids = lappend_oid(shmoids, nodeoid);

            nodeDef = PgxcNodeGetDefinition(nodeoid);
            /*
             * identify an entry with this nodeoid. If found
             * compare the name/host/port entries. If the name is
             * same and other info is different, it's an ALTER.
             * If the local entry does not exist in the shmem, it's
             * a DELETE. If the entry from shmem does not exist
             * locally, it's an ADDITION
             */
            nid = PGXCNodeGetNodeId(nodeoid, &ntype);

            if (nid == -1)
            {
                /* a new node has been added to the shmem */
                added = lappend_oid(added, nodeoid);
                elog(LOG, "Node added: name (%s) host (%s) port (%d)",
                     NameStr(nodeDef->nodename), NameStr(nodeDef->nodehost),
                     nodeDef->nodeport);
            }
            else
            {
                if (ntype == PGXC_NODE_COORDINATOR)
                    handle = &co_handles[nid];
                else if (ntype == PGXC_NODE_DATANODE)
                    handle = &dn_handles[nid];
                else
                    elog(ERROR, "Node with non-existent node type!");

                /*
                 * compare name, host, port to see if this node
                 * has been ALTERed
                 */
                if (strncmp(handle->nodename, NameStr(nodeDef->nodename), NAMEDATALEN) != 0 ||
                    strncmp(handle->nodehost, NameStr(nodeDef->nodehost), NAMEDATALEN) != 0 ||
                    handle->nodeport != nodeDef->nodeport)
                {
                    elog(LOG, "Node altered: old name (%s) old host (%s) old port (%d)"
                              " new name (%s) new host (%s) new port (%d)",
                         handle->nodename, handle->nodehost, handle->nodeport,
                         NameStr(nodeDef->nodename), NameStr(nodeDef->nodehost),
                         nodeDef->nodeport);
                    altered = lappend_oid(altered, nodeoid);
                }
                /* else do nothing */
            }
            pfree(nodeDef);
        }

        /*
         * Any entry in backend area but not in shmem means that it has
         * been deleted
         */
        for (i = 0; i < NumCoords; i++)
        {
            PGXCNodeHandle *handle = &co_handles[i];
            Oid nodeoid = handle->nodeoid;

            if (!list_member_oid(shmoids, nodeoid))
            {
                deleted = lappend_oid(deleted, nodeoid);
                elog(LOG, "Node deleted: name (%s) host (%s) port (%d)",
                     handle->nodename, handle->nodehost, handle->nodeport);
            }
        }

        for (i = 0; i < NumDataNodes; i++)
        {
            PGXCNodeHandle *handle = &dn_handles[i];
            Oid nodeoid = handle->nodeoid;

            if (!list_member_oid(shmoids, nodeoid))
            {
                deleted = lappend_oid(deleted, nodeoid);
                elog(LOG, "Node deleted: name (%s) host (%s) port (%d)",
                     handle->nodename, handle->nodehost, handle->nodeport);
            }
        }

        for (i = 0; i < NumSlaveDataNodes; i++)
        {
            PGXCNodeHandle *handle = &sdn_handles[i];
            Oid nodeoid = handle->nodeoid;

            if (!list_member_oid(shmoids, nodeoid))
            {
                deleted = lappend_oid(deleted, nodeoid);
                elog(LOG, "Node deleted: name (%s) host (%s) port (%d)",
                     handle->nodename, handle->nodehost, handle->nodeport);
            }
        }

        /* Release palloc'ed memory */
        pfree(coOids);
        pfree(dnOids);
        pfree(allOids);
        list_free(shmoids);
    }

    if (deleted != NIL || added != NIL)
    {
        elog(LOG, "Nodes added/deleted. Reload needed!");
        res = false;
    }

    if (altered == NIL)
    {
        elog(LOG, "No nodes altered. Returning");
        res = true;
    }
    else
        PgxcNodeRefreshBackendHandlesShmem(altered);

    list_free(altered);
    list_free(added);
    list_free(deleted);

    return res;
}

void PGXCNodeSetConnectionState(PGXCNodeHandle *handle, DNConnectionState new_state)
{
    elog(DEBUG5, "Changing connection state for node %s, old state %d, "
                 "new state %d",
         handle->nodename, handle->state, new_state);
    handle->state = new_state;
}

/*
 * Do a "Diff" of backend NODE metadata and the one present in catalog
 *
 * We do this in order to identify if we should do a destructive
 * cleanup or just invalidation of some specific handles
 */
bool PgxcNodeDiffBackendHandles(List **nodes_alter,
                                List **nodes_delete, List **nodes_add)
{ // #lizard forgives
    Relation rel;
    HeapScanDesc scan;
    HeapTuple tuple;
    int i;
    List *altered = NIL, *added = NIL, *deleted = NIL;
    List *catoids = NIL;
    PGXCNodeHandle *handle;
    Oid nodeoid;
    bool res = true;

    rel = heap_open(ForeignServerRelationId, AccessShareLock);
    scan = heap_beginscan_catalog(rel, 0, NULL);
    while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
    {
        Form_pg_foreign_server nodeForm = (Form_pg_foreign_server)GETSTRUCT(tuple);
        int nid;
        Oid nodeoid;
        char ntype = PGXC_NODE_NONE;
        Datum datum;
        bool isnull;
        char *node_type = NULL;

        datum = SysCacheGetAttr(FOREIGNSERVEROID,
                                tuple,
                                Anum_pg_foreign_server_srvtype,
                                &isnull);
        node_type = isnull ? NULL : TextDatumGetCString(datum);

        if (node_type == NULL || !(*node_type == PGXC_NODE_COORDINATOR || *node_type == PGXC_NODE_DATANODE))
        {
            continue;
        }
        if(enable_multi_cluster &&
                strcmp(defGetString(GetServerOptionWithName(NameStr(nodeForm->srvname),
                            "node_cluster_name")), PGXCClusterName))
            continue;

        nodeoid = HeapTupleGetOid(tuple);
        catoids = lappend_oid(catoids, nodeoid);

        /*
         * identify an entry with this nodeoid. If found
         * compare the name/host/port entries. If the name is
         * same and other info is different, it's an ALTER.
         * If the local entry does not exist in the catalog, it's
         * a DELETE. If the entry from catalog does not exist
         * locally, it's an ADDITION
         */
        nid = PGXCNodeGetNodeId(nodeoid, &ntype);

        if (nid == -1)
        {
            /* a new node has been added to the catalog */
            added = lappend_oid(added, nodeoid);
            elog(LOG, "Node added: name (%s) host (%s) port (%s)",
                 NameStr(nodeForm->srvname),
                 defGetString(GetServerOptionWithName(NameStr(nodeForm->srvname), "host")),
                 defGetString(GetServerOptionWithName(NameStr(nodeForm->srvname), "port")));
        }
        else
        {
            if (ntype == PGXC_NODE_COORDINATOR)
                handle = &co_handles[nid];
            else if (ntype == PGXC_NODE_DATANODE)
                handle = &dn_handles[nid];
            else
                elog(ERROR, "Node with non-existent node type!");

            /*
             * compare name, host, port to see if this node
             * has been ALTERed
             */
            if (strcmp(handle->nodename, NameStr(nodeForm->srvname)) != 0 ||
                strcmp(handle->nodehost, defGetString(GetServerOptionWithName(NameStr(nodeForm->srvname), "host"))) != 0 ||
                handle->nodeport != strtol(defGetString(GetServerOptionWithName(NameStr(nodeForm->srvname), "port")), NULL, 10))
            {
                elog(LOG, "Node altered: old name (%s) old host (%s) old port (%d)"
                          " new name (%s) new host (%s) new port (%ld)",
                     handle->nodename, handle->nodehost, handle->nodeport,
                     NameStr(nodeForm->srvname),
                     defGetString(GetServerOptionWithName(NameStr(nodeForm->srvname), "host")),
                     strtol(defGetString(GetServerOptionWithName(NameStr(nodeForm->srvname), "port")), NULL, 10));
                /*
                 * If this node itself is being altered, then we need to
                 * resort to a reload. Check so..
                 */
                if (pg_strcasecmp(PGXCNodeName,
                                  NameStr(nodeForm->srvname)) == 0)
                {
                    res = false;
                }
                altered = lappend_oid(altered, nodeoid);
            }
            /* else do nothing */
        }
    }
    heap_endscan(scan);

    /*
     * Any entry in backend area but not in catalog means that it has
     * been deleted
     */
    for (i = 0; i < NumCoords; i++)
    {
        handle = &co_handles[i];
        nodeoid = handle->nodeoid;
        if (!list_member_oid(catoids, nodeoid))
        {
            deleted = lappend_oid(deleted, nodeoid);
            elog(LOG, "Node deleted: name (%s) host (%s) port (%d)",
                 handle->nodename, handle->nodehost, handle->nodeport);
        }
    }
    for (i = 0; i < NumDataNodes; i++)
    {
        handle = &dn_handles[i];
        nodeoid = handle->nodeoid;
        if (!list_member_oid(catoids, nodeoid))
        {
            deleted = lappend_oid(deleted, nodeoid);
            elog(LOG, "Node deleted: name (%s) host (%s) port (%d)",
                 handle->nodename, handle->nodehost, handle->nodeport);
        }
    }

    for (i = 0; i < NumSlaveDataNodes; i++)
    {
        handle = &sdn_handles[i];
        nodeoid = handle->nodeoid;
        if (!list_member_oid(catoids, nodeoid))
        {
            deleted = lappend_oid(deleted, nodeoid);
            elog(LOG, "Node deleted: name (%s) host (%s) port (%d)",
                 handle->nodename, handle->nodehost, handle->nodeport);
        }
    }
    heap_close(rel, AccessShareLock);

    if (nodes_alter)
        *nodes_alter = altered;
    if (nodes_delete)
        *nodes_delete = deleted;
    if (nodes_add)
        *nodes_add = added;

    if (catoids)
        list_free(catoids);

    return res;
}

/*
 * Refresh specific backend handles associated with
 * nodes in the "nodes_alter" list below
 *
 * The handles are refreshed using shared memory
 */
void PgxcNodeRefreshBackendHandlesShmem(List *nodes_alter)
{
    ListCell *lc;
    Oid nodeoid;
    int nid;
    PGXCNodeHandle *handle = NULL;

    foreach (lc, nodes_alter)
    {
        char ntype = PGXC_NODE_NONE;
        NodeDefinition *nodedef;

        nodeoid = lfirst_oid(lc);
        nid = PGXCNodeGetNodeId(nodeoid, &ntype);

        if (nid == -1)
            elog(ERROR, "Looks like node metadata changed again");
        else
        {
            if (ntype == PGXC_NODE_COORDINATOR)
                handle = &co_handles[nid];
            else if (ntype == PGXC_NODE_DATANODE)
                handle = &dn_handles[nid];
            else
                elog(ERROR, "Node with non-existent node type!");
        }

        /*
         * Update the local backend handle data with data from catalog
         * Free the handle first..
         */
        pgxc_node_free(handle);
        elog(LOG, "Backend (%u), Node (%s) updated locally",
             MyBackendId, handle->nodename);
        nodedef = PgxcNodeGetDefinition(nodeoid);
        strncpy(handle->nodename, NameStr(nodedef->nodename), NAMEDATALEN);
        strncpy(handle->nodehost, NameStr(nodedef->nodehost), NAMEDATALEN);
        handle->nodeport = nodedef->nodeport;
        pfree(nodedef);
    }
    return;
}

void HandlePoolerMessages(void)
{
    if (HandlesRefreshPending)
    {
        DoRefreshRemoteHandles();

        elog(LOG, "Backend (%u), doing handles refresh",
             MyBackendId);
    }
    return;
}

bool node_ready_for_query(PGXCNodeHandle *conn)
{
    return ('Z' == (conn)->last_command);
}

/*
 * Check the socket health status.
 *
 */
static int
pgxc_check_socket_health(int sock, int forRead, int forWrite, time_t end_time)
{ // #lizard forgives
    /* We use poll(2) if available, otherwise select(2) */
#ifdef HAVE_POLL
    struct pollfd input_fd;
    int timeout_ms;

    if (!forRead && !forWrite)
        return 0;

    input_fd.fd = sock;
    input_fd.events = POLLERR;
    input_fd.revents = 0;

    if (forRead)
        input_fd.events |= POLLIN;
    if (forWrite)
        input_fd.events |= POLLOUT;

    /* Compute appropriate timeout interval */
    if (end_time == ((time_t)-1))
    {
        timeout_ms = -1;
    }
    else
    {
        time_t now = time(NULL);

        if (end_time > now)
            timeout_ms = (end_time - now) * 1000;
        else
            timeout_ms = 0;
    }

    return poll(&input_fd, 1, timeout_ms);
#else  /* !HAVE_POLL */

    fd_set input_mask;
    fd_set output_mask;
    fd_set except_mask;
    struct timeval timeout;
    struct timeval *ptr_timeout;

    if (!forRead && !forWrite)
        return 0;

    FD_ZERO(&input_mask);
    FD_ZERO(&output_mask);
    FD_ZERO(&except_mask);
    if (forRead)
        FD_SET(sock, &input_mask);

    if (forWrite)
        FD_SET(sock, &output_mask);
    FD_SET(sock, &except_mask);

    /* Compute appropriate timeout interval */
    if (end_time == ((time_t)-1))
        ptr_timeout = NULL;
    else
    {
        time_t now = time(NULL);

        if (end_time > now)
            timeout.tv_sec = end_time - now;
        else
            timeout.tv_sec = 0;
        timeout.tv_usec = 0;
        ptr_timeout = &timeout;
    }

    return select(sock + 1, &input_mask, &output_mask,
                  &except_mask, ptr_timeout);
#endif /* HAVE_POLL */
}

int pgxc_node_send_disconnect(PGXCNodeHandle *handle, char *cursor, int cons)
{
    /* size */
    int msgLen = 4 + strlen(cursor) + 1 + 4;

    /* msgType + msgLen */
    if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
    {
        add_error_message(handle, "out of memory");
        return EOF;
    }

    handle->outBuffer[handle->outEnd++] = 'N';
    /* size */
    msgLen = htonl(msgLen);
    memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
    handle->outEnd += 4;

    memcpy(handle->outBuffer + handle->outEnd, cursor, strlen(cursor) + 1);
    handle->outEnd += (strlen(cursor) + 1);

    cons = htonl(cons);
    memcpy(handle->outBuffer + handle->outEnd, &cons, 4);
    handle->outEnd += 4;

    PGXCNodeSetConnectionState(handle, DN_CONNECTION_STATE_QUERY);

    return pgxc_node_flush(handle);
}

void SetCurrentHandlesReadonly(void)
{
    int i = 0;
    PGXCNodeHandle *conn = NULL;
    PGXCNodeAllHandles *handles = get_current_handles();

    for (i = 0; i < handles->dn_conn_count; i++)
    {
        conn = handles->datanode_handles[i];

        /* Skip empty slots */
        if (conn->sock == NO_SOCKET)
            continue;

        conn->read_only = true;
    }

    for (i = 0; i < handles->co_conn_count; i++)
    {
        conn = handles->coord_handles[i];

        /* Skip empty slots */
        if (conn->sock == NO_SOCKET)
            continue;

        conn->read_only = true;
    }
    pfree_pgxc_all_handles(handles);
}

static bool
is_node_altered(uint32 hashvalue, bool *is_cluster_node)
{
    HASH_SEQ_STATUS scan;
    PGXCNodeHandlesLookupEnt *entry;
    bool in_handle = false;
    bool in_def = false;
    bool is_alter = false;

    /* ConnectionHash must exist already, if we're registered */
    hash_seq_init(&scan, node_handles_hash);
    while ((entry = (PGXCNodeHandlesLookupEnt *) hash_seq_search(&scan)))
    {
        if (entry->hashvalue == hashvalue) 
        {
            in_handle = true;
            break;
        }
    }

    in_def = IsNodeInDefHash(hashvalue);

    if(in_handle && in_def)
        is_alter = true;
    else
    {
        if(is_cluster_node)
        {
            if(!in_handle && !in_def)
                *is_cluster_node = false;
            else
                *is_cluster_node = true;

        }
        is_alter = false;
    }
        return is_alter;
}

static void
polar_handles_inval_callback(Datum arg, int cacheid, uint32 hashvalue)
{
    bool is_cluster_node = false;
    Assert(cacheid == FOREIGNSERVEROID);
    elog(LOG, "handle foreign server inval callback %d, %d, hashvalue %d", cacheid, FOREIGNSERVEROID, hashvalue);

    if (proc_exit_inprogress)
        return;

    if (PoolerReloadHoldoffCount)
    {
        return;
    }

    HOLD_POOLER_RELOAD();
    PoolManagerCatchupNodeInfo();
    PoolManagerDisconnect();


    if(hashvalue != 0 && is_node_altered(hashvalue, &is_cluster_node))
        RequestRefreshRemoteHandles(); 
    else if(is_cluster_node)
    {
        if(IS_PGXC_SINGLE_NODE)
            InitPolarxClusterGlobalInfo();
        RequestInvalidateRemoteHandles();
    }
    RESUME_POOLER_RELOAD();
}
