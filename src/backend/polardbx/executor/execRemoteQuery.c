/*-------------------------------------------------------------------------
 *
 * execRemoteQuery.c
 *
 *      Functions to execute commands on remote Datanodes
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *      src/backend/polardbx/execRemoteQuery.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "pgxc/pgxcnode.h"
#include "pgxc/recvRemote.h"

void ExecCloseRemoteStatement(const char *stmt_name, List *nodelist)
{ // #lizard forgives
    PGXCNodeAllHandles *all_handles;
    PGXCNodeHandle **connections;
    ResponseCombiner combiner;
    int conn_count;
    int i;

    /* Exit if nodelist is empty */
    if (list_length(nodelist) == 0)
        return;

    /* get needed Datanode connections */
    all_handles = get_handles(nodelist, NIL, false, true);
    conn_count = all_handles->dn_conn_count;
    connections = all_handles->datanode_handles;

    for (i = 0; i < conn_count; i++)
    {
        if (connections[i]->state == DN_CONNECTION_STATE_QUERY)
            BufferConnection(connections[i]);
        if (pgxc_node_send_close(connections[i], true, stmt_name) != 0)
        {
            /*
             * statements are not affected by statement end, so consider
             * unclosed statement on the Datanode as a fatal issue and
             * force connection is discarded
             */
            PGXCNodeSetConnectionState(connections[i],
                                       DN_CONNECTION_STATE_ERROR_FATAL);
            ereport(WARNING,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Failed to close Datanode statemrnt")));
        }
        if (pgxc_node_send_sync(connections[i]) != 0)
        {
            PGXCNodeSetConnectionState(connections[i],
                                       DN_CONNECTION_STATE_ERROR_FATAL);
            ereport(WARNING,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Failed to close Datanode statement")));

            ereport(LOG,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Failed to sync msg to node %s backend_pid:%d", connections[i]->nodename, connections[i]->backend_pid)));
        }
        PGXCNodeSetConnectionState(connections[i], DN_CONNECTION_STATE_CLOSE);
    }

    InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
    /*
     * Make sure there are zeroes in unused fields
     */
    memset(&combiner, 0, sizeof(ScanState));

    while (conn_count > 0)
    {
        if (pgxc_node_receive(conn_count, connections, NULL))
        {
            for (i = 0; i < conn_count; i++)
                PGXCNodeSetConnectionState(connections[i],
                                           DN_CONNECTION_STATE_ERROR_FATAL);

            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Failed to close Datanode statement")));
        }
        i = 0;
        while (i < conn_count)
        {
            int res = handle_response(connections[i], &combiner);
            if (res == RESPONSE_EOF)
            {
                i++;
            }
            else if (res == RESPONSE_READY ||
                     connections[i]->state == DN_CONNECTION_STATE_ERROR_FATAL)
            {
                if (--conn_count > i)
                    connections[i] = connections[conn_count];
            }
        }
    }

    ValidateAndCloseCombiner(&combiner);
    pfree_pgxc_all_handles(all_handles);
}
