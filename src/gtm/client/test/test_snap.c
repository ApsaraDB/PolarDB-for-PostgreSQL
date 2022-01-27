/*
 * Copyright (c) 2010-2012 Postgres-XC Development Group
 */
#include <sys/types.h>
#include <unistd.h>

#include "gtm/gtm_c.h"
#include "gtm/libpq-fe.h"
#include "gtm/gtm_client.h"

#define client_log(x)    printf x

int
main(int argc, char *argv[])
{// #lizard forgives
    int ii;
    GlobalTransactionId gxid[4000];
    GTM_Conn *conn;
    char connect_string[100];

    for (ii = 0; ii < 3; ii++)
        fork();

    sprintf(connect_string, "host=localhost port=6666 node_name=one remote_type=%d", GTM_NODE_COORDINATOR);

    conn = PQconnectGTM(connect_string);
    if (conn == NULL)
    {
        client_log(("Error in connection\n"));
        exit(1);
    }

    for (ii = 0; ii < 20; ii++)
    {
        gxid[ii] = begin_transaction(conn, GTM_ISOLATION_RC);
        if (gxid[ii] != InvalidGlobalTransactionId)
            client_log(("Started a new transaction (GXID:%u)\n", gxid[ii]));
        else
            client_log(("BEGIN transaction failed for ii=%d\n", ii));
    }

    for (ii = 0; ii < 5; ii++)
    {
        int jj;
        GTM_Snapshot snapshot = get_snapshot(conn, gxid[ii], true);
        if (snapshot != NULL)
        {
            client_log(("Snapshot: GXID %u, xmin=%u, xmax=%u\n", gxid[ii],
                    snapshot->sn_xmin, snapshot->sn_xmax));
            client_log(("xcnt=%d %s", snapshot->sn_xcnt,
                    snapshot->sn_xcnt > 0 ? "xip=(" : ""));
            for (jj = 0; jj < snapshot->sn_xcnt; jj++)
                client_log(("%d%c ", snapshot->sn_xip[jj],
                        ((jj + 1) == snapshot->sn_xcnt) ? ')' : ','));
            client_log(("\n"));
        }
    }

    for (ii = 0; ii < 20; ii++)
    {
        if (!prepare_transaction(conn, gxid[ii]))
            client_log(("PREPARE successful (GXID:%u)\n", gxid[ii]));
        else
            client_log(("PREPARE failed (GXID:%u)\n", gxid[ii]));
    }

    for (ii = 0; ii < 20; ii++)
    {
        if (ii % 2 == 0)
        {
            if (!abort_transaction(conn, gxid[ii]))
                client_log(("ROLLBACK successful (GXID:%u)\n", gxid[ii]));
            else
                client_log(("ROLLBACK failed (GXID:%u)\n", gxid[ii]));
        }
        else
        {
            if (!commit_transaction(conn, gxid[ii]))
                client_log(("COMMIT successful (GXID:%u)\n", gxid[ii]));
            else
                client_log(("COMMIT failed (GXID:%u)\n", gxid[ii]));
        }
    }

    GTMPQfinish(conn);
    return 0;
}
