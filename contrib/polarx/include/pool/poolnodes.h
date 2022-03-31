/*-------------------------------------------------------------------------
 *
 * poolnodes.h
 *
 *      Functions for obtaining and managing node handes from
 *      the connection pool
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * IDENTIFICATION
 *      $$
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef POOLNODES_H
#define POOLNODES_H

#include "pgxc/pgxcnode.h"

/*
 * Gracefully close connection to the PoolManager
 */
extern void PoolManagerDisconnect(void);


/*
 * Reconnect to pool manager
 * This simply does a disconnection followed by a reconnection.
 */
extern void PoolManagerReconnect(void);

/* Get pooled connections */
extern int *PoolManagerGetConnections(List *datanodelist, List *coordlist, int **pids);

/* Return connections back to the pool, for both Coordinator and Datanode connections */
extern void PoolManagerReleaseConnections(bool force);

/* Cancel a running query on Datanodes as well as on other Coordinators */
extern bool PoolManagerCancelQuery(int dn_count, int* dn_list, int co_count, int* co_list, int signal);

extern bool PoolManagerCatchupNodeInfo(void);

#endif
