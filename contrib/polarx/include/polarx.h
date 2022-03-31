/*-------------------------------------------------------------------------
 *
 * polarx.h
 *        polarx flags and connection control information
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2011  PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * contrib/polarx/include/polarx.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLARX_H
#define POLARX_H

#define PGXC_NODE_COORDINATOR        'C'
#define PGXC_NODE_DATANODE            'D'
#define PGXC_NODE_NONE                'N'
#define PGXC_NODE_GTM                'G'

/* Local node name and numer */
extern char *PGXCNodeName;
extern char *PGXCClusterName;
extern char *PGXCMainClusterName;
extern bool IsPGXCMainCluster;
extern int  PGXCNodeId;
extern bool isPGXCCoordinator;
extern bool isPGXCDataNode;
extern int  remoteConnType;

typedef enum DistributionType
{
    DISTTYPE_REPLICATION,            /* Replicated */
    DISTTYPE_HASH,                /* Hash partitioned */
    DISTTYPE_ROUNDROBIN,            /* Round Robin */
    DISTTYPE_MODULO               /* Modulo partitioned */
} DistributionType;

typedef enum
{
    REMOTE_CONN_APP,
    REMOTE_CONN_COORD,
    REMOTE_CONN_DATANODE,
    REMOTE_CONN_GTM,
    REMOTE_CONN_GTM_PROXY
} RemoteConnTypes;

typedef uint32 GlobalTransactionId;

#define IS_PGXC_COORDINATOR isPGXCCoordinator
#define IS_PGXC_DATANODE isPGXCDataNode

#define IS_PGXC_LOCAL_COORDINATOR    \
    (IS_PGXC_COORDINATOR && !IsConnFromCoord())
#define IS_PGXC_REMOTE_COORDINATOR    \
    (IS_PGXC_COORDINATOR && IsConnFromCoord())
#define IS_PGXC_SINGLE_NODE    \
    (!isPGXCCoordinator && !isPGXCDataNode)

#define REMOTE_CONN_TYPE remoteConnType

#define IsConnFromApp() (remoteConnType == REMOTE_CONN_APP)
#define IsConnFromCoord() (remoteConnType == REMOTE_CONN_COORD)
#define IsConnFromDatanode() (remoteConnType == REMOTE_CONN_DATANODE)
#define IsConnFromGtm() (remoteConnType == REMOTE_CONN_GTM)
#define IsConnFromGtmProxy() (remoteConnType == REMOTE_CONN_GTM_PROXY)
#endif   /* POLARX_H */
