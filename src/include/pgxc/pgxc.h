/*-------------------------------------------------------------------------
 *
 * pgxc.h
 *        Postgres-XC flags and connection control information
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2011  PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/pgxc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGXC_H
#define PGXC_H

#include "postgres.h"

extern bool isPGXCCoordinator;
extern bool isPGXCDataNode;
extern int        remoteConnType;

typedef enum
{
    REMOTE_CONN_APP,
    REMOTE_CONN_COORD,
    REMOTE_CONN_DATANODE,
    REMOTE_CONN_GTM,
    REMOTE_CONN_GTM_PROXY
} RemoteConnTypes;


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


#endif   /* PGXC */
