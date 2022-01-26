/*-------------------------------------------------------------------------
 *
 * nodemgr.h
 *  Routines for node management
 *
 *
 * Portions Copyright (c) 1996-2011  PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/nodemgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEMGR_H
#define NODEMGR_H

#include "nodes/parsenodes.h"

#define PGXC_NODENAME_LENGTH    64
#define        MAX_COORDINATOR_NUMBER 256
#define        MAX_DATANODE_NUMBER    256

/* MAX NODES NUMBER of the cluster */
#define        MAX_NODES_NUMBER       512

/* Global number of nodes */
extern int     NumDataNodes;
extern int     NumCoords;

extern char *PGXCNodeHost;

/* Node definition */
typedef struct
{
	Oid         nodeoid;
	NameData    nodename;
	NameData    nodehost;
	int         nodeport;
	char        nodetype;
	bool        nodeisprimary;
	bool        nodeispreferred;
	bool        nodeishealthy;
	bool        nodeislocal;
} NodeDefinition;

extern void NodeTablesShmemInit(void);
extern void NodeDefHashTabShmemInit(void);
extern Size NodeHashTableShmemSize(void);
extern Size NodeTablesShmemSize(void);

extern void PgxcNodeListAndCount(void);
extern void
PgxcNodeGetOidsExtend(Oid **coOids, Oid **dnOids, Oid **sdnOids,
                int *num_coords, int *num_dns, int *num_sdns, bool update_preferred);
#define PgxcNodeGetOids(coOids, dnOids, num_coords, num_dns, update_preferred) \
    PgxcNodeGetOidsExtend(coOids, dnOids, NULL, num_coords, num_dns, NULL, update_preferred)
extern void
PgxcNodeGetHealthMapExtend(Oid *coOids, Oid *dnOids, Oid *sdnOids,
                int *num_coords, int *num_dns, int *num_sdns, bool *coHealthMap,
                bool *dnHealthMap, bool *sdnHealthMap);
#define PgxcNodeGetHealthMap(coOids, dnOids, num_coords, num_dns, coHealthMap, dnHealthMap) \
                PgxcNodeGetHealthMapExtend(coOids, dnOids, NULL,num_coords, num_dns, NULL, coHealthMap, \
                dnHealthMap, NULL)
extern NodeDefinition *PgxcNodeGetDefinition(Oid node);
extern void PgxcNodeDnListHealth(List *nodeList, bool *dnhealth);
extern bool PgxcNodeUpdateHealth(Oid node, bool status);

/* GUC parameter */
extern bool enable_multi_cluster;
extern bool enable_multi_cluster_print;
extern DefElem *GetServerOptionWithName(const char *server_name, const char *option_name);
extern char *get_pgxc_nodename(Oid nodeoid);
extern Oid    get_pgxc_nodeoid_extend(const char *nodename, const char *clustername);
#define get_pgxc_nodeoid(nodename) get_pgxc_nodeoid_extend((nodename), (PGXCClusterName))
extern uint32 get_pgxc_node_id(Oid nodeid);
extern char get_pgxc_nodetype(Oid nodeid);
extern int get_pgxc_nodeport(Oid nodeid);
extern char *get_pgxc_nodehost(Oid nodeid);
extern bool is_pgxc_nodepreferred(Oid nodeid);
extern bool is_pgxc_nodeprimary(Oid nodeid);
extern bool is_pgxc_nodelocal(Oid nodeid);

extern NodeDefinition *get_polar_local_node_def(void);
extern char *get_polar_local_nodename(void);
extern Oid  get_polar_local_nodeoid(void);
extern char get_polar_local_nodetype(void);
extern int get_polar_local_nodeport(void);
extern char *get_polar_local_nodehost(void);
extern bool is_polar_local_nodepreferred(void);
extern bool is_polar_local_nodeprimary(void);
#endif    /* NODEMGR_H */
