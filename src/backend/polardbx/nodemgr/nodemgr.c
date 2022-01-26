/*-------------------------------------------------------------------------
 *
 * nodemgr.c
 *      Routines to support manipulation of the pgxc_node catalog
 *      Support concerns CREATE/ALTER/DROP on NODE object.
 *
 * Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "access/hash.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "commands/defrem.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/tqual.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "libpq/libpq.h"
#include "foreign/foreign.h"
#include "catalog/pg_foreign_server.h"

bool enable_multi_cluster = true;
bool enable_multi_cluster_print = false;

/* Current size of dn_handles and co_handles */
int NumDataNodes;
int NumCoords;

/*
 * How many times should we try to find a unique indetifier
 * in case hash of the node name comes out to be duplicate
 */

#define MAX_TRIES_FOR_NID 200

static DefElem *GetServerOptionInternal(List *options, const char *option_name);

/*
 * GUC parameters.
 * Shared memory block can not be resized dynamically, so we should have some
 * limits set at startup time to calculate amount of shared memory to store
 * node table. Nodes can be added to running cluster until that limit is reached
 * if cluster needs grow beyond the configuration value should be changed and
 * cluster restarted.
 */
/* Global number of nodes. Point to a shared memory block */
static int *shmemNumCoords;
static int *shmemNumDataNodes;
static int *shmemNumSlaveDataNodes;

/* Shared memory tables of node definitions */
NodeDefinition *coDefs;
NodeDefinition *dnDefs;
NodeDefinition *sdnDefs;

static NodeDefinition lnDef;
static bool lnDefValid = false;

char *PGXCNodeHost;

/* HashTable key: nodeoid  value: position of coDefs/dnDefs */
static HTAB *g_NodeDefHashTab = NULL;

typedef struct
{
    Oid nodeoid;
} NodeDefLookupTag;

typedef struct
{
    NodeDefLookupTag tag;
    int32 nodeDefIndex; /* Associated into  coDefs or dnDefs index */
} NodeDefLookupEnt;

/*
 * NodeTablesInit
 *    Initializes shared memory tables of Coordinators and Datanodes.
 */
void NodeTablesShmemInit(void)
{ // #lizard forgives
    bool found;
    int i;

    /*
     * Initialize the table of Coordinators: first sizeof(int) bytes are to
     * store actual number of Coordinators, remaining data in the structure is
     * array of NodeDefinition that can contain up to MAX_COORDINATOR_NUMBER entries.
     * That is a bit weird and probably it would be better have these in
     * separate structures, but I am unsure about cost of having shmem structure
     * containing just single integer.
     */
    shmemNumCoords = ShmemInitStruct("Coordinator Table",
                                     sizeof(int) +
                                         sizeof(NodeDefinition) * MAX_COORDINATOR_NUMBER,
                                     &found);

    /* Have coDefs pointing right behind shmemNumCoords */
    coDefs = (NodeDefinition *)(shmemNumCoords + 1);

    /* Mark it empty upon creation */
    if (!found)
    {
        *shmemNumCoords = 0;
        /* Mark nodeishealthy true at init time for all */
        for (i = 0; i < MAX_COORDINATOR_NUMBER; i++)
            coDefs[i].nodeishealthy = true;
    }

    /* Same for Datanodes */
    shmemNumDataNodes = ShmemInitStruct("Datanode Table",
                                        sizeof(int) +
                                            sizeof(NodeDefinition) * MAX_DATANODE_NUMBER,
                                        &found);

    /* Have dnDefs pointing right behind shmemNumDataNodes */
    dnDefs = (NodeDefinition *)(shmemNumDataNodes + 1);

    /* Mark it empty upon creation */
    if (!found)
    {
        *shmemNumDataNodes = 0;
        /* Mark nodeishealthy true at init time for all */
        for (i = 0; i < MAX_DATANODE_NUMBER; i++)
            dnDefs[i].nodeishealthy = true;
    }

    /* Same for Datanodes */
    shmemNumSlaveDataNodes = ShmemInitStruct("Slave Datanode Table",
                                             sizeof(int) +
                                                 sizeof(NodeDefinition) * MAX_DATANODE_NUMBER,
                                             &found);

    /* Have dnDefs pointing right behind shmemNumSlaveDataNodes */
    sdnDefs = (NodeDefinition *)(shmemNumSlaveDataNodes + 1);

    /* Mark it empty upon creation */
    if (!found)
    {
        *shmemNumSlaveDataNodes = 0;
        /* Mark nodeishealthy true at init time for all */
        for (i = 0; i < MAX_DATANODE_NUMBER; i++)
            sdnDefs[i].nodeishealthy = true;
    }

    NodeDefHashTabShmemInit();
}

void NodeDefHashTabShmemInit(void)
{
    HASHCTL info;

    /* Init hash table for nodeoid to dnDefs/coDefs lookup */
    info.keysize = sizeof(NodeDefLookupTag);
    info.entrysize = sizeof(NodeDefLookupEnt);
    info.hash = tag_hash;
    g_NodeDefHashTab = ShmemInitHash("NodeDef info look up",
                                     MAX_COORDINATOR_NUMBER + 2 * MAX_DATANODE_NUMBER,
                                     MAX_COORDINATOR_NUMBER + 2 * MAX_DATANODE_NUMBER,
                                     &info,
                                     HASH_ELEM | HASH_FUNCTION | HASH_FIXED_SIZE);

    if (!g_NodeDefHashTab)
    {
        elog(FATAL, "invalid shmem status when creating node def hash ");
    }
}

/*
 * NodeHashTableShmemSize
 *    Get the size of Node Definition hash table
 */
Size NodeHashTableShmemSize(void)
{
    Size size;

    /* hash table, here just double the element size, in case of memory corruption */
    size = mul_size((MAX_DATANODE_NUMBER + MAX_COORDINATOR_NUMBER) * 2, MAXALIGN64(sizeof(NodeDefLookupEnt)));

    return size;
}

/*
 * NodeTablesShmemSize
 *    Get the size of shared memory dedicated to node definitions
 */
Size NodeTablesShmemSize(void)
{
    Size co_size;
    Size dn_size;
    Size dn_slave_size;
    Size total_size;

    co_size = mul_size(sizeof(NodeDefinition), MAX_COORDINATOR_NUMBER);
    co_size = add_size(co_size, sizeof(int));
    dn_size = mul_size(sizeof(NodeDefinition), MAX_DATANODE_NUMBER);
    dn_size = add_size(dn_size, sizeof(int));
    dn_slave_size = mul_size(sizeof(NodeDefinition), MAX_DATANODE_NUMBER);
    dn_slave_size = add_size(dn_slave_size, sizeof(int));

    total_size = add_size(co_size, dn_size);
    total_size = add_size(total_size, dn_slave_size);
    total_size = add_size(total_size, NAMEDATALEN);
    return total_size;
}

/*
 * Check list of options and return things filled.
 * This includes check on option values.
 */
static void
check_node_options(const char *node_name, List *options, char **node_host,
                   int *node_port, char *node_type,
                   bool *is_primary, bool *is_preferred,
                   char **node_cluster_name,
                   bool *alter,
                   bool *is_current)
{ // #lizard forgives
    ListCell *option;
    bool set_node_primary_option = false;
    bool set_node_host_option = false;
    bool set_node_port_option = false;
    bool set_is_current = false;

    if (!options)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("No options specified")));

    /* Filter options */
    foreach (option, options)
    {
        DefElem *defel = (DefElem *)lfirst(option);

        if (strcmp(defel->defname, "port") == 0)
        {
            *node_port = defGetTypeLength(defel);
            set_node_host_option = true;

            if (*node_port < 1 || *node_port > 65535)
                ereport(ERROR,
                        (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                         errmsg("port value is out of range")));
        }
        else if (strcmp(defel->defname, "host") == 0)
        {
            *node_host = defGetString(defel);
            set_node_port_option = true;
        }
        else if (strcmp(defel->defname, "type") == 0)
        {
            char *type_loc;

            type_loc = defGetString(defel);

            if (strcmp(type_loc, "coordinator") != 0 &&
                strcmp(type_loc, "datanode") != 0 &&
                strcmp(type_loc, "gtm") != 0)
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("type value is incorrect, specify 'coordinator or 'datanode' or 'gtm'")));

            if (strcmp(type_loc, "coordinator") == 0)
                *node_type = PGXC_NODE_COORDINATOR;
            else if (strcmp(type_loc, "datanode") == 0)
                *node_type = PGXC_NODE_DATANODE;
            else if (strcmp(type_loc, "gtm") == 0)
                *node_type = PGXC_NODE_GTM;
        }
        else if (strcmp(defel->defname, "primary") == 0)
        {
            *is_primary = defGetBoolean(defel);
            set_node_primary_option = true;
        }
        else if (strcmp(defel->defname, "preferred") == 0)
        {
            *is_preferred = defGetBoolean(defel);
        }
        else if (strcmp(defel->defname, "cluster") == 0)
        {
            *node_cluster_name = defGetString(defel);
        }
        else if (strcmp(defel->defname, "alter") == 0)
        {
            *alter = defGetBoolean(defel);
        }
        else if (strcmp(defel->defname, "local") == 0)
        {
            if (is_current)
                *is_current = defGetBoolean(defel);
            set_is_current = true;
        }
        else
        {
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("incorrect option: %s", defel->defname)));
        }
    }

    if (set_is_current == false)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("PGXC node %s: local not specified",
                        node_name)));
    /* A primary node has to be a Datanode or Gtm */
    if (*is_primary && *node_type != PGXC_NODE_DATANODE && *node_type != PGXC_NODE_GTM)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("PGXC node %s: cannot be a primary node, it has to be a Datanode or GTM",
                        node_name)));

    /* A preferred node has to be a Datanode */
    if (*is_preferred && *node_type != PGXC_NODE_DATANODE)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("PGXC node %s: cannot be a preferred node, it has to be a Datanode",
                        node_name)));

    /* Node type check */
    if (*node_type == PGXC_NODE_NONE)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("PGXC node %s: Node type not specified",
                        node_name)));

    if (*node_type == PGXC_NODE_DATANODE && NumDataNodes >= MAX_DATANODE_NUMBER)
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("Too many datanodes, current value of max_datanodes is %d",
                        MAX_DATANODE_NUMBER)));

    if (PGXC_NODE_GTM == *node_type)
    {
        /* Currently only accept master gtm, so set to  */
        if (set_node_primary_option && (false == *is_primary))
        {
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("PGXC node %s: only master gtm accepted",
                            node_name)));
        }

        /* set gtm as primary by default */
        if (!set_node_primary_option)
        {
            *is_primary = true;
        }

        if (NULL == *node_host || 0 == *node_port || !set_node_host_option || !set_node_port_option)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("PGXC node %s: host, port should be specified",
                            node_name)));
        }
    }
}

static void
get_node_cluster_name(List *options, char **node_cluster_name)
{
    ListCell *option;

    if (!options)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("No options specified")));

    /* Filter options */
    foreach (option, options)
    {
        DefElem *defel = (DefElem *)lfirst(option);

        if (strcmp(defel->defname, "node_cluster_name") == 0)
        {
            *node_cluster_name = defGetString(defel);
        }
    }
}

/* --------------------------------
 *  cmp_nodes
 *
 *  Compare the Oids of two XC nodes
 *  to sort them in ascending order by their names
 * --------------------------------
 */
static int
cmp_nodes(const void *p1, const void *p2)
{
    Oid n1 = *((Oid *)p1);
    Oid n2 = *((Oid *)p2);

    if (strcmp(get_pgxc_nodename(n1), get_pgxc_nodename(n2)) < 0)
        return -1;

    if (strcmp(get_pgxc_nodename(n1), get_pgxc_nodename(n2)) == 0)
        return 0;

    return 1;
}

/*
 * PgxcNodeListAndCount
 *
 * Update node definitions in the shared memory tables from the catalog
 */
void PgxcNodeListAndCount(void)
{ // #lizard forgives
    Relation rel;
    HeapScanDesc scan;
    HeapTuple tuple;
    NodeDefinition *nodes = NULL;
    int numNodes;
    int loop = 0;
    NodeDefLookupTag tag;
    NodeDefLookupEnt *ent;
    bool found;

    LWLockAcquire(NodeTableLock, LW_EXCLUSIVE);

    numNodes = *shmemNumCoords + *shmemNumDataNodes + *shmemNumSlaveDataNodes;

    Assert((*shmemNumCoords >= 0) && (*shmemNumDataNodes >= 0) && (*shmemNumSlaveDataNodes >= 0));

    /*
     * Save the existing health status values because nodes
     * might get added or deleted here. We will save
     * nodeoid, status. No need to differentiate between
     * coords and datanodes since oids will be unique anyways
     */
    if (numNodes > 0)
    {
        nodes = (NodeDefinition *)palloc(numNodes * sizeof(NodeDefinition));

        /* XXX It's possible to call memcpy with */
        if (*shmemNumCoords > 0)
            memcpy(nodes, coDefs, *shmemNumCoords * sizeof(NodeDefinition));

        if (*shmemNumDataNodes > 0)
            memcpy(nodes + *shmemNumCoords, dnDefs,
                   *shmemNumDataNodes * sizeof(NodeDefinition));

        if (*shmemNumSlaveDataNodes > 0)
            memcpy(nodes + *shmemNumCoords + *shmemNumDataNodes, sdnDefs,
                   *shmemNumSlaveDataNodes * sizeof(NodeDefinition));
    }

    *shmemNumCoords = 0;
    *shmemNumDataNodes = 0;
    *shmemNumSlaveDataNodes = 0;

    /*
     * Node information initialization is made in one scan:
     * 1) Scan pgxc_node catalog to find the number of nodes for
     *    each node type and make proper allocations
     * 2) Then extract the node Oid
     * 3) Complete primary/preferred node information
     */
    rel = heap_open(ForeignServerRelationId, AccessShareLock);
    scan = heap_beginscan_catalog(rel, 0, NULL);
    while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
    {
        Form_pg_foreign_server nodeForm = (Form_pg_foreign_server)GETSTRUCT(tuple);
        NodeDefinition *node = NULL;
        int i;
        Datum datum;
        bool isnull;
        char *node_type = NULL;
        DefElem *def_option;

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

        def_option = GetServerOptionWithName(NameStr(nodeForm->srvname), "nodeis_local");
        /* Take definition for given node type */
        switch (*node_type)
        {
        case PGXC_NODE_COORDINATOR:
        {
            if (IS_PGXC_SINGLE_NODE && defGetBoolean(def_option))
            {
                elog(LOG, "node %s is coordinator", PGXCNodeName);
                IS_PGXC_COORDINATOR = true;
                if (PGXCNodeName == NULL || *PGXCNodeName == '\0')
                    PGXCNodeName = strdup(NameStr(nodeForm->srvname));
            }
            node = &coDefs[(*shmemNumCoords)++];
            break;
        }
        case PGXC_NODE_DATANODE:
        {
            if (IS_PGXC_SINGLE_NODE && defGetBoolean(def_option))
            {
                elog(LOG, "node %s is datanode", PGXCNodeName);
                IS_PGXC_DATANODE = true;
                if (PGXCNodeName == NULL || *PGXCNodeName == '\0')
                    PGXCNodeName = strdup(NameStr(nodeForm->srvname));
            }
            node = &dnDefs[(*shmemNumDataNodes)++];
            break;
        }
        default:
            /*
                 * compile warning for node uninitialized, we hope nodetype were in PGXC_NODE enum.
                 */
            node = &sdnDefs[(*shmemNumSlaveDataNodes)++];
            break;
        }

        node->nodetype = *node_type;
        node->nodeislocal = defGetBoolean(def_option);
        /* Populate the definition */
        node->nodeoid = HeapTupleGetOid(tuple);
        memcpy(&node->nodename, &nodeForm->srvname, NAMEDATALEN);
        def_option = GetServerOptionWithName(NameStr(nodeForm->srvname), "host");
        memcpy(&node->nodehost, defGetString(def_option), NAMEDATALEN);

        def_option = GetServerOptionWithName(NameStr(nodeForm->srvname), "port");
        node->nodeport = strtol(defGetString(def_option), NULL, 10);

        def_option = GetServerOptionWithName(NameStr(nodeForm->srvname), "nodeis_primary");
        node->nodeisprimary = defGetBoolean(def_option);

        def_option = GetServerOptionWithName(NameStr(nodeForm->srvname), "nodeis_preferred");
        node->nodeispreferred = defGetBoolean(def_option);

        if (enable_multi_cluster_print)
            elog(LOG, "nodename %s nodehost %s nodeport %d Oid %d",
                 node->nodename.data, node->nodehost.data, node->nodeport, node->nodeoid);
        /*
         * Copy over the health status from above for nodes that
         * existed before and after the refresh. If we do not find
         * entry for a nodeoid, we mark it as healthy
         */
        node->nodeishealthy = true;
        for (i = 0; i < numNodes; i++)
        {
            if (nodes[i].nodeoid == node->nodeoid)
            {
                node->nodeishealthy = nodes[i].nodeishealthy;
                break;
            }
        }
        if (node->nodeislocal)
        {
            memcpy(&lnDef, node, sizeof(NodeDefinition));
            lnDefValid = true;
        }
    }
    heap_endscan(scan);
    heap_close(rel, AccessShareLock);

    elog(DEBUG1, "Done pgxc_nodes scan: %d coordinators and %d datanodes and %d slavedatanodes",
         *shmemNumCoords, *shmemNumDataNodes, *shmemNumSlaveDataNodes);

    if (numNodes)
        pfree(nodes);

    /* Finally sort the lists */
    if (*shmemNumCoords > 1)
        qsort(coDefs, *shmemNumCoords, sizeof(NodeDefinition), cmp_nodes);
    if (*shmemNumDataNodes > 1)
        qsort(dnDefs, *shmemNumDataNodes, sizeof(NodeDefinition), cmp_nodes);

    if (*shmemNumSlaveDataNodes > 1)
        qsort(sdnDefs, *shmemNumSlaveDataNodes, sizeof(NodeDefinition), cmp_nodes);

    /* Add to hash table */
    for (loop = 0; loop < *shmemNumCoords; loop++)
    {
        tag.nodeoid = coDefs[loop].nodeoid;
        ent = (NodeDefLookupEnt *)hash_search(g_NodeDefHashTab, (void *)&tag, HASH_ENTER, &found);
        if (!ent)
        {
            LWLockRelease(NodeTableLock);
            elog(ERROR, "corrupted node definition hash table");
        }
        ent->nodeDefIndex = loop;
    }

    for (loop = 0; loop < *shmemNumDataNodes; loop++)
    {
        tag.nodeoid = dnDefs[loop].nodeoid;
        ent = (NodeDefLookupEnt *)hash_search(g_NodeDefHashTab, (void *)&tag, HASH_ENTER, &found);
        if (!ent)
        {
            LWLockRelease(NodeTableLock);
            elog(ERROR, "corrupted node definition hash table");
        }
        ent->nodeDefIndex = loop;
    }

    for (loop = 0; loop < *shmemNumSlaveDataNodes; loop++)
    {
        tag.nodeoid = sdnDefs[loop].nodeoid;
        ent = (NodeDefLookupEnt *)hash_search(g_NodeDefHashTab, (void *)&tag, HASH_ENTER, &found);
        if (!ent)
        {
            LWLockRelease(NodeTableLock);
            elog(ERROR, "corrupted node definition hash table");
        }
        ent->nodeDefIndex = loop;
    }

    LWLockRelease(NodeTableLock);
}

/*
 * PgxcNodeGetIds
 *
 * List into palloc'ed arrays Oids of Coordinators and Datanodes currently
 * presented in the node table, as well as number of Coordinators and Datanodes.
 * Any parameter may be NULL if caller is not interested in receiving
 * appropriate results. Preferred and primary node information can be updated
 * in session if requested.
 */
void PgxcNodeGetOidsExtend(Oid **coOids, Oid **dnOids, Oid **sdnOids,
                           int *num_coords, int *num_dns, int *num_sdns, bool update_preferred)
{ // #lizard forgives
    LWLockAcquire(NodeTableLock, LW_SHARED);

    elog(DEBUG1, "Get OIDs from table: %d coordinators and %d datanodes",
         *shmemNumCoords, *shmemNumDataNodes);

    if (num_coords)
        *num_coords = *shmemNumCoords;
    if (num_dns)
        *num_dns = *shmemNumDataNodes;
    if (num_sdns)
        *num_sdns = *shmemNumSlaveDataNodes;

    if (coOids)
    {
        int i;

        *coOids = (Oid *)palloc(*shmemNumCoords * sizeof(Oid));
        for (i = 0; i < *shmemNumCoords; i++)
        {
            (*coOids)[i] = coDefs[i].nodeoid;
            elog(DEBUG1, "i %d coOid %d", i, (*coOids)[i]);
        }
    }

    if (dnOids)
    {
        int i;

        *dnOids = (Oid *)palloc(*shmemNumDataNodes * sizeof(Oid));
        for (i = 0; i < *shmemNumDataNodes; i++)
        {
            (*dnOids)[i] = dnDefs[i].nodeoid;
            elog(DEBUG1, "i %d dnOid %d", i, (*dnOids)[i]);
        }
    }

    if (sdnOids)
    {
        int i;

        *sdnOids = (Oid *)palloc(*shmemNumSlaveDataNodes * sizeof(Oid));
        for (i = 0; i < *shmemNumSlaveDataNodes; i++)
        {
            (*sdnOids)[i] = sdnDefs[i].nodeoid;
            elog(DEBUG1, "i %d sdnOid %d", i, (*sdnOids)[i]);
        }
    }
#ifdef POLARDB_X_UPGRADE
    /* Update also preferred and primary node informations if requested */
    if (update_preferred)
    {
        int i;

        /* Initialize primary and preferred node information */
        primary_data_node = InvalidOid;
        num_preferred_data_nodes = 0;

        for (i = 0; i < *shmemNumDataNodes; i++)
        {
            if (dnDefs[i].nodeisprimary)
                primary_data_node = dnDefs[i].nodeoid;

            if (dnDefs[i].nodeispreferred && num_preferred_data_nodes < MAX_PREFERRED_NODES)
            {
                preferred_data_node[num_preferred_data_nodes] = dnDefs[i].nodeoid;
                num_preferred_data_nodes++;
            }
        }
    }
#endif
    LWLockRelease(NodeTableLock);
}

/*
 * PgxcNodeGetHealthMap
 *
 * List into palloc'ed arrays Oids of Coordinators and Datanodes currently
 * presented in the node table, as well as number of Coordinators and Datanodes.
 * Any parameter may be NULL if caller is not interested in receiving
 * appropriate results for either the Coordinators or Datanodes.
 */
void PgxcNodeGetHealthMapExtend(Oid *coOids, Oid *dnOids, Oid *sdnOids,
                                int *num_coords, int *num_dns, int *num_sdns, bool *coHealthMap,
                                bool *dnHealthMap, bool *sdnHealthMap)
{ // #lizard forgives
    elog(DEBUG1, "Get HealthMap from table: %d coordinators and %d datanodes",
         *shmemNumCoords, *shmemNumDataNodes);

    LWLockAcquire(NodeTableLock, LW_SHARED);

    if (num_coords)
        *num_coords = *shmemNumCoords;
    if (num_dns)
        *num_dns = *shmemNumDataNodes;
    if (num_sdns)
        *num_sdns = *shmemNumSlaveDataNodes;

    if (coOids)
    {
        int i;
        for (i = 0; i < *shmemNumCoords; i++)
        {
            coOids[i] = coDefs[i].nodeoid;
            if (coHealthMap)
                coHealthMap[i] = coDefs[i].nodeishealthy;
        }
    }

    if (dnOids)
    {
        int i;

        for (i = 0; i < *shmemNumDataNodes; i++)
        {
            dnOids[i] = dnDefs[i].nodeoid;
            if (dnHealthMap)
                dnHealthMap[i] = dnDefs[i].nodeishealthy;
        }
    }

    if (sdnOids)
    {
        int i;

        for (i = 0; i < *shmemNumSlaveDataNodes; i++)
        {
            sdnOids[i] = sdnDefs[i].nodeoid;
            if (sdnHealthMap)
                sdnHealthMap[i] = sdnDefs[i].nodeishealthy;
        }
    }

    LWLockRelease(NodeTableLock);
}

/*
 * Consult the shared memory NodeDefinition structures and
 * fetch the nodeishealthy value and return it back
 *
 * We will probably need a similar function for coordinators
 * in the future..
 */
void PgxcNodeDnListHealth(List *nodeList, bool *healthmap)
{
    ListCell *lc;
    int index = 0;

    elog(DEBUG1, "Get healthmap from datanodeList");

    if (!nodeList || !list_length(nodeList))
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("NIL or empty nodeList passed")));

    LWLockAcquire(NodeTableLock, LW_SHARED);
    foreach (lc, nodeList)
    {
        int node = lfirst_int(lc);

        if (node >= *shmemNumDataNodes)
        {
            LWLockRelease(NodeTableLock);
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                     errmsg("PGXC health status not found for datanode with oid (%d)",
                            node)));
        }
        healthmap[index++] = dnDefs[node].nodeishealthy;
    }
    LWLockRelease(NodeTableLock);
}

/*
 * Find node definition in the shared memory node table.
 * The structure is a copy palloc'ed in current memory context.
 */
NodeDefinition *
PgxcNodeGetDefinition(Oid node)
{
    NodeDefinition *result = NULL;
    bool found;
    NodeDefLookupTag tag;
    NodeDefLookupEnt *ent;

    tag.nodeoid = node;

    LWLockAcquire(NodeTableLock, LW_SHARED);

    ent = (NodeDefLookupEnt *)hash_search(g_NodeDefHashTab, (void *)&tag, HASH_FIND, &found);
    if (found)
    {
        if (dnDefs[ent->nodeDefIndex].nodeoid == node)
        {
            result = (NodeDefinition *)palloc(sizeof(NodeDefinition));

            memcpy(result, dnDefs + ent->nodeDefIndex, sizeof(NodeDefinition));

            LWLockRelease(NodeTableLock);

            return result;
        }
        else if (coDefs[ent->nodeDefIndex].nodeoid == node)
        {
            result = (NodeDefinition *)palloc(sizeof(NodeDefinition));

            memcpy(result, coDefs + ent->nodeDefIndex, sizeof(NodeDefinition));

            LWLockRelease(NodeTableLock);

            return result;
        }
        else if (sdnDefs[ent->nodeDefIndex].nodeoid == node)
        {
            result = (NodeDefinition *)palloc(sizeof(NodeDefinition));

            memcpy(result, sdnDefs + ent->nodeDefIndex, sizeof(NodeDefinition));

            LWLockRelease(NodeTableLock);

            return result;
        }
    }

    /* not found, return NULL */
    LWLockRelease(NodeTableLock);
    return NULL;
}

/*
 * Update health status of a node in the shared memory node table.
 *
 * We could try to optimize this by checking if the ishealthy value
 * is already the same as the passed in one.. but if the cluster is
 * impaired, dunno how much such optimizations are worth. So keeping
 * it simple for now
 */
bool PgxcNodeUpdateHealth(Oid node, bool status)
{
    bool found;
    NodeDefLookupTag tag;
    NodeDefLookupEnt *ent;

    tag.nodeoid = node;

    LWLockAcquire(NodeTableLock, LW_EXCLUSIVE);

    ent = (NodeDefLookupEnt *)hash_search(g_NodeDefHashTab, (void *)&tag, HASH_FIND, &found);
    if (found)
    {
        if (dnDefs[ent->nodeDefIndex].nodeoid == node)
        {
            dnDefs[ent->nodeDefIndex].nodeishealthy = status;

            LWLockRelease(NodeTableLock);

            return true;
        }
        else if (coDefs[ent->nodeDefIndex].nodeoid == node)
        {
            coDefs[ent->nodeDefIndex].nodeishealthy = status;

            LWLockRelease(NodeTableLock);

            return true;
        }
        else if (sdnDefs[ent->nodeDefIndex].nodeoid == node)
        {
            sdnDefs[ent->nodeDefIndex].nodeishealthy = status;

            LWLockRelease(NodeTableLock);

            return true;
        }
    }

    /* not found, return false */
    LWLockRelease(NodeTableLock);
    return false;
}

DefElem *
GetServerOptionWithName(const char *server_name, const char *option_name)
{
    ListCell *lc;
    ForeignServer *server;

    server = GetForeignServerByName(server_name, false);
    foreach (lc, server->options)
    {
        DefElem *def = (DefElem *)lfirst(lc);
        if (strcmp(def->defname, option_name) == 0)
        {
            return def;
        }
    }
    return NULL;
}

static DefElem *
GetServerOptionInternal(List *options, const char *option_name)
{
    ListCell *lc;

    foreach (lc, options)
    {
        DefElem *def = (DefElem *)lfirst(lc);
        if (strcmp(def->defname, option_name) == 0)
        {
            return def;
        }
    }
    return NULL;
}
/*
 * get_pgxc_nodeoid
 *        Obtain PGXC Node Oid for given node name
 *        Return Invalid Oid if object does not exist
 */
Oid get_pgxc_nodeoid_extend(const char *nodename, const char *clustername)
{
    ForeignServer *server;
    DefElem *def_option;

    if (nodename)
        server = GetForeignServerByName(nodename, false);
    else
        elog(ERROR, "nodename is NULL");

    if (!clustername)
        elog(ERROR, "clustername is NULL");

    if (server)
    {
        def_option = GetServerOptionInternal(server->options, "node_cluster_name");
        if (!def_option)
            elog(ERROR, "nodename %s does not have option node_cluster_name", nodename);
        if (strcmp(clustername, defGetString(def_option)) != 0)
            elog(ERROR, "nodename %s clustername %s does not exsit", nodename, clustername);
    }
    else
        elog(ERROR, "nodename %s doesn't exsit", nodename);

    pfree(server);

    return get_foreign_server_oid(nodename, false);
}

/*
 * get_pgxc_nodename
 *        Get node name for given Oid
 */
char *
get_pgxc_nodename(Oid nodeid)
{
    HeapTuple tuple;
    Form_pg_foreign_server nodeForm;
    char *result;

    tuple = SearchSysCache1(FOREIGNSERVEROID, ObjectIdGetDatum(nodeid));

    if (!HeapTupleIsValid(tuple))
        elog(ERROR, "cache lookup failed for node %u", nodeid);

    nodeForm = (Form_pg_foreign_server)GETSTRUCT(tuple);
    result = pstrdup(NameStr(nodeForm->srvname));
    ReleaseSysCache(tuple);

    return result;
}

/*
 * get_pgxc_node_id
 *        Get node identifier for a given Oid
 */
uint32
get_pgxc_node_id(Oid nodeid)
{
    ForeignServer *server;
    DefElem *def_option;
    uint32 result;

    if (nodeid == InvalidOid)
        return 0;

    server = GetForeignServer(ObjectIdGetDatum(nodeid));

    if (!server)
        elog(ERROR, "cache lookup failed for node %u", nodeid);

    def_option = GetServerOptionInternal(server->options, "node_id");
    result = strtol(defGetString(def_option), NULL, 10);

    pfree(server);

    return result;
}

/*
 * get_pgxc_nodetype
 *        Get node type for given Oid
 */
char get_pgxc_nodetype(Oid nodeid)
{
    HeapTuple tuple;
    char *result;
    Datum datum;
    bool isnull;

    tuple = SearchSysCache1(FOREIGNSERVEROID, ObjectIdGetDatum(nodeid));

    if (!HeapTupleIsValid(tuple))
        elog(ERROR, "cache lookup failed for node %u", nodeid);

    datum = SysCacheGetAttr(FOREIGNSERVEROID,
                            tuple,
                            Anum_pg_foreign_server_srvtype,
                            &isnull);
    result = isnull ? NULL : TextDatumGetCString(datum);
    if (isnull)
        elog(ERROR, "node %u is corrupt, node typy is NULL", nodeid);

    ReleaseSysCache(tuple);

    return *result;
}

/*
 * get_pgxc_nodeport
 *        Get node port for given Oid
 */
int get_pgxc_nodeport(Oid nodeid)
{
    ForeignServer *server;
    DefElem *def_option;
    uint32 result;

    if (nodeid == InvalidOid)
        return 0;

    server = GetForeignServer(ObjectIdGetDatum(nodeid));

    if (!server)
        elog(ERROR, "cache lookup failed for node %u", nodeid);

    def_option = GetServerOptionInternal(server->options, "port");
    result = strtol(defGetString(def_option), NULL, 10);

    pfree(server);

    return result;
}

/*
 * get_pgxc_nodehost
 *        Get node host for given Oid
 */
char *
get_pgxc_nodehost(Oid nodeid)
{
    ForeignServer *server;
    DefElem *def_option;
    char *result;

    if (nodeid == InvalidOid)
        return 0;

    server = GetForeignServer(ObjectIdGetDatum(nodeid));

    if (!server)
        elog(ERROR, "cache lookup failed for node %u", nodeid);

    def_option = GetServerOptionInternal(server->options, "host");

    result = pstrdup(defGetString(def_option));

    pfree(server);

    return result;
}

/*
 * is_pgxc_nodepreferred
 *        Determine if node is a preferred one
 */
bool is_pgxc_nodepreferred(Oid nodeid)
{
    ForeignServer *server;
    DefElem *def_option;
    uint32 result;

    if (nodeid == InvalidOid)
        return 0;

    server = GetForeignServer(ObjectIdGetDatum(nodeid));

    if (!server)
        elog(ERROR, "cache lookup failed for node %u", nodeid);

    def_option = GetServerOptionInternal(server->options, "nodeis_preferred");

    result = defGetBoolean(def_option);

    pfree(server);

    return result;
}

/*
 * is_pgxc_nodeprimary
 *        Determine if node is a primary one
 */
bool is_pgxc_nodeprimary(Oid nodeid)
{
    ForeignServer *server;
    DefElem *def_option;
    uint32 result;

    if (nodeid == InvalidOid)
        return 0;

    server = GetForeignServer(ObjectIdGetDatum(nodeid));

    if (!server)
        elog(ERROR, "cache lookup failed for node %u", nodeid);

    def_option = GetServerOptionInternal(server->options, "nodeis_primary");

    result = defGetBoolean(def_option);

    pfree(server);

    return result;
}

/*
 * is_pgxc_nodelocal
 *        Determine if node is a local one
 */
bool is_pgxc_nodelocal(Oid nodeid)
{
    ForeignServer *server;
    DefElem *def_option;
    uint32 result;

    if (nodeid == InvalidOid)
        return 0;

    server = GetForeignServer(ObjectIdGetDatum(nodeid));

    if (!server)
        elog(ERROR, "cache lookup failed for node %u", nodeid);

    def_option = GetServerOptionInternal(server->options, "nodeis_local");

    result = defGetBoolean(def_option);

    pfree(server);

    return result;
}

NodeDefinition *
get_polar_local_node_def(void)
{
    if (!lnDefValid)
    {
        int i = 0;

        LWLockAcquire(NodeTableLock, LW_SHARED);
        for (i = 0; lnDefValid == false && i < *shmemNumCoords; i++)
            if (coDefs[i].nodeislocal)
            {
                memcpy(&lnDef, coDefs + i, sizeof(NodeDefinition));
                lnDefValid = true;
            }
        for (i = 0; lnDefValid == false && i < *shmemNumDataNodes; i++)
            if (dnDefs[i].nodeislocal)
            {
                memcpy(&lnDef, dnDefs + i, sizeof(NodeDefinition));
                lnDefValid = true;
            }
        for (i = 0; lnDefValid == false && i < *shmemNumSlaveDataNodes; i++)
            if (sdnDefs[i].nodeislocal)
            {
                memcpy(&lnDef, sdnDefs + i, sizeof(NodeDefinition));
                lnDefValid = true;
            }
        LWLockRelease(NodeTableLock);
        if (lnDefValid == false)
            return NULL;
    }

    return &lnDef;
}

/*
 * get_polar_local_nodeoid
 *        Obtain local Node Oid for given node name
 *        Return Invalid Oid if object does not exist
 */
Oid get_polar_local_nodeoid(void)
{
    if (!lnDefValid)
    {
        if (get_polar_local_node_def() == NULL)
            elog(ERROR, "local node is not exsit");
    }
    return lnDef.nodeoid;
}

/*
 * get_polar_local_nodename
 *        Get node name for given Oid
 */
char *
get_polar_local_nodename(void)
{
    if (!lnDefValid)
    {
        if (get_polar_local_node_def() == NULL)
            elog(ERROR, "local node is not exsit");
    }

    return NameStr(lnDef.nodename);
}

/*
 * get_polar_local_nodetype
 *        Get node type for given Oid
 */
char get_polar_local_nodetype(void)
{
    if (!lnDefValid)
    {
        if (get_polar_local_node_def() == NULL)
            elog(ERROR, "local node is not exsit");
    }

    return lnDef.nodetype;
}

/*
 * get_polar_local_nodeport
 *        Get node port for given Oid
 */
int get_polar_local_nodeport(void)
{
    if (!lnDefValid)
    {
        if (get_polar_local_node_def() == NULL)
            elog(ERROR, "local node is not exsit");
    }

    return lnDef.nodeport;
}

/*
 * get_polar_local_nodehost
 *        Get node host for given Oid
 */
char *
get_polar_local_nodehost(void)
{
    if (!lnDefValid)
    {
        if (get_polar_local_node_def() == NULL)
            elog(ERROR, "local node is not exsit");
    }

    return NameStr(lnDef.nodehost);
}

/*
 * is_polar_local_nodepreferred
 *        Determine if node is a preferred one
 */
bool is_polar_local_nodepreferred(void)
{
    if (!lnDefValid)
    {
        if (get_polar_local_node_def() == NULL)
            elog(ERROR, "local node is not exsit");
    }

    return lnDef.nodeispreferred;
}

/*
 * is_polar_local_nodeprimary
 *        Determine if node is a primary one
 */
bool is_polar_local_nodeprimary(void)
{
    if (!lnDefValid)
    {
        if (get_polar_local_node_def() == NULL)
            elog(ERROR, "local node is not exsit");
    }

    return lnDef.nodeisprimary;
}
