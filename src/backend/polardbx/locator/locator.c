/*-------------------------------------------------------------------------
 *
 * locator.c
 *        Functions that help manage table location information such as
 * partitioning and replication information.
 *
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *        $$
 *
 *-------------------------------------------------------------------------
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>

#include "postgres.h"
#include "access/skey.h"
#include "access/relscan.h"
#include "catalog/indexing.h"
#include "catalog/pg_type.h"
#include "nodes/pg_list.h"
#include "nodes/nodeFuncs.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/tqual.h"
#include "utils/syscache.h"
#include "nodes/nodes.h"
#include "optimizer/clauses.h"
#include "parser/parse_coerce.h"
#include "pgxc/nodemgr.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "catalog/namespace.h"
#include "access/hash.h"
#include "utils/date.h"
#include "utils/memutils.h"
#include "parser/parsetree.h"
#include "foreign/foreign.h"
#include "commands/defrem.h"
#include "access/reloptions.h"
#include "catalog/pg_foreign_table.h"
#include "pgxc/mdcache.h"

/*
 * Locator details are private
 */
struct _Locator
{
    /*
     * Determine target nodes for value.
     * Resulting nodes are stored to the results array.
     * Function returns number of node references written to the array.
     */
    int (*locatefunc)(Locator *self, Datum value, bool isnull,
                      bool *hasprimary);
    Oid dataType; /* values of that type are passed to locateNodes function */
    LocatorListType listType;
    bool primary;

    /* locator-specific data */
    /* XXX: move them into union ? */
    int roundRobinNode;       /* for LOCATOR_TYPE_RROBIN */
    LocatorHashFunc hashfunc; /* for LOCATOR_TYPE_HASH */
    int valuelen;             /* 1, 2 or 4 for LOCATOR_TYPE_MODULO */

    int nodeCount; /* How many nodes are in the map */
    void *nodeMap; /* map index to node reference according to listType */
    void *results; /* array to output results */
};

Oid primary_data_node = InvalidOid;
int num_preferred_data_nodes = 0;
Oid preferred_data_node[MAX_PREFERRED_NODES];

static int modulo_value_len(Oid dataType);
static int locate_static(Locator *self, Datum value, bool isnull,
                         bool *hasprimary);
static int locate_roundrobin(Locator *self, Datum value, bool isnull,
                             bool *hasprimary);
static int locate_modulo_random(Locator *self, Datum value, bool isnull,
                                bool *hasprimary);
static int locate_hash_insert(Locator *self, Datum value, bool isnull,
                              bool *hasprimary);
static int locate_hash_select(Locator *self, Datum value, bool isnull,
                              bool *hasprimary);

static int locate_modulo_insert(Locator *self, Datum value, bool isnull,
                                bool *hasprimary);
static int locate_modulo_select(Locator *self, Datum value, bool isnull,
                                bool *hasprimary);
static char *get_cluster_table_option(List *options, TableOption type);
static ForeignTable *get_foreign_table(Oid relid);

/*
 * GetPreferredReplicationNode
 * Pick any Datanode from given list, however fetch a preferred node first.
 */
List *
GetPreferredReplicationNode(List *relNodes)
{
    ListCell *item;
    int nodeid = -1;

    if (list_length(relNodes) <= 0)
        elog(ERROR, "a list of nodes should have at least one node");

    foreach (item, relNodes)
    {
        int cnt_nodes;
        char nodetype = PGXC_NODE_DATANODE;
        for (cnt_nodes = 0;
             cnt_nodes < num_preferred_data_nodes && nodeid < 0;
             cnt_nodes++)
        {
            if (PGXCNodeGetNodeId(preferred_data_node[cnt_nodes],
                                  &nodetype) == lfirst_int(item))
                nodeid = lfirst_int(item);
        }
        if (nodeid >= 0)
            break;
    }
    if (nodeid < 0)
        return list_make1_int(list_nth_int(relNodes,
                                           ((unsigned int)random()) % list_length(relNodes)));

    return list_make1_int(nodeid);
}

/*
 * GetAnyDataNode
 * Pick any data node from given set, but try a preferred node
 */
int GetAnyDataNode(Bitmapset *nodes)
{
    Bitmapset *preferred = NULL;
    int i, nodeid;
    int nmembers = 0;
    int members[NumDataNodes];

    for (i = 0; i < num_preferred_data_nodes; i++)
    {
        char ntype = PGXC_NODE_DATANODE;
        nodeid = PGXCNodeGetNodeId(preferred_data_node[i], &ntype);

        /* OK, found one */
        if (bms_is_member(nodeid, nodes))
            preferred = bms_add_member(preferred, nodeid);
    }

    /*
     * If no preferred data nodes or they are not in the desired set, pick up
     * from the original set.
     */
    if (bms_is_empty(preferred))
        preferred = bms_copy(nodes);

    /*
     * Load balance.
     * We can not get item from the set, convert it to array
     */
    while ((nodeid = bms_first_member(preferred)) >= 0)
        members[nmembers++] = nodeid;
    bms_free(preferred);

    /* If there is a single member nothing to balance */
    if (nmembers == 1)
        return members[0];

    /*
     * In general, the set may contain any number of nodes, and if we save
     * previous returned index for load balancing the distribution won't be
     * flat, because small set will probably reset saved value, and lower
     * indexes will be picked up more often.
     * So we just get a random value from 0..nmembers-1.
     */
    return members[((unsigned int)random()) % nmembers];
}

/*
 * compute_modulo
 *    Computes modulo of two 64-bit unsigned values.
 */
static int
compute_modulo(uint64 numerator, uint64 denominator)
{
    Assert(denominator > 0);

    return numerator % denominator;
}

/*
 * GetRelationDistColumn - Returns the name of the hash or modulo distribution column
 * First hash distribution is checked
 * Retuens NULL if the table is neither hash nor modulo distributed
 */
char *
GetRelationDistColumn(Oid relid)
{
    char *pColName;
    RelationLocInfo *rel_loc_info = GetRelationLocInfo(relid);

    pColName = NULL;

    pColName = GetRelationHashColumn(rel_loc_info);
    if (pColName == NULL)
        pColName = GetRelationModuloColumn(rel_loc_info);

    return pColName;
}

/*
 * Returns whether or not the data type is hash distributable with PG-XC
 * PGXCTODO - expand support for other data types!
 */
bool IsTypeHashDistributable(Oid col_type)
{
    return (hash_func_ptr(col_type) != NULL);
}

/*
 * GetRelationHashColumn - return hash column for relation.
 *
 * Returns NULL if the relation is not hash partitioned.
 */
char *
GetRelationHashColumn(RelationLocInfo *rel_loc_info)
{
    char *column_str = NULL;

    if (rel_loc_info == NULL)
        column_str = NULL;
    else if (rel_loc_info->locatorType != LOCATOR_TYPE_HASH)
        column_str = NULL;
    else
    {
        int len = strlen(rel_loc_info->partAttrName);

        column_str = (char *)palloc(len + 1);
        strncpy(column_str, rel_loc_info->partAttrName, len + 1);
    }

    return column_str;
}

/*
 * IsDistColumnForRelId - return whether or not column for relation is used for hash or modulo distribution
 *
 */
bool IsDistColumnForRelId(Oid relid, char *part_col_name)
{
    RelationLocInfo *rel_loc_info;

    /* if no column is specified, we're done */
    if (!part_col_name)
        return false;

    /* if no locator, we're done too */
    if (!(rel_loc_info = GetRelationLocInfo(relid)))
        return false;

    /* is the table distributed by column value */
    if (!IsRelationDistributedByValue(rel_loc_info))
        return false;

    /* does the column name match the distribution column */
    return !strcmp(part_col_name, rel_loc_info->partAttrName);
}

/*
 * Returns whether or not the data type is modulo distributable with PG-XC
 * PGXCTODO - expand support for other data types!
 */
bool IsTypeModuloDistributable(Oid col_type)
{
    return (modulo_value_len(col_type) != -1);
}

/*
 * GetRelationModuloColumn - return modulo column for relation.
 *
 * Returns NULL if the relation is not modulo partitioned.
 */
char *
GetRelationModuloColumn(RelationLocInfo *rel_loc_info)
{
    char *column_str = NULL;

    if (rel_loc_info == NULL)
        column_str = NULL;
    else if (rel_loc_info->locatorType != LOCATOR_TYPE_MODULO)
        column_str = NULL;
    else
    {
        int len = strlen(rel_loc_info->partAttrName);

        column_str = (char *)palloc(len + 1);
        strncpy(column_str, rel_loc_info->partAttrName, len + 1);
    }

    return column_str;
}

/*
 * IsTableDistOnPrimary
 *
 * Does the table distribution list include the primary node?
 */
bool IsTableDistOnPrimary(RelationLocInfo *rel_loc_info)
{
    ListCell *item;

    if (!OidIsValid(primary_data_node) ||
        rel_loc_info == NULL ||
        list_length(rel_loc_info->rl_nodeList = 0))
        return false;

    foreach (item, rel_loc_info->rl_nodeList)
    {
        char ntype = PGXC_NODE_DATANODE;
        if (PGXCNodeGetNodeId(primary_data_node, &ntype) == lfirst_int(item))
            return true;
    }
    return false;
}

/*
 * IsLocatorInfoEqual
 * Check equality of given locator information
 */
bool IsLocatorInfoEqual(RelationLocInfo *rel_loc_info1, RelationLocInfo *rel_loc_info2)
{
    List *nodeList1, *nodeList2;
    Assert(rel_loc_info1 && rel_loc_info2);

    nodeList1 = rel_loc_info1->rl_nodeList;
    nodeList2 = rel_loc_info2->rl_nodeList;

    /* Same relation? */
    if (rel_loc_info1->relid != rel_loc_info2->relid)
        return false;

    /* Same locator type? */
    if (rel_loc_info1->locatorType != rel_loc_info2->locatorType)
        return false;

    /* Same attribute number? */
    if (rel_loc_info1->partAttrNum != rel_loc_info2->partAttrNum)
        return false;

    /* Same node list? */
    if (list_difference_int(nodeList1, nodeList2) != NIL ||
        list_difference_int(nodeList2, nodeList1) != NIL)
        return false;

    /* Everything is equal */
    return true;
}

/*
 * ConvertToLocatorType
 *        get locator distribution type
 * We really should just have pgxc_class use disttype instead...
 */
char ConvertToLocatorType(int disttype)
{
    char loctype = LOCATOR_TYPE_NONE;

    switch (disttype)
    {
    case DISTTYPE_HASH:
        loctype = LOCATOR_TYPE_HASH;
        break;
    case DISTTYPE_ROUNDROBIN:
        loctype = LOCATOR_TYPE_RROBIN;
        break;
    case DISTTYPE_REPLICATION:
        loctype = LOCATOR_TYPE_REPLICATED;
        break;
    case DISTTYPE_MODULO:
        loctype = LOCATOR_TYPE_MODULO;
        break;
    default:
        ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("Invalid distribution type")));
        break;
    }

    return loctype;
}

/*
 * GetLocatorType - Returns the locator type of the table
 *
 */
char GetLocatorType(Oid relid)
{
    char ret = '\0';

    RelationLocInfo *ret_loc_info = GetRelationLocInfo(relid);

    if (ret_loc_info != NULL)
        ret = ret_loc_info->locatorType;

    return ret;
}

/*
 * Return a list of all Datanodes.
 * We assume all tables use all nodes in the prototype, so just return a list
 * from first one.
 */
List *
GetAllDataNodes(void)
{
    int i;
    List *nodeList = NIL;

    for (i = 0; i < NumDataNodes; i++)
        nodeList = lappend_int(nodeList, i);

    return nodeList;
}

/*
 * Return a list of all Coordinators
 * This is used to send DDL to all nodes and to clean up pooler connections.
 * Do not put in the list the local Coordinator where this function is launched.
 */
List *
GetAllCoordNodes(void)
{
    int i;
    List *nodeList = NIL;

    for (i = 0; i < NumCoords; i++)
    {
        /*
         * Do not put in list the Coordinator we are on,
         * it doesn't make sense to connect to the local Coordinator.
         */

        if (i != PGXCNodeId - 1)
            nodeList = lappend_int(nodeList, i);
    }

    return nodeList;
}

/*
 * GetLocatorRelationInfo - Returns the locator information for relation,
 * in a copy of the RelationLocatorInfo struct in relcache
 */
RelationLocInfo *
GetRelationLocInfo(Oid relid)
{
    RelationLocInfo *relationLocInfo = NULL;
    ForeignTable *table;
    DistRelCacheEntry *relcache;
    int j;
    char *locatorType = NULL;
    bool found;
    MemoryContext oldcontext;

    relcache = GetDisRelationCache(relid);

    if (relcache)
        return relcache->locInfo;

    table = get_foreign_table(relid);
    if (table == NULL)
        return NULL;

    oldcontext = MemoryContextSwitchTo(CacheMemoryContext);

    locatorType = get_cluster_table_option(table->options,
                                           TABLE_OPTION_LOCATOR_TYPE);
    if (locatorType == NULL)
    {
        MemoryContextSwitchTo(oldcontext);
        return NULL;
    }

    relcache = CreateDisRelationCache(relid);
    relationLocInfo = relcache->locInfo;
    relationLocInfo->relid = relid;
    relationLocInfo->locatorType = *locatorType;
    if (IsLocatorDistributedByValue(relationLocInfo->locatorType))
    {
        char *dist_col_name = get_cluster_table_option(table->options, TABLE_OPTION_PART_ATTR_NAME);
        if (dist_col_name == NULL)
            elog(ERROR, "table: %u, dist_col_name is NULL", table->relid);

        relationLocInfo->partAttrNum = get_attnum(table->relid, dist_col_name);
        if (relationLocInfo->partAttrNum == 0)
            elog(ERROR, "table: %u, dist_col_name %s is not exist", table->relid, dist_col_name);
    }

    relationLocInfo->partAttrName = get_attname(relationLocInfo->relid, relationLocInfo->partAttrNum, true);

    relationLocInfo->rl_nodeList = NIL;

    for (j = 0; j < NumDataNodes; j++)
        relationLocInfo->rl_nodeList = lappend_int(relationLocInfo->rl_nodeList, j);

    /*
        * If the locator type is round robin, we set a node to
        * use next time. In addition, if it is replicated,
        * we choose a node to use for balancing reads.
        */
    if (relationLocInfo->locatorType == LOCATOR_TYPE_RROBIN || IsLocatorReplicated(relationLocInfo->locatorType))
    {
        int offset;
        /*
            * pick a random one to start with,
            * since each process will do this independently
            */
        offset = compute_modulo(abs(rand()), list_length(relationLocInfo->rl_nodeList));

        relationLocInfo->roundRobinNode = relationLocInfo->rl_nodeList->head; /* initialize */
        for (j = 0; j < offset && relationLocInfo->roundRobinNode->next != NULL; j++)
            relationLocInfo->roundRobinNode = relationLocInfo->roundRobinNode->next;
    }

    relcache->valid = true;
    MemoryContextSwitchTo(oldcontext);
    return relationLocInfo;
}

/*
 * Get the distribution type of relation.
 */
char GetRelationLocType(Oid relid)
{
    ForeignTable *table;
    char *locatorType = NULL;
    char result = '\0';

    table = get_foreign_table(relid);
    if (table)
    {
        locatorType = get_cluster_table_option(table->options,
                                               TABLE_OPTION_LOCATOR_TYPE);
        if (locatorType)
            result = *locatorType;
    }

    return result;
}

/*
 * Copy the RelationLocInfo struct
 */
RelationLocInfo *
CopyRelationLocInfo(RelationLocInfo *src_info)
{
    RelationLocInfo *dest_info;

    Assert(src_info);

    dest_info = (RelationLocInfo *)palloc0(sizeof(RelationLocInfo));

    dest_info->relid = src_info->relid;
    dest_info->locatorType = src_info->locatorType;
    dest_info->partAttrNum = src_info->partAttrNum;
    if (src_info->partAttrName)
        dest_info->partAttrName = pstrdup(src_info->partAttrName);

    if (src_info->rl_nodeList)
        dest_info->rl_nodeList = list_copy(src_info->rl_nodeList);
    /* Note, for round robin, we use the relcache entry */

    return dest_info;
}

/*
 * Free RelationLocInfo struct
 */
void FreeRelationLocInfo(RelationLocInfo *relationLocInfo)
{
    if (relationLocInfo)
    {
        if (relationLocInfo->partAttrName)
            pfree(relationLocInfo->partAttrName);
        pfree(relationLocInfo);
    }
}

/*
 * Free the contents of the ExecNodes expression */
void FreeExecNodes(ExecNodes **exec_nodes)
{
    ExecNodes *tmp_en = *exec_nodes;

    /* Nothing to do */
    if (!tmp_en)
        return;
    list_free(tmp_en->primarynodelist);
    list_free(tmp_en->nodeList);
    pfree(tmp_en);
    *exec_nodes = NULL;
}

/*
 * Determine value length in bytes for specified type for a module locator.
 * Return -1 if module locator is not supported for the type.
 */
static int
modulo_value_len(Oid dataType)
{ // #lizard forgives
    switch (dataType)
    {
    case BOOLOID:
    case CHAROID:
        return 1;
    case INT2OID:
        return 2;
    case INT4OID:
    case ABSTIMEOID:
    case RELTIMEOID:
    case DATEOID:
        return 4;
    case INT8OID:
        return 8;
    default:
        return -1;
    }
}

LocatorHashFunc
hash_func_ptr(Oid dataType)
{ // #lizard forgives
    switch (dataType)
    {
    case INT8OID:
    case CASHOID:
        return hashint8;
    case INT2OID:
        return hashint2;
    case OIDOID:
        return hashoid;
    case INT4OID:
    case ABSTIMEOID:
    case RELTIMEOID:
    case DATEOID:
        return hashint4;
    case FLOAT4OID:
        return hashfloat4;
    case FLOAT8OID:
        return hashfloat8;
    case JSONBOID:
        return jsonb_hash;
    case BOOLOID:
    case CHAROID:
        return hashchar;
    case NAMEOID:
        return hashname;
    case VARCHAROID:
    case TEXTOID:
        return hashtext;
    case OIDVECTOROID:
        return hashoidvector;
    case BPCHAROID:
        return hashbpchar;
    case BYTEAOID:
        return hashvarlena;
    case TIMEOID:
        return time_hash;
    case TIMESTAMPOID:
    case TIMESTAMPTZOID:
        return timestamp_hash;
    case INTERVALOID:
        return interval_hash;
    case TIMETZOID:
        return timetz_hash;
    case NUMERICOID:
        return hash_numeric;
    case UUIDOID:
        return uuid_hash;
    default:
        return NULL;
    }
}

Locator *
createLocator(char locatorType, RelationAccessType accessType,
              Oid dataType, LocatorListType listType, int nodeCount,
              void *nodeList, void **result, bool primary)
{ // #lizard forgives
    Locator *locator;
    ListCell *lc;
    void *nodeMap = NULL;
    int i;

    locator = (Locator *)palloc(sizeof(Locator));
    locator->dataType = dataType;
    locator->listType = listType;
    locator->nodeCount = nodeCount;

    /* Create node map */
    switch (listType)
    {
    case LOCATOR_LIST_NONE:
        /* No map, return indexes */
        break;
    case LOCATOR_LIST_INT:
        /* Copy integer array */
        nodeMap = palloc(nodeCount * sizeof(int));
        memcpy(nodeMap, nodeList, nodeCount * sizeof(int));
        break;
    case LOCATOR_LIST_OID:
        /* Copy array of Oids */
        nodeMap = palloc(nodeCount * sizeof(Oid));
        memcpy(nodeMap, nodeList, nodeCount * sizeof(Oid));
        break;
    case LOCATOR_LIST_POINTER:
        /* Copy array of Oids */
        nodeMap = palloc(nodeCount * sizeof(void *));
        memcpy(nodeMap, nodeList, nodeCount * sizeof(void *));
        break;
    case LOCATOR_LIST_LIST:
        /* Create map from list */
        {
            List *l = (List *)nodeList;
            locator->nodeCount = list_length(l);
            if (IsA(l, IntList))
            {
                int *intptr;
                nodeMap = palloc(locator->nodeCount * sizeof(int));
                intptr = (int *)nodeMap;
                foreach (lc, l)
                    *intptr++ = lfirst_int(lc);
                locator->listType = LOCATOR_LIST_INT;
            }
            else if (IsA(l, OidList))
            {
                Oid *oidptr;
                nodeMap = palloc(locator->nodeCount * sizeof(Oid));
                oidptr = (Oid *)nodeMap;
                foreach (lc, l)
                    *oidptr++ = lfirst_oid(lc);
                locator->listType = LOCATOR_LIST_OID;
            }
            else if (IsA(l, List))
            {
                void **voidptr;
                nodeMap = palloc(locator->nodeCount * sizeof(void *));
                voidptr = (void **)nodeMap;
                foreach (lc, l)
                    *voidptr++ = lfirst(lc);
                locator->listType = LOCATOR_LIST_POINTER;
            }
            else
            {
                /* can not get here */
                Assert(false);
            }
            break;
        }
    }
    /*
     * Determine locatefunc, allocate results, set up parameters
     * specific to locator type
     */
    switch (locatorType)
    {
    case LOCATOR_TYPE_REPLICATED:
        if (accessType == RELATION_ACCESS_INSERT ||
            accessType == RELATION_ACCESS_UPDATE ||
            accessType == RELATION_ACCESS_READ_FQS ||
            accessType == RELATION_ACCESS_READ_FOR_UPDATE)
        {
            locator->locatefunc = locate_static;
            if (nodeMap == NULL)
            {
                /* no map, prepare array with indexes */
                int *intptr;
                nodeMap = palloc(locator->nodeCount * sizeof(int));
                intptr = (int *)nodeMap;
                for (i = 0; i < locator->nodeCount; i++)
                    *intptr++ = i;
            }
            locator->nodeMap = nodeMap;
            locator->results = nodeMap;
        }
        else
        {
            /* SELECT, use random node.. */
            locator->locatefunc = locate_modulo_random;
            locator->nodeMap = nodeMap;
            switch (locator->listType)
            {
            case LOCATOR_LIST_NONE:
            case LOCATOR_LIST_INT:
                locator->results = palloc(sizeof(int));
                break;
            case LOCATOR_LIST_OID:
                locator->results = palloc(sizeof(Oid));
                break;
            case LOCATOR_LIST_POINTER:
                locator->results = palloc(sizeof(void *));
                break;
            case LOCATOR_LIST_LIST:
                /* Should never happen */
                Assert(false);
                break;
            }
            locator->roundRobinNode = -1;
        }
        break;
    case LOCATOR_TYPE_RROBIN:
        if (accessType == RELATION_ACCESS_INSERT)
        {
            locator->locatefunc = locate_roundrobin;
            locator->nodeMap = nodeMap;
            switch (locator->listType)
            {
            case LOCATOR_LIST_NONE:
            case LOCATOR_LIST_INT:
                locator->results = palloc(sizeof(int));
                break;
            case LOCATOR_LIST_OID:
                locator->results = palloc(sizeof(Oid));
                break;
            case LOCATOR_LIST_POINTER:
                locator->results = palloc(sizeof(void *));
                break;
            case LOCATOR_LIST_LIST:
                /* Should never happen */
                Assert(false);
                break;
            }
            /* randomize choice of the initial node */
            locator->roundRobinNode = (abs(rand()) % locator->nodeCount) - 1;
        }
        else
        {
            locator->locatefunc = locate_static;
            if (nodeMap == NULL)
            {
                /* no map, prepare array with indexes */
                int *intptr;
                nodeMap = palloc(locator->nodeCount * sizeof(int));
                intptr = (int *)nodeMap;
                for (i = 0; i < locator->nodeCount; i++)
                    *intptr++ = i;
            }
            locator->nodeMap = nodeMap;
            locator->results = nodeMap;
        }
        break;
    case LOCATOR_TYPE_HASH:
        if (accessType == RELATION_ACCESS_INSERT)
        {
            locator->locatefunc = locate_hash_insert;
            locator->nodeMap = nodeMap;
            switch (locator->listType)
            {
            case LOCATOR_LIST_NONE:
            case LOCATOR_LIST_INT:
                locator->results = palloc(sizeof(int));
                break;
            case LOCATOR_LIST_OID:
                locator->results = palloc(sizeof(Oid));
                break;
            case LOCATOR_LIST_POINTER:
                locator->results = palloc(sizeof(void *));
                break;
            case LOCATOR_LIST_LIST:
                /* Should never happen */
                Assert(false);
                break;
            }
        }
        else
        {
            locator->locatefunc = locate_hash_select;
            locator->nodeMap = nodeMap;
            switch (locator->listType)
            {
            case LOCATOR_LIST_NONE:
            case LOCATOR_LIST_INT:
                locator->results = palloc(locator->nodeCount * sizeof(int));
                break;
            case LOCATOR_LIST_OID:
                locator->results = palloc(locator->nodeCount * sizeof(Oid));
                break;
            case LOCATOR_LIST_POINTER:
                locator->results = palloc(locator->nodeCount * sizeof(void *));
                break;
            case LOCATOR_LIST_LIST:
                /* Should never happen */
                Assert(false);
                break;
            }
        }

        locator->hashfunc = hash_func_ptr(dataType);
        if (locator->hashfunc == NULL)
            ereport(ERROR, (errmsg("Error: unsupported data type for HASH locator: %d\n",
                                   dataType)));
        break;
    case LOCATOR_TYPE_MODULO:
        if (accessType == RELATION_ACCESS_INSERT)
        {
            locator->locatefunc = locate_modulo_insert;
            locator->nodeMap = nodeMap;
            switch (locator->listType)
            {
            case LOCATOR_LIST_NONE:
            case LOCATOR_LIST_INT:
                locator->results = palloc(sizeof(int));
                break;
            case LOCATOR_LIST_OID:
                locator->results = palloc(sizeof(Oid));
                break;
            case LOCATOR_LIST_POINTER:
                locator->results = palloc(sizeof(void *));
                break;
            case LOCATOR_LIST_LIST:
                /* Should never happen */
                Assert(false);
                break;
            }
        }
        else
        {
            locator->locatefunc = locate_modulo_select;
            locator->nodeMap = nodeMap;
            switch (locator->listType)
            {
            case LOCATOR_LIST_NONE:
            case LOCATOR_LIST_INT:
                locator->results = palloc(locator->nodeCount * sizeof(int));
                break;
            case LOCATOR_LIST_OID:
                locator->results = palloc(locator->nodeCount * sizeof(Oid));
                break;
            case LOCATOR_LIST_POINTER:
                locator->results = palloc(locator->nodeCount * sizeof(void *));
                break;
            case LOCATOR_LIST_LIST:
                /* Should never happen */
                Assert(false);
                break;
            }
        }

        locator->valuelen = modulo_value_len(dataType);
        if (locator->valuelen == -1)
            ereport(ERROR, (errmsg("Error: unsupported data type for MODULO locator: %d\n",
                                   dataType)));
        break;
    default:
        ereport(ERROR, (errmsg("Error: no such supported locator type: %c\n",
                               locatorType)));
    }

    if (result)
        *result = locator->results;

    return locator;
}

void freeLocator(Locator *locator)
{
    pfree(locator->nodeMap);
    /*
     * locator->nodeMap and locator->results may point to the same memory,
     * do not free it twice
     */
    if (locator->results != locator->nodeMap)
        pfree(locator->results);
    pfree(locator);
}

/*
 * Each time return the same predefined results
 */
static int
locate_static(Locator *self, Datum value, bool isnull,
              bool *hasprimary)
{
    /* TODO */
    if (hasprimary)
        *hasprimary = false;
    return self->nodeCount;
}

/*
 * Each time return one next node, in round robin manner
 */
static int
locate_roundrobin(Locator *self, Datum value, bool isnull,
                  bool *hasprimary)
{ // #lizard forgives
    /* TODO */
    if (hasprimary)
        *hasprimary = false;
    if (++self->roundRobinNode >= self->nodeCount)
        self->roundRobinNode = 0;
    switch (self->listType)
    {
    case LOCATOR_LIST_NONE:
        ((int *)self->results)[0] = self->roundRobinNode;
        break;
    case LOCATOR_LIST_INT:
        ((int *)self->results)[0] =
            ((int *)self->nodeMap)[self->roundRobinNode];
        break;
    case LOCATOR_LIST_OID:
        ((Oid *)self->results)[0] =
            ((Oid *)self->nodeMap)[self->roundRobinNode];
        break;
    case LOCATOR_LIST_POINTER:
        ((void **)self->results)[0] =
            ((void **)self->nodeMap)[self->roundRobinNode];
        break;
    case LOCATOR_LIST_LIST:
        /* Should never happen */
        Assert(false);
        break;
    }
    return 1;
}

/*
 * Each time return one node, in a random manner
 * This is similar to locate_modulo_select, but that
 * function does not use a random modulo..
 */
static int
locate_modulo_random(Locator *self, Datum value, bool isnull,
                     bool *hasprimary)
{
    int offset;

    if (hasprimary)
        *hasprimary = false;

    Assert(self->nodeCount > 0);
    offset = compute_modulo(abs(rand()), self->nodeCount);
    switch (self->listType)
    {
    case LOCATOR_LIST_NONE:
        ((int *)self->results)[0] = offset;
        break;
    case LOCATOR_LIST_INT:
        ((int *)self->results)[0] =
            ((int *)self->nodeMap)[offset];
        break;
    case LOCATOR_LIST_OID:
        ((Oid *)self->results)[0] =
            ((Oid *)self->nodeMap)[offset];
        break;
    case LOCATOR_LIST_POINTER:
        ((void **)self->results)[0] =
            ((void **)self->nodeMap)[offset];
        break;
    case LOCATOR_LIST_LIST:
        /* Should never happen */
        Assert(false);
        break;
    }
    return 1;
}

/*
 * Calculate hash from supplied value and use modulo by nodeCount as an index
 */
static int
locate_hash_insert(Locator *self, Datum value, bool isnull,
                   bool *hasprimary)
{ // #lizard forgives
    int index;
    if (hasprimary)
        *hasprimary = false;
    if (isnull)
        index = 0;
    else
    {
        unsigned int hash32;

        hash32 = (unsigned int)DatumGetInt32(DirectFunctionCall1(self->hashfunc, value));

        index = compute_modulo(hash32, self->nodeCount);
    }
    switch (self->listType)
    {
    case LOCATOR_LIST_NONE:
        ((int *)self->results)[0] = index;
        break;
    case LOCATOR_LIST_INT:
        ((int *)self->results)[0] = ((int *)self->nodeMap)[index];
        break;
    case LOCATOR_LIST_OID:
        ((Oid *)self->results)[0] = ((Oid *)self->nodeMap)[index];
        break;
    case LOCATOR_LIST_POINTER:
        ((void **)self->results)[0] = ((void **)self->nodeMap)[index];
        break;
    case LOCATOR_LIST_LIST:
        /* Should never happen */
        Assert(false);
        break;
    }
    return 1;
}

/*
 * Calculate hash from supplied value and use modulo by nodeCount as an index
 * if value is NULL assume no hint and return all the nodes.
 */
static int
locate_hash_select(Locator *self, Datum value, bool isnull,
                   bool *hasprimary)
{ // #lizard forgives
    if (hasprimary)
        *hasprimary = false;
    if (isnull)
    {
        int i;
        switch (self->listType)
        {
        case LOCATOR_LIST_NONE:
            for (i = 0; i < self->nodeCount; i++)
                ((int *)self->results)[i] = i;
            break;
        case LOCATOR_LIST_INT:
            memcpy(self->results, self->nodeMap,
                   self->nodeCount * sizeof(int));
            break;
        case LOCATOR_LIST_OID:
            memcpy(self->results, self->nodeMap,
                   self->nodeCount * sizeof(Oid));
            break;
        case LOCATOR_LIST_POINTER:
            memcpy(self->results, self->nodeMap,
                   self->nodeCount * sizeof(void *));
            break;
        case LOCATOR_LIST_LIST:
            /* Should never happen */
            Assert(false);
            break;
        }
        return self->nodeCount;
    }
    else
    {
        unsigned int hash32;
        int index;

        hash32 = (unsigned int)DatumGetInt32(DirectFunctionCall1(self->hashfunc, value));

        index = compute_modulo(hash32, self->nodeCount);
        switch (self->listType)
        {
        case LOCATOR_LIST_NONE:
            ((int *)self->results)[0] = index;
            break;
        case LOCATOR_LIST_INT:
            ((int *)self->results)[0] = ((int *)self->nodeMap)[index];
            break;
        case LOCATOR_LIST_OID:
            ((Oid *)self->results)[0] = ((Oid *)self->nodeMap)[index];
            break;
        case LOCATOR_LIST_POINTER:
            ((void **)self->results)[0] = ((void **)self->nodeMap)[index];
            break;
        case LOCATOR_LIST_LIST:
            /* Should never happen */
            Assert(false);
            break;
        }
        return 1;
    }
}

/*
 * Use modulo of supplied value by nodeCount as an index
 */
static int
locate_modulo_insert(Locator *self, Datum value, bool isnull,
                     bool *hasprimary)
{ // #lizard forgives
    int index;
    if (hasprimary)
        *hasprimary = false;
    if (isnull)
        index = 0;
    else
    {
        uint64 val;

        if (self->valuelen == 8)
            val = (uint64)((int64)(value));
        else if (self->valuelen == 4)
            val = (uint64)((int32)(value));
        else if (self->valuelen == 2)
            val = (uint64)((int16)(value));
        else if (self->valuelen == 1)
            val = (uint64)((int8)(value));
        else
            val = 0;

        index = compute_modulo(val, self->nodeCount);
    }
    switch (self->listType)
    {
    case LOCATOR_LIST_NONE:
        ((int *)self->results)[0] = index;
        break;
    case LOCATOR_LIST_INT:
        ((int *)self->results)[0] = ((int *)self->nodeMap)[index];
        break;
    case LOCATOR_LIST_OID:
        ((Oid *)self->results)[0] = ((Oid *)self->nodeMap)[index];
        break;
    case LOCATOR_LIST_POINTER:
        ((void **)self->results)[0] = ((void **)self->nodeMap)[index];
        break;
    case LOCATOR_LIST_LIST:
        /* Should never happen */
        Assert(false);
        break;
    }
    return 1;
}

/*
 * Use modulo of supplied value by nodeCount as an index
 * if value is NULL assume no hint and return all the nodes.
 */
static int
locate_modulo_select(Locator *self, Datum value, bool isnull,
                     bool *hasprimary)
{ // #lizard forgives
    if (hasprimary)
        *hasprimary = false;
    if (isnull)
    {
        int i;
        switch (self->listType)
        {
        case LOCATOR_LIST_NONE:
            for (i = 0; i < self->nodeCount; i++)
                ((int *)self->results)[i] = i;
            break;
        case LOCATOR_LIST_INT:
            memcpy(self->results, self->nodeMap,
                   self->nodeCount * sizeof(int));
            break;
        case LOCATOR_LIST_OID:
            memcpy(self->results, self->nodeMap,
                   self->nodeCount * sizeof(Oid));
            break;
        case LOCATOR_LIST_POINTER:
            memcpy(self->results, self->nodeMap,
                   self->nodeCount * sizeof(void *));
            break;
        case LOCATOR_LIST_LIST:
            /* Should never happen */
            Assert(false);
            break;
        }
        return self->nodeCount;
    }
    else
    {
        uint64 val;
        int index;

        if (self->valuelen == 8)
            val = (uint64)((int64)(value));
        else if (self->valuelen == 4)
            val = (unsigned int)((int32)(value));
        else if (self->valuelen == 2)
            val = (unsigned int)((int16)(value));
        else if (self->valuelen == 1)
            val = (unsigned int)((int8)(value));
        else
            val = 0;

        index = compute_modulo(val, self->nodeCount);

        switch (self->listType)
        {
        case LOCATOR_LIST_NONE:
            ((int *)self->results)[0] = index;
            break;
        case LOCATOR_LIST_INT:
            ((int *)self->results)[0] = ((int *)self->nodeMap)[index];
            break;
        case LOCATOR_LIST_OID:
            ((Oid *)self->results)[0] = ((Oid *)self->nodeMap)[index];
            break;
        case LOCATOR_LIST_POINTER:
            ((void **)self->results)[0] = ((void **)self->nodeMap)[index];
            break;
        case LOCATOR_LIST_LIST:
            /* Should never happen */
            Assert(false);
            break;
        }
        return 1;
    }
}

int GET_NODES(Locator *self, Datum value, bool isnull,
              bool *hasprimary)
{
    return (*self->locatefunc)(self, value, isnull, hasprimary);
}

bool IsDistributedColumn(AttrNumber attr, RelationLocInfo *relation_loc_info)
{
    bool result = false;

    if (relation_loc_info && IsLocatorDistributedByValue(relation_loc_info->locatorType) &&
        (attr == relation_loc_info->partAttrNum))
    {
        result = true;
    }

    return result;
}

void *
getLocatorResults(Locator *self)
{
    return self->results;
}

void *
getLocatorNodeMap(Locator *self)
{
    return self->nodeMap;
}

int getLocatorNodeCount(Locator *self)
{
    return self->nodeCount;
}

/*
 * GetRelationNodes
 *
 * Get list of relation nodes
 * If the table is replicated and we are reading, we can just pick one.
 * If the table is partitioned, we apply partitioning column value, if possible.
 *
 * If the relation is partitioned, partValue will be applied if present
 * (indicating a value appears for partitioning column), otherwise it
 * is ignored.
 *
 * preferredNodes is only used when for replicated tables. If set, it will
 * use one of the nodes specified if the table is replicated on it.
 * This helps optimize for avoiding introducing additional nodes into the
 * transaction.
 *
 * The returned List is a copy, so it should be freed when finished.
 */
ExecNodes *
GetRelationNodes(RelationLocInfo *rel_loc_info, Datum valueForDistCol,
                 bool isValueNull,
                 RelationAccessType accessType)
{ // #lizard forgives
    ExecNodes *exec_nodes;
    int *nodenums;
    int i, count;
    Locator *locator;
    Oid typeOfValueForDistCol = InvalidOid;

    if (rel_loc_info == NULL)
        return NULL;

    if (IsLocatorDistributedByValue(rel_loc_info->locatorType))
    {
        /* A sufficient lock level needs to be taken at a higher level */
        Relation rel = relation_open(rel_loc_info->relid, NoLock);
        TupleDesc tupDesc = RelationGetDescr(rel);
        /* Get the hash type of relation */
        typeOfValueForDistCol = TupleDescAttr(tupDesc, rel_loc_info->partAttrNum - 1)->atttypid;

        relation_close(rel, NoLock);
    }

    exec_nodes = makeNode(ExecNodes);
    exec_nodes->baselocatortype = rel_loc_info->locatorType;
    exec_nodes->accesstype = accessType;

    locator = createLocator(rel_loc_info->locatorType,
                            accessType,
                            typeOfValueForDistCol,
                            LOCATOR_LIST_LIST,
                            0,
                            (void *)rel_loc_info->rl_nodeList,
                            (void **)&nodenums,
                            false);

    count = GET_NODES(locator, valueForDistCol, isValueNull, NULL);

    for (i = 0; i < count; i++)
        exec_nodes->nodeList = lappend_int(exec_nodes->nodeList, nodenums[i]);

    freeLocator(locator);
    return exec_nodes;
}

/*
 * GetRelationNodesByQuals
 * A wrapper around GetRelationNodes to reduce the node list by looking at the
 * quals. varno is assumed to be the varno of reloid inside the quals. No check
 * is made to see if that's correct.
 */
ExecNodes *
GetRelationNodesByQuals(Oid reloid, RelationLocInfo *rel_loc_info,
                        Index varno, Node *quals, RelationAccessType relaccess, Node **dis_qual, Node **sec_quals)
{ // #lizard forgives
#define ONE_SECOND_DATUM 1000000
    Expr *distcol_expr = NULL;
    ExecNodes *exec_nodes;
    Datum distcol_value;
    bool distcol_isnull;

    if (!rel_loc_info)
        return NULL;
    /*
     * If the table distributed by value, check if we can reduce the Datanodes
     * by looking at the qualifiers for this relation
     */
    if (IsRelationDistributedByValue(rel_loc_info))
    {
        Oid disttype = get_atttype(reloid, rel_loc_info->partAttrNum);
        int32 disttypmod = get_atttypmod(reloid, rel_loc_info->partAttrNum);
        distcol_expr = pgxc_find_distcol_expr(varno, rel_loc_info->partAttrNum,
                                              quals,
                                              NULL);
        /*
         * If the type of expression used to find the Datanode, is not same as
         * the distribution column type, try casting it. This is same as what
         * will happen in case of inserting that type of expression value as the
         * distribution column value.
         */
        if (distcol_expr)
        {
            distcol_expr = (Expr *)coerce_to_target_type(NULL,
                                                         (Node *)distcol_expr,
                                                         exprType((Node *)distcol_expr),
                                                         disttype, disttypmod,
                                                         COERCION_ASSIGNMENT,
                                                         COERCE_IMPLICIT_CAST, -1);
            /*
             * PGXC_FQS_TODO: We should set the bound parameters here, but we don't have
             * PlannerInfo struct and we don't handle them right now.
             * Even if constant expression mutator changes the expression, it will
             * only simplify it, keeping the semantics same
             */
            distcol_expr = (Expr *)eval_const_expressions(NULL,
                                                          (Node *)distcol_expr);
        }
    }

    if (distcol_expr && IsA(distcol_expr, Const))
    {
        Const *const_expr = (Const *)distcol_expr;
        distcol_value = const_expr->constvalue;
        distcol_isnull = const_expr->constisnull;
    }
    else
    {
        distcol_value = (Datum)0;
        distcol_isnull = true;
    }

    exec_nodes = GetRelationNodes(rel_loc_info, distcol_value,
                                  distcol_isnull,
                                  relaccess);
    return exec_nodes;
}

/*
 * GetRelationDistribColumn
 * Return hash column name for relation or NULL if relation is not distributed.
 */
char *
GetRelationDistribColumn(RelationLocInfo *locInfo)
{
    /* No relation, so simply leave */
    if (!locInfo)
        return NULL;

    /* No distribution column if relation is not distributed with a key */
    if (!IsRelationDistributedByValue(locInfo))
        return NULL;

    /* Return column name */
    return get_attname(locInfo->relid, locInfo->partAttrNum, true);
}

/*
 * pgxc_find_distcol_expr
 * Search through the quals provided and find out an expression which will give
 * us value of distribution column if exists in the quals. Say for a table
 * tab1 (val int, val2 int) distributed by hash(val), a query "SELECT * FROM
 * tab1 WHERE val = fn(x, y, z) and val2 = 3", fn(x,y,z) is the expression which
 * decides the distribution column value in the rows qualified by this query.
 * Hence return fn(x, y, z). But for a query "SELECT * FROM tab1 WHERE val =
 * fn(x, y, z) || val2 = 3", there is no expression which decides the values
 * distribution column val can take in the qualified rows. So, in such cases
 * this function returns NULL.
 */
Expr *
pgxc_find_distcol_expr(Index varno,
                       AttrNumber attrNum,
                       Node *quals,
                       List *rtable)
{ // #lizard forgives
    List *lquals;
    ListCell *qual_cell;

    /* If no quals, no distribution column expression */
    if (!quals)
        return NULL;

    /* Convert the qualification into List if it's not already so */
    if (!IsA(quals, List))
        lquals = make_ands_implicit((Expr *)quals);
    else
        lquals = (List *)quals;

    /*
     * For every ANDed expression, check if that expression is of the form
     * <distribution_col> = <expr>. If so return expr.
     */
    foreach (qual_cell, lquals)
    {
        Expr *qual_expr = (Expr *)lfirst(qual_cell);
        OpExpr *op;
        Expr *lexpr;
        Expr *rexpr;
        Var *var_expr;
        Var *new_var_expr;
        Expr *distcol_expr;

        if (!IsA(qual_expr, OpExpr))
            continue;
        op = (OpExpr *)qual_expr;
        /* If not a binary operator, it can not be '='. */
        if (list_length(op->args) != 2)
            continue;

        lexpr = linitial(op->args);
        rexpr = lsecond(op->args);

        /*
         * If either of the operands is a RelabelType, extract the Var in the RelabelType.
         * A RelabelType represents a "dummy" type coercion between two binary compatible datatypes.
         * If we do not handle these then our optimization does not work in case of varchar
         * For example if col is of type varchar and is the dist key then
         * select * from vc_tab where col = 'abcdefghijklmnopqrstuvwxyz';
         * should be shipped to one of the nodes only
         */
        if (IsA(lexpr, RelabelType))
            lexpr = ((RelabelType *)lexpr)->arg;
        if (IsA(rexpr, RelabelType))
            rexpr = ((RelabelType *)rexpr)->arg;

        /*
         * If either of the operands is a Var expression, assume the other
         * one is distribution column expression. If none is Var check next
         * qual.
         */
        if (IsA(lexpr, Var))
        {
            var_expr = (Var *)lexpr;
            distcol_expr = rexpr;
        }
        else if (IsA(rexpr, Var))
        {
            var_expr = (Var *)rexpr;
            distcol_expr = lexpr;
        }
        else
            continue;

        /* 
         * For join var reference, we should find its base relation reference
         * to restrict the exec nodes for higher performance.
         * Added by Junbin Kang at Alibaba, 2020.04.28
         */
        new_var_expr = var_expr;
        if (rtable && var_expr->varlevelsup == 0) /* TODO: cannot handle outer level var now */
        {
            RangeTblEntry *rte;
            Node *node;

            rte = rt_fetch(var_expr->varno, rtable);
            if (rte->rtekind == RTE_JOIN)
            {
                if (var_expr->varattno != InvalidAttrNumber)
                {
                    node = (Node *)list_nth(rte->joinaliasvars, var_expr->varattno - 1);
                    if (IsA(node, Var))
                        new_var_expr = (Var *)node;
                }
            }
        }
        /*
         * If Var found is not the distribution column of required relation,
         * check next qual
         */
        if (new_var_expr->varno != varno || new_var_expr->varattno != attrNum)
            continue;
        /*
         * If the operator is not an assignment operator, check next
         * constraint. An operator is an assignment operator if it's
         * mergejoinable or hashjoinable. Beware that not every assignment
         * operator is mergejoinable or hashjoinable, so we might leave some
         * oportunity. But then we have to rely on the opname which may not
         * be something we know to be equality operator as well.
         */
        if (!op_mergejoinable(op->opno, exprType((Node *)lexpr)) &&
            !op_hashjoinable(op->opno, exprType((Node *)lexpr)))
            continue;
        /* Found the distribution column expression return it */
        return distcol_expr;
    }
    /* Exhausted all quals, but no distribution column expression */
    return NULL;
}
static char *
get_cluster_table_option(List *options, TableOption type)
{
    ListCell *lc;
    char *option_name = NULL;
    char *option_val = NULL;

    switch (type)
    {
    case TABLE_OPTION_LOCATOR_TYPE:
        option_name = "locator_type";
        break;
    case TABLE_OPTION_PART_ATTR_NUM:
        option_name = "part_attr_num";
        break;
    case TABLE_OPTION_PART_ATTR_NAME:
        option_name = "dist_col_name";
        break;
    default:
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("table option type %d is invalid for polarx",
                        type)));
    }

    foreach (lc, options)
    {
        DefElem *def = (DefElem *)lfirst(lc);
        if (strcmp(def->defname, option_name) == 0)
            option_val = defGetString(def);
    }

    return option_val;
}
/*
 * GetForeignTable - look up the foreign table definition by relation oid.
 */
static ForeignTable *
get_foreign_table(Oid relid)
{
    Form_pg_foreign_table tableform;
    ForeignTable *ft;
    HeapTuple tp;
    Datum datum;
    bool isnull;

    tp = SearchSysCache1(FOREIGNTABLEREL, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tp))
        return NULL;
    tableform = (Form_pg_foreign_table)GETSTRUCT(tp);

    ft = (ForeignTable *)palloc(sizeof(ForeignTable));
    ft->relid = relid;
    ft->serverid = tableform->ftserver;

    /* Extract the ftoptions */
    datum = SysCacheGetAttr(FOREIGNTABLEREL,
                            tp,
                            Anum_pg_foreign_table_ftoptions,
                            &isnull);
    if (isnull)
        ft->options = NIL;
    else
        ft->options = untransformRelOptions(datum);

    ReleaseSysCache(tp);

    return ft;
}
