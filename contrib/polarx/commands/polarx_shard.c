#include "commands/polarx_shard.h"
#include "access/genam.h"
#include "access/hash.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/index.h"
#include "catalog/pg_am.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_trigger.h"
#include "commands/defrem.h"
#include "commands/extension.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "parser/parse_expr.h"
#include "parser/parse_node.h"
#include "parser/parse_relation.h"
#include "parser/parser.h"
#include "storage/lmgr.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/inval.h"
#include "pgxc/pgxc.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "pgxc/transam/txn_util.h"
#include "pgxc/locator.h"
#include "executor/execRemoteQuery.h"
#include "distribute_transaction/txn.h"

/* usage: select polardbx_update_shard_meta(array[1], 'datanode1m'); */
PG_FUNCTION_INFO_V1(polardbx_update_shard_meta);

void CheckTableOwner(Oid relationId);
Datum *DeconstructArrayObject(ArrayType *arrayObject);
int32 ArrayObjectCount(ArrayType *arrayObject);


Datum polardbx_update_shard_meta(PG_FUNCTION_ARGS)
{
#define MAX_STR_LEN 512
    StringInfoData sql;
    int shardIdIndex = 0;
    int i = 0;
    List* coordlist = NIL;
    char tmp[MAX_STR_LEN] = {0};
    bool is_first = true;
    ArrayType *shardidArray = PG_GETARG_ARRAYTYPE_P(0);
    text *nodeName = PG_GETARG_TEXT_P(1);
    char *nodeNameString = text_to_cstring(nodeName);

    int shardIdCount = ArrayObjectCount(shardidArray);
    Datum *shardIdArrayDatum = DeconstructArrayObject(shardidArray);

    initStringInfo(&sql);
    // appendStringInfoString(&sql, "set perform_update_shard_meta to true;");

    for (shardIdIndex = 0; shardIdIndex < shardIdCount; shardIdIndex++)
    {
        if (is_first)
        {
            is_first = false;
            snprintf(tmp, MAX_STR_LEN, "%d", DatumGetInt32(shardIdArrayDatum[shardIdIndex]));
        }
        else
        {
            snprintf(tmp, MAX_STR_LEN, ",%d", DatumGetInt32(shardIdArrayDatum[shardIdIndex]));
        }
    }
    appendStringInfo(&sql, " update pg_catalog.pg_shard_map set nodeoid=(select oid from pg_catalog.pg_foreign_server where srvname='%s') where shardid in (%s);",
                     nodeNameString, tmp);
    
    for (i = 0; i < NumCoords; i++)
    {
        coordlist = lappend_int(coordlist, i);
    }
    PGXCNodeAllHandles *pgxc_handles = get_handles(NIL, coordlist, true, true);
    int conn_num = pgxc_handles->co_conn_count;
    PGXCNodeHandle **connections = pgxc_handles->coord_handles;
    if (pgxc_node_begin(conn_num, connections, 0, true, false, PGXC_NODE_COORDINATOR))
    {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("Could not begin transaction on coordiantor node")));
    }

    exec_query_on_nodes(NumCoords, connections, sql.data);
    
    if (NumCoords == 1)
        g_coord_twophase_state.force_2pc = true;
    
    XactCallbackPreCommit();
    /* we should set local_coord_participate after XactCallbackPreCommit, 
     * since XactCallbackPreCommit will clear it. 
     * */
	g_coord_twophase_state.local_coord_participate = true;
    XactCallbackPostCommit();
    
    PG_RETURN_VOID();
}


void CheckTableOwner(Oid relationId)
{
    if (!pg_class_ownercheck(relationId, GetUserId()))
    {
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE, get_rel_name(relationId));
    }
}

/*
 * DeconstructArrayObject takes in a single dimensional array, and deserializes
 * this array's members into an array of datum objects. The function then
 * returns this datum array.
 */
Datum *
DeconstructArrayObject(ArrayType *arrayObject)
{
    Datum *datumArray = NULL;
    bool *datumArrayNulls = NULL;
    int datumArrayLength = 0;

    bool typeByVal = false;
    char typeAlign = 0;
    int16 typeLength = 0;

    bool arrayHasNull = ARR_HASNULL(arrayObject);
    if (arrayHasNull)
    {
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmsg("worker array object cannot contain null values")));
    }

    Oid typeId = ARR_ELEMTYPE(arrayObject);
    get_typlenbyvalalign(typeId, &typeLength, &typeByVal, &typeAlign);

    deconstruct_array(arrayObject, typeId, typeLength, typeByVal, typeAlign,
                      &datumArray, &datumArrayNulls, &datumArrayLength);

    return datumArray;
}


/*
 * ArrayObjectCount takes in a single dimensional array, and returns the number
 * of elements in this array.
 */
int32
ArrayObjectCount(ArrayType *arrayObject)
{
    int32 dimensionCount = ARR_NDIM(arrayObject);
    int32 *dimensionLengthArray = ARR_DIMS(arrayObject);

    if (dimensionCount == 0)
    {
        return 0;
    }

    /* we currently allow split point arrays to have only one subarray */
    Assert(dimensionCount == 1);

    int32 arrayLength = ArrayGetNItems(dimensionCount, dimensionLengthArray);
    if (arrayLength <= 0)
    {
        ereport(ERROR, (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                errmsg("worker array object cannot be empty")));
    }

    return arrayLength;
}