/*-------------------------------------------------------------------------
 *
 * polarx_nodesdef.c
 *    polarx specific node define
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */
#include "nodes/polarx_node.h"
#include "nodes/polarx_copyfuncs.h"
#include "nodes/polarx_nodesdef.h"
#include "plan/polarx_planner.h"
#include "executor/execRemoteQuery.h"
#include "parser/parse_distribute.h"
#include "polarx/polarx_locator.h"

static const char *PolarxNodeTagNamesD[] = {
    "RemoteQuery",
    "RemoteQueryState",
    "DistributeBy",
    "ExecNodes",
    "SimpleSort",
    "ParamExternDataInfo",
    "BoundParamsInfo",
    "DistributionForParam"
};

const char **PolarxNodeTagNames = PolarxNodeTagNamesD;
PolarxNode *newPolarxNodeMacroHolder;

const ExtensibleNodeMethods nodeDefMethods[] =
{
    {
        "RemoteQuery",
        sizeof(RemoteQuery),
        CopyRemoteQuery,
        NULL,
        NULL,
        NULL 
    },

    {
        "RemoteQueryState",
        sizeof(RemoteQueryState),
        NULL,
        NULL,
        NULL,
        NULL 
    },

    {
        "DistributeBy",
        sizeof(DistributeBy),
        CopyDistributeBy,
        NULL,
        NULL,
        NULL 
    },

    {
        "ExecNodes",
        sizeof(ExecNodes),
        CopyExecNodes,
        NULL,
        NULL,
        NULL 
    },

    {
        "SimpleSort",
        sizeof(SimpleSort),
        NULL,
        NULL,
        NULL,
        NULL 
    },

    {
        "ParamExternDataInfo",
        sizeof(ParamExternDataInfo),
        CopyParamExternDataInfo,
        NULL,
        NULL,
        NULL 
    },

    {
        "BoundParamsInfo",
        sizeof(BoundParamsInfo),
        CopyBoundParamsInfo,
        NULL,
        NULL,
        NULL 
    },

    {
        "DistributionForParam",
        sizeof(DistributionForParam),
        CopyDistributionForParam,
        NULL,
        NULL,
        NULL 
    }
};

void
RegisterPolarxNodes(void)
{
    int i = 0;

    for (i = 0 ; i < lengthof(nodeDefMethods); i++)
    {
        RegisterExtensibleNodeMethods(&nodeDefMethods[i]);
    }
}
