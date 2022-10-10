/*-------------------------------------------------------------------------
 *
 * polarx_copyfuncs.c
 *    polarx specific node copy functions
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "nodes/params.h"
#include "nodes/polarx_node.h"
#include "nodes/polarx_copyfuncs.h"
#include "plan/polarx_planner.h"
#include "parser/parse_distribute.h"
#include "polarx/polarx_locator.h"


static inline Node *
PolarxSetTag(Node *node, int tag)
{
    PolarxNode *polarx_node = (PolarxNode *) node;
    polarx_node->polarx_tag = tag;
    return node;
}

#define POLARX_COPYFUNC_ARGS struct ExtensibleNode *tgt_node, const struct \
    ExtensibleNode *src_node

#define TRANSLATE_FOR_EXTENSIBLE_NODE_COPY(nodeTypeName) \
    nodeTypeName *newnode = (nodeTypeName *) \
    PolarxSetTag((Node *) tgt_node, T_ ## nodeTypeName); \
    nodeTypeName *from = (nodeTypeName *) src_node

/*
 * Macros to simplify copying of different kinds of fields.  Use these
 * wherever possible to reduce the chance for silly typos.  Note that these
 * hard-wire the convention that the local variables in a Copy routine are
 * named 'newnode' and 'from'.
 */

/* Copy a simple scalar field (int, float, bool, enum, etc) */
#define COPY_SCALAR_FIELD(fldname) \
    (newnode->fldname = from->fldname)

/* Copy a field that is a pointer to some kind of Node or Node tree */
#define COPY_NODE_FIELD(fldname) \
    (newnode->fldname = copyObjectImpl(from->fldname))

/* Copy a field that is a pointer to a Bitmapset */
#define COPY_BITMAPSET_FIELD(fldname) \
    (newnode->fldname = bms_copy(from->fldname))

/* Copy a field that is a pointer to a C string, or perhaps NULL */
#define COPY_STRING_FIELD(fldname) \
    (newnode->fldname = from->fldname ? pstrdup(from->fldname) : (char *) NULL)

/* Copy a field that is a pointer to a simple palloc'd object of size sz */
#define COPY_POINTER_FIELD(fldname, sz) \
    do { \
        Size    _size = (sz); \
        newnode->fldname = palloc(_size); \
        memcpy(newnode->fldname, from->fldname, _size); \
    } while (0)

/*
 * CopyPlanFields
 *
 *     This function copies the fields of the Plan node.  It is used by
 *        all the copy functions for classes which inherit from Plan.
 */
static void
CopyPlanFields(const Plan *from, Plan *newnode)
{
    COPY_SCALAR_FIELD(startup_cost);
    COPY_SCALAR_FIELD(total_cost);
    COPY_SCALAR_FIELD(plan_rows);
    COPY_SCALAR_FIELD(plan_width);
    COPY_SCALAR_FIELD(parallel_aware);
    COPY_SCALAR_FIELD(parallel_safe);
    COPY_SCALAR_FIELD(plan_node_id);
    COPY_NODE_FIELD(targetlist);
    COPY_NODE_FIELD(qual);
    COPY_NODE_FIELD(lefttree);
    COPY_NODE_FIELD(righttree);
    COPY_NODE_FIELD(initPlan);
    COPY_BITMAPSET_FIELD(extParam);
    COPY_BITMAPSET_FIELD(allParam);
}

static void
CopyScanFields(const Scan *from, Scan *newnode)
{
    CopyPlanFields((const Plan *) from, (Plan *) newnode);

    COPY_SCALAR_FIELD(scanrelid);
}

void
CopyRemoteQuery(POLARX_COPYFUNC_ARGS)
{

    TRANSLATE_FOR_EXTENSIBLE_NODE_COPY(RemoteQuery);    
    /*
     * copy node superclass fields
     */
    CopyScanFields((Scan *) (&(from->scan)), (Scan *) (&(newnode->scan)));

    /*
     * copy remainder of node
     */
    COPY_SCALAR_FIELD(exec_direct_type);
    COPY_STRING_FIELD(sql_statement);
    COPY_NODE_FIELD(exec_nodes);
    COPY_SCALAR_FIELD(combine_type);
    COPY_NODE_FIELD(sort);
    COPY_SCALAR_FIELD(read_only);
    COPY_SCALAR_FIELD(force_autocommit);
    COPY_STRING_FIELD(statement);
    COPY_STRING_FIELD(cursor);
    COPY_SCALAR_FIELD(rq_num_params);
    if (from->rq_param_types)
        COPY_POINTER_FIELD(rq_param_types,
                sizeof(from->rq_param_types[0]) * from->rq_num_params);
    else
        newnode->rq_param_types = NULL;
    COPY_SCALAR_FIELD(exec_type);

    COPY_SCALAR_FIELD(reduce_level);
    COPY_NODE_FIELD(base_tlist);
    COPY_STRING_FIELD(outer_alias);
    COPY_STRING_FIELD(inner_alias);
    COPY_SCALAR_FIELD(outer_reduce_level);
    COPY_SCALAR_FIELD(inner_reduce_level);
    COPY_BITMAPSET_FIELD(outer_relids);
    COPY_BITMAPSET_FIELD(inner_relids);
    COPY_STRING_FIELD(inner_statement);
    COPY_STRING_FIELD(outer_statement);
    COPY_STRING_FIELD(join_condition);
    COPY_SCALAR_FIELD(has_row_marks);
    COPY_SCALAR_FIELD(has_ins_child_sel_parent);

    COPY_SCALAR_FIELD(rq_finalise_aggs);
    COPY_SCALAR_FIELD(rq_sortgroup_colno);
    COPY_NODE_FIELD(remote_query);
    COPY_NODE_FIELD(coord_var_tlist);
    COPY_NODE_FIELD(query_var_tlist);
    COPY_SCALAR_FIELD(is_temp);
    COPY_STRING_FIELD(sql_select);
    COPY_STRING_FIELD(sql_select_base);
    COPY_SCALAR_FIELD(forUpadte);
    COPY_SCALAR_FIELD(ss_num_params);
    if (from->ss_num_params)
        COPY_POINTER_FIELD(ss_param_types,
                sizeof(from->ss_param_types[0]) * from->ss_num_params);
    else
        newnode->ss_param_types = NULL;
    COPY_STRING_FIELD(select_cursor);
    COPY_STRING_FIELD(sql_update);
    COPY_SCALAR_FIELD(su_num_params);
    if (from->su_num_params)
        COPY_POINTER_FIELD(su_param_types,
                sizeof(from->su_param_types[0]) * from->su_num_params);
    else
        newnode->su_param_types = NULL;
    COPY_STRING_FIELD(update_cursor);
    COPY_SCALAR_FIELD(action);
    COPY_SCALAR_FIELD(dml_on_coordinator);
    COPY_SCALAR_FIELD(jf_ctid);
    COPY_SCALAR_FIELD(jf_xc_node_id);
    COPY_SCALAR_FIELD(jf_xc_wholerow);
    COPY_BITMAPSET_FIELD(conflict_cols);
}

void
CopyDistributeBy(POLARX_COPYFUNC_ARGS)
{
    TRANSLATE_FOR_EXTENSIBLE_NODE_COPY(DistributeBy);
    COPY_SCALAR_FIELD(disttype);
    COPY_STRING_FIELD(colname);
}

void
CopyExecNodes(POLARX_COPYFUNC_ARGS)
{
    TRANSLATE_FOR_EXTENSIBLE_NODE_COPY(ExecNodes);

    COPY_NODE_FIELD(primarynodelist);
    COPY_NODE_FIELD(nodeList);
    COPY_SCALAR_FIELD(baselocatortype);
    COPY_NODE_FIELD(en_expr);
    COPY_SCALAR_FIELD(en_relid);
    COPY_SCALAR_FIELD(accesstype);
}

void
CopyParamExternDataInfo(POLARX_COPYFUNC_ARGS)
{
    TRANSLATE_FOR_EXTENSIBLE_NODE_COPY(ParamExternDataInfo);

    COPY_SCALAR_FIELD(is_cursor);
    COPY_SCALAR_FIELD(portal_need_name);
    COPY_SCALAR_FIELD(may_be_fqs);
    if(from->param_list_info)
    {
        COPY_POINTER_FIELD(param_list_info, offsetof(ParamListInfoData, params) +
                from->param_list_info->numParams * sizeof(ParamExternData));
    }
    COPY_NODE_FIELD(fqs_plannedstmt);
}

void
CopyBoundParamsInfo(POLARX_COPYFUNC_ARGS)
{
    TRANSLATE_FOR_EXTENSIBLE_NODE_COPY(BoundParamsInfo);

    COPY_SCALAR_FIELD(numParams);
    if (from->numParams)
        COPY_POINTER_FIELD(params,
                sizeof(ParamExternData) * from->numParams);
}

void
CopyDistributionForParam(POLARX_COPYFUNC_ARGS)
{
    TRANSLATE_FOR_EXTENSIBLE_NODE_COPY(DistributionForParam);

    COPY_SCALAR_FIELD(distributionType);
    COPY_SCALAR_FIELD(accessType);
    COPY_SCALAR_FIELD(paramId);
    COPY_SCALAR_FIELD(targetNode);
    COPY_NODE_FIELD(distributionExpr);
}
