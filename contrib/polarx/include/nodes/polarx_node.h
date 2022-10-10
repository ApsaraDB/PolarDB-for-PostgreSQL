/*-------------------------------------------------------------------------
 *
 * polarx_node.h
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 *
 * DENTIFICATION
 *        contrib/polarx/include/nodes/polarx_node.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLARX_NODE_H
#define POLARX_NODE_H

#include "postgres.h"
#include "nodes/extensible.h"

#define POLARX_NODE_TAG_START    1500 /* must bigger than NodeTag size in nodes.h */

typedef enum PolarxNodeTag
{
    T_RemoteQuery = POLARX_NODE_TAG_START,
    T_RemoteQueryState,
    T_DistributeBy,
    T_ExecNodes,
    T_SimpleSort,
    T_ParamExternDataInfo,
    T_BoundParamsInfo,
    T_DistributionForParam,
    T_ResponseCombiner
} PolarxNodeTag;

const char** PolarxNodeTagNames;
typedef struct PolarxNode
{
    ExtensibleNode extensible;
    PolarxNodeTag polarx_tag; /* for quick type check */
} PolarxNode;

#define polarxNodeTag(nodeptr)      GetPolarxNodeTag((Node*) nodeptr)

static inline int
GetPolarxNodeTag(Node *node)
{
    if (!IsA(node, ExtensibleNode))
    {
        return nodeTag(node);
    }

    return ((PolarxNode*)(node))->polarx_tag;
}
#ifdef __GNUC__

#define PolarxNewNode(size, tag) \
    ({  PolarxNode   *_result; \
     AssertMacro((size) >= sizeof(PolarxNode));       /* need the tag, at least */ \
     _result = (PolarxNode *) palloc0fast(size); \
     _result->extensible.type = T_ExtensibleNode; \
     _result->extensible.extnodename = PolarxNodeTagNames[tag - POLARX_NODE_TAG_START]; \
     _result->polarx_tag =(int) (tag); \
     _result; \
     })

#else

extern PolarxNode *newPolarxNodeMacroHolder;

#define PolarxNewNode(size, tag) \
    ( \
      AssertMacro((size) >= sizeof(PolarxNode)),       /* need the tag, at least */ \
      newPolarxNodeMacroHolder = (PolarxNode *) palloc0fast(size), \
      newPolarxNodeMacroHolder->extensible.type = T_ExtensibleNode, \
      newPolarxNodeMacroHolder->extensible.extnodename = PolarxNodeTagNames[tag - POLARX_NODE_TAG_START], \
      newPolarxNodeMacroHolder->polarx_tag =(int) (tag), \
      newPolarxNodeMacroHolder \
    )

#endif

#define polarxIsA(nodeptr,_type_)    (polarxNodeTag(nodeptr) == T_##_type_)
#define polarxMakeNode(_type_) ((_type_ *) PolarxNewNode(sizeof(_type_),T_##_type_))

#endif /* POLARX_NODE_H */
