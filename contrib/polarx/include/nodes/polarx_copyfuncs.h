/*-------------------------------------------------------------------------
 *
 * polarx_copyfuncs.h
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 *
 * DENTIFICATION
 *        contrib/polarx/include/nodes/polarx_copyfuncs.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLARX_COPYFUNCS_H
#define POLARX_COPYFUNCS_H
#include "postgres.h"
#include "nodes/extensible.h"

#define POLARX_COPYFUNC_ARGS struct ExtensibleNode *tgt_node, const struct \
    ExtensibleNode *src_node

extern void CopyRemoteQuery(POLARX_COPYFUNC_ARGS);
extern void CopyDistributeBy(POLARX_COPYFUNC_ARGS);
extern void CopyExecNodes(POLARX_COPYFUNC_ARGS);
extern void CopyParamExternDataInfo(POLARX_COPYFUNC_ARGS);
extern void CopyBoundParamsInfo(POLARX_COPYFUNC_ARGS);
extern void CopyDistributionForParam(POLARX_COPYFUNC_ARGS);
#endif /* POLARX_COPYFUNCS_H */
