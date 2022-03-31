/*-------------------------------------------------------------------------
 *
 * polarx_utility.h
 *    polarx utility hook and related functionality.
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 *
 * DENTIFICATION
 *        contrib/polarx/include/commands/polarx_utility.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLARX_UTILITY_H
#define POLARX_UTILITY_H

#include "postgres.h"

#include "utils/relcache.h"
#include "tcop/utility.h"
typedef struct polarx_fmgr_cache
{
    FmgrInfo    flinfo;         /* lookup info for target function */
    Oid         userid;         /* userid to set, or InvalidOid */
    ArrayType  *proconfig;      /* GUC values to set, or NULL */
    Datum       arg;            /* passthrough argument for plugin modules */
} polarx_fmgr_cache;
extern bool SetExecUtilityLocal;
extern void polarx_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
                                    ProcessUtilityContext context,
                                    ParamListInfo params,
                                    struct QueryEnvironment *queryEnv,
                                    DestReceiver *dest,
                                    char *completionTag
                                    );
extern void polarx_fmgr_hook(FmgrHookEventType event,
                                 FmgrInfo *flinfo, Datum *arg);
#endif /* POLARX_UTILITY_H */
