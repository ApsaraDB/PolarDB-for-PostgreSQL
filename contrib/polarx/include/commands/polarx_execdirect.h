/*-------------------------------------------------------------------------
 *
 * polarx_execdirect.h
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 *
 * DENTIFICATION
 *        contrib/polarx/include/commands/polarx_execdirect.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLARX_EXECDIRECT_H
#define POLARX_EXECDIRECT_H
#include "nodes/parsenodes.h"
#include "tcop/dest.h"

extern void ExecDirectPre(ExecuteStmt *stmt, DestReceiver *dest, char *completionTag, bool *all_done);
#endif /* POLARX_EXECDIRECT_H */
