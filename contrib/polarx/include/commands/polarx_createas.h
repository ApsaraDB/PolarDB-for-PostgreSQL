/*-------------------------------------------------------------------------
 *
 * polarx_createas.h
 *
 * Copyright (c) 2020, Alibaba Inc. and/or its affiliates
 * Copyright (c) 2020, Apache License Version 2.0
 *
 * DENTIFICATION
 *        contrib/polarx/include/commands/polarx_createas.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLARX_CREATEAS_H
#define POLARX_CREATEAS_H
#include "catalog/objectaddress.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "tcop/dest.h"
#include "utils/queryenvironment.h"

extern void ExecCreateTableAsPre(CreateTableAsStmt *stmt);
extern void ExecCreateTableAsPost(CreateTableAsStmt *stmt);
extern ObjectAddress ExecCreateTableAsReplace(CreateTableAsStmt *stmt, const char *queryString,
                    ParamListInfo params, QueryEnvironment *queryEnv, char *completionTag);
#endif /* POLARX_CREATEAS_H */
