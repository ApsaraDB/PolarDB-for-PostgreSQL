/*-------------------------------------------------------------------------
 *
 * polarx_tablecmds.h
 *
 * Copyright (c) 2020, Alibaba Inc. and/or its affiliates
 * Copyright (c) 2020, Apache License Version 2.0
 *
 * DENTIFICATION
 *        contrib/polarx/include/commands/polarx_tablecmds.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLARX_TABLECMDS_H
#define POLARX_TABLECMDS_H
#include "postgres.h"
#include "nodes/parsenodes.h"

extern bool IsTempTable(Oid relid);
extern bool IsLocalTempTable(Oid relid);
extern bool IsIndexUsingTempTable(Oid relid);
extern void AlterTablePrepCmds(AlterTableStmt *atstmt);
extern void DefineIndexCheck(Oid relationId, IndexStmt *stmt);
#endif /* POLARX_TABLECMDS_H */
