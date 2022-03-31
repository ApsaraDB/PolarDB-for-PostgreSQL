/*-------------------------------------------------------------------------
 *
 * polarx_tablecmds.h
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
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
