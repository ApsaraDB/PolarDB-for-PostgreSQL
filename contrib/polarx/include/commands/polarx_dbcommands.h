/*-------------------------------------------------------------------------
 *
 * polarx_dbcommands.h
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 *
 * DENTIFICATION
 *        contrib/polarx/include/commands/polarx_dbcommands.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLARX_DBCOMMANDS_H
#define POLARX_DBCOMMANDS_H
#include "nodes/parsenodes.h"
extern bool IsSetTableSpace(AlterDatabaseStmt *stmt);
#endif /* POLARX_DBCOMMANDS_H */
