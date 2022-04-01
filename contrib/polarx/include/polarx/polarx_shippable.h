/*-------------------------------------------------------------------------
 *
 * polarx_shippable.h
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * Portions Copyright (c) 2012-2018, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/polarx/include/polarx/polarx_shippable.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLARX_SHIPPABLE_H 
#define POLARX_SHIPPABLE_H 

#include "lib/stringinfo.h"
#include "nodes/relation.h"
#include "postgres_fdw.h"

extern bool is_builtin(Oid objectId);
extern bool is_shippable(Oid objectId, Oid classId, PgFdwRelationInfo *fpinfo);
#endif							/* POLARX_SHIPPABLE_H */
