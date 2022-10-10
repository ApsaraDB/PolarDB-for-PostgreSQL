/*-------------------------------------------------------------------------
 *
 * polarx_plancache.h
 *
 * Copyright (c) 2020, Alibaba Inc. and/or its affiliates
 * Copyright (c) 2020, Apache License Version 2.0
 *
 * DENTIFICATION
 *        contrib/polarx/include/plancache/polarx_plancache.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLARX_PLANCACHE_H
#define POLARX_PLANCACHE_H
#include "postgres.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"

extern int MaxPlancacheSize;
extern bool EnablePlanCache;
extern Datum get_node_hash(Node *node);
extern PlannedStmt *save_plan_into_cache(PlannedStmt *plan, Datum hash_val, Datum param_val);
extern PlannedStmt *get_plan_from_cache(Datum hash_val, Datum param_val);
#endif /* POLARX_PLANCACHE_H */
