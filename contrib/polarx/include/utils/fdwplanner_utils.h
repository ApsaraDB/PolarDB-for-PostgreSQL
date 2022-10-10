/*-------------------------------------------------------------------------
 *
 * fdwplanner_utils.h 
 *    utility functions for fdw planner
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 *
 * IDENTIFICATION
 *        contrib/polarx/include/utils/fdwplanner_utils.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef FDWPLANNER_UTILS_H
#define FDWPLANNER_UTILS_H

#include "postgres.h"
#include "plan/polarx_planner.h"

typedef struct
{
    List *rteList;
    bool has_foreign;
    bool has_sys;
} extract_rte_context;
extern void AdjustRelationBackToTable(bool is_durable);
extern void AdjustRelationToForeignTable(List *tableList, bool *is_all_foreign);
extern void AdjustRelationBackToForeignTable(void);
extern bool ExtractRelationRangeTableList(Node *node, void *context);
extern void SetParaminfoToPlan(PlannedStmt *planned_stmt, ParamExternDataInfo *value, Datum hash_val,
                                bool is_cursor);
extern ParamExternDataInfo *GetParaminfoFromPlan(PlannedStmt *planned_stmt);
extern bool CheckPlanValid(PlannedStmt *planned_stmt);
extern int CheckFQSValid(PlannedStmt *planned_stmt, ParamExternDataInfo *value);
extern void SetFQSPlannedStmtToPlan(PlannedStmt *planned_stmt, ParamExternDataInfo *value);
#endif /* FDWPLANNER_UTILS_H */
