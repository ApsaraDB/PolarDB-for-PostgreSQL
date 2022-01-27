/*-------------------------------------------------------------------------
 *
 * fdwplanner_utils.h 
 *    utility functions for fdw planner
 *
 * Copyright (c) 2020, Alibaba Inc. and/or its affiliates
 * Copyright (c) 2020, Apache License Version 2.0
 *
 * IDENTIFICATION
 *        contrib/polarx/include/utils/fdwplanner_utils.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef FDWPLANNER_UTILS_H
#define FDWPLANNER_UTILS_H

#include "postgres.h"

extern void AdjustRelationBackToTable(bool is_durable);
extern void AdjustRelationToForeignTable(List *tableList);
extern void AdjustRelationBackToForeignTable(void);
extern bool ExtractRelationRangeTableList(Node *node, List **rangeTableList);
#endif /* FDWPLANNER_UTILS_H */
