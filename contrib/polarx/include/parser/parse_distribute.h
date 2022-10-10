/*-------------------------------------------------------------------------
 *
 * parse_distribute.h
 *    parse distribute option related functionality.
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 *
 * DENTIFICATION
 *        contrib/polarx/include/parser/parse_distribute.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSE_DISTIRBUTE_H
#define PARSE_DISTIRBUTE_H

#include "polarx.h"
#include "nodes/parsenodes.h"
#include "nodes/polarx_node.h"

typedef struct DistributeBy
{
    PolarxNode        type;
    DistributionType disttype;        /* Distribution type */
    char           *colname;        /* Distribution column name */
} DistributeBy;

extern List *extractPolarxTableOption(List **defList);
extern bool isCreateLocalTable(List *defList);
extern DistributeBy *buildDistributeBy(List *defList, CreateStmt *stmt, List *orgColDefs);
extern void validDistbyOnTableConstrants(DistributeBy *dist_by, List *colDefs, CreateStmt *stmt);
extern DistributeBy *buildDistributeByForIntoClause(List *distList, List *colDefs);
extern DistributionType getDistType(List *defList);
#endif /* PARSE_DISTIRBUTE_H */
