/*-------------------------------------------------------------------------
 *
 * polarx_locator.h
 *		 Locator utility functions 
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * Portions Copyright (c) 2012-2018, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/polarx/include/polarx/polarx_locator.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLARX_LOCATOR_H
#define POLARX_LOCATOR_H 

#include "nodes/relation.h"
#include "foreign/foreign.h"
#include "pgxc/locator.h"
#include "nodes/polarx_node.h"

typedef struct Distribution
{
	char     distributionType;
	Node            *distributionExpr;
	Bitmapset       *nodes;
	Bitmapset       *restrictNodes;
} Distribution;

typedef struct DistributionForParam
{
    PolarxNode type;
	char    distributionType;
    RelationAccessType accessType;
    int     paramId;
    int     targetNode;
	Node    *distributionExpr;
} DistributionForParam;

extern List *GetRelationNodesWithDistribution(Distribution *distribution, Datum valueForDistCol,
						bool isValueNull,
						RelationAccessType accessType);
extern List *GetRelationNodesWithRelation(Relation relation, Datum valueForDistCol,
						bool isValueNull,
						RelationAccessType accessType,  int numDataNodes);
extern int GetRelationPartAttrNum(ForeignTable *table);
extern List *GetRelationNodesWithDistAndParam(DistributionForParam *dist_for_param,
                                                ParamListInfo param_list_info,
                                                List *org_nodes);
extern Locator *createLocator(char locatorType, RelationAccessType accessType,
                              Oid dataType, LocatorListType listType, int nodeCount,
                              void *nodeList, void **result, bool primary);
#endif		/* POLARX_LOCATOR_H */
