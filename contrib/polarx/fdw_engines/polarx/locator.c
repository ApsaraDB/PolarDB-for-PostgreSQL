/*-------------------------------------------------------------------------
 *
 * locator.c
 *		Functions that help manage table location information such as
 * partitioning and replication information.
 *
 * Copyright (c) 2020, Alibaba Inc. and/or its affiliates
 * Copyright (c) 2020, Apache License Version 2.0*
 *
 * IDENTIFICATION
 *        contrib/polarx/fdw_engines/polarx/locator.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "polarx/polarx_locator.h"
#include "nodes/nodeFuncs.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "postgres_fdw.h"
#include "polarx/polarx_option.h"

List *
GetRelationNodesWithDistribution(Distribution *distribution, Datum valueForDistCol,
				bool isValueNull,
				RelationAccessType accessType)
{
	int	*nodenums;
	int	i, count;
	Locator	*locator;
	Oid typeOfValueForDistCol = InvalidOid;
	List	*node_list = NIL;
	Bitmapset       *tmpset = NULL;

	if (distribution == NULL)
		return NIL;

	if(distribution->restrictNodes)
			tmpset = bms_copy(distribution->restrictNodes);
	else
			tmpset = bms_copy(distribution->nodes);

	while((i = bms_first_member(tmpset)) >= 0)
		node_list = lappend_int(node_list, i);
	bms_free(tmpset);

	if (IsLocatorDistributedByValue(distribution->distributionType))
	{
		if(distribution->distributionExpr)
			typeOfValueForDistCol = exprType(distribution->distributionExpr);
		else if(isValueNull)
			return node_list;
	}

	locator = createLocator(distribution->distributionType,
			accessType,
			typeOfValueForDistCol,
			LOCATOR_LIST_LIST,
			0,
			(void *) node_list,
			(void **) &nodenums,
			false);
	count = GET_NODES(locator, valueForDistCol, isValueNull, NULL);
	list_free(node_list);
	node_list = NIL;

	if(count)
	{
		for(i = 0; i < count; i++)
			node_list = lappend_int(node_list, nodenums[i]);
	}

	freeLocator(locator);
	return node_list;
}
List *
GetRelationNodesWithRelation(Relation relation, Datum valueForDistCol,
				bool isValueNull,
				RelationAccessType accessType, int numDataNodes)
{
	ForeignTable *table = GetForeignTable(RelationGetRelid(relation));
	TupleDesc tupDesc = RelationGetDescr(relation);
	Form_pg_attribute attr = tupDesc->attrs;
	char locatorType = LOCATOR_TYPE_NONE;
	int partAttrNum;
	Oid typeOfValueForDistCol = InvalidOid;
	int *nodenums;
	int count;
	List *node_list = NIL;
	int i = 0;
	Locator *locator;

	for (i = 0; i < numDataNodes; i++)
		node_list = lappend_int(node_list, i);

	locatorType = *(GetClusterTableOption(table->options,
				TABLE_OPTION_LOCATOR_TYPE));
	if(IsLocatorDistributedByValue(locatorType))
	{
		char *dist_col_name = GetClusterTableOption(table->options, TABLE_OPTION_PART_ATTR_NAME);

		if(dist_col_name == NULL)
			elog(ERROR, "table: %u, dist_col_name is NULL", table->relid);

		partAttrNum = get_attnum(table->relid, dist_col_name);
		if(partAttrNum == 0)
			elog(ERROR, "table: %u, dist_col_name %s is not exist", table->relid, dist_col_name);

		typeOfValueForDistCol = attr[partAttrNum - 1].atttypid;
	}
	locator = createLocator(locatorType,
			accessType,
			typeOfValueForDistCol,
			LOCATOR_LIST_LIST,
			0,
			(void *) node_list,
			(void **) &nodenums,
			false);

	count = GET_NODES(locator, valueForDistCol, isValueNull, NULL);
	list_free(node_list);
	node_list = NIL;
	for(i = 0; i < count; i++)
		node_list = lappend_int(node_list, nodenums[i]);

	freeLocator(locator);
        return node_list;
}

int
GetRelationPartAttrNum(ForeignTable *table)
{
	char locatorType = *(GetClusterTableOption(table->options,
				TABLE_OPTION_LOCATOR_TYPE));
	int partAttrNum = 0;
	if(IsLocatorDistributedByValue(locatorType))
	{
		char *dist_col_name = GetClusterTableOption(table->options, TABLE_OPTION_PART_ATTR_NAME);

		if(dist_col_name == NULL)
			elog(ERROR, "table: %u, dist_col_name is NULL", table->relid);

		partAttrNum = get_attnum(table->relid, dist_col_name);
		if(partAttrNum == 0)
			elog(ERROR, "table: %u, dist_col_name %s is not exist", table->relid, dist_col_name);
	}
	return partAttrNum;
}
