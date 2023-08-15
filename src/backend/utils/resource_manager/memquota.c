/*-------------------------------------------------------------------------
 *
 * memquota.c
 *	  Routines related to memory quota for queries.
 *
 * Portions Copyright (c) 2010, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/backend/utils/resource_manager/memquota.c
 * 
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "px/memquota.h"
#include "px/px_llize.h"
#include "storage/lwlock.h"
#include "utils/relcache.h"
#include "executor/execdesc.h"
// #include "utils/resource_manager.h"
#include "access/heapam.h"
#include "miscadmin.h"
#include "px/px_vars.h"
#include "optimizer/clauses.h"
#include "parser/parsetree.h"
#include "tcop/pquery.h"

ResManagerMemoryPolicy		px_resmanager_memory_policy_default = RESMANAGER_MEMORY_POLICY_NONE;
bool						px_log_resmanager_memory_default = false;
int							px_resmanager_memory_policy_auto_fixed_mem_default = 100;
bool						px_resmanager_print_operator_memory_limits_default = false;

ResManagerMemoryPolicy		*px_resmanager_memory_policy = &px_resmanager_memory_policy_default;
bool						*px_log_resmanager_memory = &px_log_resmanager_memory_default;
int							*px_resmanager_memory_policy_auto_fixed_mem = &px_resmanager_memory_policy_auto_fixed_mem_default;
bool						*px_resmanager_print_operator_memory_limits = &px_resmanager_print_operator_memory_limits_default;

/*
 * Calculate the amount of memory reserved for the query
 */
int64
ResourceManagerGetQueryMemoryLimit(PlannedStmt* stmt)
{
	if (px_role != PX_ROLE_QC)
		return 0;

	/* no limits in single user mode. */
	if (!IsUnderPostmaster)
		return 0;

	Assert(px_session_id > -1);
	Assert(ActivePortal != NULL);
#if 0
	if (IsResQueueEnabled())
		return ResourceQueueGetQueryMemoryLimit(stmt, ActivePortal->queueId);
	if (IsResGroupActivated())
		return ResourceGroupGetQueryMemoryLimit();
#endif
	return 1<<20;
}
