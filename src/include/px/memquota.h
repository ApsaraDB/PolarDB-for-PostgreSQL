/*-------------------------------------------------------------------------
 *
 * memquota.h
 *	  Routines related to memory quota for queries.
 *
 * Portions Copyright (c) 2005-2010, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/include/cdb/memquota.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MEMQUOTA_H_
#define MEMQUOTA_H_

#include "nodes/plannodes.h"
#include "px/px_plan.h"

typedef enum ResManagerMemoryPolicy
{
	RESMANAGER_MEMORY_POLICY_NONE,
	RESMANAGER_MEMORY_POLICY_AUTO,
	RESMANAGER_MEMORY_POLICY_EAGER_FREE
} ResManagerMemoryPolicy;

extern ResManagerMemoryPolicy px_resmanager_memory_policy_default;
extern bool						px_log_resmanager_memory_default;
extern int						px_resmanager_memory_policy_auto_fixed_mem_default;
extern bool						px_resmanager_print_operator_memory_limits_default;

extern ResManagerMemoryPolicy	*px_resmanager_memory_policy;
extern bool						*px_log_resmanager_memory;
extern int						*px_resmanager_memory_policy_auto_fixed_mem;
extern bool						*px_resmanager_print_operator_memory_limits;

#define PX_RESMANAGER_MEMORY_LOG_LEVEL NOTICE

#define IsResManagerMemoryPolicyNone() (*px_resmanager_memory_policy == RESMANAGER_MEMORY_POLICY_NONE)
#define IsResManagerMemoryPolicyAuto() (*px_resmanager_memory_policy == RESMANAGER_MEMORY_POLICY_AUTO)
#define IsResManagerMemoryPolicyEagerFree() (*px_resmanager_memory_policy == RESMANAGER_MEMORY_POLICY_EAGER_FREE)

#define LogResManagerMemory() (*px_log_resmanager_memory == true)
#define ResManagerPrintOperatorMemoryLimits() (*px_resmanager_print_operator_memory_limits == true)

extern void PolicyAutoAssignOperatorMemoryKB(PlannedStmt *stmt, uint64 memoryAvailable);
extern void PolicyEagerFreeAssignOperatorMemoryKB(PlannedStmt *stmt, uint64 memoryAvailable);

/**
 * Inverse for explain analyze.
 */
extern uint64 PolicyAutoStatementMemForNoSpill(PlannedStmt *stmt, uint64 minOperatorMemKB);

/**
 * Is result node memory intensive?
 */
extern bool IsResultMemoryIntensive(Result *res);

/*
 * Calculate the amount of memory reserved for the query
 */
extern int64 ResourceManagerGetQueryMemoryLimit(PlannedStmt* stmt);

#endif /* MEMQUOTA_H_ */
