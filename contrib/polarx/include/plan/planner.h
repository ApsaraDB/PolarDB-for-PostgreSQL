/*-------------------------------------------------------------------------
 *
 * planner.h
 *        Externally declared locator functions
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * Copyright (c) 2020, Apache License Version 2.0*
 *
 * IDENTIFICATION
 *        contrib/polarx/include/plan/planner.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLARXPLANNER_H
#define POLARXPLANNER_H

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "pgxc/locator.h"
#include "pgxc/planner.h"
#include "tcop/dest.h"
#include "nodes/relation.h"


extern PlannedStmt *polarx_planner(Query *query, int cursorOptions,
                                         ParamListInfo boundParams);
extern List *AddRemoteQueryNode(List *stmts, const char *queryString,
                                RemoteQueryExecType remoteExecType);
extern bool EnableFastQueryShipping;

#endif   /* PGXCPLANNER_H */
