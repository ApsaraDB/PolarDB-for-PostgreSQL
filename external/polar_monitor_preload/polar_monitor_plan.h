/*-------------------------------------------------------------------------
 *
 * polar_monitor_plan.h
 * 	  log running query
 *
 * Portions Copyright (c) 2021, Alibaba Group Holding Limited
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    external/polar_monitor_preload/polar_monitor_plan.h
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "storage/ipc.h"
#include "storage/spin.h"

#ifndef POLAR_MONITOR_PLAN_H
#define POLAR_MONITOR_PLAN_H

#include "storage/procsignal.h"

extern void HandleLogCurrentPlanInterrupt(void);
extern void ProcessLogCurrentPlanInterrupt(void);
extern Datum polar_log_current_plan(PG_FUNCTION_ARGS);
#endif							/* POLAR_MONITOR_PLAN_H */