/*-------------------------------------------------------------------------
 *
 * tsc_tracer_pg.c
 *
 * Global variables and utility functions definition for TSC
 * instrumentation library, PostGreSQL specific part.
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * Portions Copyright (c) 2012-2018, PostgreSQL Global Development Group
 *-------------------------------------------------------------------------
 */
/**
 */
#include <stdlib.h>
#include <stdio.h>

#include "utils/tsc_tracer.h"

#include "c.h"
#include "utils/elog.h"

#include "miscadmin.h"
#include "postmaster/autovacuum.h"

extern bool am_walsender;

/**
 * Each process handles a client session, so
 * tag storage can simply be a global variable.
 */
uint64_t tscTraceId;
HTimeTags tscTimeTagArray;
char tscHexLogBuffer[(2 * sizeof(TimeTagEntry) + 1) * TAG_BUF_SIZE + 2];

uint64_t tscTraceRandomSeed = 0;


/**
 * Output time tags to log stream and clean up all tags.
 * 
 * Note that PG's ereport takes numbers of memory allocations, multiple
 * memory copies, a kernel call into pipe, a global lock and another
 * kernal call to file write.  Seems quite expensive.
 * 
 * Using it for similicity reason at the moment. 
 */
void logTimedTrace(void) {
  if (!tscTimeTagArray.enabled) { return; }

  outputTagsForLogging(&tscTimeTagArray, tscHexLogBuffer);
  ereport(LOG, (errmsg("logTimedTrace%s", tscHexLogBuffer)));
  tscTimeTagArray.enabled = false;
  tscTimeTagArray.nextPos = 0;
}

bool isClientBackend(void) {
  return !IsAutoVacuumLauncherProcess() && !IsAutoVacuumWorkerProcess()
      && !am_walsender && !IsBackgroundWorker;
}
