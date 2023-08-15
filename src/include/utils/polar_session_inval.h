/*-------------------------------------------------------------------------
 *
 * polar_session_inval.h
 *    Process invalidation message for polar session context.
 *
 *
 * Copyright (c) 2020, Alibaba inc.
 *
 * src/include/utils/polar_session_inval.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef POLAR_SESSION_INVAL_H
#define POLAR_SESSION_INVAL_H

#include "utils/inval.h"

extern void polar_ss_cache_register_syscache_callback(int cacheid, SyscacheCallbackFunction func, Datum arg);
extern void polar_ss_cache_register_relcache_callback(RelcacheCallbackFunction func, Datum arg);

#endif							/* POLAR_SESSION_INVAL_H */
