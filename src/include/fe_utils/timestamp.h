/*-------------------------------------------------------------------------
 *
 * Simple timestamp functions for frontend code
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, Alibaba Group Holding limited
 *
 * src/include/fe_utils/timestamp.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TIMESTAMP_FE_H
#define TIMESTAMP_FE_H
#include "datatype/timestamp.h"
#include "pgtime.h"

extern const char *timestamptz_to_str(TimestampTz dt);
extern pg_time_t timestamptz_to_time_t(TimestampTz t);
#endif							/* TIMESTAMP_FE_H */
