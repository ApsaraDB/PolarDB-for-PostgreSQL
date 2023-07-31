/*-------------------------------------------------------------------------
 *
 * timestamp.c
 *	  copy from src/backend/utils/adt/timestamp.c but for fronted.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, Alibaba Group Holding Limited
 *
 *
 * IDENTIFICATION
 *	  src/fe_utils/timestamp.c
 *
 *-------------------------------------------------------------------------
 */
#include <time.h>

#include "postgres.h"

#include "fe_utils/timestamp.h"
#include "utils/datetime.h"

/* copied from timestamp.c */
pg_time_t
timestamptz_to_time_t(TimestampTz t)
{
	pg_time_t	result;

	result = (pg_time_t) (t / USECS_PER_SEC +
						  ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY));
	return result;
}

/*
 * Stopgap implementation of timestamptz_to_str that doesn't depend on backend
 * infrastructure.  This will work for timestamps that are within the range
 * of the platform time_t type.  (pg_time_t is compatible except for possibly
 * being wider.)
 *
 * XXX the return value points to a static buffer, so beware of using more
 * than one result value concurrently.
 *
 * XXX: The backend timestamp infrastructure should instead be split out and
 * moved into src/common.  That's a large project though.
 */
const char *
timestamptz_to_str(TimestampTz dt)
{
	static char buf[MAXDATELEN + 1];
	char		ts[MAXDATELEN + 1];
	char		zone[MAXDATELEN + 1];
	time_t		result = (time_t) timestamptz_to_time_t(dt);
	struct tm  *ltime = localtime(&result);

	strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", ltime);
	strftime(zone, sizeof(zone), "%Z", ltime);

	snprintf(buf, sizeof(buf), "%s.%06d %s",
			 ts, (int) (dt % USECS_PER_SEC), zone);

	return buf;
}
