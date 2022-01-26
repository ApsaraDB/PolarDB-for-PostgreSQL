/*-------------------------------------------------------------------------
 *
 * gtm.c
 *
 *      Module interfacing with GTM
 *
 *
 *-------------------------------------------------------------------------
 */

#include "pgxc/transam/txn_gtm.h"

#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "distributed_txn/logical_clock.h"

bool log_gtm_stats = false;

#ifdef POLARX_TODO
GTM_Timestamp
GetGlobalTimestampGTM(void)
{
	/* TODO get timestamp from GTM instead local  */
	return LogicalClockTick();
}

TimestampTz
GetCurrentGTMStartTimestamp(void)
{
	return 0;
}

void
SetCurrentGTMDeltaTimestamp(TimestampTz timestamp)
{
}


#endif
