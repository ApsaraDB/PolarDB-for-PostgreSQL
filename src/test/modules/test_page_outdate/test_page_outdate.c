#include "postgres.h"

#include <time.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

#include "access/polar_logindex.h"
#include "storage/polar_bufmgr.h"
#include "utils/elog.h"
#include "utils/guc.h"


PG_MODULE_MAGIC;

static void test_max_min()
{
	XLogRecPtr a, b;

	a = b = InvalidXLogRecPtr;
	Assert(polar_max_xlog_rec_ptr(a, b) == InvalidXLogRecPtr);
	Assert(polar_max_xlog_rec_ptr(a, b) == InvalidXLogRecPtr);

	a = 0x123456;
	Assert(polar_max_xlog_rec_ptr(a, b) == a);
	Assert(polar_min_xlog_rec_ptr(a, b) == a);

	b = 0x123457;
	Assert(polar_max_xlog_rec_ptr(a, b) == b);
	Assert(polar_min_xlog_rec_ptr(a, b) == a);
}

PG_FUNCTION_INFO_V1(test_page_outdate);
/*
 * SQL-callable entry point to perform all tests.
 *
 * If a 1% false positive threshold is not met, emits WARNINGs.
 *
 * See README for details of arguments.
 */
Datum
test_page_outdate(PG_FUNCTION_ARGS)
{
	polar_log_index_shmem_init();

	Assert(polar_enable_redo_logindex);
	Assert(polar_enable_page_outdate);

	while (RecoveryInProgress()) 
		pg_usleep(1 * 1000 * 1000);

	test_max_min();

	PG_RETURN_VOID();
}
