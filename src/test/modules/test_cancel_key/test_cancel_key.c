#include "postgres.h"

#include "pgstat.h"
#include "utils/guc.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_cancel_key);
/*
 * SQL-callable entry point to perform all tests.
 *
 * If a 1% false positive threshold is not met, emits WARNINGs.
 *
 * See README for details of arguments.
 */
Datum
test_cancel_key(PG_FUNCTION_ARGS)
{
	bool	enable = PG_GETARG_BOOL(0);
	polar_enable_proxy_for_unit_test(enable);

	PG_RETURN_VOID();
}
