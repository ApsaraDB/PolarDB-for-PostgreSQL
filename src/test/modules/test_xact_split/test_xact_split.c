#include "postgres.h"

#include "access/xact.h"
#include "access/xlog.h"
#include "commands/variable.h"
#include "storage/polar_fd.h"
#include "utils/elog.h"
#include "utils/guc.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_xact_split_mock_ro);
PG_FUNCTION_INFO_V1(test_xact_split_reset_mock);
PG_FUNCTION_INFO_V1(test_xact_split);

static bool save_polar_enable_xact_split;
static bool save_polar_enable_shared_storage_mode;
static PolarNodeType save_node_type;

static void test_polar_str_to_xid(void);
static void test_polar_xact_split_mock(void);

#define check_polar_str_to_xid(input, output) \
	if (polar_str_to_xid(input, INFO, NULL) != output) \
	{ \
		elog(WARNING, "result is different, %u, %u", polar_str_to_xid(input, INFO, NULL), output); \
		Assert(false); \
	}

static char *polar_xact_split_guc_xids_tbl[] =
{
	"0,5,1,2,3,4",
	"1,5,4,3,2,1",
	"2,1",
	"3,10,7,2,4,3,8,9,1,6,5",
	"4,156140752,156140751,156140750,156140749,156140748,156140747,156140746,156140745,156140744,156140743,156140742,156140741,156140740,156140739,156140738,156140737,156140736,156140735,156140734,156140733,156140732,156140731,156140730,156140729,156140728,156140727,156140726,156140725,156140724,156140723,156140722,156140721,156140720,156140719,156140718,156140717,156140716,156140715,156140714"
};

static int polar_xact_split_xact_state_xids_tbl[][100] =
{
	{0,5,1,2,3,4},
	{1,5,1,2,3,4},
	{2,1},
	{3,10,1,2,3,4,5,6,7,8,9},
	{4,156140752,156140714,156140715,156140716,156140717,156140718,156140719,156140720,156140721,156140722,156140723,156140724,156140725,156140726,156140727,156140728,156140729,156140730,156140731,156140732,156140733,156140734,156140735,156140736,156140737,156140738,156140739,156140740,156140741,156140742,156140743,156140744,156140745,156140746,156140747,156140748,156140749,156140750,156140751},
};

static void
test_polar_str_to_xid(void)
{
	bool success;
	Assert(polar_str_to_xid("", INFO, &success) == 0);
	Assert(!success);
	Assert(polar_str_to_xid("1", INFO, &success) == 1);
	Assert(success);
	Assert(polar_str_to_xid("a", INFO, &success) == 0);
	Assert(!success);
	Assert(polar_str_to_xid("4294967296", INFO, &success) == 0);
	Assert(!success);

	check_polar_str_to_xid("0", 0);
	check_polar_str_to_xid("1", 1);
	check_polar_str_to_xid("2", 2);
	check_polar_str_to_xid("3", 3);

	check_polar_str_to_xid("4294967295", 4294967295u);
	check_polar_str_to_xid("4294967296", 0); /* > MaxTransactionId */
	check_polar_str_to_xid("18446744073709551615", 0);
	check_polar_str_to_xid("18446744073709551616", 0); /* overflow in strtoul */

	check_polar_str_to_xid(" 1", 1);
	check_polar_str_to_xid(" 1 ", 1);
	check_polar_str_to_xid("	1	", 1);
	check_polar_str_to_xid("1 1", 0);
	check_polar_str_to_xid("1	1", 0);

	check_polar_str_to_xid("-1", 0);
	check_polar_str_to_xid("-2", 0);
	check_polar_str_to_xid("-3", 0);

	check_polar_str_to_xid("--1", 0);
	check_polar_str_to_xid("1-", 0);
	check_polar_str_to_xid("1--", 0);
	check_polar_str_to_xid("-1-", 0);

	check_polar_str_to_xid("a", 0);
	check_polar_str_to_xid("aa", 0);
	check_polar_str_to_xid("1a", 0);
	check_polar_str_to_xid("a1", 0);
	check_polar_str_to_xid("1a1", 0);
	check_polar_str_to_xid("a1a", 0);
}

static void
test_polar_xact_split_mock(void)
{
	int i = 0;
	int n_cases = sizeof(polar_xact_split_xact_state_xids_tbl) / sizeof(polar_xact_split_xact_state_xids_tbl[0]);
	for (i = 0; i < n_cases; ++i)
	{
		test_single_polar_xact_split_mock(polar_xact_split_guc_xids_tbl[i], polar_xact_split_xact_state_xids_tbl[i]);
		elog(LOG, "test case %d finished", i);
	}
}

Datum
test_xact_split_mock_ro(PG_FUNCTION_ARGS)
{
	save_polar_enable_xact_split = polar_enable_xact_split;
	save_polar_enable_shared_storage_mode = polar_enable_shared_storage_mode;
	save_node_type = polar_node_type();

	polar_enable_xact_split = true;
	polar_enable_shared_storage_mode = true;
	polar_set_node_type(POLAR_REPLICA);

	PG_RETURN_VOID();
}

Datum
test_xact_split_reset_mock(PG_FUNCTION_ARGS)
{
	polar_enable_xact_split = save_polar_enable_xact_split;
	polar_enable_shared_storage_mode = save_polar_enable_shared_storage_mode;
	polar_set_node_type(save_node_type);

	PG_RETURN_VOID();
}

/*
 * SQL-callable entry point to perform all tests.
 */
Datum
test_xact_split(PG_FUNCTION_ARGS)
{
	test_polar_str_to_xid();
	test_polar_xact_split_mock();

	PG_RETURN_VOID();
}