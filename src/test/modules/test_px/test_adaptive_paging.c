#include "postgres.h"
#include "fmgr.h"

#include <time.h>
#include <stdlib.h>
#include <assert.h>
#include <px/px_adaptive_paging.h>

/** test different rs_nblocks between px workers */
static void
unit_different_blocks()
{
	SeqscanPageRequest req;
	SeqscanPageResponse res;
	px_adps_array_free();
	req.worker_id = 0;
	req.task_id = 0;
	req.direction = 1;
	req.page_count = 1050;
	req.scan_count = -1;
	req.scan_round = 0;
	px_scan_unit_size = 256;
	/** first request */
	res = px_adps_get_response_block(&req, 0, 6);
	/** request worker`s page > first */
	do
	{
		req.page_count = 1100;
		res = px_adps_get_response_block(&req, 0, 6);
		if (res.success)
		{
			assert(res.page_end < 1050);
			assert(res.page_start < 1050);
		}
	}
	while (res.success);

	req.task_id = 1;
	req.page_count = 1100;
	/** first request */
	res = px_adps_get_response_block(&req, 0, 6);
	/** request worker`s page < first */
	do
	{
		req.page_count = 1050;
		res = px_adps_get_response_block(&req, 0, 6);
		if (res.success)
		{
			assert(res.page_end < 1050);
			assert(res.page_start < 1050);
		}
	}
	while (res.success);

	/** reverse scan */
	req.task_id = 2;
	req.direction = -1;
	req.page_count = 1500;
	/** first request */
	res = px_adps_get_response_block(&req, 0, 6);
	/** request worker`s page < first */
	do
	{
		req.page_count = 900;
		res = px_adps_get_response_block(&req, 0, 6);
		if (res.success)
		{
			assert(res.page_end < 900);
			assert(res.page_start < 900);
		}
	}
	while (res.success);

	req.task_id = 3;
	req.direction = -1;
	req.page_count = 1500;
	/** first request */
	res = px_adps_get_response_block(&req, 0, 6);
	/** request worker`s page > first */
	do
	{
		req.page_count = 2000;
		res = px_adps_get_response_block(&req, 0, 6);
		if (res.success)
		{
			assert(res.page_end < 1500);
			assert(res.page_start < 1500);
		}
	}
	while (res.success);
	px_adps_array_free();
}

static void
unit_scan_many_round()
{
	SeqscanPageRequest req;
	SeqscanPageResponse res;
	px_adps_array_free();
	req.worker_id = 0;
	req.task_id = 0;
	req.direction = 1;
	req.page_count = 1050;
	req.scan_count = -1;
	req.scan_round = 0;
	px_scan_unit_size = 256;
	do
	{
		res = px_adps_get_response_block(&req, 0, 6);
	}
	while (res.success);
	req.scan_round++;

	/** normal round */
	res = px_adps_get_response_block(&req, 0, 6);
	assert(res.success && res.page_start == 0);

	/** last round`s request */
	req.scan_round = 0;
	res = px_adps_get_response_block(&req, 0, 6);
	assert(!res.success);

	/** maybe meet a error when scan */
	req.scan_round = 2;
	res = px_adps_get_response_block(&req, 0, 6);
	/** skip to round 2 and ignore the unfinished */
	assert(res.success);

	/** long sleep worker fail to get page */
	req.scan_round = 0;
	res = px_adps_get_response_block(&req, 0, 6);
	assert(!res.success);

	px_adps_array_free();
}

static void
unit_scan_part(void)
{
	int i = 0, j = 0;
	int direction[5] = {1, -1, 1, -1, 1};
	int scan_unit[5] = {1, 10, 512, 1024, 4096};
	int64_t page_num[] = {510, 511, 512, 513, 1023, 1024, 1025, 100000};
	srand(time(0));
	for (j = 0; j < 5; j++)
	{
		px_adps_array_free();
		for (i = 0 ; i < sizeof(page_num) / sizeof(int64_t); i++)
		{
			/* forward + 1 */
			SeqscanPageRequest req;
			SeqscanPageResponse res;
			int64_t page_sum = 0;
			int node_count = 6;
			req.worker_id = 0;
			req.task_id = i;
			req.direction = direction[j];
			req.page_count = page_num[i];
			req.scan_count = 100;
			req.scan_start = 500;
			req.scan_round = 0;
			px_scan_unit_size = scan_unit[j];
			do
			{
				int random = rand() % node_count;
				res = px_adps_get_response_block(&req, random, node_count);
				if (res.success)
					page_sum += labs((int64_t)res.page_start - (int64_t)res.page_end) + 1;
			}
			while (res.success);
			if (req.direction == 1)
				assert(page_sum == (page_num[i] > 600 ? 100 : page_num[i] - 500));
			else
				assert(page_sum == 100);
			/* check the result */
			{
				int ip;
				for (ip = 0; ip < node_count; ip++)
				{
					res = px_adps_get_response_block(&req, ip, node_count);
					assert(res.success == 0);
				}
			}
		}
	}
	px_adps_array_free();
}

static void
unit_scan_full(void)
{
	int i = 0, j = 0;
	int direction[5] = {1, -1, 1, -1, 1};
	int scan_unit[5] = {1, 10, 512, 1024, 4096};
	int64_t page_num[] = {0, 1, 511, 512, 513, 1023, 1024, 1025, 100000, 10000000, 0xFFFFFFF0};
	srand(time(0));
	for (j = 0; j < 5; j++)
	{
		px_adps_array_free();
		for (i = 0 ; i < sizeof(page_num) / sizeof(int64_t); i++)
		{
			SeqscanPageRequest req;
			SeqscanPageResponse res;
			int64_t page_sum = 0;
			int node_count = 6;
			req.worker_id = 0;
			req.task_id = i;
			req.direction = direction[j];
			req.page_count = page_num[i];
			req.scan_count = -1;
			req.scan_round = 0;
			px_scan_unit_size = scan_unit[j];
			/* test uint32 blkno*/
			if (req.page_count > 0xF0000000 && px_scan_unit_size < 512)
				continue;
			do
			{
				int random = rand() % node_count;
				res = px_adps_get_response_block(&req, random, node_count);
				if (res.success)
					page_sum += labs((int64_t)res.page_end - (int64_t)res.page_start) + 1;
			}
			while (res.success);
			assert(page_sum == page_num[i]);
			{
				int ip;
				for (ip = 0; ip < node_count; ip++)
				{
					res = px_adps_get_response_block(&req, ip, node_count);
					assert(res.success == 0);
				}
			}
		}
	}
	px_adps_array_free();
}

/*
 * test dynamic paging.
 */
PG_FUNCTION_INFO_V1(test_px_adaptive_paging);
Datum
test_px_adaptive_paging(PG_FUNCTION_ARGS)
{
	unit_scan_full();
	unit_scan_part();
	unit_scan_many_round();
	unit_different_blocks();
	PG_RETURN_VOID();
}
