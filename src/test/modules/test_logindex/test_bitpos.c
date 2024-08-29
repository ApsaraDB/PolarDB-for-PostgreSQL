/*-------------------------------------------------------------------------
 *
 * test_bitpos.c
 *
 * Copyright (c) 2022, Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * IDENTIFICATION
 *	  src/test/modules/test_logindex/test_bitpos.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "utils/polar_bitpos.h"

#include "test_module_init.h"

PG_FUNCTION_INFO_V1(test_bitpos);

Datum
test_bitpos(PG_FUNCTION_ARGS)
{
	unsigned long x = 0xA0000000111112A1,
				y = x;
	int			dst_pos[] = {1, 6, 8, 10, 13, 17, 21, 25, 29, 62, 64};
	int			pos,
				i = 0;
	size_t		array_size = sizeof(dst_pos) / sizeof(dst_pos[0]);

	for (i = 0; i < array_size; i++)
	{
		Assert(POLAR_BIT_IS_OCCUPIED(x, dst_pos[i]));
		Assert(!POLAR_BIT_IS_OCCUPIED(x, dst_pos[i] + 1));
		POLAR_BIT_RELEASE_OCCUPIED(x, dst_pos[i]);
		Assert(!POLAR_BIT_IS_OCCUPIED(x, dst_pos[i]));
	}

	Assert(x == 0);
	x = y;
	i = 0;

	while (x)
	{
		POLAR_BIT_LEAST_POS(x, pos);
		Assert(pos == dst_pos[i++]);
		x &= (x - 1);
	}

	Assert(i == array_size);

	for (i = 0; i < array_size; i++)
	{
		POLAR_BIT_OCCUPY(x, dst_pos[i]);
		Assert(POLAR_BIT_IS_OCCUPIED(x, dst_pos[i]));
	}

	Assert(x == y);

	PG_RETURN_INT32(0);
}
