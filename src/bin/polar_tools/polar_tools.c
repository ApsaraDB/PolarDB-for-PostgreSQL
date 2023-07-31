/*-------------------------------------------------------------------------
 *
 * polar_tools.c
 *
 * Copyright (c) 2020, Alibaba Group Holding Limited
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
 *	  src/bin/polar_tools/polar_tools.c
 *
 *-------------------------------------------------------------------------
 */
#include "polar_tools.h"

static void
usage(void)
{
	printf("Polar tools include:\n");
	printf("dump-block-header\n");
	printf("control-data-change\n");
	printf("logindex-meta\n");
	printf("logindex-bloom\n");
	printf("logindex-table\n");
	printf("logindex-page\n");
	printf("dma-meta\n");
	printf("dma-log\n");
	printf("datamax-meta\n");
	printf("datamax-get-wal\n");
	printf("flashback-log-control\n");
	printf("flashback-log-file\n");
	printf("flashback-point-file\n");
	printf("flashback-snapshot-file\n");
	printf("fra-control-file\n");
}

int
main(int argc, char **argv)
{
	if (argc <= 1)
	{
		usage();
		return -1;
	}

	if (strcmp(argv[1], "dump-block-header") == 0)
		return block_header_dump_main(--argc, ++argv);
	else if (strcmp(argv[1], "control-data-change") == 0)
		return control_data_change_main(--argc, ++argv);
	else if (strcmp(argv[1], "logindex-meta") == 0)
		return logindex_meta_main(--argc, ++argv);
	else if (strcmp(argv[1], "logindex-bloom") == 0)
		return logindex_bloom_main(--argc, ++argv);
	else if (strcmp(argv[1], "logindex-table") == 0)
		return logindex_table_main(--argc, ++argv);
	else if (strcmp(argv[1], "logindex-page") == 0)
		return logindex_page_main(--argc, ++argv);
	else if (strcmp(argv[1], "dma-meta") == 0)
		return dma_meta_main(--argc, ++argv);
	else if (strcmp(argv[1], "dma-log") == 0)
		return dma_log_main(--argc, ++argv);
	else if (strcmp(argv[1], "datamax-meta") == 0)
		return datamax_meta_main(--argc, ++argv);
	else if (strcmp(argv[1], "datamax-get-wal") == 0)
		return datamax_get_wal_main(--argc, ++argv);
	else if (strcmp(argv[1], "flashback-log-control") == 0)
		return flashback_log_control_dump_main(--argc, ++argv);
	else if (strcmp(argv[1], "flashback-log-file") == 0)
		return flashback_log_file_dump_main(--argc, ++argv);
	else if (strcmp(argv[1], "flashback-point-file") == 0)
		return flashback_point_file_dump_main(--argc, ++argv);
	else if (strcmp(argv[1], "flashback-snapshot-file") == 0)
		return flashback_snapshot_dump_main(--argc, ++argv);
	else if (strcmp(argv[1], "fra-control-file") == 0)
		return fra_control_dump_main(--argc, ++argv);
	else
		usage();

	return -1;
}
