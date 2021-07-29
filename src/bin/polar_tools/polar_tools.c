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
	else
		usage();

	return -1;
}
