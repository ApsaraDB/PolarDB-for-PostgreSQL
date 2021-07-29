#include "polar_tools.h"
#include "access/polar_logindex.h"
#include "access/polar_logindex_internal.h"

static struct option long_options[] = {
	{"file-path", required_argument, NULL, 'f'},
	{"help", no_argument, NULL, '?'},
	{NULL, 0, NULL, 0}
};

static void
usage(void)
{
	printf("Dump logindex bloom usage:\n");
	printf("-f, --file_path  Specify logindex bloom file path\n");
	printf("-?, --help show this help, then exit\n");
}

int
logindex_bloom_main(int argc, char **argv)
{
	int option;
	int optindex = 0;
	char *file_path = NULL;
	FILE *fp = NULL;
	bool succeed = false;
	log_file_table_bloom_t *bloom = NULL;
	pg_crc32 crc;
	int ret;

	if (argc <= 1)
	{
		usage();
		return -1;
	}

	while ((option = getopt_long(argc, argv, "f:?", long_options, &optindex)) != -1)
	{
		switch (option)
		{
			case 'f':
				file_path = pg_strdup(optarg);
				break;
			case '?':
				usage();
				exit(0);
				break;
			default:
				usage();
				goto end;
		}
	}

	bloom = malloc(LOG_INDEX_FILE_TBL_BLOOM_SIZE);
	if (!bloom)
		goto end;

	if (file_path == NULL)
		goto end;
	
	fp = fopen(file_path, "r");
	if (fp == NULL)
	{
		fprintf(stderr, "Failed to open %s\n", file_path);
		goto end;
	}

	while ((ret = fread(bloom, 1, LOG_INDEX_FILE_TBL_BLOOM_SIZE, fp)) == LOG_INDEX_FILE_TBL_BLOOM_SIZE)
	{
		crc = bloom->crc;
		bloom->crc = 0;
		bloom->crc = log_index_calc_crc((unsigned char *)bloom, LOG_INDEX_FILE_TBL_BLOOM_SIZE);
		
		if (crc != bloom->crc)
			fprintf(stderr, "The bloom crc is incorrect ,got %u, expect %u\n", bloom->crc, crc);

		printf("idx_table_id=%ld min_lsn=%lX max_lsn=%lX buf_size=%u crc=%u\n",
				bloom->idx_table_id, bloom->min_lsn, bloom->max_lsn, bloom->buf_size, bloom->crc);

	}

	if ((ret > 0 && ret != LOG_INDEX_FILE_TBL_BLOOM_SIZE) || errno != 0)
	{
		fprintf(stderr, "Failed to read logindex bloom, errno=%d\n", errno);
		goto end;
	}

	succeed = true;

end:
	if (fp)
		fclose(fp);

	if (file_path)
		free(file_path);
	
	if (bloom)
		free(bloom);

	return succeed ? 0 : -1;
}
