/*-------------------------------------------------------------------------
 *
 * fra_control_dump.c
 *
 *
 * Copyright (c) 2020-2120, Alibaba-inc PolarDB Group
 *
 * IDENTIFICATION
 *	  src/bin/polar_tools/fra_control_dump.c
 *
 *-------------------------------------------------------------------------
 */
#include <time.h>

#include "polar_tools.h"
#include "polar_flashback/polar_fast_recovery_area.h"

/*no cover begin*/
static struct option long_options[] =
{
	{"file-path", required_argument, NULL, 'f'},
	{"help", no_argument, NULL, '?'},
	{NULL, 0, NULL, 0}
};

static void
usage(void)
{
	printf("Dump fast recovery area control file usage:\n");
	printf("-f, --file_path  fast recovery area control file path\n");
	printf("-?, --help show this help, then exit\n");
}

int
fra_control_dump_main(int argc, char **argv)
{
	int	c;
	char *file_path = NULL;
	FILE *fp = NULL;
	bool succeed = false;
	fra_ctl_file_data_t ctl_file;
	pg_crc32c crc;

	if (argc <= 1)
	{
		usage();
		return -1;
	}

	while ((c = getopt_long(argc, argv, "f:", long_options, NULL)) != -1)
	{
		switch (c)
		{
			case 'f':
				file_path = strdup(optarg);
				break;

			default:
				usage();
				return -1;
		}
	}

	fp = fopen(file_path, "r");
	if (fp == NULL)
	{
		fprintf(stderr, "Failed to open %s\n", file_path);
		goto end;
	}

	if (fread(&ctl_file, 1, sizeof(fra_ctl_file_data_t), fp) !=
			sizeof(fra_ctl_file_data_t))
	{
		fprintf(stderr, "We cannot read the file %s\n", file_path);
		goto end;
	}

	printf(_("The control file version no: %hu\n"),
		   ctl_file.version_no);
	printf(_("The next flashback point record number: %lu\n"), ctl_file.next_fbpoint_rec_no);
	printf(_("The minimal WAL keep lsn is: %X/%X\n"),
			(uint32)(ctl_file.min_keep_lsn >> 32),
			(uint32) ctl_file.min_keep_lsn);
	printf(_("The next flashback clog sub direcotry number: %X\n"),
			ctl_file.next_clog_subdir_no);
	printf(_("The minimal clog segment number when enable fast recovery area: %04X\n"),
			ctl_file.min_clog_seg_no);

	/* Verify CRC */
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, (char *) &ctl_file, offsetof(fra_ctl_file_data_t, crc));
	FIN_CRC32C(crc);
	if (!EQ_CRC32C(crc, ctl_file.crc))
	{
		fprintf(stderr, "The crc is incorrect, got %u, expect %u", crc,
				ctl_file.crc);
		goto end;
	}
	printf(_("The crc is correct: %X\n"), crc);
	succeed = true;
end:
	if (fp)
		fclose(fp);
	if (file_path)
		free(file_path);

	return succeed ? 0 : -1;
}
/*no cover end*/
