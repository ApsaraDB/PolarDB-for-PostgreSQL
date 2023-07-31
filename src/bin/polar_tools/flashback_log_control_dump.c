/*-------------------------------------------------------------------------
 *
 * flashback_log_control_dump.c
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
 *	  src/bin/polar_tools/flashback_log_control_dump.c
 *
 *-------------------------------------------------------------------------
 */
#include <time.h>

#include "postgres.h"

#include "polar_tools.h"
#include "polar_flashback/polar_flashback_log_file.h"

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
	printf("Dump flashback log control file usage:\n");
	printf("-f, --file_path  Specify flashback log control file path\n");
	printf("-?, --help show this help, then exit\n");
}

static const char *
get_flog_buf_state(uint16 version_no)
{
	if (version_no & FLOG_SHUTDOWNED)
		return "shutdowned";
	else
		return "in producton or crash while the database is shutdowned";
}

int
flashback_log_control_dump_main(int argc, char **argv)
{
	int	c;
	char *file_path = NULL;
	FILE *fp = NULL;
	bool succeed = false;
	flog_ctl_file_data_t flashback_log_ctl_file;
	pg_crc32c crc;
	char		ckpttime_str[128];
	time_t		time_tmp;
	const char *strftime_fmt = "%c";

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

	if (fread(&flashback_log_ctl_file, 1, sizeof(flog_ctl_file_data_t), fp) !=
			sizeof(flog_ctl_file_data_t))
	{
		fprintf(stderr, "We cannot read the file %s\n", file_path);
		goto end;
	}

	printf(_("The control file version no: %hu\n"),
		   (flashback_log_ctl_file.version_no) & FLOG_CTL_VERSION_MASK);
	printf(_("The flashback log state is %s\n"),
			get_flog_buf_state(flashback_log_ctl_file.version_no));
	printf(_("The flashback log record write result point in the last flashback point begining: %X/%X\n"),
		   (uint32)(flashback_log_ctl_file.fbpoint_info.flog_start_ptr >> 32),
		   (uint32) flashback_log_ctl_file.fbpoint_info.flog_start_ptr);
	printf(_("The flashback log record write result point in the last flashback point end: %X/%X\n"),
		   (uint32)(flashback_log_ctl_file.fbpoint_info.flog_end_ptr >> 32),
		   (uint32) flashback_log_ctl_file.fbpoint_info.flog_end_ptr);
	printf(_("The last flashback log record start point before shutdown: %X/%X\n"),
		   (uint32)(flashback_log_ctl_file.fbpoint_info.flog_end_ptr_prev >> 32),
		   (uint32) flashback_log_ctl_file.fbpoint_info.flog_end_ptr_prev);
	printf(_("The current flashback point WAL lsn: %X/%X\n"),
		   (uint32)(flashback_log_ctl_file.fbpoint_info.wal_info.fbpoint_lsn >> 32),
		   (uint32) flashback_log_ctl_file.fbpoint_info.wal_info.fbpoint_lsn);
	printf(_("The previous flashback point WAL lsn: %X/%X\n"),
		   (uint32)(flashback_log_ctl_file.fbpoint_info.wal_info.prior_fbpoint_lsn >> 32),
		   (uint32) flashback_log_ctl_file.fbpoint_info.wal_info.prior_fbpoint_lsn);

	time_tmp = (time_t) flashback_log_ctl_file.fbpoint_info.wal_info.fbpoint_time;
	strftime(ckpttime_str, sizeof(ckpttime_str), strftime_fmt,
			 localtime(&time_tmp));

	printf(_("The current flashback point time: %s\n"), ckpttime_str);
	printf(_("The flashback log max segment no: %lu\n"),
		   flashback_log_ctl_file.max_seg_no);

	/* Verify CRC */
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, (char *) &flashback_log_ctl_file, offsetof(flog_ctl_file_data_t, crc));
	FIN_CRC32C(crc);
	if (!EQ_CRC32C(crc, flashback_log_ctl_file.crc))
	{
		fprintf(stderr, "The crc is incorrect, got %u, expect %u", crc,
				flashback_log_ctl_file.crc);
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
