/*-------------------------------------------------------------------------
 *
 * flashback_point_file_dump.c
 *
 *
 * Copyright (c) 2021, Alibaba Group Holding limited
 *
 * IDENTIFICATION
 *	  src/bin/polar_tools/flashback_point_file_dump.c
 *
 *-------------------------------------------------------------------------
 */
#include <libgen.h>
#include <time.h>

#include "postgres.h"

#include "polar_flashback/polar_fast_recovery_area.h"
#include "polar_flashback/polar_flashback_log_file.h"
#include "polar_tools.h"

/*no cover begin*/
static struct option long_options[] =
{
	{"ctl-file-path", required_argument, NULL, 'c'},
	{"file-path", required_argument, NULL, 'f'},
	{"max-rec-no", required_argument, NULL, 'm'},
	{"help", no_argument, NULL, '?'},
	{NULL, 0, NULL, 0}
};

static void
usage(void)
{
	printf("Dump flashback point record file usage:\n");
	printf("-c, --ctl-file-path  Specify fast recovery area control path\n");
	printf("-f, --file-path  Specify flashback point record file path\n");
	printf("-m, --max-rec-no  Specify max record number, start from 0 \n");
	printf("-w, --with-snapshot  output with snapshot \n");
	printf("-?, --help show this help, then exit\n");
}

static void
ckp_file_page_dump(const char *dir, char *start_ptr, uint32 seg_no, uint32 page_no, uint64 max_slot_no, bool with_snapshot)
{
	uint64 slot_no;
	fbpoint_rec_data_t *tkp_info;
	uint64 max_slot_in_page;

	slot_no = page_no * FBPOINT_REC_PER_PAGE;
	tkp_info = (fbpoint_rec_data_t *) ((char *) start_ptr + page_no * FBPOINT_PAGE_SIZE + FBPOINT_PAGE_HEADER_SIZE);

	max_slot_in_page = Min(max_slot_no, slot_no + FBPOINT_REC_PER_PAGE - 1);
	while (slot_no <= max_slot_in_page)
	{
		char		ckpttime_str[128];
		time_t		time_tmp;
		const char *strftime_fmt = "%c";

		time_tmp = (time_t) (tkp_info->time);
		strftime(ckpttime_str, sizeof(ckpttime_str), strftime_fmt,
				 localtime(&time_tmp));

		printf(_("------------------------------------------------------------------------------\n"));
		printf(_("------------------------------------------------------------------------------\n"));
		printf(_("The %2lu flashback point record in the page %2u: "
				"flashback log pointer is %08X/%08X, "
				"WAL lsn is %08X/%08X, checkpoint time is %s, "
				"next clog sub directory number is %08X, "
				"snapshot segment no. is %u, snapshot offset is %u\n"),
				slot_no, page_no,
				(uint32) (tkp_info->flog_ptr >> 32), (uint32) (tkp_info->flog_ptr),
				(uint32) (tkp_info->redo_lsn >> 32), (uint32) (tkp_info->redo_lsn),
				ckpttime_str, tkp_info->next_clog_subdir_no,
				tkp_info->snapshot_pos.seg_no, tkp_info->snapshot_pos.offset);

		if (with_snapshot)
			flashback_snapshot_dump(dir, tkp_info->snapshot_pos);

		slot_no++;
		tkp_info = (fbpoint_rec_data_t *) ((char *) tkp_info + FBPOINT_REC_SIZE);
	}
}

static pg_crc32c
page_comp_crc(fbpoint_page_header_t *header)
{
	pg_crc32c	crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, (char *) header + FBPOINT_PAGE_HEADER_SIZE, FBPOINT_PAGE_SIZE - FBPOINT_PAGE_HEADER_SIZE);
	COMP_CRC32C(crc, (char *) header, offsetof(fbpoint_page_header_t, crc));
	FIN_CRC32C(crc);

	return crc;
}

int
flashback_point_file_dump_main(int argc, char **argv)
{
	int	c;
	char *ctl_file_path = NULL;
	FILE *ctl_fp = NULL;
	fra_ctl_file_data_t ctl_file;
	pg_crc32c crc;
	char *file_path = NULL;
	uint64 max_slot_no = 0;
	char       *fname = NULL;
	uint32 seg_no;
	uint32 max_page_no;
	uint32 page_no = 0;
	char ckp_info_seg[FBPOINT_REC_END_POS];
	bool succeed = false;
	bool use_ctl_file = false;
	bool use_input = false;
	bool with_snapshot = false;
	FILE *fp = NULL;
	char *dir;

	if (argc <= 1)
	{
		usage();
		return -1;
	}

	while ((c = getopt_long(argc, argv, "c:f:m:w", long_options, NULL)) != -1)
	{
		switch (c)
		{
			case 'c':
				ctl_file_path = strdup(optarg);
				use_ctl_file = true;
				break;
			case 'f':
				file_path = strdup(optarg);
				break;
			case 'm':
				max_slot_no = (uint64) strtoull(optarg, NULL, 10);
				use_input = true;
				break;
			case 'w':
				with_snapshot = true;
				break;
			default:
				usage();
				return -1;
		}
	}

	if (use_ctl_file)
	{
		/* Read the max slot no from control file */
		ctl_fp = fopen(ctl_file_path, "r");

		if (ctl_fp == NULL)
		{
			fprintf(stderr, "Failed to open control file %s\n", ctl_file_path);
			goto end;
		}

		if (fread(&ctl_file, 1, sizeof(fra_ctl_file_data_t), ctl_fp) !=
				sizeof(fra_ctl_file_data_t))
		{
			fprintf(stderr, "We cannot read the control file %s\n", ctl_file_path);
			goto end;
		}

		/* Verify CRC */
		INIT_CRC32C(crc);
		COMP_CRC32C(crc, (char *) &ctl_file, offsetof(fra_ctl_file_data_t, crc));
		FIN_CRC32C(crc);

		if (!EQ_CRC32C(crc, ctl_file.crc))
		{
			fprintf(stderr, "The crc in control file is incorrect, got %u, expect %u", crc,
					ctl_file.crc);
			goto end;
		}

		fclose(ctl_fp);
		ctl_fp = NULL;

		if (ctl_file.next_fbpoint_rec_no == 0)
		{
			printf(_("There are no flashback point record file.\n"));
			succeed = true;
			goto end;
		}

		max_slot_no = ctl_file.next_fbpoint_rec_no - 1;
	}
	else if (!use_input)
	{
		printf(_("Please input the -m max flashback point record or -c fast recovery control file path.\n"));
		succeed = false;
		goto end;
	}

	/* Dump the checkpoint file */
	fname = basename(file_path);
	dir = dirname(file_path);
	FBPOINT_GET_SEG_FROM_FNAME(fname, seg_no);

	if (FBPOINT_GET_SEG_NO_BY_REC_NO(max_slot_no) == seg_no)
	{
		max_page_no = FBPOINT_GET_PAGE_NO_BY_REC_NO(max_slot_no);
		max_slot_no = max_slot_no % FBPOINT_REC_PER_SEG;
	}
	else
	{
		max_page_no = FBPOINT_PAGE_PER_SEG - 1;
		max_slot_no = seg_no * FBPOINT_REC_PER_SEG - 1;
	}

	printf(_("The flashback point record file %s has %u pages\n"), file_path, max_page_no + 1);

	fp = fopen(file_path, "r");

	if (fp == NULL)
	{
		fprintf(stderr, "Failed to open flashback point record file %s\n", file_path);
		goto end;
	}

	if (fread(ckp_info_seg, 1, FBPOINT_REC_END_POS, fp) != FBPOINT_REC_END_POS)
	{
		fprintf(stderr, "We cannot read the flashback point record file %s\n", file_path);
		goto end;
	}

	fclose(fp);
	fp = NULL;

	while (page_no <= max_page_no)
	{
		char       *page_start;

		/* Check the crc of the page */
		page_start = ckp_info_seg + page_no * FBPOINT_PAGE_SIZE;
		crc = page_comp_crc((fbpoint_page_header_t *) page_start);

		if (!EQ_CRC32C(crc, ((fbpoint_page_header_t *) page_start)->crc))
		{
			fprintf(stderr, "The crc of page %u in flashback point record file %s is incorrect, "
					"got %u, expect %u", page_no, file_path, crc, ((fbpoint_page_header_t *) page_start)->crc);
			goto end;
		}

		ckp_file_page_dump(dir, ckp_info_seg, seg_no, page_no, max_slot_no, with_snapshot);

		printf(_("\n"));
		page_no++;
	}

	succeed = true;
end:
	if (ctl_fp)
		fclose(ctl_fp);
	if (ctl_file_path)
		free(ctl_file_path);
	if (fp)
		fclose(fp);
	if (file_path)
		free(file_path);

	return succeed ? 0 : -1;
}
/*no cover end*/
