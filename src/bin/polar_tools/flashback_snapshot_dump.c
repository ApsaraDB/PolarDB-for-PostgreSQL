/*-------------------------------------------------------------------------
 *
 * flashback_snapshot_dump.c
 *
 *
 * Copyright (c) 2021, Alibaba Group Holding limited
 *
 * IDENTIFICATION
 *	  src/bin/polar_tools/flashback_snapshot_dump.c
 *
 *-------------------------------------------------------------------------
 */
#include <libgen.h>
#include <time.h>

#include "postgres.h"

#include "polar_flashback/polar_fast_recovery_area.h"
#include "polar_flashback/polar_flashback_snapshot.h"
#include "polar_tools.h"

/*no cover begin*/
static struct option long_options[] =
{
	{"file-path", required_argument, NULL, 'f'},
	{"start-pos", required_argument, NULL, 's'},
	{"help", no_argument, NULL, '?'},
	{NULL, 0, NULL, 0}
};

static void
usage(void)
{
	printf("Dump flashback snapshot file usage:\n");
	printf("-f, --file-path  Specify flashback point file path\n");
	printf("-s, --start-pos  Specify snapshot data start position\n");
	printf("-?, --help show this help, then exit\n");
}

static void
flashback_snapshot_header_dump(flashback_snapshot_data_header_t *head)
{
	printf(_("The flashback snapshot data version no: %d\n"),
			GET_FLSHBAK_SNAPSHOT_VERSION(head->info));
	printf(_("The flashback snapshot data size: %u\n"),
			head->data_size);
}

static void
flashback_snapshot_data_dump(flashback_snapshot_t snapshot)
{
	int i;
	int count = 0;
	TransactionId *xip;

	printf(_("The snapshot is exported while the current insert lsn of WAL is %X/%X \n"),
			(uint32) (snapshot->lsn >> 32), (uint32) (snapshot->lsn));
	printf(_("The xmin is %u\n"), snapshot->xmin);
	printf(_("The xmax is %u\n"), snapshot->xmax);

	xip = (TransactionId *) POLAR_GET_FLSHBAK_SNAPSHOT_XIP(snapshot);

	printf(_("The snapshot is exported in RW node\n"));
	count = snapshot->xcnt;

	if (count > 0)
		printf(_("The %u running transactions between xmin and xmax are"), snapshot->xcnt);
	else
		printf(_("There no running transactions between xmin and xmax"));

	for (i = 0; i < count; i++)
		printf(_(" xip:%u"), xip[i]);

	printf(_("\n"));
}

void
flashback_snapshot_dump(const char *dir, fbpoint_pos_t snapshot_pos)
{
	char file_path[MAXPGPATH];
	FILE *fp = NULL;
	bool succeed = false;
	size_t read_len;
	pg_crc32c   crc;
	flashback_snapshot_data_header_t header;
	uint32 data_size;
	flashback_snapshot_t snapshot;
	uint32 segno = snapshot_pos.seg_no;
	uint32 offset  = snapshot_pos.offset;
	uint32 end_pos;
	char *data;

	snprintf(file_path, MAXPGPATH, "%s/%08X", dir, segno);

	/* Read the header */
	fp = fopen(file_path, "r");

	if (fp == NULL)
	{
		fprintf(stderr, "Failed to open flashback point file %s\n", file_path);
		goto end;
	}

	if (fseek(fp, offset, SEEK_SET) != 0)
	{
		fprintf(stderr, "Failed to seek %s offset %d\n", file_path, offset);
		goto end;
	}

	read_len = fread(&header, 1, FLSHBAK_SNAPSHOT_HEADER_SIZE, fp);

	if (read_len != FLSHBAK_SNAPSHOT_HEADER_SIZE)
	{
		fprintf(stderr, "could not read file header from flashback point file %s\n", file_path);
		goto end;
	}

	flashback_snapshot_header_dump(&header);
	data_size = header.data_size;
	data = palloc(data_size);
	snapshot = (flashback_snapshot_t) data;
	end_pos = GET_FLSHBAK_SNAPSHOT_END_POS(header.info);
	offset += FLSHBAK_SNAPSHOT_HEADER_SIZE;

	do{
		read_len = end_pos - offset;

		if (read_len)
		{
			if (fseek(fp, offset, SEEK_SET) != 0)
			{
				fprintf(stderr, "Failed to seek %s offset %d\n", file_path, offset);
				goto end;
			}

			if (fread(data, 1, read_len, fp) != read_len)
			{
				fprintf(stderr, "Failed to read snapshot data from %s\n", file_path);
				goto end;
			}

			fclose(fp);
			fp = NULL;
		}

		data_size -= read_len;

		/* Can break the loop only in here */
		if (data_size == 0)
			break;

		end_pos = FBPOINT_SEG_SIZE;

		if (data_size > (FBPOINT_SEG_SIZE - FBPOINT_REC_END_POS))
			offset = FBPOINT_REC_END_POS;
		else
			offset = FBPOINT_SEG_SIZE - data_size;

		segno++;
		data += read_len;
		snprintf(file_path, MAXPGPATH, "%s/%08X", dir, segno);
		fp = fopen(file_path, "r");

		if (fp == NULL)
		{
			fprintf(stderr, "Failed to open flashback point file %s\n", file_path);
			goto end;
		}
	} while (data_size > 0);

	INIT_CRC32C(crc);
	COMP_CRC32C((crc), (char *) (snapshot), (header.data_size));
	COMP_CRC32C((crc), (char *) (&header), offsetof(flashback_snapshot_data_header_t, crc));
	FIN_CRC32C(crc);

	if (!EQ_CRC32C(crc, header.crc))
	{
		/*no cover line*/
		fprintf(stderr, "calculated snapshot data CRC checksum does not match "
				"value stored in file \"%s\"", file_path);

		goto end;
	}

	flashback_snapshot_data_dump(snapshot);
	succeed = true;
end:
	if (fp)
		fclose(fp);

	if (!succeed)
		exit(EXIT_FAILURE);
}

int
flashback_snapshot_dump_main(int argc, char **argv)
{
	int	c;
	char *file_path = NULL;
	bool succeed = false;
	uint32 start_pos = 0;
	char *fname;
	char *dir;
	fbpoint_pos_t snapshot_pos;
	uint32 seg_no;

	if (argc <= 1)
	{
		usage();
		return -1;
	}

	while ((c = getopt_long(argc, argv, "f:s:", long_options, NULL)) != -1)
	{
		switch (c)
		{
			case 'f':
				file_path = strdup(optarg);
				break;
			case 's':
				start_pos = (uint32) atoi(optarg);
				break;
			default:
				usage();
				return -1;
		}
	}

	if (start_pos >= FBPOINT_SEG_SIZE || start_pos < FBPOINT_REC_END_POS)
	{
		fprintf(stderr, "Must set a valid start position (larger than %d, less than %d) with -s or --start-pos \n",
				FBPOINT_REC_END_POS, FBPOINT_SEG_SIZE);
		goto end;
	}

	fname = basename(file_path);
	FBPOINT_GET_SEG_FROM_FNAME(fname, seg_no);
	dir = dirname(file_path);
	SET_FBPOINT_POS(snapshot_pos, seg_no, start_pos);
	flashback_snapshot_dump(dir, snapshot_pos);
	succeed = true;
end:
	if (file_path)
		free(file_path);

	return succeed ? 0 : -1;
}
/*no cover end*/
