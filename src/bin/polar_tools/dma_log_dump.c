#include "polar_tools.h"
#include "polar_dma/consensus_log.h"

static struct option long_options[] = {
	{"file-path", required_argument, NULL, 'f'},
	{"help", no_argument, NULL, '?'},
	{NULL, 0, NULL, 0}
};

static void
usage(void)
{
	printf("Dump dma log usage:\n");
	printf("-f, --file_path  Specify dma log file path\n");
	printf("-?, --help show this help, then exit\n");
}

int 
dma_log_main(int argc, char **argv)
{
	int	c;
	char *file_path = NULL;
	FILE *fp = NULL;
	bool succeed = false;
	char page[CONSENSUS_LOG_PAGE_SIZE];
	ConsensusLogEntry *log_entry;
	FixedLengthLogPageTrailer *page_trailer;
	int i = 0;
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

	while (true)
	{
		if (fread(&page, 1, CONSENSUS_LOG_PAGE_SIZE, fp) != CONSENSUS_LOG_PAGE_SIZE)
			break;

		page_trailer = (FixedLengthLogPageTrailer *)
			(page + CONSENSUS_LOG_PAGE_SIZE - sizeof(FixedLengthLogPageTrailer));

		printf(_("page last term:      %lu\t"), page_trailer->last_term);
		printf(_("page start timeline: %u\t"), page_trailer->start_timeline);
		printf(_("page start LSN:      %X/%X\t"), (uint32) (page_trailer->start_lsn >> 32), (uint32) page_trailer->start_lsn);
		printf(_("page start offset:   %lu\t"), page_trailer->start_offset);
		printf(_("page end offset:     %lu\t"), page_trailer->end_offset);
		printf(_("page end etnry:      %lu\n"), page_trailer->end_entry);

		for (i = 0; i < page_trailer->end_entry; i++)
		{
			log_entry = (ConsensusLogEntry *) (page + i * sizeof(ConsensusLogEntry));

			INIT_CRC32C(crc);
			COMP_CRC32C(crc, ((char *) log_entry), offsetof(ConsensusLogEntry, log_crc));
			if (log_entry->log_crc != crc)
				fprintf(stderr, "The crc is incorrect, got %u, expect %u", crc, log_entry->log_crc);

			printf(_("log term:        %lu\t"), log_entry->header.log_term);
			printf(_("log index:       %lu\t"), log_entry->header.log_index);
			printf(_("log op type:     %u\t"), log_entry->header.op_type);
			printf(_("log LSN:         %X/%X\t"), (uint32) (log_entry->log_lsn >> 32), (uint32) log_entry->log_lsn);
			printf(_("log timeline:    %u\t"), log_entry->log_timeline);
			printf(_("variable length: %u\t"), log_entry->variable_length);
			printf(_("variable offset: %lu\n"), log_entry->variable_offset);
		}
		
		if (page_trailer->end_entry < FIXED_LENGTH_LOGS_PER_PAGE)
			break;
	}

	succeed = true;
end:
	if (fp)
		fclose(fp);
	if (file_path)
		free(file_path);

	return succeed ? 0 : -1;
}
